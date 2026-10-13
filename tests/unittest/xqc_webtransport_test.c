/**
 * @copyright Copyright (c) 2022, Alibaba Group Holding Limited
 *
 * WebTransport over HTTP/3 unit tests. Wire behaviors follow
 * draft-ietf-webtrans-http3-07 (sections cited per test), with
 * RFC 9220 (extended CONNECT) and RFC 9297 (HTTP datagrams).
 */

#include "xqc_webtransport_test.h"

#include <CUnit/CUnit.h>
#include <string.h>

#include "xquic/xquic.h"
#include "xquic/xqc_errno.h"
#include "src/http3/xqc_h3_defs.h"
#include "src/http3/xqc_h3_conn.h"
#include "src/http3/xqc_h3_stream.h"
#include "src/http3/xqc_h3_request.h"
#include "src/http3/xqc_webtransport.h"
#include "src/http3/frame/xqc_h3_frame.h"
#include "src/transport/xqc_engine.h"
#include "src/common/utils/vint/xqc_variable_len_int.h"

#include "xqc_common_test.h"

/* not exposed in xqc_h3_stream.h, but stable file-scope entries used by tests */
xqc_int_t xqc_h3_stream_process_in(xqc_h3_stream_t *h3s, unsigned char *data,
    size_t data_len, xqc_bool_t fin_flag);
int xqc_h3_stream_read_notify(xqc_stream_t *stream, void *user_data);
int xqc_h3_stream_write_notify(xqc_stream_t *stream, void *user_data);

static xqc_h3_stream_t *
xqc_wt_ctrl_test_setup(xqc_connection_t **out_conn, xqc_h3_conn_t **out_h3c,
    uint8_t wt_enabled, uint64_t wt_max_sessions)
{
    xqc_connection_t *conn = test_engine_connect();
    if (conn == NULL) {
        return NULL;
    }

    if (conn->alpn) {
        xqc_free(conn->alpn);
    }
    conn->alpn_len = strlen(XQC_ALPN_H3);
    conn->alpn = xqc_calloc(1, conn->alpn_len + 1);
    xqc_memcpy(conn->alpn, XQC_ALPN_H3, conn->alpn_len);

    conn->conn_flow_ctl.fc_max_streams_uni_can_send = 1024;
    conn->conn_state = XQC_CONN_STATE_ESTABED;

    if (wt_enabled) {
        conn->engine->config->enable_webtransport = 1;
        conn->engine->config->webtransport_max_sessions = wt_max_sessions;
    }

    xqc_h3_conn_t *h3c = xqc_h3_conn_create(conn, NULL);
    if (h3c == NULL) {
        return NULL;
    }

    xqc_stream_t *stream = xqc_create_stream_with_conn(conn,
            XQC_UNDEFINE_STREAM_ID, XQC_CLI_UNI, NULL, NULL);
    if (stream == NULL) {
        return NULL;
    }

    xqc_h3_stream_t *h3s = xqc_h3_stream_create(h3c, stream,
            XQC_H3_STREAM_TYPE_CONTROL, NULL);
    if (h3s == NULL) {
        return NULL;
    }

    *out_conn = conn;
    *out_h3c = h3c;
    return h3s;
}

static void
xqc_wt_ctrl_test_teardown(xqc_h3_stream_t *h3s, xqc_h3_conn_t *h3c,
    xqc_connection_t *conn)
{
    xqc_stream_t *stream = h3s->stream;
    stream->stream_flag |= XQC_STREAM_FLAG_DISCARDED;
    xqc_h3_stream_destroy(h3s);
    xqc_destroy_stream(stream);
    xqc_h3_conn_destroy(h3c);
    if (conn->alpn) {
        xqc_free(conn->alpn);
    }
}

static ssize_t
xqc_wt_feed_settings(xqc_h3_stream_t *h3s, const unsigned char *payload,
    size_t payload_len)
{
    unsigned char frame[XQC_H3_SETTINGS_MAX_FRAME_SIZE];
    unsigned char *pos = frame;

    CU_ASSERT_FATAL(payload_len + 16 < sizeof(frame));

    pos = xqc_put_varint(pos, XQC_H3_FRM_SETTINGS);
    pos = xqc_put_varint(pos, payload_len);
    xqc_memcpy(pos, payload, payload_len);
    pos += payload_len;

    return xqc_h3_stream_process_control(h3s, frame, pos - frame);
}

/*
 * Parse a serialized SETTINGS frame; returns the number of entries read,
 * or -1 on malformed input. Frame type and length are consumed.
 */
static ssize_t
xqc_wt_parse_settings(const unsigned char *data, size_t data_len,
    uint64_t *ids, uint64_t *vals, size_t max_pairs)
{
    const unsigned char *pos = data;
    const unsigned char *end = data + data_len;
    uint64_t frame_type = 0, frame_len = 0;
    int n;

    n = xqc_vint_read(pos, end, &frame_type);
    if (n <= 0 || frame_type != XQC_H3_FRM_SETTINGS) {
        return -1;
    }
    pos += n;

    n = xqc_vint_read(pos, end, &frame_len);
    if (n <= 0) {
        return -1;
    }
    pos += n;

    if ((size_t)(end - pos) != frame_len) {
        return -1;
    }

    size_t count = 0;
    while (pos < end) {
        if (count >= max_pairs) {
            return -1;
        }
        n = xqc_vint_read(pos, end, ids + count);
        if (n <= 0) {
            return -1;
        }
        pos += n;

        n = xqc_vint_read(pos, end, vals + count);
        if (n <= 0) {
            return -1;
        }
        pos += n;
        ++count;
    }

    return (ssize_t)count;
}

static int
xqc_wt_settings_find(const uint64_t *ids, const uint64_t *vals, size_t count,
    uint64_t id, uint64_t *value)
{
    for (size_t i = 0; i < count; ++i) {
        if (ids[i] == id) {
            *value = vals[i];
            return 1;
        }
    }
    return 0;
}

void
xqc_test_wt_settings()
{
    /*
     * draft-ietf-webtrans-http3-07 Section 8.2, RFC 9220 Section 3,
     * RFC 9297 Section 2.1.1: lock the wire constants. The draft-02
     * aliases (draft-ietf-webtrans-http3-02 Section 3,
     * draft-ietf-masque-h3-datagram-04) are advertised alongside so
     * stock browsers, which only implement draft-02, can negotiate.
     */
    CU_ASSERT(XQC_H3_SETTINGS_ENABLE_CONNECT_PROTOCOL == 0x08);
    CU_ASSERT(XQC_H3_SETTINGS_H3_DATAGRAM == 0x33);
    CU_ASSERT(XQC_H3_SETTINGS_WEBTRANSPORT_MAX_SESSIONS == 0xc671706a);
    CU_ASSERT(XQC_H3_SETTINGS_WEBTRANSPORT_DRAFT00 == 0x2b603742);
    CU_ASSERT(XQC_H3_SETTINGS_H3_DATAGRAM_DRAFT04 == 0xffd277);

    const uint64_t max_sessions = 32;

    /* --- WebTransport enabled: local settings and serialized frame --- */
    {
        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_ctrl_test_setup(&conn, &h3c, 1,
            max_sessions);
        CU_ASSERT_FATAL(h3s != NULL);

        CU_ASSERT(h3c->local_h3_conn_settings.enable_connect_protocol == 1);
        CU_ASSERT(h3c->local_h3_conn_settings.h3_datagram == 1);
        CU_ASSERT(h3c->local_h3_conn_settings.webtransport_max_sessions
                  == max_sessions);

        xqc_list_head_t send_buf;
        xqc_init_list_head(&send_buf);
        CU_ASSERT(xqc_h3_frm_write_settings(&send_buf,
                                            &h3c->local_h3_conn_settings, 0)
                  == XQC_OK);

        xqc_list_head_t *pos, *next;
        size_t bufs = 0;
        xqc_list_for_each_safe(pos, next, &send_buf) {
            xqc_list_buf_t *lb = xqc_list_entry(pos, xqc_list_buf_t,
                list_head);
            uint64_t ids[12] = {0}, vals[12] = {0}, value = 0;
            ssize_t count = xqc_wt_parse_settings(lb->buf->data,
                lb->buf->data_len, ids, vals, 12);

            /* 3 base entries + 5 WebTransport entries (draft-07 with
               draft-02 aliases) */
            CU_ASSERT(count == 8);
            if (count == 8) {
                CU_ASSERT(xqc_wt_settings_find(ids, vals, count, 0x08, &value)
                          == 1 && value == 1);
                CU_ASSERT(xqc_wt_settings_find(ids, vals, count, 0x33, &value)
                          == 1 && value == 1);
                CU_ASSERT(xqc_wt_settings_find(ids, vals, count,
                          0xc671706a, &value) == 1
                          && value == max_sessions);
                CU_ASSERT(xqc_wt_settings_find(ids, vals, count,
                          0x2b603742, &value) == 1 && value == 1);
                CU_ASSERT(xqc_wt_settings_find(ids, vals, count,
                          0xffd277, &value) == 1 && value == 1);
            }
            ++bufs;

            xqc_list_buf_free(lb);
        }
        CU_ASSERT(bufs == 1);

        xqc_wt_ctrl_test_teardown(h3s, h3c, conn);
    }

    /* --- WebTransport disabled: the frame keeps its three entries --- */
    {
        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_ctrl_test_setup(&conn, &h3c, 0, 0);
        CU_ASSERT_FATAL(h3s != NULL);

        CU_ASSERT(h3c->local_h3_conn_settings.enable_connect_protocol == 0);
        CU_ASSERT(h3c->local_h3_conn_settings.h3_datagram == 0);
        CU_ASSERT(h3c->local_h3_conn_settings.webtransport_max_sessions == 0);

        xqc_list_head_t send_buf;
        xqc_init_list_head(&send_buf);
        CU_ASSERT(xqc_h3_frm_write_settings(&send_buf,
                                            &h3c->local_h3_conn_settings, 0)
                  == XQC_OK);

        xqc_list_head_t *pos, *next;
        xqc_list_for_each_safe(pos, next, &send_buf) {
            xqc_list_buf_t *lb = xqc_list_entry(pos, xqc_list_buf_t,
                list_head);
            uint64_t ids[8] = {0}, vals[8] = {0}, value = 0;
            ssize_t count = xqc_wt_parse_settings(lb->buf->data,
                lb->buf->data_len, ids, vals, 8);

            CU_ASSERT(count == 3);
            if (count > 0) {
                CU_ASSERT(xqc_wt_settings_find(ids, vals, count, 0x08, &value)
                          == 0);
                CU_ASSERT(xqc_wt_settings_find(ids, vals, count, 0x33, &value)
                          == 0);
                CU_ASSERT(xqc_wt_settings_find(ids, vals, count,
                          0xc671706a, &value) == 0);
            }

            xqc_list_buf_free(lb);
        }

        xqc_wt_ctrl_test_teardown(h3s, h3c, conn);
    }

    /* --- Peer settings accepted and stored --- */
    {
        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_ctrl_test_setup(&conn, &h3c, 1,
            max_sessions);
        CU_ASSERT_FATAL(h3s != NULL);

        unsigned char payload[64];
        unsigned char *pos = payload;
        pos = xqc_put_varint(pos, XQC_H3_SETTINGS_ENABLE_CONNECT_PROTOCOL);
        pos = xqc_put_varint(pos, 1);
        pos = xqc_put_varint(pos, XQC_H3_SETTINGS_H3_DATAGRAM);
        pos = xqc_put_varint(pos, 1);
        pos = xqc_put_varint(pos, XQC_H3_SETTINGS_WEBTRANSPORT_MAX_SESSIONS);
        pos = xqc_put_varint(pos, 1000);
        pos = xqc_put_varint(pos, XQC_H3_SETTINGS_WEBTRANSPORT_DRAFT00);
        pos = xqc_put_varint(pos, 1);
        pos = xqc_put_varint(pos, XQC_H3_SETTINGS_H3_DATAGRAM_DRAFT04);
        pos = xqc_put_varint(pos, 1);

        CU_ASSERT(conn->conn_err == 0);
        CU_ASSERT(xqc_wt_feed_settings(h3s, payload, pos - payload) >= 0);
        CU_ASSERT(conn->conn_err == 0);
        CU_ASSERT(h3c->peer_h3_conn_settings.enable_connect_protocol == 1);
        CU_ASSERT(h3c->peer_h3_conn_settings.h3_datagram == 1);
        CU_ASSERT(h3c->peer_h3_conn_settings.webtransport_max_sessions
                  == 1000);

        xqc_wt_ctrl_test_teardown(h3s, h3c, conn);
    }

    /* --- Invalid ENABLE_CONNECT_PROTOCOL value: H3_SETTINGS_ERROR --- */
    {
        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_ctrl_test_setup(&conn, &h3c, 1,
            max_sessions);
        CU_ASSERT_FATAL(h3s != NULL);

        unsigned char payload[16];
        unsigned char *pos = payload;
        pos = xqc_put_varint(pos, XQC_H3_SETTINGS_ENABLE_CONNECT_PROTOCOL);
        pos = xqc_put_varint(pos, 2);

        CU_ASSERT(xqc_wt_feed_settings(h3s, payload, pos - payload) < 0);
        CU_ASSERT(XQC_CONN_ERR_CODE(conn->conn_err) == H3_SETTINGS_ERROR);
        CU_ASSERT(conn->conn_flag & XQC_CONN_FLAG_ERROR);

        xqc_wt_ctrl_test_teardown(h3s, h3c, conn);
    }

    /* --- Invalid H3_DATAGRAM value: H3_SETTINGS_ERROR --- */
    {
        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_ctrl_test_setup(&conn, &h3c, 1,
            max_sessions);
        CU_ASSERT_FATAL(h3s != NULL);

        unsigned char payload[16];
        unsigned char *pos = payload;
        pos = xqc_put_varint(pos, XQC_H3_SETTINGS_H3_DATAGRAM);
        pos = xqc_put_varint(pos, 2);

        CU_ASSERT(xqc_wt_feed_settings(h3s, payload, pos - payload) < 0);
        CU_ASSERT(XQC_CONN_ERR_CODE(conn->conn_err) == H3_SETTINGS_ERROR);
        CU_ASSERT(conn->conn_flag & XQC_CONN_FLAG_ERROR);

        xqc_wt_ctrl_test_teardown(h3s, h3c, conn);
    }

    /* --- Invalid WEBTRANSPORT_DRAFT00 value: H3_SETTINGS_ERROR --- */
    {
        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_ctrl_test_setup(&conn, &h3c, 1,
            max_sessions);
        CU_ASSERT_FATAL(h3s != NULL);

        unsigned char payload[16];
        unsigned char *pos = payload;
        pos = xqc_put_varint(pos, XQC_H3_SETTINGS_WEBTRANSPORT_DRAFT00);
        pos = xqc_put_varint(pos, 2);

        CU_ASSERT(xqc_wt_feed_settings(h3s, payload, pos - payload) < 0);
        CU_ASSERT(XQC_CONN_ERR_CODE(conn->conn_err) == H3_SETTINGS_ERROR);
        CU_ASSERT(conn->conn_flag & XQC_CONN_FLAG_ERROR);

        xqc_wt_ctrl_test_teardown(h3s, h3c, conn);
    }

    /* --- Invalid H3_DATAGRAM_DRAFT04 value: H3_SETTINGS_ERROR --- */
    {
        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_ctrl_test_setup(&conn, &h3c, 1,
            max_sessions);
        CU_ASSERT_FATAL(h3s != NULL);

        unsigned char payload[16];
        unsigned char *pos = payload;
        pos = xqc_put_varint(pos, XQC_H3_SETTINGS_H3_DATAGRAM_DRAFT04);
        pos = xqc_put_varint(pos, 2);

        CU_ASSERT(xqc_wt_feed_settings(h3s, payload, pos - payload) < 0);
        CU_ASSERT(XQC_CONN_ERR_CODE(conn->conn_err) == H3_SETTINGS_ERROR);
        CU_ASSERT(conn->conn_flag & XQC_CONN_FLAG_ERROR);

        xqc_wt_ctrl_test_teardown(h3s, h3c, conn);
    }
}

static xqc_h3_stream_t *
xqc_wt_req_test_setup(xqc_connection_t **out_conn, xqc_h3_conn_t **out_h3c,
    uint8_t wt_enabled)
{
    xqc_connection_t *conn = test_engine_connect();
    if (conn == NULL) {
        return NULL;
    }

    if (conn->alpn) {
        xqc_free(conn->alpn);
    }
    conn->alpn_len = strlen(XQC_ALPN_H3);
    conn->alpn = xqc_calloc(1, conn->alpn_len + 1);
    xqc_memcpy(conn->alpn, XQC_ALPN_H3, conn->alpn_len);

    /* test_connect() creates the conn with the "transport" ALPN whose callbacks
       are all NULL; production resolves conn->app_proto_cbs when the negotiated
       ALPN is known. Streams capture conn->app_proto_cbs.stream_cbs at creation,
       so refresh the callbacks to the h3 set after swapping the ALPN. */
    if (xqc_engine_get_alpn_callbacks(conn->engine, XQC_ALPN_H3,
            conn->alpn_len, &conn->app_proto_cbs) != XQC_OK)
    {
        return NULL;
    }

    conn->conn_flow_ctl.fc_max_streams_bidi_can_send = 1024;
    conn->conn_flow_ctl.fc_max_streams_uni_can_send = 1024;
    conn->conn_state = XQC_CONN_STATE_ESTABED;

    if (wt_enabled) {
        conn->engine->config->enable_webtransport = 1;
        conn->engine->config->webtransport_max_sessions = 16;
    }

    xqc_h3_conn_t *h3c = xqc_h3_conn_create(conn, NULL);
    if (h3c == NULL) {
        return NULL;
    }

    /* the QPACK encoder stream carries encoder instructions for responses */
    h3c->qenc_stream = xqc_h3_conn_create_uni_stream(h3c,
            XQC_H3_STREAM_TYPE_QPACK_ENCODER);
    if (h3c->qenc_stream == NULL) {
        return NULL;
    }

    xqc_stream_t *stream = xqc_create_stream_with_conn(conn,
            XQC_UNDEFINE_STREAM_ID, XQC_CLI_BID, NULL, NULL);
    if (stream == NULL) {
        return NULL;
    }

    xqc_h3_stream_t *h3s = xqc_h3_stream_create(h3c, stream,
            XQC_H3_STREAM_TYPE_REQUEST, NULL);
    if (h3s == NULL) {
        return NULL;
    }

    h3s->h3r = xqc_h3_request_create_inner(h3c, h3s, NULL);
    if (h3s->h3r == NULL) {
        return NULL;
    }

    *out_conn = conn;
    *out_h3c = h3c;
    return h3s;
}

static void
xqc_wt_req_test_teardown(xqc_h3_stream_t *h3s, xqc_h3_conn_t *h3c,
    xqc_connection_t *conn)
{
    xqc_stream_t *stream = h3s->stream;
    stream->stream_flag |= XQC_STREAM_FLAG_DISCARDED;
    xqc_h3_stream_destroy(h3s);
    xqc_destroy_stream(stream);

    xqc_stream_t *qenc = h3c->qenc_stream->stream;
    qenc->stream_flag |= XQC_STREAM_FLAG_DISCARDED;
    xqc_h3_stream_destroy(h3c->qenc_stream);
    xqc_destroy_stream(qenc);

    xqc_h3_conn_destroy(h3c);
    if (conn->alpn) {
        xqc_free(conn->alpn);
    }
}

/* QPACK field section builder, no Huffman, RIC/Base handled by the frame
 * wrapper below. Static table indexes follow RFC 9204 Appendix A. */
typedef struct {
    uint8_t     buf[256];
    size_t      len;
} xqc_wt_hb_t;

static void
xqc_wt_hb_indexed(xqc_wt_hb_t *hb, uint8_t static_idx)
{
    CU_ASSERT_FATAL(static_idx < 63);
    hb->buf[hb->len++] = (uint8_t)(0xC0 | static_idx);
}

static void
xqc_wt_hb_literal(xqc_wt_hb_t *hb, const char *name, const char *value)
{
    size_t name_len = strlen(name);
    size_t value_len = strlen(value);

    CU_ASSERT_FATAL(hb->len + name_len + value_len + 4 < sizeof(hb->buf));
    CU_ASSERT_FATAL(name_len < 128 && value_len < 127);

    /* Literal Field Line With Literal Name: 001 N H NameLen(3+),
     * RFC 9204 Section 4.5.6, no huffman, never-indexed clear */
    hb->buf[hb->len++] = (uint8_t)(0x20 | (name_len < 7 ? name_len : 7));
    if (name_len >= 7) {
        hb->buf[hb->len++] = (uint8_t)(name_len - 7);
    }
    xqc_memcpy(hb->buf + hb->len, name, name_len);
    hb->len += name_len;

    hb->buf[hb->len++] = (uint8_t)value_len;
    xqc_memcpy(hb->buf + hb->len, value, value_len);
    hb->len += value_len;
}

/* Wrap the section into a HEADERS frame with Required Insert Count 0 and
 * Delta Base 0. Returns the total frame length. */
static size_t
xqc_wt_hb_frame(const xqc_wt_hb_t *hb, uint8_t *out)
{
    uint8_t *pos = out;
    pos = xqc_put_varint(pos, XQC_H3_FRM_HEADERS);
    pos = xqc_put_varint(pos, hb->len + 2);
    *pos++ = 0x00;
    *pos++ = 0x00;
    xqc_memcpy(pos, hb->buf, hb->len);
    return (size_t)(pos - out) + hb->len;
}

static xqc_int_t
xqc_wt_feed_headers(xqc_h3_stream_t *h3s, const uint8_t *frame, size_t len,
    xqc_bool_t fin)
{
    unsigned char buf[512];
    CU_ASSERT_FATAL(len <= sizeof(buf));
    xqc_memcpy(buf, frame, len);
    return xqc_h3_stream_process_in(h3s, buf, len, fin);
}

/* assert that the just-fed section was rejected as malformed
 * (RFC 9114 Section 4.1.2 -> H3_MESSAGE_ERROR stream error) */
static void
xqc_wt_assert_malformed(xqc_h3_stream_t *h3s, xqc_connection_t *conn,
    xqc_int_t ret)
{
    CU_ASSERT(ret == XQC_OK);
    CU_ASSERT(h3s->stream->stream_err == H3_MESSAGE_ERROR);
    CU_ASSERT(conn->conn_err == 0);
    CU_ASSERT((conn->conn_flag & XQC_CONN_FLAG_ERROR) == 0);
    CU_ASSERT(h3s->h3r->wt_flags == 0);
}

void
xqc_test_wt_connect_validation()
{
    uint8_t frame[512];

    /* --- WebTransport CONNECT (happy): CONNECT + webtransport + https +
     * authority + path + origin --- */
    {
        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 1);
        CU_ASSERT_FATAL(h3s != NULL);
        CU_ASSERT(h3c->local_h3_conn_settings.enable_connect_protocol == 1);

        xqc_wt_hb_t hb = {0};
        xqc_wt_hb_indexed(&hb, 15);                     /* :method CONNECT */
        xqc_wt_hb_literal(&hb, ":protocol", "webtransport");
        xqc_wt_hb_indexed(&hb, 23);                     /* :scheme https */
        xqc_wt_hb_literal(&hb, ":authority", "example.com");
        xqc_wt_hb_indexed(&hb, 1);                      /* :path / */
        xqc_wt_hb_literal(&hb, "origin", "https://example.com");

        size_t len = xqc_wt_hb_frame(&hb, frame);
        CU_ASSERT(xqc_wt_feed_headers(h3s, frame, len, XQC_TRUE) == XQC_OK);
        CU_ASSERT(conn->conn_err == 0);
        CU_ASSERT(h3s->h3r->completed_header_count == 1);
        CU_ASSERT(h3s->h3r->wt_flags
                  == (XQC_H3_WT_REQ_FLAG_EXT_CONNECT
                      | XQC_H3_WT_REQ_FLAG_WEBTRANSPORT));

        xqc_wt_req_test_teardown(h3s, h3c, conn);
    }

    /* --- :protocol without CONNECT method (RFC 8441 Section 4) --- */
    {
        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 1);
        CU_ASSERT_FATAL(h3s != NULL);

        xqc_wt_hb_t hb = {0};
        xqc_wt_hb_indexed(&hb, 17);                     /* :method GET */
        xqc_wt_hb_literal(&hb, ":protocol", "webtransport");
        xqc_wt_hb_indexed(&hb, 23);
        xqc_wt_hb_literal(&hb, ":authority", "example.com");
        xqc_wt_hb_indexed(&hb, 1);

        size_t len = xqc_wt_hb_frame(&hb, frame);
        xqc_int_t ret = xqc_wt_feed_headers(h3s, frame, len, XQC_TRUE);
        xqc_wt_assert_malformed(h3s, conn, ret);

        xqc_wt_req_test_teardown(h3s, h3c, conn);
    }

    /* --- WebTransport without :scheme --- */
    {
        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 1);
        CU_ASSERT_FATAL(h3s != NULL);

        xqc_wt_hb_t hb = {0};
        xqc_wt_hb_indexed(&hb, 15);
        xqc_wt_hb_literal(&hb, ":protocol", "webtransport");
        xqc_wt_hb_literal(&hb, ":authority", "example.com");
        xqc_wt_hb_indexed(&hb, 1);

        size_t len = xqc_wt_hb_frame(&hb, frame);
        xqc_int_t ret = xqc_wt_feed_headers(h3s, frame, len, XQC_TRUE);
        xqc_wt_assert_malformed(h3s, conn, ret);

        xqc_wt_req_test_teardown(h3s, h3c, conn);
    }

    /* --- WebTransport with non-https :scheme --- */
    {
        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 1);
        CU_ASSERT_FATAL(h3s != NULL);

        xqc_wt_hb_t hb = {0};
        xqc_wt_hb_indexed(&hb, 15);
        xqc_wt_hb_literal(&hb, ":protocol", "webtransport");
        xqc_wt_hb_indexed(&hb, 22);                     /* :scheme http */
        xqc_wt_hb_literal(&hb, ":authority", "example.com");
        xqc_wt_hb_indexed(&hb, 1);

        size_t len = xqc_wt_hb_frame(&hb, frame);
        xqc_int_t ret = xqc_wt_feed_headers(h3s, frame, len, XQC_TRUE);
        xqc_wt_assert_malformed(h3s, conn, ret);

        xqc_wt_req_test_teardown(h3s, h3c, conn);
    }

    /* --- WebTransport without :path --- */
    {
        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 1);
        CU_ASSERT_FATAL(h3s != NULL);

        xqc_wt_hb_t hb = {0};
        xqc_wt_hb_indexed(&hb, 15);
        xqc_wt_hb_literal(&hb, ":protocol", "webtransport");
        xqc_wt_hb_indexed(&hb, 23);
        xqc_wt_hb_literal(&hb, ":authority", "example.com");

        size_t len = xqc_wt_hb_frame(&hb, frame);
        xqc_int_t ret = xqc_wt_feed_headers(h3s, frame, len, XQC_TRUE);
        xqc_wt_assert_malformed(h3s, conn, ret);

        xqc_wt_req_test_teardown(h3s, h3c, conn);
    }

    /* --- WebTransport without :authority (draft-07 Section 3.3) --- */
    {
        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 1);
        CU_ASSERT_FATAL(h3s != NULL);

        xqc_wt_hb_t hb = {0};
        xqc_wt_hb_indexed(&hb, 15);
        xqc_wt_hb_literal(&hb, ":protocol", "webtransport");
        xqc_wt_hb_indexed(&hb, 23);
        xqc_wt_hb_indexed(&hb, 1);

        size_t len = xqc_wt_hb_frame(&hb, frame);
        xqc_int_t ret = xqc_wt_feed_headers(h3s, frame, len, XQC_TRUE);
        xqc_wt_assert_malformed(h3s, conn, ret);

        xqc_wt_req_test_teardown(h3s, h3c, conn);
    }

    /* --- unknown upgrade token: admitted, marked for 501 (RFC 9220) --- */
    {
        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 1);
        CU_ASSERT_FATAL(h3s != NULL);

        xqc_wt_hb_t hb = {0};
        xqc_wt_hb_indexed(&hb, 15);
        xqc_wt_hb_literal(&hb, ":protocol", "webtransport-h3");
        xqc_wt_hb_indexed(&hb, 23);
        xqc_wt_hb_literal(&hb, ":authority", "example.com");
        xqc_wt_hb_indexed(&hb, 1);

        size_t len = xqc_wt_hb_frame(&hb, frame);
        CU_ASSERT(xqc_wt_feed_headers(h3s, frame, len, XQC_TRUE) == XQC_OK);
        CU_ASSERT(conn->conn_err == 0);
        CU_ASSERT(h3s->h3r->completed_header_count == 1);
        CU_ASSERT(h3s->h3r->wt_flags
                  == (XQC_H3_WT_REQ_FLAG_EXT_CONNECT
                      | XQC_H3_WT_REQ_FLAG_PROTOCOL_UNKNOWN));

        xqc_wt_req_test_teardown(h3s, h3c, conn);
    }

    /* --- :protocol without local SETTINGS_ENABLE_CONNECT_PROTOCOL=1
     * (RFC 8441 Section 3) --- */
    {
        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 0);
        CU_ASSERT_FATAL(h3s != NULL);
        CU_ASSERT(h3c->local_h3_conn_settings.enable_connect_protocol == 0);

        xqc_wt_hb_t hb = {0};
        xqc_wt_hb_indexed(&hb, 15);
        xqc_wt_hb_literal(&hb, ":protocol", "webtransport");
        xqc_wt_hb_indexed(&hb, 23);
        xqc_wt_hb_literal(&hb, ":authority", "example.com");
        xqc_wt_hb_indexed(&hb, 1);

        size_t len = xqc_wt_hb_frame(&hb, frame);
        xqc_int_t ret = xqc_wt_feed_headers(h3s, frame, len, XQC_TRUE);
        xqc_wt_assert_malformed(h3s, conn, ret);

        xqc_wt_req_test_teardown(h3s, h3c, conn);
    }

    /* --- duplicated :protocol (RFC 8441 Section 4: single valued) --- */
    {
        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 1);
        CU_ASSERT_FATAL(h3s != NULL);

        xqc_wt_hb_t hb = {0};
        xqc_wt_hb_indexed(&hb, 15);
        xqc_wt_hb_literal(&hb, ":protocol", "webtransport");
        xqc_wt_hb_literal(&hb, ":protocol", "webtransport");
        xqc_wt_hb_indexed(&hb, 23);
        xqc_wt_hb_literal(&hb, ":authority", "example.com");
        xqc_wt_hb_indexed(&hb, 1);

        size_t len = xqc_wt_hb_frame(&hb, frame);
        xqc_int_t ret = xqc_wt_feed_headers(h3s, frame, len, XQC_TRUE);
        xqc_wt_assert_malformed(h3s, conn, ret);

        xqc_wt_req_test_teardown(h3s, h3c, conn);
    }

    /* --- plain CONNECT without :protocol stays untouched --- */
    {
        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 1);
        CU_ASSERT_FATAL(h3s != NULL);

        xqc_wt_hb_t hb = {0};
        xqc_wt_hb_indexed(&hb, 15);
        xqc_wt_hb_literal(&hb, ":authority", "example.com");
        xqc_wt_hb_indexed(&hb, 1);

        size_t len = xqc_wt_hb_frame(&hb, frame);
        CU_ASSERT(xqc_wt_feed_headers(h3s, frame, len, XQC_TRUE) == XQC_OK);
        CU_ASSERT(conn->conn_err == 0);
        CU_ASSERT(h3s->h3r->completed_header_count == 1);
        CU_ASSERT(h3s->h3r->wt_flags == 0);

        xqc_wt_req_test_teardown(h3s, h3c, conn);
    }

    /* --- :protocol in a trailer section is not extended CONNECT --- */
    {
        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 1);
        CU_ASSERT_FATAL(h3s != NULL);

        xqc_wt_hb_t hb = {0};
        xqc_wt_hb_indexed(&hb, 17);                     /* :method GET */
        xqc_wt_hb_indexed(&hb, 23);
        xqc_wt_hb_indexed(&hb, 1);
        size_t len = xqc_wt_hb_frame(&hb, frame);
        CU_ASSERT(xqc_wt_feed_headers(h3s, frame, len, XQC_FALSE) == XQC_OK);
        CU_ASSERT(h3s->h3r->completed_header_count == 1);

        xqc_wt_hb_t trailer = {0};
        xqc_wt_hb_literal(&trailer, ":protocol", "webtransport");
        len = xqc_wt_hb_frame(&trailer, frame);
        CU_ASSERT(xqc_wt_feed_headers(h3s, frame, len, XQC_TRUE) == XQC_OK);
        CU_ASSERT(conn->conn_err == 0);
        CU_ASSERT(h3s->h3r->completed_header_count == 2);
        CU_ASSERT(h3s->h3r->wt_flags == 0);

        xqc_wt_req_test_teardown(h3s, h3c, conn);
    }
}

/* ------------------------------------------------------------------------
 * E3: session object and public API
 * ------------------------------------------------------------------------ */

typedef struct {
    xqc_wt_session_t   *offer_session;
    void               *offer_conn_user_data;
    int                 offer_count;
    int                 accepted_count;
    void               *accepted_user_data;
    int                 closed_count;
    void               *closed_user_data;
    int                 refuse_offer;

    /* E4: data stream callbacks */
    xqc_wt_stream_t    *opened_stream;
    xqc_wt_session_t   *opened_session;
    int                 opened_stream_count;
    xqc_wt_stream_t    *read_stream;
    void               *read_stream_user_data;
    int                 read_count;
    xqc_wt_stream_t    *closed_stream;
    void               *closed_stream_user_data;
    int                 closed_stream_count;
    xqc_wt_session_t   *dgram_session;
    void               *dgram_user_data;
    int                 dgram_count;
} xqc_wt_cb_capture_t;

static xqc_wt_cb_capture_t xqc_wt_cap;

static void
xqc_wt_cap_reset()
{
    memset(&xqc_wt_cap, 0, sizeof(xqc_wt_cap));
}

static xqc_int_t
xqc_wt_cb_on_request(xqc_wt_session_t *session, void *conn_user_data)
{
    xqc_wt_cap.offer_session = session;
    xqc_wt_cap.offer_conn_user_data = conn_user_data;
    xqc_wt_cap.offer_count++;
    return xqc_wt_cap.refuse_offer ? -1 : XQC_OK;
}

static xqc_int_t
xqc_wt_cb_on_accepted(xqc_wt_session_t *session, void *session_user_data)
{
    xqc_wt_cap.accepted_count++;
    xqc_wt_cap.accepted_user_data = session_user_data;
    return XQC_OK;
}

static xqc_int_t
xqc_wt_cb_on_closed(xqc_wt_session_t *session, void *session_user_data)
{
    xqc_wt_cap.closed_count++;
    xqc_wt_cap.closed_user_data = session_user_data;
    return XQC_OK;
}

static xqc_int_t
xqc_wt_cb_on_stream_opened(xqc_wt_session_t *session, xqc_wt_stream_t *stream,
    void *session_user_data)
{
    xqc_wt_cap.opened_stream = stream;
    xqc_wt_cap.opened_session = session;
    xqc_wt_cap.opened_stream_count++;
    return XQC_OK;
}

static xqc_int_t
xqc_wt_cb_on_stream_read(xqc_wt_stream_t *stream, void *stream_user_data)
{
    xqc_wt_cap.read_stream = stream;
    xqc_wt_cap.read_stream_user_data = stream_user_data;
    xqc_wt_cap.read_count++;
    return XQC_OK;
}

static xqc_int_t
xqc_wt_cb_on_stream_closed(xqc_wt_stream_t *stream, void *stream_user_data)
{
    xqc_wt_cap.closed_stream = stream;
    xqc_wt_cap.closed_stream_user_data = stream_user_data;
    xqc_wt_cap.closed_stream_count++;
    return XQC_OK;
}

static xqc_int_t
xqc_wt_cb_on_datagram(xqc_wt_session_t *session, void *session_user_data)
{
    xqc_wt_cap.dgram_session = session;
    xqc_wt_cap.dgram_user_data = session_user_data;
    xqc_wt_cap.dgram_count++;
    return XQC_OK;
}

static void
xqc_wt_register_cbs(xqc_h3_conn_t *h3c)
{
    xqc_wt_callbacks_t cbs;
    memset(&cbs, 0, sizeof(cbs));
    cbs.session_request_notify = xqc_wt_cb_on_request;
    cbs.session_accepted_notify = xqc_wt_cb_on_accepted;
    cbs.session_closed_notify = xqc_wt_cb_on_closed;
    cbs.stream_opened_notify = xqc_wt_cb_on_stream_opened;
    cbs.stream_read_notify = xqc_wt_cb_on_stream_read;
    cbs.stream_closed_notify = xqc_wt_cb_on_stream_closed;
    cbs.datagram_read_notify = xqc_wt_cb_on_datagram;

    CU_ASSERT(xqc_h3_conn_set_webtransport_callbacks(h3c, &cbs) == XQC_OK);
}

/* client SETTINGS that advertise WebTransport support (draft-07 3.1) */
static void
xqc_wt_make_peer_capable(xqc_h3_conn_t *h3c)
{
    h3c->flags |= XQC_H3_CONN_FLAG_SETTINGS_RECVED;
    h3c->peer_h3_conn_settings.webtransport_max_sessions = 64;
    h3c->peer_h3_conn_settings.h3_datagram = 1;
}

/* feed a valid WebTransport CONNECT request on the request stream */
static xqc_int_t
xqc_wt_feed_wt_connect(xqc_h3_stream_t *h3s, xqc_bool_t fin)
{
    uint8_t frame[512];
    xqc_wt_hb_t hb = {0};

    xqc_wt_hb_indexed(&hb, 15);                     /* :method CONNECT */
    xqc_wt_hb_literal(&hb, ":protocol", "webtransport");
    xqc_wt_hb_indexed(&hb, 23);                     /* :scheme https */
    xqc_wt_hb_literal(&hb, ":authority", "example.com");
    xqc_wt_hb_indexed(&hb, 1);                      /* :path / */
    xqc_wt_hb_literal(&hb, "origin", "https://example.com");

    size_t len = xqc_wt_hb_frame(&hb, frame);
    return xqc_wt_feed_headers(h3s, frame, len, fin);
}

void
xqc_test_wt_session_accept()
{
    /* --- offer -> accept: 2xx response, ESTABLISHED, callbacks --- */
    {
        xqc_wt_cap_reset();

        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 1);
        CU_ASSERT_FATAL(h3s != NULL);
        CU_ASSERT_FATAL(h3c->wt != NULL);

        xqc_wt_register_cbs(h3c);
        xqc_wt_make_peer_capable(h3c);

        int marker = 0x1234;

        CU_ASSERT(xqc_wt_feed_wt_connect(h3s, XQC_TRUE) == XQC_OK);
        CU_ASSERT(conn->conn_err == 0);
        CU_ASSERT(xqc_wt_cap.offer_count == 1);
        CU_ASSERT_FATAL(xqc_wt_cap.offer_session != NULL);
        CU_ASSERT(xqc_wt_cap.offer_conn_user_data == NULL);
        CU_ASSERT(xqc_wt_cap.accepted_count == 0);

        xqc_wt_session_t *session = xqc_wt_cap.offer_session;

        /* offered but not yet established */
        CU_ASSERT(session->state == XQC_WT_SESSION_OFFERED);
        CU_ASSERT(xqc_h3_request_get_wt_session(h3s->h3r) == session);
        CU_ASSERT(xqc_wt_session_get_stream_id(session) == h3s->stream_id);
        CU_ASSERT(xqc_wt_session_get_quic_conn(session) == conn);
        CU_ASSERT(xqc_wt_session_get_user_data(session) == NULL);
        CU_ASSERT(h3s->h3r->wt_flags
                  == (XQC_H3_WT_REQ_FLAG_EXT_CONNECT
                      | XQC_H3_WT_REQ_FLAG_WEBTRANSPORT));

        CU_ASSERT(xqc_wt_session_accept(session, &marker) == XQC_OK);
        CU_ASSERT(session->state == XQC_WT_SESSION_ESTABLISHED);
        CU_ASSERT(xqc_wt_cap.accepted_count == 1);
        CU_ASSERT(xqc_wt_cap.accepted_user_data == &marker);
        CU_ASSERT(xqc_wt_session_get_user_data(session) == &marker);
        /* the 200 response was encoded: ":status" 7 + "200" 3 */
        CU_ASSERT(h3s->h3r->header_sent == 10);

        /* accept/reject on an established session */
        CU_ASSERT(xqc_wt_session_accept(session, &marker) == -XQC_ESTATE);
        CU_ASSERT(xqc_wt_session_reject(session, 403) == -XQC_ESTATE);

        /* no pending close capsule: the writable hook is a no-op */
        CU_ASSERT(xqc_wt_conn_on_request_writable(h3c, h3s->h3r) == XQC_OK);
        CU_ASSERT(xqc_wt_cap.closed_count == 0);

        xqc_wt_req_test_teardown(h3s, h3c, conn);

        /* the CONNECT stream closed: session_closed_notify fires once */
        CU_ASSERT(xqc_wt_cap.closed_count == 1);
        CU_ASSERT(xqc_wt_cap.closed_user_data == &marker);
    }

    /* --- refusing via the request callback return value --- */
    {
        xqc_wt_cap_reset();

        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 1);
        CU_ASSERT_FATAL(h3s != NULL);

        xqc_wt_register_cbs(h3c);
        xqc_wt_make_peer_capable(h3c);
        xqc_wt_cap.refuse_offer = 1;

        CU_ASSERT(xqc_wt_feed_wt_connect(h3s, XQC_TRUE) == XQC_OK);
        CU_ASSERT(xqc_wt_cap.offer_count == 1);
        /* non-zero callback return: refused with 404, closed fired */
        CU_ASSERT(xqc_wt_cap.accepted_count == 0);
        CU_ASSERT(xqc_wt_cap.closed_count == 1);
        CU_ASSERT(xqc_wt_cap.closed_user_data == NULL);
        CU_ASSERT(xqc_wt_cap.offer_session->state == XQC_WT_SESSION_TERMINATED);
        CU_ASSERT(h3s->h3r->header_sent == 10);

        CU_ASSERT(xqc_wt_session_accept(xqc_wt_cap.offer_session, NULL)
                  == -XQC_ESTATE);

        xqc_wt_cap.refuse_offer = 0;
        xqc_wt_req_test_teardown(h3s, h3c, conn);

        /* terminated once, not again at teardown */
        CU_ASSERT(xqc_wt_cap.closed_count == 1);
    }
}

void
xqc_test_wt_session_reject()
{
    xqc_wt_cap_reset();

    xqc_connection_t *conn = NULL;
    xqc_h3_conn_t *h3c = NULL;
    xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 1);
    CU_ASSERT_FATAL(h3s != NULL);

    xqc_wt_register_cbs(h3c);
    xqc_wt_make_peer_capable(h3c);

    CU_ASSERT(xqc_wt_feed_wt_connect(h3s, XQC_TRUE) == XQC_OK);
    CU_ASSERT_FATAL(xqc_wt_cap.offer_session != NULL);
    xqc_wt_session_t *session = xqc_wt_cap.offer_session;

    /* invalid statuses are a parameter error, session untouched */
    CU_ASSERT(xqc_wt_session_reject(session, 99) == -XQC_EPARAM);
    CU_ASSERT(xqc_wt_session_reject(session, 600) == -XQC_EPARAM);
    CU_ASSERT(session->state == XQC_WT_SESSION_OFFERED);
    CU_ASSERT(xqc_wt_cap.closed_count == 0);

    CU_ASSERT(xqc_wt_session_reject(session, 403) == XQC_OK);
    CU_ASSERT(session->state == XQC_WT_SESSION_TERMINATED);
    CU_ASSERT(xqc_wt_cap.closed_count == 1);
    /* never accepted: no session user data in the close notification */
    CU_ASSERT(xqc_wt_cap.closed_user_data == NULL);
    CU_ASSERT(xqc_wt_cap.accepted_count == 0);
    /* the 403 response was encoded */
    CU_ASSERT(h3s->h3r->header_sent == 10);

    /* a terminated session rejects every further operation */
    CU_ASSERT(xqc_wt_session_accept(session, NULL) == -XQC_ESTATE);
    CU_ASSERT(xqc_wt_session_reject(session, 403) == -XQC_ESTATE);

    xqc_wt_req_test_teardown(h3s, h3c, conn);
    CU_ASSERT(xqc_wt_cap.closed_count == 1);
}

void
xqc_test_wt_session_no_callbacks()
{
    /* --- registration guards --- */
    {
        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 0);
        CU_ASSERT_FATAL(h3s != NULL);

        xqc_wt_callbacks_t cbs;
        memset(&cbs, 0, sizeof(cbs));
        cbs.session_request_notify = xqc_wt_cb_on_request;

        /* WebTransport not enabled on this connection */
        CU_ASSERT(xqc_h3_conn_set_webtransport_callbacks(h3c, &cbs)
                  == -XQC_ESTATE);
        CU_ASSERT(xqc_h3_conn_set_webtransport_callbacks(NULL, &cbs)
                  == -XQC_EPARAM);
        CU_ASSERT(xqc_h3_conn_set_webtransport_callbacks(h3c, NULL)
                  == -XQC_EPARAM);

        xqc_wt_req_test_teardown(h3s, h3c, conn);
    }

    /* --- WebTransport enabled but no callbacks: regular request path --- */
    {
        xqc_wt_cap_reset();

        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 1);
        CU_ASSERT_FATAL(h3s != NULL);
        CU_ASSERT_FATAL(h3c->wt != NULL);

        xqc_wt_make_peer_capable(h3c);

        CU_ASSERT(xqc_wt_feed_wt_connect(h3s, XQC_TRUE) == XQC_OK);
        CU_ASSERT(conn->conn_err == 0);
        /* no session was created, the request was not consumed */
        CU_ASSERT(xqc_wt_cap.offer_count == 0);
        CU_ASSERT(h3s->h3r->wt_session == NULL);
        CU_ASSERT(xqc_h3_request_get_wt_session(h3s->h3r) == NULL);
        CU_ASSERT(h3s->h3r->wt_flags
                  == (XQC_H3_WT_REQ_FLAG_EXT_CONNECT
                      | XQC_H3_WT_REQ_FLAG_WEBTRANSPORT));
        CU_ASSERT(h3s->h3r->completed_header_count == 1);
        CU_ASSERT(h3s->h3r->read_flag != 0);
        CU_ASSERT(h3s->stream->stream_err == 0);

        /* NULL-safe getters on an absent session */
        CU_ASSERT(xqc_wt_session_get_user_data(NULL) == NULL);
        CU_ASSERT(xqc_wt_session_get_stream_id(NULL) == 0);
        CU_ASSERT(xqc_wt_session_get_quic_conn(NULL) == NULL);
        CU_ASSERT(xqc_h3_request_get_wt_session(NULL) == NULL);
        CU_ASSERT(xqc_wt_session_accept(NULL, NULL) == -XQC_EPARAM);
        CU_ASSERT(xqc_wt_session_reject(NULL, 403) == -XQC_EPARAM);
        CU_ASSERT(xqc_wt_session_close(NULL, 0, NULL) == -XQC_EPARAM);

        xqc_wt_req_test_teardown(h3s, h3c, conn);
    }
}

void
xqc_test_wt_session_limit()
{
    /* draft-07 Section 3.4: over the local session limit the CONNECT stream
     * is reset with H3_REQUEST_REJECTED and the connection stays open */
    xqc_wt_cap_reset();

    xqc_connection_t *conn = NULL;
    xqc_h3_conn_t *h3c = NULL;
    xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 1);
    CU_ASSERT_FATAL(h3s != NULL);
    CU_ASSERT_FATAL(h3c->wt != NULL);

    xqc_wt_register_cbs(h3c);
    xqc_wt_make_peer_capable(h3c);
    h3c->local_h3_conn_settings.webtransport_max_sessions = 1;

    /* first session: offered and still live */
    CU_ASSERT(xqc_wt_feed_wt_connect(h3s, XQC_TRUE) == XQC_OK);
    CU_ASSERT(xqc_wt_cap.offer_count == 1);
    CU_ASSERT_FATAL(xqc_wt_cap.offer_session != NULL);
    CU_ASSERT(xqc_wt_cap.offer_session->state == XQC_WT_SESSION_OFFERED);

    /* second CONNECT request on the same connection */
    xqc_stream_t *stream2 = xqc_create_stream_with_conn(conn,
            XQC_UNDEFINE_STREAM_ID, XQC_CLI_BID, NULL, NULL);
    CU_ASSERT_FATAL(stream2 != NULL);
    xqc_h3_stream_t *h3s2 = xqc_h3_stream_create(h3c, stream2,
            XQC_H3_STREAM_TYPE_REQUEST, NULL);
    CU_ASSERT_FATAL(h3s2 != NULL);
    h3s2->h3r = xqc_h3_request_create_inner(h3c, h3s2, NULL);
    CU_ASSERT_FATAL(h3s2->h3r != NULL);

    CU_ASSERT(xqc_wt_feed_wt_connect(h3s2, XQC_TRUE) == XQC_OK);
    CU_ASSERT(conn->conn_err == 0);
    /* no second offer reached the application */
    CU_ASSERT(xqc_wt_cap.offer_count == 1);
    CU_ASSERT(xqc_wt_cap.closed_count == 0);
    CU_ASSERT(h3s2->h3r->wt_session == NULL);
    CU_ASSERT(h3s2->h3r->wt_flags
              == (XQC_H3_WT_REQ_FLAG_EXT_CONNECT
                  | XQC_H3_WT_REQ_FLAG_WEBTRANSPORT
                  | XQC_H3_WT_REQ_FLAG_INTERNAL_RESET));
    /* the CONNECT stream was reset, not the connection */
    CU_ASSERT(stream2->stream_err == H3_REQUEST_REJECTED);
    CU_ASSERT(stream2->stream_state_send == XQC_SEND_STREAM_ST_RESET_SENT);
    CU_ASSERT(h3s2->h3r->completed_header_count == 1);

    /* teardown: second stream first, then the shared teardown */
    stream2->stream_flag |= XQC_STREAM_FLAG_DISCARDED;
    xqc_h3_stream_destroy(h3s2);
    xqc_destroy_stream(stream2);
    xqc_wt_req_test_teardown(h3s, h3c, conn);
    CU_ASSERT(xqc_wt_cap.closed_count == 1);
}

void
xqc_test_wt_session_deferred_accept()
{
    /* draft-07 Section 3.1: the server MUST NOT establish sessions before
     * the client SETTINGS have been received */

    /* --- accept before SETTINGS, peer capable later --- */
    {
        xqc_wt_cap_reset();

        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 1);
        CU_ASSERT_FATAL(h3s != NULL);

        xqc_wt_register_cbs(h3c);

        int marker = 0x5678;

        CU_ASSERT(xqc_wt_feed_wt_connect(h3s, XQC_TRUE) == XQC_OK);
        CU_ASSERT_FATAL(xqc_wt_cap.offer_session != NULL);
        xqc_wt_session_t *session = xqc_wt_cap.offer_session;

        CU_ASSERT(xqc_wt_session_accept(session, &marker) == XQC_OK);
        /* deferred: no response, no accepted notification yet */
        CU_ASSERT(session->state == XQC_WT_SESSION_OFFERED);
        CU_ASSERT(session->accept_pending == 1);
        CU_ASSERT(xqc_wt_cap.accepted_count == 0);
        CU_ASSERT(xqc_wt_cap.closed_count == 0);
        CU_ASSERT(h3s->h3r->header_sent == 0);

        /* SETTINGS arrive (production path: xqc_h3_stream SETTINGS case) */
        xqc_wt_make_peer_capable(h3c);
        xqc_wt_conn_on_peer_settings(h3c);

        CU_ASSERT(session->state == XQC_WT_SESSION_ESTABLISHED);
        CU_ASSERT(xqc_wt_cap.accepted_count == 1);
        CU_ASSERT(xqc_wt_cap.accepted_user_data == &marker);
        CU_ASSERT(h3s->h3r->header_sent == 10);

        xqc_wt_req_test_teardown(h3s, h3c, conn);
        CU_ASSERT(xqc_wt_cap.closed_count == 1);
        CU_ASSERT(xqc_wt_cap.closed_user_data == &marker);
    }

    /* --- accept before SETTINGS, peer not capable: 404 on SETTINGS --- */
    {
        xqc_wt_cap_reset();

        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 1);
        CU_ASSERT_FATAL(h3s != NULL);

        xqc_wt_register_cbs(h3c);

        int marker = 0x9abc;

        CU_ASSERT(xqc_wt_feed_wt_connect(h3s, XQC_TRUE) == XQC_OK);
        CU_ASSERT_FATAL(xqc_wt_cap.offer_session != NULL);
        xqc_wt_session_t *session = xqc_wt_cap.offer_session;

        CU_ASSERT(xqc_wt_session_accept(session, &marker) == XQC_OK);
        CU_ASSERT(session->accept_pending == 1);

        /* SETTINGS arrive without WebTransport support (no datagrams) */
        h3c->flags |= XQC_H3_CONN_FLAG_SETTINGS_RECVED;
        h3c->peer_h3_conn_settings.webtransport_max_sessions = 64;
        h3c->peer_h3_conn_settings.h3_datagram = 0;
        xqc_wt_conn_on_peer_settings(h3c);

        CU_ASSERT(session->state == XQC_WT_SESSION_TERMINATED);
        CU_ASSERT(xqc_wt_cap.accepted_count == 0);
        CU_ASSERT(xqc_wt_cap.closed_count == 1);
        CU_ASSERT(xqc_wt_cap.closed_user_data == &marker);
        CU_ASSERT(h3s->h3r->header_sent == 10);

        xqc_wt_req_test_teardown(h3s, h3c, conn);
        CU_ASSERT(xqc_wt_cap.closed_count == 1);
    }

    /* --- accept with SETTINGS already received but peer not capable --- */
    {
        xqc_wt_cap_reset();

        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 1);
        CU_ASSERT_FATAL(h3s != NULL);

        xqc_wt_register_cbs(h3c);

        int marker = 0xdef0;

        CU_ASSERT(xqc_wt_feed_wt_connect(h3s, XQC_TRUE) == XQC_OK);
        CU_ASSERT_FATAL(xqc_wt_cap.offer_session != NULL);
        xqc_wt_session_t *session = xqc_wt_cap.offer_session;

        h3c->flags |= XQC_H3_CONN_FLAG_SETTINGS_RECVED;
        h3c->peer_h3_conn_settings.webtransport_max_sessions = 0;
        h3c->peer_h3_conn_settings.h3_datagram = 1;

        /* no deferral: refused with 404 straight away */
        CU_ASSERT(xqc_wt_session_accept(session, &marker) == XQC_OK);
        CU_ASSERT(session->state == XQC_WT_SESSION_TERMINATED);
        CU_ASSERT(session->accept_pending == 0);
        CU_ASSERT(xqc_wt_cap.accepted_count == 0);
        CU_ASSERT(xqc_wt_cap.closed_count == 1);
        CU_ASSERT(xqc_wt_cap.closed_user_data == &marker);

        xqc_wt_req_test_teardown(h3s, h3c, conn);
        CU_ASSERT(xqc_wt_cap.closed_count == 1);
    }
}

void
xqc_test_wt_session_close()
{
    xqc_wt_cap_reset();

    xqc_connection_t *conn = NULL;
    xqc_h3_conn_t *h3c = NULL;
    xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 1);
    CU_ASSERT_FATAL(h3s != NULL);

    xqc_wt_register_cbs(h3c);
    xqc_wt_make_peer_capable(h3c);

    int marker = 0x77;

    CU_ASSERT(xqc_wt_feed_wt_connect(h3s, XQC_TRUE) == XQC_OK);
    CU_ASSERT_FATAL(xqc_wt_cap.offer_session != NULL);
    xqc_wt_session_t *session = xqc_wt_cap.offer_session;

    /* close() on an offered (not established) session */
    CU_ASSERT(xqc_wt_session_close(session, 0, NULL) == -XQC_ESTATE);

    CU_ASSERT(xqc_wt_session_accept(session, &marker) == XQC_OK);
    CU_ASSERT(session->state == XQC_WT_SESSION_ESTABLISHED);

    /* close: the capsule cannot be sent yet (no 1-RTT), so it is kept for
     * the writable hook; the session is terminated on send (Section 5) */
    CU_ASSERT(xqc_wt_session_close(session, 0x42, "bye") == XQC_OK);
    CU_ASSERT(session->state == XQC_WT_SESSION_TERMINATED);
    CU_ASSERT(xqc_wt_cap.closed_count == 1);
    CU_ASSERT(xqc_wt_cap.closed_user_data == &marker);

    /* CLOSE_WEBTRANSPORT_SESSION capsule: type 0x2843, length 4+3,
     * error code 0x42, "bye" */
    static const unsigned char expected[10] = {
        0x68, 0x43, 0x07, 0x00, 0x00, 0x00, 0x42, 'b', 'y', 'e',
    };
    CU_ASSERT_FATAL(session->close_cap_buf != NULL);
    CU_ASSERT(session->close_cap_len == sizeof(expected));
    CU_ASSERT(memcmp(session->close_cap_buf, expected, sizeof(expected))
              == 0);

    /* a terminated session cannot be closed twice */
    CU_ASSERT(xqc_wt_session_close(session, 0x42, "bye") == -XQC_ESTATE);

    /* the CONNECT stream becomes writable: flush headers and capsule */
    conn->conn_flag |= XQC_CONN_FLAG_CAN_SEND_1RTT;
    conn->conn_flow_ctl.fc_max_data_can_send = 8 * 1024 * 1024;
    h3s->stream->stream_flow_ctl.fc_max_stream_data_can_send = 1024 * 1024;

    CU_ASSERT(xqc_wt_conn_on_request_writable(h3c, h3s->h3r) == XQC_OK);
    CU_ASSERT(session->close_cap_buf == NULL);
    CU_ASSERT(conn->conn_err == 0);
    /* the capsule DATA frame finished the CONNECT stream */
    CU_ASSERT(h3s->flags & XQC_HTTP3_STREAM_FLAG_FIN_SENT);
    CU_ASSERT(h3s->h3r->body_sent == 10);
    CU_ASSERT(h3s->h3r->body_sent_final_size == 10);

    xqc_wt_req_test_teardown(h3s, h3c, conn);
    CU_ASSERT(xqc_wt_cap.closed_count == 1);
}

/* ------------------------------------------------------------------------
 * E4: WebTransport data streams (draft-ietf-webtrans-http3-07 Sections 4.1
 * and 4.2)
 * ------------------------------------------------------------------------ */

typedef struct {
    xqc_connection_t   *conn;
    xqc_h3_conn_t      *h3c;
    xqc_h3_stream_t    *h3s;        /* backing CONNECT stream */
    xqc_wt_session_t   *session;
    int                 marker;
} xqc_wt_stream_env_t;

/* offered session: callbacks registered, capture reset, accept not called */
static xqc_wt_session_t *
xqc_wt_stream_test_setup_offered(xqc_wt_stream_env_t *env)
{
    xqc_wt_cap_reset();
    memset(env, 0, sizeof(*env));
    env->marker = 0x5b;

    env->h3s = xqc_wt_req_test_setup(&env->conn, &env->h3c, 1);
    if (env->h3s == NULL) {
        return NULL;
    }

    xqc_wt_register_cbs(env->h3c);
    xqc_wt_make_peer_capable(env->h3c);

    if (xqc_wt_feed_wt_connect(env->h3s, XQC_TRUE) != XQC_OK) {
        return NULL;
    }

    env->session = xqc_wt_cap.offer_session;
    if (env->session == NULL
        || env->session->state != XQC_WT_SESSION_OFFERED)
    {
        return NULL;
    }

    return env->session;
}

/* established session, callbacks registered, capture reset */
static xqc_wt_session_t *
xqc_wt_stream_test_setup(xqc_wt_stream_env_t *env)
{
    if (xqc_wt_stream_test_setup_offered(env) == NULL) {
        return NULL;
    }

    if (xqc_wt_session_accept(env->session, &env->marker) != XQC_OK
        || env->session->state != XQC_WT_SESSION_ESTABLISHED)
    {
        return NULL;
    }

    return env->session;
}

/* a peer-initiated data stream, as the transport would present it */
static xqc_h3_stream_t *
xqc_wt_open_peer_stream(xqc_connection_t *conn, xqc_h3_conn_t *h3c,
    xqc_bool_t bidi)
{
    xqc_stream_t *stream = xqc_create_stream_with_conn(conn,
            XQC_UNDEFINE_STREAM_ID, bidi ? XQC_CLI_BID : XQC_CLI_UNI,
            NULL, NULL);
    if (stream == NULL) {
        return NULL;
    }

    xqc_h3_stream_t *h3s = xqc_h3_stream_create(h3c, stream,
            XQC_H3_STREAM_TYPE_UNKNOWN, NULL);
    if (h3s == NULL) {
        xqc_destroy_stream(stream);
    }

    return h3s;
}

static xqc_int_t
xqc_wt_feed_stream_data(xqc_h3_stream_t *h3s, const unsigned char *data,
    size_t data_len, xqc_bool_t fin)
{
    unsigned char buf[512];
    CU_ASSERT_FATAL(data_len <= sizeof(buf));
    if (data_len > 0) {
        xqc_memcpy(buf, data, data_len);
    }
    return xqc_h3_stream_process_in(h3s, buf, data_len, fin);
}

static void
xqc_wt_destroy_data_stream(xqc_h3_stream_t *h3s)
{
    xqc_stream_t *stream = h3s->stream;
    stream->stream_flag |= XQC_STREAM_FLAG_DISCARDED;
    xqc_h3_stream_destroy(h3s);
    xqc_destroy_stream(stream);
}

void
xqc_test_wt_stream_uni_recv()
{
    uint8_t  wire[64];
    uint8_t *p;
    size_t   len;

    /* --- uni stream, valid session: DATA, FIN, then an unknown session --- */
    {
        xqc_wt_stream_env_t env;
        CU_ASSERT_FATAL(xqc_wt_stream_test_setup(&env) != NULL);
        xqc_wt_session_t *session = env.session;

        xqc_h3_stream_t *h3s = xqc_wt_open_peer_stream(env.conn, env.h3c,
                                                       XQC_FALSE);
        CU_ASSERT_FATAL(h3s != NULL);

        p = wire;
        p = xqc_put_varint(p, XQC_WT_UNI_STREAM_TYPE);
        p = xqc_put_varint(p, session->stream_id);
        xqc_memcpy(p, "hel", 3);
        p += 3;
        len = (size_t)(p - wire);

        /* first byte only: stream type not identified yet */
        CU_ASSERT(xqc_wt_feed_stream_data(h3s, wire, 1, XQC_FALSE) == XQC_OK);
        CU_ASSERT(h3s->type == XQC_H3_STREAM_TYPE_UNKNOWN);
        CU_ASSERT(h3s->wt_stream == NULL);
        CU_ASSERT(h3s->h3r == NULL);

        /* rest: type 0x54 recognized, prefix parsed, "hel" buffered */
        CU_ASSERT(xqc_wt_feed_stream_data(h3s, wire + 1, len - 1, XQC_FALSE)
                  == XQC_OK);
        CU_ASSERT(env.conn->conn_err == 0);
        CU_ASSERT(h3s->type == XQC_H3_STREAM_TYPE_WT);
        CU_ASSERT(h3s->h3r == NULL);
        CU_ASSERT_FATAL(h3s->wt_stream != NULL);
        CU_ASSERT(h3s->wt_stream->bidi == 0);
        CU_ASSERT(h3s->wt_stream->local == 0);
        CU_ASSERT(h3s->wt_stream->sid_done == 1);
        CU_ASSERT(h3s->wt_stream->session == session);
        CU_ASSERT(h3s->wt_stream->recv_buf_size == 3);
        CU_ASSERT(xqc_wt_cap.opened_stream == h3s->wt_stream);
        CU_ASSERT(xqc_wt_cap.opened_session == session);
        CU_ASSERT(xqc_wt_cap.opened_stream_count == 1);

        unsigned char rbuf[16];
        uint8_t fin = 0;

        CU_ASSERT(xqc_h3_stream_read_notify(h3s->stream, h3s) == XQC_OK);
        CU_ASSERT(xqc_wt_cap.read_count == 1);
        CU_ASSERT(xqc_wt_cap.read_stream == h3s->wt_stream);
        CU_ASSERT(xqc_wt_cap.read_stream_user_data == NULL);
        CU_ASSERT(xqc_wt_stream_recv(h3s->wt_stream, rbuf, sizeof(rbuf), &fin)
                  == 3);
        CU_ASSERT(memcmp(rbuf, "hel", 3) == 0);
        CU_ASSERT(fin == 0);

        /* second chunk */
        CU_ASSERT(xqc_wt_feed_stream_data(h3s, (const uint8_t *)"lo", 2,
                                          XQC_FALSE) == XQC_OK);
        CU_ASSERT(h3s->wt_stream->recv_buf_size == 2);
        CU_ASSERT(xqc_h3_stream_read_notify(h3s->stream, h3s) == XQC_OK);
        CU_ASSERT(xqc_wt_cap.read_count == 2);
        CU_ASSERT(xqc_wt_stream_recv(h3s->wt_stream, rbuf, sizeof(rbuf), &fin)
                  == 2);
        CU_ASSERT(memcmp(rbuf, "lo", 2) == 0);
        CU_ASSERT(fin == 0);
        CU_ASSERT(xqc_wt_stream_recv(h3s->wt_stream, rbuf, sizeof(rbuf), &fin)
                  == -XQC_EAGAIN);

        /* FIN without payload still reaches the WT module (Section 4.1) */
        unsigned char dummy = 0;
        CU_ASSERT(xqc_wt_feed_stream_data(h3s, &dummy, 0, XQC_TRUE) == XQC_OK);
        CU_ASSERT(h3s->wt_stream->recv_fin == 1);
        CU_ASSERT(xqc_h3_stream_read_notify(h3s->stream, h3s) == XQC_OK);
        CU_ASSERT(xqc_wt_cap.read_count == 3);
        CU_ASSERT(xqc_wt_stream_recv(h3s->wt_stream, rbuf, sizeof(rbuf), &fin)
                  == 0);
        CU_ASSERT(fin == 1);
        /* delivered exactly once */
        CU_ASSERT(xqc_h3_stream_read_notify(h3s->stream, h3s) == XQC_OK);
        CU_ASSERT(xqc_wt_cap.read_count == 3);
        CU_ASSERT(xqc_wt_stream_recv(h3s->wt_stream, rbuf, sizeof(rbuf), &fin)
                  == -XQC_EAGAIN);

        /* --- same wire, unknown session: buffered (Section 4.5) --- */
        p = wire;
        p = xqc_put_varint(p, XQC_WT_UNI_STREAM_TYPE);
        p = xqc_put_varint(p, 64);                  /* a valid-shape ID, no session */
        *p++ = 'x';
        len = (size_t)(p - wire);

        xqc_h3_stream_t *h3s2 = xqc_wt_open_peer_stream(env.conn, env.h3c,
                                                        XQC_FALSE);
        CU_ASSERT_FATAL(h3s2 != NULL);

        /* feed the session ID split across two calls */
        CU_ASSERT(xqc_wt_feed_stream_data(h3s2, wire, 3, XQC_FALSE) == XQC_OK);
        CU_ASSERT_FATAL(h3s2->wt_stream != NULL);
        CU_ASSERT(h3s2->wt_stream->sid_done == 0);
        CU_ASSERT(h3s2->wt_stream->session == NULL);

        CU_ASSERT(xqc_wt_feed_stream_data(h3s2, wire + 3, len - 3, XQC_FALSE)
                  == XQC_OK);
        CU_ASSERT(env.conn->conn_err == 0);
        CU_ASSERT(h3s2->wt_stream->sid_done == 1);
        CU_ASSERT(h3s2->wt_stream->session == NULL);
        CU_ASSERT(h3s2->wt_stream->buffered == 1);
        CU_ASSERT(h3s2->wt_stream->buffered_sid == 64);
        CU_ASSERT(env.h3c->wt->buffered_stream_count == 1);
        CU_ASSERT(h3s2->wt_stream->recv_buf_size == 1);
        CU_ASSERT(xqc_wt_cap.opened_stream_count == 1);

        /* a buffered stream is not visible to the application yet */
        CU_ASSERT(xqc_h3_stream_read_notify(h3s2->stream, h3s2) == XQC_OK);
        CU_ASSERT(xqc_wt_cap.read_count == 3);
        CU_ASSERT(xqc_wt_stream_recv(h3s2->wt_stream, rbuf, sizeof(rbuf), &fin)
                  == 1);
        CU_ASSERT(rbuf[0] == 'x');

        /* a FIN is preserved for the eventual bind (not turned into a reset) */
        CU_ASSERT(xqc_wt_feed_stream_data(h3s2, &dummy, 0, XQC_TRUE) == XQC_OK);
        CU_ASSERT(h3s2->wt_stream->recv_fin == 1);
        CU_ASSERT(h3s2->wt_stream->recv_reset == 0);
        CU_ASSERT(xqc_wt_stream_recv(h3s2->wt_stream, rbuf, sizeof(rbuf), &fin)
                  == 0);
        CU_ASSERT(fin == 1);

        /* --- session ID that cannot be a client bidi stream: H3_ID_ERROR --- */
        p = wire;
        p = xqc_put_varint(p, XQC_WT_UNI_STREAM_TYPE);
        p = xqc_put_varint(p, 1);
        *p++ = 'y';
        len = (size_t)(p - wire);

        xqc_h3_stream_t *h3s3 = xqc_wt_open_peer_stream(env.conn, env.h3c,
                                                        XQC_FALSE);
        CU_ASSERT_FATAL(h3s3 != NULL);

        CU_ASSERT(xqc_wt_feed_stream_data(h3s3, wire, len, XQC_FALSE)
                  == -XQC_H3_EPROC_CONTROL);
        CU_ASSERT(XQC_CONN_ERR_CODE(env.conn->conn_err) == H3_ID_ERROR);
        CU_ASSERT(env.conn->conn_flag & XQC_CONN_FLAG_ERROR);

        xqc_wt_destroy_data_stream(h3s);
        xqc_wt_destroy_data_stream(h3s2);
        xqc_wt_destroy_data_stream(h3s3);
        CU_ASSERT(env.h3c->wt->buffered_stream_count == 0);

        xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
        CU_ASSERT(xqc_wt_cap.closed_count == 1);
    }
}

void
xqc_test_wt_stream_bidi()
{
    xqc_wt_stream_env_t env;
    CU_ASSERT_FATAL(xqc_wt_stream_test_setup(&env) != NULL);
    xqc_wt_session_t *session = env.session;

    xqc_h3_stream_t *h3s = xqc_wt_open_peer_stream(env.conn, env.h3c, XQC_TRUE);
    CU_ASSERT_FATAL(h3s != NULL);

    uint8_t wire[64];
    uint8_t *p = wire;
    p = xqc_put_varint(p, XQC_WT_BIDI_SIGNAL_VALUE);
    p = xqc_put_varint(p, session->stream_id);
    xqc_memcpy(p, "hi", 2);
    p += 2;
    size_t len = (size_t)(p - wire);

    /* first byte only: the signal must not create a request object */
    CU_ASSERT(xqc_wt_feed_stream_data(h3s, wire, 1, XQC_FALSE) == XQC_OK);
    CU_ASSERT(h3s->type == XQC_H3_STREAM_TYPE_UNKNOWN);
    CU_ASSERT(h3s->h3r == NULL);
    CU_ASSERT(h3s->wt_stream == NULL);
    CU_ASSERT(env.conn->conn_err == 0);

    /* rest: 0x41 recognized, type switched to WT, prefix parsed, data kept */
    CU_ASSERT(xqc_wt_feed_stream_data(h3s, wire + 1, len - 1, XQC_FALSE)
              == XQC_OK);
    CU_ASSERT(env.conn->conn_err == 0);
    CU_ASSERT(h3s->type == XQC_H3_STREAM_TYPE_WT);
    CU_ASSERT(h3s->h3r == NULL);
    CU_ASSERT_FATAL(h3s->wt_stream != NULL);
    CU_ASSERT(h3s->wt_stream->bidi == 1);
    CU_ASSERT(h3s->wt_stream->local == 0);
    CU_ASSERT(h3s->wt_stream->sid_done == 1);
    CU_ASSERT(h3s->wt_stream->session == session);
    CU_ASSERT(h3s->wt_stream->recv_buf_size == 2);
    CU_ASSERT(xqc_wt_cap.opened_stream == h3s->wt_stream);
    CU_ASSERT(xqc_wt_cap.opened_session == session);
    CU_ASSERT(xqc_wt_cap.opened_stream_count == 1);

    unsigned char rbuf[16];
    uint8_t fin = 0;

    CU_ASSERT(xqc_h3_stream_read_notify(h3s->stream, h3s) == XQC_OK);
    CU_ASSERT(xqc_wt_cap.read_count == 1);
    CU_ASSERT(xqc_wt_stream_recv(h3s->wt_stream, rbuf, sizeof(rbuf), &fin) == 2);
    CU_ASSERT(memcmp(rbuf, "hi", 2) == 0);
    CU_ASSERT(fin == 0);

    /* FIN riding on the last byte */
    CU_ASSERT(xqc_wt_feed_stream_data(h3s, (const uint8_t *)"!", 1, XQC_TRUE)
              == XQC_OK);
    CU_ASSERT(h3s->wt_stream->recv_fin == 1);
    CU_ASSERT(xqc_h3_stream_read_notify(h3s->stream, h3s) == XQC_OK);
    CU_ASSERT(xqc_wt_cap.read_count == 2);
    CU_ASSERT(xqc_wt_stream_recv(h3s->wt_stream, rbuf, sizeof(rbuf), &fin) == 1);
    CU_ASSERT(rbuf[0] == '!');
    CU_ASSERT(fin == 1);
    CU_ASSERT(env.conn->conn_err == 0);

    xqc_wt_destroy_data_stream(h3s);
    xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
    CU_ASSERT(xqc_wt_cap.closed_count == 1);
}

void
xqc_test_wt_stream_bidi_signal_frame_error()
{
    /* the WEBTRANSPORT_STREAM frame as regular request-stream bytes:
     * varint(0x41) type + length 0 */
    uint8_t signal_frame[3] = { 0x40, 0x41, 0x00 };
    uint8_t frame[512];

    /* --- WebTransport enabled: connection error H3_FRAME_ERROR --- */
    {
        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 1);
        CU_ASSERT_FATAL(h3s != NULL);
        CU_ASSERT_FATAL(h3c->wt != NULL);

        CU_ASSERT(xqc_wt_feed_wt_connect(h3s, XQC_TRUE) == XQC_OK);
        CU_ASSERT(conn->conn_err == 0);

        CU_ASSERT(xqc_wt_feed_stream_data(h3s, signal_frame,
                                          sizeof(signal_frame), XQC_FALSE)
                  == -XQC_H3_EPROC_REQUEST);
        CU_ASSERT(XQC_CONN_ERR_CODE(conn->conn_err) == H3_FRAME_ERROR);
        CU_ASSERT(conn->conn_flag & XQC_CONN_FLAG_ERROR);

        xqc_wt_req_test_teardown(h3s, h3c, conn);
    }

    /* --- WebTransport disabled: 0x41 is just an unknown frame --- */
    {
        xqc_connection_t *conn = NULL;
        xqc_h3_conn_t *h3c = NULL;
        xqc_h3_stream_t *h3s = xqc_wt_req_test_setup(&conn, &h3c, 0);
        CU_ASSERT_FATAL(h3s != NULL);
        CU_ASSERT(h3c->wt == NULL);

        xqc_wt_hb_t hb = {0};
        xqc_wt_hb_indexed(&hb, 17);                 /* :method GET */
        xqc_wt_hb_indexed(&hb, 23);                 /* :scheme https */
        xqc_wt_hb_literal(&hb, ":authority", "example.com");
        xqc_wt_hb_indexed(&hb, 1);                  /* :path / */
        size_t len = xqc_wt_hb_frame(&hb, frame);

        CU_ASSERT(xqc_wt_feed_headers(h3s, frame, len, XQC_TRUE) == XQC_OK);
        CU_ASSERT(conn->conn_err == 0);
        CU_ASSERT(h3s->h3r->completed_header_count == 1);

        CU_ASSERT(xqc_wt_feed_stream_data(h3s, signal_frame,
                                          sizeof(signal_frame), XQC_FALSE)
                  == XQC_OK);
        CU_ASSERT(conn->conn_err == 0);
        CU_ASSERT((conn->conn_flag & XQC_CONN_FLAG_ERROR) == 0);

        xqc_wt_req_test_teardown(h3s, h3c, conn);
    }
}

void
xqc_test_wt_stream_local_send()
{
    xqc_wt_stream_env_t env;
    CU_ASSERT_FATAL(xqc_wt_stream_test_setup(&env) != NULL);
    xqc_wt_session_t *session = env.session;

    /* --- create a local bidi stream --- */
    xqc_wt_stream_t *wts = xqc_wt_session_create_stream(session, XQC_TRUE,
                                                        &env.marker);
    CU_ASSERT_FATAL(wts != NULL);
    CU_ASSERT(wts->local == 1);
    CU_ASSERT(wts->bidi == 1);
    CU_ASSERT(wts->session == session);
    CU_ASSERT(wts->h3s != NULL);
    CU_ASSERT(wts->h3s->type == XQC_H3_STREAM_TYPE_WT);
    CU_ASSERT(xqc_wt_stream_get_user_data(wts) == &env.marker);
    CU_ASSERT(xqc_wt_stream_get_session(wts) == session);
    CU_ASSERT(xqc_wt_cap.opened_stream == wts);
    CU_ASSERT(xqc_wt_cap.opened_stream_count == 1);

    /* the "signal + session ID" prefix is pending (no 1-RTT keys yet) */
    uint8_t expected[8];
    uint8_t *ep = expected;
    ep = xqc_put_varint(ep, XQC_WT_BIDI_SIGNAL_VALUE);
    ep = xqc_put_varint(ep, session->stream_id);
    CU_ASSERT_FATAL(wts->send_buf != NULL);
    CU_ASSERT(wts->send_buf->data_len == (size_t)(ep - expected));
    CU_ASSERT(wts->send_buf->consumed_len == 0);
    CU_ASSERT(memcmp(wts->send_buf->data, expected, (size_t)(ep - expected))
              == 0);
    CU_ASSERT(wts->send_buf->data[0] == 0x40);
    CU_ASSERT(wts->send_buf->data[1] == 0x41);

    /* --- the uni variant carries the 0x54 stream type --- */
    xqc_wt_stream_t *wts_uni = xqc_wt_session_create_stream(session, XQC_FALSE,
                                                            NULL);
    CU_ASSERT_FATAL(wts_uni != NULL);
    CU_ASSERT(wts_uni->local == 1);
    CU_ASSERT(wts_uni->bidi == 0);
    CU_ASSERT(xqc_wt_cap.opened_stream_count == 2);

    ep = expected;
    ep = xqc_put_varint(ep, XQC_WT_UNI_STREAM_TYPE);
    ep = xqc_put_varint(ep, session->stream_id);
    CU_ASSERT_FATAL(wts_uni->send_buf != NULL);
    CU_ASSERT(wts_uni->send_buf->data_len == (size_t)(ep - expected));
    CU_ASSERT(memcmp(wts_uni->send_buf->data, expected, (size_t)(ep - expected))
              == 0);
    CU_ASSERT(wts_uni->send_buf->data[0] == 0x40);
    CU_ASSERT(wts_uni->send_buf->data[1] == 0x54);

    /* --- writable hook flushes the pending prefixes --- */
    /*
     * The unit-test connection never completed a handshake, so it holds no
     * 1-RTT keys: should engine logic try to encrypt one of the packets this
     * test produces, xqc_packet_encrypt() would fail and force the connection
     * to CLOSED, breaking every later send. Park the engine flag so the
     * internal xqc_engine_conn_logic() calls are no-ops; frames are then
     * verified where they are queued, which is what this test cares about.
     */
    env.conn->engine->eng_flag |= XQC_ENG_FLAG_RUNNING;
    env.conn->conn_flag |= XQC_CONN_FLAG_CAN_SEND_1RTT;
    env.conn->conn_flow_ctl.fc_max_data_can_send = 8 * 1024 * 1024;
    wts->h3s->stream->stream_flow_ctl.fc_max_stream_data_can_send = 1024 * 1024;
    wts_uni->h3s->stream->stream_flow_ctl.fc_max_stream_data_can_send
        = 1024 * 1024;

    CU_ASSERT(xqc_h3_stream_write_notify(wts->h3s->stream, wts->h3s)
              == XQC_OK);
    CU_ASSERT(wts->send_buf->data_len == 0);
    CU_ASSERT(xqc_h3_stream_write_notify(wts_uni->h3s->stream, wts_uni->h3s)
              == XQC_OK);
    CU_ASSERT(wts_uni->send_buf->data_len == 0);

    /* --- payload with FIN --- */
    CU_ASSERT(xqc_wt_stream_send(wts, (const uint8_t *)"hello", 5, 1) == 5);
    CU_ASSERT(wts->fin_requested == 1);
    CU_ASSERT(wts->fin_sent == 1);
    CU_ASSERT(wts->send_buf->data_len == 0);
    /* no more writes after the FIN */
    CU_ASSERT(xqc_wt_stream_send(wts, (const uint8_t *)"x", 1, 0)
              == -XQC_ESTATE);

    unsigned char rbuf[8];
    uint8_t fin = 0;
    CU_ASSERT(xqc_wt_stream_recv(wts, rbuf, sizeof(rbuf), &fin) == -XQC_EAGAIN);
    CU_ASSERT(env.conn->conn_err == 0);

    /* --- closing the session resets its streams (Section 5) --- */
    env.h3s->stream->stream_flow_ctl.fc_max_stream_data_can_send = 1024 * 1024;
    CU_ASSERT(xqc_wt_session_close(session, 0x42, "bye") == XQC_OK);
    CU_ASSERT(session->state == XQC_WT_SESSION_TERMINATED);
    CU_ASSERT(xqc_wt_cap.closed_count == 1);
    CU_ASSERT(xqc_wt_cap.closed_user_data == &env.marker);
    /* with 1-RTT keys available the capsule went out immediately */
    CU_ASSERT(session->close_cap_buf == NULL);

    CU_ASSERT(wts->closed == 1);
    CU_ASSERT(wts->h3s->stream->stream_err == XQC_WT_SESSION_GONE);
    CU_ASSERT(wts->h3s->stream->stream_state_send
              == XQC_SEND_STREAM_ST_RESET_SENT);
    CU_ASSERT(wts->h3s->stream->stream_flag
              & XQC_STREAM_FLAG_STOP_SENDING_SENT);

    CU_ASSERT(wts_uni->closed == 1);
    CU_ASSERT(wts_uni->h3s->stream->stream_err == XQC_WT_SESSION_GONE);
    CU_ASSERT(wts_uni->h3s->stream->stream_state_send
              == XQC_SEND_STREAM_ST_RESET_SENT);
    CU_ASSERT(!(wts_uni->h3s->stream->stream_flag
                & XQC_STREAM_FLAG_STOP_SENDING_SENT));

    CU_ASSERT(xqc_wt_stream_recv(wts, rbuf, sizeof(rbuf), &fin) == -XQC_ESTATE);
    CU_ASSERT(xqc_wt_stream_send(wts, (const uint8_t *)"x", 1, 0)
              == -XQC_ESTATE);

    /* --- destroy the streams; the session frees the objects --- */
    xqc_h3_stream_t *h3s_bidi = wts->h3s;
    xqc_h3_stream_t *h3s_uni = wts_uni->h3s;
    xqc_wt_destroy_data_stream(h3s_bidi);
    xqc_wt_destroy_data_stream(h3s_uni);

    /* each announced stream fires stream_closed_notify exactly once when its
     * transport stream goes away; the last one fired for the uni stream */
    CU_ASSERT(xqc_wt_cap.closed_stream_count == 2);
    CU_ASSERT(xqc_wt_cap.closed_stream == wts_uni);
    CU_ASSERT(xqc_wt_cap.closed_stream_user_data == NULL);

    xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
    CU_ASSERT(xqc_wt_cap.closed_stream_count == 2);
    CU_ASSERT(xqc_wt_cap.closed_count == 1);
}

/* ------------------------------------------------------------------------
 * E5: HTTP datagrams (RFC 9297 Sections 2/2.1) and the capsule protocol
 * (RFC 9297 Section 3, draft-ietf-webtrans-http3-07 Sections 4.4/4.6/5)
 * ------------------------------------------------------------------------ */

/* wrap payload bytes into an H3 DATA frame */
static size_t
xqc_wt_data_frame(const unsigned char *payload, size_t payload_len, uint8_t *out)
{
    uint8_t *pos = out;
    pos = xqc_put_varint(pos, XQC_H3_FRM_DATA);
    pos = xqc_put_varint(pos, payload_len);
    if (payload_len > 0) {
        xqc_memcpy(pos, payload, payload_len);
    }
    return (size_t)(pos - out) + payload_len;
}

/* build one capsule (type, length, payload) */
static size_t
xqc_wt_make_capsule(uint64_t type, const unsigned char *payload,
    size_t payload_len, uint8_t *out)
{
    uint8_t *pos = out;
    pos = xqc_put_varint(pos, type);
    pos = xqc_put_varint(pos, payload_len);
    if (payload_len > 0) {
        xqc_memcpy(pos, payload, payload_len);
    }
    return (size_t)(pos - out) + payload_len;
}

/*
 * The test setup opens the CONNECT stream with the undefined stream id; give
 * the session a real client-initiated bidi id (multiple of 4) so datagrams,
 * which address the CONNECT stream by quarter stream id (RFC 9297 2.1),
 * can be mapped back to it
 */
static void
xqc_wt_set_session_stream_id(xqc_h3_stream_t *h3s, xqc_wt_session_t *session)
{
    h3s->stream_id = 4;
    session->stream_id = 4;
}

void
xqc_test_wt_datagram()
{
    uint8_t dg[64];
    uint8_t *p;

    /* --- receive: queue, notify, recv; ENOBUF; empty payload; inactive and
     * unknown sessions --- */
    {
        xqc_wt_stream_env_t env;
        CU_ASSERT_FATAL(xqc_wt_stream_test_setup(&env) != NULL);
        xqc_wt_session_t *session = env.session;
        xqc_wt_set_session_stream_id(env.h3s, session);

        /* both endpoints advertised SETTINGS_H3_DATAGRAM = 1 (2.1.1) */
        CU_ASSERT(env.h3c->local_h3_conn_settings.h3_datagram == 1);
        CU_ASSERT(env.h3c->flags & XQC_H3_CONN_FLAG_SETTINGS_RECVED);
        CU_ASSERT(env.h3c->peer_h3_conn_settings.h3_datagram == 1);

        /* quarter stream id 1 (= 4/4) + payload */
        p = dg;
        p = xqc_put_varint(p, 1);
        xqc_memcpy(p, "ping", 4);
        p += 4;
        CU_ASSERT(xqc_wt_conn_on_datagram(env.h3c, dg, (size_t)(p - dg))
                  == XQC_OK);
        CU_ASSERT(xqc_wt_cap.dgram_count == 1);
        CU_ASSERT(xqc_wt_cap.dgram_session == session);
        CU_ASSERT(xqc_wt_cap.dgram_user_data == &env.marker);
        CU_ASSERT(session->dgram_recv_count == 1);

        unsigned char buf[16];
        CU_ASSERT(xqc_wt_session_recv_datagram(session, buf, sizeof(buf)) == 4);
        CU_ASSERT(memcmp(buf, "ping", 4) == 0);
        CU_ASSERT(session->dgram_recv_count == 0);
        CU_ASSERT(xqc_wt_session_recv_datagram(session, buf, sizeof(buf))
                  == -XQC_EAGAIN);

        /* too small buffer: the payload stays queued for a retry */
        p = dg;
        p = xqc_put_varint(p, 1);
        xqc_memcpy(p, "world", 5);
        p += 5;
        CU_ASSERT(xqc_wt_conn_on_datagram(env.h3c, dg, (size_t)(p - dg))
                  == XQC_OK);
        CU_ASSERT(xqc_wt_session_recv_datagram(session, buf, 2)
                  == -XQC_ENOBUF);
        CU_ASSERT(session->dgram_recv_count == 1);
        CU_ASSERT(xqc_wt_session_recv_datagram(session, buf, sizeof(buf)) == 5);
        CU_ASSERT(memcmp(buf, "world", 5) == 0);

        /* empty payload: reads back as 0 bytes */
        p = dg;
        p = xqc_put_varint(p, 1);
        CU_ASSERT(xqc_wt_conn_on_datagram(env.h3c, dg, (size_t)(p - dg))
                  == XQC_OK);
        CU_ASSERT(xqc_wt_cap.dgram_count == 3);
        CU_ASSERT(xqc_wt_session_recv_datagram(session, buf, sizeof(buf)) == 0);

        /* unknown quarter stream id, no generic datagram callbacks:
           silently dropped (Section 2.1) */
        p = dg;
        p = xqc_put_varint(p, 2);
        p = xqc_put_varint(p, 9);
        CU_ASSERT(xqc_wt_conn_on_datagram(env.h3c, dg, (size_t)(p - dg))
                  == XQC_OK);
        CU_ASSERT(xqc_wt_cap.dgram_count == 3);
        CU_ASSERT(env.conn->conn_err == 0);

        /* a session that is still OFFERED: the datagram is buffered until
           the session is established (draft-07 Section 4.5; the flush on
           accept is asserted in xqc_test_wt_buffered_limit) */
        session->state = XQC_WT_SESSION_OFFERED;
        p = dg;
        p = xqc_put_varint(p, 1);
        p = xqc_put_varint(p, 7);
        CU_ASSERT(xqc_wt_conn_on_datagram(env.h3c, dg, (size_t)(p - dg))
                  == XQC_OK);
        CU_ASSERT(xqc_wt_cap.dgram_count == 3);
        CU_ASSERT(session->dgram_recv_count == 0);
        CU_ASSERT(env.h3c->wt->buffered_dgram_count == 1);

        /* back to ESTABLISHED; the teardown below drops the datagram that
           is still buffered for this session */
        session->state = XQC_WT_SESSION_ESTABLISHED;

        xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
    }

    /* --- malformed datagrams: too short / quarter id > 2^60-1 --- */
    {
        xqc_wt_stream_env_t env;
        CU_ASSERT_FATAL(xqc_wt_stream_test_setup(&env) != NULL);
        xqc_wt_session_t *session = env.session;
        xqc_wt_set_session_stream_id(env.h3s, session);

        /* no quarter stream id at all */
        p = dg;
        p = xqc_put_varint(p, 1);
        CU_ASSERT(xqc_wt_conn_on_datagram(env.h3c, dg, 0) == XQC_OK);
        CU_ASSERT(XQC_CONN_ERR_CODE(env.conn->conn_err) == H3_DATAGRAM_ERROR);
        CU_ASSERT(env.conn->conn_flag & XQC_CONN_FLAG_ERROR);
        CU_ASSERT(xqc_wt_cap.dgram_count == 0);

        xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
    }
    {
        xqc_wt_stream_env_t env;
        CU_ASSERT_FATAL(xqc_wt_stream_test_setup(&env) != NULL);
        xqc_wt_session_t *session = env.session;
        xqc_wt_set_session_stream_id(env.h3s, session);

        /* quarter stream id above 2^60-1 */
        p = dg;
        p = xqc_put_varint(p, 1ULL << 60);
        CU_ASSERT(xqc_wt_conn_on_datagram(env.h3c, dg, (size_t)(p - dg))
                  == XQC_OK);
        CU_ASSERT(XQC_CONN_ERR_CODE(env.conn->conn_err) == H3_DATAGRAM_ERROR);
        CU_ASSERT(xqc_wt_cap.dgram_count == 0);

        xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
    }

    /* --- datagram support not negotiated: the WT layer declines --- */
    {
        xqc_wt_stream_env_t env;
        CU_ASSERT_FATAL(xqc_wt_stream_test_setup(&env) != NULL);
        xqc_wt_session_t *session = env.session;
        xqc_wt_set_session_stream_id(env.h3s, session);

        p = dg;
        p = xqc_put_varint(p, 1);
        xqc_memcpy(p, "x", 1);
        p += 1;

        env.h3c->peer_h3_conn_settings.h3_datagram = 0;
        CU_ASSERT(xqc_wt_conn_on_datagram(env.h3c, dg, (size_t)(p - dg))
                  == XQC_ERROR);
        CU_ASSERT(xqc_wt_cap.dgram_count == 0);

        env.h3c->peer_h3_conn_settings.h3_datagram = 1;
        env.h3c->flags &= ~XQC_H3_CONN_FLAG_SETTINGS_RECVED;
        CU_ASSERT(xqc_wt_conn_on_datagram(env.h3c, dg, (size_t)(p - dg))
                  == XQC_ERROR);
        CU_ASSERT(xqc_wt_cap.dgram_count == 0);
        CU_ASSERT(env.conn->conn_err == 0);

        xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
    }

    /* --- send: gating and the quarter stream id prefix --- */
    {
        xqc_wt_stream_env_t env;
        CU_ASSERT_FATAL(xqc_wt_stream_test_setup(&env) != NULL);
        xqc_wt_session_t *session = env.session;
        xqc_wt_set_session_stream_id(env.h3s, session);

        const uint8_t payload[4] = {1, 2, 3, 4};

        /* peer SETTINGS_H3_DATAGRAM = 0: not allowed to send */
        env.h3c->peer_h3_conn_settings.h3_datagram = 0;
        CU_ASSERT(xqc_wt_session_send_datagram(session, payload, 4)
                  == -XQC_ESTATE);
        env.h3c->peer_h3_conn_settings.h3_datagram = 1;

        /* no 1-RTT keys yet: the transport waits */
        CU_ASSERT(xqc_wt_session_send_datagram(session, payload, 4)
                  == -XQC_EAGAIN);

        /* 1-RTT available: the datagram goes out (quarter stream id 1) */
        env.conn->remote_settings.max_datagram_frame_size = 1200;
        env.conn->dgram_mss = 1100;
        env.conn->engine->eng_flag |= XQC_ENG_FLAG_RUNNING;
        env.conn->conn_flag |= XQC_CONN_FLAG_CAN_SEND_1RTT;
        CU_ASSERT(xqc_wt_session_send_datagram(session, payload, 4) == XQC_OK);

        /* larger than the negotiation permits */
        unsigned char big[1101];
        memset(big, 0x5a, sizeof(big));
        CU_ASSERT(xqc_wt_session_send_datagram(session, big, sizeof(big))
                  == -XQC_EDGRAM_TOO_LARGE);

        /* NULL/EPARAM guard */
        CU_ASSERT(xqc_wt_session_send_datagram(session, NULL, 4)
                  == -XQC_EPARAM);

        xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
    }
}

void
xqc_test_wt_capsule()
{
    uint8_t cap[64];
    uint8_t wire[128];
    size_t  cap_len;
    size_t  frame_len;

    /* --- DRAIN and unknown capsules are parsed and skipped --- */
    {
        xqc_wt_stream_env_t env;
        CU_ASSERT_FATAL(xqc_wt_stream_test_setup(&env) != NULL);
        xqc_wt_session_t *session = env.session;

        /* an empty DRAIN_WEBTRANSPORT_SESSION capsule (Section 4.6) */
        cap_len = xqc_wt_make_capsule(
            XQC_WT_CAPSULE_DRAIN_WEBTRANSPORT_SESSION, NULL, 0, cap);
        CU_ASSERT(cap_len == 5);
        frame_len = xqc_wt_data_frame(cap, cap_len, wire);
        CU_ASSERT(xqc_wt_feed_stream_data(env.h3s, wire, frame_len, XQC_FALSE)
                  == XQC_OK);
        CU_ASSERT(session->state == XQC_WT_SESSION_ESTABLISHED);
        CU_ASSERT(env.conn->conn_err == 0);

        /* an unknown capsule is skipped, the following DRAIN is still
           parsed (Section 3.2) */
        cap_len = xqc_wt_make_capsule(0x0f, (const unsigned char *)"abc", 3,
                                      cap);
        cap_len += xqc_wt_make_capsule(
            XQC_WT_CAPSULE_DRAIN_WEBTRANSPORT_SESSION, NULL, 0, cap + cap_len);
        frame_len = xqc_wt_data_frame(cap, cap_len, wire);
        CU_ASSERT(xqc_wt_feed_stream_data(env.h3s, wire, frame_len, XQC_FALSE)
                  == XQC_OK);
        CU_ASSERT(session->state == XQC_WT_SESSION_ESTABLISHED);
        CU_ASSERT(session->close_received == 0);
        CU_ASSERT(env.conn->conn_err == 0);

        xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
    }

    /* --- CLOSE_WEBTRANSPORT_SESSION terminates the session, the FIN answer
     * pends without 1-RTT; later data aborts the stream --- */
    {
        xqc_wt_stream_env_t env;
        CU_ASSERT_FATAL(xqc_wt_stream_test_setup(&env) != NULL);
        xqc_wt_session_t *session = env.session;

        const unsigned char payload[7] = {0x00, 0x00, 0x00, 0x42, 'b', 'y', 'e'};
        cap_len = xqc_wt_make_capsule(
            XQC_WT_CAPSULE_CLOSE_WEBTRANSPORT_SESSION, payload, 7, cap);
        frame_len = xqc_wt_data_frame(cap, cap_len, wire);

        CU_ASSERT(xqc_wt_feed_stream_data(env.h3s, wire, frame_len, XQC_FALSE)
                  == XQC_OK);
        CU_ASSERT(session->state == XQC_WT_SESSION_TERMINATED);
        CU_ASSERT(session->close_received == 1);
        CU_ASSERT(xqc_wt_cap.closed_count == 1);
        CU_ASSERT(xqc_wt_cap.closed_user_data == &env.marker);
        CU_ASSERT(env.conn->conn_err == 0);
        /* the answering FIN is not sent yet (no 1-RTT keys) */
        CU_ASSERT(!(env.h3s->flags & XQC_HTTP3_STREAM_FLAG_FIN_SENT));

        /* any stream data after the close capsule resets the CONNECT stream
           with H3_MESSAGE_ERROR (draft-07 Section 5) */
        uint8_t junk[32];
        size_t junk_len = xqc_wt_data_frame((const unsigned char *)"x", 1,
                                            junk);
        CU_ASSERT(xqc_wt_feed_stream_data(env.h3s, junk, junk_len, XQC_FALSE)
                  == XQC_OK);
        CU_ASSERT(env.h3s->stream->stream_err == H3_MESSAGE_ERROR);
        CU_ASSERT(env.conn->conn_err == 0);

        xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
    }

    /* --- with 1-RTT available the answering FIN goes out --- */
    {
        xqc_wt_stream_env_t env;
        CU_ASSERT_FATAL(xqc_wt_stream_test_setup(&env) != NULL);
        xqc_wt_session_t *session = env.session;

        const unsigned char payload[4] = {0x00, 0x00, 0x00, 0x01};
        cap_len = xqc_wt_make_capsule(
            XQC_WT_CAPSULE_CLOSE_WEBTRANSPORT_SESSION, payload, 4, cap);
        frame_len = xqc_wt_data_frame(cap, cap_len, wire);

        env.conn->engine->eng_flag |= XQC_ENG_FLAG_RUNNING;
        env.conn->conn_flag |= XQC_CONN_FLAG_CAN_SEND_1RTT;
        env.conn->conn_flow_ctl.fc_max_data_can_send = 8 * 1024 * 1024;
        env.h3s->stream->stream_flow_ctl.fc_max_stream_data_can_send
            = 1024 * 1024;

        CU_ASSERT(xqc_wt_feed_stream_data(env.h3s, wire, frame_len, XQC_FALSE)
                  == XQC_OK);
        CU_ASSERT(session->state == XQC_WT_SESSION_TERMINATED);
        CU_ASSERT(env.h3s->flags & XQC_HTTP3_STREAM_FLAG_FIN_SENT);
        CU_ASSERT(env.conn->conn_err == 0);

        /* a terminated session no longer sends datagrams */
        uint8_t dg[16];
        uint8_t *p = dg;
        p = xqc_put_varint(p, 1);
        p = xqc_put_varint(p, 2);
        CU_ASSERT(xqc_wt_session_send_datagram(session, dg, (size_t)(p - dg))
                  == -XQC_ESTATE);

        xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
    }

    /* --- a close capsule split across two DATA frames --- */
    {
        xqc_wt_stream_env_t env;
        CU_ASSERT_FATAL(xqc_wt_stream_test_setup(&env) != NULL);
        xqc_wt_session_t *session = env.session;

        const unsigned char payload[7] = {0x00, 0x00, 0x00, 0x01, 'o', 'k', '!'};
        cap_len = xqc_wt_make_capsule(
            XQC_WT_CAPSULE_CLOSE_WEBTRANSPORT_SESSION, payload, 7, cap);

        /* first DATA frame: capsule header plus the first code byte */
        frame_len = xqc_wt_data_frame(cap, 4, wire);
        CU_ASSERT(xqc_wt_feed_stream_data(env.h3s, wire, frame_len, XQC_FALSE)
                  == XQC_OK);
        CU_ASSERT(session->state == XQC_WT_SESSION_ESTABLISHED);
        CU_ASSERT(session->close_received == 0);
        CU_ASSERT(xqc_wt_cap.closed_count == 0);

        /* second DATA frame: the rest of the capsule completes it */
        frame_len = xqc_wt_data_frame(cap + 4, cap_len - 4, wire);
        CU_ASSERT(xqc_wt_feed_stream_data(env.h3s, wire, frame_len, XQC_FALSE)
                  == XQC_OK);
        CU_ASSERT(session->state == XQC_WT_SESSION_TERMINATED);
        CU_ASSERT(session->close_received == 1);
        CU_ASSERT(xqc_wt_cap.closed_count == 1);
        CU_ASSERT(env.conn->conn_err == 0);

        xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
    }

    /* --- a close capsule shorter than its error code is malformed --- */
    {
        xqc_wt_stream_env_t env;
        CU_ASSERT_FATAL(xqc_wt_stream_test_setup(&env) != NULL);
        xqc_wt_session_t *session = env.session;

        const unsigned char payload[2] = {0x00, 0x01};
        cap_len = xqc_wt_make_capsule(
            XQC_WT_CAPSULE_CLOSE_WEBTRANSPORT_SESSION, payload, 2, cap);
        frame_len = xqc_wt_data_frame(cap, cap_len, wire);

        CU_ASSERT(xqc_wt_feed_stream_data(env.h3s, wire, frame_len, XQC_FALSE)
                  == XQC_OK);
        CU_ASSERT(env.h3s->stream->stream_err == H3_MESSAGE_ERROR);
        CU_ASSERT(env.conn->conn_err == 0);
        CU_ASSERT(session->state == XQC_WT_SESSION_ESTABLISHED);

        xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
    }

    /* --- a clean FIN with a truncated trailing capsule is malformed
     * (Section 3.3) --- */
    {
        xqc_wt_stream_env_t env;
        CU_ASSERT_FATAL(xqc_wt_stream_test_setup(&env) != NULL);
        xqc_wt_session_t *session = env.session;
        uint8_t *q;

        /* capsule header promises 10 bytes, 1 byte arrives, then FIN */
        q = cap;
        q = xqc_put_varint(q, XQC_WT_CAPSULE_CLOSE_WEBTRANSPORT_SESSION);
        q = xqc_put_varint(q, 10);
        *q++ = 0x00;
        frame_len = xqc_wt_data_frame(cap, (size_t)(q - cap), wire);
        CU_ASSERT(xqc_wt_feed_stream_data(env.h3s, wire, frame_len, XQC_TRUE)
                  == XQC_OK);
        CU_ASSERT(env.h3s->stream->stream_err == H3_MESSAGE_ERROR);
        CU_ASSERT(env.conn->conn_err == 0);
        CU_ASSERT(session->state == XQC_WT_SESSION_ESTABLISHED);

        xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
    }
    {
        xqc_wt_stream_env_t env;
        CU_ASSERT_FATAL(xqc_wt_stream_test_setup(&env) != NULL);
        xqc_wt_session_t *session = env.session;
        uint8_t *q;

        /* FIN in the middle of the capsule length field */
        q = cap;
        q = xqc_put_varint(q, XQC_WT_CAPSULE_CLOSE_WEBTRANSPORT_SESSION);
        frame_len = xqc_wt_data_frame(cap, (size_t)(q - cap), wire);
        CU_ASSERT(xqc_wt_feed_stream_data(env.h3s, wire, frame_len, XQC_TRUE)
                  == XQC_OK);
        CU_ASSERT(env.h3s->stream->stream_err == H3_MESSAGE_ERROR);
        CU_ASSERT(env.conn->conn_err == 0);
        CU_ASSERT(session->state == XQC_WT_SESSION_ESTABLISHED);

        xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
    }
}

/* ------------------------------------------------------------------------
 * E6: buffering before session establishment and the per-connection caps
 * (draft-ietf-webtrans-http3-07 Section 4.5)
 * ------------------------------------------------------------------------ */

void
xqc_test_wt_buffered_limit()
{
    uint8_t  wire[64];
    uint8_t *p;
    size_t   len;
    int      i;

    /* the caps are fixed and small (design decision, locked as literals) */
    CU_ASSERT(XQC_WT_BUFFERED_STREAM_MAX == 4);
    CU_ASSERT(XQC_WT_BUFFERED_DGRAM_MAX == 16);
    CU_ASSERT(XQC_WT_BUFFERED_STREAM_REJECTED == 0x3994bd84);

    /* --- a stream that arrives before the session is established waits in
     * the per-connection buffer and is attached by accept (Section 4.5) --- */
    {
        xqc_wt_stream_env_t env;
        CU_ASSERT_FATAL(xqc_wt_stream_test_setup_offered(&env) != NULL);
        xqc_wt_session_t *session = env.session;
        xqc_wt_set_session_stream_id(env.h3s, session);

        xqc_h3_stream_t *h3s = xqc_wt_open_peer_stream(env.conn, env.h3c,
                                                       XQC_FALSE);
        CU_ASSERT_FATAL(h3s != NULL);

        p = wire;
        p = xqc_put_varint(p, XQC_WT_UNI_STREAM_TYPE);
        p = xqc_put_varint(p, session->stream_id);
        xqc_memcpy(p, "hi", 2);
        p += 2;
        len = (size_t)(p - wire);

        CU_ASSERT(xqc_wt_feed_stream_data(h3s, wire, len, XQC_FALSE)
                  == XQC_OK);
        CU_ASSERT_FATAL(h3s->wt_stream != NULL);
        CU_ASSERT(h3s->wt_stream->buffered == 1);
        CU_ASSERT(h3s->wt_stream->buffered_sid == session->stream_id);
        CU_ASSERT(h3s->wt_stream->session == NULL);
        CU_ASSERT(h3s->wt_stream->recv_buf_size == 2);
        CU_ASSERT(env.h3c->wt->buffered_stream_count == 1);

        /* a buffered stream is not visible to the application yet */
        CU_ASSERT(xqc_wt_cap.opened_stream_count == 0);
        CU_ASSERT(xqc_h3_stream_read_notify(h3s->stream, h3s) == XQC_OK);
        CU_ASSERT(xqc_wt_cap.read_count == 0);

        /* more bytes and a FIN arrive while buffering */
        CU_ASSERT(xqc_wt_feed_stream_data(h3s, (const uint8_t *)"!", 1,
                                          XQC_FALSE) == XQC_OK);
        CU_ASSERT(h3s->wt_stream->recv_buf_size == 3);
        unsigned char dummy = 0;
        CU_ASSERT(xqc_wt_feed_stream_data(h3s, &dummy, 0, XQC_TRUE) == XQC_OK);
        CU_ASSERT(h3s->wt_stream->recv_fin == 1);
        CU_ASSERT(h3s->wt_stream->recv_reset == 0);

        /* accept establishes the session and attaches the stream, replaying
           the read notification for the bytes that waited */
        CU_ASSERT(xqc_wt_session_accept(session, &env.marker) == XQC_OK);
        CU_ASSERT(session->state == XQC_WT_SESSION_ESTABLISHED);
        CU_ASSERT(env.h3c->wt->buffered_stream_count == 0);
        CU_ASSERT(h3s->wt_stream->buffered == 0);
        CU_ASSERT(h3s->wt_stream->session == session);
        CU_ASSERT(h3s->wt_stream->opened_notified == 1);
        CU_ASSERT(xqc_wt_cap.opened_stream == h3s->wt_stream);
        CU_ASSERT(xqc_wt_cap.opened_session == session);
        CU_ASSERT(xqc_wt_cap.opened_stream_count == 1);
        CU_ASSERT(xqc_wt_cap.read_count == 1);
        CU_ASSERT(xqc_wt_cap.read_stream == h3s->wt_stream);

        unsigned char rbuf[16];
        uint8_t fin = 0;
        CU_ASSERT(xqc_wt_stream_recv(h3s->wt_stream, rbuf, sizeof(rbuf), &fin)
                  == 3);
        CU_ASSERT(memcmp(rbuf, "hi!", 3) == 0);
        CU_ASSERT(fin == 1);
        CU_ASSERT(xqc_wt_stream_recv(h3s->wt_stream, rbuf, sizeof(rbuf), &fin)
                  == -XQC_EAGAIN);

        xqc_wt_destroy_data_stream(h3s);
        xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
        CU_ASSERT(xqc_wt_cap.closed_count == 1);
        CU_ASSERT(xqc_wt_cap.closed_user_data == &env.marker);
    }

    /* --- the buffered-stream cap (Section 4.5): the fifth stream is
     * rejected with WEBTRANSPORT_BUFFERED_STREAM_REJECTED --- */
    {
        xqc_wt_stream_env_t env;
        CU_ASSERT_FATAL(xqc_wt_stream_test_setup_offered(&env) != NULL);
        xqc_wt_session_t *session = env.session;
        xqc_wt_set_session_stream_id(env.h3s, session);

        xqc_h3_stream_t *buffered[XQC_WT_BUFFERED_STREAM_MAX];

        p = wire;
        p = xqc_put_varint(p, XQC_WT_UNI_STREAM_TYPE);
        p = xqc_put_varint(p, session->stream_id);
        len = (size_t)(p - wire);

        for (i = 0; i < XQC_WT_BUFFERED_STREAM_MAX; i++) {
            buffered[i] = xqc_wt_open_peer_stream(env.conn, env.h3c, XQC_FALSE);
            CU_ASSERT_FATAL(buffered[i] != NULL);
            CU_ASSERT(xqc_wt_feed_stream_data(buffered[i], wire, len, XQC_FALSE)
                      == XQC_OK);
            CU_ASSERT_FATAL(buffered[i]->wt_stream != NULL);
            CU_ASSERT(buffered[i]->wt_stream->buffered == 1);
        }
        CU_ASSERT(env.h3c->wt->buffered_stream_count
                  == XQC_WT_BUFFERED_STREAM_MAX);

        /* one more: over the cap, reset with 0x3994bd84 */
        xqc_h3_stream_t *extra = xqc_wt_open_peer_stream(env.conn, env.h3c,
                                                         XQC_FALSE);
        CU_ASSERT_FATAL(extra != NULL);
        CU_ASSERT(xqc_wt_feed_stream_data(extra, wire, len, XQC_FALSE)
                  == XQC_OK);
        CU_ASSERT_FATAL(extra->wt_stream != NULL);
        CU_ASSERT(extra->wt_stream->buffered == 0);
        CU_ASSERT(extra->wt_stream->closed == 1);
        CU_ASSERT(extra->stream->stream_err
                  == XQC_WT_BUFFERED_STREAM_REJECTED);
        CU_ASSERT(env.h3c->wt->buffered_stream_count
                  == XQC_WT_BUFFERED_STREAM_MAX);

        /* accept attaches the four that were within the cap */
        CU_ASSERT(xqc_wt_session_accept(session, &env.marker) == XQC_OK);
        CU_ASSERT(env.h3c->wt->buffered_stream_count == 0);
        CU_ASSERT(xqc_wt_cap.opened_stream_count == XQC_WT_BUFFERED_STREAM_MAX);
        for (i = 0; i < XQC_WT_BUFFERED_STREAM_MAX; i++) {
            CU_ASSERT(buffered[i]->wt_stream->session == session);
            CU_ASSERT(buffered[i]->wt_stream->buffered == 0);
            xqc_wt_destroy_data_stream(buffered[i]);
        }
        xqc_wt_destroy_data_stream(extra);

        xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
        CU_ASSERT(xqc_wt_cap.closed_count == 1);
    }

    /* --- the buffered-datagram cap (Section 4.5): overflow is dropped, the
     * queued ones are delivered by accept --- */
    {
        xqc_wt_stream_env_t env;
        CU_ASSERT_FATAL(xqc_wt_stream_test_setup_offered(&env) != NULL);
        xqc_wt_session_t *session = env.session;
        xqc_wt_set_session_stream_id(env.h3s, session);

        for (i = 0; i < XQC_WT_BUFFERED_DGRAM_MAX + 4; i++) {
            p = wire;
            p = xqc_put_varint(p, 1);               /* quarter stream id 1 */
            *p++ = (uint8_t)i;
            CU_ASSERT(xqc_wt_conn_on_datagram(env.h3c, wire, (size_t)(p - wire))
                      == XQC_OK);
        }
        CU_ASSERT(env.h3c->wt->buffered_dgram_count
                  == XQC_WT_BUFFERED_DGRAM_MAX);
        CU_ASSERT(xqc_wt_cap.dgram_count == 0);

        CU_ASSERT(xqc_wt_session_accept(session, &env.marker) == XQC_OK);
        CU_ASSERT(env.h3c->wt->buffered_dgram_count == 0);
        CU_ASSERT(session->dgram_recv_count == XQC_WT_BUFFERED_DGRAM_MAX);
        /* one notification covers the whole flush */
        CU_ASSERT(xqc_wt_cap.dgram_count == 1);
        CU_ASSERT(xqc_wt_cap.dgram_session == session);

        /* FIFO order; the overflow (payloads 16..19) is gone */
        unsigned char buf[8];
        for (i = 0; i < XQC_WT_BUFFERED_DGRAM_MAX; i++) {
            CU_ASSERT(xqc_wt_session_recv_datagram(session, buf, sizeof(buf))
                      == 1);
            CU_ASSERT(buf[0] == (uint8_t)i);
        }
        CU_ASSERT(xqc_wt_session_recv_datagram(session, buf, sizeof(buf))
                  == -XQC_EAGAIN);

        xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
        CU_ASSERT(xqc_wt_cap.closed_count == 1);
    }

    /* --- a stream buffered for a session that never exists on this
     * connection is rejected when the connection closes (Section 4.5) --- */
    {
        xqc_wt_stream_env_t env;
        CU_ASSERT_FATAL(xqc_wt_stream_test_setup(&env) != NULL);

        xqc_h3_stream_t *orphan = xqc_wt_open_peer_stream(env.conn, env.h3c,
                                                          XQC_FALSE);
        CU_ASSERT_FATAL(orphan != NULL);

        p = wire;
        p = xqc_put_varint(p, XQC_WT_UNI_STREAM_TYPE);
        p = xqc_put_varint(p, 64);                  /* no such session */
        *p++ = 'q';
        len = (size_t)(p - wire);

        CU_ASSERT(xqc_wt_feed_stream_data(orphan, wire, len, XQC_FALSE)
                  == XQC_OK);
        CU_ASSERT_FATAL(orphan->wt_stream != NULL);
        CU_ASSERT(orphan->wt_stream->buffered == 1);
        CU_ASSERT(env.h3c->wt->buffered_stream_count == 1);

        /* connection teardown rejects it; the WT stream object is taken
           down by the flush even though its h3 stream is still alive */
        xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
        CU_ASSERT(orphan->wt_stream == NULL);
        CU_ASSERT(orphan->stream->stream_err
                  == XQC_WT_BUFFERED_STREAM_REJECTED);

        xqc_wt_destroy_data_stream(orphan);
    }
}

/* ------------------------------------------------------------------------
 * E6: session teardown fans out to the streams and datagrams
 * (draft-ietf-webtrans-http3-07 Section 5)
 * ------------------------------------------------------------------------ */

void
xqc_test_wt_session_teardown()
{
    uint8_t  wire[64];
    uint8_t *p;
    size_t   len;

    /* --- refuse: the buffered stream is rejected, the buffered datagram is
     * dropped (Section 4.5) --- */
    {
        xqc_wt_stream_env_t env;
        CU_ASSERT_FATAL(xqc_wt_stream_test_setup_offered(&env) != NULL);
        xqc_wt_session_t *session = env.session;
        xqc_wt_set_session_stream_id(env.h3s, session);

        xqc_h3_stream_t *h3s = xqc_wt_open_peer_stream(env.conn, env.h3c,
                                                       XQC_FALSE);
        CU_ASSERT_FATAL(h3s != NULL);

        p = wire;
        p = xqc_put_varint(p, XQC_WT_UNI_STREAM_TYPE);
        p = xqc_put_varint(p, session->stream_id);
        *p++ = 'z';
        len = (size_t)(p - wire);
        CU_ASSERT(xqc_wt_feed_stream_data(h3s, wire, len, XQC_FALSE)
                  == XQC_OK);
        CU_ASSERT_FATAL(h3s->wt_stream != NULL);
        CU_ASSERT(h3s->wt_stream->buffered == 1);

        p = wire;
        p = xqc_put_varint(p, 1);                   /* quarter stream id 1 */
        p = xqc_put_varint(p, 9);
        CU_ASSERT(xqc_wt_conn_on_datagram(env.h3c, wire, (size_t)(p - wire))
                  == XQC_OK);
        CU_ASSERT(env.h3c->wt->buffered_stream_count == 1);
        CU_ASSERT(env.h3c->wt->buffered_dgram_count == 1);

        /* the server refuses: both buffered items are done */
        CU_ASSERT(xqc_wt_session_reject(session, 403) == XQC_OK);
        CU_ASSERT(session->state == XQC_WT_SESSION_TERMINATED);
        CU_ASSERT(xqc_wt_cap.closed_count == 1);
        CU_ASSERT(xqc_wt_cap.opened_stream_count == 0);
        CU_ASSERT(env.h3c->wt->buffered_stream_count == 0);
        CU_ASSERT(env.h3c->wt->buffered_dgram_count == 0);
        CU_ASSERT(h3s->wt_stream->buffered == 0);
        CU_ASSERT(h3s->wt_stream->closed == 1);
        CU_ASSERT(h3s->stream->stream_err
                  == XQC_WT_BUFFERED_STREAM_REJECTED);

        xqc_wt_destroy_data_stream(h3s);
        xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
        CU_ASSERT(xqc_wt_cap.closed_count == 1);
        /* the stream was never announced, so no stream_closed_notify */
        CU_ASSERT(xqc_wt_cap.closed_stream_count == 0);
    }

    /* --- a stream or datagram that arrives after the session died: the
     * stream is reset with WEBTRANSPORT_SESSION_GONE, the datagram is
     * dropped (Section 5) --- */
    {
        xqc_wt_stream_env_t env;
        CU_ASSERT_FATAL(xqc_wt_stream_test_setup(&env) != NULL);
        xqc_wt_session_t *session = env.session;
        xqc_wt_set_session_stream_id(env.h3s, session);

        /* the server closes the session with a CLOSE capsule */
        CU_ASSERT(xqc_wt_session_close(session, 0x9, "bye") == XQC_OK);
        CU_ASSERT(session->state == XQC_WT_SESSION_TERMINATED);
        CU_ASSERT(xqc_wt_cap.closed_count == 1);

        xqc_h3_stream_t *h3s = xqc_wt_open_peer_stream(env.conn, env.h3c,
                                                       XQC_FALSE);
        CU_ASSERT_FATAL(h3s != NULL);

        p = wire;
        p = xqc_put_varint(p, XQC_WT_UNI_STREAM_TYPE);
        p = xqc_put_varint(p, session->stream_id);
        *p++ = 'x';
        len = (size_t)(p - wire);
        CU_ASSERT(xqc_wt_feed_stream_data(h3s, wire, len, XQC_FALSE)
                  == XQC_OK);
        CU_ASSERT_FATAL(h3s->wt_stream != NULL);
        CU_ASSERT(h3s->wt_stream->closed == 1);
        CU_ASSERT(h3s->wt_stream->buffered == 0);
        CU_ASSERT(h3s->wt_stream->session == NULL);
        CU_ASSERT(h3s->stream->stream_err == XQC_WT_SESSION_GONE);
        CU_ASSERT(env.h3c->wt->buffered_stream_count == 0);

        /* a datagram for the dead session is silently dropped */
        p = wire;
        p = xqc_put_varint(p, 1);
        p = xqc_put_varint(p, 5);
        CU_ASSERT(xqc_wt_conn_on_datagram(env.h3c, wire, (size_t)(p - wire))
                  == XQC_OK);
        CU_ASSERT(xqc_wt_cap.dgram_count == 0);
        CU_ASSERT(session->dgram_recv_count == 0);
        CU_ASSERT(env.h3c->wt->buffered_dgram_count == 0);
        CU_ASSERT(env.conn->conn_err == 0);

        xqc_wt_destroy_data_stream(h3s);
        xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
        CU_ASSERT(xqc_wt_cap.closed_count == 1);
        /* the stream was never announced, so no stream_closed_notify */
        CU_ASSERT(xqc_wt_cap.closed_stream_count == 0);
    }

    /* --- the CONNECT stream closes: the session terminates and every
     * associated stream is reset with WEBTRANSPORT_SESSION_GONE
     * (Section 5) --- */
    {
        xqc_wt_stream_env_t env;
        CU_ASSERT_FATAL(xqc_wt_stream_test_setup(&env) != NULL);
        xqc_wt_session_t *session = env.session;
        xqc_wt_set_session_stream_id(env.h3s, session);

        xqc_h3_stream_t *h3s = xqc_wt_open_peer_stream(env.conn, env.h3c,
                                                       XQC_FALSE);
        CU_ASSERT_FATAL(h3s != NULL);

        p = wire;
        p = xqc_put_varint(p, XQC_WT_UNI_STREAM_TYPE);
        p = xqc_put_varint(p, session->stream_id);
        *p++ = 'k';
        len = (size_t)(p - wire);
        CU_ASSERT(xqc_wt_feed_stream_data(h3s, wire, len, XQC_FALSE)
                  == XQC_OK);
        CU_ASSERT_FATAL(h3s->wt_stream != NULL);
        CU_ASSERT(h3s->wt_stream->session == session);
        CU_ASSERT(xqc_wt_cap.opened_stream_count == 1);

        /* the h3 layer destroys the CONNECT request (stream closed) */
        env.h3s->h3r->wt_session = NULL;
        xqc_wt_session_on_request_closed(session);

        CU_ASSERT(xqc_wt_cap.closed_count == 1);
        CU_ASSERT(xqc_wt_cap.closed_user_data == &env.marker);
        CU_ASSERT(h3s->wt_stream->closed == 1);
        CU_ASSERT(h3s->wt_stream->session == NULL);
        CU_ASSERT(h3s->stream->stream_err == XQC_WT_SESSION_GONE);

        /* bytes received before the teardown stay readable */
        unsigned char rbuf[8];
        uint8_t fin = 0;
        CU_ASSERT(xqc_wt_stream_recv(h3s->wt_stream, rbuf, sizeof(rbuf), &fin)
                  == 1);
        CU_ASSERT(rbuf[0] == 'k');
        CU_ASSERT(fin == 0);
        CU_ASSERT(xqc_wt_stream_recv(h3s->wt_stream, rbuf, sizeof(rbuf), &fin)
                  == -XQC_ESTATE);

        xqc_wt_destroy_data_stream(h3s);
        xqc_wt_req_test_teardown(env.h3s, env.h3c, env.conn);
        CU_ASSERT(xqc_wt_cap.closed_count == 1);

        /* stream_closed_notify fires after session_closed_notify: the
         * announced stream died only when its transport stream was
         * destroyed, and the application user data was never set */
        CU_ASSERT(xqc_wt_cap.closed_stream_count == 1);
        CU_ASSERT(xqc_wt_cap.closed_stream_user_data == NULL);
    }
}