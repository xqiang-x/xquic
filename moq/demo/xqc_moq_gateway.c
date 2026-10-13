/*
 * MoQ gateway: SFU Frame IPC client -> MoQ publisher.
 *
 * Runs a QUIC server (ALPN "moq-quic", transport XQC_MOQ_TRANSPORT_QUIC)
 * like xqc_moq_demo_server, but frames come from the SFU's FrameDispatcher
 * over a UDS socket (FrameIpc v1, see xqc_moq_gateway_frame_ipc.h) instead
 * of a fake generator. Subscriber interest drives the SFU subscription:
 * a stream is SUBscribed on the first subscriber and UNSUBscribed when the
 * last one leaves (connection close). Frame mapping, params caching and
 * per-subscription fan-out live in xqc_moq_gateway_feed.c.
 *
 * CLI: -p port, -l log level, -c cong, -d drop, -f fec (downstream QUIC),
 *      --stream <id> (repeatable, required), --ipc <path>,
 *      --codec/--width/--height/--fps/--bitrate (catalog metadata),
 *      --wt (also serve MoQ over WebTransport on the h3 ALPN, with
 *      --wt-cert/--wt-key overrides and a printed certificate fingerprint
 *      for browser serverCertificateHashes).
 */

#define _GNU_SOURCE

#include "xqc_moq_demo_comm.h"
#include "xqc_moq_gateway_feed.h"
#include "xqc_moq_gateway_ipc.h"

#include <moq/moq_transport/xqc_moq_stream_webtransport.h>
#include <xquic/xqc_webtransport.h>

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <signal.h>

#define GW_DEFAULT_PORT      8443
#define GW_DEFAULT_IPC_PATH  "/tmp/tj_rtc_sfu.sock"
#define GW_DEFAULT_CERT_PATH "./server.crt"
#define GW_DEFAULT_KEY_PATH  "./server.key"
#define GW_STREAMS_MAX       32
#define GW_FANOUT_CAP        XQC_GW_FANOUT_CAP
#define GW_CID_LEN           12
#define GW_PACKET_TMP_BUF    1500
#define GW_WT_MAX_SESSIONS   16

#define GW_SUB_RETRY_US      (3 * 1000000)   /* SUB re-send after SUB_ERR */
#define GW_PLI_SUB_OK_US     (1 * 1000000)   /* PLI after SUB_OK */
#define GW_PLI_REQUEST_US    (300 * 1000)    /* PLI for keyframe requests */
#define GW_STATS_INTERVAL_S  5
#define GW_RETRY_TICK_S      3

/* MOQ draft subscribe error codes. The library defines no constants, and
 * the stock client only logs the value. */
#define GW_MOQ_ERR_INTERNAL        0x0
#define GW_MOQ_ERR_TRACK_NOT_FOUND 0x4

extern xqc_usec_t xqc_now(void);

static xqc_app_ctx_t ctx;
static struct event_base *eb;

static char g_ipc_path[256] = GW_DEFAULT_IPC_PATH;
static char g_cert_path[256] = GW_DEFAULT_CERT_PATH;
static char g_key_path[256] = GW_DEFAULT_KEY_PATH;
static int g_fec_on = 0;
static int g_wt_on = 0;
static xqc_moq_role_t g_role = XQC_MOQ_PUBLISHER;

struct gw_video_cfg_s {
    char        codec[64];
    int         width;
    int         height;
    int         fps;
    uint64_t    bitrate;
} g_video = { "avc1.42E01E", 1280, 720, 30, 4000000 };

static xqc_gw_stream_t g_streams[GW_STREAMS_MAX];
static int g_stream_count = 0;
static xqc_gw_fanout_t g_fanout;
static xqc_gw_ipc_t *g_ipc = NULL;
static struct event *g_ev_stats = NULL;
static struct event *g_ev_sub_retry = NULL;
static uint64_t g_unknown_sid_frames = 0;

/*
 * Per-connection state. The embedded user_conn_t keeps the layout expected
 * by xqc_moq_demo_comm.c (xqc_app_write_socket reads ->fd), so the shared
 * comm helpers are reused as-is.
 */
typedef struct gw_conn_s {
    user_conn_t      uc;
    xqc_moq_session_t *moq_session;   /* fan-out session cookie */
    xqc_gw_stream_t *streams[GW_STREAMS_MAX];
    xqc_moq_track_t *tracks[GW_STREAMS_MAX];
    int              track_count;
} gw_conn_t;

static void gw_pump_streams(void);

/* ── IPC callbacks (SFU side of the contract) ────────────────────────────── */

static int
gw_pli_allowed(xqc_gw_stream_t *st, uint64_t window_us)
{
    uint64_t now = xqc_now();
    if (st->last_pli_us != 0 && now - st->last_pli_us < window_us) {
        return 0;
    }
    st->last_pli_us = now;
    return 1;
}

static void
gw_ipc_on_frame(void *ud, const xqc_gw_ipc_data_t *data)
{
    xqc_gw_stream_t *st;
    xqc_gw_feed_out_t out;
    xqc_gw_frame_action_t action;
    (void)ud;

    st = xqc_gw_stream_find_by_sid(g_streams, g_stream_count, data->sid);
    if (st == NULL) {
        /* covers the window before SUB_OK, and stale sids after reconnect */
        g_unknown_sid_frames++;
        return;
    }

    action = xqc_gw_feed_on_frame(st, data->frame_type, data->flags,
        data->pts_us, data->seq, data->payload, data->payload_len, &out);
    if (action == XQC_GW_FRAME_KEY || action == XQC_GW_FRAME_DELTA) {
        xqc_gw_fanout_deliver_video(&g_fanout, st, &out);
    }
    xqc_gw_feed_out_cleanup(&out);
}

static void
gw_ipc_on_sub_ok(void *ud, const xqc_gw_ipc_sub_ok_t *ok)
{
    xqc_gw_stream_t *st;
    (void)ud;

    st = xqc_gw_stream_find_by_name(g_streams, g_stream_count, ok->name,
        ok->name_len);
    if (st == NULL) {
        printf("[gw] SUB_OK for unknown stream '%.*s'\n", (int)ok->name_len,
               ok->name);
        return;
    }

    st->sid = ok->sid;
    st->sub_pending = 0;
    printf("[gw] stream '%s' subscribed, sid=%u\n", st->name, st->sid);

    if (xqc_gw_fanout_refs_on_stream(&g_fanout, st) > 0
        && gw_pli_allowed(st, GW_PLI_SUB_OK_US))
    {
        xqc_gw_ipc_send_pli(g_ipc, st->sid);
    }
    gw_pump_streams();
}

static void
gw_ipc_on_sub_err(void *ud, const xqc_gw_ipc_sub_err_t *err)
{
    xqc_gw_stream_t *st;
    (void)ud;

    st = xqc_gw_stream_find_by_name(g_streams, g_stream_count, err->name,
        err->name_len);
    if (st == NULL) {
        printf("[gw] SUB_ERR for unknown stream '%.*s' reason=%u\n",
               (int)err->name_len, err->name, err->reason);
        return;
    }

    st->sid = 0;
    st->sub_pending = 0;
    st->sub_retry_us = xqc_now() + GW_SUB_RETRY_US;
    printf("[gw] stream '%s' subscribe error reason=%u, retry in 3s\n",
           st->name, err->reason);
}

static void
gw_ipc_on_state(void *ud, int connected)
{
    int i;
    (void)ud;

    if (!connected) {
        for (i = 0; i < g_stream_count; i++) {
            g_streams[i].sid = 0;
            g_streams[i].sub_pending = 0;
        }
        printf("[gw] SFU IPC disconnected, reconnecting every 1s\n");
        return;
    }

    printf("[gw] SFU IPC connected\n");
    if (g_ipc != NULL) {
        gw_pump_streams();
    }
}

/*
 * Reconcile SFU subscriptions with downstream interest. refs() is derived
 * from the fan-out table (single source of truth), so this is idempotent
 * and can be called after any state change.
 */
static void
gw_pump_streams(void)
{
    uint64_t now;
    int i;

    if (g_ipc == NULL || !xqc_gw_ipc_connected(g_ipc)) {
        return;
    }
    now = xqc_now();

    for (i = 0; i < g_stream_count; i++) {
        xqc_gw_stream_t *st = &g_streams[i];
        int refs = xqc_gw_fanout_refs_on_stream(&g_fanout, st);

        if (refs > 0) {
            if (st->sid == 0 && !st->sub_pending && now >= st->sub_retry_us) {
                if (xqc_gw_ipc_send_sub(g_ipc, st->name,
                        (uint16_t)strlen(st->name)) == 0)
                {
                    st->sub_pending = 1;
                }
            }
        } else {
            if (st->sid != 0) {
                xqc_gw_ipc_send_unsub(g_ipc, st->sid);
                st->sid = 0;
            }
            st->sub_pending = 0;
        }
    }
}

/* ── periodic timers ─────────────────────────────────────────────────────── */

static void
gw_stats_cb(evutil_socket_t fd, short what, void *arg)
{
    xqc_gw_ipc_stats_t ipc;
    struct timeval tv = { GW_STATS_INTERVAL_S, 0 };
    int i, active_subs = 0;
    (void)fd;
    (void)what;
    (void)arg;

    for (i = 0; i < g_fanout.cap; i++) {
        if (g_fanout.subs[i].active) {
            active_subs++;
        }
    }
    xqc_gw_ipc_get_stats(g_ipc, &ipc);

    printf("[gw] ipc: %s connects=%"PRIu64" disconnects=%"PRIu64
           " frames_rx=%"PRIu64" sub_ok=%"PRIu64" sub_err=%"PRIu64
           " dropped=%"PRIu64" proto_err=%"PRIu64" send_fail=%"PRIu64"\n",
           xqc_gw_ipc_connected(g_ipc) ? "up" : "down",
           ipc.connects, ipc.disconnects, ipc.frames_rx, ipc.sub_ok_rx,
           ipc.sub_err_rx, ipc.dropped_non_data, ipc.protocol_errors,
           ipc.send_failures);
    printf("[gw] fanout: active_subs=%d writes=%"PRIu64" wait_key_drops=%"PRIu64
           " write_errors=%"PRIu64" no_sub=%"PRIu64" unknown_sid=%"PRIu64"\n",
           active_subs, g_fanout.writes, g_fanout.dropped_wait_key,
           g_fanout.write_errors, g_fanout.no_sub, g_unknown_sid_frames);
    for (i = 0; i < g_stream_count; i++) {
        xqc_gw_stream_t *st = &g_streams[i];
        printf("[gw] stream %s: sid=%u refs=%d relayed=%"PRIu64" ignored=%"PRIu64
               " params_cached=%"PRIu64"\n",
               st->name, st->sid, xqc_gw_fanout_refs_on_stream(&g_fanout, st),
               st->frames_relayed, st->frames_ignored, st->params_cached);
    }

    evtimer_add(g_ev_stats, &tv);
}

static void
gw_sub_retry_cb(evutil_socket_t fd, short what, void *arg)
{
    struct timeval tv = { GW_RETRY_TICK_S, 0 };
    (void)fd;
    (void)what;
    (void)arg;

    gw_pump_streams();
    evtimer_add(g_ev_sub_retry, &tv);
}

/* ── MoQ callbacks (client side of the QUIC session) ─────────────────────── */

static int
gw_session_publishes(gw_conn_t *gw, xqc_gw_stream_t *st)
{
    int i;
    for (i = 0; i < gw->track_count; i++) {
        if (gw->streams[i] == st) {
            return 1;
        }
    }
    return 0;
}

static xqc_int_t
gw_write_video_shim(void *session, uint64_t subscribe_id,
    xqc_moq_track_t *track, xqc_moq_video_frame_t *frame)
{
    return xqc_moq_write_video_frame((xqc_moq_session_t *)session,
        subscribe_id, track, frame);
}

static void
gw_on_session_setup(xqc_moq_user_session_t *user_session, char *extdata)
{
    gw_conn_t *gw = (gw_conn_t *)user_session->data;
    xqc_moq_session_t *session = user_session->session;
    int sel = -1;
    int publish_all = 1;
    int i;

    gw->moq_session = session;
    gw->track_count = 0;

    /* extdata selects a single stream; anything unrecognized (the stock
     * moq_demo_client sends a fixed "extdata") falls back to all streams. */
    if (extdata != NULL && extdata[0] != '\0') {
        xqc_gw_stream_t *st = xqc_gw_stream_find_by_name(g_streams,
            g_stream_count, extdata, strlen(extdata));
        if (st != NULL) {
            sel = (int)(st - g_streams);
            publish_all = 0;
        } else {
            printf("[gw] unrecognized extdata '%s', publishing all streams\n",
                   extdata);
        }
    }

    for (i = 0; i < g_stream_count; i++) {
        xqc_moq_selection_params_t params;
        xqc_moq_track_t *track;

        if (!publish_all && i != sel) {
            continue;
        }

        memset(&params, 0, sizeof(params));
        params.codec = g_video.codec;
        params.mime_type = "video/mp4";
        params.bitrate = g_video.bitrate;
        params.framerate = g_video.fps;
        params.width = g_video.width;
        params.height = g_video.height;

        track = xqc_moq_track_create(session, g_streams[i].track_namespace,
            XQC_GW_TRACK_NAME, XQC_MOQ_TRACK_VIDEO, &params,
            XQC_MOQ_CONTAINER_LOC, XQC_MOQ_TRACK_FOR_PUB);
        if (track == NULL) {
            printf("[gw] track create failed for '%s'\n", g_streams[i].name);
            continue;
        }
        gw->streams[gw->track_count] = &g_streams[i];
        gw->tracks[gw->track_count] = track;
        gw->track_count++;
    }
    printf("[gw] session setup: %d track(s) published\n", gw->track_count);
}

static void
gw_on_datachannel(xqc_moq_user_session_t *user_session)
{
    (void)user_session;
}

static void
gw_on_datachannel_msg(xqc_moq_user_session_t *user_session, uint8_t *msg,
    size_t msg_len)
{
    (void)user_session;
    (void)msg;
    (void)msg_len;
}

static void
gw_on_subscribe(xqc_moq_user_session_t *user_session, uint64_t subscribe_id,
    xqc_moq_track_t *track, xqc_moq_subscribe_msg_t *msg)
{
    xqc_moq_session_t *session = user_session->session;
    gw_conn_t *gw = (gw_conn_t *)user_session->data;
    xqc_gw_stream_t *st;
    xqc_gw_sub_t *sub;

    st = xqc_gw_stream_find_by_track(g_streams, g_stream_count,
        msg->track_namespace, msg->track_namespace_len,
        msg->track_name, msg->track_name_len);

    if (st == NULL || !gw_session_publishes(gw, st)) {
        xqc_moq_subscribe_error_msg_t err;
        memset(&err, 0, sizeof(err));
        err.subscribe_id = subscribe_id;
        err.error_code = GW_MOQ_ERR_TRACK_NOT_FOUND;
        err.reason_phrase = "track not found";
        err.reason_phrase_len = strlen("track not found");
        err.track_alias = msg->track_alias;
        if (xqc_moq_write_subscribe_error(session, &err) < 0) {
            printf("[gw] write_subscribe_error failed\n");
        }
        printf("[gw] SUB rejected: %.*s/%.*s\n",
               (int)msg->track_namespace_len, msg->track_namespace,
               (int)msg->track_name_len, msg->track_name);
        return;
    }

    sub = xqc_gw_fanout_add(&g_fanout, st, subscribe_id, track, session);
    if (sub == NULL) {
        xqc_moq_subscribe_error_msg_t err;
        memset(&err, 0, sizeof(err));
        err.subscribe_id = subscribe_id;
        err.error_code = GW_MOQ_ERR_INTERNAL;
        err.reason_phrase = "too many subscriptions";
        err.reason_phrase_len = strlen("too many subscriptions");
        err.track_alias = msg->track_alias;
        if (xqc_moq_write_subscribe_error(session, &err) < 0) {
            printf("[gw] write_subscribe_error failed\n");
        }
        printf("[gw] SUB rejected: fan-out table full\n");
        return;
    }

    xqc_moq_subscribe_ok_msg_t ok;
    memset(&ok, 0, sizeof(ok));
    ok.subscribe_id = subscribe_id;
    ok.expire_ms = 0;
    ok.content_exist = 1;
    if (xqc_moq_write_subscribe_ok(session, &ok) < 0) {
        printf("[gw] write_subscribe_ok failed\n");
    }
    printf("[gw] stream '%s' +1 subscriber\n", st->name);

    gw_pump_streams();
}

static void
gw_on_request_keyframe(xqc_moq_user_session_t *user_session,
    uint64_t subscribe_id, xqc_moq_track_t *track)
{
    int i;
    (void)track;

    for (i = 0; i < g_fanout.cap; i++) {
        xqc_gw_sub_t *sub = &g_fanout.subs[i];
        if (sub->active && sub->session == user_session->session
            && sub->subscribe_id == subscribe_id)
        {
            xqc_gw_stream_t *st = sub->stream;
            if (st->sid != 0 && gw_pli_allowed(st, GW_PLI_REQUEST_US)) {
                xqc_gw_ipc_send_pli(g_ipc, st->sid);
                printf("[gw] keyframe request -> PLI stream '%s' sid=%u\n",
                       st->name, st->sid);
            }
            return;
        }
    }
}

static void
gw_on_bitrate_change(xqc_moq_user_session_t *user_session, uint64_t bitrate)
{
    (void)user_session;
    (void)bitrate;
}

/* subscriber-side callbacks; a publisher session never fires them */
static void
gw_on_subscribe_ok(xqc_moq_user_session_t *user_session,
    xqc_moq_subscribe_ok_msg_t *subscribe_ok)
{
    (void)user_session;
    (void)subscribe_ok;
}

static void
gw_on_subscribe_error(xqc_moq_user_session_t *user_session,
    xqc_moq_subscribe_error_msg_t *subscribe_error)
{
    (void)user_session;
    (void)subscribe_error;
}

static void
gw_on_catalog(xqc_moq_user_session_t *user_session,
    xqc_moq_track_info_t **track_info_array, xqc_int_t array_size)
{
    (void)user_session;
    (void)track_info_array;
    (void)array_size;
}

static void
gw_on_video_frame(xqc_moq_user_session_t *user_session, uint64_t subscribe_id,
    xqc_moq_video_frame_t *video_frame)
{
    (void)user_session;
    (void)subscribe_id;
    (void)video_frame;
}

static void
gw_on_audio_frame(xqc_moq_user_session_t *user_session, uint64_t subscribe_id,
    xqc_moq_audio_frame_t *audio_frame)
{
    (void)user_session;
    (void)subscribe_id;
    (void)audio_frame;
}

/* ── QUIC transport callbacks ────────────────────────────────────────────── */

static void
gw_fill_session_callbacks(xqc_moq_session_callbacks_t *callbacks)
{
    memset(callbacks, 0, sizeof(*callbacks));
    callbacks->on_session_setup = gw_on_session_setup;
    callbacks->on_datachannel = gw_on_datachannel;
    callbacks->on_datachannel_msg = gw_on_datachannel_msg;
    callbacks->on_subscribe = gw_on_subscribe;
    callbacks->on_request_keyframe = gw_on_request_keyframe;
    callbacks->on_bitrate_change = gw_on_bitrate_change;
    callbacks->on_subscribe_ok = gw_on_subscribe_ok;
    callbacks->on_subscribe_error = gw_on_subscribe_error;
    callbacks->on_catalog = gw_on_catalog;
    callbacks->on_video = gw_on_video_frame;
    callbacks->on_audio = gw_on_audio_frame;
}

static xqc_moq_session_t *
gw_moq_session_start(void *trans_conn, xqc_moq_transport_type_t transport_type,
    xqc_moq_user_session_t *user_session)
{
    xqc_moq_session_callbacks_t callbacks;

    gw_fill_session_callbacks(&callbacks);
    return xqc_moq_session_create(trans_conn, user_session, transport_type,
        g_role, callbacks, NULL);
}

/*
 * Per-connection arena. The embedded user_conn_t keeps the layout expected
 * by xqc_moq_demo_comm.c (xqc_app_write_socket reads ->fd), so the shared
 * comm helpers are reused as-is.
 */
static xqc_moq_user_session_t *
gw_conn_alloc(void)
{
    xqc_moq_user_session_t *user_session;
    gw_conn_t *gw;

    user_session = calloc(1, sizeof(xqc_moq_user_session_t) + sizeof(gw_conn_t));
    if (user_session == NULL) {
        return NULL;
    }
    gw = (gw_conn_t *)user_session->data;
    gw->uc.fd = ctx.listen_fd;   /* used by the shared write_socket helper */
    return user_session;
}

static int
gw_server_accept(xqc_engine_t *engine, xqc_connection_t *conn,
    const xqc_cid_t *cid, void *user_data)
{
    xqc_moq_user_session_t *user_session;
    gw_conn_t *gw;

    user_session = gw_conn_alloc();
    if (user_session == NULL) {
        return -1;
    }
    gw = (gw_conn_t *)user_session->data;

    if (g_wt_on) {
        /*
         * The ALPN is not known yet on the first Initial. Attach the user
         * data now: the shared write_socket helper dereferences it for
         * every packet of every connection. The MoQ session starts once the
         * ALPN resolved: gw_server_conn_create_notify for moq-quic, the WT
         * session-accepted callback for h3.
         */
        xqc_conn_set_transport_user_data(conn, user_session);
        memcpy(&gw->uc.cid, cid, sizeof(*cid));
        printf("[gw] connection accepted (alpn pending) scid=%s\n",
               xqc_scid_str(engine, cid));
        (void)user_data;
        return 0;
    }

    if (gw_moq_session_start(conn, XQC_MOQ_TRANSPORT_QUIC,
            user_session) == NULL)
    {
        printf("[gw] create session error\n");
        free(user_session);
        return -1;
    }
    xqc_conn_set_transport_user_data(conn, user_session);
    memcpy(&gw->uc.cid, cid, sizeof(*cid));
    printf("[gw] connection accepted scid=%s\n", xqc_scid_str(engine, cid));
    (void)user_data;
    return 0;
}

static void
gw_server_refuse(xqc_engine_t *engine, xqc_connection_t *conn,
    const xqc_cid_t *cid, void *user_data)
{
    printf("[gw] connection refused cid=%p\n", (void *)cid);
    if (g_wt_on && user_data != NULL) {
        /* ALPN never resolved: the arena attached in accept has no owner */
        free(user_data);
    }
    (void)engine;
    (void)conn;
}

static ssize_t
gw_server_stateless_reset(const unsigned char *buf, size_t size,
    const struct sockaddr *peer_addr, socklen_t peer_addrlen,
    const struct sockaddr *local_addr, socklen_t local_addrlen, void *user_data)
{
    ssize_t res;
    (void)local_addr;
    (void)local_addrlen;
    (void)user_data;

    do {
        set_sys_errno(0);
        res = sendto(ctx.listen_fd, buf, size, 0, peer_addr, peer_addrlen);
        if (res < 0) {
            if (get_sys_errno() == EAGAIN) {
                res = XQC_SOCKET_EAGAIN;
            }
        }
    } while ((res < 0) && (EINTR == get_sys_errno()));

    return res;
}

static int
gw_server_conn_closing_notify(xqc_connection_t *conn, const xqc_cid_t *cid,
    xqc_int_t err_code, void *conn_user_data)
{
    printf("[gw] connection closing scid=%s err=%d\n",
           xqc_scid_str(ctx.engine, cid), err_code);
    (void)conn;
    (void)conn_user_data;
    return XQC_OK;
}

static int
gw_server_conn_create_notify(xqc_connection_t *conn, const xqc_cid_t *cid,
    void *user_data, void *conn_proto_data)
{
    printf("[gw] connection created scid=%s\n", xqc_scid_str(ctx.engine, cid));

    if (g_wt_on) {
        /*
         * ALPN resolved to moq-quic: --wt mode defers the MoQ session from
         * accept() to here, when the protocol is actually known. user_data
         * is the arena attached by accept().
         */
        xqc_moq_user_session_t *user_session =
            (xqc_moq_user_session_t *)user_data;
        if (user_session == NULL
            || gw_moq_session_start(conn, XQC_MOQ_TRANSPORT_QUIC,
                   user_session) == NULL)
        {
            printf("[gw] create session error\n");
            return -1;
        }
    }

    (void)conn_proto_data;
    return 0;
}

static int
gw_server_conn_close_notify(xqc_connection_t *conn, const xqc_cid_t *cid,
    void *user_data, void *conn_proto_data)
{
    xqc_moq_user_session_t *user_session = (xqc_moq_user_session_t *)user_data;
    gw_conn_t *gw = (gw_conn_t *)user_session->data;
    xqc_conn_stats_t stats = xqc_conn_get_stats(ctx.engine, cid);

    printf("[gw] connection closed scid=%s conn_err=%d srtt=%"PRIu64
           ", dropping %d track(s)\n",
           xqc_scid_str(ctx.engine, cid), stats.conn_err, stats.srtt,
           gw->track_count);

    /* last-subscriber release: UNSUB streams whose refs dropped to zero */
    xqc_gw_fanout_remove_session(&g_fanout, gw->moq_session);
    gw_pump_streams();

    xqc_moq_session_destroy(user_session->session);
    free(user_session);
    (void)conn;
    (void)conn_proto_data;
    return 0;
}

static void
gw_server_conn_handshake_finished(xqc_connection_t *conn, void *user_data,
    void *conn_proto_data)
{
    (void)conn;
    (void)user_data;
    (void)conn_proto_data;
}

/* ── WebTransport callbacks (--wt: MoQ over WebTransport on ALPN h3) ─────── */

static int
gw_wt_session_request(xqc_wt_session_t *session, void *conn_user_data)
{
    xqc_moq_user_session_t *user_session =
        (xqc_moq_user_session_t *)conn_user_data;

    if (user_session == NULL) {
        return -XQC_ESTATE;
    }
    if (user_session->session != NULL) {
        /* one MoQ session per connection */
        printf("[gw] second WebTransport session refused\n");
        return -XQC_ESTATE;
    }
    if (xqc_wt_session_accept(session, user_session) != XQC_OK) {
        printf("[gw] WebTransport session accept failed\n");
        return -XQC_ESTATE;
    }
    printf("[gw] WebTransport session offered, accepting stream_id=%"PRIu64"\n",
           xqc_wt_session_get_stream_id(session));
    return XQC_OK;
}

static int
gw_wt_session_accepted(xqc_wt_session_t *session, void *session_user_data)
{
    xqc_moq_user_session_t *user_session =
        (xqc_moq_user_session_t *)session_user_data;

    printf("[gw] WebTransport session established stream_id=%"PRIu64"\n",
           xqc_wt_session_get_stream_id(session));

    if (user_session == NULL
        || gw_moq_session_start(session, XQC_MOQ_TRANSPORT_WEBTRANSPORT,
               user_session) == NULL)
    {
        printf("[gw] create MoQ session over WebTransport error\n");
        xqc_wt_session_close(session, 0, "session setup failed");
        return -XQC_ESTATE;
    }
    return XQC_OK;
}

static int
gw_wt_session_closed(xqc_wt_session_t *session, void *session_user_data)
{
    xqc_moq_user_session_t *user_session =
        (xqc_moq_user_session_t *)session_user_data;
    gw_conn_t *gw;

    printf("[gw] WebTransport session closed stream_id=%"PRIu64"\n",
           xqc_wt_session_get_stream_id(session));

    if (user_session == NULL) {
        /* refused or aborted before accept(): no MoQ session exists */
        return XQC_OK;
    }

    gw = (gw_conn_t *)user_session->data;
    xqc_gw_fanout_remove_session(&g_fanout, gw->moq_session);
    gw_pump_streams();

    if (user_session->session != NULL) {
        xqc_moq_session_destroy(user_session->session);
        user_session->session = NULL;
    }
    gw->moq_session = NULL;
    gw->track_count = 0;
    /* the arena stays until the h3 connection closes: write_socket still
     * needs it for the other streams of the connection */
    return XQC_OK;
}

static int
gw_h3_conn_create_notify(xqc_h3_conn_t *h3c, const xqc_cid_t *cid,
    void *h3c_user_data)
{
    xqc_wt_callbacks_t wt_cbs;

    printf("[gw] h3 connection created scid=%s\n",
           xqc_scid_str(ctx.engine, cid));

    memset(&wt_cbs, 0, sizeof(wt_cbs));
    wt_cbs.session_request_notify = gw_wt_session_request;
    wt_cbs.session_accepted_notify = gw_wt_session_accepted;
    wt_cbs.session_closed_notify = gw_wt_session_closed;
    wt_cbs.stream_opened_notify = xqc_moq_wt_on_stream_opened;
    wt_cbs.stream_read_notify = xqc_moq_wt_on_stream_read;
    wt_cbs.stream_closed_notify = xqc_moq_wt_on_stream_closed;

    if (xqc_h3_conn_set_webtransport_callbacks(h3c, &wt_cbs) != XQC_OK) {
        printf("[gw] set webtransport callbacks failed\n");
        return -1;
    }
    printf("[gw] webtransport ready on h3 connection\n");
    (void)h3c_user_data;
    return 0;
}

static int
gw_h3_conn_close_notify(xqc_h3_conn_t *h3c, const xqc_cid_t *cid,
    void *h3c_user_data)
{
    xqc_moq_user_session_t *user_session =
        (xqc_moq_user_session_t *)h3c_user_data;
    gw_conn_t *gw;
    xqc_conn_stats_t stats;

    if (user_session == NULL) {
        return 0;
    }
    gw = (gw_conn_t *)user_session->data;
    stats = xqc_conn_get_stats(ctx.engine, cid);

    printf("[gw] h3 connection closed scid=%s conn_err=%d srtt=%"PRIu64
           ", dropping %d track(s)\n",
           xqc_scid_str(ctx.engine, cid), stats.conn_err, stats.srtt,
           gw->track_count);

    xqc_gw_fanout_remove_session(&g_fanout, gw->moq_session);
    gw_pump_streams();

    if (user_session->session != NULL) {
        xqc_moq_session_destroy(user_session->session);
        user_session->session = NULL;
    }
    free(user_session);
    (void)h3c;
    return 0;
}

static void
gw_h3_conn_handshake_finished(xqc_h3_conn_t *h3c, void *h3c_user_data)
{
    (void)h3c;
    (void)h3c_user_data;
}

/* ── socket + engine glue ────────────────────────────────────────────────── */

static int
gw_server_create_socket(unsigned int port)
{
    int fd;
    int size;
    int optval = 1;
    struct sockaddr_in *addr_v4 = (struct sockaddr_in *)&ctx.local_addr;

    fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        printf("[gw] create socket failed, errno: %d\n", get_sys_errno());
        return -1;
    }
    if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1) {
        printf("[gw] set socket nonblock failed, errno: %d\n", errno);
        close(fd);
        return -1;
    }
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) < 0) {
        goto err;
    }
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval)) < 0) {
        goto err;
    }
    size = 1 * 1024 * 1024;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &size, sizeof(int)) < 0) {
        goto err;
    }
    if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof(int)) < 0) {
        goto err;
    }

    memset(addr_v4, 0, sizeof(*addr_v4));
    addr_v4->sin_family = AF_INET;
    addr_v4->sin_port = htons(port);
    addr_v4->sin_addr.s_addr = htonl(INADDR_ANY);
    ctx.local_addrlen = sizeof(*addr_v4);

    if (bind(fd, (struct sockaddr *)addr_v4, ctx.local_addrlen) < 0) {
        printf("[gw] bind socket failed, errno: %d\n", get_sys_errno());
        goto err;
    }
    return fd;

err:
    printf("[gw] setsockopt failed, errno: %d\n", get_sys_errno());
    close(fd);
    return -1;
}

static void
gw_server_socket_read_handler(xqc_app_ctx_t *ctx)
{
    ssize_t recv_size;
    ssize_t recv_sum = 0;
    unsigned char packet_buf[GW_PACKET_TMP_BUF];
    uint64_t recv_time;
    xqc_int_t ret;

    do {
        struct sockaddr_in peer_addr;
        socklen_t peer_addrlen = sizeof(peer_addr);

        recv_size = recvfrom(ctx->listen_fd, packet_buf, sizeof(packet_buf), 0,
            (struct sockaddr *)&peer_addr, &peer_addrlen);
        if (recv_size < 0 && get_sys_errno() == EAGAIN) {
            break;
        }
        if (recv_size < 0) {
            printf("[gw] recvfrom err %zd %s\n", recv_size,
                   strerror(get_sys_errno()));
            break;
        }

        recv_sum += recv_size;
        recv_time = xqc_now();
        ret = xqc_engine_packet_process(ctx->engine, packet_buf, recv_size,
            (struct sockaddr *)&ctx->local_addr, ctx->local_addrlen,
            (struct sockaddr *)&peer_addr, peer_addrlen,
            (xqc_usec_t)recv_time, NULL);
        if (ret != XQC_OK) {
            printf("[gw] packet process err: %d\n", ret);
            return;
        }
    } while (recv_size > 0);

    xqc_engine_finish_recv(ctx->engine);
    (void)recv_sum;
}

static void
gw_server_socket_event_callback(int fd, short what, void *arg)
{
    xqc_app_ctx_t *ctx = (xqc_app_ctx_t *)arg;

    if (what & EV_READ) {
        gw_server_socket_read_handler(ctx);
    } else {
        printf("[gw] event callback unexpected what=%d\n", what);
        exit(1);
    }
    (void)fd;
}

static void
gw_stop(int signo)
{
    (void)signo;
    event_base_loopbreak(eb);
    xqc_engine_destroy(ctx.engine);
    fflush(stdout);
    exit(0);
}

/*
 * Print the cert's sha-256/<base64> fingerprint, the form the WebTransport
 * browser API expects in serverCertificateHashes, and validate that the key
 * file is readable.
 */
static int
gw_print_cert_fingerprint(const char *cert_file, const char *key_file)
{
    FILE *fp;
    X509 *cert;
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digest_len = 0;
    unsigned char b64[4 * ((EVP_MAX_MD_SIZE + 2) / 3) + 1];
    int b64_len;

    fp = fopen(cert_file, "r");
    if (fp == NULL) {
        printf("[gw] cannot open cert file '%s'\n", cert_file);
        goto fail;
    }
    cert = PEM_read_X509(fp, NULL, NULL, NULL);
    fclose(fp);
    if (cert == NULL) {
        printf("[gw] cannot parse PEM certificate '%s'\n", cert_file);
        goto fail;
    }
    if (X509_digest(cert, EVP_sha256(), digest, &digest_len) != 1) {
        printf("[gw] X509_digest failed\n");
        X509_free(cert);
        goto fail;
    }
    X509_free(cert);

    b64_len = EVP_EncodeBlock(b64, digest, (int)digest_len);
    if (b64_len <= 0) {
        printf("[gw] base64 encode failed\n");
        goto fail;
    }
    b64[b64_len] = '\0';

    fp = fopen(key_file, "r");
    if (fp == NULL) {
        printf("[gw] cannot open key file '%s'\n", key_file);
        goto fail;
    }
    fclose(fp);

    printf("[gw] certificate fingerprint: sha-256/%s\n", b64);
    return 0;

fail:
    printf("[gw] --wt expects a TLS cert with a browser-trusted validity"
           " window (<= 14 days) and an ECDSA P-256 key; generate one:\n"
           "  openssl ecparam -name prime256v1 -genkey -noout -out %s\n"
           "  openssl req -new -x509 -key %s -out %s -days 14"
           " -subj '/CN=localhost'\n",
           key_file, key_file, cert_file);
    return -1;
}

static void
gw_usage(void)
{
    printf("usage: moq_gateway [options]\n"
           "  -p PORT        QUIC listen port (default %d)\n"
           "  -l LEVEL       log level e/w/i/s/d (default d)\n"
           "  -c b|c         congestion control bbr|cubic (default b)\n"
           "  -d RATE        artificial downstream drop rate per mille\n"
           "  -f             enable downstream QUIC FEC\n"
           "  --stream ID    SFU stream to bridge (repeatable, required)\n"
           "  --ipc PATH     SFU Frame IPC socket (default %s)\n"
           "  --codec C      H.264 codec string (default %s)\n"
           "  --width N --height N --fps N --bitrate BPS\n"
           "  --wt           also serve MoQ over WebTransport on ALPN h3\n"
           "  --wt-cert FILE TLS certificate for --wt (default %s)\n"
           "  --wt-key FILE  TLS key for --wt (default %s)\n",
           GW_DEFAULT_PORT, GW_DEFAULT_IPC_PATH, "avc1.42E01E",
           GW_DEFAULT_CERT_PATH, GW_DEFAULT_KEY_PATH);
}

int
main(int argc, char *argv[])
{
    char c_log_level = 'd';
    int ch;
    int port = GW_DEFAULT_PORT;
    xqc_cong_ctrl_callback_t cong_ctrl = xqc_bbr_cb;
    struct timeval stats_tv = { GW_STATS_INTERVAL_S, 0 };
    struct timeval retry_tv = { GW_RETRY_TICK_S, 0 };
    xqc_gw_ipc_cbs_t ipc_cbs;

    static const struct option long_opts[] = {
        { "stream",  required_argument, NULL, 's' },
        { "ipc",     required_argument, NULL, 'i' },
        { "codec",   required_argument, NULL, 'C' },
        { "width",   required_argument, NULL, 'W' },
        { "height",  required_argument, NULL, 'H' },
        { "fps",     required_argument, NULL, 'F' },
        { "bitrate", required_argument, NULL, 'B' },
        { "wt",      no_argument,       NULL, 1001 },
        { "wt-cert", required_argument, NULL, 1002 },
        { "wt-key",  required_argument, NULL, 1003 },
        { NULL, 0, NULL, 0 },
    };

    signal(SIGINT, gw_stop);
    signal(SIGTERM, gw_stop);

    while ((ch = getopt_long(argc, argv, "p:l:c:d:f", long_opts, NULL)) != -1) {
        switch (ch) {
        case 'p':
            port = atoi(optarg);
            break;
        case 'l':
            c_log_level = optarg[0];
            break;
        case 'c':
            switch (*optarg) {
            case 'b':
                cong_ctrl = xqc_bbr_cb;
                break;
            case 'c':
                cong_ctrl = xqc_cubic_cb;
                break;
#ifdef XQC_ENABLE_RENO
            case 'r':
                cong_ctrl = xqc_reno_cb;
                break;
#endif
            default:
                printf("[gw] unsupported cong_ctl '%s'\n", optarg);
                return -1;
            }
            break;
        case 'd':
            g_drop_rate = atoi(optarg);
            break;
        case 'f':
            g_fec_on = 1;
            break;
        case 's':
            if (g_stream_count >= GW_STREAMS_MAX) {
                printf("[gw] too many --stream options (max %d)\n",
                       GW_STREAMS_MAX);
                return -1;
            }
            xqc_gw_stream_init(&g_streams[g_stream_count], optarg);
            g_stream_count++;
            break;
        case 'i':
            snprintf(g_ipc_path, sizeof(g_ipc_path), "%s", optarg);
            break;
        case 'C':
            snprintf(g_video.codec, sizeof(g_video.codec), "%s", optarg);
            break;
        case 'W':
            g_video.width = atoi(optarg);
            break;
        case 'H':
            g_video.height = atoi(optarg);
            break;
        case 'F':
            g_video.fps = atoi(optarg);
            break;
        case 'B':
            g_video.bitrate = strtoull(optarg, NULL, 10);
            break;
        case 1001:
            g_wt_on = 1;
            break;
        case 1002:
            snprintf(g_cert_path, sizeof(g_cert_path), "%s", optarg);
            break;
        case 1003:
            snprintf(g_key_path, sizeof(g_key_path), "%s", optarg);
            break;
        default:
            gw_usage();
            return -1;
        }
    }
    if (g_stream_count == 0) {
        printf("[gw] at least one --stream is required\n");
        gw_usage();
        return -1;
    }
    if (g_wt_on && gw_print_cert_fingerprint(g_cert_path, g_key_path) != 0) {
        return -1;
    }

    memset(&ctx, 0, sizeof(ctx));
    xqc_app_open_log_file(&ctx, "./glog");
    xqc_platform_init_env();

    xqc_engine_ssl_config_t engine_ssl_config;
    memset(&engine_ssl_config, 0, sizeof(engine_ssl_config));
    engine_ssl_config.private_key_file = g_key_path;
    engine_ssl_config.cert_file = g_cert_path;
    engine_ssl_config.ciphers = XQC_TLS_CIPHERS;
    engine_ssl_config.groups = XQC_TLS_GROUPS;

    /* for 0RTT */
    char g_session_ticket_key[2048];
    char g_session_ticket_file[] = "session_ticket.key";
    int ticket_key_len = xqc_app_read_file_data(
        (unsigned char *)g_session_ticket_key, sizeof(g_session_ticket_key),
        g_session_ticket_file);
    if (ticket_key_len < 0) {
        engine_ssl_config.session_ticket_key_data = NULL;
        engine_ssl_config.session_ticket_key_len = 0;
    } else {
        engine_ssl_config.session_ticket_key_data = g_session_ticket_key;
        engine_ssl_config.session_ticket_key_len = ticket_key_len;
    }

    xqc_conn_settings_t conn_settings = {
        .cong_ctrl_callback = cong_ctrl,
        .cc_params = {
            .customize_on = 1,
            .bbr_ignore_app_limit = 1,
        },
        .fec_level = XQC_FEC_STREAM_LEVEL,
        .enable_encode_fec = g_fec_on,
        .enable_decode_fec = g_fec_on,
        .fec_params = {
            .fec_encoder_schemes[0] = XQC_PACKET_MASK_CODE,
            .fec_decoder_schemes[0] = XQC_PACKET_MASK_CODE,
            .fec_encoder_schemes_num = 1,
            .fec_decoder_schemes_num = 1,
            .fec_code_rate = 0.2
        }
    };
    if (g_wt_on) {
        /* WebTransport datagrams carry the MoQ datachannel */
        conn_settings.max_datagram_frame_size = 65535;
    }

    xqc_config_t config;
    if (xqc_engine_get_default_config(&config, XQC_ENGINE_SERVER) < 0) {
        return -1;
    }
    config.cid_len = GW_CID_LEN;
    if (g_wt_on) {
        config.enable_webtransport = 1;
        config.webtransport_max_sessions = GW_WT_MAX_SESSIONS;
    }
    xqc_app_set_log_level(c_log_level, &config);

    xqc_engine_callback_t callback = {
        .set_event_timer = xqc_app_set_event_timer,
        .log_callbacks = {
            .xqc_log_write_err = xqc_app_write_log,
            .xqc_log_write_stat = xqc_app_write_log,
        },
    };

    xqc_transport_callbacks_t tcbs = {
        .server_accept = gw_server_accept,
        .server_refuse = gw_server_refuse,
        .write_socket = xqc_app_write_socket,
        .write_socket_ex = xqc_app_write_socket_ex,
        .stateless_reset = gw_server_stateless_reset,
        .conn_closing = gw_server_conn_closing_notify,
    };

    ctx.engine = xqc_engine_create(XQC_ENGINE_SERVER, &config,
        &engine_ssl_config, &callback, &tcbs, &ctx);
    if (ctx.engine == NULL) {
        printf("[gw] create engine error\n");
        return -1;
    }

    xqc_server_set_conn_settings(ctx.engine, &conn_settings);

    xqc_conn_callbacks_t conn_cbs = {
        .conn_create_notify = gw_server_conn_create_notify,
        .conn_close_notify = gw_server_conn_close_notify,
        .conn_handshake_finished = gw_server_conn_handshake_finished,
    };
    xqc_moq_init_alpn(ctx.engine, &conn_cbs, XQC_MOQ_TRANSPORT_QUIC);

    if (g_wt_on) {
        xqc_h3_callbacks_t h3_cbs = {
            .h3c_cbs = {
                .h3_conn_create_notify = gw_h3_conn_create_notify,
                .h3_conn_close_notify = gw_h3_conn_close_notify,
                .h3_conn_handshake_finished = gw_h3_conn_handshake_finished,
            },
        };
        if (xqc_moq_init_alpn_h3(ctx.engine, &h3_cbs) != XQC_OK) {
            printf("[gw] register h3 ALPN error\n");
            return -1;
        }
    }

    if (xqc_gw_fanout_init(&g_fanout, GW_FANOUT_CAP, gw_write_video_shim) != 0) {
        printf("[gw] fanout init failed\n");
        return -1;
    }

    ctx.listen_fd = gw_server_create_socket(port);
    if (ctx.listen_fd < 0) {
        printf("[gw] create socket error\n");
        return -1;
    }

    eb = event_base_new();
    ctx.ev_engine = event_new(eb, -1, 0, xqc_app_engine_callback, &ctx);
    ctx.ev_socket = event_new(eb, ctx.listen_fd, EV_READ | EV_PERSIST,
        gw_server_socket_event_callback, &ctx);
    event_add(ctx.ev_socket, NULL);

    ipc_cbs.on_frame = gw_ipc_on_frame;
    ipc_cbs.on_sub_ok = gw_ipc_on_sub_ok;
    ipc_cbs.on_sub_err = gw_ipc_on_sub_err;
    ipc_cbs.on_state = gw_ipc_on_state;
    g_ipc = xqc_gw_ipc_create(eb, g_ipc_path, &ipc_cbs, NULL);
    if (g_ipc == NULL) {
        printf("[gw] ipc client create failed (path '%s')\n", g_ipc_path);
        return -1;
    }
    /* the first connect can succeed during create, before g_ipc was set */
    if (xqc_gw_ipc_connected(g_ipc)) {
        gw_pump_streams();
    }

    g_ev_stats = evtimer_new(eb, gw_stats_cb, NULL);
    g_ev_sub_retry = evtimer_new(eb, gw_sub_retry_cb, NULL);
    evtimer_add(g_ev_stats, &stats_tv);
    evtimer_add(g_ev_sub_retry, &retry_tv);

    printf("[gw] listening on port %d, %d stream(s), ipc '%s'%s\n",
           port, g_stream_count, g_ipc_path,
           g_wt_on ? ", webtransport on h3" : "");

    event_base_dispatch(eb);

    xqc_engine_destroy(ctx.engine);

    return 0;
}