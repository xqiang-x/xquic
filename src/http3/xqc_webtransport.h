/**
 * @copyright Copyright (c) 2022, Alibaba Group Holding Limited
 */

#ifndef _XQC_WEBTRANSPORT_H_INCLUDED_
#define _XQC_WEBTRANSPORT_H_INCLUDED_

/*
 * WebTransport over HTTP/3 session layer (draft-ietf-webtrans-http3-07).
 *
 * One xqc_wt_conn_t per h3 connection when the engine enables WebTransport;
 * it owns the session object list of the connection. A session is created
 * when a "webtransport" extended CONNECT request passes h3 admission, lives
 * through the OFFERED and ESTABLISHED states, and is torn down with its
 * backing h3 request.
 */

#include <xquic/xquic_typedef.h>
#include <xquic/xqc_http3.h>
#include <xquic/xqc_webtransport.h>
#include "src/http3/xqc_h3_defs.h"
#include "src/common/xqc_list.h"
#include "src/common/utils/var_buf/xqc_var_buf.h"
#include "src/common/utils/vint/xqc_discrete_int_parser.h"

/* session close capsule / error codes, draft-07 Sections 4.6, 5, 8.5 */
#define XQC_WT_CAPSULE_CLOSE_WEBTRANSPORT_SESSION   0x2843
#define XQC_WT_CAPSULE_DRAIN_WEBTRANSPORT_SESSION   0x78ae
#define XQC_WT_SESSION_GONE                         0x170d7b68

/* stream signals, draft-07 Sections 4.1, 4.2 */
#define XQC_WT_UNI_STREAM_TYPE                      0x54
#define XQC_WT_BIDI_SIGNAL_VALUE                    0x41

/* stream rejection before session establishment, draft-07 Section 8.5 */
#define XQC_WT_BUFFERED_STREAM_REJECTED             0x3994bd84

/* draft-07 Section 4.5: limits on streams and datagrams buffered per
   connection before their session is established */
#define XQC_WT_BUFFERED_STREAM_MAX                  4
#define XQC_WT_BUFFERED_DGRAM_MAX                   16

/* upper bound of the CLOSE_WEBTRANSPORT_SESSION error message */
#define XQC_WT_CLOSE_MSG_MAX                        1024

/* upper bound of datagram payloads buffered per session for the application */
#define XQC_WT_DGRAM_RECV_QUEUE_MAX                 64

/* upper bound of application bytes buffered on one stream send side */
#define XQC_WT_STREAM_SEND_BUF_MAX                  (1024 * 1024)

typedef enum {
    /* CONNECT request admitted by the h3 layer, awaiting app decision */
    XQC_WT_SESSION_OFFERED      = 0,

    /* 2xx response sent; the session is usable */
    XQC_WT_SESSION_ESTABLISHED  = 1,

    /* session unusable; object stays alive until the request is destroyed */
    XQC_WT_SESSION_TERMINATED   = 2,

} xqc_wt_session_state_t;

/* capsule protocol parser states of the CONNECT stream receive side
   (RFC 9297 Section 3.2: type, length, payload) */
typedef enum {
    XQC_WT_CAP_STATE_TYPE   = 0,    /* waiting for a capsule type */
    XQC_WT_CAP_STATE_LENGTH = 1,    /* waiting for the capsule length */
    XQC_WT_CAP_STATE_VALUE  = 2,    /* consuming the capsule payload */

} xqc_wt_cap_state_t;

typedef struct xqc_wt_conn_s {
    xqc_h3_conn_t          *h3c;
    xqc_wt_callbacks_t      callbacks;
    uint8_t                 callbacks_registered;

    /* sessions in OFFERED, ESTABLISHED or TERMINATED state, in arrival order */
    xqc_list_head_t         sessions;

    /* draft-07 Section 4.5: streams and datagrams that arrived before their
       session was established, in arrival order; per-connection caps above */
    xqc_list_head_t         buffered_streams;
    uint64_t                buffered_stream_count;
    xqc_list_head_t         buffered_dgrams;
    uint64_t                buffered_dgram_count;
} xqc_wt_conn_t;

/*
 * a datagram buffered before its session was established (draft-07
 * Section 4.5): the target session ID plus the payload with the quarter
 * stream ID already stripped
 */
typedef struct xqc_wt_buffered_dgram_s {
    xqc_list_head_t         list_head;          /* wtc->buffered_dgrams */
    uint64_t                stream_id;          /* target session ID */
    xqc_var_buf_t          *payload;
} xqc_wt_buffered_dgram_t;

struct xqc_wt_session_s {
    xqc_list_head_t         list_head;
    xqc_wt_conn_t          *wtc;
    xqc_h3_request_t       *h3r;                /* backing CONNECT request */
    void                   *user_data;          /* set by accept() */
    uint64_t                stream_id;          /* session ID = CONNECT stream ID */
    xqc_wt_session_state_t  state;
    uint8_t                 accept_pending;     /* accept() before client SETTINGS */
    uint8_t                 closed_notified;    /* session_closed_notify fired */

    /* established data streams, in creation order (draft-07 Section 5: all
       reset with XQC_WT_SESSION_GONE when the session is terminated) */
    xqc_list_head_t         streams;

    /* CLOSE_WEBTRANSPORT_SESSION capsule that hit EAGAIN, flushed when
       the CONNECT stream becomes writable */
    unsigned char          *close_cap_buf;
    size_t                  close_cap_len;
    size_t                  close_cap_sent;

    /* received datagram payloads (quarter stream ID stripped), waiting for
       xqc_wt_session_recv_datagram() */
    xqc_list_head_t         dgram_recv_buf;
    uint64_t                dgram_recv_count;

    /* capsule protocol state of the CONNECT stream receive side
       (RFC 9297 Section 3.2) */
    xqc_wt_cap_state_t      cap_state;
    xqc_discrete_int_pctx_t cap_int_pctx;
    uint64_t                cap_type;
    uint64_t                cap_len;
    uint64_t                cap_consumed;   /* payload bytes consumed so far */

    /* CLOSE_WEBTRANSPORT_SESSION payload captured so far: error code (4) +
       at most XQC_WT_CLOSE_MSG_MAX message bytes */
    unsigned char          *close_msg_buf;
    size_t                  close_msg_len;

    /* a CLOSE_WEBTRANSPORT_SESSION capsule was received; draft-07 Section 5:
       any stream data after it aborts the CONNECT stream */
    uint8_t                 close_received;
};

/*
 * one WebTransport data stream (draft-07 Sections 4.1/4.2). Created either
 * when the "signal + session ID" prefix of a peer-initiated stream is
 * recognized, or by xqc_wt_session_create_stream(). A stream whose session
 * is not established yet is buffered on wtc->buffered_streams (draft-07
 * Section 4.5) and attached on establishment or rejected on teardown. An
 * unattached stream is freed when its h3 stream is destroyed
 * (xqc_wt_stream_on_h3_stream_destroy); a bound stream is detached there
 * but stays allocated, and is freed when the session is destroyed or, if
 * the session is already gone, by the h3 stream destruction itself.
 */
typedef struct xqc_wt_stream_s {
    xqc_list_head_t         list_head;          /* session->streams or wtc->buffered_streams */
    xqc_wt_conn_t          *wtc;                /* owning connection layer */
    xqc_wt_session_t       *session;            /* NULL until bound (or once terminated) */
    xqc_h3_stream_t        *h3s;                /* NULL once the h3 stream is destroyed */
    void                   *user_data;
    uint64_t                stream_id;          /* QUIC stream ID when known */
    uint8_t                 bidi;
    uint8_t                 local;              /* created by xqc_wt_session_create_stream */
    uint8_t                 closed;             /* transport stream no longer usable */

    /* prefix parsing: session ID following the stream signal */
    uint8_t                 sid_done;
    xqc_discrete_int_pctx_t sid_pctx;

    /* draft-07 Section 4.5: waiting on wtc->buffered_streams for the
       session named by buffered_sid */
    uint8_t                 buffered;
    uint64_t                buffered_sid;

    /* receive side */
    xqc_list_head_t         recv_buf;           /* xqc_list_buf_t of received bytes */
    uint64_t                recv_buf_size;
    uint8_t                 recv_fin;           /* peer finished the stream */
    uint8_t                 fin_read;           /* FIN delivered to the application */
    uint8_t                 recv_reset;         /* stream died before a clean FIN */
    uint8_t                 opened_notified;    /* stream_opened_notify fired */

    /* send side */
    xqc_var_buf_t          *send_buf;           /* pending bytes, consumed_len consumed */
    uint8_t                 fin_requested;      /* application will send no more data */
    uint8_t                 fin_sent;           /* FIN handed to the transport */
} xqc_wt_stream_t;

/*
 * lifecycle of the per-connection WT layer, driven from xqc_h3_conn.c
 */
xqc_wt_conn_t *xqc_wt_conn_create(xqc_h3_conn_t *h3c);
void xqc_wt_conn_destroy(xqc_wt_conn_t *wtc);

/*
 * a "webtransport" extended CONNECT request passed h3 admission:
 * creates the OFFERED session and fires session_request_notify, or resets
 * the CONNECT stream when the session limit is exceeded (draft-07 Section 3.4)
 */
void xqc_wt_conn_on_request(xqc_h3_conn_t *h3c, xqc_h3_request_t *h3r);

/*
 * client SETTINGS frame processed: completes accepts deferred by
 * draft-07 Section 3.1
 */
void xqc_wt_conn_on_peer_settings(xqc_h3_conn_t *h3c);

/*
 * the CONNECT stream became writable: flush WT frames that hit EAGAIN
 */
xqc_int_t xqc_wt_conn_on_request_writable(xqc_h3_conn_t *h3c, xqc_h3_request_t *h3r);

/*
 * a QUIC datagram arrived on the connection (RFC 9297 Section 2.1): strips
 * the quarter stream ID and routes the payload. Returns XQC_OK when the
 * datagram was consumed by the WebTransport layer (delivered or silently
 * dropped), XQC_ERROR when it does not belong to a WebTransport session and
 * the caller should use the generic datagram path.
 */
xqc_int_t xqc_wt_conn_on_datagram(xqc_h3_conn_t *h3c, const unsigned char *data,
    size_t data_len);

/*
 * CONNECT stream DATA bytes carry the capsule protocol of the session
 * (RFC 9297 Section 3, draft-07 Sections 4.6/5): consumes all input.
 * Returns XQC_OK, or a negative error when the capsule protocol is
 * malformed (RFC 9297 Section 3.3: the caller then resets the CONNECT
 * stream with H3_MESSAGE_ERROR).
 */
xqc_int_t xqc_wt_session_process_capsules(xqc_wt_session_t *session,
    const unsigned char *data, size_t data_len, xqc_bool_t fin);

/*
 * the backing h3 request is destroyed: notify if needed and free the session
 */
void xqc_wt_session_on_request_closed(xqc_wt_session_t *session);

/*
 * heard the WebTransport bidi signal (0x41) as the first bytes of a peer
 * initiated bidirectional stream: switches the h3 stream type to the WT
 * data stream type; the session ID prefix and payload that follow are then
 * consumed by xqc_wt_stream_process_in()
 */
void xqc_wt_stream_on_bidi_signal(xqc_h3_stream_t *h3s);

/*
 * payload of a WebTransport data stream (bytes after "signal/session ID"),
 * consumes all of them, returns bytes consumed or a negative error
 */
ssize_t xqc_wt_stream_process_in(xqc_h3_stream_t *h3s, unsigned char *data,
    size_t data_len, xqc_bool_t fin);

/*
 * the QUIC stream became writable: flush pending stream bytes (called from
 * the h3 stream write notify hook)
 */
xqc_int_t xqc_wt_stream_flush(xqc_wt_stream_t *wts);

/*
 * an h3 stream was destroyed: unlink the WT stream and free it
 */
void xqc_wt_stream_on_h3_stream_destroy(xqc_wt_stream_t *wts);

/*
 * read notification support for xqc_h3_stream_read_notify()
 */
xqc_bool_t xqc_wt_stream_should_notify_read(xqc_wt_stream_t *wts);
xqc_int_t xqc_wt_stream_notify_read(xqc_wt_stream_t *wts);

#endif /* _XQC_WEBTRANSPORT_H_INCLUDED_ */