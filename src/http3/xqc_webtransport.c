/**
 * @copyright Copyright (c) 2022, Alibaba Group Holding Limited
 */

#include <stdio.h>
#include <string.h>
#include "src/http3/xqc_webtransport.h"
#include "src/http3/xqc_h3_conn.h"
#include "src/http3/xqc_h3_request.h"
#include "src/http3/xqc_h3_stream.h"
#include "src/transport/xqc_stream.h"
#include "src/transport/xqc_engine.h"
#include "src/common/utils/vint/xqc_variable_len_int.h"
#include <xquic/xqc_errno.h>

static void
xqc_wt_stream_free(xqc_wt_stream_t *wts);

static void
xqc_wt_conn_flush_buffered(xqc_wt_conn_t *wtc, xqc_wt_session_t *session);

static void
xqc_wt_session_fire_closed(xqc_wt_session_t *session)
{
    xqc_wt_conn_t *wtc = session->wtc;

    if (session->closed_notified) {
        return;
    }
    session->closed_notified = 1;

    if (wtc->callbacks_registered && wtc->callbacks.session_closed_notify) {
        wtc->callbacks.session_closed_notify(session, session->user_data);
    }
}

/*
 * draft-07 Section 5: upon learning that the session has been terminated,
 * the endpoint MUST reset the send side and abort reading on the receive
 * side of all streams associated with the session, using the
 * WEBTRANSPORT_SESSION_GONE error code.
 */
static void
xqc_wt_session_abort_streams(xqc_wt_session_t *session)
{
    xqc_list_head_t *pos, *next;
    xqc_wt_stream_t *wts;

    xqc_list_for_each_safe(pos, next, &session->streams) {
        wts = xqc_list_entry(pos, xqc_wt_stream_t, list_head);
        wts->closed = 1;
        if (wts->h3s && wts->h3s->stream) {
            xqc_stream_close_with_error(wts->h3s->stream, XQC_WT_SESSION_GONE);
        }
    }
}

/*
 * detach the streams still associated with the session; called right before
 * the session object itself is freed. A stream whose h3 stream is already
 * gone is freed here; one that still has a live h3 stream survives until
 * xqc_wt_stream_on_h3_stream_destroy frees it.
 */
static void
xqc_wt_session_free_streams(xqc_wt_session_t *session)
{
    xqc_list_head_t *pos, *next;
    xqc_wt_stream_t *wts;

    xqc_list_for_each_safe(pos, next, &session->streams) {
        wts = xqc_list_entry(pos, xqc_wt_stream_t, list_head);
        xqc_list_del_init(&wts->list_head);
        wts->session = NULL;
        wts->closed = 1;
        if (wts->h3s == NULL) {
            xqc_wt_stream_free(wts);
        }
    }
}

/*
 * free the receive-side buffers of the session (datagrams waiting for the
 * application and a partially captured close message)
 */
static void
xqc_wt_session_free_recv_state(xqc_wt_session_t *session)
{
    xqc_list_buf_list_free(&session->dgram_recv_buf);
    session->dgram_recv_count = 0;

    if (session->close_msg_buf) {
        xqc_free(session->close_msg_buf);
        session->close_msg_buf = NULL;
        session->close_msg_len = 0;
    }
}

static void
xqc_wt_session_terminate(xqc_wt_session_t *session)
{
    session->state = XQC_WT_SESSION_TERMINATED;
    xqc_wt_session_abort_streams(session);
    xqc_wt_conn_flush_buffered(session->wtc, session);
    xqc_wt_session_fire_closed(session);
}

/*
 * draft-07 Section 3.1: the extension is in effect when the client SETTINGS
 * advertise session support and HTTP/3 datagrams.
 */
static xqc_bool_t
xqc_wt_peer_ready(xqc_h3_conn_t *h3c)
{
    return (h3c->flags & XQC_H3_CONN_FLAG_SETTINGS_RECVED)
           && h3c->peer_h3_conn_settings.webtransport_max_sessions > 0
           && h3c->peer_h3_conn_settings.h3_datagram == 1;
}

static uint64_t
xqc_wt_live_session_count(xqc_wt_conn_t *wtc)
{
    uint64_t count = 0;
    xqc_list_head_t *pos, *next;
    xqc_wt_session_t *session;

    xqc_list_for_each_safe(pos, next, &wtc->sessions) {
        session = xqc_list_entry(pos, xqc_wt_session_t, list_head);
        if (session->state == XQC_WT_SESSION_OFFERED
            || session->state == XQC_WT_SESSION_ESTABLISHED)
        {
            count++;
        }
    }

    return count;
}

/*
 * send the CONNECT response header section carrying the given :status
 */
static ssize_t
xqc_wt_session_send_response(xqc_wt_session_t *session, int status, uint8_t fin)
{
    char                status_buf[8];
    xqc_http_header_t   hdr;
    xqc_http_headers_t  headers;
    int                 status_len;

    status_len = snprintf(status_buf, sizeof(status_buf), "%d", status);
    if (status_len <= 0 || (size_t)status_len >= sizeof(status_buf)) {
        return -XQC_EPARAM;
    }

    memset(&hdr, 0, sizeof(hdr));
    hdr.name.iov_base = (void *)":status";
    hdr.name.iov_len = 7;
    hdr.value.iov_base = status_buf;
    hdr.value.iov_len = status_len;

    memset(&headers, 0, sizeof(headers));
    headers.headers = &hdr;
    headers.count = 1;
    headers.capacity = 1;
    headers.total_len = 7 + status_len;

    return xqc_h3_request_send_headers(session->h3r, &headers, fin);
}

/*
 * refuse an OFFERED session with the given HTTP status (draft-07 Section 3.3)
 */
static xqc_int_t
xqc_wt_session_refuse(xqc_wt_session_t *session, int status)
{
    xqc_h3_conn_t *h3c = session->wtc->h3c;
    ssize_t        sent;

    if (session->state != XQC_WT_SESSION_OFFERED || session->h3r == NULL) {
        return -XQC_ESTATE;
    }

    sent = xqc_wt_session_send_response(session, status, 1);
    if (sent < 0) {
        xqc_log(h3c->log, XQC_LOG_ERROR,
                "|send webtransport refusal failed|conn:%p|stream_id:%ui|"
                "status:%d|ret:%z|", h3c->conn, session->stream_id, status, sent);
    } else {
        xqc_log(h3c->log, XQC_LOG_DEBUG,
                "|webtransport session refused|conn:%p|stream_id:%ui|status:%d|",
                h3c->conn, session->stream_id, status);
    }

    xqc_wt_session_terminate(session);
    return sent < 0 ? (xqc_int_t)sent : XQC_OK;
}

/*
 * establish the session: both-sides capability is given, send the 2xx
 * response (draft-07 Section 3.3: the server establishes the session when
 * it sends the 2xx response)
 */
static xqc_int_t
xqc_wt_session_do_accept(xqc_wt_session_t *session)
{
    xqc_wt_conn_t *wtc = session->wtc;
    xqc_h3_conn_t *h3c = wtc->h3c;
    ssize_t        sent;

    if (session->h3r == NULL) {
        return -XQC_ESTATE;
    }

    if (!xqc_wt_peer_ready(h3c)) {
        xqc_log(h3c->log, XQC_LOG_INFO,
                "|peer does not support webtransport, refusing with 404|"
                "conn:%p|stream_id:%ui|settings_recved:%ud|"
                "peer_max_sessions:%ui|peer_h3_datagram:%ui|",
                h3c->conn, session->stream_id,
                (unsigned int)((h3c->flags & XQC_H3_CONN_FLAG_SETTINGS_RECVED) != 0),
                h3c->peer_h3_conn_settings.webtransport_max_sessions,
                h3c->peer_h3_conn_settings.h3_datagram);
        return xqc_wt_session_refuse(session, 404);
    }

    sent = xqc_wt_session_send_response(session, 200, 0);
    if (sent < 0) {
        xqc_log(h3c->log, XQC_LOG_ERROR,
                "|send webtransport 200 failed|conn:%p|stream_id:%ui|ret:%z|",
                h3c->conn, session->stream_id, sent);
        xqc_wt_session_terminate(session);
        return (xqc_int_t)sent;
    }

    session->state = XQC_WT_SESSION_ESTABLISHED;
    xqc_log(h3c->log, XQC_LOG_INFO,
            "|webtransport session established|conn:%p|stream_id:%ui|",
            h3c->conn, session->stream_id);

    if (wtc->callbacks_registered && wtc->callbacks.session_accepted_notify) {
        wtc->callbacks.session_accepted_notify(session, session->user_data);
    }

    /* draft-07 Section 4.5: the session is established, deliver the streams
       and datagrams that were buffered waiting for it */
    xqc_wt_conn_flush_buffered(wtc, session);

    return XQC_OK;
}

xqc_wt_conn_t *
xqc_wt_conn_create(xqc_h3_conn_t *h3c)
{
    xqc_wt_conn_t *wtc = xqc_calloc(1, sizeof(xqc_wt_conn_t));
    if (wtc == NULL) {
        xqc_log(h3c->log, XQC_LOG_ERROR, "|xqc_calloc webtransport conn error|");
        return NULL;
    }

    wtc->h3c = h3c;
    xqc_init_list_head(&wtc->sessions);
    xqc_init_list_head(&wtc->buffered_streams);
    xqc_init_list_head(&wtc->buffered_dgrams);

    return wtc;
}

void
xqc_wt_conn_destroy(xqc_wt_conn_t *wtc)
{
    xqc_list_head_t  *pos, *next;
    xqc_wt_session_t *session;

    /*
     * request streams are destroyed before the h3 connection, so this loop
     * only covers sessions whose request never reached destruction
     */
    xqc_list_for_each_safe(pos, next, &wtc->sessions) {
        session = xqc_list_entry(pos, xqc_wt_session_t, list_head);
        xqc_log(wtc->h3c->log, XQC_LOG_WARN,
                "|webtransport session alive at conn destroy|conn:%p|"
                "stream_id:%ui|state:%d|", wtc->h3c->conn, session->stream_id,
                session->state);
        session->h3r = NULL;
        xqc_wt_session_terminate(session);
        xqc_list_del_init(&session->list_head);
        xqc_wt_session_free_streams(session);
        xqc_wt_session_free_recv_state(session);
        if (session->close_cap_buf) {
            xqc_free(session->close_cap_buf);
        }
        xqc_free(session);
    }

    /*
     * draft-07 Section 4.5: streams and datagrams that never found their
     * session are rejected (WEBTRANSPORT_BUFFERED_STREAM_REJECTED) and
     * dropped when the connection closes
     */
    xqc_wt_conn_flush_buffered(wtc, NULL);

    xqc_free(wtc);
}

void
xqc_wt_conn_on_request(xqc_h3_conn_t *h3c, xqc_h3_request_t *h3r)
{
    xqc_wt_conn_t   *wtc = h3c->wt;
    xqc_wt_session_t *session;
    uint64_t         limit;
    xqc_int_t        ret;

    if (wtc == NULL || !wtc->callbacks_registered
        || wtc->callbacks.session_request_notify == NULL)
    {
        /*
         * no WebTransport server on this connection: leave the request to
         * the regular application request path
         */
        return;
    }

    limit = h3c->local_h3_conn_settings.webtransport_max_sessions;
    if (xqc_wt_live_session_count(wtc) >= limit) {
        /*
         * draft-07 Section 3.4: reset the CONNECT stream the server is not
         * willing to process; the connection stays open
         */
        xqc_log(h3c->log, XQC_LOG_INFO,
                "|webtransport session limit reached, resetting CONNECT stream|"
                "conn:%p|stream_id:%ui|limit:%ui|", h3c->conn,
                h3r->h3_stream->stream_id, limit);
        xqc_stream_close_with_error(h3r->h3_stream->stream, H3_REQUEST_REJECTED);
        h3r->wt_flags |= XQC_H3_WT_REQ_FLAG_INTERNAL_RESET;
        return;
    }

    session = xqc_calloc(1, sizeof(xqc_wt_session_t));
    if (session == NULL) {
        xqc_log(h3c->log, XQC_LOG_ERROR,
                "|xqc_calloc webtransport session error|conn:%p|stream_id:%ui|",
                h3c->conn, h3r->h3_stream->stream_id);
        xqc_stream_close_with_error(h3r->h3_stream->stream, H3_INTERNAL_ERROR);
        h3r->wt_flags |= XQC_H3_WT_REQ_FLAG_INTERNAL_RESET;
        return;
    }

    session->wtc = wtc;
    session->h3r = h3r;
    session->stream_id = h3r->h3_stream->stream_id;
    session->state = XQC_WT_SESSION_OFFERED;
    xqc_init_list_head(&session->streams);
    xqc_init_list_head(&session->dgram_recv_buf);
    xqc_list_add_tail(&session->list_head, &wtc->sessions);
    h3r->wt_session = session;

    xqc_log(h3c->log, XQC_LOG_DEBUG,
            "|webtransport session offered|conn:%p|stream_id:%ui|",
            h3c->conn, session->stream_id);

    ret = wtc->callbacks.session_request_notify(session, h3c->user_data);
    if (ret != XQC_OK && session->state == XQC_WT_SESSION_OFFERED) {
        xqc_log(h3c->log, XQC_LOG_INFO,
                "|session_request_notify error, refusing with 404|"
                "conn:%p|stream_id:%ui|ret:%d|", h3c->conn, session->stream_id,
                ret);
        xqc_wt_session_refuse(session, 404);
    }
}

void
xqc_wt_conn_on_peer_settings(xqc_h3_conn_t *h3c)
{
    xqc_wt_conn_t   *wtc = h3c->wt;
    xqc_list_head_t *pos, *next;
    xqc_wt_session_t *session;
    uint64_t         pending = 0;

    if (wtc == NULL) {
        return;
    }

    xqc_list_for_each_safe(pos, next, &wtc->sessions) {
        session = xqc_list_entry(pos, xqc_wt_session_t, list_head);
        if (session->state == XQC_WT_SESSION_OFFERED && session->accept_pending) {
            pending++;
        }
    }

    if (pending == 0) {
        return;
    }

    xqc_log(h3c->log, XQC_LOG_DEBUG,
            "|webtransport completing deferred accepts|conn:%p|count:%ui|",
            h3c->conn, pending);

    xqc_list_for_each_safe(pos, next, &wtc->sessions) {
        session = xqc_list_entry(pos, xqc_wt_session_t, list_head);
        if (session->state == XQC_WT_SESSION_OFFERED && session->accept_pending) {
            session->accept_pending = 0;
            xqc_wt_session_do_accept(session);
        }
    }
}

xqc_int_t
xqc_wt_conn_on_request_writable(xqc_h3_conn_t *h3c, xqc_h3_request_t *h3r)
{
    xqc_wt_session_t *session = h3r->wt_session;
    ssize_t           sent;
    size_t            remaining;

    if (session == NULL || session->close_cap_buf == NULL) {
        return XQC_OK;
    }

    while (session->close_cap_sent < session->close_cap_len) {
        remaining = session->close_cap_len - session->close_cap_sent;
        sent = xqc_h3_request_send_body(session->h3r,
                   session->close_cap_buf + session->close_cap_sent,
                   remaining, 1);
        if (sent == -XQC_EAGAIN) {
            return XQC_OK;
        }

        if (sent < 0) {
            xqc_log(h3c->log, XQC_LOG_ERROR,
                    "|flush webtransport close capsule failed|conn:%p|"
                    "stream_id:%ui|ret:%z|", h3c->conn, session->stream_id, sent);
            xqc_free(session->close_cap_buf);
            session->close_cap_buf = NULL;
            return XQC_OK;
        }

        session->close_cap_sent += sent;
        if (sent == 0) {
            /* no progress, avoid spinning */
            return XQC_OK;
        }
    }

    xqc_log(h3c->log, XQC_LOG_DEBUG,
            "|webtransport close capsule flushed|conn:%p|stream_id:%ui|",
            h3c->conn, session->stream_id);
    xqc_free(session->close_cap_buf);
    session->close_cap_buf = NULL;
    return XQC_OK;
}

void
xqc_wt_session_on_request_closed(xqc_wt_session_t *session)
{
    session->h3r = NULL;
    xqc_wt_session_terminate(session);

    xqc_list_del_init(&session->list_head);
    xqc_wt_session_free_streams(session);
    xqc_wt_session_free_recv_state(session);
    if (session->close_cap_buf) {
        xqc_free(session->close_cap_buf);
    }
    xqc_free(session);
}

xqc_int_t
xqc_h3_conn_set_webtransport_callbacks(xqc_h3_conn_t *h3c, xqc_wt_callbacks_t *cbs)
{
    if (h3c == NULL || cbs == NULL) {
        return -XQC_EPARAM;
    }

    if (h3c->wt == NULL) {
        xqc_log(h3c->log, XQC_LOG_ERROR,
                "|webtransport not enabled on h3 connection|conn:%p|",
                h3c->conn);
        return -XQC_ESTATE;
    }

    h3c->wt->callbacks = *cbs;
    h3c->wt->callbacks_registered = 1;

    xqc_log(h3c->log, XQC_LOG_DEBUG,
            "|webtransport callbacks registered|conn:%p|", h3c->conn);

    return XQC_OK;
}

xqc_int_t
xqc_wt_session_accept(xqc_wt_session_t *session, void *user_data)
{
    if (session == NULL) {
        return -XQC_EPARAM;
    }

    if (session->state != XQC_WT_SESSION_OFFERED || session->h3r == NULL) {
        return -XQC_ESTATE;
    }

    session->user_data = user_data;

    if (!(session->wtc->h3c->flags & XQC_H3_CONN_FLAG_SETTINGS_RECVED)) {
        /*
         * draft-07 Section 3.1: the server MUST NOT establish sessions
         * before the client SETTINGS have been received; defer the accept
         */
        session->accept_pending = 1;
        xqc_log(session->wtc->h3c->log, XQC_LOG_DEBUG,
                "|webtransport accept deferred until client settings|"
                "conn:%p|stream_id:%ui|", session->wtc->h3c->conn,
                session->stream_id);
        return XQC_OK;
    }

    return xqc_wt_session_do_accept(session);
}

xqc_int_t
xqc_wt_session_reject(xqc_wt_session_t *session, int status)
{
    if (session == NULL) {
        return -XQC_EPARAM;
    }

    if (status < 100 || status > 599) {
        return -XQC_EPARAM;
    }

    return xqc_wt_session_refuse(session, status);
}

xqc_int_t
xqc_wt_session_close(xqc_wt_session_t *session, uint32_t error_code,
    const char *error_msg)
{
    xqc_h3_conn_t *h3c;
    unsigned char  cap_buf[2 * 8 + 4 + XQC_WT_CLOSE_MSG_MAX];
    unsigned char *p = cap_buf;
    size_t         msg_len = 0;
    size_t         cap_len;
    ssize_t        sent;

    if (session == NULL) {
        return -XQC_EPARAM;
    }

    if (session->state != XQC_WT_SESSION_ESTABLISHED || session->h3r == NULL) {
        return -XQC_ESTATE;
    }

    h3c = session->wtc->h3c;

    if (error_msg) {
        msg_len = strlen(error_msg);
        if (msg_len > XQC_WT_CLOSE_MSG_MAX) {
            xqc_log(h3c->log, XQC_LOG_WARN,
                    "|webtransport close message truncated|conn:%p|"
                    "stream_id:%ui|len:%uz|", h3c->conn, session->stream_id,
                    msg_len);
            msg_len = XQC_WT_CLOSE_MSG_MAX;
        }
    }

    /* CLOSE_WEBTRANSPORT_SESSION capsule, draft-07 Section 5 */
    p = xqc_put_varint(p, XQC_WT_CAPSULE_CLOSE_WEBTRANSPORT_SESSION);
    p = xqc_put_varint(p, 4 + msg_len);
    p = xqc_put_uint32be(p, error_code);
    if (msg_len) {
        p = xqc_cpymem(p, error_msg, msg_len);
    }
    cap_len = p - cap_buf;

    /* a session is terminated once the close capsule is sent (Section 5) */
    xqc_log(h3c->log, XQC_LOG_INFO,
            "|webtransport session closing|conn:%p|stream_id:%ui|"
            "error_code:%ui|msg_len:%uz|", h3c->conn, session->stream_id,
            error_code, msg_len);
    xqc_wt_session_terminate(session);

    sent = xqc_h3_request_send_body(session->h3r, cap_buf, cap_len, 1);
    if (sent == -XQC_EAGAIN || (sent >= 0 && (size_t)sent < cap_len)) {
        /*
         * keep the unsent remainder and flush it when the CONNECT stream
         * becomes writable (xqc_wt_conn_on_request_writable)
         */
        size_t off = sent > 0 ? (size_t)sent : 0;
        size_t remain = cap_len - off;

        session->close_cap_buf = xqc_malloc(remain);
        if (session->close_cap_buf == NULL) {
            xqc_log(h3c->log, XQC_LOG_ERROR,
                    "|xqc_malloc close capsule retry buf error|conn:%p|"
                    "stream_id:%ui|", h3c->conn, session->stream_id);
            return -XQC_EMALLOC;
        }
        memcpy(session->close_cap_buf, cap_buf + off, remain);
        session->close_cap_len = remain;
        session->close_cap_sent = 0;

        xqc_log(h3c->log, XQC_LOG_DEBUG,
                "|webtransport close capsule pending|conn:%p|stream_id:%ui|"
                "pending:%uz|", h3c->conn, session->stream_id, remain);
        return XQC_OK;
    }

    if (sent < 0) {
        xqc_log(h3c->log, XQC_LOG_ERROR,
                "|send webtransport close capsule failed|conn:%p|"
                "stream_id:%ui|ret:%z|", h3c->conn, session->stream_id, sent);
        return (xqc_int_t)sent;
    }

    return XQC_OK;
}

void *
xqc_wt_session_get_user_data(xqc_wt_session_t *session)
{
    return session ? session->user_data : NULL;
}

uint64_t
xqc_wt_session_get_stream_id(xqc_wt_session_t *session)
{
    return session ? session->stream_id : 0;
}

xqc_connection_t *
xqc_wt_session_get_quic_conn(xqc_wt_session_t *session)
{
    return session ? session->wtc->h3c->conn : NULL;
}

xqc_wt_session_t *
xqc_h3_request_get_wt_session(xqc_h3_request_t *h3_request)
{
    return h3_request ? h3_request->wt_session : NULL;
}

/*
 * WebTransport data streams (draft-ietf-webtrans-http3-07 Sections 4.1/4.2).
 *
 * A peer-initiated stream is recognized from its "signal + session ID"
 * prefix, in xqc_wt_stream_on_bidi_signal() (bidi, Section 4.2) or in
 * xqc_wt_stream_process_in() on the first bytes of a uni stream whose type
 * is the WebTransport uni stream type (Section 4.1). Locally opened streams
 * go through xqc_wt_session_create_stream(), which writes the same prefix.
 */

static xqc_wt_stream_t *
xqc_wt_stream_alloc(xqc_h3_stream_t *h3s, uint8_t bidi, uint8_t local, void *user_data)
{
    xqc_wt_stream_t *wts = xqc_calloc(1, sizeof(xqc_wt_stream_t));
    if (wts == NULL) {
        return NULL;
    }

    wts->h3s = h3s;
    wts->wtc = h3s->h3c->wt;
    wts->stream_id = h3s->stream_id;
    wts->bidi = bidi;
    wts->local = local;
    wts->user_data = user_data;
    xqc_init_list_head(&wts->list_head);
    xqc_init_list_head(&wts->recv_buf);
    xqc_discrete_int_pctx_clear(&wts->sid_pctx);

    h3s->wt_stream = wts;
    return wts;
}

static void
xqc_wt_stream_free(xqc_wt_stream_t *wts)
{
    xqc_list_buf_list_free(&wts->recv_buf);
    if (wts->send_buf) {
        xqc_var_buf_free(wts->send_buf);
    }
    xqc_free(wts);
}

static xqc_wt_session_t *
xqc_wt_session_find_by_stream_id(xqc_wt_conn_t *wtc, uint64_t stream_id)
{
    xqc_list_head_t  *pos, *next;
    xqc_wt_session_t *session;

    xqc_list_for_each_safe(pos, next, &wtc->sessions) {
        session = xqc_list_entry(pos, xqc_wt_session_t, list_head);
        if (session->stream_id == stream_id) {
            return session;
        }
    }

    return NULL;
}

/*
 * attach a data stream to an established session: from this point on the
 * stream lives on session->streams and the application sees it
 */
static void
xqc_wt_stream_attach(xqc_wt_stream_t *wts, xqc_wt_session_t *session)
{
    xqc_wt_conn_t *wtc = session->wtc;

    wts->session = session;
    wts->buffered = 0;
    xqc_list_add_tail(&wts->list_head, &session->streams);

    xqc_log(wtc->h3c->log, XQC_LOG_DEBUG,
            "|webtransport stream bound|conn:%p|stream_id:%ui|session_id:%ui|"
            "bidi:%d|local:%d|", wtc->h3c->conn, wts->stream_id,
            session->stream_id, wts->bidi, wts->local);

    if (!wts->opened_notified) {
        wts->opened_notified = 1;
        if (wtc->callbacks_registered && wtc->callbacks.stream_opened_notify) {
            wtc->callbacks.stream_opened_notify(session, wts, session->user_data);
        }
    }

    /* bytes that arrived while the stream was buffered (draft-07 Section 4.5)
       are already in recv_buf and need a read notification */
    if (xqc_wt_stream_should_notify_read(wts)) {
        xqc_wt_stream_notify_read(wts);
    }
}

/*
 * bind a peer-initiated stream to the session named by its prefix.
 * draft-07 Section 4.5: a stream whose session is not established yet is
 * buffered until it is (or rejected on teardown); a stream for a session
 * that is already gone is reset with WEBTRANSPORT_SESSION_GONE (Section 5).
 */
static void
xqc_wt_stream_bind_session(xqc_wt_stream_t *wts, uint64_t sid)
{
    xqc_wt_conn_t    *wtc = wts->wtc;
    xqc_wt_session_t *session = xqc_wt_session_find_by_stream_id(wtc, sid);

    if (session != NULL && session->state == XQC_WT_SESSION_ESTABLISHED) {
        xqc_wt_stream_attach(wts, session);
        return;
    }

    if (session != NULL && session->state == XQC_WT_SESSION_TERMINATED) {
        /*
         * draft-07 Section 5: the session died before the peer learned about
         * it; the stream is of no use to the application, reset it so the
         * peer stops sending
         */
        xqc_log(wtc->h3c->log, XQC_LOG_INFO,
                "|webtransport stream for terminated session, resetting|"
                "conn:%p|stream_id:%ui|session_id:%ui|",
                wtc->h3c->conn, wts->stream_id, sid);
        wts->closed = 1;
        if (wts->h3s && wts->h3s->stream) {
            xqc_stream_close_with_error(wts->h3s->stream, XQC_WT_SESSION_GONE);
        }
        return;
    }

    /*
     * draft-07 Section 4.5: the session is not established yet (missing or
     * still OFFERED); buffer the stream, bounded per connection
     */
    if (wtc->buffered_stream_count >= XQC_WT_BUFFERED_STREAM_MAX) {
        xqc_log(wtc->h3c->log, XQC_LOG_INFO,
                "|webtransport buffered stream rejected|conn:%p|stream_id:%ui|"
                "session_id:%ui|limit:%d|", wtc->h3c->conn, wts->stream_id, sid,
                XQC_WT_BUFFERED_STREAM_MAX);
        wts->closed = 1;
        if (wts->h3s && wts->h3s->stream) {
            xqc_stream_close_with_error(wts->h3s->stream,
                                        XQC_WT_BUFFERED_STREAM_REJECTED);
        }
        return;
    }

    wts->buffered = 1;
    wts->buffered_sid = sid;
    xqc_list_add_tail(&wts->list_head, &wtc->buffered_streams);
    wtc->buffered_stream_count++;

    xqc_log(wtc->h3c->log, XQC_LOG_DEBUG,
            "|webtransport stream buffered|conn:%p|stream_id:%ui|"
            "session_id:%ui|count:%ui|", wtc->h3c->conn, wts->stream_id, sid,
            wtc->buffered_stream_count);
}

/*
 * draft-07 Section 4.5: flush the buffered streams and datagrams. With an
 * established session the matching items are attached/delivered; with a
 * session that did not make it (or none at all, connection closing) streams
 * are rejected with WEBTRANSPORT_BUFFERED_STREAM_REJECTED and datagrams are
 * dropped.
 */
static void
xqc_wt_conn_flush_buffered(xqc_wt_conn_t *wtc, xqc_wt_session_t *session)
{
    xqc_list_head_t         *pos, *next;
    xqc_wt_stream_t         *wts;
    xqc_wt_buffered_dgram_t *bdg;
    xqc_stream_t            *stream;
    uint64_t                 sid = session ? session->stream_id : 0;
    uint32_t                 delivered = 0;
    xqc_bool_t               attach_to = session != NULL
                             && session->state == XQC_WT_SESSION_ESTABLISHED;

    xqc_list_for_each_safe(pos, next, &wtc->buffered_streams) {
        wts = xqc_list_entry(pos, xqc_wt_stream_t, list_head);
        if (session != NULL && wts->buffered_sid != sid) {
            continue;
        }

        xqc_list_del_init(&wts->list_head);
        wtc->buffered_stream_count--;
        wts->buffered = 0;

        if (attach_to) {
            xqc_wt_stream_attach(wts, session);
            continue;
        }

        xqc_log(wtc->h3c->log, XQC_LOG_INFO,
                "|webtransport buffered stream rejected|conn:%p|stream_id:%ui|"
                "session_id:%ui|", wtc->h3c->conn, wts->stream_id,
                wts->buffered_sid);
        stream = (wts->h3s != NULL) ? wts->h3s->stream : NULL;
        wts->closed = 1;
        if (session == NULL && wts->h3s != NULL) {
            /*
             * connection teardown: the h3 destroy hook will not run for
             * this stream, take it down here
             */
            wts->h3s->wt_stream = NULL;
            wts->h3s = NULL;
            xqc_wt_stream_free(wts);
        }
        if (stream != NULL) {
            xqc_stream_close_with_error(stream, XQC_WT_BUFFERED_STREAM_REJECTED);
        }
    }

    xqc_list_for_each_safe(pos, next, &wtc->buffered_dgrams) {
        bdg = xqc_list_entry(pos, xqc_wt_buffered_dgram_t, list_head);
        if (session != NULL && bdg->stream_id != sid) {
            continue;
        }

        xqc_list_del_init(&bdg->list_head);
        wtc->buffered_dgram_count--;

        if (attach_to
            && xqc_list_buf_to_tail(&session->dgram_recv_buf, bdg->payload)
               == XQC_OK)
        {
            session->dgram_recv_count++;
            delivered++;
        } else {
            xqc_var_buf_free(bdg->payload);
        }
        xqc_free(bdg);
    }

    if (delivered > 0 && wtc->callbacks_registered
        && wtc->callbacks.datagram_read_notify)
    {
        wtc->callbacks.datagram_read_notify(session, session->user_data);
    }
}

static xqc_int_t
xqc_wt_stream_save_recv_data(xqc_wt_stream_t *wts, const unsigned char *data,
    size_t data_len)
{
    xqc_var_buf_t *buf;
    xqc_int_t      ret;

    buf = xqc_var_buf_create(xqc_max(data_len, (size_t)XQC_VAR_BUF_INIT_SIZE));
    if (buf == NULL) {
        return -XQC_EMALLOC;
    }

    ret = xqc_var_buf_save_data(buf, data, data_len);
    if (ret != XQC_OK) {
        xqc_var_buf_free(buf);
        return ret;
    }

    ret = xqc_list_buf_to_tail(&wts->recv_buf, buf);
    if (ret != XQC_OK) {
        xqc_var_buf_free(buf);
        return ret;
    }

    wts->recv_buf_size += data_len;
    return XQC_OK;
}

ssize_t
xqc_wt_stream_recv(xqc_wt_stream_t *wts, unsigned char *buf, size_t buf_size,
    uint8_t *fin)
{
    xqc_list_head_t *pos, *next;
    xqc_list_buf_t  *list_buf;
    xqc_var_buf_t   *vbuf;
    ssize_t          n_recv = 0;
    size_t           avail, copy;

    if (wts == NULL || buf == NULL || fin == NULL) {
        return -XQC_EPARAM;
    }

    *fin = 0;

    xqc_list_for_each_safe(pos, next, &wts->recv_buf) {
        list_buf = xqc_list_entry(pos, xqc_list_buf_t, list_head);
        vbuf = list_buf->buf;

        avail = vbuf->data_len - vbuf->consumed_len;
        copy = xqc_min(avail, buf_size - n_recv);
        xqc_memcpy(buf + n_recv, vbuf->data + vbuf->consumed_len, copy);
        vbuf->consumed_len += copy;
        wts->recv_buf_size -= copy;
        n_recv += copy;

        if (vbuf->consumed_len == vbuf->data_len) {
            xqc_list_buf_free(list_buf);
        }

        if ((size_t)n_recv == buf_size) {
            break;
        }
    }

    /* a FIN is delivered only after all buffered data has been read */
    if (wts->recv_fin && !wts->fin_read && xqc_list_empty(&wts->recv_buf)) {
        *fin = 1;
        wts->fin_read = 1;
    }

    if (n_recv == 0 && !*fin) {
        if (wts->recv_reset) {
            return -XQC_ESTREAM_RESET;
        }
        if (wts->closed && !wts->recv_fin) {
            return -XQC_ESTATE;
        }
        return -XQC_EAGAIN;
    }

    xqc_log(wts->wtc->h3c->log, XQC_LOG_DEBUG,
            "|webtransport stream recv|conn:%p|stream_id:%ui|"
            "n_recv:%z|buf_size:%uz|buffered:%ui|fin:%d|",
            wts->wtc->h3c->conn, wts->stream_id, n_recv, buf_size,
            wts->recv_buf_size, *fin);

    return n_recv;
}

static xqc_int_t
xqc_wt_stream_send_fin(xqc_wt_stream_t *wts)
{
    xqc_connection_t *conn;
    ssize_t           sent;

    conn = wts->h3s->stream->stream_conn;
    sent = xqc_stream_send(wts->h3s->stream, NULL, 0, 1);
    if (sent == -XQC_EAGAIN) {
        /* retried when the stream becomes writable */
        return XQC_OK;
    }

    if (sent < 0) {
        xqc_log(wts->wtc->h3c->log, XQC_LOG_ERROR,
                "|webtransport stream fin send failed|conn:%p|stream_id:%ui|"
                "ret:%z|", conn, wts->stream_id, sent);
        return (xqc_int_t)sent;
    }

    wts->fin_sent = 1;
    xqc_engine_conn_logic(conn->engine, conn);
    return XQC_OK;
}

xqc_int_t
xqc_wt_stream_flush(xqc_wt_stream_t *wts)
{
    xqc_connection_t *conn;
    xqc_var_buf_t    *buf;
    size_t            remaining;
    ssize_t           sent;

    if (wts == NULL || wts->closed || wts->fin_sent) {
        return XQC_OK;
    }

    if (wts->h3s == NULL || wts->h3s->stream == NULL) {
        return XQC_OK;
    }

    buf = wts->send_buf;
    remaining = buf ? buf->data_len - buf->consumed_len : 0;

    if (remaining == 0) {
        if (wts->fin_requested) {
            return xqc_wt_stream_send_fin(wts);
        }
        return XQC_OK;
    }

    conn = wts->h3s->stream->stream_conn;
    sent = xqc_stream_send(wts->h3s->stream, buf->data + buf->consumed_len,
                           remaining, wts->fin_requested ? 1 : 0);
    if (sent == -XQC_EAGAIN) {
        /* retried when the stream becomes writable */
        return XQC_OK;
    }

    if (sent < 0) {
        xqc_log(wts->wtc->h3c->log, XQC_LOG_ERROR,
                "|webtransport stream send failed|conn:%p|stream_id:%ui|"
                "ret:%z|", conn, wts->stream_id, sent);
        return (xqc_int_t)sent;
    }

    if (sent == 0) {
        return -XQC_ESTATE;
    }

    buf->consumed_len += sent;
    if (buf->consumed_len == buf->data_len) {
        /*
         * the FIN rides on the final data write only when that write carried
         * the whole remaining payload, which it did: nothing is left
         */
        if (wts->fin_requested) {
            wts->fin_sent = 1;
        }
        xqc_var_buf_clear(buf);
    }

    xqc_engine_conn_logic(conn->engine, conn);
    return XQC_OK;
}

ssize_t
xqc_wt_stream_send(xqc_wt_stream_t *wts, const unsigned char *data,
    size_t data_len, uint8_t fin)
{
    size_t    pending, space, accept;
    xqc_int_t ret;

    if (wts == NULL || (data == NULL && data_len > 0)) {
        return -XQC_EPARAM;
    }

    if (wts->closed || wts->h3s == NULL || wts->session == NULL
        || wts->fin_requested || wts->fin_sent)
    {
        return -XQC_ESTATE;
    }

    ret = xqc_wt_stream_flush(wts);
    if (ret != XQC_OK) {
        return ret;
    }

    if (data_len == 0) {
        if (fin) {
            wts->fin_requested = 1;
            ret = xqc_wt_stream_flush(wts);
            if (ret != XQC_OK) {
                return ret;
            }
        }
        return 0;
    }

    pending = wts->send_buf
              ? wts->send_buf->data_len - wts->send_buf->consumed_len : 0;
    space = pending < XQC_WT_STREAM_SEND_BUF_MAX
            ? XQC_WT_STREAM_SEND_BUF_MAX - pending : 0;
    accept = xqc_min(data_len, space);
    if (accept == 0) {
        return -XQC_EAGAIN;
    }

    if (wts->send_buf == NULL) {
        wts->send_buf = xqc_var_buf_create(xqc_max(accept,
                                  (size_t)XQC_VAR_BUF_INIT_SIZE));
        if (wts->send_buf == NULL) {
            return -XQC_EMALLOC;
        }
    }

    ret = xqc_var_buf_save_data(wts->send_buf, data, accept);
    if (ret != XQC_OK) {
        return ret;
    }

    if (accept == data_len && fin) {
        wts->fin_requested = 1;
    }

    ret = xqc_wt_stream_flush(wts);
    if (ret != XQC_OK) {
        xqc_log(wts->wtc->h3c->log, XQC_LOG_ERROR,
                "|webtransport stream flush error|conn:%p|stream_id:%ui|"
                "ret:%d|", wts->wtc->h3c->conn, wts->stream_id, ret);
    }

    return (ssize_t)accept;
}

xqc_int_t
xqc_wt_stream_close(xqc_wt_stream_t *wts)
{
    if (wts == NULL) {
        return -XQC_EPARAM;
    }

    if (wts->closed || wts->h3s == NULL || wts->h3s->stream == NULL) {
        wts->closed = 1;
        return XQC_OK;
    }

    wts->closed = 1;
    return xqc_stream_close(wts->h3s->stream);
}

ssize_t
xqc_wt_stream_process_in(xqc_h3_stream_t *h3s, unsigned char *data,
    size_t data_len, xqc_bool_t fin)
{
    xqc_wt_stream_t *wts = h3s->wt_stream;
    xqc_bool_t       sid_fin = XQC_FALSE;
    uint64_t         sid;
    size_t           total = data_len;
    ssize_t          consumed;
    xqc_int_t        ret;

    if (wts == NULL) {
        wts = xqc_wt_stream_alloc(h3s,
                xqc_stream_is_uni(h3s->stream_id) ? 0 : 1, 0, NULL);
        if (wts == NULL) {
            xqc_log(h3s->log, XQC_LOG_ERROR,
                    "|xqc_wt_stream_alloc error|stream_id:%ui|", h3s->stream_id);
            XQC_H3_CONN_ERR(h3s->h3c, H3_INTERNAL_ERROR, -XQC_H3_EMALLOC);
            return -XQC_H3_EMALLOC;
        }
    }

    if (!wts->sid_done) {
        consumed = xqc_discrete_vint_parse(data, data_len, &wts->sid_pctx,
                                           &sid_fin);
        data += consumed;
        data_len -= consumed;

        if (!sid_fin) {
            /*
             * the stream is still inside the prefix; a FIN here means the
             * peer finished before the session ID was complete
             */
            if (fin) {
                xqc_log(h3s->log, XQC_LOG_INFO,
                        "|webtransport stream finished before session id|"
                        "stream_id:%ui|", h3s->stream_id);
                wts->recv_reset = 1;
            }
            return (ssize_t)total;
        }

        wts->sid_done = 1;
        sid = wts->sid_pctx.vi;

        if ((sid & 0x3) != 0) {
            /*
             * draft-07 Section 4: a session ID that cannot be a
             * client-initiated bidirectional stream ID is a connection
             * error of type H3_ID_ERROR
             */
            xqc_log(h3s->log, XQC_LOG_ERROR,
                    "|webtransport invalid session id|conn:%p|stream_id:%ui|"
                    "session_id:%ui|", h3s->h3c->conn, h3s->stream_id, sid);
            XQC_H3_CONN_ERR(h3s->h3c, H3_ID_ERROR, -XQC_H3_WT_SESSION_ID_ERROR);
            return -XQC_H3_WT_SESSION_ID_ERROR;
        }

        xqc_wt_stream_bind_session(wts, sid);
    }

    if (!wts->closed && (wts->session != NULL || wts->buffered)
        && data_len > 0)
    {
        ret = xqc_wt_stream_save_recv_data(wts, data, data_len);
        if (ret != XQC_OK) {
            xqc_log(h3s->log, XQC_LOG_ERROR,
                    "|xqc_wt_stream_save_recv_data error|stream_id:%ui|ret:%d|",
                    h3s->stream_id, ret);
            return ret;
        }
    }

    if (fin) {
        if (wts->session != NULL || wts->buffered) {
            wts->recv_fin = 1;
        } else {
            wts->recv_reset = 1;
        }
    }

    return (ssize_t)total;
}

void
xqc_wt_stream_on_bidi_signal(xqc_h3_stream_t *h3s)
{
    /*
     * draft-07 Section 4.2: the client sends WEBTRANSPORT_STREAM as the
     * very first bytes of a request stream. The stream is now a WebTransport
     * stream: the session ID prefix and the payload follow and are consumed
     * by xqc_wt_stream_process_in().
     */
    xqc_log(h3s->log, XQC_LOG_DEBUG,
            "|webtransport bidi stream signal|stream_id:%ui|", h3s->stream_id);

    h3s->type = XQC_H3_STREAM_TYPE_WT;
}

xqc_wt_stream_t *
xqc_wt_session_create_stream(xqc_wt_session_t *session, xqc_bool_t bidi,
    void *user_data)
{
    xqc_wt_stream_t  *wts;
    xqc_h3_conn_t    *h3c;
    xqc_h3_stream_t  *h3s;
    xqc_stream_t     *stream;
    unsigned char     prefix[16];
    unsigned char    *p = prefix;
    xqc_int_t         ret;

    if (session == NULL || session->wtc == NULL
        || session->state != XQC_WT_SESSION_ESTABLISHED || session->h3r == NULL)
    {
        return NULL;
    }

    h3c = session->wtc->h3c;

    stream = xqc_stream_create_with_direction(h3c->conn,
                 bidi ? XQC_STREAM_BIDI : XQC_STREAM_UNI, NULL);
    if (stream == NULL) {
        xqc_log(h3c->log, XQC_LOG_ERROR,
                "|create transport stream for webtransport error|conn:%p|",
                h3c->conn);
        return NULL;
    }

    h3s = xqc_h3_stream_create(h3c, stream, XQC_H3_STREAM_TYPE_WT, NULL);
    if (h3s == NULL) {
        xqc_destroy_stream(stream);
        return NULL;
    }

    wts = xqc_wt_stream_alloc(h3s, bidi ? 1 : 0, 1, user_data);
    if (wts == NULL) {
        xqc_h3_stream_destroy(h3s);
        xqc_destroy_stream(stream);
        return NULL;
    }

    /* "signal + session ID" prefix (draft-07 Sections 4.1/4.2); the payload
     * follows the prefix on the same stream */
    p = xqc_put_varint(p, bidi ? XQC_WT_BIDI_SIGNAL_VALUE
                               : XQC_WT_UNI_STREAM_TYPE);
    p = xqc_put_varint(p, session->stream_id);

    wts->send_buf = xqc_var_buf_create((size_t)(p - prefix));
    if (wts->send_buf == NULL) {
        xqc_h3_stream_destroy(h3s);
        xqc_destroy_stream(stream);
        return NULL;
    }

    ret = xqc_var_buf_save_data(wts->send_buf, prefix, (size_t)(p - prefix));
    if (ret != XQC_OK) {
        xqc_h3_stream_destroy(h3s);
        xqc_destroy_stream(stream);
        return NULL;
    }

    wts->session = session;
    xqc_list_add_tail(&wts->list_head, &session->streams);
    wts->opened_notified = 1;
    if (session->wtc->callbacks_registered
        && session->wtc->callbacks.stream_opened_notify)
    {
        session->wtc->callbacks.stream_opened_notify(session, wts,
                                                     session->user_data);
    }

    xqc_log(h3c->log, XQC_LOG_DEBUG,
            "|webtransport stream created|conn:%p|stream_id:%ui|session_id:%ui|"
            "bidi:%d|", h3c->conn, h3s->stream_id, session->stream_id,
            (int)bidi);

    ret = xqc_wt_stream_flush(wts);
    if (ret != XQC_OK) {
        xqc_log(h3c->log, XQC_LOG_ERROR,
                "|webtransport stream prefix flush error|conn:%p|stream_id:%ui|"
                "ret:%d|", h3c->conn, h3s->stream_id, ret);
    }

    return wts;
}

void
xqc_wt_stream_on_h3_stream_destroy(xqc_wt_stream_t *wts)
{
    wts->h3s = NULL;
    wts->closed = 1;
    if (!wts->recv_fin) {
        wts->recv_reset = 1;
    }

    /*
     * final notification for an announced stream: from here on the stream
     * has no transport left, so the application must release whatever it
     * bound to it. stream_user_data may be NULL when the application
     * detached it earlier with xqc_wt_stream_set_user_data().
     */
    if (wts->opened_notified && wts->wtc != NULL
        && wts->wtc->callbacks_registered
        && wts->wtc->callbacks.stream_closed_notify)
    {
        wts->wtc->callbacks.stream_closed_notify(wts, wts->user_data);
    }

    /*
     * a stream attached to a live session is detached later by
     * xqc_wt_session_free_streams; everything else is done now
     */
    if (wts->session != NULL) {
        return;
    }

    if (wts->buffered) {
        xqc_list_del_init(&wts->list_head);
        wts->buffered = 0;
        if (wts->wtc != NULL) {
            wts->wtc->buffered_stream_count--;
        }
    }

    xqc_wt_stream_free(wts);
}

xqc_bool_t
xqc_wt_stream_should_notify_read(xqc_wt_stream_t *wts)
{
    if (wts == NULL || wts->fin_read || wts->buffered || wts->session == NULL) {
        return XQC_FALSE;
    }

    return !xqc_list_empty(&wts->recv_buf)
           || (wts->recv_fin && !wts->fin_read);
}

xqc_int_t
xqc_wt_stream_notify_read(xqc_wt_stream_t *wts)
{
    if (wts == NULL || wts->wtc == NULL) {
        return XQC_OK;
    }

    if (wts->wtc->callbacks_registered && wts->wtc->callbacks.stream_read_notify) {
        wts->wtc->callbacks.stream_read_notify(wts, wts->user_data);
    }

    return XQC_OK;
}

void *
xqc_wt_stream_get_user_data(xqc_wt_stream_t *stream)
{
    return stream ? stream->user_data : NULL;
}

void
xqc_wt_stream_set_user_data(xqc_wt_stream_t *stream, void *user_data)
{
    if (stream) {
        stream->user_data = user_data;
    }
}

xqc_wt_session_t *
xqc_wt_stream_get_session(xqc_wt_stream_t *stream)
{
    return stream ? stream->session : NULL;
}

/*
 * HTTP datagrams (RFC 9297 Sections 2/2.1).
 */

/* RFC 9297 Section 2.1.1: HTTP/3 datagrams are in effect only when
   SETTINGS_H3_DATAGRAM was both sent and received with a value of 1 */
static xqc_bool_t
xqc_wt_datagram_ready(xqc_h3_conn_t *h3c)
{
    return h3c->local_h3_conn_settings.h3_datagram == 1
           && (h3c->flags & XQC_H3_CONN_FLAG_SETTINGS_RECVED)
           && h3c->peer_h3_conn_settings.h3_datagram == 1;
}

static void
xqc_wt_session_queue_datagram(xqc_wt_session_t *session,
    const unsigned char *data, size_t data_len)
{
    xqc_h3_conn_t *h3c = session->wtc->h3c;
    xqc_var_buf_t *buf;
    xqc_int_t      ret;

    /*
     * draft-07 Section 4.5: the number of buffered datagrams is limited;
     * when the limit is exceeded, datagrams are dropped
     */
    if (session->dgram_recv_count >= XQC_WT_DGRAM_RECV_QUEUE_MAX) {
        xqc_log(h3c->log, XQC_LOG_INFO,
                "|webtransport datagram dropped, recv queue full|conn:%p|"
                "stream_id:%ui|limit:%d|", h3c->conn, session->stream_id,
                XQC_WT_DGRAM_RECV_QUEUE_MAX);
        return;
    }

    buf = xqc_var_buf_create(xqc_max(data_len, (size_t)XQC_VAR_BUF_INIT_SIZE));
    if (buf == NULL) {
        xqc_log(h3c->log, XQC_LOG_ERROR,
                "|xqc_var_buf_create datagram error|conn:%p|stream_id:%ui|",
                h3c->conn, session->stream_id);
        return;
    }

    ret = xqc_var_buf_save_data(buf, data, data_len);
    if (ret != XQC_OK) {
        xqc_var_buf_free(buf);
        return;
    }

    ret = xqc_list_buf_to_tail(&session->dgram_recv_buf, buf);
    if (ret != XQC_OK) {
        xqc_var_buf_free(buf);
        return;
    }

    session->dgram_recv_count++;
}

xqc_int_t
xqc_wt_session_send_datagram(xqc_wt_session_t *session, const unsigned char *data,
    size_t data_len)
{
    xqc_h3_conn_t *h3c;
    unsigned char *pkt;
    unsigned char *p;
    size_t         pkt_len;
    xqc_int_t      ret;

    if (session == NULL || data == NULL) {
        return -XQC_EPARAM;
    }

    if (session->state != XQC_WT_SESSION_ESTABLISHED || session->h3r == NULL) {
        return -XQC_ESTATE;
    }

    h3c = session->wtc->h3c;

    /* RFC 9297 Section 2.1: HTTP/3 datagrams MUST NOT be sent unless the
       corresponding stream's send side is open */
    if (session->h3r->h3_stream->flags & XQC_HTTP3_STREAM_FLAG_FIN_SENT) {
        return -XQC_ESTATE;
    }

    /* RFC 9297 Section 2.1.1: not before SETTINGS_H3_DATAGRAM was both
       sent and received with a value of 1 */
    if (!xqc_wt_datagram_ready(h3c)) {
        return -XQC_ESTATE;
    }

    /* HTTP/3 Datagram: Quarter Stream ID (i), HTTP Datagram Payload (..) */
    pkt = xqc_malloc(8 + data_len);
    if (pkt == NULL) {
        return -XQC_EMALLOC;
    }

    p = xqc_put_varint(pkt, session->stream_id / 4);
    xqc_memcpy(p, data, data_len);
    pkt_len = (size_t)(p - pkt) + data_len;

    ret = xqc_datagram_send(h3c->conn, pkt, pkt_len, NULL, XQC_DATA_QOS_NORMAL);
    xqc_free(pkt);

    if (ret != XQC_OK) {
        xqc_log(h3c->log, XQC_LOG_DEBUG,
                "|webtransport send datagram failed|conn:%p|stream_id:%ui|"
                "len:%uz|ret:%d|", h3c->conn, session->stream_id, data_len, ret);
        return ret;
    }

    xqc_log(h3c->log, XQC_LOG_DEBUG,
            "|webtransport datagram sent|conn:%p|stream_id:%ui|len:%uz|",
            h3c->conn, session->stream_id, data_len);

    return XQC_OK;
}

ssize_t
xqc_wt_session_recv_datagram(xqc_wt_session_t *session, unsigned char *buf,
    size_t buf_size)
{
    xqc_list_head_t *pos;
    xqc_list_buf_t  *list_buf;
    xqc_var_buf_t   *vbuf;
    size_t           len;

    if (session == NULL || buf == NULL || buf_size == 0) {
        return -XQC_EPARAM;
    }

    if (session->state != XQC_WT_SESSION_ESTABLISHED) {
        return -XQC_ESTATE;
    }

    pos = session->dgram_recv_buf.next;
    if (pos == &session->dgram_recv_buf) {
        return -XQC_EAGAIN;
    }

    list_buf = xqc_list_entry(pos, xqc_list_buf_t, list_head);
    vbuf = list_buf->buf;
    len = vbuf->data_len;

    if (buf_size < len) {
        /* keep the payload queued so the application can retry */
        return -XQC_ENOBUF;
    }

    if (len > 0) {
        xqc_memcpy(buf, vbuf->data, len);
    }
    xqc_list_buf_free(list_buf);
    session->dgram_recv_count--;

    return (ssize_t)len;
}

/*
 * draft-07 Section 4.5: buffer a datagram whose session is not established
 * yet, bounded per connection; once the cap is reached datagrams are dropped
 */
static void
xqc_wt_conn_buffer_datagram(xqc_wt_conn_t *wtc, uint64_t sid,
    const unsigned char *data, size_t data_len)
{
    xqc_wt_buffered_dgram_t *bdg;
    xqc_var_buf_t           *buf;

    if (wtc->buffered_dgram_count >= XQC_WT_BUFFERED_DGRAM_MAX) {
        xqc_log(wtc->h3c->log, XQC_LOG_INFO,
                "|webtransport datagram dropped, buffered cap reached|conn:%p|"
                "session_id:%ui|limit:%d|", wtc->h3c->conn, sid,
                XQC_WT_BUFFERED_DGRAM_MAX);
        return;
    }

    bdg = xqc_calloc(1, sizeof(xqc_wt_buffered_dgram_t));
    if (bdg == NULL) {
        return;
    }

    buf = xqc_var_buf_create(xqc_max(data_len, (size_t)XQC_VAR_BUF_INIT_SIZE));
    if (buf == NULL || xqc_var_buf_save_data(buf, data, data_len) != XQC_OK) {
        if (buf) {
            xqc_var_buf_free(buf);
        }
        xqc_free(bdg);
        return;
    }

    bdg->stream_id = sid;
    bdg->payload = buf;
    xqc_init_list_head(&bdg->list_head);
    xqc_list_add_tail(&bdg->list_head, &wtc->buffered_dgrams);
    wtc->buffered_dgram_count++;

    xqc_log(wtc->h3c->log, XQC_LOG_DEBUG,
            "|webtransport datagram buffered|conn:%p|session_id:%ui|"
            "count:%ui|", wtc->h3c->conn, sid, wtc->buffered_dgram_count);
}

xqc_int_t
xqc_wt_conn_on_datagram(xqc_h3_conn_t *h3c, const unsigned char *data,
    size_t data_len)
{
    xqc_wt_conn_t          *wtc = h3c->wt;
    xqc_wt_session_t       *session;
    xqc_discrete_int_pctx_t pctx;
    xqc_bool_t              fin = XQC_FALSE;
    xqc_stream_t           *stream;
    uint64_t                sid;
    ssize_t                 n;

    if (wtc == NULL || !xqc_wt_datagram_ready(h3c)) {
        return XQC_ERROR;
    }

    /* RFC 9297 Section 2.1: a datagram too short to parse the quarter
       stream ID, or a quarter stream ID larger than 2^60-1, is a
       connection error of type H3_DATAGRAM_ERROR */
    xqc_discrete_int_pctx_clear(&pctx);
    n = xqc_discrete_vint_parse(data, data_len, &pctx, &fin);
    if (!fin || pctx.vi > ((1ULL << 60) - 1)) {
        xqc_log(h3c->log, XQC_LOG_ERROR,
                "|malformed http datagram|conn:%p|len:%uz|complete:%d|"
                "quarter_id:%ui|", h3c->conn, data_len, (int)fin, pctx.vi);
        XQC_H3_CONN_ERR(h3c, H3_DATAGRAM_ERROR, -XQC_H3_WT_DATAGRAM_ERROR);
        return XQC_OK;
    }

    sid = pctx.vi * 4;
    data += n;
    data_len -= n;

    session = xqc_wt_session_find_by_stream_id(wtc, sid);
    if (session != NULL) {
        if (session->state == XQC_WT_SESSION_OFFERED) {
            /* draft-07 Section 4.5: the session may still be accepted;
               the datagram waits for its establishment */
            xqc_wt_conn_buffer_datagram(wtc, sid, data, data_len);
            return XQC_OK;
        }

        if (session->state != XQC_WT_SESSION_ESTABLISHED) {
            /* RFC 9297 Section 2.1: datagrams whose receive side is closed
               are silently dropped (a session here is TERMINATED) */
            xqc_log(h3c->log, XQC_LOG_DEBUG,
                    "|webtransport datagram for inactive session dropped|"
                    "conn:%p|session_id:%ui|state:%d|", h3c->conn, sid,
                    (int)session->state);
            return XQC_OK;
        }

        xqc_wt_session_queue_datagram(session, data, data_len);

        if (wtc->callbacks_registered && wtc->callbacks.datagram_read_notify) {
            wtc->callbacks.datagram_read_notify(session, session->user_data);
        }
        return XQC_OK;
    }

    /*
     * No WebTransport session owns this quarter stream ID. When the
     * application registered generic HTTP datagram callbacks, datagrams are
     * associated with its own semantics and are handed back to the caller
     * (XQC_ERROR). Otherwise RFC 9297 Section 2 applies: a datagram for a
     * request with no known datagram semantics aborts that request with
     * H3_DATAGRAM_ERROR, and a datagram that maps to a stream that has not
     * yet been created is silently dropped (Section 2.1).
     */
    if (h3c->h3_ext_dgram_callbacks.dgram_read_notify != NULL) {
        return XQC_ERROR;
    }

    stream = xqc_find_stream_by_id(sid, h3c->conn->streams_hash);
    if (stream != NULL) {
        xqc_log(h3c->log, XQC_LOG_INFO,
                "|http datagram for request without datagram semantics, "
                "aborting request|conn:%p|stream_id:%ui|", h3c->conn, sid);
        xqc_stream_close_with_error(stream, H3_DATAGRAM_ERROR);
    } else {
        xqc_log(h3c->log, XQC_LOG_DEBUG,
                "|http datagram for unknown stream dropped|conn:%p|"
                "stream_id:%ui|", h3c->conn, sid);
    }

    return XQC_OK;
}

/*
 * Capsule protocol of the CONNECT stream receive side (RFC 9297 Section 3).
 */

/* Application Error Code (4) + at most XQC_WT_CLOSE_MSG_MAX message bytes */
#define XQC_WT_CLOSE_CAP_MAX (4 + XQC_WT_CLOSE_MSG_MAX)

/*
 * handle a completed capsule (cap_consumed == cap_len). Returns XQC_OK, or
 * a negative error for a capsule that violates its type definition.
 */
static xqc_int_t
xqc_wt_session_finish_capsule(xqc_wt_session_t *session)
{
    xqc_h3_conn_t *h3c = session->wtc->h3c;

    switch (session->cap_type) {
    case XQC_WT_CAPSULE_CLOSE_WEBTRANSPORT_SESSION:
    {
        uint32_t code = 0;
        size_t   msg_len = 0;
        xqc_int_t ret;

        if (session->close_msg_buf != NULL && session->close_msg_len >= 4) {
            const unsigned char *b = session->close_msg_buf;
            code = ((uint32_t)b[0] << 24) | ((uint32_t)b[1] << 16)
                   | ((uint32_t)b[2] << 8) | (uint32_t)b[3];
            msg_len = session->close_msg_len - 4;
        }

        xqc_log(h3c->log, XQC_LOG_INFO,
                "|webtransport peer closed session|conn:%p|stream_id:%ui|"
                "error_code:%ui|msg_len:%uz|", h3c->conn, session->stream_id,
                (uint64_t)code, msg_len);

        if (session->close_msg_buf) {
            xqc_free(session->close_msg_buf);
            session->close_msg_buf = NULL;
            session->close_msg_len = 0;
        }

        /* draft-07 Section 5: receiving the capsule terminates the session */
        session->close_received = 1;
        xqc_wt_session_terminate(session);

        /*
         * the peer sends a FIN right after its close capsule; complete the
         * CONNECT stream in the same way so it closes cleanly on both sides
         * (a clean close is equivalent to a CLOSE capsule with code 0)
         */
        if (session->h3r != NULL && session->h3r->h3_stream != NULL
            && !(session->h3r->h3_stream->flags & XQC_HTTP3_STREAM_FLAG_FIN_SENT)
            && session->close_cap_buf == NULL)
        {
            ret = xqc_h3_request_finish(session->h3r);
            if (ret < 0) {
                xqc_log(h3c->log, XQC_LOG_ERROR,
                        "|send fin after peer close failed|conn:%p|"
                        "stream_id:%ui|ret:%d|", h3c->conn, session->stream_id,
                        ret);
            }
        }
        break;
    }

    case XQC_WT_CAPSULE_DRAIN_WEBTRANSPORT_SESSION:
        /* draft-07 Section 4.6: the session may still be used, but should
           be terminated gracefully as soon as possible */
        xqc_log(h3c->log, XQC_LOG_INFO,
                "|webtransport drain capsule received|conn:%p|stream_id:%ui|",
                h3c->conn, session->stream_id);
        break;

    default:
        /* RFC 9297 Section 3.2: unknown capsule types are silently dropped
           and skipped (the payload was consumed already) */
        xqc_log(h3c->log, XQC_LOG_DEBUG,
                "|webtransport unknown capsule skipped|conn:%p|stream_id:%ui|"
                "type:%ui|len:%ui|", h3c->conn, session->stream_id,
                session->cap_type, session->cap_len);
        break;
    }

    return XQC_OK;
}

xqc_int_t
xqc_wt_session_process_capsules(xqc_wt_session_t *session,
    const unsigned char *data, size_t data_len, xqc_bool_t fin)
{
    xqc_h3_conn_t       *h3c = session->wtc->h3c;
    const unsigned char *pos = data;
    const unsigned char *end = data + data_len;
    xqc_bool_t           int_fin;
    xqc_int_t            ret;
    ssize_t              n;

    while (pos < end) {
        if (session->close_received) {
            /*
             * draft-07 Section 5: any stream data after a
             * CLOSE_WEBTRANSPORT_SESSION capsule aborts the CONNECT stream
             * with H3_MESSAGE_ERROR
             */
            xqc_log(h3c->log, XQC_LOG_ERROR,
                    "|stream data after close capsule|conn:%p|stream_id:%ui|",
                    h3c->conn, session->stream_id);
            return XQC_ERROR;
        }

        switch (session->cap_state) {
        case XQC_WT_CAP_STATE_TYPE:
            n = xqc_discrete_vint_parse(pos, end - pos, &session->cap_int_pctx,
                                        &int_fin);
            pos += n;
            if (!int_fin) {
                break;
            }
            session->cap_type = session->cap_int_pctx.vi;
            xqc_discrete_int_pctx_clear(&session->cap_int_pctx);
            session->cap_state = XQC_WT_CAP_STATE_LENGTH;
            break;

        case XQC_WT_CAP_STATE_LENGTH:
            n = xqc_discrete_vint_parse(pos, end - pos, &session->cap_int_pctx,
                                        &int_fin);
            pos += n;
            if (!int_fin) {
                break;
            }
            session->cap_len = session->cap_int_pctx.vi;
            xqc_discrete_int_pctx_clear(&session->cap_int_pctx);
            session->cap_consumed = 0;

            /*
             * RFC 9297 Section 3.3: a payload that terminates before the
             * end of the fields identified for its type is malformed
             */
            if (session->cap_type == XQC_WT_CAPSULE_CLOSE_WEBTRANSPORT_SESSION
                && session->cap_len < 4)
            {
                xqc_log(h3c->log, XQC_LOG_ERROR,
                        "|close capsule too short|conn:%p|stream_id:%ui|"
                        "len:%ui|", h3c->conn, session->stream_id,
                        session->cap_len);
                return XQC_ERROR;
            }

            if (session->cap_type == XQC_WT_CAPSULE_DRAIN_WEBTRANSPORT_SESSION
                && session->cap_len != 0)
            {
                xqc_log(h3c->log, XQC_LOG_ERROR,
                        "|drain capsule with payload|conn:%p|stream_id:%ui|"
                        "len:%ui|", h3c->conn, session->stream_id,
                        session->cap_len);
                return XQC_ERROR;
            }

            if (session->cap_type == XQC_WT_CAPSULE_CLOSE_WEBTRANSPORT_SESSION) {
                size_t cap = (size_t)xqc_min(session->cap_len,
                                             (uint64_t)XQC_WT_CLOSE_CAP_MAX);
                session->close_msg_buf = xqc_malloc(cap);
                session->close_msg_len = 0;
                if (session->close_msg_buf == NULL) {
                    xqc_log(h3c->log, XQC_LOG_ERROR,
                            "|xqc_malloc close message capture error|conn:%p|"
                            "stream_id:%ui|", h3c->conn, session->stream_id);
                    /* keep parsing; only the logged message is lost */
                }
            }

            if (session->cap_len == 0) {
                ret = xqc_wt_session_finish_capsule(session);
                if (ret < 0) {
                    return ret;
                }
                session->cap_state = XQC_WT_CAP_STATE_TYPE;
                break;
            }

            session->cap_state = XQC_WT_CAP_STATE_VALUE;
            break;

        case XQC_WT_CAP_STATE_VALUE:
        {
            size_t take = (size_t)xqc_min((uint64_t)(end - pos),
                                          session->cap_len - session->cap_consumed);

            if (session->close_msg_buf != NULL
                && session->cap_consumed < XQC_WT_CLOSE_CAP_MAX)
            {
                size_t c = (size_t)xqc_min((uint64_t)take,
                               (uint64_t)XQC_WT_CLOSE_CAP_MAX
                                   - session->cap_consumed);
                xqc_memcpy(session->close_msg_buf + session->cap_consumed, pos, c);
                session->close_msg_len = (size_t)session->cap_consumed + c;
            }
            session->cap_consumed += take;
            pos += take;

            if (session->cap_consumed == session->cap_len) {
                ret = xqc_wt_session_finish_capsule(session);
                if (ret < 0) {
                    return ret;
                }
                session->cap_state = XQC_WT_CAP_STATE_TYPE;
            }
            break;
        }

        default:
            return XQC_ERROR;
        }
    }

    /*
     * RFC 9297 Section 3.3: a clean FIN with a truncated trailing capsule
     * is treated as a malformed message
     */
    if (fin
        && (session->cap_state != XQC_WT_CAP_STATE_TYPE
            || session->cap_int_pctx.left != 0))
    {
        xqc_log(h3c->log, XQC_LOG_ERROR,
                "|capsule truncated at stream end|conn:%p|stream_id:%ui|"
                "state:%d|", h3c->conn, session->stream_id,
                (int)session->cap_state);
        return XQC_ERROR;
    }

    return XQC_OK;
}