/**
 * @copyright Copyright (c) 2022, Alibaba Group Holding Limited
 */

#ifndef XQC_WEBTRANSPORT_H
#define XQC_WEBTRANSPORT_H

/**
 * WebTransport over HTTP/3, server side.
 *
 * Wire behavior follows draft-ietf-webtrans-http3-07 with RFC 9220
 * (extended CONNECT), RFC 8441 Section 4 (:protocol semantics) and
 * RFC 9297 (HTTP datagrams, capsule protocol) as normative references.
 *
 * WebTransport is enabled per engine through xqc_config_t.enable_webtransport.
 * When enabled, the server advertises SETTINGS_ENABLE_CONNECT_PROTOCOL,
 * SETTINGS_H3_DATAGRAM and SETTINGS_WEBTRANSPORT_MAX_SESSIONS in its HTTP/3
 * SETTINGS. A client "webtransport" CONNECT request is validated by the h3
 * layer and handed to the application through
 * xqc_wt_callbacks_t.session_request_notify; the application establishes or
 * refuses the session with xqc_wt_session_accept() / xqc_wt_session_reject().
 * The session identifier is the CONNECT stream ID (draft-07 Section 4).
 */

#include "xquic.h"
#include "xqc_http3.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief opaque WebTransport session handler
 */
typedef struct xqc_wt_session_s xqc_wt_session_t;

/**
 * @brief opaque WebTransport stream handler (draft-07 Sections 4.1/4.2)
 */
typedef struct xqc_wt_stream_s xqc_wt_stream_t;

/**
 * @brief WebTransport server callbacks
 *
 * Callbacks are registered on the h3 connection with
 * xqc_h3_conn_set_webtransport_callbacks(). Callback return values are
 * ignored; session outcome is expressed through the session APIs.
 */
typedef struct xqc_wt_callbacks_s {
    /**
     * @brief a "webtransport" extended CONNECT request passed h3 validation
     * and is offered to the application (draft-07 Section 3.3)
     *
     * The application establishes or refuses the session by calling
     * xqc_wt_session_accept() or xqc_wt_session_reject() (either from this
     * callback or later). If the callback returns non-zero while the session
     * is still undecided, the session is refused with a 404 response.
     *
     * @param session newly created session in OFFERED state
     * @param conn_user_data user data of the h3 connection
     */
    xqc_int_t (*session_request_notify)(xqc_wt_session_t *session, void *conn_user_data);

    /**
     * @brief the session is established: the 2xx response has been sent
     *
     * Fires exactly once per established session; may fire asynchronously
     * after xqc_wt_session_accept() when acceptance had to wait for the
     * client SETTINGS (draft-07 Section 3.1).
     *
     * @param session established session
     * @param session_user_data user data given to xqc_wt_session_accept()
     */
    xqc_int_t (*session_accepted_notify)(xqc_wt_session_t *session, void *session_user_data);

    /**
     * @brief the session is terminated and MUST NOT be used any further
     *
     * Fires exactly once for every session offered to the application,
     * whatever the outcome (refused, established then closed, or aborted
     * with the connection). After this callback the peer may still hold
     * the pointer, but every API call on it fails.
     *
     * @param session terminated session
     * @param session_user_data user data given to xqc_wt_session_accept(),
     *        NULL if accept() was never called
     */
    xqc_int_t (*session_closed_notify)(xqc_wt_session_t *session, void *session_user_data);

    /**
     * @brief the peer opened a WebTransport stream (draft-07 Sections 4.1/4.2)
     *
     * @param session session the stream belongs to
     * @param stream newly created stream in read mode
     * @param session_user_data user data given to xqc_wt_session_accept()
     */
    xqc_int_t (*stream_opened_notify)(xqc_wt_session_t *session, xqc_wt_stream_t *stream,
        void *session_user_data);

    /**
     * @brief data or FIN became available on a WebTransport stream
     *
     * @param stream readable stream
     * @param stream_user_data user data given to the stream creation call
     */
    xqc_int_t (*stream_read_notify)(xqc_wt_stream_t *stream, void *stream_user_data);

    /**
     * @brief a stream announced through stream_opened_notify is destroyed
     *
     * Fires at most once per announced stream and only when
     * stream_opened_notify fired for it. It may fire after
     * session_closed_notify, in which case the session the stream belonged
     * to is already gone. After this callback the stream pointer is invalid
     * and must not be used anymore.
     *
     * @param stream destroyed stream
     * @param stream_user_data user data of the stream, NULL if the
     *        application detached it with xqc_wt_stream_set_user_data()
     *        before the stream was destroyed
     */
    xqc_int_t (*stream_closed_notify)(xqc_wt_stream_t *stream, void *stream_user_data);

    /**
     * @brief the peer sent a WebTransport datagram (RFC 9297 Section 2.1)
     *
     * @param session session the datagram belongs to
     * @param session_user_data user data given to xqc_wt_session_accept()
     */
    xqc_int_t (*datagram_read_notify)(xqc_wt_session_t *session, void *session_user_data);

} xqc_wt_callbacks_t;

/**
 * @brief register WebTransport callbacks on an h3 connection
 *
 * Must be called when the h3 connection is created (h3_conn_create_notify).
 * The engine must be created with xqc_config_t.enable_webtransport set;
 * otherwise this call fails and "webtransport" CONNECT requests are treated
 * as malformed by the h3 layer.
 *
 * @param h3c handler of http3 connection
 * @param cbs WebTransport callbacks
 * @return XQC_OK for success, -XQC_EPARAM for wrong parameters,
 *         -XQC_ESTATE when WebTransport is not enabled on this connection
 */
XQC_EXPORT_PUBLIC_API
xqc_int_t xqc_h3_conn_set_webtransport_callbacks(xqc_h3_conn_t *h3c, xqc_wt_callbacks_t *cbs);

/**
 * @brief accept an offered session (send the 2xx response, draft-07 Section 3.3)
 *
 * If the client SETTINGS have been received and advertise WebTransport
 * support (SETTINGS_WEBTRANSPORT_MAX_SESSIONS > 0 and SETTINGS_H3_DATAGRAM = 1,
 * draft-07 Section 3.1), the 2xx response is sent and
 * session_accepted_notify fires before this call returns. Otherwise
 * acceptance is deferred until the SETTINGS arrive, at which point the
 * session is either established or refused with a 404 response (and
 * session_closed_notify fires).
 *
 * @param session session in OFFERED state
 * @param user_data user data for the session callbacks
 * @return XQC_OK for success (possibly deferred), -XQC_EPARAM for NULL session,
 *         -XQC_ESTATE when the session is not in OFFERED state
 */
XQC_EXPORT_PUBLIC_API
xqc_int_t xqc_wt_session_accept(xqc_wt_session_t *session, void *user_data);

/**
 * @brief refuse an offered session with an HTTP status (draft-07 Section 3.3)
 *
 * Sends a response carrying the given non-2xx status and terminates the
 * session. Typical statuses: 403 for a request the server refuses, 404 when
 * no WebTransport endpoint exists for the request.
 *
 * @param session session in OFFERED state
 * @param status HTTP status code of the response
 * @return XQC_OK for success, -XQC_EPARAM for NULL session,
 *         -XQC_ESTATE when the session is not in OFFERED state
 */
XQC_EXPORT_PUBLIC_API
xqc_int_t xqc_wt_session_reject(xqc_wt_session_t *session, int status);

/**
 * @brief close an established session with the CLOSE_WEBTRANSPORT_SESSION
 * capsule (draft-07 Section 5)
 *
 * Sends the capsule with the given application error code and message
 * (at most 1024 bytes) followed by a FIN, and terminates the session.
 *
 * @param session established session
 * @param error_code application error code
 * @param error_msg UTF-8 error message, may be NULL
 * @return XQC_OK for success, -XQC_EPARAM for NULL session,
 *         -XQC_ESTATE when the session is not established
 */
XQC_EXPORT_PUBLIC_API
xqc_int_t xqc_wt_session_close(xqc_wt_session_t *session, uint32_t error_code,
    const char *error_msg);

/**
 * @brief send a WebTransport datagram payload (RFC 9297 Section 2.1)
 *
 * The quarter stream ID of the CONNECT stream is prepended by the library.
 *
 * @param session established session
 * @param data datagram payload
 * @param data_len payload length
 * @return XQC_OK for success, negative error otherwise
 */
XQC_EXPORT_PUBLIC_API
xqc_int_t xqc_wt_session_send_datagram(xqc_wt_session_t *session, const unsigned char *data,
    size_t data_len);

/**
 * @brief receive a WebTransport datagram payload (RFC 9297 Section 2.1)
 *
 * Called after datagram_read_notify with the datagram payload that followed
 * the quarter stream ID. Datagrams are delivered in order; each call returns
 * the next buffered payload.
 *
 * @param session session the datagram belongs to
 * @param buf receive buffer
 * @param buf_size capacity of buf
 * @return >= 0 for bytes read (0 for a datagram with an empty payload),
 *         -XQC_EAGAIN when nothing is buffered, -XQC_ENOBUF when buf_size
 *         is smaller than the next payload (the payload stays buffered),
 *         -XQC_ESTATE when the session is not established, -XQC_EPARAM for
 *         wrong parameters
 */
XQC_EXPORT_PUBLIC_API
ssize_t xqc_wt_session_recv_datagram(xqc_wt_session_t *session, unsigned char *buf,
    size_t buf_size);

/**
 * @brief create a WebTransport stream on an established session
 *
 * The "type + session ID" prefix (draft-07 Sections 4.1/4.2) is written by
 * the library when the stream opens.
 *
 * @param session established session
 * @param bidi XQC_TRUE for a bidirectional stream, XQC_FALSE for unidirectional
 * @param user_data user data for stream callbacks
 * @return stream handler, NULL on failure
 */
XQC_EXPORT_PUBLIC_API
xqc_wt_stream_t *xqc_wt_session_create_stream(xqc_wt_session_t *session, xqc_bool_t bidi,
    void *user_data);

/**
 * @brief send data on a WebTransport stream
 *
 * @param stream stream handler
 * @param data payload bytes
 * @param data_len payload length
 * @param fin whether to finish the stream after this data
 * @return > 0 for bytes written, -XQC_EAGAIN retry when writable, < 0 for error
 */
XQC_EXPORT_PUBLIC_API
ssize_t xqc_wt_stream_send(xqc_wt_stream_t *stream, const unsigned char *data, size_t data_len,
    uint8_t fin);

/**
 * @brief receive data from a WebTransport stream
 *
 * Called after stream_read_notify.
 *
 * @param stream stream handler
 * @param buf receive buffer
 * @param buf_size capacity of buf
 * @param fin [out] whether the peer finished the stream
 * @return > 0 for bytes read, -XQC_EAGAIN when nothing is buffered, < 0 for error
 */
XQC_EXPORT_PUBLIC_API
ssize_t xqc_wt_stream_recv(xqc_wt_stream_t *stream, unsigned char *buf, size_t buf_size,
    uint8_t *fin);

/**
 * @brief close a WebTransport stream (reset the QUIC stream)
 *
 * @param stream stream handler
 * @return XQC_OK for success, negative error otherwise
 */
XQC_EXPORT_PUBLIC_API
xqc_int_t xqc_wt_stream_close(xqc_wt_stream_t *stream);

/**
 * @brief get user data of a stream
 */
XQC_EXPORT_PUBLIC_API
void *xqc_wt_stream_get_user_data(xqc_wt_stream_t *stream);

/**
 * @brief set user data of a stream
 */
XQC_EXPORT_PUBLIC_API
void xqc_wt_stream_set_user_data(xqc_wt_stream_t *stream, void *user_data);

/**
 * @brief get the session a stream belongs to
 */
XQC_EXPORT_PUBLIC_API
xqc_wt_session_t *xqc_wt_stream_get_session(xqc_wt_stream_t *stream);

/**
 * @brief get user data of a session
 */
XQC_EXPORT_PUBLIC_API
void *xqc_wt_session_get_user_data(xqc_wt_session_t *session);

/**
 * @brief get the session ID of a session
 *
 * The session ID is the ID of the underlying CONNECT stream (draft-07
 * Section 4).
 */
XQC_EXPORT_PUBLIC_API
uint64_t xqc_wt_session_get_stream_id(xqc_wt_session_t *session);

/**
 * @brief get the QUIC connection a session runs on
 */
XQC_EXPORT_PUBLIC_API
xqc_connection_t *xqc_wt_session_get_quic_conn(xqc_wt_session_t *session);

/**
 * @brief get the WebTransport session bound to an h3 request
 *
 * Usable from h3 request callbacks to distinguish WebTransport CONNECT
 * requests from regular requests.
 *
 * @param h3_request handler of http3 request
 * @return session bound to the request, NULL for a non-WebTransport request
 */
XQC_EXPORT_PUBLIC_API
xqc_wt_session_t *xqc_h3_request_get_wt_session(xqc_h3_request_t *h3_request);

#ifdef __cplusplus
}
#endif

#endif /* XQC_WEBTRANSPORT_H */