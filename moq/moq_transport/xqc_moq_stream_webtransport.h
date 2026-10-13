#ifndef _XQC_MOQ_STREAM_WEBTRANSPORT_H_INCLUDED_
#define _XQC_MOQ_STREAM_WEBTRANSPORT_H_INCLUDED_

#include "xquic/xqc_webtransport.h"
#include "moq/moq_transport/xqc_moq_stream.h"

/*
 * WebTransport transport for MoQ streams (draft-07 Sections 4.1/4.2):
 * every MoQ stream is a WebTransport stream of the session, written through
 * the same "signal + session ID" prefix as any other WebTransport stream.
 */
extern const xqc_moq_trans_stream_ops_t xqc_moq_wt_stream_ops;

/*
 * Glue callbacks for the application's xqc_wt_callbacks_t. The application
 * keeps session_request/closed handling, but must route stream events to
 * these functions with the MoQ user session as session user data:
 *
 *   stream_opened_notify -> xqc_moq_wt_on_stream_opened
 *   stream_read_notify   -> xqc_moq_wt_on_stream_read
 *   stream_closed_notify -> xqc_moq_wt_on_stream_closed
 */

/*
 * bind a peer-created WebTransport stream to a MoQ stream; locally created
 * streams (user data already set) are skipped. A client-initiated bidi
 * stream becomes the session control stream.
 */
xqc_int_t xqc_moq_wt_on_stream_opened(xqc_wt_session_t *session,
    xqc_wt_stream_t *stream, void *session_user_data);

/*
 * drain a readable WebTransport stream into xqc_moq_stream_process()
 */
xqc_int_t xqc_moq_wt_on_stream_read(xqc_wt_stream_t *stream,
    void *stream_user_data);

/*
 * release the MoQ stream bound to a destroyed WebTransport stream
 */
xqc_int_t xqc_moq_wt_on_stream_closed(xqc_wt_stream_t *stream,
    void *stream_user_data);

#endif /* _XQC_MOQ_STREAM_WEBTRANSPORT_H_INCLUDED_ */