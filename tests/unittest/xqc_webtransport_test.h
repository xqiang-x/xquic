/**
 * @copyright Copyright (c) 2022, Alibaba Group Holding Limited
 */

#ifndef XQUIC_XQC_WEBTRANSPORT_TEST_H
#define XQUIC_XQC_WEBTRANSPORT_TEST_H

/* draft-ietf-webtrans-http3-07 settings negotiation (E1) */
void xqc_test_wt_settings();

/* extended CONNECT (:protocol) admission, RFC 8441/9220 + draft-07 3.3 (E2) */
void xqc_test_wt_connect_validation();

/* session object and public API, draft-07 3.1/3.3/3.4/5 (E3) */
void xqc_test_wt_session_accept();
void xqc_test_wt_session_reject();
void xqc_test_wt_session_no_callbacks();
void xqc_test_wt_session_limit();
void xqc_test_wt_session_deferred_accept();
void xqc_test_wt_session_close();

/* data streams, draft-07 4.1/4.2 (E4) */
void xqc_test_wt_stream_uni_recv();
void xqc_test_wt_stream_bidi();
void xqc_test_wt_stream_bidi_signal_frame_error();
void xqc_test_wt_stream_local_send();

/* http datagrams and the capsule protocol, RFC 9297 2/2.1/3 (E5) */
void xqc_test_wt_datagram();
void xqc_test_wt_capsule();

/* pre-session buffering caps and teardown fan-out, draft-07 4.5/5 (E6) */
void xqc_test_wt_buffered_limit();
void xqc_test_wt_session_teardown();

#endif /* XQUIC_XQC_WEBTRANSPORT_TEST_H */