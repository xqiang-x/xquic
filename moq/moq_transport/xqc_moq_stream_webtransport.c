#include "src/transport/xqc_stream.h"
#include "src/http3/xqc_h3_stream.h"
#include "src/http3/xqc_webtransport.h"
#include "moq/moq_transport/xqc_moq_stream_webtransport.h"
#include "moq/moq_transport/xqc_moq_session.h"
#include "moq/moq_transport/xqc_moq_stream.h"

static void *xqc_moq_wt_stream_create(void *conn, xqc_stream_direction_t dir, void *user_data);
static xqc_stream_t *xqc_moq_wt_stream_quic_stream(void *stream);
static xqc_int_t xqc_moq_wt_stream_close(void *stream);
static ssize_t xqc_moq_wt_stream_send(void *stream, uint8_t *send_data, size_t send_data_size, uint8_t fin);

const xqc_moq_trans_stream_ops_t xqc_moq_wt_stream_ops = {
    .create      = xqc_moq_wt_stream_create,
    .quic_stream = xqc_moq_wt_stream_quic_stream,
    .close       = xqc_moq_wt_stream_close,
    .write       = xqc_moq_wt_stream_send,
};

static void *
xqc_moq_wt_stream_create(void *conn, xqc_stream_direction_t dir, void *user_data)
{
    xqc_wt_session_t *wt_session = (xqc_wt_session_t *)conn;
    return xqc_wt_session_create_stream(wt_session,
               dir == XQC_STREAM_BIDI ? XQC_TRUE : XQC_FALSE, user_data);
}

static xqc_stream_t *
xqc_moq_wt_stream_quic_stream(void *stream)
{
    xqc_wt_stream_t *wts = (xqc_wt_stream_t *)stream;

    if (wts == NULL || wts->h3s == NULL) {
        return NULL;
    }
    return wts->h3s->stream;
}

static xqc_int_t
xqc_moq_wt_stream_close(void *stream)
{
    return xqc_wt_stream_close((xqc_wt_stream_t *)stream);
}

static ssize_t
xqc_moq_wt_stream_send(void *stream, uint8_t *send_data, size_t send_data_size, uint8_t fin)
{
    return xqc_wt_stream_send((xqc_wt_stream_t *)stream, send_data, send_data_size, fin);
}

xqc_int_t
xqc_moq_wt_on_stream_opened(xqc_wt_session_t *session, xqc_wt_stream_t *stream,
    void *session_user_data)
{
    xqc_moq_user_session_t *user_session = (xqc_moq_user_session_t *)session_user_data;
    xqc_moq_session_t *moq_session;
    xqc_moq_stream_t *moq_stream;

    if (xqc_wt_stream_get_user_data(stream) != NULL) {
        /* local stream: xqc_moq_stream_create_with_transport already bound it */
        return XQC_OK;
    }

    if (user_session == NULL || user_session->session == NULL) {
        return -XQC_ENULLPTR;
    }
    moq_session = user_session->session;

    moq_stream = xqc_moq_stream_create(moq_session);
    if (moq_stream == NULL) {
        xqc_log(moq_session->log, XQC_LOG_ERROR, "|create moq stream error|");
        return -XQC_EMALLOC;
    }
    moq_stream->trans_stream = stream;
    xqc_wt_stream_set_user_data(stream, moq_stream);

    /* draft-07 Section 4.2: the client-initiated bidirectional stream is the
     * MoQ control stream, like XQC_CLI_BID over plain QUIC */
    if (stream->bidi) {
        if (moq_session->ctl_stream == NULL) {
            moq_session->ctl_stream = moq_stream;
        } else {
            xqc_log(moq_session->log, XQC_LOG_ERROR, "|duplicate control stream|");
            return -XQC_EPROTO;
        }
    }

    return XQC_OK;
}

xqc_int_t
xqc_moq_wt_on_stream_read(xqc_wt_stream_t *stream, void *stream_user_data)
{
    xqc_moq_stream_t *moq_stream = (xqc_moq_stream_t *)stream_user_data;
    uint8_t buff[4096] = {0};
    uint8_t fin = 0;
    ssize_t read;
    xqc_int_t ret;

    if (moq_stream == NULL) {
        return XQC_OK;
    }

    do {
        read = xqc_wt_stream_recv(stream, buff, sizeof(buff), &fin);
        if (read == -XQC_EAGAIN) {
            break;
        } else if (read < 0) {
            break;
        }

        ret = xqc_moq_stream_process(moq_stream, buff, read, fin);
        if (ret < 0) {
            xqc_log(moq_stream->session->log, XQC_LOG_ERROR,
                    "|xqc_moq_stream_process error|ret:%d|", ret);
            return ret;
        }

    } while (read > 0 && !fin);

    return XQC_OK;
}

xqc_int_t
xqc_moq_wt_on_stream_closed(xqc_wt_stream_t *stream, void *stream_user_data)
{
    xqc_moq_stream_t *moq_stream = (xqc_moq_stream_t *)stream_user_data;
    xqc_moq_session_t *session;

    if (moq_stream == NULL) {
        /* the application already released this stream */
        return XQC_OK;
    }

    session = moq_stream->session;

    /*
     * The transport stream died on its own: on WebTransport this is the end
     * of one stream of the session, not a violation of the MoQ session.
     * Detach the control stream / datachannel anchors first so
     * xqc_moq_stream_destroy() does not close the session error path.
     */
    if (session != NULL) {
        if (session->ctl_stream == moq_stream) {
            session->ctl_stream = NULL;
        }
        if (session->datachannel.ordered_stream == moq_stream) {
            session->datachannel.ordered_stream = NULL;
        }
    }

    xqc_moq_stream_destroy(moq_stream);
    return XQC_OK;
}