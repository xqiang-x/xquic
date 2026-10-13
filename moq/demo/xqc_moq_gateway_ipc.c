/*
 * Frame IPC v1 client implementation (POSIX sockets + libevent).
 *
 * A non-blocking UDS stream socket; connect is retried every second until
 * the SFU's FrameDispatcher is up. Complete frames are drained in a loop
 * after each read so callbacks observe one message at a time. Callbacks may
 * call the send helpers; disconnecting from within a callback (via
 * destroy) is not supported.
 */

#include "xqc_moq_gateway_ipc.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

#define XQC_GW_IPC_RX_CAP (4 + XQC_GW_IPC_MAX_MESSAGE_SIZE)

struct xqc_gw_ipc_s {
    struct event_base   *eb;
    char                 sock_path[256];
    xqc_gw_ipc_cbs_t     cbs;
    void                *ud;

    int                  fd;        /* -1 while disconnected */
    struct event        *ev_read;   /* non-NULL only while connected */
    struct event        *ev_retry;  /* reconnect timer, armed when down */

    uint8_t             *rx;
    size_t               rx_cap;
    size_t               rx_len;

    xqc_gw_ipc_stats_t   stats;
};

static void gw_ipc_try_connect(xqc_gw_ipc_t *ipc);
static void gw_ipc_connected(xqc_gw_ipc_t *ipc);
static void gw_ipc_disconnect(xqc_gw_ipc_t *ipc);
static void gw_ipc_drain(xqc_gw_ipc_t *ipc);
static void gw_ipc_on_readable(evutil_socket_t fd, short what, void *arg);

static void
gw_ipc_schedule_retry(xqc_gw_ipc_t *ipc)
{
    struct timeval tv = { 1, 0 };
    evtimer_add(ipc->ev_retry, &tv);
}

static void
gw_ipc_on_retry(evutil_socket_t fd, short what, void *arg)
{
    xqc_gw_ipc_t *ipc = arg;
    (void)fd;
    (void)what;
    if (ipc->fd < 0) {
        gw_ipc_try_connect(ipc);
    }
}

static void
gw_ipc_connected(xqc_gw_ipc_t *ipc)
{
    evtimer_del(ipc->ev_retry);

    ipc->ev_read = event_new(ipc->eb, ipc->fd, EV_READ | EV_PERSIST,
        gw_ipc_on_readable, ipc);
    if (ipc->ev_read == NULL) {
        close(ipc->fd);
        ipc->fd = -1;
        gw_ipc_schedule_retry(ipc);
        return;
    }
    event_add(ipc->ev_read, NULL);

    ipc->stats.connects++;
    if (ipc->cbs.on_state) {
        ipc->cbs.on_state(ipc->ud, 1);
    }
}

static void
gw_ipc_disconnect(xqc_gw_ipc_t *ipc)
{
    if (ipc->ev_read != NULL) {
        event_free(ipc->ev_read);
        ipc->ev_read = NULL;
    }
    if (ipc->fd >= 0) {
        close(ipc->fd);
        ipc->fd = -1;
    }
    ipc->rx_len = 0;
    ipc->stats.disconnects++;
    if (ipc->cbs.on_state) {
        ipc->cbs.on_state(ipc->ud, 0);
    }
    gw_ipc_schedule_retry(ipc);
}

static void
gw_ipc_try_connect(xqc_gw_ipc_t *ipc)
{
    struct sockaddr_un addr;
    size_t path_len;
    int fd;
    int flags;

    path_len = strlen(ipc->sock_path);
    if (path_len == 0 || path_len >= sizeof(addr.sun_path)) {
        gw_ipc_schedule_retry(ipc);
        return;
    }

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        gw_ipc_schedule_retry(ipc);
        return;
    }
    flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        close(fd);
        gw_ipc_schedule_retry(ipc);
        return;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    memcpy(addr.sun_path, ipc->sock_path, path_len + 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        /* EINPROGRESS is treated like any other failure: on a local UDS
         * the connect completes immediately in practice, and the retry
         * timer covers the rare async case. */
        close(fd);
        gw_ipc_schedule_retry(ipc);
        return;
    }

    ipc->fd = fd;
    gw_ipc_connected(ipc);
}

static int
gw_ipc_rx_reserve(xqc_gw_ipc_t *ipc, size_t need)
{
    uint8_t *p;
    size_t cap;

    if (need <= ipc->rx_cap) {
        return 0;
    }
    cap = ipc->rx_cap ? ipc->rx_cap : 4096;
    while (cap < need) {
        cap *= 2;
    }
    if (cap > XQC_GW_IPC_RX_CAP) {
        cap = XQC_GW_IPC_RX_CAP;
    }
    if (cap < need) {
        return -1;
    }
    p = realloc(ipc->rx, cap);
    if (p == NULL) {
        return -1;
    }
    ipc->rx = p;
    ipc->rx_cap = cap;
    return 0;
}

/* returns 0 to keep draining, -1 when the connection must be dropped */
static int
gw_ipc_dispatch(xqc_gw_ipc_t *ipc, const uint8_t *msg, uint32_t len)
{
    xqc_gw_ipc_msg_t m;

    if (!xqc_gw_ipc_parse_message(msg, len, &m)) {
        ipc->stats.protocol_errors++;
        return 0;
    }
    if (m.version != XQC_GW_IPC_VERSION) {
        /* design: unknown version is a protocol error -> drop and reconnect */
        ipc->stats.protocol_errors++;
        return -1;
    }

    switch (m.kind) {
    case XQC_GW_IPC_KIND_DATA: {
        xqc_gw_ipc_data_t d;
        if (!xqc_gw_ipc_parse_data(&m, &d)) {
            ipc->stats.protocol_errors++;
            return 0;
        }
        ipc->stats.frames_rx++;
        if (ipc->cbs.on_frame) {
            ipc->cbs.on_frame(ipc->ud, &d);
        }
        break;
    }
    case XQC_GW_IPC_KIND_SUB_OK: {
        xqc_gw_ipc_sub_ok_t ok;
        if (!xqc_gw_ipc_parse_sub_ok(&m, &ok)) {
            ipc->stats.protocol_errors++;
            return 0;
        }
        ipc->stats.sub_ok_rx++;
        if (ipc->cbs.on_sub_ok) {
            ipc->cbs.on_sub_ok(ipc->ud, &ok);
        }
        break;
    }
    case XQC_GW_IPC_KIND_SUB_ERR: {
        xqc_gw_ipc_sub_err_t err;
        if (!xqc_gw_ipc_parse_sub_err(&m, &err)) {
            ipc->stats.protocol_errors++;
            return 0;
        }
        ipc->stats.sub_err_rx++;
        if (ipc->cbs.on_sub_err) {
            ipc->cbs.on_sub_err(ipc->ud, &err);
        }
        break;
    }
    default:
        /* SUB / UNSUB / PLI are gateway->SFU directions; the dispatcher
         * never sends them back. */
        ipc->stats.dropped_non_data++;
        break;
    }
    return 0;
}

static void
gw_ipc_drain(xqc_gw_ipc_t *ipc)
{
    while (ipc->rx_len >= 4) {
        uint32_t msg_len = xqc_gw_ipc_get_u32(ipc->rx);

        if (msg_len < 2 || msg_len > XQC_GW_IPC_MAX_MESSAGE_SIZE) {
            ipc->stats.protocol_errors++;
            gw_ipc_disconnect(ipc);
            return;
        }
        if (ipc->rx_len < 4u + msg_len) {
            break;
        }
        if (gw_ipc_dispatch(ipc, ipc->rx + 4, msg_len) != 0) {
            gw_ipc_disconnect(ipc);
            return;
        }
        memmove(ipc->rx, ipc->rx + 4 + msg_len, ipc->rx_len - 4 - msg_len);
        ipc->rx_len -= 4u + msg_len;
    }
}

static void
gw_ipc_on_readable(evutil_socket_t fd, short what, void *arg)
{
    xqc_gw_ipc_t *ipc = arg;
    uint8_t tmp[64 * 1024];
    (void)what;

    for (;;) {
        ssize_t n = recv(fd, tmp, sizeof(tmp), 0);
        if (n > 0) {
            if (ipc->rx_len + (size_t)n > XQC_GW_IPC_RX_CAP
                || gw_ipc_rx_reserve(ipc, ipc->rx_len + (size_t)n) != 0)
            {
                ipc->stats.protocol_errors++;
                gw_ipc_disconnect(ipc);
                return;
            }
            memcpy(ipc->rx + ipc->rx_len, tmp, (size_t)n);
            ipc->rx_len += (size_t)n;
            continue;
        }
        if (n == 0) {
            gw_ipc_disconnect(ipc);
            return;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        }
        gw_ipc_disconnect(ipc);
        return;
    }

    gw_ipc_drain(ipc);
}

static int
gw_ipc_send(xqc_gw_ipc_t *ipc, const uint8_t *buf, size_t len)
{
    size_t off = 0;

    if (ipc->fd < 0) {
        ipc->stats.send_failures++;
        return -1;
    }
    while (off < len) {
        ssize_t n = send(ipc->fd, buf + off, len - off, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            /* EAGAIN on a small control message means the dispatcher is
             * stalled; drop it rather than block the event loop. */
            ipc->stats.send_failures++;
            return -1;
        }
        off += (size_t)n;
    }
    return 0;
}

xqc_gw_ipc_t *
xqc_gw_ipc_create(struct event_base *eb, const char *sock_path,
    const xqc_gw_ipc_cbs_t *cbs, void *ud)
{
    xqc_gw_ipc_t *ipc;

    if (eb == NULL || sock_path == NULL || cbs == NULL) {
        return NULL;
    }
    if (strlen(sock_path) == 0
        || strlen(sock_path) >= sizeof(((struct sockaddr_un *)0)->sun_path))
    {
        return NULL;
    }

    ipc = calloc(1, sizeof(*ipc));
    if (ipc == NULL) {
        return NULL;
    }
    ipc->eb = eb;
    ipc->cbs = *cbs;
    ipc->ud = ud;
    ipc->fd = -1;
    snprintf(ipc->sock_path, sizeof(ipc->sock_path), "%s", sock_path);

    ipc->ev_retry = evtimer_new(eb, gw_ipc_on_retry, ipc);
    if (ipc->ev_retry == NULL) {
        free(ipc);
        return NULL;
    }

    gw_ipc_try_connect(ipc);
    return ipc;
}

void
xqc_gw_ipc_destroy(xqc_gw_ipc_t *ipc)
{
    if (ipc == NULL) {
        return;
    }
    if (ipc->ev_read != NULL) {
        event_free(ipc->ev_read);
    }
    if (ipc->ev_retry != NULL) {
        event_free(ipc->ev_retry);
    }
    if (ipc->fd >= 0) {
        close(ipc->fd);
    }
    free(ipc->rx);
    free(ipc);
}

int
xqc_gw_ipc_send_sub(xqc_gw_ipc_t *ipc, const char *name, uint16_t name_len)
{
    uint8_t buf[4 + 4 + XQC_GW_IPC_MAX_STREAM_NAME];
    size_t n;

    if (ipc == NULL) {
        return -1;
    }
    n = xqc_gw_ipc_serialize_sub(buf, sizeof(buf), name, name_len);
    if (n == 0) {
        ipc->stats.send_failures++;
        return -1;
    }
    return gw_ipc_send(ipc, buf, n);
}

int
xqc_gw_ipc_send_unsub(xqc_gw_ipc_t *ipc, uint32_t sid)
{
    uint8_t buf[10];
    size_t n;

    if (ipc == NULL) {
        return -1;
    }
    n = xqc_gw_ipc_serialize_unsub(buf, sizeof(buf), sid);
    if (n == 0) {
        ipc->stats.send_failures++;
        return -1;
    }
    return gw_ipc_send(ipc, buf, n);
}

int
xqc_gw_ipc_send_pli(xqc_gw_ipc_t *ipc, uint32_t sid)
{
    uint8_t buf[10];
    size_t n;

    if (ipc == NULL) {
        return -1;
    }
    n = xqc_gw_ipc_serialize_pli(buf, sizeof(buf), sid);
    if (n == 0) {
        ipc->stats.send_failures++;
        return -1;
    }
    return gw_ipc_send(ipc, buf, n);
}

int
xqc_gw_ipc_connected(const xqc_gw_ipc_t *ipc)
{
    return ipc != NULL && ipc->fd >= 0;
}

void
xqc_gw_ipc_get_stats(const xqc_gw_ipc_t *ipc, xqc_gw_ipc_stats_t *out)
{
    if (out == NULL) {
        return;
    }
    if (ipc == NULL) {
        memset(out, 0, sizeof(*out));
        return;
    }
    *out = ipc->stats;
}