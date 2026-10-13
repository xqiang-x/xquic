#ifndef _XQC_MOQ_GATEWAY_IPC_H_INCLUDED_
#define _XQC_MOQ_GATEWAY_IPC_H_INCLUDED_

/*
 * Frame IPC v1 client for the MoQ gateway (POSIX / libevent).
 *
 * Connects to the SFU FrameDispatcher UDS path, reconnects every second
 * after any failure, reads framed messages, and exposes typed callbacks.
 * Send helpers are safe to call from within callbacks.
 */

#include <event2/event.h>

#include "xqc_moq_gateway_frame_ipc.h"

typedef struct xqc_gw_ipc_s xqc_gw_ipc_t;

typedef struct xqc_gw_ipc_cbs_s {
    void (*on_frame)(void *ud, const xqc_gw_ipc_data_t *data);
    void (*on_sub_ok)(void *ud, const xqc_gw_ipc_sub_ok_t *ok);
    void (*on_sub_err)(void *ud, const xqc_gw_ipc_sub_err_t *err);
    /* 1 on connect, 0 on disconnect; transitions only */
    void (*on_state)(void *ud, int connected);
} xqc_gw_ipc_cbs_t;

typedef struct xqc_gw_ipc_stats_s {
    uint64_t    connects;
    uint64_t    disconnects;
    uint64_t    frames_rx;
    uint64_t    sub_ok_rx;
    uint64_t    sub_err_rx;
    uint64_t    dropped_non_data;   /* SUB/UNSUB/PLI/unknown from SFU side */
    uint64_t    protocol_errors;
    uint64_t    send_failures;
} xqc_gw_ipc_stats_t;

/* cbs and all callbacks must stay valid for the lifetime of the client.
 * The first connect attempt happens immediately; failures schedule a retry. */
xqc_gw_ipc_t *xqc_gw_ipc_create(struct event_base *eb, const char *sock_path,
    const xqc_gw_ipc_cbs_t *cbs, void *ud);

void xqc_gw_ipc_destroy(xqc_gw_ipc_t *ipc);

/* Return 0 on success, -1 when disconnected or the send failed. */
int xqc_gw_ipc_send_sub(xqc_gw_ipc_t *ipc, const char *name, uint16_t name_len);
int xqc_gw_ipc_send_unsub(xqc_gw_ipc_t *ipc, uint32_t sid);
int xqc_gw_ipc_send_pli(xqc_gw_ipc_t *ipc, uint32_t sid);

int xqc_gw_ipc_connected(const xqc_gw_ipc_t *ipc);

void xqc_gw_ipc_get_stats(const xqc_gw_ipc_t *ipc, xqc_gw_ipc_stats_t *out);

#endif /* _XQC_MOQ_GATEWAY_IPC_H_INCLUDED_ */