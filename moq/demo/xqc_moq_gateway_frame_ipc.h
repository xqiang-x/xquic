#ifndef _XQC_MOQ_GATEWAY_FRAME_IPC_H_INCLUDED_
#define _XQC_MOQ_GATEWAY_FRAME_IPC_H_INCLUDED_

/*
 * Frame IPC v1 contract (C twin).
 *
 * Mirrors rtc/common/sfu_protocol/FrameIpc.h in the tj_rtc repository with
 * identical numeric values and byte order, as a standalone C header. The two
 * repositories must not share a build dependency, so this file is a copy;
 * the contract is locked by unit tests on both sides
 * (tests/unittest/xqc_moq_gateway_test.c asserts every constant against its
 * literal value and exercises serialize/parse round trips).
 *
 * Topology: SFU is the server (FrameDispatcher), this gateway is the client.
 *
 * Wire format (all integers big endian):
 *   frame = [u32 msg_len][msg], msg_len = bytes of msg excluding itself
 *   msg   = [u8 version][u8 kind][body]
 *
 *   kind=DATA   body=[u32 sid][u8 frame_type][u8 flags][u64 pts_us][u64 seq][payload]
 *   kind=SUB    body=[u16 name_len][name]                       (gw -> SFU)
 *   kind=UNSUB  body=[u32 sid]                                  (gw -> SFU)
 *   kind=PLI    body=[u32 sid]                                  (gw -> SFU)
 *   kind=SUBOK  body=[u32 sid][u16 name_len][name]              (SFU -> gw)
 *   kind=SUBERR body=[u32 sid=0][u16 name_len][name][u8 reason] (SFU -> gw)
 *
 *   sid       : SFU-assigned per stream name (u32, non-zero, stable); DATA /
 *               UNSUB / PLI carry the sid only.
 *   frame_type: numeric values match frame_protoc::frame_type in tj_rtc.
 *   flags     : bit0 = key frame, other bits reserved as 0.
 *   pts_us    : SFU assembly time (monotonic, us).
 *   seq       : per-stream frame sequence, strictly increasing from 0.
 */

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>

#define XQC_GW_IPC_VERSION          1
#define XQC_GW_IPC_MAX_MESSAGE_SIZE (2 * 1024 * 1024)  /* per message */
#define XQC_GW_IPC_MAX_STREAM_NAME  255                /* name bytes */

/* kind values (frame_ipc::Kind) */
#define XQC_GW_IPC_KIND_DATA    0x01
#define XQC_GW_IPC_KIND_SUB     0x02
#define XQC_GW_IPC_KIND_UNSUB   0x03
#define XQC_GW_IPC_KIND_PLI     0x04
#define XQC_GW_IPC_KIND_SUB_OK  0x05
#define XQC_GW_IPC_KIND_SUB_ERR 0x06

/* frame_type values (frame_ipc::FrameType, frame_protoc::frame_type) */
#define XQC_GW_IPC_FRAME_AUDIO        0x01
#define XQC_GW_IPC_FRAME_VIDEO        0x02
#define XQC_GW_IPC_FRAME_VIDEO_IDR    0x03
#define XQC_GW_IPC_FRAME_VIDEO_PARAMS 0x04
#define XQC_GW_IPC_FRAME_DATA         0x05

/* DATA flags (frame_ipc::kFlagKey) */
#define XQC_GW_IPC_FLAG_KEY 0x01

/* SUBERR reasons (frame_ipc::SubErrReason) */
#define XQC_GW_IPC_SUB_ERR_TOO_MANY_STREAMS 1
#define XQC_GW_IPC_SUB_ERR_BAD_REQUEST      2

/* DATA fixed part: sid(4)+frame_type(1)+flags(1)+pts_us(8)+seq(8) */
#define XQC_GW_IPC_DATA_FIXED_SIZE 22
#define XQC_GW_IPC_DATA_MSG_SIZE   (2 + XQC_GW_IPC_DATA_FIXED_SIZE)

/* -- big-endian helpers ---------------------------------------------------- */

static inline void
xqc_gw_ipc_put_u16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)v;
}

static inline void
xqc_gw_ipc_put_u32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)v;
}

static inline void
xqc_gw_ipc_put_u64(uint8_t *p, uint64_t v)
{
    xqc_gw_ipc_put_u32(p, (uint32_t)(v >> 32));
    xqc_gw_ipc_put_u32(p + 4, (uint32_t)v);
}

static inline uint16_t
xqc_gw_ipc_get_u16(const uint8_t *p)
{
    return (uint16_t)(((uint16_t)p[0] << 8) | p[1]);
}

static inline uint32_t
xqc_gw_ipc_get_u32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8) | (uint32_t)p[3];
}

static inline uint64_t
xqc_gw_ipc_get_u64(const uint8_t *p)
{
    return ((uint64_t)xqc_gw_ipc_get_u32(p) << 32) | xqc_gw_ipc_get_u32(p + 4);
}

/* -- serialization (with the 4-byte length prefix) ------------------------- */
/* Return 0 when the capacity is insufficient (or the message is above the
 * cap); nothing is written in that case. */

static inline size_t
xqc_gw_ipc_serialize_data(uint8_t *out, size_t cap, uint32_t sid,
    uint8_t frame_type, uint8_t flags, uint64_t pts_us, uint64_t seq,
    const uint8_t *payload, uint32_t payload_len)
{
    uint32_t msg_len = XQC_GW_IPC_DATA_MSG_SIZE + payload_len;
    if (msg_len > XQC_GW_IPC_MAX_MESSAGE_SIZE || cap < 4u + msg_len) {
        return 0;
    }
    xqc_gw_ipc_put_u32(out, msg_len);
    out[4] = XQC_GW_IPC_VERSION;
    out[5] = XQC_GW_IPC_KIND_DATA;
    xqc_gw_ipc_put_u32(out + 6, sid);
    out[10] = frame_type;
    out[11] = flags;
    xqc_gw_ipc_put_u64(out + 12, pts_us);
    xqc_gw_ipc_put_u64(out + 20, seq);
    if (payload_len > 0 && payload != NULL) {
        memcpy(out + 28, payload, payload_len);
    }
    return 4u + msg_len;
}

static inline size_t
xqc_gw_ipc_serialize_sub(uint8_t *out, size_t cap, const char *name,
    uint16_t name_len)
{
    size_t msg_len;
    if (name == NULL || name_len == 0 || name_len > XQC_GW_IPC_MAX_STREAM_NAME) {
        return 0;
    }
    msg_len = 4u + name_len;
    if (cap < 4u + msg_len) {
        return 0;
    }
    xqc_gw_ipc_put_u32(out, (uint32_t)msg_len);
    out[4] = XQC_GW_IPC_VERSION;
    out[5] = XQC_GW_IPC_KIND_SUB;
    xqc_gw_ipc_put_u16(out + 6, name_len);
    memcpy(out + 8, name, name_len);
    return 4u + msg_len;
}

static inline size_t
xqc_gw_ipc_serialize_unsub(uint8_t *out, size_t cap, uint32_t sid)
{
    if (cap < 10) {
        return 0;
    }
    xqc_gw_ipc_put_u32(out, 6);
    out[4] = XQC_GW_IPC_VERSION;
    out[5] = XQC_GW_IPC_KIND_UNSUB;
    xqc_gw_ipc_put_u32(out + 6, sid);
    return 10;
}

static inline size_t
xqc_gw_ipc_serialize_pli(uint8_t *out, size_t cap, uint32_t sid)
{
    if (cap < 10) {
        return 0;
    }
    xqc_gw_ipc_put_u32(out, 6);
    out[4] = XQC_GW_IPC_VERSION;
    out[5] = XQC_GW_IPC_KIND_PLI;
    xqc_gw_ipc_put_u32(out + 6, sid);
    return 10;
}

static inline size_t
xqc_gw_ipc_serialize_sub_ok(uint8_t *out, size_t cap, uint32_t sid,
    const char *name, uint16_t name_len)
{
    size_t msg_len;
    if (name == NULL || name_len == 0 || name_len > XQC_GW_IPC_MAX_STREAM_NAME) {
        return 0;
    }
    msg_len = 8u + name_len;  /* ver+kind(2)+sid(4)+name_len(2)+name */
    if (cap < 4u + msg_len) {
        return 0;
    }
    xqc_gw_ipc_put_u32(out, (uint32_t)msg_len);
    out[4] = XQC_GW_IPC_VERSION;
    out[5] = XQC_GW_IPC_KIND_SUB_OK;
    xqc_gw_ipc_put_u32(out + 6, sid);
    xqc_gw_ipc_put_u16(out + 10, name_len);
    memcpy(out + 12, name, name_len);
    return 4u + msg_len;
}

static inline size_t
xqc_gw_ipc_serialize_sub_err(uint8_t *out, size_t cap, const char *name,
    uint16_t name_len, uint8_t reason)
{
    size_t msg_len;
    if (name == NULL || name_len == 0 || name_len > XQC_GW_IPC_MAX_STREAM_NAME) {
        return 0;
    }
    msg_len = 9u + name_len;  /* ver+kind(2)+sid(4)+name_len(2)+name+reason */
    if (cap < 4u + msg_len) {
        return 0;
    }
    xqc_gw_ipc_put_u32(out, (uint32_t)msg_len);
    out[4] = XQC_GW_IPC_VERSION;
    out[5] = XQC_GW_IPC_KIND_SUB_ERR;
    xqc_gw_ipc_put_u32(out + 6, 0);
    xqc_gw_ipc_put_u16(out + 10, name_len);
    memcpy(out + 12, name, name_len);
    out[12 + name_len] = reason;
    return 4u + msg_len;
}

/* -- parsing --------------------------------------------------------------- */
/* Parsed views point into the caller's buffer; no data is copied. */

typedef struct xqc_gw_ipc_msg_s {
    uint8_t         version;
    uint8_t         kind;
    const uint8_t  *body;
    uint32_t        body_len;
} xqc_gw_ipc_msg_t;

static inline bool
xqc_gw_ipc_parse_message(const uint8_t *msg, uint32_t len, xqc_gw_ipc_msg_t *out)
{
    if (msg == NULL || len < 2) {
        return false;
    }
    out->version = msg[0];
    out->kind = msg[1];
    out->body = msg + 2;
    out->body_len = len - 2;
    return true;
}

typedef struct xqc_gw_ipc_data_s {
    uint32_t        sid;
    uint8_t         frame_type;
    uint8_t         flags;
    uint64_t        pts_us;
    uint64_t        seq;
    const uint8_t  *payload;
    uint32_t        payload_len;
} xqc_gw_ipc_data_t;

static inline bool
xqc_gw_ipc_parse_data(const xqc_gw_ipc_msg_t *m, xqc_gw_ipc_data_t *out)
{
    if (m->kind != XQC_GW_IPC_KIND_DATA || m->body_len < XQC_GW_IPC_DATA_FIXED_SIZE) {
        return false;
    }
    out->sid = xqc_gw_ipc_get_u32(m->body);
    out->frame_type = m->body[4];
    out->flags = m->body[5];
    out->pts_us = xqc_gw_ipc_get_u64(m->body + 6);
    out->seq = xqc_gw_ipc_get_u64(m->body + 14);
    out->payload = m->body + XQC_GW_IPC_DATA_FIXED_SIZE;
    out->payload_len = m->body_len - XQC_GW_IPC_DATA_FIXED_SIZE;
    return true;
}

typedef struct xqc_gw_ipc_name_s {
    const char     *name;
    uint16_t        name_len;
} xqc_gw_ipc_name_t;

static inline bool
xqc_gw_ipc_parse_sub(const xqc_gw_ipc_msg_t *m, xqc_gw_ipc_name_t *out)
{
    uint16_t n;
    if (m->kind != XQC_GW_IPC_KIND_SUB || m->body_len < 2) {
        return false;
    }
    n = xqc_gw_ipc_get_u16(m->body);
    if (n == 0 || n > XQC_GW_IPC_MAX_STREAM_NAME || m->body_len < 2u + n) {
        return false;
    }
    out->name = (const char *)(m->body + 2);
    out->name_len = n;
    return true;
}

static inline bool
xqc_gw_ipc_parse_sid(const xqc_gw_ipc_msg_t *m, uint32_t *sid)
{
    if ((m->kind != XQC_GW_IPC_KIND_UNSUB && m->kind != XQC_GW_IPC_KIND_PLI) ||
        m->body_len < 4)
    {
        return false;
    }
    *sid = xqc_gw_ipc_get_u32(m->body);
    return *sid != 0;
}

typedef struct xqc_gw_ipc_sub_ok_s {
    uint32_t        sid;
    const char     *name;
    uint16_t        name_len;
} xqc_gw_ipc_sub_ok_t;

static inline bool
xqc_gw_ipc_parse_sub_ok(const xqc_gw_ipc_msg_t *m, xqc_gw_ipc_sub_ok_t *out)
{
    uint16_t n;
    if (m->kind != XQC_GW_IPC_KIND_SUB_OK || m->body_len < 6) {
        return false;
    }
    out->sid = xqc_gw_ipc_get_u32(m->body);
    n = xqc_gw_ipc_get_u16(m->body + 4);
    if (out->sid == 0 || n == 0 || n > XQC_GW_IPC_MAX_STREAM_NAME ||
        m->body_len < 6u + n)
    {
        return false;
    }
    out->name = (const char *)(m->body + 6);
    out->name_len = n;
    return true;
}

typedef struct xqc_gw_ipc_sub_err_s {
    const char     *name;
    uint16_t        name_len;
    uint8_t         reason;
} xqc_gw_ipc_sub_err_t;

static inline bool
xqc_gw_ipc_parse_sub_err(const xqc_gw_ipc_msg_t *m, xqc_gw_ipc_sub_err_t *out)
{
    uint16_t n;
    if (m->kind != XQC_GW_IPC_KIND_SUB_ERR || m->body_len < 7) {
        return false;
    }
    n = xqc_gw_ipc_get_u16(m->body + 4);
    if (n == 0 || n > XQC_GW_IPC_MAX_STREAM_NAME || m->body_len < 7u + n) {
        return false;
    }
    out->name = (const char *)(m->body + 6);
    out->name_len = n;
    out->reason = m->body[6 + n];
    return true;
}

#endif /* _XQC_MOQ_GATEWAY_FRAME_IPC_H_INCLUDED_ */