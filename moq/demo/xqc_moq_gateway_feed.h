#ifndef _XQC_MOQ_GATEWAY_FEED_H_INCLUDED_
#define _XQC_MOQ_GATEWAY_FEED_H_INCLUDED_

/*
 * Engine-free relay logic for the MoQ gateway.
 *
 * Frame mapping (IPC frame_type -> MoQ), params cache, and the downstream
 * subscription table. No xquic engine objects are touched here: the MoQ
 * write call is injected so unit tests can run without an engine. The
 * executable supplies a shim over xqc_moq_write_video_frame.
 */

#include <stddef.h>
#include <stdint.h>

#include <moq/xqc_moq.h>

#define XQC_GW_STREAM_NAME_MAX 256                      /* incl. NUL */
#define XQC_GW_NS_MAX          (3 + 255 + 1)            /* "tj/" + name */
#define XQC_GW_NS_PREFIX       "tj/"
#define XQC_GW_TRACK_NAME      "video"
#define XQC_GW_PARAMS_CAP      (8 * 1024)               /* params cache cap */
#define XQC_GW_FANOUT_CAP      256                      /* subscriptions total */

/* ── stream state ────────────────────────────────────────────────────────── */

typedef struct xqc_gw_stream_s {
    char            name[XQC_GW_STREAM_NAME_MAX];       /* SFU stream id */
    char            track_namespace[XQC_GW_NS_MAX];     /* "tj/<name>" */

    uint32_t        sid;            /* IPC sid from SUB_OK; 0 = not active */
    int             sub_pending;    /* SUB sent, SUB_OK outstanding */
    uint64_t        sub_retry_us;   /* earliest next SUB after SUB_ERR */
    uint64_t        last_pli_us;    /* last PLI sent for this stream */

    uint8_t        *params;         /* cached VIDEO_PARAMS payload, owned */
    uint32_t        params_len;

    /* stats */
    uint64_t        frames_relayed;    /* KEY + DELTA frames produced */
    uint64_t        frames_ignored;    /* audio / data / unknown / empty */
    uint64_t        params_cached;     /* VIDEO_PARAMS frames stored */
} xqc_gw_stream_t;

void xqc_gw_stream_init(xqc_gw_stream_t *st, const char *name);
void xqc_gw_stream_clear(xqc_gw_stream_t *st);

/* Registry helpers over a caller-owned array. */
xqc_gw_stream_t *xqc_gw_stream_find_by_sid(xqc_gw_stream_t *items, int count,
    uint32_t sid);
xqc_gw_stream_t *xqc_gw_stream_find_by_name(xqc_gw_stream_t *items, int count,
    const char *name, size_t name_len);
xqc_gw_stream_t *xqc_gw_stream_find_by_track(xqc_gw_stream_t *items, int count,
    const char *ns, size_t ns_len, const char *name, size_t name_len);

/* ── frame feed mapping ──────────────────────────────────────────────────── */

typedef enum {
    XQC_GW_FRAME_IGNORE = 0,    /* audio/data/unknown/empty: dropped */
    XQC_GW_FRAME_PARAMS,        /* cached for the next key frame */
    XQC_GW_FRAME_KEY,
    XQC_GW_FRAME_DELTA,
} xqc_gw_frame_action_t;

typedef struct xqc_gw_feed_out_s {
    xqc_gw_frame_action_t   action;
    xqc_moq_video_frame_t   frame;  /* valid for KEY / DELTA */
    uint8_t                *merged; /* owned params||payload buffer, or NULL */
} xqc_gw_feed_out_t;

/*
 * Map one IPC DATA frame. For KEY frames with a non-empty params cache the
 * payload is prepended into an owned buffer (out->merged); otherwise
 * out->frame.video_data points at payload. Call
 * xqc_gw_feed_out_cleanup() after the frame has been delivered.
 */
xqc_gw_frame_action_t xqc_gw_feed_on_frame(xqc_gw_stream_t *st,
    uint8_t frame_type, uint8_t flags, uint64_t pts_us, uint64_t seq,
    const uint8_t *payload, uint32_t payload_len, xqc_gw_feed_out_t *out);

void xqc_gw_feed_out_cleanup(xqc_gw_feed_out_t *out);

/* ── fan-out ─────────────────────────────────────────────────────────────── */

typedef xqc_int_t (*xqc_gw_write_video_pt)(void *session, uint64_t subscribe_id,
    xqc_moq_track_t *track, xqc_moq_video_frame_t *frame);

typedef struct xqc_gw_sub_s {
    int              active;
    xqc_gw_stream_t *stream;
    uint64_t         subscribe_id;
    xqc_moq_track_t *track;
    void            *session;       /* per-connection cookie */
    int              waiting_key;   /* 1 until the first KEY is delivered */
} xqc_gw_sub_t;

typedef struct xqc_gw_fanout_s {
    xqc_gw_sub_t            *subs;
    int                      cap;

    xqc_gw_write_video_pt    write_video;

    /* stats */
    uint64_t                 writes;
    uint64_t                 dropped_wait_key;
    uint64_t                 write_errors;
    uint64_t                 no_sub;    /* frame with no matching sub */
} xqc_gw_fanout_t;

int xqc_gw_fanout_init(xqc_gw_fanout_t *f, int cap, xqc_gw_write_video_pt w);
void xqc_gw_fanout_destroy(xqc_gw_fanout_t *f);

/* Returns NULL when the table is full. */
xqc_gw_sub_t *xqc_gw_fanout_add(xqc_gw_fanout_t *f, xqc_gw_stream_t *st,
    uint64_t subscribe_id, xqc_moq_track_t *track, void *session);

void xqc_gw_fanout_remove_session(xqc_gw_fanout_t *f, void *session);

int xqc_gw_fanout_refs_on_stream(const xqc_gw_fanout_t *f,
    const xqc_gw_stream_t *st);

/* Deliver one mapped frame to every active subscription of the stream.
 * DELTA frames are suppressed per subscription until its first KEY.
 * IGNORE / PARAMS actions deliver nothing. */
void xqc_gw_fanout_deliver_video(xqc_gw_fanout_t *f, xqc_gw_stream_t *st,
    xqc_gw_feed_out_t *out);

#endif /* _XQC_MOQ_GATEWAY_FEED_H_INCLUDED_ */