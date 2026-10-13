/**
 * @copyright Copyright (c) 2022, Alibaba Group Holding Limited
 */

#include "xqc_moq_gateway_test.h"

#include <CUnit/CUnit.h>

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <event2/event.h>

#include "moq/demo/xqc_moq_gateway_feed.h"
#include "moq/demo/xqc_moq_gateway_ipc.h"

#include "xquic/xquic.h"
#include "xquic/xqc_errno.h"
#include "xquic/xqc_http3.h"
#include "xquic/xqc_webtransport.h"
#include "src/http3/xqc_h3_defs.h"
#include "src/http3/xqc_h3_conn.h"
#include "src/http3/xqc_h3_stream.h"
#include "src/http3/xqc_h3_request.h"
#include "src/http3/xqc_webtransport.h"
#include "src/http3/frame/xqc_h3_frame.h"
#include "src/transport/xqc_engine.h"
#include "src/transport/xqc_stream.h"
#include "src/common/utils/vint/xqc_variable_len_int.h"
#include "moq/xqc_moq.h"
#include "moq/moq_transport/xqc_moq_message.h"
#include "moq/moq_transport/xqc_moq_session.h"
#include "moq/moq_transport/xqc_moq_stream.h"
#include "moq/moq_transport/xqc_moq_stream_webtransport.h"
#include "xqc_common_test.h"

/* ── mock writer for the fan-out tests ───────────────────────────────────── */

typedef struct xqc_gw_test_writer_s {
    int                     calls;
    int                     fail;
    void                   *last_session;
    uint64_t                last_subscribe_id;
    xqc_moq_track_t        *last_track;
    xqc_moq_video_frame_t   last_frame;
    uint8_t                 bytes[128];
    uint64_t                bytes_len;
} xqc_gw_test_writer_t;

static xqc_gw_test_writer_t g_gw_writer;

static xqc_int_t
gw_test_write_video(void *session, uint64_t subscribe_id,
    xqc_moq_track_t *track, xqc_moq_video_frame_t *frame)
{
    g_gw_writer.calls++;
    g_gw_writer.last_session = session;
    g_gw_writer.last_subscribe_id = subscribe_id;
    g_gw_writer.last_track = track;
    g_gw_writer.last_frame = *frame;
    g_gw_writer.bytes_len = 0;
    if (frame->video_data != NULL && frame->video_len > 0
        && frame->video_len <= sizeof(g_gw_writer.bytes))
    {
        memcpy(g_gw_writer.bytes, frame->video_data, (size_t)frame->video_len);
        g_gw_writer.bytes_len = frame->video_len;
    }
    return g_gw_writer.fail ? -1 : 0;
}

static void
gw_test_writer_reset(void)
{
    memset(&g_gw_writer, 0, sizeof(g_gw_writer));
}

/**
 * Test: IPC DATA frame_type -> MoQ mapping, sequence / timestamp / payload
 * pass-through, and drop accounting for audio / data / unknown / empty frames.
 */
void
xqc_test_moq_gateway_frame_map(void)
{
    xqc_gw_stream_t st;
    xqc_gw_feed_out_t out;
    static const uint8_t key[] = { 0xAA, 0xBB };
    static const uint8_t delta[] = { 0x01 };

    xqc_gw_stream_init(&st, "cam0");
    CU_ASSERT_STRING_EQUAL(st.name, "cam0");
    CU_ASSERT_STRING_EQUAL(st.track_namespace, "tj/cam0");
    CU_ASSERT_EQUAL(st.sid, 0);

    /* VIDEO_IDR is a key frame even without the key flag */
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&st, XQC_GW_IPC_FRAME_VIDEO_IDR, 0,
        1000, 5, key, sizeof(key), &out), XQC_GW_FRAME_KEY);
    CU_ASSERT_EQUAL(out.frame.type, XQC_MOQ_VIDEO_KEY);
    CU_ASSERT_EQUAL(out.frame.seq_num, 5);
    CU_ASSERT_EQUAL(out.frame.timestamp_us, 1000);
    CU_ASSERT_PTR_EQUAL(out.frame.video_data, key);     /* zero-copy, no params */
    CU_ASSERT_EQUAL(out.frame.video_len, sizeof(key));
    CU_ASSERT_PTR_NULL(out.merged);
    CU_ASSERT_EQUAL(st.frames_relayed, 1);
    xqc_gw_feed_out_cleanup(&out);

    /* VIDEO honors the key flag */
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&st, XQC_GW_IPC_FRAME_VIDEO,
        XQC_GW_IPC_FLAG_KEY, 2000, 6, delta, sizeof(delta), &out),
        XQC_GW_FRAME_KEY);
    CU_ASSERT_EQUAL(out.frame.type, XQC_MOQ_VIDEO_KEY);
    xqc_gw_feed_out_cleanup(&out);

    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&st, XQC_GW_IPC_FRAME_VIDEO, 0,
        3000, 7, delta, sizeof(delta), &out), XQC_GW_FRAME_DELTA);
    CU_ASSERT_EQUAL(out.frame.type, XQC_MOQ_VIDEO_DELTA);
    CU_ASSERT_PTR_EQUAL(out.frame.video_data, delta);
    CU_ASSERT_EQUAL(out.frame.seq_num, 7);
    CU_ASSERT_EQUAL(st.frames_relayed, 3);
    xqc_gw_feed_out_cleanup(&out);

    /* audio / data / unknown types / empty payloads are dropped and counted */
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&st, XQC_GW_IPC_FRAME_AUDIO, 0, 0, 8,
        delta, sizeof(delta), &out), XQC_GW_FRAME_IGNORE);
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&st, XQC_GW_IPC_FRAME_DATA, 0, 0, 9,
        delta, sizeof(delta), &out), XQC_GW_FRAME_IGNORE);
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&st, 0x77, 0, 0, 10,
        delta, sizeof(delta), &out), XQC_GW_FRAME_IGNORE);
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&st, XQC_GW_IPC_FRAME_VIDEO_IDR, 0, 0, 11,
        NULL, 0, &out), XQC_GW_FRAME_IGNORE);
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&st, XQC_GW_IPC_FRAME_VIDEO, 0, 0, 12,
        NULL, 0, &out), XQC_GW_FRAME_IGNORE);
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&st, XQC_GW_IPC_FRAME_VIDEO_PARAMS, 0, 0, 13,
        NULL, 0, &out), XQC_GW_FRAME_IGNORE);
    CU_ASSERT_EQUAL(st.frames_relayed, 3);
    CU_ASSERT_EQUAL(st.frames_ignored, 6);
    CU_ASSERT_PTR_NULL(st.params);
    CU_ASSERT_PTR_NULL(out.merged);

    xqc_gw_stream_clear(&st);
}

/**
 * Test: params caching (append order, key-frame prefix merge, clear after
 * merge, 8 KiB cap fallback, ignore counters).
 */
void
xqc_test_moq_gateway_params_merge(void)
{
    xqc_gw_stream_t st;
    xqc_gw_feed_out_t out;
    static const uint8_t p1[] = { 'P', '1' };
    static const uint8_t p2[] = { 'P', '2' };
    static const uint8_t kf[] = { 'K' };
    uint8_t big1[5000];
    uint8_t big2[4000];

    xqc_gw_stream_init(&st, "cam0");

    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&st, XQC_GW_IPC_FRAME_VIDEO_PARAMS, 0,
        0, 0, p1, sizeof(p1), &out), XQC_GW_FRAME_PARAMS);
    CU_ASSERT_EQUAL(st.params_len, 2);
    CU_ASSERT_EQUAL(st.params_cached, 1);
    CU_ASSERT_EQUAL(st.frames_relayed, 0);
    CU_ASSERT_EQUAL(memcmp(st.params, "P1", 2), 0);

    /* consecutive params frames append in arrival order */
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&st, XQC_GW_IPC_FRAME_VIDEO_PARAMS, 0,
        0, 1, p2, sizeof(p2), &out), XQC_GW_FRAME_PARAMS);
    CU_ASSERT_EQUAL(st.params_len, 4);
    CU_ASSERT_EQUAL(st.params_cached, 2);
    CU_ASSERT_EQUAL(memcmp(st.params, "P1P2", 4), 0);

    /* the next key frame carries the params as prefix, then the cache clears */
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&st, XQC_GW_IPC_FRAME_VIDEO_IDR, 0,
        10, 2, kf, sizeof(kf), &out), XQC_GW_FRAME_KEY);
    CU_ASSERT_PTR_NOT_NULL(out.merged);
    CU_ASSERT_PTR_EQUAL(out.frame.video_data, out.merged);
    CU_ASSERT_EQUAL(out.frame.video_len, 5);
    CU_ASSERT_EQUAL(memcmp(out.frame.video_data, "P1P2K", 5), 0);
    CU_ASSERT_PTR_NULL(st.params);
    CU_ASSERT_EQUAL(st.params_len, 0);
    xqc_gw_feed_out_cleanup(&out);

    /* delta frames never merge */
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&st, XQC_GW_IPC_FRAME_VIDEO, 0,
        20, 3, kf, sizeof(kf), &out), XQC_GW_FRAME_DELTA);
    CU_ASSERT_PTR_NULL(out.merged);
    CU_ASSERT_PTR_EQUAL(out.frame.video_data, kf);
    xqc_gw_feed_out_cleanup(&out);

    /* a single params frame is enough to prefix the next key frame */
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&st, XQC_GW_IPC_FRAME_VIDEO_PARAMS, 0,
        0, 4, p1, sizeof(p1), &out), XQC_GW_FRAME_PARAMS);
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&st, XQC_GW_IPC_FRAME_VIDEO_IDR, 0,
        30, 5, kf, sizeof(kf), &out), XQC_GW_FRAME_KEY);
    CU_ASSERT_EQUAL(out.frame.video_len, 3);
    CU_ASSERT_EQUAL(memcmp(out.frame.video_data, "P1K", 3), 0);
    xqc_gw_feed_out_cleanup(&out);

    /* above the 8 KiB cap only the newest params frame is kept */
    memset(big1, 'A', sizeof(big1));
    memset(big2, 'B', sizeof(big2));
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&st, XQC_GW_IPC_FRAME_VIDEO_PARAMS, 0,
        0, 6, big1, sizeof(big1), &out), XQC_GW_FRAME_PARAMS);
    CU_ASSERT_EQUAL(st.params_len, sizeof(big1));
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&st, XQC_GW_IPC_FRAME_VIDEO_PARAMS, 0,
        0, 7, big2, sizeof(big2), &out), XQC_GW_FRAME_PARAMS);
    CU_ASSERT_EQUAL(st.params_len, sizeof(big2));
    CU_ASSERT_EQUAL(st.params_cached, 5);
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&st, XQC_GW_IPC_FRAME_VIDEO_IDR, 0,
        40, 8, kf, sizeof(kf), &out), XQC_GW_FRAME_KEY);
    CU_ASSERT_EQUAL(out.frame.video_len, sizeof(big2) + 1);
    CU_ASSERT_EQUAL(out.frame.video_data[0], 'B');
    CU_ASSERT_EQUAL(out.frame.video_data[sizeof(big2)], 'K');
    xqc_gw_feed_out_cleanup(&out);
    CU_ASSERT_PTR_NULL(st.params);

    xqc_gw_stream_clear(&st);
}

/**
 * Test: fan-out table — table-full refusal, per-stream refs, waiting-key
 * gating, write-failure isolation, session removal, no-sub accounting.
 */
void
xqc_test_moq_gateway_fanout(void)
{
    xqc_gw_stream_t s0, s1;
    xqc_gw_fanout_t f;
    xqc_gw_sub_t *sub0, *sub1;
    xqc_gw_feed_out_t out;
    static const uint8_t kf[] = { 0x0A };
    static const uint8_t df[] = { 0x0B };
    void *sess0 = (void *)(uintptr_t)0x2000;
    void *sess1 = (void *)(uintptr_t)0x2001;
    xqc_moq_track_t *t0 = (xqc_moq_track_t *)(uintptr_t)0x1000;
    xqc_moq_track_t *t1 = (xqc_moq_track_t *)(uintptr_t)0x1001;

    xqc_gw_stream_init(&s0, "cam0");
    xqc_gw_stream_init(&s1, "cam1");
    gw_test_writer_reset();
    CU_ASSERT_EQUAL(xqc_gw_fanout_init(&f, 2, gw_test_write_video), 0);

    CU_ASSERT_EQUAL(xqc_gw_fanout_refs_on_stream(&f, &s0), 0);
    sub0 = xqc_gw_fanout_add(&f, &s0, 7, t0, sess0);
    sub1 = xqc_gw_fanout_add(&f, &s0, 8, t1, sess1);
    CU_ASSERT_PTR_NOT_NULL(sub0);
    CU_ASSERT_PTR_NOT_NULL(sub1);
    CU_ASSERT_EQUAL(sub0->waiting_key, 1);
    /* a cap-2 table refuses the third subscription */
    CU_ASSERT_PTR_NULL(xqc_gw_fanout_add(&f, &s1, 9, t0, sess0));
    CU_ASSERT_EQUAL(xqc_gw_fanout_refs_on_stream(&f, &s0), 2);
    CU_ASSERT_EQUAL(xqc_gw_fanout_refs_on_stream(&f, &s1), 0);

    /* deltas are suppressed until the first key frame per subscription */
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&s0, XQC_GW_IPC_FRAME_VIDEO, 0, 1, 1,
        df, sizeof(df), &out), XQC_GW_FRAME_DELTA);
    xqc_gw_fanout_deliver_video(&f, &s0, &out);
    CU_ASSERT_EQUAL(f.writes, 0);
    CU_ASSERT_EQUAL(f.dropped_wait_key, 2);
    CU_ASSERT_EQUAL(f.write_errors, 0);
    xqc_gw_feed_out_cleanup(&out);

    /* the key frame unlocks both subscriptions */
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&s0, XQC_GW_IPC_FRAME_VIDEO_IDR, 0, 2, 2,
        kf, sizeof(kf), &out), XQC_GW_FRAME_KEY);
    xqc_gw_fanout_deliver_video(&f, &s0, &out);
    CU_ASSERT_EQUAL(f.writes, 2);
    CU_ASSERT_EQUAL(f.dropped_wait_key, 2);
    CU_ASSERT_EQUAL(sub0->waiting_key, 0);
    CU_ASSERT_EQUAL(sub1->waiting_key, 0);
    CU_ASSERT_EQUAL(g_gw_writer.calls, 2);
    CU_ASSERT_PTR_EQUAL(g_gw_writer.last_session, sess1);
    CU_ASSERT_EQUAL(g_gw_writer.last_subscribe_id, 8);
    CU_ASSERT_PTR_EQUAL(g_gw_writer.last_track, t1);
    CU_ASSERT_EQUAL(g_gw_writer.last_frame.type, XQC_MOQ_VIDEO_KEY);
    CU_ASSERT_EQUAL(g_gw_writer.last_frame.seq_num, 2);
    CU_ASSERT_EQUAL(g_gw_writer.bytes_len, sizeof(kf));
    CU_ASSERT_EQUAL(g_gw_writer.bytes[0], 0x0A);
    xqc_gw_feed_out_cleanup(&out);

    /* deltas now flow to both subscriptions */
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&s0, XQC_GW_IPC_FRAME_VIDEO, 0, 3, 3,
        df, sizeof(df), &out), XQC_GW_FRAME_DELTA);
    xqc_gw_fanout_deliver_video(&f, &s0, &out);
    CU_ASSERT_EQUAL(f.writes, 4);
    xqc_gw_feed_out_cleanup(&out);

    /* no subscription on s1: frame counted but not delivered */
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&s1, XQC_GW_IPC_FRAME_VIDEO_IDR, 0, 4, 0,
        kf, sizeof(kf), &out), XQC_GW_FRAME_KEY);
    xqc_gw_fanout_deliver_video(&f, &s1, &out);
    CU_ASSERT_EQUAL(f.no_sub, 1);
    CU_ASSERT_EQUAL(f.writes, 4);
    xqc_gw_feed_out_cleanup(&out);

    /* write failures are isolated per subscription */
    g_gw_writer.fail = 1;
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&s0, XQC_GW_IPC_FRAME_VIDEO_IDR, 0, 5, 4,
        kf, sizeof(kf), &out), XQC_GW_FRAME_KEY);
    xqc_gw_fanout_deliver_video(&f, &s0, &out);
    CU_ASSERT_EQUAL(f.writes, 4);
    CU_ASSERT_EQUAL(f.write_errors, 2);
    xqc_gw_feed_out_cleanup(&out);
    g_gw_writer.fail = 0;

    /* removing one session drops only its subscriptions */
    xqc_gw_fanout_remove_session(&f, sess1);
    CU_ASSERT_EQUAL(xqc_gw_fanout_refs_on_stream(&f, &s0), 1);
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&s0, XQC_GW_IPC_FRAME_VIDEO_IDR, 0, 6, 5,
        kf, sizeof(kf), &out), XQC_GW_FRAME_KEY);
    xqc_gw_fanout_deliver_video(&f, &s0, &out);
    CU_ASSERT_EQUAL(f.writes, 5);
    CU_ASSERT_PTR_EQUAL(g_gw_writer.last_session, sess0);
    xqc_gw_feed_out_cleanup(&out);

    /* params and ignored frames deliver nothing */
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&s0, XQC_GW_IPC_FRAME_VIDEO_PARAMS, 0, 0, 6,
        kf, sizeof(kf), &out), XQC_GW_FRAME_PARAMS);
    xqc_gw_fanout_deliver_video(&f, &s0, &out);
    CU_ASSERT_EQUAL(f.writes, 5);
    xqc_gw_feed_out_cleanup(&out);
    CU_ASSERT_EQUAL(xqc_gw_feed_on_frame(&s0, XQC_GW_IPC_FRAME_AUDIO, 0, 0, 7,
        kf, sizeof(kf), &out), XQC_GW_FRAME_IGNORE);
    xqc_gw_fanout_deliver_video(&f, &s0, &out);
    CU_ASSERT_EQUAL(f.writes, 5);
    CU_ASSERT_EQUAL(f.no_sub, 1);

    xqc_gw_fanout_destroy(&f);
    xqc_gw_stream_clear(&s0);
    xqc_gw_stream_clear(&s1);
}

/* ── IPC client test ─────────────────────────────────────────────────────── */

typedef struct xqc_gw_test_evs_s {
    int         on_state_calls;
    int         connected;
    int         connects;
    int         disconnects;

    int         frames;
    uint32_t    last_frame_sid;
    uint8_t     last_frame_type;
    uint8_t     last_frame_flags;
    uint64_t    last_frame_pts;
    uint64_t    last_frame_seq;
    uint64_t    last_frame_len;
    uint8_t     last_frame_payload[64];

    int         sub_oks;
    uint32_t    last_sub_ok_sid;
    char        last_sub_ok_name[64];

    int         sub_errs;
    uint8_t     last_sub_err_reason;
    char        last_sub_err_name[64];
} xqc_gw_test_evs_t;

static xqc_gw_test_evs_t g_gw_evs;

static void
gw_test_on_frame(void *ud, const xqc_gw_ipc_data_t *d)
{
    xqc_gw_test_evs_t *evs = ud;
    uint32_t copy = d->payload_len;

    evs->frames++;
    evs->last_frame_sid = d->sid;
    evs->last_frame_type = d->frame_type;
    evs->last_frame_flags = d->flags;
    evs->last_frame_pts = d->pts_us;
    evs->last_frame_seq = d->seq;
    evs->last_frame_len = d->payload_len;
    if (copy > sizeof(evs->last_frame_payload)) {
        copy = sizeof(evs->last_frame_payload);
    }
    if (copy > 0) {
        memcpy(evs->last_frame_payload, d->payload, copy);
    }
}

static void
gw_test_on_sub_ok(void *ud, const xqc_gw_ipc_sub_ok_t *ok)
{
    xqc_gw_test_evs_t *evs = ud;
    uint16_t copy = ok->name_len;

    evs->sub_oks++;
    evs->last_sub_ok_sid = ok->sid;
    if (copy >= sizeof(evs->last_sub_ok_name)) {
        copy = sizeof(evs->last_sub_ok_name) - 1;
    }
    memcpy(evs->last_sub_ok_name, ok->name, copy);
    evs->last_sub_ok_name[copy] = '\0';
}

static void
gw_test_on_sub_err(void *ud, const xqc_gw_ipc_sub_err_t *err)
{
    xqc_gw_test_evs_t *evs = ud;
    uint16_t copy = err->name_len;

    evs->sub_errs++;
    evs->last_sub_err_reason = err->reason;
    if (copy >= sizeof(evs->last_sub_err_name)) {
        copy = sizeof(evs->last_sub_err_name) - 1;
    }
    memcpy(evs->last_sub_err_name, err->name, copy);
    evs->last_sub_err_name[copy] = '\0';
}

static void
gw_test_on_state(void *ud, int connected)
{
    xqc_gw_test_evs_t *evs = ud;

    evs->on_state_calls++;
    if (connected && !evs->connected) {
        evs->connects++;
    }
    if (!connected && evs->connected) {
        evs->disconnects++;
    }
    evs->connected = connected;
}

static int
gw_test_server_open(char *path_out, size_t path_cap)
{
    struct sockaddr_un addr;
    char path[96];
    size_t path_len;
    int fd;

    snprintf(path, sizeof(path), "/tmp/xqc_gw_ipc_test_%d.sock", (int)getpid());
    path_len = strlen(path);
    if (path_len + 1 > path_cap || path_len >= sizeof(addr.sun_path)) {
        return -1;
    }
    unlink(path);

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    memcpy(addr.sun_path, path, path_len + 1);
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0
        || listen(fd, 4) < 0)
    {
        close(fd);
        return -1;
    }
    memcpy(path_out, path, path_len + 1);
    return fd;
}

static int
gw_test_server_accept(int lfd)
{
    struct timeval tv = { 2, 0 };
    int fd;

    for (;;) {
        fd = accept(lfd, NULL, NULL);
        if (fd >= 0) {
            setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            return fd;
        }
        if (errno != EINTR) {
            return -1;
        }
    }
}

static int
gw_test_server_send(int fd, const uint8_t *buf, size_t len)
{
    size_t off = 0;

    while (off < len) {
        ssize_t n = write(fd, buf + off, len - off);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        off += (size_t)n;
    }
    return 0;
}

static ssize_t
gw_test_server_recv(int fd, uint8_t *buf, size_t cap)
{
    ssize_t n;

    for (;;) {
        n = recv(fd, buf, cap, 0);
        if (n >= 0) {
            return n;
        }
        if (errno == EINTR) {
            continue;
        }
        return -1;
    }
}

static void
gw_test_loop_once(struct event_base *eb)
{
    CU_ASSERT_EQUAL(event_base_loop(eb, EVLOOP_ONCE), 0);
}

/**
 * Test: Frame IPC v1 contract — every constant is asserted against its
 * literal value and every message kind survives a serialize/parse round trip.
 */
static void
gw_test_ipc_contract(void)
{
    uint8_t buf[64];
    uint8_t databuf[XQC_GW_IPC_DATA_MSG_SIZE + 8];
    xqc_gw_ipc_msg_t m;
    xqc_gw_ipc_name_t name;
    xqc_gw_ipc_data_t data;
    xqc_gw_ipc_sub_ok_t ok;
    xqc_gw_ipc_sub_err_t err;
    uint32_t sid;
    size_t n;

    /* literal values are shared with rtc/common/sfu_protocol/FrameIpc.h */
    CU_ASSERT_EQUAL(XQC_GW_IPC_VERSION, 1);
    CU_ASSERT_EQUAL(XQC_GW_IPC_MAX_MESSAGE_SIZE, 2 * 1024 * 1024);
    CU_ASSERT_EQUAL(XQC_GW_IPC_MAX_STREAM_NAME, 255);

    CU_ASSERT_EQUAL(XQC_GW_IPC_KIND_DATA, 0x01);
    CU_ASSERT_EQUAL(XQC_GW_IPC_KIND_SUB, 0x02);
    CU_ASSERT_EQUAL(XQC_GW_IPC_KIND_UNSUB, 0x03);
    CU_ASSERT_EQUAL(XQC_GW_IPC_KIND_PLI, 0x04);
    CU_ASSERT_EQUAL(XQC_GW_IPC_KIND_SUB_OK, 0x05);
    CU_ASSERT_EQUAL(XQC_GW_IPC_KIND_SUB_ERR, 0x06);

    CU_ASSERT_EQUAL(XQC_GW_IPC_FRAME_AUDIO, 0x01);
    CU_ASSERT_EQUAL(XQC_GW_IPC_FRAME_VIDEO, 0x02);
    CU_ASSERT_EQUAL(XQC_GW_IPC_FRAME_VIDEO_IDR, 0x03);
    CU_ASSERT_EQUAL(XQC_GW_IPC_FRAME_VIDEO_PARAMS, 0x04);
    CU_ASSERT_EQUAL(XQC_GW_IPC_FRAME_DATA, 0x05);

    CU_ASSERT_EQUAL(XQC_GW_IPC_FLAG_KEY, 0x01);
    CU_ASSERT_EQUAL(XQC_GW_IPC_SUB_ERR_TOO_MANY_STREAMS, 1);
    CU_ASSERT_EQUAL(XQC_GW_IPC_SUB_ERR_BAD_REQUEST, 2);

    CU_ASSERT_EQUAL(XQC_GW_IPC_DATA_FIXED_SIZE, 22);
    CU_ASSERT_EQUAL(XQC_GW_IPC_DATA_MSG_SIZE, 24);

    /* SUB: exact byte layout [len][ver][kind][name_len][name] */
    n = xqc_gw_ipc_serialize_sub(buf, sizeof(buf), "cam0", 4);
    CU_ASSERT_EQUAL(n, 12);
    CU_ASSERT_EQUAL(xqc_gw_ipc_get_u32(buf), 8);
    CU_ASSERT_EQUAL(buf[4], 1);
    CU_ASSERT_EQUAL(buf[5], 2);
    CU_ASSERT_EQUAL(xqc_gw_ipc_get_u16(buf + 6), 4);
    CU_ASSERT_EQUAL(memcmp(buf + 8, "cam0", 4), 0);
    CU_ASSERT_TRUE(xqc_gw_ipc_parse_message(buf + 4, 8, &m));
    CU_ASSERT_EQUAL(m.version, 1);
    CU_ASSERT_EQUAL(m.kind, XQC_GW_IPC_KIND_SUB);
    CU_ASSERT_TRUE(xqc_gw_ipc_parse_sub(&m, &name));
    CU_ASSERT_EQUAL(name.name_len, 4);
    CU_ASSERT_EQUAL(memcmp(name.name, "cam0", 4), 0);

    /* rejected inputs: NULL / empty / oversized name, short capacity */
    CU_ASSERT_EQUAL(xqc_gw_ipc_serialize_sub(buf, sizeof(buf), NULL, 4), 0);
    CU_ASSERT_EQUAL(xqc_gw_ipc_serialize_sub(buf, sizeof(buf), "cam0", 0), 0);
    CU_ASSERT_EQUAL(xqc_gw_ipc_serialize_sub(buf, sizeof(buf), "cam0", 256), 0);
    CU_ASSERT_EQUAL(xqc_gw_ipc_serialize_sub(buf, 5, "cam0", 4), 0);

    /* UNSUB / PLI round trip; a zero sid never parses */
    CU_ASSERT_EQUAL(xqc_gw_ipc_serialize_unsub(buf, sizeof(buf), 7), 10);
    CU_ASSERT_TRUE(xqc_gw_ipc_parse_message(buf + 4, 6, &m));
    CU_ASSERT_EQUAL(m.kind, XQC_GW_IPC_KIND_UNSUB);
    CU_ASSERT_TRUE(xqc_gw_ipc_parse_sid(&m, &sid));
    CU_ASSERT_EQUAL(sid, 7);
    CU_ASSERT_EQUAL(xqc_gw_ipc_serialize_pli(buf, sizeof(buf), 9), 10);
    CU_ASSERT_TRUE(xqc_gw_ipc_parse_message(buf + 4, 6, &m));
    CU_ASSERT_EQUAL(m.kind, XQC_GW_IPC_KIND_PLI);
    CU_ASSERT_TRUE(xqc_gw_ipc_parse_sid(&m, &sid));
    CU_ASSERT_EQUAL(sid, 9);
    CU_ASSERT_EQUAL(xqc_gw_ipc_serialize_pli(buf, sizeof(buf), 0), 10);
    CU_ASSERT_TRUE(xqc_gw_ipc_parse_message(buf + 4, 6, &m));
    CU_ASSERT_FALSE(xqc_gw_ipc_parse_sid(&m, &sid));

    /* DATA: fixed part and payload round trip, big endian on the wire */
    n = xqc_gw_ipc_serialize_data(databuf, sizeof(databuf), 42,
        XQC_GW_IPC_FRAME_VIDEO_IDR, XQC_GW_IPC_FLAG_KEY,
        0x1122334455667788ULL, 0xAABBCCDDULL, (const uint8_t *)"xy", 2);
    CU_ASSERT_EQUAL(n, 4 + XQC_GW_IPC_DATA_MSG_SIZE + 2);
    CU_ASSERT_EQUAL(databuf[6], 0x00);
    CU_ASSERT_EQUAL(databuf[7], 0x00);
    CU_ASSERT_EQUAL(databuf[8], 0x00);
    CU_ASSERT_EQUAL(databuf[9], 42);
    CU_ASSERT_EQUAL(databuf[10], XQC_GW_IPC_FRAME_VIDEO_IDR);
    CU_ASSERT_EQUAL(databuf[11], XQC_GW_IPC_FLAG_KEY);
    CU_ASSERT_TRUE(xqc_gw_ipc_parse_message(databuf + 4,
        XQC_GW_IPC_DATA_MSG_SIZE + 2, &m));
    CU_ASSERT_EQUAL(m.kind, XQC_GW_IPC_KIND_DATA);
    CU_ASSERT_TRUE(xqc_gw_ipc_parse_data(&m, &data));
    CU_ASSERT_EQUAL(data.sid, 42);
    CU_ASSERT_EQUAL(data.frame_type, XQC_GW_IPC_FRAME_VIDEO_IDR);
    CU_ASSERT_EQUAL(data.flags, XQC_GW_IPC_FLAG_KEY);
    CU_ASSERT_EQUAL(data.pts_us, 0x1122334455667788ULL);
    CU_ASSERT_EQUAL(data.seq, 0xAABBCCDDULL);
    CU_ASSERT_EQUAL(data.payload_len, 2);
    CU_ASSERT_EQUAL(memcmp(data.payload, "xy", 2), 0);
    /* one byte short of the required capacity */
    CU_ASSERT_EQUAL(xqc_gw_ipc_serialize_data(databuf,
        4 + XQC_GW_IPC_DATA_MSG_SIZE + 1, 42, XQC_GW_IPC_FRAME_VIDEO_IDR,
        XQC_GW_IPC_FLAG_KEY, 1, 1, (const uint8_t *)"xy", 2), 0);
    /* short DATA body is rejected */
    CU_ASSERT_TRUE(xqc_gw_ipc_parse_message(databuf + 4, 10, &m));
    CU_ASSERT_FALSE(xqc_gw_ipc_parse_data(&m, &data));

    /* SUB_OK / SUB_ERR round trip */
    n = xqc_gw_ipc_serialize_sub_ok(buf, sizeof(buf), 42, "cam0", 4);
    CU_ASSERT_EQUAL(n, 16);
    CU_ASSERT_TRUE(xqc_gw_ipc_parse_message(buf + 4, 12, &m));
    CU_ASSERT_TRUE(xqc_gw_ipc_parse_sub_ok(&m, &ok));
    CU_ASSERT_EQUAL(ok.sid, 42);
    CU_ASSERT_EQUAL(ok.name_len, 4);
    CU_ASSERT_EQUAL(memcmp(ok.name, "cam0", 4), 0);
    n = xqc_gw_ipc_serialize_sub_err(buf, sizeof(buf), "cam0", 4,
        XQC_GW_IPC_SUB_ERR_TOO_MANY_STREAMS);
    CU_ASSERT_EQUAL(n, 17);
    CU_ASSERT_TRUE(xqc_gw_ipc_parse_message(buf + 4, 13, &m));
    CU_ASSERT_EQUAL(m.kind, XQC_GW_IPC_KIND_SUB_ERR);
    CU_ASSERT_TRUE(xqc_gw_ipc_parse_sub_err(&m, &err));
    CU_ASSERT_EQUAL(err.name_len, 4);
    CU_ASSERT_EQUAL(memcmp(err.name, "cam0", 4), 0);
    CU_ASSERT_EQUAL(err.reason, XQC_GW_IPC_SUB_ERR_TOO_MANY_STREAMS);

    /* a message shorter than [ver][kind] never parses */
    CU_ASSERT_FALSE(xqc_gw_ipc_parse_message(buf, 1, &m));
    CU_ASSERT_FALSE(xqc_gw_ipc_parse_message(NULL, 2, &m));
}

/**
 * Test: IPC client over a real UDS — immediate connect, typed callbacks,
 * framing across reads, upstream-only kinds dropped, reconnect after the 1 s
 * retry, and connection drop on an unknown protocol version.
 */
void
xqc_test_moq_gateway_ipc_client(void)
{
    struct event_base *eb;
    xqc_gw_ipc_t *ipc;
    xqc_gw_ipc_cbs_t cbs;
    xqc_gw_ipc_stats_t stats;
    xqc_gw_test_evs_t evs2;
    char path[128];
    char missing_path[128];
    char long_path[128];
    uint8_t buf[XQC_GW_IPC_DATA_MSG_SIZE + 8];
    uint8_t rbuf[64];
    size_t n;
    int lfd, sfd, sfd2;

    gw_test_ipc_contract();

    /* argument validation happens before any socket is created */
    eb = event_base_new();
    CU_ASSERT_PTR_NOT_NULL_FATAL(eb);

    memset(&cbs, 0, sizeof(cbs));
    cbs.on_frame = gw_test_on_frame;
    cbs.on_sub_ok = gw_test_on_sub_ok;
    cbs.on_sub_err = gw_test_on_sub_err;
    cbs.on_state = gw_test_on_state;

    CU_ASSERT_PTR_NULL(xqc_gw_ipc_create(NULL, "/tmp/x.sock", &cbs, &g_gw_evs));
    CU_ASSERT_PTR_NULL(xqc_gw_ipc_create(eb, NULL, &cbs, &g_gw_evs));
    CU_ASSERT_PTR_NULL(xqc_gw_ipc_create(eb, "", &cbs, &g_gw_evs));
    CU_ASSERT_PTR_NULL(xqc_gw_ipc_create(eb, "/tmp/x.sock", NULL, &g_gw_evs));
    memset(long_path, 'a', sizeof(long_path) - 1);
    long_path[0] = '/';
    long_path[sizeof(long_path) - 1] = '\0';
    CU_ASSERT_PTR_NULL(xqc_gw_ipc_create(eb, long_path, &cbs, &g_gw_evs));

    /* stats getters tolerate NULL */
    memset(&stats, 0xff, sizeof(stats));
    xqc_gw_ipc_get_stats(NULL, &stats);
    CU_ASSERT_EQUAL(stats.connects, 0);
    CU_ASSERT_EQUAL(stats.frames_rx, 0);
    xqc_gw_ipc_get_stats(NULL, NULL);

    /* connect failure at create time arms the retry, no state transition */
    snprintf(missing_path, sizeof(missing_path),
        "/tmp/xqc_gw_ipc_test_missing_%d.sock", (int)getpid());
    unlink(missing_path);
    memset(&evs2, 0, sizeof(evs2));
    ipc = xqc_gw_ipc_create(eb, missing_path, &cbs, &evs2);
    CU_ASSERT_PTR_NOT_NULL_FATAL(ipc);
    CU_ASSERT_EQUAL(xqc_gw_ipc_connected(ipc), 0);
    CU_ASSERT_EQUAL(evs2.on_state_calls, 0);
    xqc_gw_ipc_get_stats(ipc, &stats);
    CU_ASSERT_EQUAL(stats.connects, 0);
    CU_ASSERT_EQUAL(stats.disconnects, 0);
    xqc_gw_ipc_destroy(ipc);

    /* ── live session ── */
    lfd = gw_test_server_open(path, sizeof(path));
    CU_ASSERT_TRUE_FATAL(lfd >= 0);

    memset(&g_gw_evs, 0, sizeof(g_gw_evs));
    ipc = xqc_gw_ipc_create(eb, path, &cbs, &g_gw_evs);
    CU_ASSERT_PTR_NOT_NULL_FATAL(ipc);
    /* a UDS connect completes immediately: state flips to connected */
    CU_ASSERT_EQUAL(xqc_gw_ipc_connected(ipc), 1);
    CU_ASSERT_EQUAL(g_gw_evs.on_state_calls, 1);
    CU_ASSERT_EQUAL(g_gw_evs.connected, 1);
    CU_ASSERT_EQUAL(g_gw_evs.connects, 1);

    sfd = gw_test_server_accept(lfd);
    CU_ASSERT_TRUE_FATAL(sfd >= 0);

    /* SUB_OK: sid and name reach the callback */
    n = xqc_gw_ipc_serialize_sub_ok(buf, sizeof(buf), 42, "cam0", 4);
    CU_ASSERT_TRUE(n > 0);
    CU_ASSERT_EQUAL(gw_test_server_send(sfd, buf, n), 0);
    gw_test_loop_once(eb);
    CU_ASSERT_EQUAL(g_gw_evs.sub_oks, 1);
    CU_ASSERT_EQUAL(g_gw_evs.last_sub_ok_sid, 42);
    CU_ASSERT_STRING_EQUAL(g_gw_evs.last_sub_ok_name, "cam0");

    /* DATA: every field and the payload reach the callback */
    n = xqc_gw_ipc_serialize_data(buf, sizeof(buf), 42,
        XQC_GW_IPC_FRAME_VIDEO_IDR, XQC_GW_IPC_FLAG_KEY,
        123456, 77, (const uint8_t *)"hi", 2);
    CU_ASSERT_EQUAL(gw_test_server_send(sfd, buf, n), 0);
    gw_test_loop_once(eb);
    CU_ASSERT_EQUAL(g_gw_evs.frames, 1);
    CU_ASSERT_EQUAL(g_gw_evs.last_frame_sid, 42);
    CU_ASSERT_EQUAL(g_gw_evs.last_frame_type, XQC_GW_IPC_FRAME_VIDEO_IDR);
    CU_ASSERT_EQUAL(g_gw_evs.last_frame_flags, XQC_GW_IPC_FLAG_KEY);
    CU_ASSERT_EQUAL(g_gw_evs.last_frame_pts, 123456);
    CU_ASSERT_EQUAL(g_gw_evs.last_frame_seq, 77);
    CU_ASSERT_EQUAL(g_gw_evs.last_frame_len, 2);
    CU_ASSERT_EQUAL(memcmp(g_gw_evs.last_frame_payload, "hi", 2), 0);

    /* SUB_ERR: reason and name reach the callback */
    n = xqc_gw_ipc_serialize_sub_err(buf, sizeof(buf), "cam0", 4,
        XQC_GW_IPC_SUB_ERR_TOO_MANY_STREAMS);
    CU_ASSERT_EQUAL(gw_test_server_send(sfd, buf, n), 0);
    gw_test_loop_once(eb);
    CU_ASSERT_EQUAL(g_gw_evs.sub_errs, 1);
    CU_ASSERT_EQUAL(g_gw_evs.last_sub_err_reason,
        XQC_GW_IPC_SUB_ERR_TOO_MANY_STREAMS);
    CU_ASSERT_STRING_EQUAL(g_gw_evs.last_sub_err_name, "cam0");

    /* a frame split across two reads is reassembled before dispatch */
    n = xqc_gw_ipc_serialize_data(buf, sizeof(buf), 42,
        XQC_GW_IPC_FRAME_VIDEO, 0, 1, 78, (const uint8_t *)"xy", 2);
    CU_ASSERT_TRUE(n > 5);
    CU_ASSERT_EQUAL(gw_test_server_send(sfd, buf, 5), 0);
    gw_test_loop_once(eb);
    CU_ASSERT_EQUAL(g_gw_evs.frames, 1);            /* still incomplete */
    CU_ASSERT_EQUAL(gw_test_server_send(sfd, buf + 5, n - 5), 0);
    gw_test_loop_once(eb);
    CU_ASSERT_EQUAL(g_gw_evs.frames, 2);
    CU_ASSERT_EQUAL(g_gw_evs.last_frame_seq, 78);

    /* upstream-only kinds (SUB) coming back are dropped, not dispatched */
    n = xqc_gw_ipc_serialize_sub(buf, sizeof(buf), "cam0", 4);
    CU_ASSERT_EQUAL(gw_test_server_send(sfd, buf, n), 0);
    gw_test_loop_once(eb);
    CU_ASSERT_EQUAL(g_gw_evs.frames, 2);
    CU_ASSERT_EQUAL(g_gw_evs.sub_oks, 1);

    /* client -> server: SUB / PLI / UNSUB wire frames */
    CU_ASSERT_EQUAL(xqc_gw_ipc_send_sub(ipc, "cam0", 4), 0);
    CU_ASSERT_EQUAL(gw_test_server_recv(sfd, rbuf, sizeof(rbuf)), 12);
    CU_ASSERT_EQUAL(xqc_gw_ipc_get_u32(rbuf), 8);
    {
        xqc_gw_ipc_msg_t m;
        xqc_gw_ipc_name_t name;

        CU_ASSERT_TRUE(xqc_gw_ipc_parse_message(rbuf + 4, 8, &m));
        CU_ASSERT_EQUAL(m.kind, XQC_GW_IPC_KIND_SUB);
        CU_ASSERT_TRUE(xqc_gw_ipc_parse_sub(&m, &name));
        CU_ASSERT_EQUAL(name.name_len, 4);
        CU_ASSERT_EQUAL(memcmp(name.name, "cam0", 4), 0);
    }
    CU_ASSERT_EQUAL(xqc_gw_ipc_send_pli(ipc, 42), 0);
    CU_ASSERT_EQUAL(gw_test_server_recv(sfd, rbuf, sizeof(rbuf)), 10);
    {
        xqc_gw_ipc_msg_t m;
        uint32_t sid;

        CU_ASSERT_TRUE(xqc_gw_ipc_parse_message(rbuf + 4, 6, &m));
        CU_ASSERT_EQUAL(m.kind, XQC_GW_IPC_KIND_PLI);
        CU_ASSERT_TRUE(xqc_gw_ipc_parse_sid(&m, &sid));
        CU_ASSERT_EQUAL(sid, 42);
    }
    CU_ASSERT_EQUAL(xqc_gw_ipc_send_unsub(ipc, 42), 0);
    CU_ASSERT_EQUAL(gw_test_server_recv(sfd, rbuf, sizeof(rbuf)), 10);
    {
        xqc_gw_ipc_msg_t m;
        uint32_t sid;

        CU_ASSERT_TRUE(xqc_gw_ipc_parse_message(rbuf + 4, 6, &m));
        CU_ASSERT_EQUAL(m.kind, XQC_GW_IPC_KIND_UNSUB);
        CU_ASSERT_TRUE(xqc_gw_ipc_parse_sid(&m, &sid));
        CU_ASSERT_EQUAL(sid, 42);
    }

    /* a locally rejected send counts as a failure */
    CU_ASSERT_EQUAL(xqc_gw_ipc_send_sub(ipc, NULL, 0), -1);

    /* unknown protocol version drops the connection and reconnects later */
    n = xqc_gw_ipc_serialize_data(buf, sizeof(buf), 42,
        XQC_GW_IPC_FRAME_VIDEO, 0, 1, 9, (const uint8_t *)"x", 1);
    CU_ASSERT_TRUE(n > 4);
    buf[4] = 2;                                     /* version 2 */
    CU_ASSERT_EQUAL(gw_test_server_send(sfd, buf, n), 0);
    gw_test_loop_once(eb);
    CU_ASSERT_EQUAL(g_gw_evs.connected, 0);
    CU_ASSERT_EQUAL(g_gw_evs.disconnects, 1);
    CU_ASSERT_EQUAL(g_gw_evs.on_state_calls, 2);
    CU_ASSERT_EQUAL(xqc_gw_ipc_connected(ipc), 0);

    /* sends while disconnected fail fast */
    CU_ASSERT_EQUAL(xqc_gw_ipc_send_pli(ipc, 42), -1);

    /* the 1 s retry timer reconnects on its own */
    gw_test_loop_once(eb);
    CU_ASSERT_EQUAL(g_gw_evs.connects, 2);
    CU_ASSERT_EQUAL(g_gw_evs.connected, 1);
    CU_ASSERT_EQUAL(g_gw_evs.on_state_calls, 3);
    CU_ASSERT_EQUAL(xqc_gw_ipc_connected(ipc), 1);
    sfd2 = gw_test_server_accept(lfd);
    CU_ASSERT_TRUE(sfd2 >= 0);

    xqc_gw_ipc_get_stats(ipc, &stats);
    CU_ASSERT_EQUAL(stats.connects, 2);
    CU_ASSERT_EQUAL(stats.disconnects, 1);
    CU_ASSERT_EQUAL(stats.frames_rx, 2);
    CU_ASSERT_EQUAL(stats.sub_ok_rx, 1);
    CU_ASSERT_EQUAL(stats.sub_err_rx, 1);
    CU_ASSERT_EQUAL(stats.dropped_non_data, 1);
    CU_ASSERT_EQUAL(stats.protocol_errors, 1);
    CU_ASSERT_EQUAL(stats.send_failures, 2);        /* NULL name + disconnected */

    xqc_gw_ipc_destroy(ipc);
    close(sfd);
    close(sfd2);
    close(lfd);
    unlink(path);
    event_base_free(eb);
}

/* ------------------------------------------------------------------------
 * E9.2: MoQ over WebTransport — transport ops + glue callbacks end to end
 *
 * Drives the whole seam the gateway will use (E8): a WebTransport CONNECT
 * offer is accepted with the MoQ user session as session user data, a MoQ
 * session is created on the accepted WT session (server engine), and the
 * E7 glue callbacks are registered as the WT stream callbacks. The test
 * exercises the WT transport ops (create/write/close/quic_stream) on a
 * local stream and the driver path (client SETUP over a peer-initiated
 * bidi control stream), including both stream teardown orders: the local
 * one is detached by the application first, the control one is released
 * by the glue when its transport stream dies.
 * ------------------------------------------------------------------------ */

/* not exposed in xqc_h3_stream.h, but stable file-scope entries used by tests */
xqc_int_t xqc_h3_stream_process_in(xqc_h3_stream_t *h3s, unsigned char *data,
    size_t data_len, xqc_bool_t fin_flag);
int xqc_h3_stream_read_notify(xqc_stream_t *stream, void *user_data);

typedef struct {
    /* embedded: xqc_moq_session_create() stores this address in the session */
    xqc_moq_user_session_t  us;

    /* WebTransport callbacks */
    xqc_wt_session_t       *offer_session;
    void                   *offer_conn_user_data;
    int                     offer_count;
    int                     accepted_count;
    void                   *accepted_user_data;
    xqc_wt_session_t       *session_closed;
    void                   *session_closed_user_data;
    int                     session_closed_count;
    xqc_wt_session_t       *opened_session;
    xqc_wt_stream_t        *opened_stream[4];
    int                     opened_count;
    xqc_wt_stream_t        *read_stream;
    void                   *read_stream_user_data;
    int                     read_count;
    xqc_wt_stream_t        *closed_stream;
    void                   *closed_stream_user_data;
    int                     closed_stream_count;

    /* MoQ session callbacks */
    xqc_moq_user_session_t *moq_setup_user_session;
    char                   *moq_setup_extdata;
    int                     moq_setup_count;
} moq_wt_cap_t;

static moq_wt_cap_t g_moq_wt;

static void
moq_wt_cap_reset(void)
{
    memset(&g_moq_wt, 0, sizeof(g_moq_wt));
}

static xqc_int_t
moq_wt_cb_on_request(xqc_wt_session_t *session, void *conn_user_data)
{
    g_moq_wt.offer_session = session;
    g_moq_wt.offer_conn_user_data = conn_user_data;
    g_moq_wt.offer_count++;
    return XQC_OK;
}

static xqc_int_t
moq_wt_cb_on_accepted(xqc_wt_session_t *session, void *session_user_data)
{
    (void)session;
    g_moq_wt.accepted_count++;
    g_moq_wt.accepted_user_data = session_user_data;
    return XQC_OK;
}

static xqc_int_t
moq_wt_cb_on_session_closed(xqc_wt_session_t *session, void *session_user_data)
{
    g_moq_wt.session_closed = session;
    g_moq_wt.session_closed_user_data = session_user_data;
    g_moq_wt.session_closed_count++;
    return XQC_OK;
}

/* stream events are recorded here and then routed to the E7 MoQ glue, the
 * exact wiring an application does with its xqc_wt_callbacks_t */
static xqc_int_t
moq_wt_cb_on_stream_opened(xqc_wt_session_t *session, xqc_wt_stream_t *stream,
    void *session_user_data)
{
    if (g_moq_wt.opened_count < 4) {
        g_moq_wt.opened_stream[g_moq_wt.opened_count] = stream;
    }
    g_moq_wt.opened_session = session;
    g_moq_wt.opened_count++;
    return xqc_moq_wt_on_stream_opened(session, stream, session_user_data);
}

static xqc_int_t
moq_wt_cb_on_stream_read(xqc_wt_stream_t *stream, void *stream_user_data)
{
    g_moq_wt.read_stream = stream;
    g_moq_wt.read_stream_user_data = stream_user_data;
    g_moq_wt.read_count++;
    return xqc_moq_wt_on_stream_read(stream, stream_user_data);
}

static xqc_int_t
moq_wt_cb_on_stream_closed(xqc_wt_stream_t *stream, void *stream_user_data)
{
    g_moq_wt.closed_stream = stream;
    g_moq_wt.closed_stream_user_data = stream_user_data;
    g_moq_wt.closed_stream_count++;
    return xqc_moq_wt_on_stream_closed(stream, stream_user_data);
}

static xqc_int_t
moq_wt_cb_on_datagram(xqc_wt_session_t *session, void *session_user_data)
{
    (void)session;
    (void)session_user_data;
    return XQC_OK;
}

static void
moq_wt_register_wt_cbs(xqc_h3_conn_t *h3c)
{
    xqc_wt_callbacks_t cbs;
    memset(&cbs, 0, sizeof(cbs));
    cbs.session_request_notify = moq_wt_cb_on_request;
    cbs.session_accepted_notify = moq_wt_cb_on_accepted;
    cbs.session_closed_notify = moq_wt_cb_on_session_closed;
    cbs.stream_opened_notify = moq_wt_cb_on_stream_opened;
    cbs.stream_read_notify = moq_wt_cb_on_stream_read;
    cbs.stream_closed_notify = moq_wt_cb_on_stream_closed;
    cbs.datagram_read_notify = moq_wt_cb_on_datagram;

    CU_ASSERT(xqc_h3_conn_set_webtransport_callbacks(h3c, &cbs) == XQC_OK);
}

/* MoQ session callbacks: only on_session_setup is exercised by this test */
static void
moq_wt_moq_session_setup(xqc_moq_user_session_t *user_session, char *extdata)
{
    g_moq_wt.moq_setup_user_session = user_session;
    g_moq_wt.moq_setup_extdata = extdata;
    g_moq_wt.moq_setup_count++;
}

static void
moq_wt_moq_stub_datachannel(xqc_moq_user_session_t *user_session)
{
    (void)user_session;
}

static void
moq_wt_moq_stub_datachannel_msg(xqc_moq_user_session_t *user_session,
    uint8_t *msg, size_t msg_len)
{
    (void)user_session;
    (void)msg;
    (void)msg_len;
}

static void
moq_wt_moq_stub_subscribe(xqc_moq_user_session_t *user_session,
    uint64_t subscribe_id, xqc_moq_track_t *track, xqc_moq_subscribe_msg_t *msg)
{
    (void)user_session;
    (void)subscribe_id;
    (void)track;
    (void)msg;
}

static void
moq_wt_moq_stub_request_keyframe(xqc_moq_user_session_t *user_session,
    uint64_t subscribe_id, xqc_moq_track_t *track)
{
    (void)user_session;
    (void)subscribe_id;
    (void)track;
}

static void
moq_wt_moq_stub_bitrate_change(xqc_moq_user_session_t *user_session,
    uint64_t bitrate)
{
    (void)user_session;
    (void)bitrate;
}

static void
moq_wt_moq_stub_subscribe_ok(xqc_moq_user_session_t *user_session,
    xqc_moq_subscribe_ok_msg_t *subscribe_ok)
{
    (void)user_session;
    (void)subscribe_ok;
}

static void
moq_wt_moq_stub_subscribe_error(xqc_moq_user_session_t *user_session,
    xqc_moq_subscribe_error_msg_t *subscribe_error)
{
    (void)user_session;
    (void)subscribe_error;
}

static void
moq_wt_moq_stub_catalog(xqc_moq_user_session_t *user_session,
    xqc_moq_track_info_t **track_info_array, xqc_int_t array_size)
{
    (void)user_session;
    (void)track_info_array;
    (void)array_size;
}

static void
moq_wt_moq_stub_video(xqc_moq_user_session_t *user_session,
    uint64_t subscribe_id, xqc_moq_video_frame_t *video_frame)
{
    (void)user_session;
    (void)subscribe_id;
    (void)video_frame;
}

static void
moq_wt_moq_stub_audio(xqc_moq_user_session_t *user_session,
    uint64_t subscribe_id, xqc_moq_audio_frame_t *audio_frame)
{
    (void)user_session;
    (void)subscribe_id;
    (void)audio_frame;
}

static xqc_moq_session_callbacks_t
moq_wt_session_callbacks(void)
{
    xqc_moq_session_callbacks_t cbs;
    memset(&cbs, 0, sizeof(cbs));
    cbs.on_session_setup = moq_wt_moq_session_setup;
    cbs.on_datachannel = moq_wt_moq_stub_datachannel;
    cbs.on_datachannel_msg = moq_wt_moq_stub_datachannel_msg;
    cbs.on_subscribe = moq_wt_moq_stub_subscribe;
    cbs.on_request_keyframe = moq_wt_moq_stub_request_keyframe;
    cbs.on_bitrate_change = moq_wt_moq_stub_bitrate_change;
    cbs.on_subscribe_ok = moq_wt_moq_stub_subscribe_ok;
    cbs.on_subscribe_error = moq_wt_moq_stub_subscribe_error;
    cbs.on_catalog = moq_wt_moq_stub_catalog;
    cbs.on_video = moq_wt_moq_stub_video;
    cbs.on_audio = moq_wt_moq_stub_audio;
    return cbs;
}

/* QPACK field section builder, no Huffman, RIC/Base handled by the frame
 * wrapper below. Static table indexes follow RFC 9204 Appendix A. */
typedef struct {
    uint8_t     buf[256];
    size_t      len;
} moq_wt_hb_t;

static void
moq_wt_hb_indexed(moq_wt_hb_t *hb, uint8_t static_idx)
{
    CU_ASSERT_FATAL(static_idx < 63);
    hb->buf[hb->len++] = (uint8_t)(0xC0 | static_idx);
}

static void
moq_wt_hb_literal(moq_wt_hb_t *hb, const char *name, const char *value)
{
    size_t name_len = strlen(name);
    size_t value_len = strlen(value);

    CU_ASSERT_FATAL(hb->len + name_len + value_len + 4 < sizeof(hb->buf));
    CU_ASSERT_FATAL(name_len < 128 && value_len < 127);

    /* Literal Field Line With Literal Name: 001 N H NameLen(3+),
     * RFC 9204 Section 4.5.6, no huffman, never-indexed clear */
    hb->buf[hb->len++] = (uint8_t)(0x20 | (name_len < 7 ? name_len : 7));
    if (name_len >= 7) {
        hb->buf[hb->len++] = (uint8_t)(name_len - 7);
    }
    xqc_memcpy(hb->buf + hb->len, name, name_len);
    hb->len += name_len;

    hb->buf[hb->len++] = (uint8_t)value_len;
    xqc_memcpy(hb->buf + hb->len, value, value_len);
    hb->len += value_len;
}

/* Wrap the section into a HEADERS frame with Required Insert Count 0 and
 * Delta Base 0. Returns the total frame length. */
static size_t
moq_wt_hb_frame(const moq_wt_hb_t *hb, uint8_t *out)
{
    uint8_t *pos = out;
    pos = xqc_put_varint(pos, XQC_H3_FRM_HEADERS);
    pos = xqc_put_varint(pos, hb->len + 2);
    *pos++ = 0x00;
    *pos++ = 0x00;
    xqc_memcpy(pos, hb->buf, hb->len);
    return (size_t)(pos - out) + hb->len;
}

static xqc_int_t
moq_wt_feed_headers(xqc_h3_stream_t *h3s, const uint8_t *frame, size_t len,
    xqc_bool_t fin)
{
    unsigned char buf[512];
    CU_ASSERT_FATAL(len <= sizeof(buf));
    xqc_memcpy(buf, frame, len);
    return xqc_h3_stream_process_in(h3s, buf, len, fin);
}

/* client SETTINGS that advertise WebTransport support (draft-07 3.1) */
static void
moq_wt_make_peer_capable(xqc_h3_conn_t *h3c)
{
    h3c->flags |= XQC_H3_CONN_FLAG_SETTINGS_RECVED;
    h3c->peer_h3_conn_settings.webtransport_max_sessions = 64;
    h3c->peer_h3_conn_settings.h3_datagram = 1;
}

/* feed a valid WebTransport CONNECT request on the request stream */
static xqc_int_t
moq_wt_feed_wt_connect(xqc_h3_stream_t *h3s, xqc_bool_t fin)
{
    uint8_t frame[512];
    moq_wt_hb_t hb = {0};

    moq_wt_hb_indexed(&hb, 15);                     /* :method CONNECT */
    moq_wt_hb_literal(&hb, ":protocol", "webtransport");
    moq_wt_hb_indexed(&hb, 23);                     /* :scheme https */
    moq_wt_hb_literal(&hb, ":authority", "example.com");
    moq_wt_hb_indexed(&hb, 1);                      /* :path / */
    moq_wt_hb_literal(&hb, "origin", "https://example.com");

    size_t len = moq_wt_hb_frame(&hb, frame);
    return moq_wt_feed_headers(h3s, frame, len, fin);
}

/* established h3 request context on a server-like engine: the conn is built
 * as a client by the shared helper, then its engine is retyped SERVER because
 * the MoQ WebTransport session-create guard requires it */
static xqc_h3_stream_t *
moq_wt_req_setup(xqc_connection_t **out_conn, xqc_h3_conn_t **out_h3c)
{
    xqc_connection_t *conn = test_engine_connect();
    if (conn == NULL) {
        return NULL;
    }

    conn->engine->eng_type = XQC_ENGINE_SERVER;

    if (conn->alpn) {
        xqc_free(conn->alpn);
    }
    conn->alpn_len = strlen(XQC_ALPN_H3);
    conn->alpn = xqc_calloc(1, conn->alpn_len + 1);
    xqc_memcpy(conn->alpn, XQC_ALPN_H3, conn->alpn_len);

    /* test_connect() creates the conn with the "transport" ALPN whose callbacks
       are all NULL; production resolves conn->app_proto_cbs when the negotiated
       ALPN is known. Streams capture conn->app_proto_cbs.stream_cbs at creation,
       so refresh the callbacks to the h3 set after swapping the ALPN. */
    if (xqc_engine_get_alpn_callbacks(conn->engine, XQC_ALPN_H3,
            conn->alpn_len, &conn->app_proto_cbs) != XQC_OK)
    {
        return NULL;
    }

    conn->conn_flow_ctl.fc_max_streams_bidi_can_send = 1024;
    conn->conn_flow_ctl.fc_max_streams_uni_can_send = 1024;
    conn->conn_state = XQC_CONN_STATE_ESTABED;

    conn->engine->config->enable_webtransport = 1;
    conn->engine->config->webtransport_max_sessions = 16;

    xqc_h3_conn_t *h3c = xqc_h3_conn_create(conn, NULL);
    if (h3c == NULL) {
        return NULL;
    }

    /* the QPACK encoder stream carries encoder instructions for responses */
    h3c->qenc_stream = xqc_h3_conn_create_uni_stream(h3c,
            XQC_H3_STREAM_TYPE_QPACK_ENCODER);
    if (h3c->qenc_stream == NULL) {
        return NULL;
    }

    xqc_stream_t *stream = xqc_create_stream_with_conn(conn,
            XQC_UNDEFINE_STREAM_ID, XQC_CLI_BID, NULL, NULL);
    if (stream == NULL) {
        return NULL;
    }

    xqc_h3_stream_t *h3s = xqc_h3_stream_create(h3c, stream,
            XQC_H3_STREAM_TYPE_REQUEST, NULL);
    if (h3s == NULL) {
        return NULL;
    }

    h3s->h3r = xqc_h3_request_create_inner(h3c, h3s, NULL);
    if (h3s->h3r == NULL) {
        return NULL;
    }

    *out_conn = conn;
    *out_h3c = h3c;
    return h3s;
}

static void
moq_wt_req_teardown(xqc_h3_stream_t *h3s, xqc_h3_conn_t *h3c,
    xqc_connection_t *conn)
{
    xqc_stream_t *stream = h3s->stream;
    stream->stream_flag |= XQC_STREAM_FLAG_DISCARDED;
    xqc_h3_stream_destroy(h3s);
    xqc_destroy_stream(stream);

    xqc_stream_t *qenc = h3c->qenc_stream->stream;
    qenc->stream_flag |= XQC_STREAM_FLAG_DISCARDED;
    xqc_h3_stream_destroy(h3c->qenc_stream);
    xqc_destroy_stream(qenc);

    xqc_h3_conn_destroy(h3c);
    if (conn->alpn) {
        xqc_free(conn->alpn);
    }
}

/* a peer-initiated data stream, as the transport would present it */
static xqc_h3_stream_t *
moq_wt_open_peer_stream(xqc_connection_t *conn, xqc_h3_conn_t *h3c,
    xqc_bool_t bidi)
{
    xqc_stream_t *stream = xqc_create_stream_with_conn(conn,
            XQC_UNDEFINE_STREAM_ID, bidi ? XQC_CLI_BID : XQC_CLI_UNI,
            NULL, NULL);
    if (stream == NULL) {
        return NULL;
    }

    xqc_h3_stream_t *h3s = xqc_h3_stream_create(h3c, stream,
            XQC_H3_STREAM_TYPE_UNKNOWN, NULL);
    if (h3s == NULL) {
        xqc_destroy_stream(stream);
    }

    return h3s;
}

static xqc_int_t
moq_wt_feed_stream_data(xqc_h3_stream_t *h3s, const unsigned char *data,
    size_t data_len, xqc_bool_t fin)
{
    unsigned char buf[512];
    CU_ASSERT_FATAL(data_len <= sizeof(buf));
    if (data_len > 0) {
        xqc_memcpy(buf, data, data_len);
    }
    return xqc_h3_stream_process_in(h3s, buf, data_len, fin);
}

static void
moq_wt_destroy_data_stream(xqc_h3_stream_t *h3s)
{
    xqc_stream_t *stream = h3s->stream;
    stream->stream_flag |= XQC_STREAM_FLAG_DISCARDED;
    xqc_h3_stream_destroy(h3s);
    xqc_destroy_stream(stream);
}

void
xqc_test_moq_wt_ops(void)
{
    moq_wt_cap_reset();

    xqc_connection_t *conn = NULL;
    xqc_h3_conn_t *h3c = NULL;
    xqc_h3_stream_t *h3s = moq_wt_req_setup(&conn, &h3c);
    CU_ASSERT_FATAL(h3s != NULL);
    CU_ASSERT_FATAL(h3c->wt != NULL);

    moq_wt_register_wt_cbs(h3c);
    moq_wt_make_peer_capable(h3c);

    /* --- CONNECT offer accepted with the MoQ user session --- */
    CU_ASSERT(moq_wt_feed_wt_connect(h3s, XQC_TRUE) == XQC_OK);
    CU_ASSERT(conn->conn_err == 0);
    CU_ASSERT_EQUAL(g_moq_wt.offer_count, 1);
    CU_ASSERT_FATAL(g_moq_wt.offer_session != NULL);
    CU_ASSERT_PTR_NULL(g_moq_wt.offer_conn_user_data);
    CU_ASSERT_EQUAL(g_moq_wt.accepted_count, 0);

    xqc_wt_session_t *wt_session = g_moq_wt.offer_session;
    CU_ASSERT_EQUAL(xqc_wt_session_get_stream_id(wt_session), h3s->stream_id);
    CU_ASSERT_PTR_EQUAL(xqc_wt_session_get_quic_conn(wt_session), conn);
    CU_ASSERT_PTR_NULL(xqc_wt_session_get_user_data(wt_session));

    CU_ASSERT(xqc_wt_session_accept(wt_session, &g_moq_wt.us) == XQC_OK);
    CU_ASSERT_EQUAL(g_moq_wt.accepted_count, 1);
    CU_ASSERT_PTR_EQUAL(g_moq_wt.accepted_user_data, &g_moq_wt.us);
    CU_ASSERT_PTR_EQUAL(xqc_wt_session_get_user_data(wt_session), &g_moq_wt.us);
    CU_ASSERT(conn->conn_err == 0);

    /* --- the MoQ session requires a server engine (E7 guard) --- */
    xqc_moq_user_session_t neg_us;
    memset(&neg_us, 0, sizeof(neg_us));
    conn->engine->eng_type = XQC_ENGINE_CLIENT;
    CU_ASSERT_PTR_NULL(xqc_moq_session_create(wt_session, &neg_us,
        XQC_MOQ_TRANSPORT_WEBTRANSPORT, XQC_MOQ_PUBSUB,
        moq_wt_session_callbacks(), NULL));
    CU_ASSERT_PTR_NULL(neg_us.session);
    conn->engine->eng_type = XQC_ENGINE_SERVER;

    /* --- MoQ session on the accepted WT session --- */
    xqc_moq_session_t *moq_session = xqc_moq_session_create(wt_session,
        &g_moq_wt.us, XQC_MOQ_TRANSPORT_WEBTRANSPORT, XQC_MOQ_PUBSUB,
        moq_wt_session_callbacks(), NULL);
    CU_ASSERT_FATAL(moq_session != NULL);
    CU_ASSERT_PTR_EQUAL(g_moq_wt.us.session, moq_session);
    CU_ASSERT_EQUAL(moq_session->transport_type, XQC_MOQ_TRANSPORT_WEBTRANSPORT);
    CU_ASSERT_EQUAL(moq_session->role, XQC_MOQ_PUBSUB);
    CU_ASSERT_PTR_EQUAL(moq_session->trans_conn, wt_session);
    CU_ASSERT_PTR_EQUAL(moq_session->quic_conn, conn);
    CU_ASSERT_EQUAL(moq_session->session_setup_done, 0);
    CU_ASSERT_PTR_NULL(moq_session->ctl_stream);

    /* --- local stream through the WT transport ops --- */
    xqc_moq_stream_t *uni = xqc_moq_stream_create_with_transport(moq_session,
        XQC_STREAM_UNI);
    CU_ASSERT_FATAL(uni != NULL);
    CU_ASSERT(memcmp(&uni->trans_ops, &xqc_moq_wt_stream_ops,
        sizeof(uni->trans_ops)) == 0);
    CU_ASSERT_EQUAL(g_moq_wt.opened_count, 1);      /* local creates announce too */
    CU_ASSERT_PTR_EQUAL(g_moq_wt.opened_stream[0], uni->trans_stream);
    CU_ASSERT_PTR_EQUAL(g_moq_wt.opened_session, wt_session);

    xqc_wt_stream_t *wts_uni = (xqc_wt_stream_t *)uni->trans_stream;
    CU_ASSERT_FATAL(wts_uni != NULL);
    CU_ASSERT_EQUAL(wts_uni->local, 1);
    CU_ASSERT_EQUAL(wts_uni->bidi, 0);
    CU_ASSERT_EQUAL(wts_uni->closed, 0);
    CU_ASSERT_PTR_EQUAL(wts_uni->session, wt_session);
    /* the glue must leave the ops-bound user data alone on local streams */
    CU_ASSERT_PTR_EQUAL(xqc_wt_stream_get_user_data(wts_uni), uni);
    CU_ASSERT_PTR_EQUAL(uni->trans_ops.quic_stream(uni->trans_stream),
        wts_uni->h3s->stream);

    /* "signal + session ID" prefix, pending without 1-RTT keys */
    uint8_t expected[16];
    uint8_t *ep = expected;
    ep = xqc_put_varint(ep, XQC_WT_UNI_STREAM_TYPE);
    ep = xqc_put_varint(ep, wt_session->stream_id);
    size_t prefix_len = (size_t)(ep - expected);
    CU_ASSERT_FATAL(wts_uni->send_buf != NULL);
    CU_ASSERT_EQUAL(wts_uni->send_buf->data_len, prefix_len);
    CU_ASSERT_EQUAL(wts_uni->send_buf->consumed_len, 0);
    CU_ASSERT(memcmp(wts_uni->send_buf->data, expected, prefix_len) == 0);
    CU_ASSERT_EQUAL(wts_uni->send_buf->data[0], 0x40);
    CU_ASSERT_EQUAL(wts_uni->send_buf->data[1], 0x54);

    /* ops.write appends behind the prefix */
    CU_ASSERT(uni->trans_ops.write(uni->trans_stream, (uint8_t *)"xy", 2, 0)
        == 2);
    CU_ASSERT_EQUAL(wts_uni->send_buf->data_len, prefix_len + 2);
    CU_ASSERT_EQUAL(wts_uni->send_buf->consumed_len, 0);
    CU_ASSERT(memcmp(wts_uni->send_buf->data + prefix_len, "xy", 2) == 0);

    /* ops.close resets the transport stream; further writes are refused */
    CU_ASSERT(uni->trans_ops.close(uni->trans_stream) == XQC_OK);
    CU_ASSERT_EQUAL(wts_uni->closed, 1);
    CU_ASSERT(uni->trans_ops.write(uni->trans_stream, (uint8_t *)"z", 1, 0)
        == -XQC_ESTATE);

    /* --- peer bidi stream: the glue binds it as the MoQ control stream --- */
    xqc_h3_stream_t *h3s_ctl = moq_wt_open_peer_stream(conn, h3c, XQC_TRUE);
    CU_ASSERT_FATAL(h3s_ctl != NULL);

    uint8_t ctl_wire[16];
    ep = ctl_wire;
    ep = xqc_put_varint(ep, XQC_WT_BIDI_SIGNAL_VALUE);
    ep = xqc_put_varint(ep, wt_session->stream_id);
    size_t ctl_wire_len = (size_t)(ep - ctl_wire);

    /* first byte only: the signal must not create a stream yet */
    CU_ASSERT(moq_wt_feed_stream_data(h3s_ctl, ctl_wire, 1, XQC_FALSE)
        == XQC_OK);
    CU_ASSERT_PTR_NULL(h3s_ctl->wt_stream);
    /* rest: 0x41 recognized, prefix parsed, stream opened */
    CU_ASSERT(moq_wt_feed_stream_data(h3s_ctl, ctl_wire + 1,
        ctl_wire_len - 1, XQC_FALSE) == XQC_OK);
    CU_ASSERT(conn->conn_err == 0);
    CU_ASSERT_EQUAL(g_moq_wt.opened_count, 2);

    CU_ASSERT_FATAL(moq_session->ctl_stream != NULL);
    xqc_moq_stream_t *moq_ctl = moq_session->ctl_stream;
    xqc_wt_stream_t *wts_ctl = h3s_ctl->wt_stream;
    CU_ASSERT_FATAL(wts_ctl != NULL);
    CU_ASSERT_PTR_EQUAL(g_moq_wt.opened_stream[1], wts_ctl);
    CU_ASSERT_EQUAL(wts_ctl->local, 0);
    CU_ASSERT_EQUAL(wts_ctl->bidi, 1);
    CU_ASSERT_PTR_EQUAL(wts_ctl->session, wt_session);
    CU_ASSERT_PTR_EQUAL(moq_ctl->trans_stream, wts_ctl);
    CU_ASSERT_PTR_EQUAL(xqc_wt_stream_get_user_data(wts_ctl), moq_ctl);
    CU_ASSERT_PTR_NULL(wts_ctl->send_buf);          /* peer streams carry no prefix */

    /* --- client SETUP: buffered first, processed by the read pump --- */
    uint8_t setup[32];
    ep = setup;
    ep = xqc_put_varint(ep, XQC_MOQ_MSG_CLIENT_SETUP);
    ep = xqc_put_varint(ep, 1);                     /* versions_num */
    ep = xqc_put_varint(ep, XQC_MOQ_VERSION_5);
    ep = xqc_put_varint(ep, 1);                     /* params_num */
    ep = xqc_put_varint(ep, XQC_MOQ_PARAM_ROLE);
    ep = xqc_put_varint(ep, 1);                     /* param value length */
    *ep++ = XQC_MOQ_SUBSCRIBER;
    size_t setup_len = (size_t)(ep - setup);

    CU_ASSERT(moq_wt_feed_stream_data(h3s_ctl, setup, setup_len, XQC_TRUE)
        == XQC_OK);
    CU_ASSERT(conn->conn_err == 0);
    CU_ASSERT_EQUAL(g_moq_wt.read_count, 0);        /* buffered, not notified */
    CU_ASSERT_EQUAL(moq_session->session_setup_done, 0);

    CU_ASSERT(xqc_h3_stream_read_notify(h3s_ctl->stream, h3s_ctl) == XQC_OK);
    CU_ASSERT(conn->conn_err == 0);
    CU_ASSERT_EQUAL(g_moq_wt.read_count, 1);
    CU_ASSERT_PTR_EQUAL(g_moq_wt.read_stream, wts_ctl);
    CU_ASSERT_PTR_EQUAL(g_moq_wt.read_stream_user_data, moq_ctl);
    CU_ASSERT_EQUAL(moq_session->session_setup_done, 1);
    CU_ASSERT_EQUAL(moq_session->version, XQC_MOQ_VERSION_5);
    CU_ASSERT_EQUAL(moq_session->peer_role, XQC_MOQ_SUBSCRIBER);
    CU_ASSERT_EQUAL(g_moq_wt.moq_setup_count, 1);
    CU_ASSERT_PTR_EQUAL(g_moq_wt.moq_setup_user_session, &g_moq_wt.us);
    CU_ASSERT_PTR_NULL(g_moq_wt.moq_setup_extdata);

    /* server SETUP and the initial SUBSCRIBEs are queued behind it */
    uint8_t exp_setup[32];
    ep = exp_setup;
    ep = xqc_put_varint(ep, XQC_MOQ_MSG_SERVER_SETUP);
    ep = xqc_put_varint(ep, XQC_MOQ_VERSION_5);
    ep = xqc_put_varint(ep, 1);                     /* params_num */
    ep = xqc_put_varint(ep, XQC_MOQ_PARAM_ROLE);
    ep = xqc_put_varint(ep, 1);                     /* param value length */
    *ep++ = XQC_MOQ_PUBSUB;
    size_t exp_setup_len = (size_t)(ep - exp_setup);

    CU_ASSERT_FATAL(wts_ctl->send_buf != NULL);
    CU_ASSERT_EQUAL(wts_ctl->send_buf->consumed_len, 0);
    CU_ASSERT(wts_ctl->send_buf->data_len > exp_setup_len);
    CU_ASSERT(memcmp(wts_ctl->send_buf->data, exp_setup, exp_setup_len) == 0);

    /* --- teardown: local stream detached first, then its transport dies --- */
    xqc_moq_stream_destroy(uni);
    CU_ASSERT_PTR_NULL(xqc_wt_stream_get_user_data(wts_uni));
    moq_wt_destroy_data_stream(wts_uni->h3s);
    CU_ASSERT_EQUAL(g_moq_wt.closed_stream_count, 1);
    CU_ASSERT_PTR_EQUAL(g_moq_wt.closed_stream, wts_uni);
    CU_ASSERT_PTR_NULL(g_moq_wt.closed_stream_user_data);
    CU_ASSERT_PTR_NULL(wts_uni->h3s);
    CU_ASSERT_EQUAL(wts_uni->closed, 1);

    /* --- control stream death is reported to the glue, which releases the
     * MoQ stream and clears the anchor without killing the connection --- */
    moq_wt_destroy_data_stream(h3s_ctl);
    CU_ASSERT_EQUAL(g_moq_wt.closed_stream_count, 2);
    CU_ASSERT_PTR_EQUAL(g_moq_wt.closed_stream, wts_ctl);
    CU_ASSERT_PTR_EQUAL(g_moq_wt.closed_stream_user_data, moq_ctl);
    CU_ASSERT_PTR_NULL(moq_session->ctl_stream);
    CU_ASSERT_PTR_NULL(xqc_wt_stream_get_user_data(wts_ctl));
    CU_ASSERT_PTR_NULL(wts_ctl->h3s);
    CU_ASSERT((conn->conn_flag & XQC_CONN_FLAG_ERROR) == 0);
    CU_ASSERT(conn->conn_err == 0);

    /* --- the MoQ session goes first; the WT session and its CONNECT request
     * close last and notify exactly once --- */
    xqc_moq_session_destroy(moq_session);

    moq_wt_req_teardown(h3s, h3c, conn);
    CU_ASSERT_EQUAL(g_moq_wt.session_closed_count, 1);
    CU_ASSERT_PTR_EQUAL(g_moq_wt.session_closed, wt_session);
    CU_ASSERT_PTR_EQUAL(g_moq_wt.session_closed_user_data, &g_moq_wt.us);
    CU_ASSERT_EQUAL(g_moq_wt.closed_stream_count, 2);
    CU_ASSERT(conn->conn_err == 0);
}