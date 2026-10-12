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