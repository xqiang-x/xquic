#include "xqc_moq_gateway_feed.h"
#include "xqc_moq_gateway_frame_ipc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void
xqc_gw_stream_init(xqc_gw_stream_t *st, const char *name)
{
    memset(st, 0, sizeof(*st));
    snprintf(st->name, sizeof(st->name), "%s", name);
    snprintf(st->track_namespace, sizeof(st->track_namespace), "%s%s",
             XQC_GW_NS_PREFIX, st->name);
}

void
xqc_gw_stream_clear(xqc_gw_stream_t *st)
{
    free(st->params);
    st->params = NULL;
    st->params_len = 0;
}

xqc_gw_stream_t *
xqc_gw_stream_find_by_sid(xqc_gw_stream_t *items, int count, uint32_t sid)
{
    int i;
    if (sid == 0) {
        return NULL;
    }
    for (i = 0; i < count; i++) {
        if (items[i].sid == sid) {
            return &items[i];
        }
    }
    return NULL;
}

xqc_gw_stream_t *
xqc_gw_stream_find_by_name(xqc_gw_stream_t *items, int count,
    const char *name, size_t name_len)
{
    int i;
    if (name == NULL || name_len == 0) {
        return NULL;
    }
    for (i = 0; i < count; i++) {
        if (strlen(items[i].name) == name_len
            && memcmp(items[i].name, name, name_len) == 0)
        {
            return &items[i];
        }
    }
    return NULL;
}

xqc_gw_stream_t *
xqc_gw_stream_find_by_track(xqc_gw_stream_t *items, int count,
    const char *ns, size_t ns_len, const char *name, size_t name_len)
{
    int i;
    for (i = 0; i < count; i++) {
        if (strlen(items[i].track_namespace) == ns_len
            && memcmp(items[i].track_namespace, ns, ns_len) == 0
            && name_len == strlen(XQC_GW_TRACK_NAME)
            && memcmp(name, XQC_GW_TRACK_NAME, name_len) == 0)
        {
            return &items[i];
        }
    }
    return NULL;
}

static void
xqc_gw_stream_cache_params(xqc_gw_stream_t *st, const uint8_t *payload,
    uint32_t payload_len)
{
    size_t new_len = (size_t)st->params_len + payload_len;

    if (new_len > XQC_GW_PARAMS_CAP) {
        /* Boundary leftovers never stack this far in practice; fall back to
         * keeping only the newest frame instead of growing without bound. */
        uint8_t *fresh = malloc(payload_len);
        if (fresh == NULL) {
            return;
        }
        memcpy(fresh, payload, payload_len);
        free(st->params);
        st->params = fresh;
        st->params_len = payload_len;
        st->params_cached++;
        return;
    }

    uint8_t *grown = realloc(st->params, new_len);
    if (grown == NULL) {
        return;
    }
    memcpy(grown + st->params_len, payload, payload_len);
    st->params = grown;
    st->params_len = (uint32_t)new_len;
    st->params_cached++;
}

xqc_gw_frame_action_t
xqc_gw_feed_on_frame(xqc_gw_stream_t *st, uint8_t frame_type, uint8_t flags,
    uint64_t pts_us, uint64_t seq, const uint8_t *payload,
    uint32_t payload_len, xqc_gw_feed_out_t *out)
{
    int is_key = 0;

    memset(out, 0, sizeof(*out));

    switch (frame_type) {
    case XQC_GW_IPC_FRAME_VIDEO_IDR:
        is_key = 1;
        break;
    case XQC_GW_IPC_FRAME_VIDEO:
        is_key = (flags & XQC_GW_IPC_FLAG_KEY) != 0;
        break;
    case XQC_GW_IPC_FRAME_VIDEO_PARAMS:
        if (payload_len == 0) {
            st->frames_ignored++;
            out->action = XQC_GW_FRAME_IGNORE;
            return out->action;
        }
        xqc_gw_stream_cache_params(st, payload, payload_len);
        out->action = XQC_GW_FRAME_PARAMS;
        return out->action;
    default:
        /* AUDIO, DATA, unknown */
        st->frames_ignored++;
        out->action = XQC_GW_FRAME_IGNORE;
        return out->action;
    }

    if (payload_len == 0) {
        st->frames_ignored++;
        out->action = XQC_GW_FRAME_IGNORE;
        return out->action;
    }

    out->frame.type = is_key ? XQC_MOQ_VIDEO_KEY : XQC_MOQ_VIDEO_DELTA;
    out->frame.seq_num = seq;
    out->frame.timestamp_us = pts_us;

    if (is_key && st->params_len > 0) {
        uint8_t *merged = malloc((size_t)st->params_len + payload_len);
        if (merged != NULL) {
            memcpy(merged, st->params, st->params_len);
            memcpy(merged + st->params_len, payload, payload_len);
            out->merged = merged;
            out->frame.video_data = merged;
            out->frame.video_len = (uint64_t)st->params_len + payload_len;
            free(st->params);
            st->params = NULL;
            st->params_len = 0;
        } else {
            /* out of memory: deliver the frame without the params prefix */
            out->frame.video_data = (uint8_t *)payload;
            out->frame.video_len = payload_len;
        }
    } else {
        out->frame.video_data = (uint8_t *)payload;
        out->frame.video_len = payload_len;
    }

    st->frames_relayed++;
    out->action = is_key ? XQC_GW_FRAME_KEY : XQC_GW_FRAME_DELTA;
    return out->action;
}

void
xqc_gw_feed_out_cleanup(xqc_gw_feed_out_t *out)
{
    free(out->merged);
    out->merged = NULL;
}

int
xqc_gw_fanout_init(xqc_gw_fanout_t *f, int cap, xqc_gw_write_video_pt w)
{
    memset(f, 0, sizeof(*f));
    f->subs = calloc((size_t)cap, sizeof(xqc_gw_sub_t));
    if (f->subs == NULL) {
        return -1;
    }
    f->cap = cap;
    f->write_video = w;
    return 0;
}

void
xqc_gw_fanout_destroy(xqc_gw_fanout_t *f)
{
    free(f->subs);
    f->subs = NULL;
    f->cap = 0;
}

xqc_gw_sub_t *
xqc_gw_fanout_add(xqc_gw_fanout_t *f, xqc_gw_stream_t *st,
    uint64_t subscribe_id, xqc_moq_track_t *track, void *session)
{
    int i;
    for (i = 0; i < f->cap; i++) {
        if (!f->subs[i].active) {
            f->subs[i].active = 1;
            f->subs[i].stream = st;
            f->subs[i].subscribe_id = subscribe_id;
            f->subs[i].track = track;
            f->subs[i].session = session;
            f->subs[i].waiting_key = 1;
            return &f->subs[i];
        }
    }
    return NULL;
}

void
xqc_gw_fanout_remove_session(xqc_gw_fanout_t *f, void *session)
{
    int i;
    for (i = 0; i < f->cap; i++) {
        if (f->subs[i].active && f->subs[i].session == session) {
            memset(&f->subs[i], 0, sizeof(f->subs[i]));
        }
    }
}

int
xqc_gw_fanout_refs_on_stream(const xqc_gw_fanout_t *f,
    const xqc_gw_stream_t *st)
{
    int i, refs = 0;
    for (i = 0; i < f->cap; i++) {
        if (f->subs[i].active && f->subs[i].stream == st) {
            refs++;
        }
    }
    return refs;
}

void
xqc_gw_fanout_deliver_video(xqc_gw_fanout_t *f, xqc_gw_stream_t *st,
    xqc_gw_feed_out_t *out)
{
    int i, matched = 0;

    if (out->action != XQC_GW_FRAME_KEY && out->action != XQC_GW_FRAME_DELTA) {
        return;
    }

    for (i = 0; i < f->cap; i++) {
        xqc_gw_sub_t *sub = &f->subs[i];
        xqc_int_t ret;

        if (!sub->active || sub->stream != st) {
            continue;
        }
        matched++;

        if (sub->waiting_key && out->action == XQC_GW_FRAME_DELTA) {
            f->dropped_wait_key++;
            continue;
        }
        if (out->action == XQC_GW_FRAME_KEY) {
            sub->waiting_key = 0;
        }

        ret = f->write_video(sub->session, sub->subscribe_id, sub->track,
                             &out->frame);
        if (ret < 0) {
            f->write_errors++;
        } else {
            f->writes++;
        }
    }

    if (matched == 0) {
        f->no_sub++;
    }
}