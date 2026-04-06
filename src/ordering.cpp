#include "chrona/ordering.hpp"

namespace chrona {

// ---------------------------------------------------------------------------
// WatermarkState
// ---------------------------------------------------------------------------

bool WatermarkState::observe_event_time(TimestampMicros event_time,
                                        const WatermarkConfig& cfg) {
    auto prev = watermark;
    max_event_time_seen = max_event_time_seen
        ? std::max(*max_event_time_seen, event_time)
        : event_time;
    watermark = *max_event_time_seen - cfg.out_of_orderness_bound_micros;
    return watermark != prev;
}

// ---------------------------------------------------------------------------
// OrderingBuffer
// ---------------------------------------------------------------------------

bool OrderingBuffer::push(const Event& ev) {
    if (!seen_ids_.insert(ev.event_id).second)
        return false;
    heap_.push(BufferedEvent{{ev.system_time, ev.ingest_seq}, ev});
    return true;
}

std::optional<PopResult> OrderingBuffer::pop_next() {
    if (heap_.empty())
        return std::nullopt;

    auto be = heap_.top();
    heap_.pop();

    bool ooo = wm_.max_event_time_seen.has_value()
            && be.ev.event_time < *wm_.max_event_time_seen;
    bool advanced = wm_.observe_event_time(be.ev.event_time, cfg_);
    return PopResult{std::move(be.ev), ooo, advanced, wm_.watermark};
}

} // namespace chrona
