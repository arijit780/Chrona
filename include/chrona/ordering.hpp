#pragma once

#include <functional>
#include <limits>
#include <optional>
#include <queue>
#include <set>
#include <tuple>
#include <vector>

#include "chrona/model.hpp"

namespace chrona {

// ---------------------------------------------------------------------------
// WatermarkConfig
// ---------------------------------------------------------------------------

struct WatermarkConfig {
    int64_t out_of_orderness_bound_micros = 0;
};

inline void to_json(json& j, const WatermarkConfig& c) {
    j = json{{"out_of_orderness_bound_micros", c.out_of_orderness_bound_micros}};
}
inline void from_json(const json& j, WatermarkConfig& c) {
    j.at("out_of_orderness_bound_micros").get_to(c.out_of_orderness_bound_micros);
}

// ---------------------------------------------------------------------------
// WatermarkState – pure function of events processed so far (deterministic).
// ---------------------------------------------------------------------------

struct WatermarkState {
    std::optional<TimestampMicros> max_event_time_seen;
    std::optional<TimestampMicros> watermark;

    /// Returns true if the watermark value changed.
    bool observe_event_time(TimestampMicros event_time,
                            const WatermarkConfig& cfg);
};

// ---------------------------------------------------------------------------
// OrderKey – total-order processing key: (system_time, ingest_seq).
// ---------------------------------------------------------------------------

struct OrderKey {
    TimestampMicros system_time = 0;
    IngestSeq       ingest_seq  = 0;

    bool operator<(const OrderKey& o) const {
        return std::tie(system_time, ingest_seq)
             < std::tie(o.system_time, o.ingest_seq);
    }
    bool operator>(const OrderKey& o) const { return o < *this; }
    bool operator==(const OrderKey& o) const {
        return std::tie(system_time, ingest_seq)
            == std::tie(o.system_time, o.ingest_seq);
    }
};

// ---------------------------------------------------------------------------
// PopResult
// ---------------------------------------------------------------------------

struct PopResult {
    Event                          ev;
    bool                           out_of_order      = false;
    bool                           watermark_advanced = false;
    std::optional<TimestampMicros> watermark;
};

// ---------------------------------------------------------------------------
// OrderingBuffer – deterministic ordering + deduplication + watermark.
// ---------------------------------------------------------------------------

class OrderingBuffer {
    struct BufferedEvent {
        OrderKey key;
        Event    ev;
        bool operator>(const BufferedEvent& o) const { return key > o.key; }
    };

    WatermarkConfig cfg_;
    WatermarkState  wm_;
    std::set<EventId> seen_ids_;
    std::priority_queue<BufferedEvent,
                        std::vector<BufferedEvent>,
                        std::greater<BufferedEvent>> heap_;

public:
    explicit OrderingBuffer(WatermarkConfig cfg) : cfg_(cfg) {}

    std::optional<TimestampMicros> watermark() const { return wm_.watermark; }
    std::optional<TimestampMicros> max_event_time_seen() const {
        return wm_.max_event_time_seen;
    }

    /// Returns false if the event is a duplicate (by event_id).
    bool push(const Event& ev);

    /// Pop the next event in deterministic processing order.
    std::optional<PopResult> pop_next();
};

} // namespace chrona
