#pragma once

#include <algorithm>
#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

// nlohmann/json v3 does not natively handle std::optional.
// This specialization must appear before any to_json/from_json that uses it.
namespace nlohmann {
template <typename T>
struct adl_serializer<std::optional<T>> {
    static void to_json(json& j, const std::optional<T>& opt) {
        j = opt.has_value() ? json(*opt) : json(nullptr);
    }
    static void from_json(const json& j, std::optional<T>& opt) {
        opt = j.is_null() ? std::nullopt : std::optional<T>(j.get<T>());
    }
};
} // namespace nlohmann

namespace chrona {

using TimestampMicros = int64_t;
using IngestSeq       = uint64_t;
using json            = nlohmann::json;

// ---------------------------------------------------------------------------
// EventId – thin wrapper for type safety; serialises as a bare string.
// ---------------------------------------------------------------------------

struct EventId {
    std::string id;

    bool operator< (const EventId& o) const { return id <  o.id; }
    bool operator==(const EventId& o) const { return id == o.id; }
    bool operator!=(const EventId& o) const { return id != o.id; }
};

inline void to_json(json& j, const EventId& e) { j = e.id; }
inline void from_json(const json& j, EventId& e) { j.get_to(e.id); }

// ---------------------------------------------------------------------------
// Kv – demo payload: keyed integer value.
// ---------------------------------------------------------------------------

struct Kv {
    std::string key;
    int64_t     value = 0;
};

inline void to_json(json& j, const Kv& kv) {
    j = json{{"key", kv.key}, {"value", kv.value}};
}
inline void from_json(const json& j, Kv& kv) {
    j.at("key").get_to(kv.key);
    j.at("value").get_to(kv.value);
}

// ---------------------------------------------------------------------------
// Event – carries both event_time and system_time explicitly.
// ---------------------------------------------------------------------------

struct Event {
    EventId         event_id;
    TimestampMicros event_time  = 0;
    TimestampMicros system_time = 0;
    IngestSeq       ingest_seq  = 0;
    Kv              payload;
};

inline void to_json(json& j, const Event& e) {
    j = json::object();
    j["event_id"]    = e.event_id;
    j["event_time"]  = e.event_time;
    j["system_time"] = e.system_time;
    j["ingest_seq"]  = e.ingest_seq;
    j["payload"]     = e.payload;
}
inline void from_json(const json& j, Event& e) {
    j.at("event_id").get_to(e.event_id);
    j.at("event_time").get_to(e.event_time);
    j.at("system_time").get_to(e.system_time);
    j.at("ingest_seq").get_to(e.ingest_seq);
    j.at("payload").get_to(e.payload);
}

// ---------------------------------------------------------------------------
// WindowId – [start, end) for a given key; deterministic Ord via tuple.
// ---------------------------------------------------------------------------

struct WindowId {
    std::string     key;
    TimestampMicros start = 0;
    TimestampMicros end   = 0;

    bool operator< (const WindowId& o) const {
        return std::tie(key, start, end) < std::tie(o.key, o.start, o.end);
    }
    bool operator==(const WindowId& o) const {
        return std::tie(key, start, end) == std::tie(o.key, o.start, o.end);
    }
    bool operator!=(const WindowId& o) const { return !(*this == o); }
};

inline void to_json(json& j, const WindowId& w) {
    j = json{{"key", w.key}, {"start", w.start}, {"end", w.end}};
}
inline void from_json(const json& j, WindowId& w) {
    j.at("key").get_to(w.key);
    j.at("start").get_to(w.start);
    j.at("end").get_to(w.end);
}

// ---------------------------------------------------------------------------
// EmissionReason
// ---------------------------------------------------------------------------

enum class EmissionReason { Initial, UpdateLateData, Correction };

NLOHMANN_JSON_SERIALIZE_ENUM(EmissionReason, {
    {EmissionReason::Initial,        "Initial"},
    {EmissionReason::UpdateLateData, "UpdateLateData"},
    {EmissionReason::Correction,     "Correction"},
})

// ---------------------------------------------------------------------------
// OutputDiff
// ---------------------------------------------------------------------------

struct OutputDiff {
    int64_t              old_value = 0;
    int64_t              new_value = 0;
    std::vector<EventId> added_event_ids;
    std::vector<EventId> removed_event_ids;
};

inline void to_json(json& j, const OutputDiff& d) {
    j = json::object();
    j["old_value"]         = d.old_value;
    j["new_value"]         = d.new_value;
    j["added_event_ids"]   = d.added_event_ids;
    j["removed_event_ids"] = d.removed_event_ids;
}
inline void from_json(const json& j, OutputDiff& d) {
    j.at("old_value").get_to(d.old_value);
    j.at("new_value").get_to(d.new_value);
    j.at("added_event_ids").get_to(d.added_event_ids);
    j.at("removed_event_ids").get_to(d.removed_event_ids);
}

// ---------------------------------------------------------------------------
// OutputRecord – every emitted result includes provenance + diff.
// ---------------------------------------------------------------------------

struct OutputRecord {
    WindowId                  window_id;
    int64_t                   value   = 0;
    uint64_t                  version = 0;
    std::vector<EventId>      contributors;
    EmissionReason            reason = EmissionReason::Initial;
    std::optional<EventId>    trigger_event_id;
    std::optional<OutputDiff> diff;
};

inline void to_json(json& j, const OutputRecord& r) {
    j = json::object();
    j["window_id"]        = r.window_id;
    j["value"]            = r.value;
    j["version"]          = r.version;
    j["contributors"]     = r.contributors;
    j["reason"]           = r.reason;
    j["trigger_event_id"] = r.trigger_event_id;
    j["diff"]             = r.diff;
}
inline void from_json(const json& j, OutputRecord& r) {
    j.at("window_id").get_to(r.window_id);
    j.at("value").get_to(r.value);
    j.at("version").get_to(r.version);
    j.at("contributors").get_to(r.contributors);
    j.at("reason").get_to(r.reason);
    r.trigger_event_id = j.at("trigger_event_id").get<std::optional<EventId>>();
    r.diff             = j.at("diff").get<std::optional<OutputDiff>>();
}

// ---------------------------------------------------------------------------
// DropDecision / DropRecord
// ---------------------------------------------------------------------------

enum class DropDecision { DuplicateDrop, LateDrop };

NLOHMANN_JSON_SERIALIZE_ENUM(DropDecision, {
    {DropDecision::DuplicateDrop, "DuplicateDrop"},
    {DropDecision::LateDrop,      "LateDrop"},
})

struct DropRecord {
    EventId                   event_id;
    std::optional<WindowId>   window_id;
    DropDecision              decision = DropDecision::LateDrop;
    TimestampMicros           watermark_at_decision = 0;
};

inline void to_json(json& j, const DropRecord& d) {
    j = json::object();
    j["event_id"]              = d.event_id;
    j["window_id"]             = d.window_id;
    j["decision"]              = d.decision;
    j["watermark_at_decision"] = d.watermark_at_decision;
}
inline void from_json(const json& j, DropRecord& d) {
    j.at("event_id").get_to(d.event_id);
    d.window_id = j.at("window_id").get<std::optional<WindowId>>();
    j.at("decision").get_to(d.decision);
    j.at("watermark_at_decision").get_to(d.watermark_at_decision);
}

// ---------------------------------------------------------------------------
// Contributors – deterministic set backed by std::set (ordered).
// ---------------------------------------------------------------------------

class Contributors {
    std::set<EventId> set_;

public:
    Contributors() = default;

    bool insert(const EventId& id) { return set_.insert(id).second; }
    bool contains(const EventId& id) const { return set_.count(id) > 0; }

    std::vector<EventId> to_sorted_vec() const;

    std::pair<std::vector<EventId>, std::vector<EventId>>
    diff(const Contributors& prev) const;

    bool operator==(const Contributors& o) const { return set_ == o.set_; }
    bool operator!=(const Contributors& o) const { return set_ != o.set_; }

    friend void to_json(json& j, const Contributors& c) { j = c.to_sorted_vec(); }
    friend void from_json(const json& j, Contributors& c) {
        c.set_.clear();
        for (const auto& item : j) c.set_.insert(item.get<EventId>());
    }
};

} // namespace chrona
