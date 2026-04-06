#pragma once

#include <map>
#include <optional>
#include <vector>

#include <nlohmann/json.hpp>

#include "chrona/model.hpp"

namespace chrona {

// ---------------------------------------------------------------------------
// OutputNodeId – identifies one emitted version of one window.
// ---------------------------------------------------------------------------

struct OutputNodeId {
    WindowId window_id;
    uint64_t version = 0;

    bool operator<(const OutputNodeId& o) const {
        if (window_id != o.window_id) return window_id < o.window_id;
        return version < o.version;
    }
    bool operator==(const OutputNodeId& o) const {
        return window_id == o.window_id && version == o.version;
    }
};

inline void to_json(json& j, const OutputNodeId& n) {
    j = json{{"window_id", n.window_id}, {"version", n.version}};
}
inline void from_json(const json& j, OutputNodeId& n) {
    j.at("window_id").get_to(n.window_id);
    j.at("version").get_to(n.version);
}

// ---------------------------------------------------------------------------
// CausalRecord – minimal explanation payload for one output version.
// ---------------------------------------------------------------------------

struct CausalRecord {
    std::optional<EventId>   trigger_event_id;
    std::vector<EventId>     added_event_ids;
    std::vector<EventId>     removed_event_ids;
};

inline void to_json(json& j, const CausalRecord& r) {
    j = json::object();
    j["trigger_event_id"]  = r.trigger_event_id;
    j["added_event_ids"]   = r.added_event_ids;
    j["removed_event_ids"] = r.removed_event_ids;
}
inline void from_json(const json& j, CausalRecord& r) {
    j.at("trigger_event_id").get_to(r.trigger_event_id);
    j.at("added_event_ids").get_to(r.added_event_ids);
    j.at("removed_event_ids").get_to(r.removed_event_ids);
}

// ---------------------------------------------------------------------------
// CausalIndex – bipartite provenance graph stored as a map.
// ---------------------------------------------------------------------------

class CausalIndex {
    std::map<OutputNodeId, CausalRecord> by_output_;

public:
    void record(OutputNodeId node, CausalRecord rec);
    const CausalRecord* explain(const WindowId& wid, uint64_t version) const;

    // JSON friends must stay inline for ADL template lookup.
    friend void to_json(json& j, const CausalIndex& idx) {
        json arr = json::array();
        for (const auto& [k, v] : idx.by_output_)
            arr.push_back(json{{"node", k}, {"record", v}});
        j = arr;
    }
    friend void from_json(const json& j, CausalIndex& idx) {
        idx.by_output_.clear();
        for (const auto& item : j) {
            idx.by_output_.emplace(item.at("node").get<OutputNodeId>(),
                                   item.at("record").get<CausalRecord>());
        }
    }
};

} // namespace chrona
