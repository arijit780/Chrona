#pragma once

#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <variant>
#include <vector>

#include <nlohmann/json.hpp>

#include "chrona/causal.hpp"
#include "chrona/model.hpp"
#include "chrona/ordering.hpp"
#include "chrona/window.hpp"

namespace chrona {

// ---------------------------------------------------------------------------
// WindowSpec variant – tumbling or sliding.
// ---------------------------------------------------------------------------

using WindowSpec = std::variant<TumblingWindowSpec, SlidingWindowSpec>;

inline void to_json(json& j, const WindowSpec& ws) {
    std::visit([&](auto&& spec) {
        using T = std::decay_t<decltype(spec)>;
        if constexpr (std::is_same_v<T, TumblingWindowSpec>)
            j = json{{"type", "Tumbling"}, {"spec", spec}};
        else
            j = json{{"type", "Sliding"},  {"spec", spec}};
    }, ws);
}
inline void from_json(const json& j, WindowSpec& ws) {
    if (j.at("type").get<std::string>() == "Tumbling")
        ws = j.at("spec").get<TumblingWindowSpec>();
    else
        ws = j.at("spec").get<SlidingWindowSpec>();
}

// ---------------------------------------------------------------------------
// EngineConfig
// ---------------------------------------------------------------------------

struct EngineConfig {
    WatermarkConfig watermark;
    WindowSpec      window;
};

// ---------------------------------------------------------------------------
// WindowState – per-window aggregate + provenance + emission history.
// ---------------------------------------------------------------------------

struct WindowState {
    int64_t      agg_state = 0;
    Contributors contributors;
    std::optional<int64_t> last_emitted_value;
    Contributors last_emitted_contributors;
    uint64_t     version = 0;
};

inline void to_json(json& j, const WindowState& ws) {
    j = json::object();
    j["agg_state"]                 = ws.agg_state;
    j["contributors"]              = ws.contributors;
    j["last_emitted_value"]        = ws.last_emitted_value;
    j["last_emitted_contributors"] = ws.last_emitted_contributors;
    j["version"]                   = ws.version;
}
inline void from_json(const json& j, WindowState& ws) {
    j.at("agg_state").get_to(ws.agg_state);
    j.at("contributors").get_to(ws.contributors);
    ws.last_emitted_value = j.at("last_emitted_value").get<std::optional<int64_t>>();
    j.at("last_emitted_contributors").get_to(ws.last_emitted_contributors);
    j.at("version").get_to(ws.version);
}

// ---------------------------------------------------------------------------
// EngineState – the portion that is persisted in snapshots.
// ---------------------------------------------------------------------------

struct EngineState {
    std::map<WindowId, WindowState> windows;
    CausalIndex                     causal;
    std::optional<uint64_t>         max_ingest_seq_processed;
};

inline void to_json(json& j, const EngineState& es) {
    json w = json::array();
    for (const auto& [k, v] : es.windows) {
        json entry = json::object();
        entry["window_id"] = k;
        entry["state"]     = v;
        w.push_back(std::move(entry));
    }
    j = json::object();
    j["windows"]                  = w;
    j["causal"]                   = es.causal;
    j["max_ingest_seq_processed"] = es.max_ingest_seq_processed;
}
inline void from_json(const json& j, EngineState& es) {
    es.windows.clear();
    for (const auto& item : j.at("windows"))
        es.windows.emplace(item.at("window_id").get<WindowId>(),
                           item.at("state").get<WindowState>());
    j.at("causal").get_to(es.causal);
    es.max_ingest_seq_processed =
        j.at("max_ingest_seq_processed").get<std::optional<uint64_t>>();
}

// ---------------------------------------------------------------------------
// StepOutput – outputs + drops produced by processing one buffered event.
// ---------------------------------------------------------------------------

struct StepOutput {
    std::vector<OutputRecord> outputs;
    std::vector<DropRecord>   drops;
};

// ---------------------------------------------------------------------------
// StreamEngine
// ---------------------------------------------------------------------------

class StreamEngine {
    EngineConfig   cfg_;
    OrderingBuffer buffer_;
    EngineState    state_;

    std::vector<WindowId> assign_windows(const Event& ev) const;

    std::optional<OutputRecord> maybe_emit(const EventId& trigger,
                                           bool out_of_order,
                                           const WindowId& wid,
                                           WindowState& ws,
                                           int64_t candidate);

public:
    StreamEngine(EngineConfig cfg, EngineState state);

    const EngineState& state() const { return state_; }

    std::optional<TimestampMicros> watermark() const {
        return buffer_.watermark();
    }

    /// Push an event into the buffer; returns a DropRecord if duplicate.
    std::optional<DropRecord> ingest(const Event& ev);

    /// Process exactly one buffered event in deterministic order.
    std::optional<StepOutput> step();

    /// Drain all buffered events.
    std::vector<StepOutput> drain();
};

} // namespace chrona
