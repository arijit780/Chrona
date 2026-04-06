#pragma once

#include <fstream>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "chrona/engine.hpp"

namespace chrona {

// ---------------------------------------------------------------------------
// Snapshot envelope
// ---------------------------------------------------------------------------

struct Snapshot {
    EngineState engine_state;
};

inline void to_json(json& j, const Snapshot& s) {
    j = json{{"engine_state", s.engine_state}};
}
inline void from_json(const json& j, Snapshot& s) {
    j.at("engine_state").get_to(s.engine_state);
}

// ---------------------------------------------------------------------------
// Load / Save / JSONL
// ---------------------------------------------------------------------------

std::optional<EngineState> load_snapshot(const std::string& path);

/// Write-to-temp then atomic rename; best-effort fsync.
bool save_snapshot_atomic(const std::string& path,
                          const EngineState& state);

void write_jsonl(std::ofstream& f, const json& j);

} // namespace chrona
