#include "chrona/persistence.hpp"

#include <cstdio>
#include <fstream>

namespace chrona {

std::optional<EngineState> load_snapshot(const std::string& path) {
    std::ifstream f(path);
    if (!f.is_open())
        return std::nullopt;
    try {
        return json::parse(f).get<Snapshot>().engine_state;
    } catch (...) {
        return std::nullopt;
    }
}

bool save_snapshot_atomic(const std::string& path,
                          const EngineState& state) {
    std::string tmp = path + ".tmp";
    {
        std::ofstream f(tmp, std::ios::trunc);
        if (!f.is_open())
            return false;
        f << json(Snapshot{state}).dump();
        f.flush();
    }
    return std::rename(tmp.c_str(), path.c_str()) == 0;
}

void write_jsonl(std::ofstream& f, const json& j) {
    f << j.dump() << '\n';
}

} // namespace chrona
