#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "chrona/model.hpp"

namespace chrona {

/// Euclidean floor-division (correct for negative dividends).
inline int64_t floor_div(int64_t a, int64_t b) {
    return a / b - (a % b != 0 && (a ^ b) < 0);
}

// ---------------------------------------------------------------------------
// TumblingWindowSpec
// ---------------------------------------------------------------------------

struct TumblingWindowSpec {
    int64_t size_micros = 0;

    WindowId window_for(const std::string& key, TimestampMicros t) const;
};

inline void to_json(json& j, const TumblingWindowSpec& s) {
    j = json{{"size_micros", s.size_micros}};
}
inline void from_json(const json& j, TumblingWindowSpec& s) {
    j.at("size_micros").get_to(s.size_micros);
}

// ---------------------------------------------------------------------------
// SlidingWindowSpec
// ---------------------------------------------------------------------------

struct SlidingWindowSpec {
    int64_t size_micros  = 0;
    int64_t slide_micros = 0;

    /// All windows [start,end) of length size with step slide that contain t.
    std::vector<WindowId> windows_for(const std::string& key,
                                      TimestampMicros t) const;
};

inline void to_json(json& j, const SlidingWindowSpec& s) {
    j = json{{"size_micros", s.size_micros}, {"slide_micros", s.slide_micros}};
}
inline void from_json(const json& j, SlidingWindowSpec& s) {
    j.at("size_micros").get_to(s.size_micros);
    j.at("slide_micros").get_to(s.slide_micros);
}

// ---------------------------------------------------------------------------
// SumAggregator – stateless helper; state is a plain int64_t.
// ---------------------------------------------------------------------------

struct SumAggregator {
    static int64_t zero()                          { return 0; }
    static void    add(int64_t& state, int64_t v)  { state += v; }
    static int64_t output(int64_t state)            { return state; }
};

} // namespace chrona
