#include "chrona/window.hpp"

namespace chrona {

// ---------------------------------------------------------------------------
// TumblingWindowSpec
// ---------------------------------------------------------------------------

WindowId TumblingWindowSpec::window_for(const std::string& key,
                                        TimestampMicros t) const {
    int64_t s = floor_div(t, size_micros) * size_micros;
    return WindowId{key, s, s + size_micros};
}

// ---------------------------------------------------------------------------
// SlidingWindowSpec
// ---------------------------------------------------------------------------

std::vector<WindowId>
SlidingWindowSpec::windows_for(const std::string& key,
                               TimestampMicros t) const {
    int64_t latest = floor_div(t, slide_micros) * slide_micros;
    int64_t first  = floor_div(t - size_micros + 1, slide_micros)
                     * slide_micros;

    std::vector<WindowId> out;
    for (int64_t s = first; s <= latest; s += slide_micros) {
        int64_t e = s + size_micros;
        if (s <= t && t < e)
            out.push_back(WindowId{key, s, e});
    }
    std::sort(out.begin(), out.end());
    return out;
}

} // namespace chrona
