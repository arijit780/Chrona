#include "chrona/model.hpp"

#include <algorithm>
#include <iterator>

namespace chrona {

std::vector<EventId> Contributors::to_sorted_vec() const {
    return {set_.begin(), set_.end()};
}

std::pair<std::vector<EventId>, std::vector<EventId>>
Contributors::diff(const Contributors& prev) const {
    std::vector<EventId> added;
    std::set_difference(set_.begin(), set_.end(),
                        prev.set_.begin(), prev.set_.end(),
                        std::back_inserter(added));

    std::vector<EventId> removed;
    std::set_difference(prev.set_.begin(), prev.set_.end(),
                        set_.begin(), set_.end(),
                        std::back_inserter(removed));

    return {added, removed};
}

} // namespace chrona
