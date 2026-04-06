#include "chrona/causal.hpp"

namespace chrona {

void CausalIndex::record(OutputNodeId node, CausalRecord rec) {
    by_output_.emplace(std::move(node), std::move(rec));
}

const CausalRecord*
CausalIndex::explain(const WindowId& wid, uint64_t version) const {
    auto it = by_output_.find(OutputNodeId{wid, version});
    return it != by_output_.end() ? &it->second : nullptr;
}

} // namespace chrona
