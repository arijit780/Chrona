#include "chrona/engine.hpp"

#include <limits>

namespace chrona {

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

StreamEngine::StreamEngine(EngineConfig cfg, EngineState state)
    : cfg_(std::move(cfg))
    , buffer_(cfg_.watermark)
    , state_(std::move(state)) {}

// ---------------------------------------------------------------------------
// Ingestion
// ---------------------------------------------------------------------------

std::optional<DropRecord> StreamEngine::ingest(const Event& ev) {
    auto wm = buffer_.watermark().value_or(
        std::numeric_limits<int64_t>::min());

    if (!buffer_.push(ev))
        return DropRecord{ev.event_id, std::nullopt,
                          DropDecision::DuplicateDrop, wm};
    return std::nullopt;
}

// ---------------------------------------------------------------------------
// Window assignment
// ---------------------------------------------------------------------------

std::vector<WindowId> StreamEngine::assign_windows(const Event& ev) const {
    return std::visit([&](auto&& spec) -> std::vector<WindowId> {
        using T = std::decay_t<decltype(spec)>;
        if constexpr (std::is_same_v<T, TumblingWindowSpec>)
            return {spec.window_for(ev.payload.key, ev.event_time)};
        else
            return spec.windows_for(ev.payload.key, ev.event_time);
    }, cfg_.window);
}

// ---------------------------------------------------------------------------
// Revision / emission logic
// ---------------------------------------------------------------------------

std::optional<OutputRecord>
StreamEngine::maybe_emit(const EventId& trigger,
                         bool out_of_order,
                         const WindowId& wid,
                         WindowState& ws,
                         int64_t candidate) {
    auto new_c  = ws.contributors.to_sorted_vec();
    auto prev_c = ws.last_emitted_contributors.to_sorted_vec();

    bool changed = !ws.last_emitted_value.has_value()
                || *ws.last_emitted_value != candidate
                || prev_c != new_c;
    if (!changed)
        return std::nullopt;

    EmissionReason reason;
    if (!ws.last_emitted_value.has_value())
        reason = EmissionReason::Initial;
    else if (out_of_order)
        reason = EmissionReason::UpdateLateData;
    else
        reason = EmissionReason::Correction;

    std::optional<OutputDiff> diff;
    if (ws.last_emitted_value.has_value()) {
        auto [added, removed] =
            ws.contributors.diff(ws.last_emitted_contributors);
        diff = OutputDiff{*ws.last_emitted_value, candidate,
                          std::move(added), std::move(removed)};
    }

    ++ws.version;

    OutputRecord rec;
    rec.window_id        = wid;
    rec.value            = candidate;
    rec.version          = ws.version;
    rec.contributors     = new_c;
    rec.reason           = reason;
    rec.trigger_event_id = trigger;
    rec.diff             = diff;

    auto [ca, cr] = ws.contributors.diff(ws.last_emitted_contributors);
    state_.causal.record(
        OutputNodeId{wid, ws.version},
        CausalRecord{trigger, std::move(ca), std::move(cr)});

    ws.last_emitted_value        = candidate;
    ws.last_emitted_contributors = ws.contributors;

    return rec;
}

// ---------------------------------------------------------------------------
// Step / Drain
// ---------------------------------------------------------------------------

std::optional<StepOutput> StreamEngine::step() {
    auto pop = buffer_.pop_next();
    if (!pop)
        return std::nullopt;

    auto& [ev, ooo, wm_adv, wm_opt] = *pop;
    state_.max_ingest_seq_processed = ev.ingest_seq;

    StepOutput out;
    auto wm = wm_opt.value_or(std::numeric_limits<int64_t>::min());

    if (ev.event_time < wm) {
        out.drops.push_back(
            DropRecord{ev.event_id, std::nullopt,
                       DropDecision::LateDrop, wm});
        return out;
    }

    for (auto& wid : assign_windows(ev)) {
        auto& ws = state_.windows[wid];
        SumAggregator::add(ws.agg_state, ev.payload.value);
        ws.contributors.insert(ev.event_id);

        int64_t candidate = SumAggregator::output(ws.agg_state);
        if (auto rec = maybe_emit(ev.event_id, ooo, wid, ws, candidate))
            out.outputs.push_back(std::move(*rec));
    }
    return out;
}

std::vector<StepOutput> StreamEngine::drain() {
    std::vector<StepOutput> outs;
    while (auto s = step())
        outs.push_back(std::move(*s));
    return outs;
}

} // namespace chrona
