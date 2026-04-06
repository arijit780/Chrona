# Chrona

Single-node stream processing engine (C++17) with:

- **event-time semantics** + explicit system-time arrival tracking
- **speculative outputs** with versioned revisions (diffs, not overwrites)
- **causal provenance** for "why did this change?"
- **deterministic replay** via event log + snapshots

## Layout

```
include/chrona/
  model.hpp         – Event, WindowId, OutputRecord, DropRecord, Contributors
  ordering.hpp      – OrderingBuffer, WatermarkState (deterministic)
  window.hpp        – TumblingWindowSpec, SlidingWindowSpec, SumAggregator
  causal.hpp        – CausalIndex, CausalRecord
  engine.hpp        – StreamEngine, EngineState, WindowState
  persistence.hpp   – Snapshot load/save, JSONL I/O
src/
  main.cpp          – CLI (run + replay-check)
data/
  example_events.jsonl
```

## Build

Requires CMake 3.14+ and a C++17 compiler.
nlohmann/json is fetched automatically via CMake FetchContent.

```bash
cmake -B build
cmake --build build
```

## Example

Run the example event log (out-of-order + revision + very-late drop):

```bash
./build/chrona run \
  --event-log data/example_events.jsonl \
  --output-log out/outputs.jsonl \
  --drops-log out/drops.jsonl \
  --snapshot-dir out/snapshots \
  --window-size-micros 10000000 \
  --out-of-orderness-bound-micros 6000000
```

Deterministic replay check (full run vs crash+resume with snapshot):

```bash
./build/chrona replay-check \
  --event-log data/example_events.jsonl \
  --work-dir out/replay_check \
  --window-size-micros 10000000 \
  --out-of-orderness-bound-micros 6000000 \
  --crash-after 2
```

## Walkthrough

With `window_size = 10s`, `out_of_orderness_bound = 6s`, and `sum(value)`:

| Step | Event            | max_event_time | watermark | Action                                                            |
|------|------------------|----------------|-----------|-------------------------------------------------------------------|
| 1    | E1 (t=12s, v=5)  | 12s            | 6s        | Window [10,20) v1: value=5, reason=Initial                       |
| 2    | E2 (t=18s, v=2)  | 18s            | 12s       | Window [10,20) v2: value=7, reason=Correction, added=[E2]        |
| 3    | E3 (t=14s, v=7)  | 18s            | 12s       | Window [10,20) v3: value=14, reason=UpdateLateData, added=[E3]   |
| 4    | E4 (t=11s, v=3)  | 18s            | 12s       | LateDrop (11s < 12s watermark) — no revision                     |

The output for step 3 answers "why did the value change from 7 to 14?" via
`trigger_event_id = E3` and `added_event_ids = [E3]`.
