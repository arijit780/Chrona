#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>

#include "chrona/engine.hpp"
#include "chrona/persistence.hpp"

namespace fs = std::filesystem;
using namespace chrona;

// ===================================================================
// CLI argument helpers
// ===================================================================

struct RunArgs {
    std::string event_log;
    std::string output_log;
    std::string drops_log;
    std::string snapshot_dir;
    int64_t  window_size_micros            = 0;
    int64_t  out_of_orderness_bound_micros = 0;
    uint64_t snapshot_every                = 0;
    bool     use_snapshot                  = true;
    bool     append_logs                   = false;
    uint64_t stop_after                    = 0; // 0 = unlimited
};

struct ReplayCheckArgs {
    std::string event_log;
    std::string work_dir;
    int64_t  window_size_micros            = 0;
    int64_t  out_of_orderness_bound_micros = 0;
    uint64_t crash_after                   = 2;
};

using FlagMap = std::unordered_map<std::string, std::string>;

static FlagMap parse_flags(int argc, char* argv[], int start) {
    FlagMap m;
    for (int i = start; i + 1 < argc; i += 2)
        m[argv[i]] = argv[i + 1];
    return m;
}

// ===================================================================
// run
// ===================================================================

static int run(const RunArgs& args) {
    fs::create_directories(args.snapshot_dir);
    std::string snap_path = args.snapshot_dir + "/snapshot.json";

    EngineState state;
    if (args.use_snapshot) {
        if (auto loaded = load_snapshot(snap_path))
            state = std::move(*loaded);
    }

    EngineConfig cfg;
    cfg.watermark.out_of_orderness_bound_micros = args.out_of_orderness_bound_micros;
    cfg.window = TumblingWindowSpec{args.window_size_micros};

    StreamEngine engine(cfg, std::move(state));

    auto mode = args.append_logs ? std::ios::app : std::ios::trunc;
    std::ofstream out_f(args.output_log, mode);
    std::ofstream drops_f(args.drops_log, mode);

    std::ifstream in_f(args.event_log);
    if (!in_f.is_open()) {
        std::cerr << "Cannot open event log: " << args.event_log << '\n';
        return 1;
    }

    uint64_t min_seq   = engine.state().max_ingest_seq_processed.value_or(0);
    uint64_t processed = 0;

    std::string line;
    while (std::getline(in_f, line)) {
        if (line.empty()
            || line.find_first_not_of(" \t\r\n") == std::string::npos)
            continue;

        auto ev = json::parse(line).get<Event>();
        if (ev.ingest_seq <= min_seq) continue;

        if (auto drop = engine.ingest(ev))
            write_jsonl(drops_f, json(*drop));

        for (auto& step : engine.drain()) {
            for (auto& o : step.outputs) write_jsonl(out_f, json(o));
            for (auto& d : step.drops)   write_jsonl(drops_f, json(d));

            ++processed;
            if (args.snapshot_every > 0
                && processed % args.snapshot_every == 0)
                save_snapshot_atomic(snap_path, engine.state());

            if (args.stop_after > 0 && processed >= args.stop_after) {
                save_snapshot_atomic(snap_path, engine.state());
                out_f.flush();
                drops_f.flush();
                return 0;
            }
        }
    }

    save_snapshot_atomic(snap_path, engine.state());
    out_f.flush();
    drops_f.flush();
    return 0;
}

// ===================================================================
// replay-check
// ===================================================================

static std::string slurp(const std::string& path) {
    std::ifstream f(path);
    return {std::istreambuf_iterator<char>(f), {}};
}

static int replay_check(const ReplayCheckArgs& args) {
    fs::create_directories(args.work_dir);

    std::string full_dir   = args.work_dir + "/full";
    std::string resume_dir = args.work_dir + "/resume";
    fs::create_directories(full_dir);
    fs::create_directories(resume_dir);

    // 1) Full run from scratch.
    {
        RunArgs ra{};
        ra.event_log                   = args.event_log;
        ra.output_log                  = full_dir + "/outputs.jsonl";
        ra.drops_log                   = full_dir + "/drops.jsonl";
        ra.snapshot_dir                = full_dir + "/snapshots";
        ra.window_size_micros          = args.window_size_micros;
        ra.out_of_orderness_bound_micros = args.out_of_orderness_bound_micros;
        ra.use_snapshot                = false;
        if (int rc = run(ra)) return rc;
    }

    // 2) Partial run (simulated crash after N processed events).
    {
        RunArgs ra{};
        ra.event_log                   = args.event_log;
        ra.output_log                  = resume_dir + "/outputs.jsonl";
        ra.drops_log                   = resume_dir + "/drops.jsonl";
        ra.snapshot_dir                = resume_dir + "/snapshots";
        ra.window_size_micros          = args.window_size_micros;
        ra.out_of_orderness_bound_micros = args.out_of_orderness_bound_micros;
        ra.use_snapshot                = false;
        ra.stop_after                  = args.crash_after;
        if (int rc = run(ra)) return rc;
    }

    // 3) Resume from snapshot.
    {
        RunArgs ra{};
        ra.event_log                   = args.event_log;
        ra.output_log                  = resume_dir + "/outputs.jsonl";
        ra.drops_log                   = resume_dir + "/drops.jsonl";
        ra.snapshot_dir                = resume_dir + "/snapshots";
        ra.window_size_micros          = args.window_size_micros;
        ra.out_of_orderness_bound_micros = args.out_of_orderness_bound_micros;
        ra.use_snapshot                = true;
        ra.append_logs                 = true;
        if (int rc = run(ra)) return rc;
    }

    // Compare byte-for-byte.
    if (slurp(full_dir + "/outputs.jsonl")
        != slurp(resume_dir + "/outputs.jsonl")) {
        std::cerr << "ReplayCheck FAILED: outputs differ\n";
        return 1;
    }
    if (slurp(full_dir + "/drops.jsonl")
        != slurp(resume_dir + "/drops.jsonl")) {
        std::cerr << "ReplayCheck FAILED: drops differ\n";
        return 1;
    }

    std::cout << "ReplayCheck PASSED: outputs and drops are byte-identical.\n";
    return 0;
}

// ===================================================================
// main
// ===================================================================

static void usage() {
    std::cerr << R"(Usage:
  chrona run [flags]
  chrona replay-check [flags]

Run flags:
  --event-log PATH
  --output-log PATH
  --drops-log PATH
  --snapshot-dir PATH
  --window-size-micros N
  --out-of-orderness-bound-micros N
  --snapshot-every N        (default 0 = off)
  --use-snapshot true|false (default true)
  --append-logs  true|false (default false)
  --stop-after   N          (default 0 = unlimited)

Replay-check flags:
  --event-log PATH
  --work-dir PATH
  --window-size-micros N
  --out-of-orderness-bound-micros N
  --crash-after N           (default 2)
)";
}

int main(int argc, char* argv[]) {
    if (argc < 2) { usage(); return 1; }

    std::string cmd = argv[1];

    if (cmd == "run") {
        auto fl = parse_flags(argc, argv, 2);
        RunArgs ra{};
        ra.event_log   = fl["--event-log"];
        ra.output_log  = fl["--output-log"];
        ra.drops_log   = fl["--drops-log"];
        ra.snapshot_dir = fl["--snapshot-dir"];
        if (fl.count("--window-size-micros"))
            ra.window_size_micros = std::stoll(fl["--window-size-micros"]);
        if (fl.count("--out-of-orderness-bound-micros"))
            ra.out_of_orderness_bound_micros =
                std::stoll(fl["--out-of-orderness-bound-micros"]);
        if (fl.count("--snapshot-every"))
            ra.snapshot_every = std::stoull(fl["--snapshot-every"]);
        if (fl.count("--use-snapshot"))
            ra.use_snapshot = fl["--use-snapshot"] != "false";
        if (fl.count("--append-logs"))
            ra.append_logs = fl["--append-logs"] == "true";
        if (fl.count("--stop-after"))
            ra.stop_after = std::stoull(fl["--stop-after"]);
        return run(ra);

    } else if (cmd == "replay-check") {
        auto fl = parse_flags(argc, argv, 2);
        ReplayCheckArgs ra{};
        ra.event_log = fl["--event-log"];
        ra.work_dir  = fl["--work-dir"];
        if (fl.count("--window-size-micros"))
            ra.window_size_micros = std::stoll(fl["--window-size-micros"]);
        if (fl.count("--out-of-orderness-bound-micros"))
            ra.out_of_orderness_bound_micros =
                std::stoll(fl["--out-of-orderness-bound-micros"]);
        if (fl.count("--crash-after"))
            ra.crash_after = std::stoull(fl["--crash-after"]);
        return replay_check(ra);
    }

    usage();
    return 1;
}
