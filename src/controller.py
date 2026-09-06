from pathlib import Path
import json
import subprocess
import sys
import shutil

ROOT = Path(__file__).resolve().parent.parent

DRIFT_FILE = ROOT / "workload" / "drift_workload.json"
FREQ_FILE = ROOT / "workload" / "adaptive_frequency.json"
SELECTED_FILE = ROOT / "workload" / "selected_views.json"
REWRITE_FILE = ROOT / "workload" / "rewrite_plan.json"
RESULT_FILE = ROOT / "workload" / "adaptive_experiment.json"

SELECTOR = ROOT / "src" / "selector" / "selector.py"
APPLY = ROOT / "src" / "materialization" / "apply_selection.py"
REWRITER = ROOT / "src" / "rewriter.py"
FREQ_SCRIPT = ROOT / "src" / "workload_monitor" / "adaptive_frequency.py"
MATERIALIZER = ROOT / "src" / "materialization" / "generic_materializer.py"


def load_json(path, default=None):
    if not path.exists():
        return default
    with open(path) as f:
        return json.load(f)


def run_script(script):
    print(f"\n>>> Running {script.name}")
    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )

    if result.stdout:
        print(result.stdout)

    if result.returncode != 0:
        if result.stderr:
            print(result.stderr)
        raise RuntimeError(f"{script.name} failed")

    return result


def get_selected():
    data = load_json(SELECTED_FILE, [])

    selected = []

    for view in data:
        selected.append(view["candidate_id"])

    return selected


def get_storage():
    data = load_json(SELECTED_FILE, [])

    return round(
        sum(float(v.get("storage_mb", 0)) for v in data),
        4,
    )


def get_hit_rate():
    data = load_json(REWRITE_FILE, [])

    if isinstance(data, dict):
        rewritten = data.get("rewritten", [])
        total = data.get("total_queries", 10)

        if isinstance(rewritten, list):
            return round(len(rewritten) / total * 100, 2)

    if isinstance(data, list):
        total = len(data)

        if total == 0:
            return 0.0

        rewritten = 0

        for item in data:
            if item.get("match") is True:
                rewritten += 1
            elif item.get("rewritten") is True:
                rewritten += 1
            elif item.get("result") == "REWRITE":
                rewritten += 1

        return round(rewritten / total * 100, 2)

    return 0.0


def main():
    print("=" * 75)
    print("WAMVS END-TO-END ADAPTIVE EXPERIMENT")
    print("=" * 75)

    workload = load_json(DRIFT_FILE)

    if not workload:
        raise FileNotFoundError(
            "workload/drift_workload.json was not found."
        )

    if isinstance(workload, dict):
        intervals = workload.get("intervals", [])
    else:
        intervals = workload

    if not intervals:
        raise ValueError("No workload intervals found.")

    print(f"\nIntervals available: {len(intervals)}")

    # Backup current state.
    backup_dir = ROOT / "workload" / "adaptive_backup"
    backup_dir.mkdir(exist_ok=True)

    for source in [
        FREQ_FILE,
        SELECTED_FILE,
        REWRITE_FILE,
    ]:
        if source.exists():
            shutil.copy2(
                source,
                backup_dir / source.name,
            )

    experiment = []

    # Start this adaptive experiment from a clean frequency state.
    # Previous experiment history is preserved in adaptive_backup/.
    frequencies = {}

    # Reset the frequency file so old workload history cannot
    # contaminate this experiment.
    with open(FREQ_FILE, "w") as f:
        json.dump([], f, indent=2)

    decay = 0.70

    for interval in intervals:
        interval_id = interval.get(
            "interval",
            interval.get("interval_id", len(experiment) + 1),
        )

        phase = interval.get(
            "phase",
            "unknown",
        )

        # drift_workload.json stores workload frequencies
        # under the "query_counts" field.
        queries = interval.get(
            "query_counts",
            interval.get(
                "queries",
                interval.get("workload", {}),
            ),
        )

        print("\n")
        print("=" * 75)
        print(f"INTERVAL {interval_id} | {phase}")
        print("=" * 75)

        # ---------------------------------------------------------
        # Update exponentially decayed workload frequencies.
        # ---------------------------------------------------------
        all_queries = set(frequencies.keys()) | set(queries.keys())

        updated = {}

        for q in all_queries:
            old = frequencies.get(q, 0.0)
            current = float(queries.get(q, 0))

            updated[q] = decay * old + current

        frequencies = updated

        # The selector expects a list of interval records.
        # Preserve the complete adaptive-frequency history.
        existing_frequency_history = []

        if FREQ_FILE.exists():
            try:
                with open(FREQ_FILE) as f:
                    existing_frequency_history = json.load(f)

                if not isinstance(existing_frequency_history, list):
                    existing_frequency_history = []
            except Exception:
                existing_frequency_history = []

        # Keep only records produced by this experiment run.
        existing_frequency_history = [
            x for x in existing_frequency_history
            if isinstance(x, dict)
            and x.get("interval") != interval_id
        ]

        existing_frequency_history.append({
            "interval": interval_id,
            "phase": phase,
            "decay": decay,
            "frequencies": frequencies,
        })

        with open(FREQ_FILE, "w") as f:
            json.dump(
                existing_frequency_history,
                f,
                indent=2,
            )

        print("\nDecayed frequencies:")

        for q in sorted(frequencies):
            print(
                f"  {q}: "
                f"{frequencies[q]:.4f}"
            )

        # ---------------------------------------------------------
        # Restore all candidate MVs before making the next adaptive
        # decision. This is necessary because a view dropped in one
        # interval must be able to return in a later interval.
        # ---------------------------------------------------------
        run_script(MATERIALIZER)

        # ---------------------------------------------------------
        # Performance-aware selection.
        # ---------------------------------------------------------
        run_script(SELECTOR)

        selected = get_selected()
        storage = get_storage()

        # ---------------------------------------------------------
        # Track adaptive configuration changes.
        # ---------------------------------------------------------
        previous_selected = (
            experiment[-1]["selected_views"]
            if experiment
            else []
        )

        added_views = sorted(
            set(selected) - set(previous_selected)
        )

        dropped_views = sorted(
            set(previous_selected) - set(selected)
        )

        if added_views:
            print(
                "Added MVs    : "
                + ", ".join(added_views)
            )

        if dropped_views:
            print(
                "Dropped MVs  : "
                + ", ".join(dropped_views)
            )

        # ---------------------------------------------------------
        # Apply physical MV state.
        # ---------------------------------------------------------
        run_script(APPLY)

        # ---------------------------------------------------------
        # Regenerate rewrite plan.
        # ---------------------------------------------------------
        run_script(REWRITER)

        hit_rate = get_hit_rate()

        print("\n")
        print("-" * 75)
        print(f"INTERVAL {interval_id} RESULT")
        print("-" * 75)

        print(
            "Selected MVs : "
            + ", ".join(selected)
        )

        print(
            f"Storage      : {storage:.4f} MB"
        )

        print(
            f"Hit rate     : {hit_rate:.2f}%"
        )

        experiment.append(
            {
                "interval": interval_id,
                "phase": phase,
                "workload": queries,
                "decayed_frequency": frequencies.copy(),
                "selected_views": selected,
                "selected_count": len(selected),
                "added_views": added_views,
                "dropped_views": dropped_views,
                "storage_mb": storage,
                "hit_rate_percent": hit_rate,
            }
        )

    with open(RESULT_FILE, "w") as f:
        json.dump(
            experiment,
            f,
            indent=2,
        )

    print("\n")
    print("=" * 75)
    print("ADAPTIVE EXPERIMENT COMPLETE")
    print("=" * 75)

    print(
        f"\nResults written to:\n"
        f"  {RESULT_FILE}"
    )

    print("\nConfiguration timeline:")

    for row in experiment:
        print(
            f"  Interval {row['interval']} "
            f"({row['phase']:<8}) | "
            f"MVs={row['selected_views']} | "
            f"storage={row['storage_mb']:.4f} MB | "
            f"hit={row['hit_rate_percent']:.2f}%"
        )


if __name__ == "__main__":
    main()
