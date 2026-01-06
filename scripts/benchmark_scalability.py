"""
Benchmark Spark vs single-node sequence building to illustrate scalability.

What it does:
- Spark path: uses existing pipeline functions to load parsed logs and build sequences with Spark.
- Single-node path: loads the same parsed logs into pandas and builds sequences with groupby/aggregation.
- Writes a JSON summary to results/benchmark/scalability_comparison.json.

Usage:
    python scripts/benchmark_scalability.py           # run both paths
    python scripts/benchmark_scalability.py --spark   # Spark only
    python scripts/benchmark_scalability.py --pandas  # pandas only
"""

import argparse
import json
import os
import time

import psutil

import config


class ResourceSampler:
    """Lightweight CPU/memory sampler for the current process."""
    
    def __init__(self, interval: float = 0.2):
        self.interval = interval
        self.process = psutil.Process(os.getpid())
        self.cpu_samples = []
        self.mem_samples = []
        self.running = False
    
    def start(self):
        self.running = True
        while self.running:
            self.cpu_samples.append(self.process.cpu_percent(interval=None))
            mem = self.process.memory_info().rss / (1024 * 1024)  # MB
            self.mem_samples.append(mem)
            time.sleep(self.interval)
    
    def stop_and_stats(self):
        self.running = False
        if not self.cpu_samples:
            return {}
        return {
            "cpu_avg": sum(self.cpu_samples) / len(self.cpu_samples),
            "cpu_max": max(self.cpu_samples),
            "mem_avg_mb": sum(self.mem_samples) / len(self.mem_samples),
            "mem_max_mb": max(self.mem_samples),
        }


def benchmark_spark():
    """Run the Spark sequence-building path and time it."""
    from steps.build_sequences import (
        create_spark_session,
        stop_spark_session,
        build_sequences_optimized,
        add_sequence_metadata,
        filter_sequences_by_length,
        load_parsed_logs,
    )

    start = time.perf_counter()
    sampler = ResourceSampler()
    import threading
    t = threading.Thread(target=sampler.start, daemon=True)
    t.start()

    spark = create_spark_session()
    try:
        df_parsed = load_parsed_logs(spark)
        df_sequences = build_sequences_optimized(df_parsed)
        df_sequences = add_sequence_metadata(df_sequences)
        df_sequences = filter_sequences_by_length(df_sequences, min_length=config.MIN_SEQUENCE_LENGTH)
        count = df_sequences.count()
    finally:
        stop_spark_session(spark)
        sampler.stop_and_stats()
    elapsed = time.perf_counter() - start
    resource_stats = sampler.stop_and_stats()
    return {
        "path": "spark",
        "seconds": elapsed,
        "num_sequences": count,
        "resources": resource_stats,
    }


def benchmark_pandas():
    """Run a single-node pandas sequence-building path and time it."""
    import pandas as pd

    start = time.perf_counter()
    sampler = ResourceSampler()
    import threading
    t = threading.Thread(target=sampler.start, daemon=True)
    t.start()

    df = pd.read_parquet(config.PARSED_LOGS_PATH)

    # Ensure expected columns exist
    required_cols = {"BlockId", "EventId", "Timestamp"}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise ValueError(f"Parsed logs missing required columns: {missing}")

    # Sort to preserve order within BlockId
    df = df.sort_values(["BlockId", "Timestamp"])

    grouped = df.groupby("BlockId")
    sequences = grouped["EventId"].apply(list)
    timestamps = grouped["Timestamp"].apply(list)

    seq_lengths = sequences.apply(len)
    unique_events = grouped["EventId"].nunique()

    # Compute time span (seconds) if timestamps are datetime-like
    if pd.api.types.is_datetime64_any_dtype(df["Timestamp"]):
        time_spans = (grouped["Timestamp"].max() - grouped["Timestamp"].min()).dt.total_seconds()
    else:
        time_spans = None

    result = {
        "num_sequences": int(len(sequences)),
        "avg_length": float(seq_lengths.mean()),
        "min_length": int(seq_lengths.min()),
        "max_length": int(seq_lengths.max()),
        "avg_unique_events": float(unique_events.mean()),
    }
    if time_spans is not None:
        result["avg_time_span_seconds"] = float(time_spans.mean())

    elapsed = time.perf_counter() - start
    resource_stats = sampler.stop_and_stats()
    result["seconds"] = elapsed
    result["path"] = "pandas"
    result["resources"] = resource_stats
    return result


def run_benchmark(run_spark: bool = True, run_pandas: bool = True):
    """Run configured benchmarks and return the results list."""
    results = []
    if run_spark:
        print("Running Spark benchmark...")
        results.append(benchmark_spark())

    if run_pandas:
        print("Running pandas benchmark...")
        results.append(benchmark_pandas())

    os.makedirs(os.path.join("results", "benchmark"), exist_ok=True)
    out_path = os.path.join("results", "benchmark", "scalability_comparison.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n✓ Wrote benchmark results to {out_path}")

    # Print a quick comparison if both ran
    if len(results) == 2:
        spark_time = next(r["seconds"] for r in results if r["path"] == "spark")
        pandas_time = next(r["seconds"] for r in results if r["path"] == "pandas")
        speedup = pandas_time / spark_time if spark_time > 0 else float("inf")
        print(f"Spark time:  {spark_time:.2f}s")
        print(f"Pandas time: {pandas_time:.2f}s")
        print(f"Speed-up (Spark vs pandas): {speedup:.2f}x")
        
        # Resource comparison summary (if available)
        spark_res = next(r.get("resources", {}) for r in results if r["path"] == "spark")
        pandas_res = next(r.get("resources", {}) for r in results if r["path"] == "pandas")
        if spark_res and pandas_res:
            print("\nResource usage (avg CPU / max RAM MB):")
            print(f"  Spark:  {spark_res.get('cpu_avg', 0):.1f}% / {spark_res.get('mem_max_mb', 0):.1f} MB")
            print(f"  Pandas: {pandas_res.get('cpu_avg', 0):.1f}% / {pandas_res.get('mem_max_mb', 0):.1f} MB")

    return results


def main():
    parser = argparse.ArgumentParser(description="Benchmark Spark vs single-node sequence building")
    parser.add_argument("--spark", action="store_true", help="Run Spark benchmark only")
    parser.add_argument("--pandas", action="store_true", help="Run pandas benchmark only")
    args = parser.parse_args()

    run_spark = args.spark or not (args.spark or args.pandas)
    run_pandas = args.pandas or not (args.spark or args.pandas)

    run_benchmark(run_spark=run_spark, run_pandas=run_pandas)


if __name__ == "__main__":
    main()
