import time
import sys
import os
import numpy as np
from memory_profiler import memory_usage
from tabulate import tabulate  # Add tabulate for table formatting

# Ensure the project root is in the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from msms_spectra_dataset.in_memory_dataset import InMemoryMGFSpectraDataset
from msms_spectra_dataset.on_demand_dataset import OnDemandMGFSpectraDataset

def measure_memory(func, *args, **kwargs):
    """
    Measure the peak memory usage during the execution of a function.
    """
    mem_usage = memory_usage((func, args, kwargs), max_usage=True)
    return mem_usage

def benchmark_loading(mgf_files, iterations=3):
    total_time = 0.0
    total_memory = 0.0
    for _ in range(iterations):
        start = time.time()
        mem_usage = measure_memory(InMemoryMGFSpectraDataset, mgf_files)
        elapsed = time.time() - start
        total_time += elapsed
        total_memory += mem_usage
    return total_time / iterations, total_memory / iterations

def benchmark_querying(ds, query_func, iterations=3):
    total_time = 0.0
    for _ in range(iterations):
        start = time.time()
        query_func(ds)
        elapsed = time.time() - start
        total_time += elapsed
    return total_time / iterations

def benchmark_batch_reading(ds, batch_size, iterations=3, random_access=False):
    total_time = 0.0
    for _ in range(iterations):
        start = time.time()
        if random_access:
            indices = np.random.choice(len(ds), size=len(ds), replace=False)
            for idx in range(0, len(indices), batch_size):
                _ = [ds[j] for j in indices[idx:idx + batch_size]]
        else:
            for idx in range(0, len(ds), batch_size):
                _ = ds[idx:idx + batch_size]
        elapsed = time.time() - start
        total_time += elapsed
    return total_time / iterations

def create_virtual_mgf_copies(mgf_file, copies):
    """
    Create a list of virtual MGF file paths by repeating the same file path.
    """
    return [mgf_file] * copies

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Benchmark MS/MS Spectra Dataset")
    parser.add_argument("mgf_file", help="Path to the MGF file")
    parser.add_argument("--copies", type=int, default=10, help="Number of virtual copies of the MGF file")
    parser.add_argument("--batch_size", type=int, default=512, help="Batch size for batch reading")
    parser.add_argument("--iterations", type=int, default=3, help="Number of iterations for each benchmark")
    parser.add_argument("--max_memory", type=float, default=16.0, help="Maximum CPU memory in GiB (default: 16 GiB)")
    args = parser.parse_args()

    # Simulate higher load by creating virtual copies of the MGF file
    mgf_files = create_virtual_mgf_copies(args.mgf_file, args.copies)
    print(f"Simulating {args.copies} virtual copies of the MGF file.")

    # Benchmark InMemoryMGFSpectraDataset
    ds = InMemoryMGFSpectraDataset(mgf_files)
    print(f"Loaded {len(ds)} spectra from {len(mgf_files)} virtual files using InMemoryMGFSpectraDataset.")

    # Run benchmarks
    loading_time, loading_memory = benchmark_loading(mgf_files, iterations=args.iterations)
    memory_per_million = (loading_memory / len(ds)) * 1_000_000 if len(ds) > 0 else 0

    # Calculate maximum number of spectra that can fit in memory
    max_spectra = int((args.max_memory * 1024) / (memory_per_million / 1_000_000)) if memory_per_million > 0 else 0

    query_time_mz = benchmark_querying(ds, lambda d: d.query(lambda s: s.precursor_mz > 500), iterations=args.iterations)
    query_time_charge = benchmark_querying(ds, lambda d: d.query(lambda s: s.precursor_charge == 2), iterations=args.iterations)

    batch_time_sequential = benchmark_batch_reading(ds, batch_size=args.batch_size, iterations=args.iterations, random_access=False)
    batch_time_random = benchmark_batch_reading(ds, batch_size=args.batch_size, iterations=args.iterations, random_access=True)

    # Print summary table
    table = [
        ["Loading Time (s)", f"{loading_time:.3f}"],
        ["Loading Memory (MiB)", f"{loading_memory:.2f}"],
        ["Memory per 1M Spectra (MiB)", f"{memory_per_million:.2f}"],
        [f"Max Spectra in {args.max_memory} GiB", f"{max_spectra}"],
        ["Query Time (m/z > 500) (s)", f"{query_time_mz:.3f}"],
        ["Query Time (charge == 2) (s)", f"{query_time_charge:.3f}"],
        ["Batch Time (Sequential) per 1M Spectra (s)", f"{(batch_time_sequential / len(ds)) * 1_000_000:.3f}"],
        ["Batch Time (Random) per 1M Spectra (s)", f"{(batch_time_random / len(ds)) * 1_000_000:.3f}"],
    ]
    print("\nBenchmark Summary:")
    print(tabulate(table, headers=["Metric", "Value"], tablefmt="grid"))

    # Benchmark OnDemandMGFSpectraDataset
    print("\nBenchmarking OnDemandMGFSpectraDataset:")
    start = time.time()
    naive_ds = OnDemandMGFSpectraDataset(mgf_files)
    loading_time_naive = time.time() - start
    print(f"Loaded {len(naive_ds)} spectra using OnDemandMGFSpectraDataset.")

    # Sequential batch reading
    batch_time_sequential_naive = benchmark_batch_reading(
        naive_ds, batch_size=args.batch_size, iterations=args.iterations, random_access=False
    )

    # Random batch reading (expected to be slow)
    batch_time_random_naive = benchmark_batch_reading(
        naive_ds, batch_size=args.batch_size, iterations=args.iterations, random_access=True
    )

    # Print summary table for OnDemandMGFSpectraDataset
    table_naive = [
        ["Loading Time (s)", f"{loading_time_naive:.3f}"],
        ["Batch Time (Random) per 1M Spectra (s)", f"{(batch_time_random_naive / len(naive_ds)) * 1_000_000:.3f}"],
    ]
    print("\nOnDemandMGFSpectraDataset Benchmark Summary:")
    print(tabulate(table_naive, headers=["Metric", "Value"], tablefmt="grid"))