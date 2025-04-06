import time
import sys
import os
import numpy as np
from memory_profiler import memory_usage
from tabulate import tabulate

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from msms_spectra_dataset.in_memory_dataset import InMemoryMGFSpectraDataset
from msms_spectra_dataset.on_demand_dataset import OnDemandMGFSpectraDataset
from msms_spectra_dataset.duckdb_dataset import DuckDBSpectraDataset
from msms_spectra_dataset.duckdb_hdf5_dataset import DuckDBHDF5SpectraDataset

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

def benchmark_duckdb(mgf_files, duckdb_file, batch_size, iterations):
    """
    Benchmark the DuckDB dataset.
    """
    # Measure initial loading time (when DuckDB file does not exist)
    if os.path.exists(duckdb_file):
        os.remove(duckdb_file)  # Ensure the DuckDB file is removed for the first load
    start = time.time()
    with DuckDBSpectraDataset(duckdb_file, mgf_files, sequential_mode=False) as ds:
        initial_loading_time = time.time() - start
    # Measure subsequent loading time (when DuckDB file already exists)
    start = time.time()
    with DuckDBSpectraDataset(duckdb_file, mgf_files, sequential_mode=False) as ds:
        subsequent_loading_time = time.time() - start

        # Measure DuckDB file size for memory usage
        duckdb_file_size = os.path.getsize(duckdb_file) / (1024 * 1024)  # Convert bytes to MiB
        memory_per_million = (duckdb_file_size / len(ds)) * 1_000_000 if len(ds) > 0 else 0

        # Benchmark random access with batches
        total_time_random = 0.0
        for _ in range(iterations):
            start = time.time()
            indices = np.random.choice(len(ds), size=len(ds), replace=False)
            for idx in range(0, len(indices), batch_size):
                _ = ds[list(indices[idx:idx + batch_size])]
            total_time_random += time.time() - start
        random_access_time = total_time_random / iterations

        # Benchmark sequential access with batches
        ds.sequential_mode = True
        total_time_sequential = 0.0
        for _ in range(iterations):
            start = time.time()
            for idx in range(0, len(ds), batch_size):
                _ = ds[idx:idx + batch_size]
            total_time_sequential += time.time() - start
        sequential_access_time = total_time_sequential / iterations

        # Query benchmarks
        query_time_mz = benchmark_querying(ds, lambda d: d.query("precursor_mz > 500"), iterations)
        query_time_charge = benchmark_querying(ds, lambda d: d.query("precursor_charge = 2"), iterations)

        return {
            "Initial Loading Time (s)": f"{initial_loading_time:.3f}",
            "Subsequent Loading Time (s)": f"{subsequent_loading_time:.3f}",
            "Loading Memory (MiB)": f"{duckdb_file_size:.2f}",
            "Memory per 1M Spectra (MiB)": f"{memory_per_million:.2f}",
            "Query Time (m/z > 500) (s)": f"{query_time_mz:.3f}",
            "Query Time (charge == 2) (s)": f"{query_time_charge:.3f}",
            "Batch Time (Random) per 1M Spectra (s)": f"{(random_access_time / len(ds)) * 1_000_000:.3f}",
            "Batch Time (Sequential) per 1M Spectra (s)": f"{(sequential_access_time / len(ds)) * 1_000_000:.3f}",
        }
    
def benchmark_duckdb_hdf5(mgf_files, duckdb_file, hdf5_file, batch_size, iterations):
    """
    Benchmark the DuckDBHDF5 dataset.
    """
    # Measure initial loading time (when DuckDB and HDF5 files do not exist)
    if os.path.exists(duckdb_file):
        os.remove(duckdb_file)  # Ensure the DuckDB file is removed for the first load
    if os.path.exists(hdf5_file):
        os.remove(hdf5_file)
    start = time.time()
    with DuckDBHDF5SpectraDataset(duckdb_file, hdf5_file, mgf_files, sequential_mode=False) as ds:
        initial_loading_time = time.time() - start

    # Measure subsequent loading time (when DuckDB and HDF5 files already exist)
    start = time.time()
    with DuckDBHDF5SpectraDataset(duckdb_file, hdf5_file, mgf_files, sequential_mode=False) as ds:
        subsequent_loading_time = time.time() - start

        # Measure combined file size for memory usage
        duckdb_file_size = os.path.getsize(duckdb_file) / (1024 * 1024)  # Convert bytes to MiB
        hdf5_file_size = os.path.getsize(hdf5_file) / (1024 * 1024)  # Convert bytes to MiB
        total_file_size = duckdb_file_size + hdf5_file_size
        memory_per_million = (total_file_size / len(ds)) * 1_000_000 if len(ds) > 0 else 0

        # Benchmark random access with batches
        total_time_random = 0.0
        for _ in range(iterations):
            start = time.time()
            indices = np.random.choice(len(ds), size=len(ds), replace=False)
            for idx in range(0, len(indices), batch_size):
                _ = ds[list(indices[idx:idx + batch_size])]
            total_time_random += time.time() - start
        random_access_time = total_time_random / iterations

        # Benchmark sequential access with batches
        ds.sequential_mode = True
        total_time_sequential = 0.0
        for _ in range(iterations):
            start = time.time()
            for idx in range(0, len(ds), batch_size):
                _ = ds[idx:idx + batch_size]
            total_time_sequential += time.time() - start
        sequential_access_time = total_time_sequential / iterations

        # Query benchmarks
        query_time_mz = benchmark_querying(ds, lambda d: d.query("precursor_mz > 500"), iterations)
        query_time_charge = benchmark_querying(ds, lambda d: d.query("precursor_charge = 2"), iterations)

        return {
            "Initial Loading Time (s)": f"{initial_loading_time:.3f}",
            "Subsequent Loading Time (s)": f"{subsequent_loading_time:.3f}",
            "Loading Memory (MiB)": f"{total_file_size:.2f}",
            "Memory per 1M Spectra (MiB)": f"{memory_per_million:.2f}",
            "Query Time (m/z > 500) (s)": f"{query_time_mz:.3f}",
            "Query Time (charge == 2) (s)": f"{query_time_charge:.3f}",
            "Batch Time (Random) per 1M Spectra (s)": f"{(random_access_time / len(ds)) * 1_000_000:.3f}",
            "Batch Time (Sequential) per 1M Spectra (s)": f"{(sequential_access_time / len(ds)) * 1_000_000:.3f}",
        }

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
    print("\nInMemoryMGFSpectraDataset benchmark Summary:")
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

    # Benchmark DuckDB
    duckdb_file = "spectra.duckdb"
    duckdb_results = benchmark_duckdb(
        mgf_files, duckdb_file, args.batch_size, args.iterations,
    )

    # Print summary table for DuckDB
    print("\nDuckDB Benchmark Summary:")
    print(tabulate(duckdb_results.items(), headers=["Metric", "Value"], tablefmt="grid"))

    # Benchmark HDF5DuckDB
    hdf5_file = "spectra.hdf5"
    duckdb_file = "spectra.duckdb"
    hdf5_duckdb_results = benchmark_duckdb_hdf5(
        mgf_files, duckdb_file, hdf5_file, args.batch_size, args.iterations,
    )
    # Print summary table for HDF5DuckDB
    print("\nHDF5DuckDB Benchmark Summary:")
    print(tabulate(hdf5_duckdb_results.items(), headers=["Metric", "Value"], tablefmt="grid"))
