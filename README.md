# Benchmark Results and Implementation Analysis

This project is designed to efficiently load, query, and batch-read MS/MS spectra from MGF files for high-throughput machine learning applications. Below is an overview of the four dataset implementations and their performance characteristics.

## Dataset Implementations

### 1. **InMemoryMGFSpectraDataset**
- **Approach:** Loads all spectra into memory as `MsmsSpectrum` objects.
- **Performance:**
  - Extremely fast sequential and random batch reading.
  - Limited by memory: ~3 million spectra can be loaded in 16 GiB of RAM.
- **Use Case:** Best for small-to-medium datasets where low-latency access is critical and memory is sufficient.

### 2. **OnDemandMGFSpectraDataset**
- **Approach:** Naive solution storing only file paths and byte offsets, loading spectra from disk on demand.
- **Performance:**
  - Fast indexing/loading.
  - Slow random access due to disk reads for each access.
- **Use Case:** Baseline implementation for comparison purposes.

### 3. **DuckDBSpectraDataset**
- **Approach:** Uses DuckDB to store metadata and spectra arrays, enabling SQL-like querying.
- **Performance:**
  - Slower initial loading from MGF files; subsequent loads are almost instantaneous.
  - No RAM limits; supports any dataset size.
  - Disk usage: 2–3x less space per spectrum compared to MGF files.
  - Faster random and sequential batch reading compared to OnDemand.
- **Use Case:** Ideal for large datasets requiring efficient querying.

### 4. **DuckDBHDF5SpectraDataset**
- **Approach:** Combines DuckDB for metadata storage with HDF5 for spectra storage.
- **Performance:**
  - Slowest initial load due to HDF5 overhead; subsequent loads are instantaneous.
  - No RAM limits; supports any dataset size.
  - Disk usage: More than DuckDBSpectraDataset but less than MGF files.
  - Random access: 2x faster; sequential access: 5x faster compared to DuckDBSpectraDataset.
  - Query time slightly slower due to HDF5 overhead.
- **Use Case:** Best for high-throughput sequential access in machine learning training.

## Performance Summary

| Dataset          | Loading Time (per 1M spectra) | Memory Efficiency                     | Sequential Batch Reading (1M spectra) | Random Batch Reading (1M spectra) | Query Time (m/z > 500) | Query Time (charge == 2) |
|-------------------|-------------------------------|---------------------------------------|---------------------------------------|-----------------------------------|------------------------|--------------------------|
| **InMemory**      | ~38 s                        | Limited to ~3M spectra in 16 GiB RAM  | <0.01 s (extremely fast)              | <0.01 s (extremely fast)          | ~0.032 s               | ~0.028 s                |
| **On-Demand**     | ~0.5 s                       | Minimal footprint                     | -                                     | ~66 s                             | -                      | -                       |
| **DuckDB**        | ~50 s (initial), <0.01 s subsequent | Compact (250 MiB per 1M spectra)     | ~11.7 s                              | ~42 s                             | ~1.9 s                 | ~1.1 s                  |
| **DuckDBHDF5**    | ~100 s (initial)             | Moderate (367 MiB per 1M spectra)     | ~2.1 s                               | ~22 s                             | ~4.7 s                 | ~7.2 s                  |

> **Note:** Benchmarks are based on tests with ~1 million spectra on an **Apple M2 Pro** with **16 GiB memory**. While the InMemory approach offers ultrafast access, it is limited by available RAM. DuckDB and DuckDBHDF5 provide scalable solutions with significantly improved access times compared to OnDemand.

## Conclusion

- **InMemoryMGFSpectraDataset:** Best for small datasets that fit in memory and require low-latency processing.
- **OnDemandMGFSpectraDataset:** Baseline implementation for comparison.
- **DuckDBSpectraDataset:** Scalable for large datasets with efficient querying.
- **DuckDBHDF5SpectraDataset:** Excels in sequential batch processing and improved random access for machine learning.

## Further Steps

1. **Explore alternative integrations of DuckDB and HDF5:**  
   Consider storing spectra (m/z and intensities) in both DuckDB and HDF5. While this may increase storage requirements, it could allow queries to execute as quickly as in the DuckDB-only approach.

2. **Investigate multithreading and multiprocessing:**  
   Parallelization could significantly improve performance, especially for large datasets or computationally intensive queries.

3. **Enable querying based on spectra data:**  
   Current benchmarks focus on metadata queries. Adding support for queries like "find all spectra with peaks at m/z = 123.456" could expand the use cases.

4. **Benchmark in constrained environments:**  
   Conduct benchmarks in containerized environments with memory and resource limitations. Additionally, study the impact of dataset size on performance beyond theoretical analysis.

# Setup, testing, linting and bechmarking

## Setup

1. **Create and activate a Conda environment:**

   ```bash
   conda create -n msms-spectra-dataset python=3.11 -y
   conda activate msms-spectra-dataset
   ```

2. **Install Poetry (if not already installed):**

   ```bash
   pip install poetry
   ```

3. **Install project dependencies:**

   From the repository root, run:

   ```bash
   poetry install
   ```

   This will install all dependencies.

   If you also need development dependencies (e.g., for linting or testing), use:

   ```bash
   poetry install --with dev
   ```

## Running Tests Locally

To run the unit tests locally using Poetry, ensure the `PYTHONPATH` is set to the project root. Use the following command:

```bash
PYTHONPATH=. poetry run pytest --maxfail=1 --disable-warnings -q
```

## Running Ruff Locally

To check the code for linting issues using [Ruff](https://github.com/charliermarsh/ruff), run:

```bash
poetry run ruff check .
```

To automatically fix linting issues, run:

```bash
poetry run ruff check . --fix
```

## Folder Structure

```
msms-spectra-dataset/
├── .github/
│   └── workflows/
│       └── ci.yml
├── benchmarks/
│   └── benchmark.py
├── msms_spectra_dataset/
│   ├── in_memory_dataset.py
│   ├── on_demand_dataset.py
│   ├── duckdb_dataset.py
│   ├── duckdb_hdf5_dataset.py
│   └── __init__.py
├── tests/
│   ├── test_in_memory_dataset.py
│   ├── test_on_demand_dataset.py
│   ├── test_duckdb_dataset.py
│   └── test_duckdb_hdf5_dataset.py
├── .gitignore
├── poetry.lock
├── pyproject.toml
└── README.md
```

## Benchmarking

To evaluate the performance of the datasets, you can use the provided benchmarking script. It measures metrics such as loading time, memory usage, and batch reading performance.

### Running the Benchmark Script

Run the following command to execute the benchmark:

```bash
poetry run python benchmarks/benchmark.py path/to/your_sample.mgf --copies 10 --batch_size 512 --iterations 3 --max_memory 16
```

### Benchmark Options
- `--copies`: Number of virtual copies of the MGF file to simulate higher load (default: 10).
- `--batch_size`: Batch size for batch reading (default: 512).
- `--iterations`: Number of iterations for each benchmark (default: 3).
- `--max_memory`: Maximum CPU memory in GiB to calculate the maximum number of spectra that can fit in memory (default: 16 GiB).
