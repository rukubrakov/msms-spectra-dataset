# msms-spectra-dataset

A dataset tool to load and query MS/MS spectra from MGF files.

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

## Dataset Types

### InMemoryMGFSpectraDataset
Loads all spectra into memory as `MsmsSpectrum` objects.

### OnDemandMGFSpectraDataset
Stores file paths and byte offsets for spectra, loading them on demand.

### DuckDBSpectraDataset
Uses DuckDB to store both metadata and spectra data (including `mz` and `intensity` arrays) and enables SQL-like querying.

### DuckDBHDF5SpectraDataset
Combines DuckDB for metadata storage and HDF5 for spectra storage, enabling efficient querying and random access.

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
