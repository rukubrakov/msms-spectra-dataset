# msms-spectra-dataset

A dataset tool to load and query MS/MS spectra from MGF files.

## Setup

This project uses [Poetry](https://python-poetry.org/) for dependency management and packaging.

### Using Conda and Poetry

1. **Create and activate a Conda environment:**

   ```bash
   conda create -n msms-spectra-dataset python=3.11 -y
   conda init zsh   # if using zsh; otherwise, init for your shell
   # Restart your terminal, then activate:
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

   This will create the virtual environment (if not already created) and install all dependencies.

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
│       └── ci.yml         # GitHub Actions workflow for tests
├── benchmarks/
│   └── benchmark.py       # Benchmarking scripts
├── msms_spectra_dataset/  # Your package folder containing source code
│   └── dataset.py         # Dataset implementation
├── tests/
│   └── test_dataset.py    # Unit tests for your package
├── .gitignore
├── poetry.lock
├── pyproject.toml         # Project metadata and dependency management info
└── README.md
```

## Dataset Types

### InMemoryMGFSpectraDataset
- **Description**: Loads all spectra into memory as `MsmsSpectrum` objects.
- **Use Case**: Suitable for smaller datasets that fit entirely in memory.
- **Advantages**:
  - Fast querying and batch operations.
  - No file I/O after initial loading.
- **Limitations**:
  - Memory-intensive for large datasets.

### OnDemandMGFSpectraDataset
- **Description**: Stores file paths and byte offsets for spectra, loading them on demand.
- **Use Case**: Ideal for large datasets that cannot fit in memory.
- **Advantages**:
  - Low memory usage.
  - Can handle very large datasets.
- **Caching Mechanism**: Keeps the last accessed MGF file open to reduce file I/O overhead.
- **Limitations**:
  - Slower querying and batch operations due to file I/O.
  - Random access is less efficient compared to sequential access.

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

### Example Output

The benchmark results will be summarized in a table format. Example:

```
Benchmark Summary:
+---------------------------+---------+
| Metric                    | Value   |
+---------------------------+---------+
| Loading Time (s)          | 0.984   |
| Loading Memory (MiB)      | 453.92  |
| Memory per Element (MiB)  | 0.025123|
| Max Spectra in 16 GiB     | 655360  |
| Query Time (m/z > 500) (s)| 0.123   |
| Query Time (charge == 2) (s)| 0.098  |
| Batch Time (Sequential) (s)| 0.456  |
| Batch Time (Random) (s)   | 0.789   |
+---------------------------+---------+
```
