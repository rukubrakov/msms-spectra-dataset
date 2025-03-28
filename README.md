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

## Local Benchmarking

You can also run the benchmark script locally. For example:

```bash
poetry run python benchmarks/benchmark.py path/to/your_sample.mgf
```

This will run the benchmark and print the loading time for the specified MGF file(s).