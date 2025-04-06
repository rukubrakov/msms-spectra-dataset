import os
import pytest
from msms_spectra_dataset.duckdb_dataset import DuckDBSpectraDataset

DUCKDB_FILE = "test_spectra.duckdb"

def test_empty_dataset():
    if os.path.exists(DUCKDB_FILE):
        os.remove(DUCKDB_FILE)
    ds = DuckDBSpectraDataset(DUCKDB_FILE, [])
    assert len(ds) == 0

def test_load_single_spectrum(tmp_path):
    mgf_content = """BEGIN IONS
TITLE=TestSpectrum
PEPMASS=500.25 1000
CHARGE=2+
RTINSECONDS=100
123.45 678.90
END IONS
"""
    mgf_file = tmp_path / "test.mgf"
    mgf_file.write_text(mgf_content)
    if os.path.exists(DUCKDB_FILE):
        os.remove(DUCKDB_FILE)
    ds = DuckDBSpectraDataset(DUCKDB_FILE, [str(mgf_file)])
    assert len(ds) == 1
    spec = ds[0]
    assert spec["id"] == "TestSpectrum"
    assert spec["precursor_mz"] == pytest.approx(500.25, rel=1e-6)
    assert spec["precursor_charge"] == 2
    assert spec["retention_time"] == pytest.approx(100, rel=1e-6)
    assert spec["mz"][0] == pytest.approx(123.45, rel=1e-6)
    assert spec["intensity"][0] == pytest.approx(678.90, rel=1e-6)

def test_multiple_spectra(tmp_path):
    mgf_content = """BEGIN IONS
TITLE=Spectrum1
PEPMASS=400.5
CHARGE=3+
123.45 678.90
END IONS
BEGIN IONS
TITLE=Spectrum2
PEPMASS=600.75
CHARGE=2+
223.45 778.90
END IONS
"""
    mgf_file = tmp_path / "multiple_spectra.mgf"
    mgf_file.write_text(mgf_content)
    if os.path.exists(DUCKDB_FILE):
        os.remove(DUCKDB_FILE)
    ds = DuckDBSpectraDataset(DUCKDB_FILE, [str(mgf_file)])
    assert len(ds) == 2

    spec1 = ds[0]
    assert spec1["id"] == "Spectrum1"
    assert spec1["precursor_mz"] == pytest.approx(400.5, rel=1e-6)
    assert spec1["precursor_charge"] == 3
    assert spec1["mz"][0] == pytest.approx(123.45, rel=1e-6)
    assert spec1["intensity"][0] == pytest.approx(678.90, rel=1e-6)

    spec2 = ds[1]
    assert spec2["id"] == "Spectrum2"
    assert spec2["precursor_mz"] == pytest.approx(600.75, rel=1e-6)
    assert spec2["precursor_charge"] == 2
    assert spec2["mz"][0] == pytest.approx(223.45, rel=1e-6)
    assert spec2["intensity"][0] == pytest.approx(778.90, rel=1e-6)

def test_invalid_charge_format(tmp_path):
    mgf_content = """BEGIN IONS
TITLE=InvalidCharge
PEPMASS=500.25
CHARGE=invalid
123.45 678.90
END IONS
"""
    mgf_file = tmp_path / "invalid_charge.mgf"
    mgf_file.write_text(mgf_content)
    if os.path.exists(DUCKDB_FILE):
        os.remove(DUCKDB_FILE)
    ds = DuckDBSpectraDataset(DUCKDB_FILE, [str(mgf_file)])
    assert len(ds) == 1
    spec = ds[0]
    assert spec["id"] == "InvalidCharge"
    assert spec["precursor_charge"] == 0  # Default charge for invalid input
    assert spec["precursor_mz"] == pytest.approx(500.25, rel=1e-6)
    assert spec["mz"][0] == pytest.approx(123.45, rel=1e-6)
    assert spec["intensity"][0] == pytest.approx(678.90, rel=1e-6)

def test_query_spectra(tmp_path):
    mgf_content = """BEGIN IONS
TITLE=Spectrum1
PEPMASS=400.5
CHARGE=3+
123.45 678.90
END IONS
BEGIN IONS
TITLE=Spectrum2
PEPMASS=600.75
CHARGE=2+
223.45 778.90
END IONS
"""
    mgf_file = tmp_path / "query_spectra.mgf"
    mgf_file.write_text(mgf_content)
    if os.path.exists(DUCKDB_FILE):
        os.remove(DUCKDB_FILE)
    ds = DuckDBSpectraDataset(DUCKDB_FILE, [str(mgf_file)])

    # Query spectra with precursor_mz > 500
    filtered = ds.query("precursor_mz > 500")
    assert len(filtered) == 1
    assert filtered[0]["id"] == "Spectrum2"
    assert filtered[0]["precursor_mz"] == pytest.approx(600.75, rel=1e-6)


