from msms_spectra_dataset.on_demand_dataset import OnDemandMGFSpectraDataset
import pytest

def test_empty_dataset():
    ds = OnDemandMGFSpectraDataset([])
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
    ds = OnDemandMGFSpectraDataset([str(mgf_file)])
    assert len(ds) == 1
    spec = ds[0]
    assert spec.identifier == "TestSpectrum"
    assert spec.precursor_mz == pytest.approx(500.25, rel=1e-6)
    assert spec.precursor_charge == 2
    assert spec.retention_time == pytest.approx(100, rel=1e-6)
    assert spec.mz[0] == pytest.approx(123.45, rel=1e-6)
    assert spec.intensity[0] == pytest.approx(678.90, rel=1e-6)

def test_missing_parameters(tmp_path):
    mgf_content = """BEGIN IONS
TITLE=MissingParams
123.45 678.90
END IONS
"""
    mgf_file = tmp_path / "missing_params.mgf"
    mgf_file.write_text(mgf_content)
    ds = OnDemandMGFSpectraDataset([str(mgf_file)])
    assert len(ds) == 1
    spec = ds[0]
    assert spec.identifier == "MissingParams"
    assert spec.precursor_mz == pytest.approx(0.0, rel=1e-6)
    assert spec.precursor_charge == 0
    assert spec.retention_time != spec.retention_time  # Check for NaN
    assert spec.mz[0] == pytest.approx(123.45, rel=1e-6)
    assert spec.intensity[0] == pytest.approx(678.90, rel=1e-6)

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
    ds = OnDemandMGFSpectraDataset([str(mgf_file)])
    assert len(ds) == 2

    spec1 = ds[0]
    assert spec1.identifier == "Spectrum1"
    assert spec1.precursor_mz == pytest.approx(400.5, rel=1e-6)
    assert spec1.precursor_charge == 3
    assert spec1.mz[0] == pytest.approx(123.45, rel=1e-6)
    assert spec1.intensity[0] == pytest.approx(678.90, rel=1e-6)

    spec2 = ds[1]
    assert spec2.identifier == "Spectrum2"
    assert spec2.precursor_mz == pytest.approx(600.75, rel=1e-6)
    assert spec2.precursor_charge == 2
    assert spec2.mz[0] == pytest.approx(223.45, rel=1e-6)
    assert spec2.intensity[0] == pytest.approx(778.90, rel=1e-6)

