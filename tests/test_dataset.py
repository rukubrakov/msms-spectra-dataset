from msms_spectra_dataset.dataset import MGFSpectraDataset

def test_empty_dataset():
    ds = MGFSpectraDataset([])
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
    ds = MGFSpectraDataset([str(mgf_file)])
    assert len(ds) == 1
    spec = ds[0]
    assert spec.identifier == "TestSpectrum"
    assert spec.precursor_mz == 500.25

def test_missing_parameters(tmp_path):
    mgf_content = """BEGIN IONS
TITLE=MissingParams
123.45 678.90
END IONS
"""
    mgf_file = tmp_path / "missing_params.mgf"
    mgf_file.write_text(mgf_content)
    ds = MGFSpectraDataset([str(mgf_file)])
    assert len(ds) == 1
    spec = ds[0]
    assert spec.identifier == "MissingParams"
    assert spec.precursor_mz == 0.0
    assert spec.precursor_charge == 0

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
    ds = MGFSpectraDataset([str(mgf_file)])
    assert len(ds) == 2
    assert ds[0].identifier == "Spectrum1"
    assert ds[1].identifier == "Spectrum2"

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
    ds = MGFSpectraDataset([str(mgf_file)])
    assert len(ds) == 1
    spec = ds[0]
    assert spec.identifier == "InvalidCharge"
    assert spec.precursor_charge == 0