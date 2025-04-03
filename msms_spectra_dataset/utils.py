from typing import Dict, Any
import numpy as np
from spectrum_utils.spectrum import MsmsSpectrum
from pyteomics.auxiliary import PyteomicsError


def parse_spectrum(parsed: Dict[str, Any]) -> MsmsSpectrum:
    """
    Parse a spectrum dictionary and create an MsmsSpectrum object.

    Args:
        parsed (Dict[str, Any]): Parsed spectrum data from pyteomics.

    Returns:
        MsmsSpectrum: The created spectrum object.
    """
    params = parsed.get("params", {})
    title = params.get("title", "")
    precursor_mz = params.get("pepmass", [0.0])[0]
    pepmass = params.get("pepmass", [None, None])
    precursor_intensity = pepmass[1] if len(pepmass) > 1 else None
    mz_array = np.array(parsed.get("m/z array", []))
    intensity_array = np.array(parsed.get("intensity array", []))
    retention_time = params.get("rtinseconds", float("nan"))

    # Process and convert charge to int8 if available; default to 0
    try:
        charge = params.get("charge", None)
        if isinstance(charge, str) and charge.endswith('+'):
            charge = int(charge[:-1])
        elif charge is None:
            charge = 0
        charge = np.int8(charge)
    except (ValueError, PyteomicsError):
        charge = np.int8(0)

    spectrum = MsmsSpectrum(
        identifier=title,
        precursor_mz=precursor_mz,
        precursor_charge=charge,
        mz=mz_array,
        intensity=intensity_array,
        retention_time=retention_time,
    )
    spectrum.precursor_intensity = precursor_intensity
    return spectrum
