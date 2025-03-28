from typing import List, Callable, Union
import io  # Add this import

import numpy as np
from pyteomics import mgf
from pyteomics.auxiliary import PyteomicsError
from spectrum_utils.spectrum import MsmsSpectrum


class MGFSpectraDataset:
    def __init__(self, mgf_files: List[str]):
        """
        Create a dataset from a list of MGF files.
        Each spectrum is parsed into a MsmsSpectrum object.
        """
        self.mgf_files = mgf_files
        self.spectra: List[MsmsSpectrum] = []
        self._load_spectra()

    def _sanitize_mgf(self, file):
        """
        Preprocess the MGF file content to sanitize invalid CHARGE values.
        """
        sanitized_lines = []
        with open(file, 'r') as f:
            for line in f:
                if line.startswith("CHARGE="):
                    charge = line.split("=", 1)[1].strip()
                    if not (charge.endswith(("+", "-")) and charge[:-1].isdigit()):
                        line = "CHARGE=0\n"  # Replace invalid CHARGE with a default value
                sanitized_lines.append(line)
        
        # Use sanitized content with pyteomics via StringIO
        sanitized_content = io.StringIO("".join(sanitized_lines))
        return mgf.MGF(sanitized_content, convert_arrays=0)

    def _load_spectra(self):
        """
        Parse each provided MGF file and store all spectra in a list.
        
        The parameters are normalized to lowercase to handle keys in any case.
        The precursor charge is explicitly converted to np.int8 if possible.
        If missing or not convertible, defaults to 0.
        """
        for file in self.mgf_files:
            for spec in self._sanitize_mgf(file):
                params = spec.get("params", {})
                title = params.get("title", "")
                if "pepmass" in params:
                    precursor_mz = params["pepmass"][0]
                else:
                    precursor_mz = 0.0

                mz_array = np.array(spec.get("m/z array", []))
                intensity_array = np.array(spec.get("intensity array", []))
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
                    # Handle invalid charge formats
                    charge = np.int8(0)

                self.spectra.append(
                    MsmsSpectrum(
                        identifier=title,
                        precursor_mz=precursor_mz,
                        precursor_charge=charge,
                        mz=mz_array,
                        intensity=intensity_array,
                        retention_time=retention_time,
                    )
                )

    def __getitem__(self, idx: Union[int, slice]) -> Union[MsmsSpectrum, List[MsmsSpectrum]]:
        return self.spectra[idx]

    def __len__(self):
        return len(self.spectra)

    def query(self, filter_func: Callable[[MsmsSpectrum], bool]) -> List[MsmsSpectrum]:
        """
        Return a list of spectra matching a user-defined filter function.
        The filter function receives an MsmsSpectrum and should return True when it matches.
        """
        return [spec for spec in self.spectra if filter_func(spec)]