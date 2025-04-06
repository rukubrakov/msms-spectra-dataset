from typing import List, Callable, Union
import io

from pyteomics import mgf
from spectrum_utils.spectrum import MsmsSpectrum
from msms_spectra_dataset.utils import parse_spectrum  # Import the utility function


class InMemoryMGFSpectraDataset:
    def __init__(self, mgf_files: List[str]):
        """
        Create a dataset from a list of MGF files.
        Each spectrum is parsed into a MsmsSpectrum object and stored in memory.
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
        
        sanitized_content = io.StringIO("".join(sanitized_lines))
        return mgf.MGF(sanitized_content, convert_arrays=0)

    def _load_spectra(self):
        """
        Parse each provided MGF file and store all spectra in a list.
        """
        for file in self.mgf_files:
            for spec in self._sanitize_mgf(file):
                self.spectra.append(parse_spectrum(spec))

    def __getitem__(self, idx: Union[int, slice]) -> Union[MsmsSpectrum, List[MsmsSpectrum]]:
        return self.spectra[idx]

    def __len__(self):
        return len(self.spectra)

    def query(self, filter_func: Callable[[MsmsSpectrum], bool]) -> List[MsmsSpectrum]:
        """
        Return a list of spectra matching a user-defined filter function.
        """
        return [spec for spec in self.spectra if filter_func(spec)]
