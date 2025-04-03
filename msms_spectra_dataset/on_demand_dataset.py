from typing import List, Union, Tuple
from pyteomics import mgf
from msms_spectra_dataset.utils import parse_spectrum
import io


class OnDemandMGFSpectraDataset:
    def __init__(self, mgf_files: List[str]):
        """
        Create a dataset from a list of MGF files.
        Instead of loading all spectra into memory, store file paths and line offsets.
        """
        self.mgf_files = mgf_files
        self.index: List[Tuple[str, int]] = []  # List of (file_path, line_offset)
        self._build_index()

    def _build_index(self):
        """
        Build an index of spectra by storing the file path and starting byte offset for each spectrum.
        """
        for file in self.mgf_files:
            with open(file, 'r') as f:
                content = f.read()  # Read the entire file into memory
                offset = 0
                while True:
                    begin_idx = content.find("BEGIN IONS", offset)
                    if begin_idx == -1:
                        break
                    self.index.append((file, begin_idx))
                    offset = begin_idx + len("BEGIN IONS")

    def _load_spectrum(self, file: str, offset: int):
        """
        Load a single spectrum from the specified file and byte offset.
        """
        with open(file, 'r') as f:
            f.seek(offset)
            lines = []
            for line in f:
                lines.append(line)
                if line.startswith("END IONS"):
                    break
        spectrum_data = "\n".join(lines)
        spectrum_stream = io.StringIO(spectrum_data)
        parsed = next(mgf.read(spectrum_stream, convert_arrays=1, use_index=False))
        return parse_spectrum(parsed)

    def __getitem__(self, idx: Union[int, slice]):
        if isinstance(idx, slice):
            return [self._load_spectrum(file, offset) for file, offset in self.index[idx]]
        file, offset = self.index[idx]
        return self._load_spectrum(file, offset)

    def __len__(self):
        return len(self.index)
