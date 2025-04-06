import duckdb
import h5py
import numpy as np
import pandas as pd
from typing import List, Union
from msms_spectra_dataset.in_memory_dataset import InMemoryMGFSpectraDataset


class DuckDBHDF5SpectraDataset:
    def __init__(self, duckdb_file: str, hdf5_file: str, mgf_files: List[str] = None,
                 sequential_mode: bool = False, chunk_size: int = 100000):
        """
        Initialize the DuckDB and HDF5 dataset.
        Args:
            duckdb_file (str): Path to the DuckDB database file.
            hdf5_file (str): Path to the HDF5 file.
            mgf_files (List[str]): List of MGF files to load spectra from.
            sequential_mode (bool): If True, load spectra in chunks for sequential access.
            chunk_size (int): Size of each chunk to load in sequential mode.
        """
        self.duckdb_file = duckdb_file
        self.hdf5_file = hdf5_file
        self.conn = duckdb.connect(duckdb_file)
        self.sequential_mode = sequential_mode
        self.chunk_size = chunk_size
        self.current_chunk = []
        self.current_chunk_start = 0

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS spectra (
                id TEXT,
                precursor_mz REAL,
                precursor_charge INTEGER,
                retention_time REAL
            )
        """)
        if mgf_files:
            self._load_spectra_from_mgf(mgf_files)

        self.h5 = h5py.File(self.hdf5_file, "r")

        if self.sequential_mode:
            self._load_chunk(0)

    def _load_spectra_from_mgf(self, mgf_files: List[str]):
        if self.conn.execute("SELECT COUNT(*) FROM spectra").fetchone()[0] > 0:
            return

        spectra_to_insert = []
        spectra = []
        for mgf_file in mgf_files:
            for spec in InMemoryMGFSpectraDataset([mgf_file]):
                spectra_to_insert.append((
                    spec.identifier,
                    spec.precursor_mz,
                    spec.precursor_charge,
                    spec.retention_time
                ))
                spectra.append((spec.mz, spec.intensity))

        df = pd.DataFrame(spectra_to_insert, columns=[
            "id", "precursor_mz", "precursor_charge", "retention_time"
        ])
        self.conn.register("temp_df", df)
        self.conn.execute("""
            INSERT INTO spectra (id, precursor_mz, precursor_charge, retention_time)
            SELECT id, precursor_mz, precursor_charge, retention_time FROM temp_df
        """)

        with h5py.File(self.hdf5_file, "w") as f:
            dt = h5py.vlen_dtype(np.float32)
            spectra_ds = f.create_dataset("spectra", shape=(len(spectra), 2), dtype=dt)
            for i, (mz, intensity) in enumerate(spectra):
                spectra_ds[i, 0] = mz
                spectra_ds[i, 1] = intensity

    def get_spectrum_batch_by_rowids(self, rowids: List[int]) -> List[dict]:
        if not rowids:
            return []

        rowids = sorted(rowids)
        query = f"""
            SELECT id, precursor_mz, precursor_charge, retention_time
            FROM spectra
            WHERE rowid IN ({','.join(map(str, rowids))})
        """
        rows = self.conn.execute(query).fetchall()

        spectra_data = self.h5["spectra"][rowids]

        result = [
            {
                "id": row[0],
                "precursor_mz": row[1],
                "precursor_charge": row[2],
                "retention_time": row[3],
                "mz": spectra_data[i, 0],
                "intensity": spectra_data[i, 1]
            }
            for i, row in enumerate(rows)
        ]

        return result

    def query(self, filter_query: str) -> List[dict]:
        results = self.conn.execute(
            f"SELECT rowid FROM spectra WHERE {filter_query}"
        ).fetchall()
        rowids = [row[0] for row in results]
        return self.get_spectrum_batch_by_rowids(rowids)

    def _load_chunk(self, start_rowid: int):
        query = f"""
            SELECT rowid, id, precursor_mz, precursor_charge, retention_time
            FROM spectra
            WHERE rowid BETWEEN {start_rowid} AND {start_rowid + self.chunk_size - 1}
        """
        rows = self.conn.execute(query).fetchall()

        indices = [row[0] for row in rows]
        spectra_data = self.h5["spectra"][min(indices):max(indices) + 1]

        self.current_chunk = [
            {
                "id": row[1],
                "precursor_mz": row[2],
                "precursor_charge": row[3],
                "retention_time": row[4],
                "mz": spectra_data[i, 0],
                "intensity": spectra_data[i, 1]
            }
            for i, row in enumerate(rows)
        ]
        self.current_chunk_start = start_rowid

    def _get_from_chunk(self, idx: int) -> dict:
        chunk_start = (idx // self.chunk_size) * self.chunk_size
        if not self.current_chunk or idx < self.current_chunk_start or idx >= self.current_chunk_start + len(self.current_chunk):
            self._load_chunk(chunk_start)

        return self.current_chunk[idx - self.current_chunk_start]

    def __getitem__(self, idx: Union[int, slice, List[int]]) -> Union[dict, List[dict]]:
        if self.sequential_mode:
            if isinstance(idx, slice):
                start = idx.start or 0
                stop = min(idx.stop if idx.stop is not None else len(self), len(self))
                step = idx.step or 1
                return [self._get_from_chunk(i) for i in range(start, stop, step)]
            elif isinstance(idx, list):
                return [self._get_from_chunk(i) for i in idx]
            else:
                return self._get_from_chunk(idx)

        if isinstance(idx, slice):
            rowids = list(range(idx.start or 0, idx.stop or len(self), idx.step or 1))
        elif isinstance(idx, list):
            rowids = idx
        else:
            rowids = [idx]

        result = self.get_spectrum_batch_by_rowids(rowids)
        return result if isinstance(idx, (slice, list)) else result[0]

    def __len__(self):
        return self.conn.execute("SELECT COUNT(*) FROM spectra").fetchone()[0]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        if self.conn:
            self.conn.close()
        if self.h5:
            self.h5.close()
