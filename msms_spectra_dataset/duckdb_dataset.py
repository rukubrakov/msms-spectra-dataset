import duckdb
import pandas as pd  # Add pandas import
from typing import List, Union
from msms_spectra_dataset.in_memory_dataset import InMemoryMGFSpectraDataset


class DuckDBSpectraDataset:
    def __init__(self, duckdb_file: str, mgf_files: List[str] = None, sequential_mode: bool = False, chunk_size: int = 100000):
        """
        Initialize the dataset with DuckDB.
        Optionally load spectra from MGF files.

        Args:
            duckdb_file (str): Path to the DuckDB file for metadata and spectral data.
            mgf_files (List[str], optional): List of MGF files to load spectra from.
            sequential_mode (bool): If True, enable sequential mode with chunked preloading.
            chunk_size (int): Number of spectra to preload in sequential mode.
        """
        self.duckdb_file = duckdb_file
        self.conn = duckdb.connect(duckdb_file)
        self.sequential_mode = sequential_mode
        self.chunk_size = chunk_size
        self.current_chunk = []
        self.current_chunk_start = 0

        # Create a single table for spectra with mz and intensity as arrays
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS spectra (
                id TEXT,
                precursor_mz REAL,
                precursor_charge INTEGER,
                retention_time REAL,
                mz REAL[],  -- mz values as an array
                intensity REAL[]  -- intensity values as an array
            )
            """
        )

        if mgf_files:
            self._load_spectra_from_mgf(mgf_files)

        if self.sequential_mode:
            self._load_chunk(0)

    def _load_spectra_from_mgf(self, mgf_files: List[str]):
        """
        Load spectra from MGF files into the dataset in batches.

        Args:
            mgf_files (List[str]): List of MGF files to load spectra from.
        """
        if self.conn.execute("SELECT COUNT(*) FROM spectra").fetchone()[0] > 0:
            return

        spectra_to_insert = []
        for mgf_file in mgf_files:
            for spec in InMemoryMGFSpectraDataset([mgf_file]):
                spectra_to_insert.append((
                    spec.identifier,
                    spec.precursor_mz,
                    spec.precursor_charge,
                    spec.retention_time,
                    spec.mz,
                    spec.intensity,
                ))

        df = pd.DataFrame(spectra_to_insert, columns=[
            "id", "precursor_mz", "precursor_charge", "retention_time", "mz", "intensity"
        ])
        self.conn.register("temp_df", df)

        self.conn.execute("""
            INSERT INTO spectra (id, precursor_mz, precursor_charge, retention_time, mz, intensity)
            SELECT id, precursor_mz, precursor_charge, retention_time, mz, intensity FROM temp_df
        """)

    def get_spectrum_batch_by_rowids(self, rowids: List[int]) -> List[dict]:
        """
        Retrieve multiple spectra by their row IDs in a single batch.

        Args:
            rowids (List[int]): List of row IDs to retrieve.

        Returns:
            List[dict]: List of retrieved spectra as dictionaries.
        """
        if not rowids:
            return []

        rowids = sorted(rowids)
        # Fetch spectra with mz and intensity arrays
        query = f"""
            SELECT 
                rowid,
                id,
                precursor_mz,
                precursor_charge,
                retention_time,
                mz,
                intensity
            FROM spectra
            WHERE rowid IN ({','.join(map(str, rowids))})
            ORDER BY rowid
        """
        rows = self.conn.execute(query).fetchall()

        return [
            {
                "id": row[1],
                "precursor_mz": row[2],
                "precursor_charge": row[3],
                "retention_time": row[4],
                "mz": row[5] or [],
                "intensity": row[6] or [],
            }
            for row in rows
        ]

    def query(self, filter_query: str) -> List[dict]:
        """
        Query spectra using a DuckDB SQL query.

        Args:
            filter_query (str): SQL WHERE clause to filter spectra.

        Returns:
            List[dict]: List of spectra matching the query.
        """
        results = self.conn.execute(
            f"SELECT rowid FROM spectra WHERE {filter_query}"
        ).fetchall()

        rowids = [row[0] for row in results]
        return self.get_spectrum_batch_by_rowids(rowids)


    def _load_chunk(self, start_rowid: int):
        """
        Load a chunk of spectra into memory starting from the given row ID.

        Args:
            start_rowid (int): The starting row ID for the chunk.
        """
        query = f"""
            SELECT rowid, id, precursor_mz, precursor_charge, retention_time, mz, intensity
            FROM spectra
            WHERE rowid BETWEEN {start_rowid} AND {start_rowid + self.chunk_size - 1}
        """
        rows = self.conn.execute(query).fetchall()

        # Convert rows to dictionaries
        self.current_chunk = [
            {
                "id": row[1],
                "precursor_mz": row[2],
                "precursor_charge": row[3],
                "retention_time": row[4],
                "mz": row[5],
                "intensity": row[6],
            }
            for row in rows
        ]
        self.current_chunk_start = start_rowid

    def _get_from_chunk(self, idx: int) -> dict:
        """
        Retrieve a spectrum from the current chunk.

        Args:
            idx (int): The index of the spectrum to retrieve.

        Returns:
            dict: The retrieved spectrum as a dictionary.
        """
        chunk_start = (idx // self.chunk_size) * self.chunk_size

        # If the index is outside the current chunk, load the appropriate chunk
        if not self.current_chunk or idx < self.current_chunk_start or idx >= self.current_chunk_start + len(self.current_chunk):
            query = f"""
                SELECT 
                    rowid,
                    id,
                    precursor_mz,
                    precursor_charge,
                    retention_time,
                    mz,
                    intensity
                FROM spectra
                WHERE rowid BETWEEN {chunk_start} AND {chunk_start + self.chunk_size - 1}
                ORDER BY rowid
            """
            rows = self.conn.execute(query).fetchall()

            # Convert rows to dictionaries
            self.current_chunk = [
                {
                    "id": row[1],
                    "precursor_mz": row[2],
                    "precursor_charge": row[3],
                    "retention_time": row[4],
                    "mz": row[5] or [],
                    "intensity": row[6] or [],
                }
                for row in rows
            ]
            self.current_chunk_start = chunk_start

        # Adjust the index relative to the current chunk and return the spectrum
        relative_idx = idx - self.current_chunk_start
        return self.current_chunk[relative_idx]

    def __getitem__(self, idx: Union[int, slice, List[int]]) -> Union[dict, List[dict]]:
        """
        Retrieve spectra by index, slice, or list of indices.

        Args:
            idx (Union[int, slice, List[int]]): Index, slice, or list of indices of spectra to retrieve.

        Returns:
            Union[dict, List[dict]]: The retrieved spectrum or list of spectra.
        """
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

        return self.get_spectrum_batch_by_rowids(rowids) if isinstance(idx, (slice, list)) else self.get_spectrum_batch_by_rowids(rowids)[0]

    def __len__(self):
        """
        Get the number of spectra in the dataset.

        Returns:
            int: The number of spectra.
        """
        return self.conn.execute("SELECT COUNT(*) FROM spectra").fetchone()[0]

    def __enter__(self):
        """
        Enable the use of the dataset as a context manager.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Ensure the connections are closed when exiting the context.
        """
        self.close()

    def close(self):
        """
        Close the DuckDB connection.
        """
        if self.conn:
            self.conn.close()
