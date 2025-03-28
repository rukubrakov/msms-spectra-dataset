import time
import sys
import os

# Ensure the project root is in the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from msms_spectra_dataset.dataset import MGFSpectraDataset

def benchmark_loading(mgf_files, iterations=3):
    total_time = 0.0
    for i in range(iterations):
        start = time.time()
        ds = MGFSpectraDataset(mgf_files)
        elapsed = time.time() - start
        print(f"Iteration {i+1}: Loaded {len(ds)} spectra in {elapsed:.3f} seconds.")
        total_time += elapsed
    print(f"Average loading time over {iterations} iterations: {total_time/iterations:.3f} seconds.")

if __name__ == "__main__":
    mgf_files = sys.argv[1:]
    if not mgf_files:
        print("Usage: python benchmark.py <mgf_file1> <mgf_file2> ...")
    else:
        benchmark_loading(mgf_files)