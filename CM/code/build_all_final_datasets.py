"""
Build the three final long-format datasets and then combine them into one deduplicated table.

Execution order
---------------
1. Extract bank-level people from Wikipedia bank pages.
2. Extract people from Wikipedia central-banker categories.
3. Process the KOF workbook into the shared final schema.
4. Append the three final datasets and remove duplicates.

Outputs
-------
Final datasets in `CM/data`:
- `central_bank_people_from_banks_long.csv`
- `central_bank_people_from_categories_long.csv`
- `kof_governors_with_sources.csv`
- `central_bank_people_combined_long.csv`
"""

from pathlib import Path
import subprocess
import sys


BASE_DIR = Path(__file__).resolve().parents[1]
CODE_DIR = BASE_DIR / "code"

PIPELINE_STEPS = [
    ("Wikipedia bank pages", CODE_DIR / "extract_central_banks_from_wikipedia.py"),
    ("Wikipedia categories", CODE_DIR / "extract_central_bankers_from_categories.py"),
    ("KOF workbook", CODE_DIR / "process_kof_governors_with_sources.py"),
    ("Append and deduplicate", CODE_DIR / "combine_final_long_datasets.py"),
]


def run_step(label: str, script_path: Path) -> None:
    print(f"\nRunning: {label}")
    print(f"Script : {script_path}")
    subprocess.run([sys.executable, str(script_path)], check=True)


def main() -> None:
    for label, script_path in PIPELINE_STEPS:
        if not script_path.exists():
            raise FileNotFoundError(f"No se encontró el script: {script_path}")
        run_step(label, script_path)

    print("\nPipeline completed successfully.")
    print(f"Final outputs saved in: {BASE_DIR / 'data'}")


if __name__ == "__main__":
    main()
