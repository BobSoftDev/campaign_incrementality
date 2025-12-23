from __future__ import annotations

from pathlib import Path
import importlib
import sys


def _project_root_from_this_file(this_file: Path) -> Path:
    # scripts/run_all.py -> project root is parent of "scripts"
    return this_file.resolve().parents[1]


def main() -> None:
    project_root = _project_root_from_this_file(Path(__file__))

    # Ensure imports work regardless of where you run the command from
    sys.path.insert(0, str(project_root))

    gen_mod = importlib.import_module("scripts.00_generate_data")
    out_mod = importlib.import_module("scripts.01_prepare_outcomes")
    kpi_mod = importlib.import_module("scripts.02_compute_kpis")

    gen_mod.main()
    out_mod.main()
    kpi_mod.main()

    print("\n✅ Pipeline complete.")
    print("Next:")
    print("  streamlit run app/app.py")


if __name__ == "__main__":
    main()
