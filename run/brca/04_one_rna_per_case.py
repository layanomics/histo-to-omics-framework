from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

try:
    import yaml
except ImportError as e:
    raise SystemExit("Missing dependency: PyYAML. Install with: pip install pyyaml") from e


def _load_cfg(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _run(cmd: list[str], log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("w", encoding="utf-8") as lf:
        lf.write("COMMAND:\n" + " ".join(cmd) + "\n\n")
        lf.flush()
        subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT, check=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="BRCA Phase-1: select exactly one RNA-seq file per paired case.")
    ap.add_argument("--config", default="configs/brca_phase1.yaml")
    args = ap.parse_args()

    cfg = _load_cfg(Path(args.config))
    meta_dir = Path(cfg["paths"]["meta_dir"])
    processed_dir = Path(cfg["paths"]["processed_dir"])
    out_logs = Path(cfg["paths"]["out_logs"])
    out_tables = Path(cfg["paths"]["out_tables"])

    out_logs.mkdir(parents=True, exist_ok=True)
    out_tables.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable, "scripts/shared/build_one_rnaseq_per_case_manifest.py",
        "--paired_csv",        str(processed_dir / "brca_paired_cohort.csv"),
        "--rnaseq_meta_tsv",   str(meta_dir / "brca_rnaseq_metadata.tsv"),
        "--out_manifest_tsv",  str(meta_dir / "brca_rnaseq_manifest_one_per_case.tsv"),
        "--out_selection_csv", str(out_tables / "rnaseq_one_per_case_selection.csv"),
    ]

    log = out_logs / f"04_one_rna_per_case_{_ts()}.log"
    _run(cmd, log)

    print(f"[OK] Built one-RNA-per-case manifest + selection table. Log: {log}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
