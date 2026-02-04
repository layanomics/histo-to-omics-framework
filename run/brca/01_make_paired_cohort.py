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
    ap = argparse.ArgumentParser(description="BRCA Phase-1: make paired cohort table (RNA ∩ WSI).")
    ap.add_argument("--config", default="configs/brca_phase1.yaml")
    args = ap.parse_args()

    cfg = _load_cfg(Path(args.config))
    meta_dir = Path(cfg["paths"]["meta_dir"])
    processed_dir = Path(cfg["paths"]["processed_dir"])
    out_logs = Path(cfg["paths"]["out_logs"])

    processed_dir.mkdir(parents=True, exist_ok=True)
    out_logs.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        "scripts/shared/make_paired_cohort.py",
        "--rna_meta",
        str(meta_dir / "brca_rnaseq_metadata.tsv"),
        "--wsi_meta",
        str(meta_dir / "brca_wsi_metadata.tsv"),
        "--out",
        str(processed_dir / "brca_paired_cohort.csv"),
    ]

    log = out_logs / f"01_make_paired_cohort_{_ts()}.log"
    _run(cmd, log)

    print(f"[OK] Built paired cohort CSV. Log: {log}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
