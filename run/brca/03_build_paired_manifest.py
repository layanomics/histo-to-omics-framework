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
    ap = argparse.ArgumentParser(description="BRCA Phase-1: build RNA manifest restricted to paired cases.")
    ap.add_argument("--config", default="configs/brca_phase1.yaml")
    args = ap.parse_args()

    cfg = _load_cfg(Path(args.config))
    meta_dir = Path(cfg["paths"]["meta_dir"])
    processed_dir = Path(cfg["paths"]["processed_dir"])
    out_logs = Path(cfg["paths"]["out_logs"])

    out_logs.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable, "scripts/shared/build_manifest_from_paired_cases.py",
        "--paired_csv",        str(processed_dir / "brca_paired_cohort.csv"),
        "--rnaseq_meta_tsv",   str(meta_dir / "brca_rnaseq_metadata.tsv"),
        "--out_manifest_tsv",  str(meta_dir / "brca_rnaseq_manifest_paired.tsv"),
        "--require_paired",
    ]

    log = out_logs / f"03_build_paired_manifest_{_ts()}.log"
    _run(cmd, log)

    print(f"[OK] Built paired RNA manifest. Log: {log}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
