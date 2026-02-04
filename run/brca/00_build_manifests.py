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
        return yaml.safe_load(f) or {}


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _run(cmd: list[str], log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("w", encoding="utf-8") as lf:
        lf.write("COMMAND:\n" + " ".join(cmd) + "\n\n")
        lf.flush()
        subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT, check=True)


def _get_project_id(cfg: dict) -> str:
    gdcq = cfg.get("gdc_query") or {}
    if isinstance(gdcq, dict) and gdcq.get("project_id"):
        return str(gdcq["project_id"])

    proj = cfg.get("project")
    if isinstance(proj, str):
        return proj
    if isinstance(proj, dict):
        return str(proj.get("project_id") or proj.get("name") or "TCGA-BRCA")

    return "TCGA-BRCA"


def main() -> int:
    ap = argparse.ArgumentParser(description="BRCA Phase-1: build GDC manifests + metadata (RNA-seq + WSI).")
    ap.add_argument("--config", default="configs/brca_phase1.yaml")
    args = ap.parse_args()

    cfg = _load_cfg(Path(args.config))
    project_id = _get_project_id(cfg)

    meta_dir = Path(cfg["paths"]["meta_dir"])
    out_logs = Path(cfg["paths"]["out_logs"])

    meta_dir.mkdir(parents=True, exist_ok=True)
    out_logs.mkdir(parents=True, exist_ok=True)

    log = out_logs / f"00_build_manifests_{_ts()}.log"

    cmd_rna = [
        sys.executable, "scripts/gdc/gdc_build_manifests.py",
        "--project", project_id,
        "--data_kind", "rnaseq",
        "--n", "all",
        "--out_manifest", str(meta_dir / "brca_rnaseq_manifest.tsv"),
        "--out_meta",     str(meta_dir / "brca_rnaseq_metadata.tsv"),
    ]

    cmd_wsi = [
        sys.executable, "scripts/gdc/gdc_build_manifests.py",
        "--project", project_id,
        "--data_kind", "wsi",
        "--n", "all",
        "--out_manifest", str(meta_dir / "brca_wsi_manifest.tsv"),
        "--out_meta",     str(meta_dir / "brca_wsi_metadata.tsv"),
    ]

    # Run both; append into one log
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("w", encoding="utf-8") as lf:
        for cmd in (cmd_rna, cmd_wsi):
            lf.write("COMMAND:\n" + " ".join(cmd) + "\n\n")
            lf.flush()
            subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT, check=True)
            lf.write("\n" + ("-" * 80) + "\n\n")
            lf.flush()

    print(f"[OK] Built manifests + metadata. Log: {log}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
