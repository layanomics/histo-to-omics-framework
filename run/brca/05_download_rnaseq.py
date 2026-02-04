# run/brca/05_download_rnaseq.py
import argparse
import sys
from datetime import datetime
from pathlib import Path

import yaml
import pandas as pd
import subprocess


def _load_cfg(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _ts(cfg: dict) -> str:
    fmt = (cfg.get("logging", {}) or {}).get("timestamp_format", "%Y%m%d_%H%M%S")
    return datetime.now().strftime(fmt)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/brca_phase1.yaml")
    ap.add_argument("--dry_run", action="store_true")
    args = ap.parse_args()

    cfg = _load_cfg(Path(args.config))
    paths = cfg["paths"]
    download = cfg.get("download", {}) or {}

    manifest = Path(paths["meta_dir"]) / "brca_rnaseq_manifest_one_per_case.tsv"
    out_dir = Path(paths["raw_dir"]) / "rnaseq_star_counts"

    out_logs = Path(paths["out_logs"])
    out_logs.mkdir(parents=True, exist_ok=True)
    gdc_log_dir = out_logs / "gdc_download"
    gdc_log_dir.mkdir(parents=True, exist_ok=True)

    runner_log = out_logs / f"05_download_rnaseq_{_ts(cfg)}.log"

    gdc_client = download.get("gdc_client", "gdc-client")
    n_conn = int(download.get("threads", 8))
    verify_after = bool(download.get("verify_after", True))
    fail_on_verify = bool(download.get("fail_on_verify", True))
    token_file = download.get("token_file")  # optional

    if not manifest.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest}")

    df = pd.read_csv(manifest, sep="\t")
    total = int(df.shape[0])

    cmd = [
        sys.executable,
        "scripts/gdc/gdc_download.py",
        "--manifest", str(manifest),
        "--out_dir", str(out_dir),
        "--log_dir", str(gdc_log_dir),
        "--threads", str(n_conn),
        "--gdc_client", str(gdc_client),
    ]
    if token_file:
        cmd += ["--token_file", str(token_file)]
    if verify_after:
        cmd.append("--verify_after")
    if fail_on_verify:
        cmd.append("--fail_on_verify")

    if args.dry_run:
        print("[DRY RUN] RNA-seq download preview")
        print(f"  Manifest: {manifest}")
        print(f"  Files to download: {total}")
        print(f"  Output directory: {out_dir}")
        print(f"  Connections (-n): {n_conn}")
        print(f"  GDC client: {gdc_client}")
        print(f"  verify_after: {verify_after}")
        print(f"  fail_on_verify: {fail_on_verify}")
        if token_file:
            print(f"  token_file: {token_file}")
        print("\n  First 5 files:")
        print(df.head())
        print("\n[DRY RUN] No files were downloaded.")
        return 0

    out_dir.mkdir(parents=True, exist_ok=True)

    # runner log keeps only the command + pointers to the real gdc_download logs
    with runner_log.open("w", encoding="utf-8") as lf:
        lf.write("COMMAND:\n")
        lf.write(" ".join(cmd) + "\n\n")
        lf.write(f"GDC_DOWNLOAD_LOG_DIR: {gdc_log_dir}\n")

    # IMPORTANT:
    # Do not capture stdout/stderr here.
    # gdc_download.py prints a single progress line and logs the full gdc-client output itself.
    try:
        subprocess.run(cmd, check=True)
        print(f"[OK] Download finished. Runner log:\n  {runner_log}")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Download failed (exit={e.returncode}). Runner log:\n  {runner_log}")
        print(f"[INFO] Check gdc logs in:\n  {gdc_log_dir}")
        return e.returncode


if __name__ == "__main__":
    raise SystemExit(main())
