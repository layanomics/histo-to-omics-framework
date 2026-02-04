# run/brca/08_preprocess_expression.py
import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import yaml


def _ts(cfg: dict) -> str:
    fmt = (cfg.get("logging", {}) or {}).get("timestamp_format", "%Y%m%d_%H%M%S")
    return datetime.now().strftime(fmt)


def _run_and_log(cmd: list[str], log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as lf:
        lf.write("COMMAND:\n" + " ".join(cmd) + "\n\n")
        subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT, check=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    paths = cfg["paths"]
    prep = cfg["preprocess"]

    logs_dir = Path(paths["out_logs"])
    log_file = logs_dir / f"08_preprocess_expression_{_ts(cfg)}.log"

    # Minimal terminal output (so you know it started + where the log is)
    print("[RUNNING] Step 08 - Preprocess expression matrix")
    print(f"[INFO] Log: {log_file}")

    # Build the required CLI for scripts/shared/preprocess_expression.py
    cmd = [
        sys.executable,
        "scripts/shared/preprocess_expression.py",
        "--counts_csv", prep["counts_csv"],
        "--samples_csv", prep["samples_csv"],
        "--out_counts_preprocessed_csv", prep["out_counts_preprocessed_csv"],
    ]

    # Optional args
    if prep.get("gene_id_col"):
        cmd += ["--gene_id_col", str(prep["gene_id_col"])]

    if prep.get("min_total_count") is not None:
        cmd += ["--min_total_count", str(prep["min_total_count"])]

    if prep.get("min_samples_nonzero") is not None:
        cmd += ["--min_samples_nonzero", str(prep["min_samples_nonzero"])]

    if prep.get("make_log2cpm", False):
        cmd.append("--make_log2cpm")
        cmd += ["--out_counts_log2cpm_csv", prep["out_counts_log2cpm_csv"]]
        if prep.get("pseudocount") is not None:
            cmd += ["--pseudocount", str(prep["pseudocount"])]

    try:
        _run_and_log(cmd, log_file)
    except subprocess.CalledProcessError:
        print(f"[ERROR] Step 08 failed. See log: {log_file}")
        return 1

    print("[OK] Preprocess finished.")
    print(f"[OK] Log: {log_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
