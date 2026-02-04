#!/usr/bin/env python
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
import yaml


def _ts(fmt: str) -> str:
    return datetime.now().strftime(fmt)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text())
    fmt = cfg.get("logging", {}).get("timestamp_format", "%Y%m%d_%H%M%S")
    out_logs = Path(cfg["paths"]["out_logs"])
    out_logs.mkdir(parents=True, exist_ok=True)

    log = out_logs / f"10_make_wsi_pam50_labels_{_ts(fmt)}.log"

    wcfg = cfg["wsi_labeling"]

    cmd = [
        "python",
        "scripts/shared/make_wsi_pam50_labels.py",
        "--config",
        args.config,
        "--paired_cohort_csv",
        wcfg["paired_cohort_csv"],
        "--wsi_metadata_tsv",
        wcfg["wsi_metadata_tsv"],
        "--pam50_clean_calls_csv",
        wcfg["pam50_clean_calls_csv"],
        "--out_slide_labels_csv",
        wcfg["out_slide_labels_csv"],
        "--out_case_labels_csv",
        wcfg["out_case_labels_csv"],
        "--require_only_paired",
    ]

    print("[RUNNING] Step 10 - Build WSI<->PAM50 label tables")
    print(f"[INFO] Log: {log}")

    with log.open("w", encoding="utf-8") as lf:
        lf.write("COMMAND:\n" + " ".join(cmd) + "\n\n")
        subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT, check=True)

    print("[OK] Step 10 finished.")
    print(f"[OK] Log: {log}")


if __name__ == "__main__":
    raise SystemExit(main())
