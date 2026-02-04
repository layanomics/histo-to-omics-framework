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

    log = out_logs / f"10b_qc_wsi_pam50_labels_{_ts(fmt)}.log"
    wcfg = cfg["wsi_labeling"]

    cmd = [
        "python",
        "scripts/shared/qc_wsi_pam50_labels.py",
        "--slide_labels_csv",
        wcfg["out_slide_labels_csv"],
        "--case_labels_csv",
        wcfg["out_case_labels_csv"],
        "--out_qc_txt",
        wcfg["out_qc_txt"],
    ]

    print("[RUNNING] Step 10b - QC WSI<->PAM50 label tables")
    print(f"[INFO] Log: {log}")

    with log.open("w", encoding="utf-8") as lf:
        lf.write("COMMAND:\n" + " ".join(cmd) + "\n\n")
        subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT, check=True)

    print("[OK] QC finished.")
    print(f"[OK] Log: {log}")


if __name__ == "__main__":
    raise SystemExit(main())
