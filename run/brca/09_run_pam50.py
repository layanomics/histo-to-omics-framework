# run/brca/09_run_pam50.py
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
import yaml

def _ts():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def _ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def _must_exist(p: Path, label: str):
    if not p.exists():
        raise SystemExit(f"[ERROR] {label} not found: {p}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/brca_phase1.yaml")
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))

    logs_dir = Path(cfg["paths"]["out_logs"])
    tables_dir = Path(cfg["paths"]["out_tables"])
    _ensure_dir(logs_dir)
    _ensure_dir(tables_dir)

    log_path = logs_dir / f"09_run_pam50_{_ts()}.log"

    pam = cfg.get("pam50", {})
    missing = [k for k in ["r_script", "counts_preprocessed_csv", "samples_csv", "out_raw_calls_csv"] if not pam.get(k)]
    if missing:
        raise SystemExit(f"[ERROR] Missing keys under config `pam50:`: {', '.join(missing)}")

    r_script = Path(pam["r_script"])
    counts_csv = Path(pam["counts_preprocessed_csv"])
    samples_csv = Path(pam["samples_csv"])
    out_raw = Path(pam["out_raw_calls_csv"])

    _must_exist(r_script, "R script")
    _must_exist(counts_csv, "Expression matrix")
    _must_exist(samples_csv, "Samples CSV")

    cmd = [
        "Rscript",
        str(r_script),
        "--counts_preprocessed_csv", str(counts_csv),
        "--samples_csv", str(samples_csv),
        "--out_raw_calls_csv", str(out_raw),
    ]

    print("[RUNNING] Step 09 - Run PAM50 (R)")
    print(f"[INFO] Log: {log_path}")
    print(f"[INFO] Expr matrix: {counts_csv.name}")
    print(f"[INFO] Output calls: {out_raw.name}")

    with log_path.open("w", encoding="utf-8") as lf:
        lf.write("COMMAND:\n" + " ".join(cmd) + "\n\n")
        try:
            subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT, check=True)
        except subprocess.CalledProcessError:
            print("[ERROR] Step 09 failed (Rscript returned non-zero).")
            print(f"[INFO] Full log: {log_path}")
            raise SystemExit(1)

    print("[OK] PAM50 run finished.")
    print(f"[OK] Log: {log_path}")

if __name__ == "__main__":
    main()
