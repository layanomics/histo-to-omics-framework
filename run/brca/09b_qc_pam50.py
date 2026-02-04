# run/brca/09b_qc_pam50.py
import argparse
from pathlib import Path
from datetime import datetime
import yaml
import pandas as pd

VALID = {"LumA", "LumB", "Basal", "Her2"}

def _ts(fmt: str) -> str:
    return datetime.now().strftime(fmt)

def _ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    ts_fmt = cfg.get("logging", {}).get("timestamp_format", "%Y%m%d_%H%M%S")
    ts = _ts(ts_fmt)

    out_logs = Path(cfg["paths"]["out_logs"])
    _ensure_dir(out_logs)

    log_path = out_logs / f"09b_qc_pam50_{ts}.log"

    print("[RUNNING] Step 09b - QC PAM50 outputs")
    print(f"[INFO] Config: {args.config}")
    print(f"[INFO] Log: {log_path}")

    pam = cfg.get("pam50", {})
    clean_csv = pam.get("out_clean_calls_csv")
    qc_txt = pam.get("out_qc_txt")

    if not clean_csv or not qc_txt:
        print("[ERROR] Missing pam50.out_clean_calls_csv or pam50.out_qc_txt in config")
        raise SystemExit(2)

    clean_csv = Path(clean_csv)
    qc_txt = Path(qc_txt)

    if not clean_csv.exists():
        print(f"[ERROR] Missing clean calls CSV: {clean_csv}")
        raise SystemExit(2)

    _ensure_dir(qc_txt.parent)

    with log_path.open("w", encoding="utf-8") as lf:
        lf.write(f"CLEAN: {clean_csv}\n")
        lf.write(f"QC_TXT: {qc_txt}\n\n")

        df = pd.read_csv(clean_csv)

        if "sample_id" not in df.columns or "pam50_subtype" not in df.columns:
            raise ValueError(f"Expected columns sample_id + pam50_subtype. Columns={list(df.columns)}")

        n = len(df)
        n_unique = df["sample_id"].nunique()
        dup = n - n_unique

        counts = df["pam50_subtype"].value_counts(dropna=False)
        invalid = sorted(set(df["pam50_subtype"].dropna().astype(str)) - VALID)

        report = []
        report.append(f"FILE: {clean_csv.as_posix()}")
        report.append(f"ROWS: {n}")
        report.append(f"UNIQUE_SAMPLE_ID: {n_unique}")
        report.append(f"DUP_SAMPLE_ID: {dup}")
        report.append("SUBTYPE_COUNTS:")
        report.append(counts.to_string())
        report.append(f"INVALID_SUBTYPES: {invalid}")
        report.append("QC_STATUS: OK" if (dup == 0 and len(invalid) == 0) else "QC_STATUS: WARN")

        qc_txt.write_text("\n".join(report) + "\n", encoding="utf-8")

        lf.write("\n".join(report) + "\n")

    print(f"[OK] QC written: {qc_txt}")
    print(f"[OK] Log: {log_path}")

if __name__ == "__main__":
    main()
