# run/brca/09a_clean_pam50_outputs.py
import argparse
from pathlib import Path
from datetime import datetime
import yaml
import pandas as pd

SUBTYPE_MAP = {
    # Common normalizations
    "luma": "LumA",
    "lumb": "LumB",
    "basal": "Basal",
    "basal-like": "Basal",
    "basallike": "Basal",
    "her2": "Her2",
    "her2-enriched": "Her2",
    "her2 enriched": "Her2",
    "her2enriched": "Her2",
    "her2_e": "Her2",
    "her2+": "Her2",
}

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

    log_path = out_logs / f"09a_clean_pam50_outputs_{ts}.log"

    print("[RUNNING] Step 09a - Clean PAM50 outputs")
    print(f"[INFO] Config: {args.config}")
    print(f"[INFO] Log: {log_path}")

    pam = cfg.get("pam50", {})
    raw_csv = pam.get("out_raw_calls_csv")
    clean_csv = pam.get("out_clean_calls_csv")

    if not raw_csv or not clean_csv:
        print("[ERROR] Missing pam50.out_raw_calls_csv or pam50.out_clean_calls_csv in config")
        raise SystemExit(2)

    raw_csv = Path(raw_csv)
    clean_csv = Path(clean_csv)

    if not raw_csv.exists():
        print(f"[ERROR] Missing raw calls CSV: {raw_csv}")
        raise SystemExit(2)

    _ensure_dir(clean_csv.parent)

    with log_path.open("w", encoding="utf-8") as lf:
        lf.write(f"RAW: {raw_csv}\n")
        lf.write(f"CLEAN: {clean_csv}\n\n")

        df = pd.read_csv(raw_csv)

        # Accept either naming
        if "pam50_subtype" in df.columns:
            subtype_col = "pam50_subtype"
        elif "pam50" in df.columns:
            subtype_col = "pam50"
        else:
            raise ValueError(f"Expected column pam50_subtype (or pam50). Columns={list(df.columns)}")

        if "sample_id" not in df.columns:
            raise ValueError(f"Expected column sample_id. Columns={list(df.columns)}")

        df["sample_id"] = df["sample_id"].astype(str).str.strip()
        df[subtype_col] = df[subtype_col].astype(str).str.strip()

        # Normalize subtype labels
        def norm(x: str) -> str:
            k = x.strip().lower()
            return SUBTYPE_MAP.get(k, x)

        df["pam50_subtype"] = df[subtype_col].map(norm)

        # Keep only required cols
        df = df[["sample_id", "pam50_subtype"]].copy()

        # Drop empty/NA-like
        df = df[df["sample_id"].notna()]
        df = df[df["pam50_subtype"].notna()]
        df = df[df["pam50_subtype"].astype(str).str.len() > 0]

        # De-duplicate on sample_id deterministically (first row)
        n0 = len(df)
        dup = df["sample_id"].duplicated().sum()
        if dup > 0:
            df = df.drop_duplicates("sample_id", keep="first")

        # Report invalid labels (don’t fail here; QC step can decide)
        invalid = sorted(set(df["pam50_subtype"]) - VALID)

        df.to_csv(clean_csv, index=False)

        lf.write(f"ROWS_IN: {n0}\n")
        lf.write(f"DUP_SAMPLE_ID: {dup}\n")
        lf.write(f"ROWS_OUT: {len(df)}\n")
        lf.write("SUBTYPE_COUNTS:\n")
        lf.write(df["pam50_subtype"].value_counts(dropna=False).to_string() + "\n")
        lf.write(f"INVALID_SUBTYPES: {invalid}\n")

    print(f"[OK] Clean calls written: {clean_csv.name}")
    print(f"[OK] Log: {log_path}")

if __name__ == "__main__":
    main()
