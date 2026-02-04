import argparse
from pathlib import Path
import pandas as pd

def main():
    ap = argparse.ArgumentParser(
        description="Build a download manifest restricted to paired cases (RNA ∩ WSI) using case_id."
    )
    ap.add_argument("--paired_csv", required=True, help="Paired cohort CSV (must include case_id, has_rnaseq, has_wsi)")
    ap.add_argument("--rnaseq_meta_tsv", required=True, help="RNA metadata TSV from gdc_build_manifests.py")
    ap.add_argument("--out_manifest_tsv", required=True, help="Output manifest TSV (id, filename)")
    ap.add_argument("--require_paired", action="store_true", help="Keep only cases with has_rnaseq=True AND has_wsi=True")
    args = ap.parse_args()

    paired_csv = Path(args.paired_csv)
    rnaseq_meta_tsv = Path(args.rnaseq_meta_tsv)
    out_manifest_tsv = Path(args.out_manifest_tsv)
    out_manifest_tsv.parent.mkdir(parents=True, exist_ok=True)

    p = pd.read_csv(paired_csv)
    r = pd.read_csv(rnaseq_meta_tsv, sep="\t")

    required_cols_p = {"case_id", "has_rnaseq", "has_wsi"}
    missing_p = required_cols_p - set(p.columns)
    if missing_p:
        raise ValueError(f"paired_csv missing columns: {sorted(missing_p)}")

    required_cols_r = {"case_id", "file_id", "file_name"}
    missing_r = required_cols_r - set(r.columns)
    if missing_r:
        raise ValueError(f"rnaseq_meta_tsv missing columns: {sorted(missing_r)}")

    if args.require_paired:
        p = p[(p["has_rnaseq"] == True) & (p["has_wsi"] == True)]

    paired_cases = set(p["case_id"].astype(str))
    r = r[r["case_id"].astype(str).isin(paired_cases)].copy()

    # Manifest format for gdc-client
    manifest = r[["file_id", "file_name"]].rename(columns={"file_id": "id", "file_name": "filename"})
    manifest.to_csv(out_manifest_tsv, sep="\t", index=False)

    print(f"[OK] wrote manifest -> {out_manifest_tsv}")
    print(f"[INFO] paired cases used = {len(paired_cases)}")
    print(f"[INFO] RNA rows in manifest = {len(manifest)}")

if __name__ == "__main__":
    main()
