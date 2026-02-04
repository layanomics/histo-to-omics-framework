import argparse
from pathlib import Path
import pandas as pd

def main():
    ap = argparse.ArgumentParser(description="Build a manifest with exactly one RNA-seq file per case_id.")
    ap.add_argument("--paired_csv", required=True)
    ap.add_argument("--rnaseq_meta_tsv", required=True)
    ap.add_argument("--out_manifest_tsv", required=True)
    ap.add_argument("--out_selection_csv", required=True, help="Record which file was chosen per case_id.")
    args = ap.parse_args()

    p = pd.read_csv(args.paired_csv)
    r = pd.read_csv(args.rnaseq_meta_tsv, sep="\t")

    # keep paired cases only
    p = p[(p["has_rnaseq"] == True) & (p["has_wsi"] == True)]
    paired_cases = set(p["case_id"].astype(str))

    r = r[r["case_id"].astype(str).isin(paired_cases)].copy()

    # deterministic pick: sort and take first per case_id
    r["file_name"] = r["file_name"].astype(str)
    r_sorted = r.sort_values(["case_id", "file_name", "file_id"], ascending=True)

    chosen = r_sorted.groupby("case_id", as_index=False).first()

    # write manifest
    out_manifest = Path(args.out_manifest_tsv)
    out_manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest = chosen[["file_id", "file_name"]].rename(columns={"file_id": "id", "file_name": "filename"})
    manifest.to_csv(out_manifest, sep="\t", index=False)

    # write selection record
    out_sel = Path(args.out_selection_csv)
    out_sel.parent.mkdir(parents=True, exist_ok=True)
    chosen.to_csv(out_sel, index=False)

    # stats
    dup_cases = (r.groupby("case_id").size() > 1).sum()
    print(f"[OK] wrote one-per-case manifest -> {out_manifest} rows={len(manifest)}")
    print(f"[OK] wrote selection table      -> {out_sel}")
    print(f"[INFO] paired cases             = {len(paired_cases)}")
    print(f"[INFO] RNA rows candidate set   = {len(r)}")
    print(f"[INFO] cases with >1 RNA file   = {int(dup_cases)}")

if __name__ == "__main__":
    main()
