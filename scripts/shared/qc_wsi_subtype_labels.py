#!/usr/bin/env python
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def main() -> int:
    ap = argparse.ArgumentParser(description="QC WSI<->subtype label tables (generic).")
    ap.add_argument("--slide_labels_csv", required=True)
    ap.add_argument("--case_labels_csv", required=True)
    ap.add_argument("--out_qc_txt", required=True)

    ap.add_argument("--label_col", required=True, help="Subtype column name (e.g., cms_subtype, pam50_subtype).")
    ap.add_argument("--allowed_labels", default=None, help="Comma-separated allowed labels (optional).")

    args = ap.parse_args()

    slide = pd.read_csv(args.slide_labels_csv)
    case = pd.read_csv(args.case_labels_csv)

    label_col = args.label_col
    allowed = None
    if args.allowed_labels:
        allowed = set(x.strip() for x in args.allowed_labels.split(",") if x.strip())

    qc = []
    qc.append(f"SLIDE_FILE: {args.slide_labels_csv}")
    qc.append(f"CASE_FILE : {args.case_labels_csv}")
    qc.append(f"LABEL_COL : {label_col}")
    qc.append("")

    qc.append(f"SLIDE_ROWS: {len(slide)}")
    qc.append(f"CASE_ROWS : {len(case)}")
    if "case_id" in slide.columns:
        qc.append(f"UNIQUE_CASES_IN_SLIDE: {slide['case_id'].nunique()}")
    if "wsi_file_id" in slide.columns:
        qc.append(f"UNIQUE_WSI_FILE_ID  : {slide['wsi_file_id'].nunique()}")

    qc.append("")
    if label_col in slide.columns:
        missing = int(slide[label_col].isna().sum())
        qc.append(f"MISSING_LABEL_ON_SLIDES: {missing}")
        qc.append("LABEL_COUNTS_ON_SLIDES:")
        counts = slide[label_col].value_counts(dropna=False)
        qc.extend([f"  {k}: {v}" for k, v in counts.items()])
    else:
        qc.append(f"[WARN] label_col '{label_col}' missing in slide table columns={list(slide.columns)}")

    qc.append("")
    invalid = []
    if allowed is not None and label_col in case.columns:
        invalid = sorted(set(case[label_col].dropna().astype(str).unique()) - allowed)
        qc.append(f"ALLOWED_LABELS: {sorted(allowed)}")
        qc.append(f"INVALID_LABELS_IN_CASE_TABLE: {invalid}")

    out = Path(args.out_qc_txt)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(qc) + "\n", encoding="utf-8")

    print(f"[OK] QC written: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
