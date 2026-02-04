#!/usr/bin/env python
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def _pick_col(df: pd.DataFrame, candidates: list[str], label: str) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise ValueError(
        f"Missing required column for {label}. Tried {candidates}. "
        f"Found: {list(df.columns)[:80]} ..."
    )


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Build slide-level + case-level subtype label tables by joining WSI metadata with case-level labels."
    )

    ap.add_argument("--paired_cohort_csv", required=True)
    ap.add_argument("--wsi_metadata_tsv", required=True)

    ap.add_argument("--labels_csv", required=True, help="Case-level labels CSV (must contain case_id + label column).")
    ap.add_argument("--labels_case_id_col", default="case_id")
    ap.add_argument("--labels_label_col", required=True, help="Column in labels_csv containing the subtype (e.g., cms_subtype).")

    ap.add_argument("--label_name", required=True, help="Output label column name (e.g., cms_subtype, pam50_subtype).")

    # Main outputs (ALWAYS written; may contain NaNs)
    ap.add_argument("--out_slide_labels_csv", required=True)
    ap.add_argument("--out_case_labels_csv", required=True)

    # Optional filtered outputs (ONLY written if missing labels exist)
    ap.add_argument("--out_slide_labels_filtered_csv", default=None)
    ap.add_argument("--out_case_labels_filtered_csv", default=None)

    ap.add_argument(
        "--require_only_paired",
        action="store_true",
        help="Restrict to cases where has_wsi==True and has_rnaseq==True in paired cohort.",
    )

    ap.add_argument(
        "--auto_filter_missing",
        action="store_true",
        help="If any missing labels exist, also write filtered outputs with NaNs removed.",
    )

    args = ap.parse_args()

    paired = pd.read_csv(args.paired_cohort_csv)
    wsi = pd.read_csv(args.wsi_metadata_tsv, sep="\t")
    labels = pd.read_csv(args.labels_csv)

    # paired cohort schema
    paired_case_col = _pick_col(paired, ["case_id", "cases.case_id"], "paired.case_id")
    has_wsi_col = _pick_col(paired, ["has_wsi"], "paired.has_wsi")
    has_rna_col = _pick_col(paired, ["has_rnaseq"], "paired.has_rnaseq")

    if args.require_only_paired:
        paired = paired[(paired[has_wsi_col] == True) & (paired[has_rna_col] == True)].copy()

    paired_cases = set(paired[paired_case_col].astype(str).str.strip())

    # WSI schema
    wsi_case_col = _pick_col(
        wsi,
        ["case_id", "cases.case_id", "cases.0.case_id", "associated_entities.case_id"],
        "wsi.case_id",
    )
    wsi_file_id_col = _pick_col(wsi, ["file_id", "id"], "wsi.file_id")

    wsi_file_name_col = None
    for cand in ["file_name", "filename"]:
        if cand in wsi.columns:
            wsi_file_name_col = cand
            break

    # labels schema
    if args.labels_case_id_col not in labels.columns:
        raise ValueError(
            f"labels_case_id_col='{args.labels_case_id_col}' not found in labels CSV columns={list(labels.columns)}"
        )
    if args.labels_label_col not in labels.columns:
        raise ValueError(
            f"labels_label_col='{args.labels_label_col}' not found in labels CSV columns={list(labels.columns)}"
        )

    # normalize keys
    labels = labels.rename(columns={args.labels_case_id_col: "case_id", args.labels_label_col: args.label_name}).copy()
    labels["case_id"] = labels["case_id"].astype(str).str.strip()

    # IMPORTANT: do NOT coerce NaNs into "nan" strings
    labels[args.label_name] = labels[args.label_name].where(labels[args.label_name].notna(), pd.NA)

    wsi = wsi.rename(columns={wsi_case_col: "case_id", wsi_file_id_col: "wsi_file_id"}).copy()
    wsi["case_id"] = wsi["case_id"].astype(str).str.strip()
    wsi["wsi_file_id"] = wsi["wsi_file_id"].astype(str).str.strip()
    if wsi_file_name_col:
        wsi = wsi.rename(columns={wsi_file_name_col: "wsi_filename"})

    # Restrict to paired cases
    wsi = wsi[wsi["case_id"].isin(paired_cases)].copy()

    # Merge subtype onto slides
    slide = wsi.merge(labels[["case_id", args.label_name]], on="case_id", how="left")

    # Case-level summary (includes NA groups if present)
    case = (
        slide.groupby(["case_id", args.label_name], dropna=False)
        .size()
        .reset_index(name="n_wsi_slides")
    )

    # Always write full outputs
    out_slide = Path(args.out_slide_labels_csv)
    out_case = Path(args.out_case_labels_csv)
    out_slide.parent.mkdir(parents=True, exist_ok=True)
    out_case.parent.mkdir(parents=True, exist_ok=True)

    slide.to_csv(out_slide, index=False)
    case.to_csv(out_case, index=False)

    missing_slides = int(slide[args.label_name].isna().sum())
    missing_cases = int(case[args.label_name].isna().sum())

    print(f"[OK] Wrote slide labels (FULL): {out_slide} (rows={len(slide)})")
    print(f"[OK] Wrote case labels  (FULL): {out_case} (rows={len(case)})")
    print(f"[INFO] Missing labels: cases={missing_cases}, slides={missing_slides}")

    # Conditional filtered outputs
    if args.auto_filter_missing and missing_slides > 0:
        if not args.out_slide_labels_filtered_csv or not args.out_case_labels_filtered_csv:
            raise ValueError(
                "auto_filter_missing requires --out_slide_labels_filtered_csv and --out_case_labels_filtered_csv"
            )

        slide_f = slide[slide[args.label_name].notna()].copy()
        case_f = (
            slide_f.groupby(["case_id", args.label_name], dropna=False)
            .size()
            .reset_index(name="n_wsi_slides")
        )

        out_slide_f = Path(args.out_slide_labels_filtered_csv)
        out_case_f = Path(args.out_case_labels_filtered_csv)
        out_slide_f.parent.mkdir(parents=True, exist_ok=True)
        out_case_f.parent.mkdir(parents=True, exist_ok=True)

        slide_f.to_csv(out_slide_f, index=False)
        case_f.to_csv(out_case_f, index=False)

        print(f"[OK] Wrote slide labels (FILTERED): {out_slide_f} (rows={len(slide_f)})")
        print(f"[OK] Wrote case labels  (FILTERED): {out_case_f} (rows={len(case_f)})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
