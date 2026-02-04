# scripts/shared/make_paired_cohort.py
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path


REQUIRED_COLS = {"file_id", "file_name"}  # case_id + submitter_id handled with fallbacks


def _read_tsv(path: Path) -> list[dict]:
    # utf-8-sig avoids BOM causing headers like "\ufefffile_id"
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        if reader.fieldnames is None:
            raise ValueError(f"{path} has no header row (fieldnames=None).")

        # normalize header whitespace just in case
        reader.fieldnames = [h.strip() if h else h for h in reader.fieldnames]

        rows = []
        for r in reader:
            # strip keys + values
            cleaned = {}
            for k, v in r.items():
                if k is None:
                    continue
                k2 = k.strip()
                v2 = v.strip() if isinstance(v, str) else v
                cleaned[k2] = v2
            rows.append(cleaned)
        return rows


def _get_case_id(row: dict) -> str:
    # primary expected column
    cid = row.get("case_id", "")
    if cid:
        return cid

    # fallbacks (in case upstream schema changes)
    for alt in ("cases.case_id", "cases_case_id", "case"):
        cid = row.get(alt, "")
        if cid:
            return cid

    return ""


def _get_submitter_id(row: dict) -> str:
    sid = row.get("submitter_id", "")
    if sid:
        return sid

    for alt in ("cases.submitter_id", "cases_submitter_id"):
        sid = row.get(alt, "")
        if sid:
            return sid

    return ""


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Build paired cohort table by intersecting RNA and WSI on case_id."
    )
    ap.add_argument("--rna_meta", required=True, help="RNA metadata TSV from gdc_build_manifests.py")
    ap.add_argument("--wsi_meta", required=True, help="WSI metadata TSV from gdc_build_manifests.py")
    ap.add_argument("--out", required=True, help="Output CSV path")
    args = ap.parse_args()

    rna_meta = Path(args.rna_meta)
    wsi_meta = Path(args.wsi_meta)
    out = Path(args.out)

    rna = _read_tsv(rna_meta)
    wsi = _read_tsv(wsi_meta)

    # quick schema check: must have file_id + file_name at minimum
    for name, rows, src in (("RNA", rna, rna_meta), ("WSI", wsi, wsi_meta)):
        if not rows:
            raise ValueError(f"{name} metadata is empty: {src}")
        cols = set(rows[0].keys())
        missing = REQUIRED_COLS - cols
        if missing:
            raise ValueError(
                f"{name} metadata missing columns {sorted(missing)} in {src}\n"
                f"Columns found: {sorted(cols)}"
            )

    rna_by_case = defaultdict(list)
    skipped_rna = 0
    for row in rna:
        case_id = _get_case_id(row)
        if not case_id:
            skipped_rna += 1
            continue
        rna_by_case[case_id].append(row)

    wsi_by_case = defaultdict(list)
    skipped_wsi = 0
    for row in wsi:
        case_id = _get_case_id(row)
        if not case_id:
            skipped_wsi += 1
            continue
        wsi_by_case[case_id].append(row)

    all_cases = sorted(set(rna_by_case) | set(wsi_by_case))

    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "case_id",
                "submitter_id",
                "has_rnaseq",
                "has_wsi",
                "rnaseq_file_id",
                "rnaseq_file_name",
                "wsi_slide_count",
                "example_wsi_file_id",
                "example_wsi_file_name",
            ]
        )

        paired_count = 0
        for case_id in all_cases:
            has_rna = case_id in rna_by_case
            has_wsi = case_id in wsi_by_case
            if has_rna and has_wsi:
                paired_count += 1

            # submitter_id (prefer RNA row, else WSI row)
            submitter_id = ""
            if has_rna:
                submitter_id = _get_submitter_id(rna_by_case[case_id][0])
            elif has_wsi:
                submitter_id = _get_submitter_id(wsi_by_case[case_id][0])

            # one RNA example
            rna_file_id = rna_by_case[case_id][0].get("file_id", "") if has_rna else ""
            rna_file_name = rna_by_case[case_id][0].get("file_name", "") if has_rna else ""

            # WSI count + one example
            wsi_count = len(wsi_by_case[case_id]) if has_wsi else 0
            wsi_file_id = wsi_by_case[case_id][0].get("file_id", "") if has_wsi else ""
            wsi_file_name = wsi_by_case[case_id][0].get("file_name", "") if has_wsi else ""

            w.writerow(
                [
                    case_id,
                    submitter_id,
                    True if has_rna else False,
                    True if has_wsi else False,
                    rna_file_id,
                    rna_file_name,
                    wsi_count,
                    wsi_file_id,
                    wsi_file_name,
                ]
            )

    print(f"[OK] Wrote paired cohort table: {out}")
    print(f"[INFO] Cases with RNA: {len(rna_by_case)}")
    print(f"[INFO] Cases with WSI: {len(wsi_by_case)}")
    print(f"[INFO] Paired cases (RNA INTERSECT WSI): {paired_count}")
    if skipped_rna or skipped_wsi:
        print(f"[WARN] Skipped rows with missing case_id: RNA={skipped_rna}, WSI={skipped_wsi}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
