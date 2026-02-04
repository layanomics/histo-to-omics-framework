import argparse
import csv
from pathlib import Path
import time
import yaml


def _load_cfg(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _count_rows_fast(csv_path: Path) -> int:
    # counts data rows (excluding header) without loading whole file
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        n = -1
        for n, _ in enumerate(f):
            pass
    return max(0, n)  # header line makes n start at 0; rows = n


def _read_header(csv_path: Path) -> list[str]:
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        r = csv.reader(f)
        return next(r)


def main() -> int:
    ap = argparse.ArgumentParser(description="QC checks for expression matrix outputs (Step 07).")
    ap.add_argument("--config", required=True, help="Path to YAML config.")
    args = ap.parse_args()

    t0 = time.time()
    cfg = _load_cfg(Path(args.config))

    expr = cfg.get("expression", {})
    out_counts = Path(expr["out_counts_csv"])
    out_samples = Path(expr["out_samples_csv"])

    # --- FIX: default QC output goes under paths.out_logs (cancer-agnostic) ---
    paths = cfg.get("paths", {}) or {}
    out_logs = Path(paths.get("out_logs", "outputs"))
    default_qc_out = out_logs / "expr_matrix_qc.txt"
    qc_out = Path(expr.get("qc_out_txt", default_qc_out))
    # ------------------------------------------------------------------------

    qc_out.parent.mkdir(parents=True, exist_ok=True)

    lines = []
    lines.append(f"CONFIG: {args.config}")
    lines.append(f"COUNTS_CSV: {out_counts}")
    lines.append(f"SAMPLES_CSV: {out_samples}")
    lines.append("")

    # existence
    lines.append(f"EXISTS counts_csv: {out_counts.exists()}")
    lines.append(f"EXISTS samples_csv: {out_samples.exists()}")
    lines.append("")

    if not out_counts.exists() or not out_samples.exists():
        qc_out.write_text("\n".join(lines) + "\n", encoding="utf-8")
        print(f"[ERROR] Missing outputs. Wrote QC summary: {qc_out}")
        return 2

    # counts matrix quick stats (no full load)
    counts_header = _read_header(out_counts)
    if len(counts_header) < 2:
        raise ValueError("Counts CSV header looks wrong (expected gene_id + >=1 sample columns).")

    gene_col = counts_header[0]
    sample_cols = counts_header[1:]
    genes_n = _count_rows_fast(out_counts)
    samples_n = len(sample_cols)

    lines.append("COUNTS_MATRIX:")
    lines.append(f"  gene_id_col: {gene_col}")
    lines.append(f"  genes (rows): {genes_n}")
    lines.append(f"  samples (cols): {samples_n}")
    lines.append("")

    # samples sheet load (small)
    with out_samples.open("r", encoding="utf-8", newline="") as f:
        dr = csv.DictReader(f)
        sample_rows = list(dr)

    lines.append("SAMPLE_SHEET:")
    lines.append(f"  rows: {len(sample_rows)}")
    lines.append(f"  columns: {dr.fieldnames}")
    lines.append("")

    # check sample alignment between files
    sample_sheet_ids = []
    for row in sample_rows:
        # accept either "sample_id" or "submitter_id" or "case_id" depending on your builder
        if "sample_id" in row and row["sample_id"]:
            sample_sheet_ids.append(row["sample_id"])
        elif "submitter_id" in row and row["submitter_id"]:
            sample_sheet_ids.append(row["submitter_id"])
        else:
            sample_sheet_ids.append("")

    # basic duplicates/missing
    nonempty = [x for x in sample_sheet_ids if x]
    dup_count = len(nonempty) - len(set(nonempty))

    lines.append("ALIGNMENT_CHECKS:")
    lines.append(f"  counts_csv_sample_cols: {samples_n}")
    lines.append(f"  samples_csv_rows: {len(sample_rows)}")
    lines.append(f"  sample_id_nonempty: {len(nonempty)}")
    lines.append(f"  sample_id_duplicates: {dup_count}")
    lines.append("")

    ok_dim = (samples_n == len(sample_rows))
    lines.append(f"PASS dims_match (samples cols == sample rows): {ok_dim}")
    lines.append("")

    # write qc
    elapsed = time.time() - t0
    lines.append(f"ELAPSED_SEC: {elapsed:.2f}")

    qc_out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[OK] Wrote expression matrix QC -> {qc_out}")
    if not ok_dim:
        print("[WARN] Sample count mismatch between counts matrix and sample sheet (check builder).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
