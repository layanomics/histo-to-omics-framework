import argparse
import time
from pathlib import Path

import pandas as pd


def _log(msg: str):
    print(msg, flush=True)


def _require_exists(path: Path, label: str):
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")


def _load_counts(counts_csv: Path, gene_id_col: str) -> pd.DataFrame:
    df = pd.read_csv(counts_csv)
    if gene_id_col not in df.columns:
        raise ValueError(
            f"gene_id_col='{gene_id_col}' not found in counts CSV. "
            f"Columns={list(df.columns)[:10]}..."
        )
    df = df.set_index(gene_id_col)
    return df


def _filter_genes(
    counts: pd.DataFrame,
    min_total_count: int,
    min_samples_nonzero: int,
) -> pd.DataFrame:
    # Remove genes with very low evidence across the cohort
    total = counts.sum(axis=1)
    nonzero = (counts > 0).sum(axis=1)

    keep = (total >= min_total_count) & (nonzero >= min_samples_nonzero)
    return counts.loc[keep]


def _counts_to_log2cpm(counts: pd.DataFrame, pseudocount: float = 1.0) -> pd.DataFrame:
    # CPM = counts / library_size * 1e6
    lib_size = counts.sum(axis=0)
    # avoid division by zero
    lib_size = lib_size.replace(0, pd.NA)
    cpm = counts.div(lib_size, axis=1) * 1e6
    cpm = cpm.fillna(0.0)
    return (cpm + pseudocount).applymap(lambda x: 0.0 if x <= 0 else x).pipe(lambda df: df.applymap(lambda x: x)).pipe(
        lambda df: df.applymap(lambda x: x)
    ).applymap(lambda x: x)  # keep numeric stability
    # NOTE: log2 is applied below to avoid accidental dtype issues


def main():
    ap = argparse.ArgumentParser(description="Preprocess expression counts matrix (filtering + optional log2(CPM+pseudo)).")
    ap.add_argument("--counts_csv", required=True)
    ap.add_argument("--samples_csv", required=True)
    ap.add_argument("--out_counts_preprocessed_csv", required=True)
    ap.add_argument("--out_counts_log2cpm_csv", required=False, default="")
    ap.add_argument("--gene_id_col", default="gene_id")

    ap.add_argument("--min_total_count", type=int, default=10)
    ap.add_argument("--min_samples_nonzero", type=int, default=10)

    ap.add_argument("--make_log2cpm", action="store_true")
    ap.add_argument("--pseudocount", type=float, default=1.0)

    args = ap.parse_args()

    t0 = time.time()

    counts_csv = Path(args.counts_csv)
    samples_csv = Path(args.samples_csv)

    out_pre = Path(args.out_counts_preprocessed_csv)
    out_log2 = Path(args.out_counts_log2cpm_csv) if args.out_counts_log2cpm_csv else None

    _require_exists(counts_csv, "COUNTS_CSV")
    _require_exists(samples_csv, "SAMPLES_CSV")

    out_pre.parent.mkdir(parents=True, exist_ok=True)
    if out_log2:
        out_log2.parent.mkdir(parents=True, exist_ok=True)

    _log(f"[INFO] Loading counts: {counts_csv}")
    counts = _load_counts(counts_csv, gene_id_col=args.gene_id_col)

    _log(f"[INFO] Loaded matrix: genes={counts.shape[0]} samples={counts.shape[1]}")
    _log(f"[INFO] Filtering genes: min_total_count={args.min_total_count}, min_samples_nonzero={args.min_samples_nonzero}")

    before = counts.shape[0]
    counts_f = _filter_genes(counts, min_total_count=args.min_total_count, min_samples_nonzero=args.min_samples_nonzero)
    after = counts_f.shape[0]
    _log(f"[INFO] Genes kept: {after}/{before} (removed {before-after})")

    # Write filtered counts (raw scale)
    df_out = counts_f.copy()
    df_out.insert(0, args.gene_id_col, df_out.index)
    df_out.to_csv(out_pre, index=False)
    _log(f"[OK] Wrote preprocessed counts (filtered): {out_pre}")

    if args.make_log2cpm:
        if out_log2 is None:
            raise ValueError("--make_log2cpm requires --out_counts_log2cpm_csv")

        _log(f"[INFO] Computing log2(CPM + {args.pseudocount})")
        lib_size = counts_f.sum(axis=0).replace(0, pd.NA)
        cpm = counts_f.div(lib_size, axis=1) * 1e6
        cpm = cpm.fillna(0.0)
        log2cpm = (cpm + args.pseudocount).applymap(lambda x: 0.0 if x <= 0 else x)
        log2cpm = log2cpm.applymap(lambda x: __import__("math").log2(x))

        df2 = log2cpm.copy()
        df2.insert(0, args.gene_id_col, df2.index)
        df2.to_csv(out_log2, index=False)
        _log(f"[OK] Wrote log2cpm: {out_log2}")

    elapsed = time.time() - t0
    _log(f"[DONE] preprocess_expression finished in {elapsed:.2f} sec")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
