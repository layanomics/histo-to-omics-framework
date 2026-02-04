import argparse
from pathlib import Path
import yaml
import pandas as pd
import numpy as np


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _p(pth: str) -> Path:
    return Path(pth)


def _write_txt(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _describe_series(x: pd.Series) -> str:
    x = pd.to_numeric(x, errors="coerce")
    return (
        f"min={np.nanmin(x):.3g} | p25={np.nanpercentile(x,25):.3g} | "
        f"median={np.nanmedian(x):.3g} | p75={np.nanpercentile(x,75):.3g} | "
        f"max={np.nanmax(x):.3g}"
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="QC for Step 08 preprocess outputs.")
    ap.add_argument("--config", required=True, help="Path to YAML config")
    args = ap.parse_args()

    cfg = _load_yaml(Path(args.config))

    required = ["paths", "expression", "preprocess"]
    for k in required:
        if k not in cfg:
            raise ValueError(f"Missing config key: {k}")

    counts_in = _p(cfg["expression"]["out_counts_csv"])
    samples_in = _p(cfg["expression"]["out_samples_csv"])

    out_counts_pre = _p(cfg["preprocess"]["out_counts_preprocessed_csv"])
    out_log2cpm = _p(cfg["preprocess"].get("out_counts_log2cpm_csv", "")) if cfg["preprocess"].get("make_log2cpm", False) else None

    out_txt = _p(cfg.get("qc_preprocess", {}).get(
        "out_txt",
        str(Path(cfg["paths"]["out_logs"]) / "preprocess_expression_qc.txt")
    ))

    # ---- Load ----
    if not counts_in.exists():
        raise FileNotFoundError(f"Missing input counts matrix: {counts_in}")
    if not samples_in.exists():
        raise FileNotFoundError(f"Missing input sample sheet: {samples_in}")
    if not out_counts_pre.exists():
        raise FileNotFoundError(f"Missing preprocessed counts matrix: {out_counts_pre}")

    df_raw = pd.read_csv(counts_in)
    df_pre = pd.read_csv(out_counts_pre)

    # Identify gene column (first col)
    gene_col_raw = df_raw.columns[0]
    gene_col_pre = df_pre.columns[0]

    raw_mat = df_raw.drop(columns=[gene_col_raw])
    pre_mat = df_pre.drop(columns=[gene_col_pre])

    # ---- Basic integrity ----
    report_lines = []
    report_lines.append("QC: Step 08 preprocess expression\n")
    report_lines.append(f"INPUT_COUNTS: {counts_in.as_posix()}")
    report_lines.append(f"INPUT_SAMPLES: {samples_in.as_posix()}")
    report_lines.append(f"PREPROCESSED_COUNTS: {out_counts_pre.as_posix()}")
    if out_log2cpm is not None:
        report_lines.append(f"LOG2CPM_COUNTS: {out_log2cpm.as_posix()}")
    report_lines.append("")

    report_lines.append(f"RAW_SHAPE: genes={raw_mat.shape[0]} samples={raw_mat.shape[1]}")
    report_lines.append(f"PRE_SHAPE: genes={pre_mat.shape[0]} samples={pre_mat.shape[1]}")
    report_lines.append(f"GENES_REMOVED: {raw_mat.shape[0] - pre_mat.shape[0]}")
    report_lines.append("")

    # ---- Missing values ----
    report_lines.append(f"RAW_NA_COUNT: {int(raw_mat.isna().sum().sum())}")
    report_lines.append(f"PRE_NA_COUNT: {int(pre_mat.isna().sum().sum())}")
    report_lines.append("")

    # ---- Sample-level totals + sparsity ----
    raw_totals = raw_mat.sum(axis=0, numeric_only=True)
    pre_totals = pre_mat.sum(axis=0, numeric_only=True)

    raw_zero_frac = (raw_mat == 0).mean(axis=0)
    pre_zero_frac = (pre_mat == 0).mean(axis=0)

    report_lines.append("RAW_LIBRARY_SIZE (sum counts per sample):")
    report_lines.append(_describe_series(raw_totals))
    report_lines.append("RAW_ZERO_FRACTION (fraction genes with 0 per sample):")
    report_lines.append(_describe_series(raw_zero_frac))
    report_lines.append("")

    report_lines.append("PRE_LIBRARY_SIZE (sum counts per sample):")
    report_lines.append(_describe_series(pre_totals))
    report_lines.append("PRE_ZERO_FRACTION (fraction genes with 0 per sample):")
    report_lines.append(_describe_series(pre_zero_frac))
    report_lines.append("")

    # ---- Optional log2cpm QC ----
    if out_log2cpm is not None:
        if not out_log2cpm.exists():
            raise FileNotFoundError(f"Expected log2cpm output but not found: {out_log2cpm}")
        df_l2 = pd.read_csv(out_log2cpm)
        gene_col_l2 = df_l2.columns[0]
        l2_mat = df_l2.drop(columns=[gene_col_l2])

        finite_ok = np.isfinite(l2_mat.to_numpy(dtype=float, copy=False)).all()
        report_lines.append(f"LOG2CPM_SHAPE: genes={l2_mat.shape[0]} samples={l2_mat.shape[1]}")
        report_lines.append(f"LOG2CPM_ALL_FINITE: {finite_ok}")
        report_lines.append("LOG2CPM_VALUE_RANGE (global):")
        report_lines.append(f"min={np.nanmin(l2_mat.to_numpy(dtype=float)):.3g} max={np.nanmax(l2_mat.to_numpy(dtype=float)):.3g}")
        report_lines.append("")

    report = "\n".join(report_lines).rstrip() + "\n"
    _write_txt(out_txt, report)

    # Minimal terminal output
    print(f"[OK] Preprocess QC written: {out_txt}")
    print(f"[INFO] RAW genes={raw_mat.shape[0]} samples={raw_mat.shape[1]} | PRE genes={pre_mat.shape[0]} samples={pre_mat.shape[1]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
