# scripts/shared/inspect_star_counts_schema.py
import argparse
import csv
from pathlib import Path
import yaml
from datetime import datetime

SUMMARY_PREFIX = "N_"  # STAR summary rows begin with "N_"
COMMENT_PREFIX = "#"


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _infer_short_from_meta_dir(meta_dir: Path) -> str:
    """
    meta_dir examples:
      data/metadata/brca_phase1  -> brca
      data/metadata/crc_phase1   -> crc
      data/metadata/luad_phase1  -> luad
    """
    name = meta_dir.name.lower()
    for suf in ("_phase1", "-phase1", "_phase_1", "-phase_1"):
        if name.endswith(suf):
            return name[: -len(suf)]
    return name


def _get_project_short(cfg: dict, meta_dir: Path) -> str:
    proj = cfg.get("project", {}) or {}
    short = proj.get("short")
    if isinstance(short, str) and short.strip():
        return short.strip().lower()
    return _infer_short_from_meta_dir(meta_dir)


def _resolve_from_config(cfg: dict) -> tuple[Path, Path, Path]:
    """
    Returns: (manifest_tsv, download_dir, out_txt)
    """
    paths = cfg.get("paths", {}) or {}
    expr = cfg.get("expression", {}) or {}

    meta_dir = Path(paths["meta_dir"])
    raw_dir = Path(paths["raw_dir"])
    out_logs = Path(paths["out_logs"])

    short = _get_project_short(cfg, meta_dir)

    manifest_default = meta_dir / f"{short}_rnaseq_manifest_one_per_case.tsv"
    download_default = raw_dir / "rnaseq_star_counts"

    manifest = Path(expr.get("rnaseq_manifest_one_per_case", manifest_default))
    download_dir = Path(expr.get("rnaseq_download_dir", download_default))
    out_txt = out_logs / "star_counts_schema_inspection.txt"
    return manifest, download_dir, out_txt


def _read_manifest(manifest_tsv: Path) -> list[dict]:
    with manifest_tsv.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        rows = list(r)
    if not rows or "id" not in rows[0] or "filename" not in rows[0]:
        raise ValueError(
            f"Manifest must have columns: id, filename. Got: {list(rows[0].keys()) if rows else 'EMPTY'}"
        )
    return rows


def _peek_lines(path: Path, max_lines: int = 80) -> list[str]:
    out = []
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for i, ln in enumerate(f):
            if i >= max_lines:
                break
            out.append(ln.rstrip("\n"))
    return out


def _detect_header(lines: list[str]) -> tuple[int, list[str]]:
    for i, ln in enumerate(lines):
        if not ln:
            continue
        if ln.startswith(COMMENT_PREFIX):
            continue
        cols = ln.split("\t")
        if len(cols) >= 2 and (cols[0].lower() in {"gene_id", "gene"} or "unstranded" in ln.lower()):
            return i, cols
    return -1, []


def _count_summary_rows(lines: list[str]) -> dict:
    counts = {}
    for ln in lines:
        if not ln:
            continue
        first = ln.split("\t", 1)[0]
        if first.startswith(SUMMARY_PREFIX):
            counts[first] = counts.get(first, 0) + 1
    return counts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", help="YAML config (preferred)")
    ap.add_argument("--manifest_tsv", help="Manifest TSV (id, filename)")
    ap.add_argument("--download_dir", help="Download dir containing <file_id>/<filename>")
    ap.add_argument("--out_txt", help="Output report txt")
    ap.add_argument("--n", type=int, default=5, help="Inspect first N samples from manifest (deterministic)")
    ap.add_argument("--max_lines", type=int, default=80, help="Max lines to peek per file")
    args = ap.parse_args()

    if args.config:
        cfg = _load_yaml(Path(args.config))
        manifest_tsv, download_dir, out_txt_default = _resolve_from_config(cfg)
    else:
        if not args.manifest_tsv or not args.download_dir:
            raise SystemExit("Provide --config OR (--manifest_tsv AND --download_dir).")
        manifest_tsv = Path(args.manifest_tsv)
        download_dir = Path(args.download_dir)
        out_txt_default = Path("star_counts_schema_inspection.txt")

    out_txt = Path(args.out_txt) if args.out_txt else out_txt_default
    out_txt.parent.mkdir(parents=True, exist_ok=True)

    rows = _read_manifest(manifest_tsv)
    rows = rows[: min(args.n, len(rows))]

    header_ok = 0
    header_fail = 0
    missing = 0

    with out_txt.open("w", encoding="utf-8") as out:
        out.write(f"TIMESTAMP: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        out.write(f"MANIFEST: {manifest_tsv}\n")
        out.write(f"DOWNLOAD_DIR: {download_dir}\n")
        out.write(f"INSPECT_N: {len(rows)}\n\n")

        for r in rows:
            file_id = r["id"]
            filename = r["filename"]
            tsv_path = download_dir / file_id / filename

            out.write("=" * 80 + "\n")
            out.write(f"id: {file_id}\n")
            out.write(f"filename: {filename}\n")
            out.write(f"path: {tsv_path}\n")
            out.write(f"exists: {tsv_path.exists()}\n")

            if not tsv_path.exists():
                missing += 1
                out.write("[WARN] Missing file on disk.\n\n")
                continue

            lines = _peek_lines(tsv_path, max_lines=args.max_lines)
            header_idx, header_cols = _detect_header(lines)
            out.write(f"peek_lines: {len(lines)}\n")
            out.write(f"header_line_index: {header_idx}\n")
            out.write(f"header_ncols: {len(header_cols)}\n")

            if header_cols:
                header_ok += 1
                out.write("header_cols:\n")
                for c in header_cols:
                    out.write(f"  - {c}\n")
            else:
                header_fail += 1
                out.write("[WARN] Could not detect header line.\n")

            summary = _count_summary_rows(lines)
            if summary:
                out.write("summary_rows_seen:\n")
                for k, v in summary.items():
                    out.write(f"  {k}: {v}\n")
            out.write("\n")

        out.write("\n" + "=" * 80 + "\n")
        out.write(f"header_ok={header_ok}\n")
        out.write(f"header_fail={header_fail}\n")
        out.write(f"missing={missing}\n")

    print(f"[OK] Wrote schema inspection: {out_txt}")
    print(f"[INFO] header_ok={header_ok} header_fail={header_fail} missing={missing}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
