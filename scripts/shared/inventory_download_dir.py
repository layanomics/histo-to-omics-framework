# scripts/shared/inventory_download_dir.py
from __future__ import annotations

import argparse
import time
from pathlib import Path
from collections import Counter


def count_files(root: Path) -> int:
    return sum(1 for p in root.rglob("*") if p.is_file())


def extension_counts(root: Path) -> list[tuple[str, int]]:
    c = Counter()
    for p in root.rglob("*"):
        if p.is_file():
            ext = p.suffix.lower() if p.suffix else "<no_ext>"
            c[ext] += 1
    return c.most_common()


def main() -> int:
    ap = argparse.ArgumentParser(description="Inventory a download directory (counts + extensions + examples).")
    ap.add_argument("--root", required=True, help="Download root directory to inventory.")
    ap.add_argument("--out_txt", required=True, help="Where to write the inventory report (.txt).")
    ap.add_argument("--examples", type=int, default=5, help="How many example .tsv paths to print.")
    args = ap.parse_args()

    root = Path(args.root)
    out = Path(args.out_txt)
    out.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    lines.append(f"TIMESTAMP: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"ROOT: {root.resolve()}")
    lines.append(f"EXISTS: {root.exists()}")

    if root.exists():
        lines.append(f"TOTAL_FILES: {count_files(root)}")
        lines.append("EXT_COUNTS:")
        for ext, n in extension_counts(root):
            lines.append(f"  {ext}: {n}")

        tsv_files = sorted(root.rglob("*.tsv"))
        lines.append(f"TSV_COUNT: {len(tsv_files)}")
        if tsv_files:
            lines.append("TSV_EXAMPLES:")
            for p in tsv_files[: max(0, args.examples)]:
                lines.append(f"  {p}")

    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"[OK] wrote inventory report -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
