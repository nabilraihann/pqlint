from __future__ import annotations

import argparse
import sys
from collections.abc import Iterator
from pathlib import Path

from .report import lint_path, to_json, to_text


def _expand(paths: list[str]) -> Iterator[Path]:
    for p in map(Path, paths):
        if p.is_dir():
            yield from sorted(p.rglob("*.parquet"))
        else:
            yield p


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="pqlint", description="Lint Parquet files using only their footer metadata."
    )
    parser.add_argument("paths", nargs="+", help="Parquet files or directories")
    parser.add_argument("--json", action="store_true", help="emit JSON instead of text")
    parser.add_argument(
        "--fail-under",
        type=int,
        metavar="N",
        help="exit with status 1 if any file scores below N",
    )
    args = parser.parse_args(argv)

    reports = [lint_path(p) for p in _expand(args.paths)]
    if not reports:
        print("pqlint: no parquet files found", file=sys.stderr)
        return 2
    print(to_json(reports) if args.json else to_text(reports))

    if any(r.error for r in reports):
        return 2
    if args.fail_under is not None and any(r.score < args.fail_under for r in reports):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
