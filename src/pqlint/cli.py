from __future__ import annotations

import argparse
import sys

from .reader import expand_paths
from .render import Style, to_text, use_color
from .report import lint_path, to_json


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == "compact":
        from .compact_cli import main as compact_main

        return compact_main(argv[1:])
    if argv and argv[0] == "suggest-sort":
        from .suggest import main as suggest_main

        return suggest_main(argv[1:])
    parser = argparse.ArgumentParser(
        prog="pqlint",
        description="Lint Parquet files using only their footer metadata.",
        epilog="Subcommands: `pqlint compact --help` rewrites files with larger row groups and an "
        "optional sort; `pqlint suggest-sort --help` measures which sort order makes a file smallest.",
    )
    parser.add_argument("paths", nargs="+", help="Parquet files or directories")
    parser.add_argument("--json", action="store_true", help="emit JSON instead of text")
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="print only the dataset summary, not per-file findings",
    )
    parser.add_argument(
        "--details",
        action="store_true",
        help="with several files, also print the findings table of every file "
        "(a single file always shows it)",
    )
    parser.add_argument(
        "--color",
        choices=["auto", "always", "never"],
        default="auto",
        help="colorize text output (default: auto = only on a terminal, honoring NO_COLOR)",
    )
    parser.add_argument(
        "--fail-under",
        type=int,
        metavar="N",
        help="exit with status 1 if any file scores below N",
    )
    args = parser.parse_args(argv)

    reports = [lint_path(p) for p in expand_paths(args.paths)]
    if not reports:
        print("pqlint: no parquet files found", file=sys.stderr)
        return 2
    if args.json:
        print(to_json(reports, args.summary_only))
    else:
        print(to_text(reports, args.summary_only, Style(use_color(args.color)), details=args.details))

    if any(r.error for r in reports):
        return 2
    if args.fail_under is not None and any(r.score < args.fail_under for r in reports):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
