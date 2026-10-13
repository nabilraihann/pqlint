from __future__ import annotations

import argparse
import statistics
import sys

from .compact import CompactError, compact, describe, make_plan, parse_sort
from .report import lint_path

MiB = 1024 * 1024


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="pqlint compact",
        description="Rewrite Parquet files into one file with larger row groups, "
        "optionally sorted. Inputs are never modified.",
    )
    parser.add_argument("inputs", nargs="+", help="Parquet files or directories (same schema)")
    parser.add_argument("-o", "--output", required=True, help="output file")
    parser.add_argument(
        "--sort-by",
        metavar="COLS",
        help="comma-separated columns, optionally with :desc, e.g. dc,store:desc. "
        "Sorting happens in memory.",
    )
    parser.add_argument(
        "--row-group-rows",
        type=int,
        metavar="N",
        help="rows per row group (default: sized to about 64 MiB encoded)",
    )
    parser.add_argument("--compression", default="zstd", help="codec (default: zstd)")
    parser.add_argument("--overwrite", action="store_true", help="replace an existing output")
    parser.add_argument("--dry-run", action="store_true", help="show the plan and write nothing")
    parser.add_argument(
        "--force", action="store_true", help="sort even if the estimate exceeds available memory"
    )
    args = parser.parse_args(argv)

    try:
        sort_by = parse_sort(args.sort_by) if args.sort_by else None
        plan = make_plan(args.inputs, sort_by, args.row_group_rows)
        print(describe(plan))
        if args.dry_run:
            return 0
        out = compact(plan, args.output, args.compression, args.overwrite, args.force)
    except CompactError as exc:
        print(f"pqlint compact: {exc}", file=sys.stderr)
        return 2

    before = [lint_path(p) for p in plan.inputs]
    after = lint_path(out)
    scores = [r.score for r in before if r.score is not None]
    print(f"\nWrote {out}")
    print(
        f"  row groups {plan.row_groups} -> {after.num_row_groups}, "
        f"size {plan.input_bytes / MiB:.1f} MiB -> {after.size / MiB:.1f} MiB"
    )
    if scores and after.score is not None:
        print(f"  score {statistics.fmean(scores):.0f} -> {after.score}")
    for f in after.findings:
        print(f"  {f.severity.value.upper():7} {f.rule}: {f.message}")
    return 0
