from __future__ import annotations

import argparse
import statistics
import sys

from . import render
from .compact import CompactError, compact, make_plan, parse_sort, plan_rows
from .report import lint_path


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
    parser.add_argument(
        "--color", choices=["auto", "always", "never"], default="auto", help="colorize output"
    )
    args = parser.parse_args(argv)
    st = render.Style(render.use_color(args.color))

    try:
        sort_by = parse_sort(args.sort_by) if args.sort_by else None
        plan = make_plan(args.inputs, sort_by, args.row_group_rows)
        print(render.heading(st, "Plan"))
        rows = plan_rows(plan)
        if not plan.fits_in_memory:
            rows[-1] = (rows[-1][0], (rows[-1][1] + "  (does not fit)", ("red", "bold")))
        print("\n".join(render.kv(st, rows)))
        if args.dry_run:
            return 0
        out = compact(plan, args.output, args.compression, args.overwrite, args.force)
    except CompactError as exc:
        print(f"{st('pqlint compact:', 'red', 'bold')} {exc}", file=sys.stderr)
        return 2

    before = [lint_path(p) for p in plan.inputs]
    after = lint_path(out)
    scores = [r.score for r in before if r.score is not None]
    before_score = round(statistics.fmean(scores)) if scores else None
    print()
    print(f"{render.heading(st, 'Wrote')} {out}")
    print()
    print(
        "\n".join(
            render.table(
                st,
                ["", "before", "after"],
                [
                    ["Row groups", str(plan.row_groups), str(after.num_row_groups)],
                    [
                        "Size",
                        render.human_size(plan.input_bytes),
                        render.human_size(after.size),
                    ],
                    [
                        "Score",
                        (str(before_score), render.score_style(before_score)),
                        (str(after.score), (*render.score_style(after.score), "bold")),
                    ],
                ],
                "<>>",
            )
        )
    )
    if after.findings:
        print()
        print("\n".join(render.findings_table(st, after.findings)))
    return 0
