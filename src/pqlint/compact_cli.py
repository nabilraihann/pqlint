from __future__ import annotations

import argparse
import gc
import statistics
import sys

import pyarrow as pa

from . import render
from . import suggest as suggestions
from .compact import (
    CompactError, check_compression, compact, make_plan, parse_size, parse_sort, plan_rows,
)
from .report import lint_path


AUTO = "auto"


def _auto_sort(args, st) -> list[tuple[str, str]] | None:
    """`--sort-by auto`: let suggest-sort pick the order (ascending) and show what it chose.

    Returns None when no order pays off, so the compaction runs unsorted.
    """

    def progress(msg: str) -> None:
        print(st(f"… {msg}", "dim"), file=sys.stderr)

    try:
        s = suggestions.suggest(args.inputs, compression=args.compression, progress=progress)
    except suggestions.SuggestError as exc:
        raise CompactError(f"--sort-by auto: {exc}") from exc
    # The allocator keeps the sample's memory after it is freed, and planning reads the
    # free memory from the OS: give it back first (measured: ~0.8 of 1.5 GiB on the real file).
    gc.collect()
    pa.default_memory_pool().release_unused()
    if s.steps:
        rows = [
            ("Order", ", ".join(s.best.keys)),
            ("Estimated", f"{render.human_size(s.best.est_bytes)} against "
                          f"{render.human_size(s.unsorted.est_bytes)} unsorted"),
        ]
    else:
        rows = [
            ("Order", "none"),
            ("Why", "no sort order shrinks the sample noticeably; compacting without a sort"),
        ]
    rows.append(("Chosen by", f"suggest-sort, from a sample of {s.sample_rows:,} of {s.rows:,} rows"))
    print(render.heading(st, "Auto sort"))
    print("\n".join(render.kv(st, rows)))
    print()
    return [(k, "ascending") for k in s.best.keys] or None


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
        "Use 'auto' alone to let suggest-sort choose the order for the smallest file "
        "(it reads a sample first, also with --dry-run; a column really named auto is "
        "written auto:asc).",
    )
    parser.add_argument(
        "--row-group-rows",
        type=int,
        metavar="N",
        help="rows per row group (default: sized to about 64 MiB encoded)",
    )
    parser.add_argument("--compression", default="zstd", help="codec (default: zstd)")
    parser.add_argument(
        "--compression-level",
        type=int,
        metavar="N",
        help="codec level for the output (default: the codec's own, 1 for zstd). zstd 9 made "
        "a 33.6M-row test file 14%% smaller (49.2 to 42.4 MiB) with no visible extra time. "
        "Not every codec has levels",
    )
    parser.add_argument("--overwrite", action="store_true", help="replace an existing output")
    parser.add_argument("--dry-run", action="store_true", help="show the plan and write nothing")
    parser.add_argument(
        "--force", action="store_true", help="sort even if the estimate exceeds available memory"
    )
    parser.add_argument(
        "--memory-limit",
        metavar="SIZE",
        help="memory the sort may plan for, e.g. 2G (default: available RAM). "
        "A sort that does not fit is split into buckets by the first sort column.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        metavar="N",
        help="buckets to sort at once when the sort is split into buckets "
        "(default: 1; fewer than asked if memory is short). No speed-up was reliably "
        "measured on the test machine, so measure before raising it",
    )
    parser.add_argument(
        "--color", choices=["auto", "always", "never"], default="auto", help="colorize output"
    )
    args = parser.parse_args(argv)
    if args.threads is not None and args.threads < 1:
        parser.error("--threads must be at least 1")
    st = render.Style(render.use_color(args.color))

    try:
        items = [item.strip() for item in (args.sort_by or "").split(",")]
        if AUTO in items and items != [AUTO]:
            raise CompactError(
                "--sort-by auto cannot be combined with column names; use it alone, or list the "
                "columns yourself (pqlint suggest-sort --start-with COLS fixes leading keys)"
            )
        if items == [AUTO]:
            sort_by = _auto_sort(args, st)
        else:
            sort_by = parse_sort(args.sort_by) if args.sort_by else None
        limit = parse_size(args.memory_limit) if args.memory_limit else None
        check_compression(args.compression, args.compression_level)
        plan = make_plan(args.inputs, sort_by, args.row_group_rows, limit, args.threads)
        print(render.heading(st, "Plan"))
        rows = plan_rows(plan)
        if not plan.fits_in_memory:
            rows = [
                (k, (v + "  (does not fit)" if k == "Peak memory" else v, ("red", "bold")))
                if k in ("Peak memory", "Problem") else (k, v)
                for k, v in rows
            ]
        print("\n".join(render.kv(st, rows)))
        if args.dry_run:
            return 0
        out = compact(
            plan, args.output, args.compression, args.overwrite, args.force, args.compression_level
        )
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
