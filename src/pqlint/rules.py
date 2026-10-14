from __future__ import annotations

from bisect import bisect_left
from collections.abc import Callable, Iterator
from dataclasses import dataclass

from .fmt import human_size
from .models import Finding, Severity
from .reader import FileInfo

MiB = 1024 * 1024


@dataclass(frozen=True)
class Config:
    small_file_bytes: int = 8 * MiB
    # Parquet's row group total_byte_size is the uncompressed size.
    row_group_min_bytes: int = 32 * MiB
    row_group_max_bytes: int = 1024 * MiB
    # A row group is only "small" if it is also short: narrow, repetitive data
    # encodes to few bytes even with millions of rows.
    row_group_min_rows: int = 1_000_000
    # Default dictionary page limit of the common writers is 1 MiB.
    dictionary_page_limit_bytes: int = 1 * MiB
    # Sort suggestions: need a few row groups for overlap to mean anything.
    sort_min_row_groups: int = 4
    sort_overlap_threshold: float = 0.5
    sort_max_suggestions: int = 5
    warning_penalty: int = 10
    info_penalty: int = 3


Rule = Callable[[FileInfo, Config], Iterator[Finding]]


def _chunks(info: FileInfo) -> Iterator[tuple[int, str, object]]:
    md = info.metadata
    for rg_idx in range(md.num_row_groups):
        rg = md.row_group(rg_idx)
        for col_idx in range(rg.num_columns):
            col = rg.column(col_idx)
            yield rg_idx, col.path_in_schema, col


def _null_typed(info: FileInfo) -> set[str]:
    """Columns with the Parquet Null logical type: always empty, never any stats."""
    schema = info.metadata.schema
    return {
        schema.column(i).path
        for i in range(len(schema))
        if str(schema.column(i).logical_type) == "Null"
    }


def _is_small(info: FileInfo, cfg: Config) -> bool:
    return info.size < cfg.small_file_bytes


def small_file(info: FileInfo, cfg: Config) -> Iterator[Finding]:
    if _is_small(info, cfg):
        yield Finding(
            "small-file",
            Severity.WARNING,
            f"File is {human_size(info.size)}, below {human_size(cfg.small_file_bytes)}.",
            "Compact small files into larger ones to cut per-file open and metadata overhead.",
        )


def row_group_size(info: FileInfo, cfg: Config) -> Iterator[Finding]:
    md = info.metadata
    groups = [md.row_group(i) for i in range(md.num_row_groups)]
    sizes = [g.total_byte_size for g in groups]
    # The last row group is naturally the remainder, and a lone row group of a
    # small file is already covered by small-file.
    judged = groups[:-1] if len(groups) > 1 else ([] if _is_small(info, cfg) else groups)
    small = [
        g.total_byte_size
        for g in judged
        if g.total_byte_size < cfg.row_group_min_bytes and g.num_rows < cfg.row_group_min_rows
    ]
    if small:
        yield Finding(
            "row-group-small",
            Severity.WARNING,
            f"{len(small)} of {len(groups)} row groups are under "
            f"{human_size(cfg.row_group_min_bytes)} uncompressed and "
            f"{cfg.row_group_min_rows:,} rows (smallest {human_size(min(small))}).",
            "Raise the writer's row group size; tiny row groups weaken compression and stats.",
        )
    large = [s for s in sizes if s > cfg.row_group_max_bytes]
    if large:
        yield Finding(
            "row-group-large",
            Severity.WARNING,
            f"{len(large)} of {len(groups)} row groups exceed "
            f"{human_size(cfg.row_group_max_bytes)} uncompressed "
            f"(largest {human_size(max(large))}).",
            "Lower the row group size; huge row groups need a lot of memory and limit parallelism.",
        )


def missing_stats(info: FileInfo, cfg: Config) -> Iterator[Finding]:
    if info.metadata.num_rows == 0:  # writers emit no stats for empty files
        return
    null_typed = _null_typed(info)
    cols = sorted(
        {n for _, n, c in _chunks(info) if not c.is_stats_set and n not in null_typed}
    )
    if cols:
        yield Finding(
            "missing-stats",
            Severity.WARNING,
            f"{len(cols)} column(s) have no min/max statistics in at least one row group.",
            "Enable statistics in the writer so engines can skip row groups.",
            tuple(cols),
        )


def uncompressed(info: FileInfo, cfg: Config) -> Iterator[Finding]:
    cols = sorted({n for _, n, c in _chunks(info) if c.compression == "UNCOMPRESSED"})
    if cols:
        yield Finding(
            "uncompressed",
            Severity.WARNING,
            f"{len(cols)} column(s) are stored without compression.",
            "Use a codec such as zstd or snappy.",
            tuple(cols),
        )


def dictionary_ineffective(info: FileInfo, cfg: Config) -> Iterator[Finding]:
    """Heuristic: footer metadata cannot tell whether a writer fell back to PLAIN.

    An on-disk dictionary page at or above the writer's dictionary limit means
    the dictionary filled up, so a high-cardinality column likely stopped
    benefiting from dictionary encoding. This is conservative: compression can
    shrink a full dictionary below the limit, and fallback after a small
    dictionary is not caught. A size ratio was tried and flagged ordinary
    unique columns such as ids.
    """
    cols = {
        name
        for _, name, c in _chunks(info)
        if c.has_dictionary_page
        and c.data_page_offset - c.dictionary_page_offset >= cfg.dictionary_page_limit_bytes
    }
    if cols:
        yield Finding(
            "dictionary-ineffective",
            Severity.INFO,
            f"{len(cols)} column(s) have a dictionary page of "
            f"{human_size(cfg.dictionary_page_limit_bytes)} or more.",
            "Likely high-cardinality; consider disabling dictionary encoding for them.",
            tuple(sorted(cols)),
        )


def all_null_column(info: FileInfo, cfg: Config) -> Iterator[Finding]:
    md = info.metadata
    if md.num_rows == 0:
        return
    # In repeated columns an empty list counts as a null at the leaf, so
    # null_count == num_values does not mean the column holds no data.
    repeated = {
        md.schema.column(i).path
        for i in range(len(md.schema))
        if md.schema.column(i).max_repetition_level > 0
    }
    null_typed = _null_typed(info) - repeated
    seen: dict[str, bool] = {}
    for _, name, c in _chunks(info):
        if name in repeated or name in null_typed:
            continue
        all_null = (
            c.is_stats_set
            and c.statistics.has_null_count
            and c.statistics.null_count == c.num_values
        )
        seen[name] = seen.get(name, True) and bool(all_null)
    cols = sorted(null_typed | {n for n, v in seen.items() if v})
    if cols:
        yield Finding(
            "all-null-column",
            Severity.INFO,
            f"{len(cols)} column(s) contain only nulls.",
            "Drop the column or check the upstream pipeline.",
            tuple(cols),
        )


def _overlap(ranges: list[tuple]) -> float:
    """Fraction of row group pairs whose min/max ranges intersect.

    0 = all ranges disjoint (clustered), 1 = every pair overlaps. Counts the
    disjoint pairs instead: j is disjoint from every range whose max lies
    below j's min, found by bisecting the sorted maxima. Rank-based, so it
    works for strings and dates, not only numbers. Ranges that merely touch
    at a shared value still count as overlapping.
    """
    n = len(ranges)
    maxs = sorted(hi for _, hi in ranges)
    disjoint = sum(bisect_left(maxs, lo) for lo, _ in ranges)
    return 1 - disjoint / (n * (n - 1) / 2)


def _is_nan(v: object) -> bool:
    return isinstance(v, float) and v != v


def sort_candidates(info: FileInfo, cfg: Config) -> Iterator[Finding]:
    """Suggest columns whose row group ranges overlap heavily.

    Footer stats show how values are spread across row groups, not which
    columns queries filter on, so this is a ranked list of candidates only.
    """
    md = info.metadata
    if md.num_rows == 0 or md.num_row_groups < cfg.sort_min_row_groups:
        return
    skip = {md.schema.column(i).path for i in range(len(md.schema))
            if md.schema.column(i).max_repetition_level > 0}
    skip |= _null_typed(info)
    if md.num_row_groups:
        skip |= {md.schema.column(sc.column_index).path
                 for sc in md.row_group(0).sorting_columns}

    ranges: dict[str, list[tuple] | None] = {}
    for _, name, c in _chunks(info):
        if name in skip or c.physical_type == "BOOLEAN":
            continue
        st = c.statistics if c.is_stats_set else None
        if st is None or not st.has_min_max or _is_nan(st.min) or _is_nan(st.max):
            ranges[name] = None  # a gap in stats makes the column unjudgeable
        elif ranges.get(name, []) is not None:
            ranges.setdefault(name, []).append((st.min, st.max))

    scored = []
    for name, rs in ranges.items():
        if rs is None or len(rs) < cfg.sort_min_row_groups:
            continue
        if all(lo == hi == rs[0][0] for lo, hi in rs):
            continue  # one constant value: nothing to gain from sorting
        try:
            score = _overlap(rs)
        except TypeError:  # values that cannot be ordered against each other
            continue
        if score >= cfg.sort_overlap_threshold:
            scored.append((score, name))
    if not scored:
        return
    scored.sort(key=lambda t: (-t[0], t[1]))
    top = scored[: cfg.sort_max_suggestions]
    yield Finding(
        "sort-candidates",
        Severity.HINT,
        f"Row group min/max ranges overlap heavily in {len(top)} column(s), "
        "most overlapping first.",
        "Sort or cluster the data by the column(s) you filter on most; engines can then "
        "skip row groups. Footer stats cannot tell which column that is.",
        tuple(n for _, n in top),
        {n: round(s, 3) for s, n in top},
    )


DEFAULT_RULES: tuple[Rule, ...] = (
    small_file,
    row_group_size,
    missing_stats,
    uncompressed,
    dictionary_ineffective,
    all_null_column,
    sort_candidates,
)


def lint(info: FileInfo, cfg: Config = Config(), rules: tuple[Rule, ...] = DEFAULT_RULES):
    """Run the rules and return (findings, score)."""
    findings = [f for rule in rules for f in rule(info, cfg)]
    weights = {
        Severity.WARNING: cfg.warning_penalty,
        Severity.INFO: cfg.info_penalty,
        Severity.HINT: 0,
    }
    penalty = sum(weights[f.severity] for f in findings)
    return findings, max(0, 100 - penalty)
