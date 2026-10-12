from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass

from .models import Finding, Severity
from .reader import FileInfo

MiB = 1024 * 1024


@dataclass(frozen=True)
class Config:
    small_file_bytes: int = 8 * MiB
    # Parquet's row group total_byte_size is the uncompressed size.
    row_group_min_bytes: int = 32 * MiB
    row_group_max_bytes: int = 1024 * MiB
    # Default dictionary page limit of the common writers is 1 MiB.
    dictionary_page_limit_bytes: int = 1 * MiB
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


def _is_small(info: FileInfo, cfg: Config) -> bool:
    return info.size < cfg.small_file_bytes


def small_file(info: FileInfo, cfg: Config) -> Iterator[Finding]:
    if _is_small(info, cfg):
        yield Finding(
            "small-file",
            Severity.WARNING,
            f"File is {info.size / MiB:.2f} MiB, below {cfg.small_file_bytes / MiB:.0f} MiB.",
            "Compact small files into larger ones to cut per-file open and metadata overhead.",
        )


def row_group_size(info: FileInfo, cfg: Config) -> Iterator[Finding]:
    md = info.metadata
    sizes = [md.row_group(i).total_byte_size for i in range(md.num_row_groups)]
    # The last row group is naturally the remainder, and a lone row group of a
    # small file is already covered by small-file.
    judged = sizes[:-1] if len(sizes) > 1 else ([] if _is_small(info, cfg) else sizes)
    small = [s for s in judged if s < cfg.row_group_min_bytes]
    if small:
        yield Finding(
            "row-group-small",
            Severity.WARNING,
            f"{len(small)} of {len(sizes)} row groups are under "
            f"{cfg.row_group_min_bytes / MiB:.0f} MiB uncompressed "
            f"(smallest {min(small) / MiB:.2f} MiB).",
            "Raise the writer's row group size; tiny row groups weaken compression and stats.",
        )
    large = [s for s in sizes if s > cfg.row_group_max_bytes]
    if large:
        yield Finding(
            "row-group-large",
            Severity.WARNING,
            f"{len(large)} of {len(sizes)} row groups exceed "
            f"{cfg.row_group_max_bytes / MiB:.0f} MiB uncompressed "
            f"(largest {max(large) / MiB:.0f} MiB).",
            "Lower the row group size; huge row groups need a lot of memory and limit parallelism.",
        )


def missing_stats(info: FileInfo, cfg: Config) -> Iterator[Finding]:
    cols = sorted({name for _, name, c in _chunks(info) if not c.is_stats_set})
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
            f"{cfg.dictionary_page_limit_bytes / MiB:.0f} MiB or more.",
            "Likely high-cardinality; consider disabling dictionary encoding for them.",
            tuple(sorted(cols)),
        )


def all_null_column(info: FileInfo, cfg: Config) -> Iterator[Finding]:
    md = info.metadata
    if md.num_rows == 0:
        return
    seen: dict[str, bool] = {}
    for _, name, c in _chunks(info):
        all_null = c.is_stats_set and c.statistics.null_count == c.num_values
        seen[name] = seen.get(name, True) and bool(all_null)
    cols = sorted(n for n, v in seen.items() if v)
    if cols:
        yield Finding(
            "all-null-column",
            Severity.INFO,
            f"{len(cols)} column(s) contain only nulls.",
            "Drop the column or check the upstream pipeline.",
            tuple(cols),
        )


DEFAULT_RULES: tuple[Rule, ...] = (
    small_file,
    row_group_size,
    missing_stats,
    uncompressed,
    dictionary_ineffective,
    all_null_column,
)


def lint(info: FileInfo, cfg: Config = Config(), rules: tuple[Rule, ...] = DEFAULT_RULES):
    """Run the rules and return (findings, score)."""
    findings = [f for rule in rules for f in rule(info, cfg)]
    penalty = sum(
        cfg.warning_penalty if f.severity is Severity.WARNING else cfg.info_penalty
        for f in findings
    )
    return findings, max(0, 100 - penalty)
