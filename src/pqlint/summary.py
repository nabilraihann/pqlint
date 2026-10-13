from __future__ import annotations

import statistics
from collections import Counter
from dataclasses import dataclass, field
from pathlib import PurePath

from .models import FileReport

WORST_FILES = 5


@dataclass
class DatasetSummary:
    files: int = 0
    errors: int = 0
    total_rows: int = 0
    total_bytes: int = 0
    min_bytes: int | None = None
    median_bytes: int | None = None
    max_bytes: int | None = None
    mean_score: float | None = None
    # rule name -> number of files with at least one finding of that rule
    files_per_rule: dict[str, int] = field(default_factory=dict)
    worst_files: list[tuple[str, int]] = field(default_factory=list)
    # Hive-style partition key -> number of distinct values seen in paths
    partitions: dict[str, int] = field(default_factory=dict)


def partition_values(path: str) -> dict[str, str]:
    """Hive-style `key=value` directory segments of a path (file name excluded)."""
    parts = PurePath(path).parts[:-1]
    return dict(p.split("=", 1) for p in parts if "=" in p and not p.startswith("="))


def summarize(reports: list[FileReport]) -> DatasetSummary:
    ok = [r for r in reports if r.error is None]
    s = DatasetSummary(files=len(reports), errors=len(reports) - len(ok))
    if not ok:
        return s

    sizes = [r.size for r in ok]
    s.total_rows = sum(r.num_rows for r in ok)
    s.total_bytes = sum(sizes)
    s.min_bytes, s.max_bytes = min(sizes), max(sizes)
    s.median_bytes = int(statistics.median(sizes))
    s.mean_score = round(statistics.fmean(r.score for r in ok), 1)

    counts = Counter(rule for r in ok for rule in {f.rule for f in r.findings})
    s.files_per_rule = dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))

    ranked = sorted(ok, key=lambda r: (r.score, r.path))
    s.worst_files = [(r.path, r.score) for r in ranked[:WORST_FILES] if r.score < 100]

    seen: dict[str, set[str]] = {}
    for r in ok:
        for key, value in partition_values(r.path).items():
            seen.setdefault(key, set()).add(value)
    s.partitions = {k: len(v) for k, v in sorted(seen.items())}
    return s
