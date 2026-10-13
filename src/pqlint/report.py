from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from .models import FileReport
from .reader import read_local
from .rules import Config, lint
from .summary import DatasetSummary, summarize


def lint_path(path: str | Path, cfg: Config = Config()) -> FileReport:
    """Lint one local file; failures are captured in the report, not raised."""
    report = FileReport(path=str(path))
    try:
        info = read_local(path)
    except Exception as exc:  # unreadable or not a Parquet file
        report.error = f"{type(exc).__name__}: {exc}"
        return report
    md = info.metadata
    report.size = info.size
    report.num_rows = md.num_rows
    report.num_row_groups = md.num_row_groups
    report.created_by = md.created_by
    try:
        report.findings, report.score = lint(info, cfg)
    except Exception as exc:  # a rule tripped on unusual metadata
        report.error = f"rule failed: {type(exc).__name__}: {exc}"
    return report


def to_json(reports: list[FileReport], summary_only: bool = False) -> str:
    out: dict = {"summary": asdict(summarize(reports))}
    if not summary_only:
        out["files"] = [asdict(r) for r in reports]
    return json.dumps(out, indent=2, default=str)


def _mib(n: int) -> str:
    return f"{n / (1024 * 1024):.2f} MiB"


def summary_text(s: DatasetSummary) -> str:
    lines = [f"Dataset summary: {s.files} file(s)" + (f", {s.errors} unreadable" if s.errors else "")]
    if s.mean_score is None:
        return "\n".join(lines)
    lines += [
        f"  rows: {s.total_rows:,}   size: {_mib(s.total_bytes)}",
        f"  file size min/median/max: {_mib(s.min_bytes)} / {_mib(s.median_bytes)} / {_mib(s.max_bytes)}",
        f"  mean score: {s.mean_score}/100",
    ]
    if s.files_per_rule:
        lines.append("  files affected per rule:")
        lines += [f"    {rule}: {n}/{s.files - s.errors}" for rule, n in s.files_per_rule.items()]
    if s.worst_files:
        lines.append("  lowest scores:")
        lines += [f"    {score:3}  {path}" for path, score in s.worst_files]
    if s.partitions:
        lines.append("  partition keys (distinct values): " + ", ".join(
            f"{k}={n}" for k, n in s.partitions.items()
        ))
    return "\n".join(lines)


def to_text(reports: list[FileReport], summary_only: bool = False) -> str:
    lines: list[str] = []
    for r in [] if summary_only else reports:
        if r.error:
            lines.append(f"{r.path}: ERROR {r.error}")
            continue
        lines.append(
            f"{r.path}: score {r.score}/100 "
            f"({r.num_rows} rows, {r.num_row_groups} row groups, {r.size} bytes)"
        )
        for f in r.findings:
            if f.details:
                cols = " [" + ", ".join(f"{c} {v:.0%}" for c, v in f.details.items()) + "]"
            else:
                cols = f" [{', '.join(f.columns)}]" if f.columns else ""
            lines.append(f"  {f.severity.value.upper():7} {f.rule}: {f.message}{cols}")
            lines.append(f"          -> {f.recommendation}")
        if not r.findings:
            lines.append("  no findings")
    if len(reports) > 1 or summary_only:
        if lines:
            lines.append("")
        lines.append(summary_text(summarize(reports)))
    return "\n".join(lines)
