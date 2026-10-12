from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from .models import FileReport
from .reader import read_local
from .rules import Config, lint


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
    report.findings, report.score = lint(info, cfg)
    return report


def to_json(reports: list[FileReport]) -> str:
    return json.dumps([asdict(r) for r in reports], indent=2, default=str)


def to_text(reports: list[FileReport]) -> str:
    lines: list[str] = []
    for r in reports:
        if r.error:
            lines.append(f"{r.path}: ERROR {r.error}")
            continue
        lines.append(
            f"{r.path}: score {r.score}/100 "
            f"({r.num_rows} rows, {r.num_row_groups} row groups, {r.size} bytes)"
        )
        for f in r.findings:
            cols = f" [{', '.join(f.columns)}]" if f.columns else ""
            lines.append(f"  {f.severity.value.upper():7} {f.rule}: {f.message}{cols}")
            lines.append(f"          -> {f.recommendation}")
        if not r.findings:
            lines.append("  no findings")
    return "\n".join(lines)
