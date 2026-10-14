from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from .models import FileReport
from .reader import read_local
from .rules import Config, lint
from .summary import summarize


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
