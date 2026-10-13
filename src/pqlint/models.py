from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Severity(str, Enum):
    HINT = "hint"  # a suggestion, not a defect: never lowers the score
    INFO = "info"
    WARNING = "warning"


@dataclass(frozen=True)
class Finding:
    rule: str
    severity: Severity
    message: str
    recommendation: str
    columns: tuple[str, ...] = ()
    # Optional per-column numbers, e.g. overlap fraction for sort-candidates
    details: dict[str, float] = field(default_factory=dict)


@dataclass
class FileReport:
    path: str
    size: int | None = None
    num_rows: int | None = None
    num_row_groups: int | None = None
    created_by: str | None = None
    score: int | None = None
    findings: list[Finding] = field(default_factory=list)
    error: str | None = None
