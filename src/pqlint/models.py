from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"


@dataclass(frozen=True)
class Finding:
    rule: str
    severity: Severity
    message: str
    recommendation: str
    columns: tuple[str, ...] = ()


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
