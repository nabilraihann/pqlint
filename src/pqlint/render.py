"""Plain-text tables for the CLI: aligned columns, wrapping, optional color.

No third-party dependency; the needs are small (a few table shapes). Color is
applied after padding so escape codes never distort column widths.
"""

from __future__ import annotations

import os
import shutil
import sys
import textwrap
from collections.abc import Sequence

from .fmt import human_size, plural
from .models import FileReport, Finding, Severity
from .summary import DatasetSummary, summarize

_ANSI = {"bold": "1", "dim": "2", "red": "31", "green": "32", "yellow": "33", "cyan": "36"}

Styled = tuple[str, tuple[str, ...]]  # (text, style names)
Cell = str | Styled | list[Styled]  # a list is several paragraphs (last column only)


class Style:
    def __init__(self, color: bool = False, unicode: bool | None = None) -> None:
        self.color = color
        self.unicode = _stdout_is_unicode() if unicode is None else unicode

    def __call__(self, text: str, *names: str) -> str:
        if not self.color or not names or not text.strip():
            return text
        return f"\x1b[{';'.join(_ANSI[n] for n in names)}m{text}\x1b[0m"

    @property
    def arrow(self) -> str:
        return "→" if self.unicode else "->"

    @property
    def dot(self) -> str:
        return "·" if self.unicode else "|"

    @property
    def rule(self) -> str:
        return "─" if self.unicode else "-"


def _stdout_is_unicode() -> bool:
    return "utf" in (getattr(sys.stdout, "encoding", None) or "").lower()


def use_color(mode: str = "auto", stream=None) -> bool:
    if mode == "always":
        return True
    if mode == "never":
        return False
    stream = stream or sys.stdout
    return bool(
        stream.isatty() and "NO_COLOR" not in os.environ and os.environ.get("TERM") != "dumb"
    )


def term_width() -> int:
    return max(60, min(shutil.get_terminal_size((100, 24)).columns, 140))


def score_style(score: int | float | None) -> tuple[str, ...]:
    if score is None:
        return ()
    return ("green",) if score >= 90 else ("yellow",) if score >= 70 else ("red",)


def _paragraphs(cell: Cell) -> list[Styled]:
    if isinstance(cell, str):
        return [(cell, ())]
    if isinstance(cell, tuple):
        return [cell]
    return list(cell)


def _wrap(text: str, width: int, arrow: str) -> list[str]:
    out: list[str] = []
    for para in text.split("\n"):
        hang = "  " if para.startswith(arrow) else ""
        out += textwrap.wrap(
            para, width, subsequent_indent=hang, break_long_words=False, break_on_hyphens=False
        ) or [""]
    return out


def table(
    st: Style,
    headers: Sequence[str],
    rows: Sequence[Sequence[Cell]],
    aligns: str | None = None,
    width: int | None = None,
    indent: int = 2,
) -> list[str]:
    """Aligned table whose last column wraps to the terminal width.

    `aligns` has one '<' or '>' per column; missing entries default to '<'.
    """
    width = width or term_width()
    n = len(headers)
    aligns = (aligns or "").ljust(n, "<")
    plain = [[_paragraphs(c)[0][0] if i < n - 1 else "" for i, c in enumerate(r)] for r in rows]
    widths = [max([len(headers[i])] + [len(p[i]) for p in plain]) for i in range(n - 1)]
    gaps = 2 * (n - 1)
    avail = max(20, width - indent - sum(widths) - gaps)
    pad = " " * indent

    wrapped: list[list[list[Styled]]] = []  # per row: lines of (text, styles)
    for r in rows:
        lines: list[Styled] = []
        for text, styles in _paragraphs(r[-1]):
            lines += [(line, styles) for line in _wrap(text, avail, st.arrow)]
        wrapped.append(lines)

    last_w = max([len(headers[-1])] + [len(t) for ls in wrapped for t, _ in ls])
    last_w = min(last_w, avail) if avail >= len(headers[-1]) else len(headers[-1])

    def join(cols: list[str]) -> str:
        return pad + "  ".join(cols).rstrip()

    def fit(text: str, i: int, w: int) -> str:
        return text.rjust(w) if aligns[i] == ">" else text.ljust(w)

    head = [st(fit(h.upper(), i, widths[i]), "dim") for i, h in enumerate(headers[:-1])]
    last_head = headers[-1].upper()
    head.append(st(last_head.rjust(last_w) if aligns[-1] == ">" else last_head, "dim"))
    out = [join(head)]
    out.append(join([st.rule * w for w in widths] + [st.rule * last_w]))
    for r, lines, texts in zip(rows, wrapped, plain):
        for k, (text, styles) in enumerate(lines or [("", ())]):
            cols = []
            for i in range(n - 1):
                if k == 0:
                    _, cell_styles = _paragraphs(r[i])[0]
                    cols.append(st(fit(texts[i], i, widths[i]), *cell_styles))
                else:
                    cols.append(" " * widths[i])
            cols.append(st(fit(text, n - 1, last_w) if aligns[-1] == ">" else text, *styles))
            out.append(join(cols))
    return out


def kv(st: Style, rows: Sequence[tuple[str, str | Styled]], indent: int = 2) -> list[str]:
    w = max(len(k) for k, _ in rows)
    out = []
    for k, v in rows:
        text, styles = (v, ()) if isinstance(v, str) else v
        out.append(" " * indent + st(k.ljust(w), "dim") + "  " + st(text, *styles))
    return out


def heading(st: Style, text: str) -> str:
    return st(text, "bold")


_SEVERITY = {
    Severity.WARNING: ("WARNING", ("yellow", "bold")),
    Severity.INFO: ("INFO", ("cyan",)),
    Severity.HINT: ("HINT", ("green",)),
}
_RANK = {Severity.WARNING: 0, Severity.INFO: 1, Severity.HINT: 2}


def findings_table(st: Style, findings: Sequence[Finding], width: int | None = None) -> list[str]:
    rows: list[Sequence[Cell]] = []
    for f in sorted(findings, key=lambda f: _RANK[f.severity]):
        label, styles = _SEVERITY[f.severity]
        body: list[Styled] = [(f.message, ())]
        if f.details:
            listing = ", ".join(f"{c} {v:.0%}" for c, v in f.details.items())
            body.append((f"overlap: {listing}", ("dim",)))
        elif f.columns:
            body.append((f"columns: {', '.join(f.columns)}", ("dim",)))
        body.append((f"{st.arrow} {f.recommendation}", ("dim",)))
        rows.append([(label, styles), (f.rule, ("bold",)), body])
    return table(st, ["level", "rule", "finding"], rows, width=width)


def _metrics(st: Style, r: FileReport) -> str:
    sep = f" {st.dot} "
    return sep.join(
        [plural(r.num_rows, "row"), plural(r.num_row_groups, "row group"), human_size(r.size)]
    )


def file_block(st: Style, r: FileReport, width: int | None = None) -> list[str]:
    lines = [heading(st, r.path)]
    if r.error:
        return lines + [f"  {st('ERROR', 'red', 'bold')}  {r.error}"]
    lines.append(
        f"  {st('Score', 'dim')} {st(f'{r.score}/100', *score_style(r.score), 'bold')}"
        f"  {st.dot}  {_metrics(st, r)}"
    )
    if r.created_by:
        lines.append(f"  {st('Writer', 'dim')} {r.created_by}")
    lines.append("")
    if r.findings:
        lines += findings_table(st, r.findings, width)
    else:
        lines.append("  " + st("No findings.", "green"))
    return lines


def overview(st: Style, reports: Sequence[FileReport], width: int | None = None) -> list[str]:
    """One row per file; the rules that fired sit under the path, so details can stay hidden."""
    rows: list[Sequence[Cell]] = []
    for r in reports:
        if r.error:
            rows.append(
                [("ERR", ("red", "bold")), "", "", "", [(r.path, ()), (r.error, ("red",))]]
            )
            continue
        cell: list[Styled] = [(r.path, ())]
        if r.findings:
            cell.append((", ".join(f.rule for f in r.findings), ("dim",)))
        rows.append(
            [
                (str(r.score), score_style(r.score)),
                f"{r.num_rows:,}",
                str(r.num_row_groups),
                human_size(r.size),
                cell,
            ]
        )
    return table(st, ["score", "rows", "row groups", "size", "file"], rows, ">>>>", width)


def summary_block(st: Style, s: DatasetSummary, width: int | None = None) -> list[str]:
    files = f"{s.files:,}" + (f"  ({s.errors} unreadable)" if s.errors else "")
    lines = [heading(st, "Dataset summary")]
    if s.mean_score is None:
        return lines + kv(st, [("Files", (files, ("red",) if s.errors else ()))])
    sep = f" {st.dot} "
    lines += kv(
        st,
        [
            ("Files", (files, ("yellow",) if s.errors else ())),
            ("Rows", f"{s.total_rows:,}"),
            ("Total size", human_size(s.total_bytes)),
            (
                "File size",
                sep.join(
                    f"{k} {human_size(v)}"
                    for k, v in (("min", s.min_bytes), ("median", s.median_bytes), ("max", s.max_bytes))
                ),
            ),
            ("Mean score", (f"{s.mean_score} / 100", score_style(s.mean_score))),
        ],
    )
    ok = s.files - s.errors
    if s.files_per_rule:
        lines.append("")
        rows = [
            [(rule, ("bold",)), f"{n} / {ok}", f"{n / ok:.0%}"] for rule, n in s.files_per_rule.items()
        ]
        lines += _rule_table(st, rows, width)
    if s.worst_files:
        lines.append("")
        rows = [[(str(score), score_style(score)), (path, ())] for path, score in s.worst_files]
        lines += table(st, ["score", "lowest-scoring files"], rows, ">", width)
    if s.partitions:
        lines.append("")
        rows = [[(k, ("bold",)), str(v)] for k, v in s.partitions.items()]
        lines += table(st, ["partition key", "distinct values"], rows, "<", width)
    return lines


def _rule_table(st: Style, rows: Sequence[Sequence[Cell]], width: int | None) -> list[str]:
    # The wrapping column goes last, so put the percentage there.
    return table(st, ["rule", "files affected", "share"], rows, "<>", width)


def to_text(
    reports: Sequence[FileReport],
    summary_only: bool = False,
    st: Style | None = None,
    width: int | None = None,
    details: bool = False,
) -> str:
    st = st or Style(False)
    blocks: list[list[str]] = []
    many = len(reports) > 1
    if not summary_only:
        if many:
            blocks.append([heading(st, f"Files ({len(reports)})")] + overview(st, reports, width))
            if details:
                blocks += [file_block(st, r, width) for r in reports if r.error or r.findings]
        else:
            blocks += [file_block(st, r, width) for r in reports]
    if many or summary_only:
        blocks.append(summary_block(st, summarize(list(reports)), width))
    return "\n\n".join("\n".join(b) for b in blocks)
