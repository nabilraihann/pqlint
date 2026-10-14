import re

import pytest

from pqlint import render
from pqlint.cli import main
from pqlint.fmt import human_size, plural
from pqlint.models import FileReport, Finding, Severity
from pqlint.render import Style, to_text

ANSI = re.compile(r"\x1b\[[0-9;]*m")


def finding(rule="r", sev=Severity.WARNING, msg="m " * 40, rec="do this " * 12, **kw):
    return Finding(rule, sev, msg, rec, **kw)


def report(path="a.parquet", score=80, findings=(), **kw):
    return FileReport(
        path=path, size=2048, num_rows=1500, num_row_groups=2, created_by="w 1",
        score=score, findings=list(findings), **kw,
    )


def test_human_size_picks_a_sensible_unit():
    assert human_size(512) == "512 B"
    assert human_size(2048) == "2.00 KiB"
    assert human_size(5 * 1024**2) == "5.00 MiB"
    assert human_size(3 * 1024**3) == "3.00 GiB"


def test_plural():
    assert plural(1, "row group") == "1 row group"
    assert plural(1234, "row") == "1,234 rows"


def test_findings_are_ordered_by_severity():
    fs = [finding("c", Severity.HINT), finding("a", Severity.WARNING), finding("b", Severity.INFO)]
    out = "\n".join(render.findings_table(Style(False), fs, width=100))
    assert out.index(" a ") < out.index(" b ") < out.index(" c ")


def test_text_wraps_to_terminal_width():
    out = to_text([report(findings=[finding()])], st=Style(False), width=70)
    assert max(len(line) for line in out.splitlines()) <= 70


def test_recommendation_continuation_is_indented():
    out = to_text([report(findings=[finding()])], st=Style(False), width=60).splitlines()
    first = next(i for i, l in enumerate(out) if "→" in l)
    assert out[first + 1].lstrip().startswith("do this")  # hanging indent, no arrow repeated


def test_long_path_is_not_broken():
    path = "x" * 120 + ".parquet"
    out = to_text([report(path=path), report(path="b")], st=Style(False), width=70)
    assert path in out


def test_columns_and_details_shown_under_the_message():
    f = finding("dup", columns=("a", "b"))
    g = finding("sort", Severity.HINT, details={"dc": 1.0, "store": 0.724})
    out = to_text([report(findings=[f, g])], st=Style(False), width=100)
    assert "columns: a, b" in out
    assert "overlap: dc 100%, store 72%" in out


def test_color_does_not_change_layout():
    r = [report(findings=[finding("x", columns=("a",))]), report("b", 95)]
    plain = to_text(r, st=Style(False), width=90, details=True)
    colored = to_text(r, st=Style(True), width=90, details=True)
    assert "\x1b[" in colored
    assert ANSI.sub("", colored) == plain


def test_ascii_fallback_has_no_unicode():
    r = [report(findings=[finding()]), report("b")]
    out = to_text(r, st=Style(False, unicode=False), width=90, details=True)
    assert out.isascii()
    assert "->" in out


def test_overview_lists_rules_under_each_path():
    r = [report("a", findings=[finding("small-file"), finding("uncompressed")]), report("b")]
    out = to_text(r, st=Style(False), width=100)
    lines = out.splitlines()
    i = next(i for i, l in enumerate(lines) if l.rstrip().endswith(" a"))
    assert "small-file, uncompressed" in lines[i + 1]
    assert "LEVEL" not in out  # per-file tables are opt-in for several files


def test_details_flag_adds_per_file_tables_for_several_files():
    r = [report("a", findings=[finding()]), report("b", findings=[finding()])]
    assert "LEVEL" not in to_text(r, st=Style(False), width=100)
    assert to_text(r, st=Style(False), width=100, details=True).count("LEVEL") == 2


def test_single_file_always_shows_details():
    assert "LEVEL" in to_text([report(findings=[finding()])], st=Style(False))


def test_error_message_is_visible_in_overview():
    bad = FileReport(path="bad.parquet", error="ArrowInvalid: nope")
    out = to_text([report("ok"), bad], st=Style(False), width=100)
    assert "ERR" in out and "ArrowInvalid: nope" in out


def test_all_unreadable_summary_does_not_crash():
    out = to_text([FileReport(path="x", error="boom")], summary_only=True, st=Style(False))
    assert "Dataset summary" in out and "1  (1 unreadable)" in out


def test_summary_tables(write):
    r = [
        report("p/dt=1/a.parquet", 60, [finding("small-file")]),
        report("p/dt=2/b.parquet", 100),
    ]
    out = to_text(r, summary_only=True, st=Style(False), width=100)
    assert "FILES AFFECTED" in out and "1 / 2" in out and "50%" in out
    assert "LOWEST-SCORING FILES" in out
    assert "PARTITION KEY" in out and "dt" in out


class FakeTTY:
    def __init__(self, tty):
        self._tty = tty

    def isatty(self):
        return self._tty


def test_use_color_modes(monkeypatch):
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.setenv("TERM", "xterm")
    assert render.use_color("always", FakeTTY(False))
    assert not render.use_color("never", FakeTTY(True))
    assert render.use_color("auto", FakeTTY(True))
    assert not render.use_color("auto", FakeTTY(False))
    monkeypatch.setenv("NO_COLOR", "1")
    assert not render.use_color("auto", FakeTTY(True))
    assert render.use_color("always", FakeTTY(True))  # an explicit flag beats NO_COLOR
    monkeypatch.delenv("NO_COLOR")
    monkeypatch.setenv("TERM", "dumb")
    assert not render.use_color("auto", FakeTTY(True))


def test_cli_color_flag(write, table, capsys):
    path = str(write(table, compression="none"))
    main([path])
    assert "\x1b[" not in capsys.readouterr().out  # not a tty -> plain
    main([path, "--color", "always"])
    assert "\x1b[" in capsys.readouterr().out
    main([path, "--color", "never"])
    assert "\x1b[" not in capsys.readouterr().out


def test_cli_details_flag(write, table, tmp_path, capsys):
    write(table, "a.parquet", compression="none")
    write(table, "b.parquet", compression="none")
    main([str(tmp_path)])
    assert "LEVEL" not in capsys.readouterr().out
    main([str(tmp_path), "--details"])
    assert capsys.readouterr().out.count("LEVEL") == 2


def test_json_is_unaffected_by_text_options(write, table, capsys):
    import json

    path = str(write(table))
    main([path, "--json", "--color", "always", "--details"])
    out = capsys.readouterr().out
    assert "\x1b[" not in out
    assert json.loads(out)["files"][0]["path"] == path
