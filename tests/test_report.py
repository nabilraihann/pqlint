from pqlint import report
from pqlint.cli import main


def test_rule_failure_is_captured_not_raised(write, table, monkeypatch, capsys):
    def boom(info, cfg):
        raise RuntimeError("bad metadata")

    monkeypatch.setattr(report, "lint", boom)
    r = report.lint_path(write(table))
    assert r.score is None
    assert r.error == "rule failed: RuntimeError: bad metadata"


def test_one_failing_file_does_not_stop_the_scan(write, table, tmp_path, monkeypatch, capsys):
    write(table, "a.parquet")
    write(table, "b.parquet")
    real = report.lint
    calls = []

    def flaky(info, cfg):
        calls.append(info.path)
        if len(calls) == 1:
            raise RuntimeError("boom")
        return real(info, cfg)

    monkeypatch.setattr(report, "lint", flaky)
    assert main([str(tmp_path)]) == 2  # error present -> exit 2
    out = capsys.readouterr().out
    assert "ERR" in out and "rule failed: RuntimeError: boom" in out
    assert "b.parquet" in out and "(1 unreadable)" in out  # the second file was still linted
