import json

from pqlint.cli import main


def test_text_output(write, table, capsys):
    path = write(table, compression="none")
    assert main([str(path)]) == 0
    out = capsys.readouterr().out
    assert "score" in out
    assert "uncompressed" in out
    assert "->" in out


def test_json_output(write, table, capsys):
    path = write(table, compression="none")
    main([str(path), "--json"])
    data = json.loads(capsys.readouterr().out)
    file = data["files"][0]
    assert file["path"] == str(path)
    assert {f["rule"] for f in file["findings"]} >= {"uncompressed", "small-file"}
    assert file["findings"][0]["severity"] in ("info", "warning")
    assert data["summary"]["files"] == 1


def test_directory_is_scanned_recursively(write, table, tmp_path, capsys):
    write(table, "a.parquet")
    (tmp_path / "sub").mkdir()
    write(table, "sub/b.parquet")
    (tmp_path / "ignored.txt").write_text("x")
    main([str(tmp_path), "--json"])
    assert len(json.loads(capsys.readouterr().out)["files"]) == 2


def test_fail_under(write, table):
    path = str(write(table, compression="none"))  # score 80
    assert main([path, "--fail-under", "90"]) == 1
    assert main([path, "--fail-under", "80"]) == 0


def test_corrupt_file_reports_error_and_exits_2(tmp_path, capsys):
    bad = tmp_path / "bad.parquet"
    bad.write_bytes(b"not parquet")
    assert main([str(bad)]) == 2
    assert "ERROR" in capsys.readouterr().out


def test_no_files_found(tmp_path, capsys):
    assert main([str(tmp_path)]) == 2


def test_summary_only_text(write, table, tmp_path, capsys):
    write(table, "a.parquet")
    write(table, "b.parquet")
    main([str(tmp_path), "--summary-only"])
    out = capsys.readouterr().out
    assert out.startswith("Dataset summary: 2 file(s)")
    assert "a.parquet: score" not in out


def test_summary_only_json_omits_files(write, table, capsys):
    path = write(table)
    main([str(path), "--summary-only", "--json"])
    data = json.loads(capsys.readouterr().out)
    assert "files" not in data
    assert data["summary"]["files"] == 1


def test_single_file_text_has_no_summary(write, table, capsys):
    main([str(write(table))])
    assert "Dataset summary" not in capsys.readouterr().out
