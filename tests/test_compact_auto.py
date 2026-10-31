"""`compact --sort-by auto`: the order is chosen by suggest-sort, then used for the compaction."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pqlint import compact as C
from pqlint import suggest as S
from pqlint.cli import main
from test_suggest import sales  # noqa: F401  (fixture shared with the suggest tests)


def sorting_names(path):
    """Column names the footer declares as the sort order of the first row group."""
    f = pq.ParquetFile(path)
    names = f.schema_arrow.names
    return [names[c.column_index] for c in f.metadata.row_group(0).sorting_columns]


def test_auto_uses_the_order_suggest_sort_recommends(sales, tmp_path, capsys):  # noqa: F811
    keys = S.suggest([str(sales)]).best.keys
    assert keys  # the fixture is built so that sorting pays off
    out = tmp_path / "out.parquet"
    assert main(["compact", str(sales), "-o", str(out), "--sort-by", "auto"]) == 0

    assert sorting_names(out) == keys
    expected = pq.read_table(sales).sort_by([(k, "ascending") for k in keys])
    assert pq.read_table(out).equals(expected)
    text = capsys.readouterr().out
    assert "suggest-sort" in text and ", ".join(keys) in text  # the choice is shown, not hidden


def test_auto_beats_no_sort(sales, tmp_path):  # noqa: F811
    auto, plain = tmp_path / "auto.parquet", tmp_path / "plain.parquet"
    assert main(["compact", str(sales), "-o", str(auto), "--sort-by", "auto"]) == 0
    assert main(["compact", str(sales), "-o", str(plain)]) == 0
    assert auto.stat().st_size < 0.6 * plain.stat().st_size


def test_auto_dry_run_shows_the_plan_and_writes_nothing(sales, tmp_path, capsys):  # noqa: F811
    keys = S.suggest([str(sales)]).best.keys
    out = tmp_path / "out.parquet"
    assert main(["compact", str(sales), "-o", str(out), "--sort-by", "auto", "--dry-run"]) == 0
    text = capsys.readouterr().out
    assert "Plan" in text and " asc, ".join(keys[:2]) in text
    assert not out.exists()


def test_auto_cannot_be_mixed_with_columns(sales, tmp_path, capsys):  # noqa: F811
    out = tmp_path / "out.parquet"
    assert main(["compact", str(sales), "-o", str(out), "--sort-by", "auto,store"]) == 2
    err = capsys.readouterr().err
    assert "auto" in err and "combined" in err and "Traceback" not in err
    assert not out.exists()


def test_a_column_named_auto_is_still_reachable(write, tmp_path, monkeypatch):
    def fail(*a, **k):
        raise AssertionError("suggest-sort must not run for a real column")

    monkeypatch.setattr(S, "suggest", fail)
    t = pa.table({"auto": [3, 1, 2, 3, 1], "v": [1, 2, 3, 4, 5]})
    src = write(t, "named.parquet")
    out = tmp_path / "out.parquet"
    assert main(["compact", str(src), "-o", str(out), "--sort-by", "auto:asc"]) == 0
    assert pq.read_table(out)["auto"].to_pylist() == [1, 1, 2, 3, 3]


def test_auto_when_no_sort_pays_off_compacts_unsorted(write, tmp_path, capsys):
    t = pa.table({"k": [i // 10 for i in range(5000)], "v": [float(i) for i in range(5000)]})
    src = write(t, "sorted.parquet")
    out = tmp_path / "out.parquet"
    assert main(["compact", str(src), "-o", str(out), "--sort-by", "auto"]) == 0
    text = capsys.readouterr().out
    assert "no sort" in text.lower() and "streaming" in text
    assert sorting_names(out) == []
    assert pq.read_table(out).equals(pq.read_table(src))  # input order kept


def test_auto_reports_why_suggest_sort_failed(write, tmp_path, capsys):
    t = pa.table({"id": list(range(1000)), "v": [float(i) for i in range(1000)]})
    src = write(t, "x.parquet")
    out = tmp_path / "out.parquet"
    assert main(["compact", str(src), "-o", str(out), "--sort-by", "auto"]) == 2
    err = capsys.readouterr().err
    assert "no column is usable" in err and "Traceback" not in err
    assert not out.exists()


def test_auto_on_a_directory_and_several_files(sales, write, tmp_path):  # noqa: F811
    folder = tmp_path / "in"
    folder.mkdir()
    t = pq.read_table(sales)
    pq.write_table(t.slice(0, 20_000), folder / "a.parquet", row_group_size=2000)
    pq.write_table(t.slice(20_000), folder / "b.parquet", row_group_size=2000)
    out = tmp_path / "merged.parquet"
    assert main(["compact", str(folder), "-o", str(out), "--sort-by", "auto"]) == 0
    assert pq.ParquetFile(out).metadata.num_rows == t.num_rows
    assert sorting_names(out)


def test_auto_gives_the_samples_memory_back_before_planning(sales, tmp_path, monkeypatch):  # noqa: F811
    # The allocator keeps what the sample used; planning would then see that memory as
    # taken (measured: MemAvailable fell by 1.5 GiB on the real file and the plan was refused).
    order = []
    real_pool = pa.default_memory_pool()

    class Pool:
        def release_unused(self):
            order.append("release")
            real_pool.release_unused()

    monkeypatch.setattr(pa, "default_memory_pool", lambda: Pool())
    real_plan = C.make_plan

    def spy(*a, **k):
        order.append("plan")
        return real_plan(*a, **k)

    monkeypatch.setattr("pqlint.compact_cli.make_plan", spy)
    out = tmp_path / "out.parquet"
    assert main(["compact", str(sales), "-o", str(out), "--sort-by", "auto"]) == 0
    assert order[:2] == ["release", "plan"]
