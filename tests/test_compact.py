import random
import re

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pqlint import compact as C
from pqlint.cli import main

N = 1000


@pytest.fixture(autouse=True)
def no_process_baseline(monkeypatch):
    # The fixed interpreter overhead in the memory model dwarfs KB-sized fixtures.
    monkeypatch.setattr(C, "PROCESS_BASELINE", 0)


def shuffled(n=N, seed=0):
    v = list(range(n))
    random.Random(seed).shuffle(v)
    return v


@pytest.fixture
def src(write):
    """1000 rows written as 20 tiny row groups, unsorted."""
    t = pa.table({"k": shuffled(), "g": [i % 4 for i in shuffled(seed=1)], "s": [f"v{i}" for i in range(N)]})
    return write(t, "src.parquet", row_group_size=50, compression="zstd")


def run(src, out, *extra):
    return main(["compact", str(src), "-o", str(out), *extra])


def read(path):
    return pq.read_table(path)


def test_streaming_rewrites_row_groups_and_keeps_rows(src, tmp_path, capsys):
    out = tmp_path / "out.parquet"
    assert run(src, out, "--row-group-rows", "300") == 0
    md = pq.read_metadata(out)
    assert md.num_rows == N
    assert [md.row_group(i).num_rows for i in range(md.num_row_groups)] == [300, 300, 300, 100]
    assert read(out).equals(read(src))  # same rows, same order
    out = capsys.readouterr().out
    assert re.search(r"Row groups\s+20\s+4\b", out)


def test_streaming_exact_multiple_has_no_tiny_tail(src, tmp_path):
    out = tmp_path / "out.parquet"
    run(src, out, "--row-group-rows", "250")
    md = pq.read_metadata(out)
    assert [md.row_group(i).num_rows for i in range(md.num_row_groups)] == [250] * 4


def test_sort_orders_rows_and_declares_sorting_columns(src, tmp_path):
    out = tmp_path / "out.parquet"
    assert run(src, out, "--sort-by", "g,k", "--row-group-rows", "250") == 0
    t = read(out)
    pairs = list(zip(t["g"].to_pylist(), t["k"].to_pylist()))
    assert pairs == sorted(pairs)
    assert sorted(t["s"].to_pylist()) == sorted(read(src)["s"].to_pylist())  # rows intact
    declared = pq.read_metadata(out).row_group(0).sorting_columns
    assert [(c.column_index, c.descending) for c in declared] == [(1, False), (0, False)]


def test_sort_descending(src, tmp_path):
    out = tmp_path / "out.parquet"
    run(src, out, "--sort-by", "k:desc")
    ks = read(out)["k"].to_pylist()
    assert ks == sorted(ks, reverse=True)


def test_sort_puts_nulls_last(write, tmp_path):
    t = pa.table({"k": [3, None, 1, None, 2]})
    out = tmp_path / "out.parquet"
    run(write(t, "n.parquet"), out, "--sort-by", "k")
    assert read(out)["k"].to_pylist() == [1, 2, 3, None, None]


def test_sorting_makes_row_group_ranges_disjoint(src, tmp_path):
    from pqlint.rules import _overlap

    def k_ranges(path):
        md = pq.read_metadata(path)
        stats = [md.row_group(i).column(0).statistics for i in range(md.num_row_groups)]
        return [(s.min, s.max) for s in stats]

    assert _overlap(k_ranges(src)) > 0.8  # shuffled input
    out = tmp_path / "out.parquet"
    run(src, out, "--sort-by", "k", "--row-group-rows", "100")
    assert _overlap(k_ranges(out)) == 0


def test_merges_directory_of_files(write, tmp_path):
    d = tmp_path / "in"
    d.mkdir()
    for i in range(3):
        pq.write_table(pa.table({"k": [i * 10 + j for j in range(10)]}), d / f"p{i}.parquet")
    out = tmp_path / "out.parquet"
    assert main(["compact", str(d), "-o", str(out), "--sort-by", "k:desc"]) == 0
    assert read(out)["k"].to_pylist() == sorted(
        (i * 10 + j for i in range(3) for j in range(10)), reverse=True
    )


def test_schema_mismatch_is_rejected(tmp_path, capsys):
    pq.write_table(pa.table({"k": [1]}), tmp_path / "a.parquet")
    pq.write_table(pa.table({"k": ["x"]}), tmp_path / "b.parquet")
    assert main(["compact", str(tmp_path), "-o", str(tmp_path / "o.parquet")]) == 2
    assert "schema differs" in capsys.readouterr().err


def test_unknown_sort_column(src, tmp_path, capsys):
    assert run(src, tmp_path / "o.parquet", "--sort-by", "nope") == 2
    assert "unknown sort column 'nope'" in capsys.readouterr().err
    assert not (tmp_path / "o.parquet").exists()


def test_bad_sort_direction(src, tmp_path, capsys):
    assert run(src, tmp_path / "o.parquet", "--sort-by", "k:sideways") == 2
    assert "bad sort direction" in capsys.readouterr().err


def test_refuses_to_overwrite_input(src, capsys):
    before = src.read_bytes()
    assert run(src, src, "--overwrite") == 2
    assert "also an input" in capsys.readouterr().err
    assert src.read_bytes() == before


def test_refuses_existing_output_without_overwrite(src, tmp_path, capsys):
    out = tmp_path / "out.parquet"
    out.write_bytes(b"keep me")
    assert run(src, out) == 2
    assert out.read_bytes() == b"keep me"
    assert run(src, out, "--overwrite") == 0
    assert pq.read_metadata(out).num_rows == N


def test_missing_output_directory(src, tmp_path, capsys):
    assert run(src, tmp_path / "nodir" / "o.parquet") == 2
    assert "does not exist" in capsys.readouterr().err


def test_dry_run_writes_nothing(src, tmp_path, capsys):
    out = tmp_path / "o.parquet"
    assert run(src, out, "--dry-run", "--sort-by", "k") == 0
    assert not out.exists()
    assert re.search(r"Sort\s+k asc \(in memory\)", capsys.readouterr().out)


def test_sort_refused_when_one_key_value_does_not_fit(write, tmp_path, capsys):
    # A single value of the first sort column cannot be split across buckets.
    t = pa.table({"g": ["a"] * 900 + ["b"] * 100, "k": shuffled()})
    src = write(t, "skew.parquet", row_group_size=100)
    out = tmp_path / "o.parquet"
    assert run(src, out, "--sort-by", "g,k", "--memory-limit", "8K") == 2
    err = capsys.readouterr().err
    assert "'a'" in err and "--force" in err
    assert not out.exists()
    assert run(src, out, "--sort-by", "g,k", "--memory-limit", "8K", "--force") == 0
    assert read(out).equals(read(src).sort_by([("g", "ascending"), ("k", "ascending")]))


def test_streaming_is_not_blocked_by_low_memory(src, tmp_path, monkeypatch):
    monkeypatch.setattr(C, "available_memory", lambda: 1024)  # also never asks to bucket
    assert run(src, tmp_path / "o.parquet") == 0


def test_unknown_memory_does_not_block(src, tmp_path, monkeypatch):
    monkeypatch.setattr(C, "available_memory", lambda: None)
    assert run(src, tmp_path / "o.parquet", "--sort-by", "k") == 0


def test_failed_write_leaves_no_partial_output(src, tmp_path, monkeypatch):
    def boom(plan, tmp, compression, level=None):
        tmp.write_bytes(b"partial")
        raise RuntimeError("disk full")

    monkeypatch.setattr(C, "_write_streaming", boom)
    out = tmp_path / "o.parquet"
    with pytest.raises(RuntimeError):
        run(src, out)
    assert not out.exists()
    assert not [p for p in tmp_path.iterdir() if p.name.endswith(".tmp")]


def test_verification_catches_row_count_mismatch(src, tmp_path, monkeypatch, capsys):
    real = C._write_streaming

    def drop_rows(plan, tmp, compression, level=None):
        real(plan, tmp, compression, level)
        pq.write_table(read(tmp).slice(0, 10), tmp)

    monkeypatch.setattr(C, "_write_streaming", drop_rows)
    out = tmp_path / "o.parquet"
    assert run(src, out) == 2
    assert "verification failed" in capsys.readouterr().err
    assert not out.exists()


def test_empty_input_rejected(write, tmp_path, capsys):
    path = write(pa.table({"k": pa.array([], type=pa.int64())}), "e.parquet")
    assert run(path, tmp_path / "o.parquet") == 2
    assert "no rows" in capsys.readouterr().err


def test_auto_row_group_size_is_bounded(src):
    plan = C.make_plan([str(src)])
    assert 1 <= plan.row_group_rows <= N


def test_schema_metadata_survives(write, tmp_path):
    t = pa.table({"k": [1, 2, 3]}).replace_schema_metadata({"owner": "me"})
    out = tmp_path / "o.parquet"
    run(write(t, "m.parquet"), out, "--sort-by", "k:desc")
    assert pq.ParquetFile(out).schema_arrow.metadata[b"owner"] == b"me"


def test_auto_row_group_size_shrinks_on_low_memory(write, monkeypatch):
    t = pa.table({"k": list(range(200_000))})
    path = str(write(t, "big.parquet", row_group_size=50_000))
    monkeypatch.setattr(C, "available_memory", lambda: None)
    roomy = C.make_plan([path]).row_group_rows
    monkeypatch.setattr(C, "available_memory", lambda: 8 * 1024 * 1024)
    tight = C.make_plan([path]).row_group_rows
    assert tight < roomy
    assert C.make_plan([path], row_group_rows=77).row_group_rows == 77  # explicit wins


def test_low_memory_does_not_shrink_sorted_plan(write, monkeypatch):
    # Sorting cannot trade row group size for memory, so the size is unchanged.
    t = pa.table({"k": list(range(200_000))})
    path = str(write(t, "big.parquet", row_group_size=50_000))
    monkeypatch.setattr(C, "available_memory", lambda: None)
    roomy = C.make_plan([path], [("k", "ascending")]).row_group_rows
    monkeypatch.setattr(C, "available_memory", lambda: 8 * 1024 * 1024)
    assert C.make_plan([path], [("k", "ascending")]).row_group_rows == roomy
