"""Bucketed sort: a sort that does not fit in memory, split by the first sort column."""

import random

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pqlint import compact as C
from pqlint.cli import main

N = 2000


@pytest.fixture(autouse=True)
def no_process_baseline(monkeypatch):
    # The fixed interpreter overhead in the memory model dwarfs KB-sized fixtures.
    monkeypatch.setattr(C, "PROCESS_BASELINE", 0)


@pytest.fixture
def src(write):
    rnd = random.Random(7)
    g = [rnd.choice(["n", "s", "e", "w", "c", "x", "y", None]) for _ in range(N)]
    k = list(range(N))
    rnd.shuffle(k)
    t = pa.table({"g": g, "k": k, "v": [rnd.random() for _ in range(N)], "s": [f"r{i}" for i in range(N)]})
    return write(t, "src.parquet", row_group_size=100)


def limit_for(path, keys, share):
    """A memory limit that is `share` of what the full in-memory sort needs."""
    plan = C.make_plan([str(path)], keys, memory_limit=None)
    assert plan.strategy == C.MEMORY
    return int(plan.est_peak_bytes * share)


def compact_with(src, out, keys, limit, *extra):
    spec = ",".join(f"{n}:desc" if d == "descending" else n for n, d in keys)
    return main(["compact", str(src), "-o", str(out), "--sort-by", spec,
                 "--memory-limit", str(limit), *extra])


@pytest.mark.parametrize(
    "keys",
    [
        [("k", "ascending")],
        [("k", "descending")],
        [("g", "ascending"), ("k", "descending")],
        [("g", "descending"), ("v", "ascending")],
        [("s", "ascending")],
    ],
)
def test_bucketed_result_equals_in_memory_sort(src, tmp_path, keys):
    first_unique = keys[0][0] in ("k", "s", "v")
    limit = limit_for(src, keys, 0.33 if first_unique else 0.95)
    plan = C.make_plan([str(src)], keys, memory_limit=limit)
    assert plan.strategy == C.BUCKETED and plan.fits_in_memory
    assert len(plan.bucket_rows) > 1

    out = tmp_path / "out.parquet"
    assert compact_with(src, out, keys, limit) == 0
    expected = pq.read_table(src).sort_by(keys)  # nulls last, stable
    assert pq.read_table(out).equals(expected)


def test_nulls_of_first_key_come_last(src, tmp_path):
    keys = [("g", "descending"), ("k", "ascending")]
    out = tmp_path / "out.parquet"
    compact_with(src, out, keys, limit_for(src, keys, 0.95))
    g = pq.read_table(out)["g"].to_pylist()
    first_null = g.index(None)
    assert all(x is None for x in g[first_null:])


def test_row_groups_are_uniform_across_buckets(src, tmp_path):
    keys = [("k", "ascending")]
    out = tmp_path / "out.parquet"
    compact_with(src, out, keys, limit_for(src, keys, 0.33), "--row-group-rows", "300")
    md = pq.read_metadata(out)
    sizes = [md.row_group(i).num_rows for i in range(md.num_row_groups)]
    assert sizes == [300] * 6 + [200]


def test_sort_is_declared_in_footer(src, tmp_path):
    keys = [("g", "ascending"), ("k", "descending")]
    out = tmp_path / "out.parquet"
    compact_with(src, out, keys, limit_for(src, keys, 0.95))
    cols = pq.read_metadata(out).row_group(0).sorting_columns
    assert [(c.column_index, c.descending) for c in cols] == [(0, False), (1, True)]


def test_merges_several_files(write, tmp_path):
    d = tmp_path / "in"
    d.mkdir()
    rnd = random.Random(3)
    for i in range(3):
        vals = list(range(i * 500, (i + 1) * 500))
        rnd.shuffle(vals)
        pq.write_table(pa.table({"k": vals}), d / f"p{i}.parquet", row_group_size=50)
    keys = [("k", "ascending")]
    first = sorted(d.iterdir())[0]
    limit = limit_for(first, keys, 1.0)  # fits one file, not all three
    out = tmp_path / "out.parquet"
    assert main(["compact", str(d), "-o", str(out), "--sort-by", "k",
                 "--memory-limit", str(limit)]) == 0
    assert pq.read_table(out)["k"].to_pylist() == list(range(1500))


def test_dry_run_shows_buckets(src, tmp_path, capsys):
    keys = [("k", "ascending")]
    out = tmp_path / "out.parquet"
    compact_with(src, out, keys, limit_for(src, keys, 0.33), "--dry-run")
    text = capsys.readouterr().out
    assert "bucketed by k" in text and "Temp disk" in text
    assert not out.exists()


def test_temporary_buckets_are_removed(src, tmp_path):
    keys = [("k", "ascending")]
    out = tmp_path / "out.parquet"
    compact_with(src, out, keys, limit_for(src, keys, 0.33))
    assert sorted(p.name for p in tmp_path.iterdir()) == ["out.parquet", "src.parquet"]


def test_failure_in_pass_two_cleans_up(src, tmp_path, monkeypatch):
    def boom(self, data):
        raise RuntimeError("disk full")

    monkeypatch.setattr(C._RowGroupWriter, "add", boom)
    keys = [("k", "ascending")]
    out = tmp_path / "out.parquet"
    with pytest.raises(RuntimeError):
        compact_with(src, out, keys, limit_for(src, keys, 0.33))
    assert sorted(p.name for p in tmp_path.iterdir()) == ["src.parquet"]


def test_too_many_distinct_values(src, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(C, "MAX_BUCKET_KEYS", 10)
    keys = [("k", "ascending")]
    assert compact_with(src, tmp_path / "o.parquet", keys, limit_for(src, keys, 0.33)) == 2
    assert "distinct values" in capsys.readouterr().err


def test_too_many_buckets(src, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(C, "MAX_BUCKETS", 2)
    keys = [("k", "ascending")]
    assert compact_with(src, tmp_path / "o.parquet", keys, limit_for(src, keys, 0.33)) == 2
    assert "buckets (limit 2)" in capsys.readouterr().err


def test_not_enough_disk(src, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(C.shutil, "disk_usage", lambda p: type("U", (), {"free": 10})())
    keys = [("k", "ascending")]
    assert compact_with(src, tmp_path / "o.parquet", keys, limit_for(src, keys, 0.33)) == 2
    assert "free disk" in capsys.readouterr().err


def test_sort_that_fits_stays_in_memory(src):
    plan = C.make_plan([str(src)], [("k", "ascending")], memory_limit=10 * 1024**3)
    assert plan.strategy == C.MEMORY


@pytest.mark.parametrize(
    "text, expected",
    [("512", 512), ("8K", 8192), ("2M", 2 * 1024**2), ("1.5G", int(1.5 * 1024**3)), ("2GiB", 2 * 1024**3)],
)
def test_parse_size(text, expected):
    assert C.parse_size(text) == expected


def test_parse_size_rejects_garbage():
    with pytest.raises(C.CompactError):
        C.parse_size("lots")


def test_big_single_value_within_memory_is_not_refused(write, tmp_path):
    # "a" holds 60% of the rows: over the per-bucket grouping budget (60% of memory
    # spread over buckets) but its own sort still fits in the limit.
    t = pa.table({"g": ["a"] * 1200 + ["b"] * 400 + ["c"] * 400, "k": list(range(2000))[::-1]})
    src = write(t, "skew.parquet", row_group_size=100)
    keys = [("g", "ascending"), ("k", "ascending")]
    full = C.make_plan([str(src)], keys)
    limit = int(full.est_peak_bytes * 0.9)
    plan = C.make_plan([str(src)], keys, memory_limit=limit)
    assert plan.strategy == C.BUCKETED
    assert plan.problem is None and plan.fits_in_memory
    out = tmp_path / "o.parquet"
    assert compact_with(src, out, keys, limit) == 0
    assert pq.read_table(out).equals(pq.read_table(src).sort_by(keys))


def test_limit_below_fixed_overhead_says_so(src, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(C, "PROCESS_BASELINE", 200 * 1024**2)
    keys = [("k", "ascending")]
    assert compact_with(src, tmp_path / "o.parquet", keys, 150 * 1024**2) == 2
    err = capsys.readouterr().err
    assert "below what the compaction needs" in err
    assert "cannot be split" not in err


def test_plan_labels_a_given_limit(src, capsys):
    main(["compact", str(src), "-o", "x.parquet", "--sort-by", "k", "--memory-limit", "1G", "--dry-run"])
    assert "1.00 GiB limit" in capsys.readouterr().out


# --- dictionary-typed columns (e.g. pandas category) ---------------------------------


def dict_table(seed, n=N):
    rnd = random.Random(seed)
    g = [rnd.choice(["n", "s", "e", "w", "c", "x", "y", None]) for _ in range(n)]
    k = list(range(n))
    rnd.shuffle(k)
    return pa.table({"g": pa.array(g, pa.string()).dictionary_encode(), "k": k})


def plain(table):
    return table.set_column(0, "g", table["g"].cast(pa.string()))


@pytest.mark.parametrize("keys", [[("g", "ascending"), ("k", "descending")], [("g", "descending")]])
def test_sort_by_dictionary_column_in_memory(write, tmp_path, keys):
    src = write(dict_table(1), "d.parquet", row_group_size=100)
    out = tmp_path / "out.parquet"
    assert compact_with(src, out, keys, 10**9) == 0
    got = pq.read_table(out)
    assert pa.types.is_dictionary(got.schema.field("g").type)
    assert plain(got).equals(plain(pq.read_table(src)).sort_by(keys))


@pytest.mark.parametrize("keys", [[("g", "ascending"), ("k", "descending")], [("g", "descending")]])
def test_sort_by_dictionary_column_bucketed(write, tmp_path, keys):
    src = write(dict_table(1), "d.parquet", row_group_size=100)
    limit = limit_for(src, keys, 0.95)
    plan = C.make_plan([str(src)], keys, memory_limit=limit)
    assert plan.strategy == C.BUCKETED and len(plan.bucket_rows) > 1
    out = tmp_path / "out.parquet"
    assert compact_with(src, out, keys, limit) == 0
    got = pq.read_table(out)
    assert pa.types.is_dictionary(got.schema.field("g").type)
    assert plain(got).equals(plain(pq.read_table(src)).sort_by(keys))


def test_dictionary_sort_across_files_with_different_dictionaries(write, tmp_path):
    a = write(dict_table(1, 600), "a.parquet", row_group_size=100)
    # Same values, different first-seen order -> different dictionary per file.
    t = dict_table(2, 600)
    b = write(t.take(pa.array(range(599, -1, -1))), "b.parquet", row_group_size=100)
    keys = [("g", "ascending"), ("k", "ascending")]
    out = tmp_path / "out.parquet"
    rc = main(["compact", str(a), str(b), "-o", str(out), "--sort-by", "g,k"])
    assert rc == 0
    both = pa.concat_tables([plain(pq.read_table(a)), plain(pq.read_table(b))])
    assert plain(pq.read_table(out)).equals(both.sort_by(keys))


# --- sorting several buckets at once (--threads) -------------------------------------


@pytest.mark.parametrize("threads", [1, 2, 4])
@pytest.mark.parametrize(
    "keys",
    [[("k", "ascending")], [("g", "ascending"), ("k", "descending")]],
)
def test_parallel_buckets_equal_in_memory_sort(src, tmp_path, keys, threads):
    limit = limit_for(src, keys, 0.5)
    plan = C.make_plan([str(src)], keys, memory_limit=limit, threads=threads)
    assert plan.strategy == C.BUCKETED and plan.fits_in_memory
    assert len(plan.bucket_rows) > 1

    out = tmp_path / "out.parquet"
    assert compact_with(src, out, keys, limit, "--threads", str(threads)) == 0
    assert pq.read_table(out).equals(pq.read_table(src).sort_by(keys))
    assert sorted(p.name for p in tmp_path.iterdir()) == ["out.parquet", "src.parquet"]


def test_workers_never_exceed_what_was_asked(src):
    keys = [("k", "ascending")]
    limit = limit_for(src, keys, 0.5)
    assert C.make_plan([str(src)], keys, memory_limit=limit, threads=1).workers == 1
    assert 1 <= C.make_plan([str(src)], keys, memory_limit=limit, threads=3).workers <= 3


def test_workers_shrink_when_memory_is_tight(src):
    # Eight values of g cannot be split, so only a few buckets can be in flight.
    keys = [("g", "ascending")]
    roomy = C.make_plan([str(src)], keys, memory_limit=limit_for(src, keys, 0.95), threads=4)
    tight = C.make_plan([str(src)], keys, memory_limit=limit_for(src, keys, 0.3), threads=4)
    assert roomy.workers == 4
    assert tight.fits_in_memory and tight.workers < roomy.workers
    # The estimate counts every bucket in flight, so it stays inside the limit.
    assert tight.est_peak_bytes <= tight.available_bytes
    assert roomy.est_peak_bytes <= roomy.available_bytes


def test_more_workers_mean_smaller_buckets_within_the_limit(src):
    keys = [("k", "ascending")]
    limit = limit_for(src, keys, 0.5)
    one = C.make_plan([str(src)], keys, memory_limit=limit, threads=1)
    four = C.make_plan([str(src)], keys, memory_limit=limit, threads=4)
    assert four.workers > 1
    assert max(four.bucket_rows) <= max(one.bucket_rows)
    assert len(four.bucket_rows) >= len(one.bucket_rows)
    assert four.est_peak_bytes <= limit


def test_plan_shows_parallel_buckets(src):
    keys = [("k", "ascending")]
    plan = C.make_plan([str(src)], keys, memory_limit=limit_for(src, keys, 0.5), threads=4)
    text = dict(C.plan_rows(plan))["Sort"]
    assert f"{plan.workers} in parallel" in text
    one = C.make_plan([str(src)], keys, memory_limit=limit_for(src, keys, 0.5), threads=1)
    assert "in parallel" not in dict(C.plan_rows(one))["Sort"]


def test_buckets_are_written_in_order_even_if_they_finish_late(src, tmp_path, monkeypatch):
    import threading
    import time

    real = C._sort_table
    first = threading.Event()

    def slow_first(table, keys):
        # The first bucket to start sorting is the slowest one.
        if not first.is_set():
            first.set()
            time.sleep(0.3)
        return real(table, keys)

    monkeypatch.setattr(C, "_sort_table", slow_first)
    keys = [("k", "ascending")]
    limit = limit_for(src, keys, 0.5)
    out = tmp_path / "out.parquet"
    assert compact_with(src, out, keys, limit, "--threads", "4") == 0
    assert pq.read_table(out).equals(pq.read_table(src).sort_by(keys))


def test_failure_in_parallel_pass_two_cleans_up(src, tmp_path, monkeypatch):
    calls = []
    real = C._sort_table

    def boom(table, keys):
        calls.append(1)
        if len(calls) == 2:
            raise RuntimeError("bad bucket")
        return real(table, keys)

    monkeypatch.setattr(C, "_sort_table", boom)
    keys = [("k", "ascending")]
    out = tmp_path / "out.parquet"
    with pytest.raises(RuntimeError, match="bad bucket"):
        compact_with(src, out, keys, limit_for(src, keys, 0.5), "--threads", "4")
    assert sorted(p.name for p in tmp_path.iterdir()) == ["src.parquet"]


@pytest.mark.parametrize("bad", ["0", "-1"])
def test_threads_must_be_positive(src, tmp_path, bad):
    with pytest.raises(SystemExit) as exc:
        main(["compact", str(src), "-o", str(tmp_path / "o.parquet"), "--threads", bad])
    assert exc.value.code == 2


# --- memory used by the sort itself ---------------------------------------------------


def wide_table(rows=1_000_000, columns=10):
    rnd = random.Random(3)
    data = {"key": pa.array([rnd.randrange(1000) for _ in range(rows)], pa.int64())}
    for i in range(columns - 1):
        data[f"c{i}"] = pa.array(range(rows), pa.int64())
    return pa.table(data)


PEAK_SCRIPT = """
import sys
import pyarrow as pa
sys.path.insert(0, sys.argv[1])
from test_bucketed_sort import wide_table
from pqlint import compact as C

pool = pa.default_memory_pool()
table = wide_table()
size, table = table.nbytes, None
before = pool.max_memory()
# No name of ours holds the table, so the callee has the only reference.
result = C._sort_table(wide_table(), [("key", "ascending")])
print(size, before, pool.max_memory(), result.num_rows)
"""


def test_sort_does_not_hold_two_full_copies():
    # Table.sort_by needs the table plus a full sorted copy (~2x). _sort_table sorts the key
    # columns, then copies one column at a time while releasing the original: ~1 table plus
    # one column. Measured in a fresh process because the pool's peak is process-wide.
    import subprocess
    import sys
    from pathlib import Path

    out = subprocess.run(
        [sys.executable, "-c", PEAK_SCRIPT, str(Path(__file__).parent)],
        capture_output=True, text=True, check=True,
    ).stdout.split()
    size, before, peak, rows = map(int, out)
    assert rows == 1_000_000
    assert peak > before  # the measurement did observe this sort
    assert peak < 1.5 * size, f"peak was {peak / size:.2f}x the table"


def test_sort_result_unchanged_by_releasing_columns():
    t = wide_table(5000, 4)
    keys = [("key", "descending"), ("c0", "ascending")]
    assert C._sort_table(t, keys).equals(t.sort_by(keys))


def test_default_is_one_bucket_at_a_time(src):
    keys = [("k", "ascending")]
    plan = C.make_plan([str(src)], keys, memory_limit=limit_for(src, keys, 0.5))
    assert plan.workers == 1
    assert "in parallel" not in dict(C.plan_rows(plan))["Sort"]
