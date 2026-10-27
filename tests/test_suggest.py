import datetime
import decimal
import random
import threading

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from pqlint import suggest as S
from pqlint.cli import main


@pytest.fixture
def sales(write):
    """Day-by-day sales like the real file: region > store > item > day, plus noise columns."""
    rnd = random.Random(5)
    rows = []
    for day in range(1, 21):
        block = [(s, i) for s in range(80) for i in range(30)]
        rnd.shuffle(block)
        for s, i in block:
            # Like real sales amounts: an item's price times a small quantity.
            rows.append((f"R{s % 4}", f"S{s:03d}", i, f"item-{i:02d}", day, (100 + i) * rnd.randint(1, 3) * 1.0))
    cols = list(zip(*rows))
    t = pa.table(
        {
            "region": cols[0],
            "store": cols[1],
            "item": cols[2],
            "item_name": cols[3],
            "day": cols[4],
            "amount": cols[5],
            "year": [2025] * len(rows),
        }
    )
    return write(t, "sales.parquet", row_group_size=2000, compression="zstd")


def run(path, **kw):
    kw.setdefault("sample_rows", 15_000)
    return S.suggest([str(path)], **kw)


def test_skips_measures_constants_and_duplicate_groupings(sales):
    s = run(sales)
    assert "floating" in s.skipped["amount"]
    assert "constant" in s.skipped["year"]
    assert "same grouping as 'item'" in s.skipped["item_name"]


def test_skips_near_unique_ids(write):
    t = pa.table({"id": list(range(5000)), "g": [i % 7 for i in range(5000)]})
    s = S.suggest([str(write(t, "ids.parquet"))], sample_rows=10_000)
    assert "unique" in s.skipped["id"]


def test_entity_is_the_richest_non_unique_column(sales):
    assert run(sales).entity == "store"


def test_suggests_store_and_item_and_shrinks_a_lot(sales):
    s = run(sales)
    keys = s.best.keys
    assert "store" in keys and "item" in keys
    assert s.best.est_bytes < 0.6 * s.unsorted.est_bytes
    assert all("amount" not in st.keys for st in s.steps)


def test_steps_are_prefixes_of_the_recommendation(sales):
    s = run(sales)
    for n, st in enumerate(s.steps, 1):
        assert st.keys == s.best.keys[:n]


def test_suggested_order_really_compacts_smaller(sales, tmp_path):
    s = run(sales)
    sorted_out, plain_out = tmp_path / "sorted.parquet", tmp_path / "plain.parquet"
    assert main(["compact", str(sales), "-o", str(sorted_out), "--sort-by", ",".join(s.best.keys)]) == 0
    assert main(["compact", str(sales), "-o", str(plain_out)]) == 0
    assert sorted_out.stat().st_size < 0.6 * plain_out.stat().st_size


def test_start_with_fixes_the_leading_keys(sales):
    s = run(sales, start_with=["day"])
    assert s.best.keys[0] == "day"
    assert all(st.keys[0] == "day" for st in s.steps)


def test_max_keys_caps_the_order(sales):
    assert len(run(sales, max_keys=1).best.keys) <= 1


def test_already_sorted_file_gets_no_suggestion(write):
    t = pa.table({"k": [i // 10 for i in range(5000)], "v": [float(i) for i in range(5000)]})
    s = S.suggest([str(write(t, "sorted.parquet"))], sample_rows=10_000)
    assert s.steps == []
    assert "No sort order" in S.render_suggestion(S.render.Style(False), s)


def test_no_usable_columns(write):
    t = pa.table({"id": list(range(1000)), "v": [float(i) for i in range(1000)]})
    with pytest.raises(S.SuggestError, match="no column is usable"):
        S.suggest([str(write(t, "x.parquet"))])


def test_unknown_start_column(sales):
    with pytest.raises(S.SuggestError, match="unknown column 'nope'"):
        run(sales, start_with=["nope"])


def test_cli(sales, capsys):
    assert main(["suggest-sort", str(sales), "--sample-rows", "15000", "--color", "never"]) == 0
    out = capsys.readouterr().out
    assert "Recommended" in out and "--sort-by" in out
    assert "pqlint compact" in out
    assert "SKIPPED COLUMN" in out


def test_cli_error_exit_code(sales, capsys):
    assert main(["suggest-sort", str(sales), "--start-with", "nope"]) == 2
    assert "unknown column" in capsys.readouterr().err


def test_cli_rejects_empty_input(tmp_path, capsys):
    assert main(["suggest-sort", str(tmp_path)]) == 2


# --- rank-encoded sorting -------------------------------------------------


@pytest.fixture
def typed_table():
    """One column of every sortable type, duplicates everywhere and nulls sprinkled in."""
    rnd = random.Random(11)
    n = 600

    def pick(values, null_rate=0.15):
        return [None if rnd.random() < null_rate else rnd.choice(values) for _ in range(n)]

    return pa.table(
        {
            "i": pa.array(pick(list(range(7))), pa.int64()),
            "s": pa.array(pick([f"s{k}" for k in range(9)]), pa.string()),
            "ls": pa.array(pick([f"L{k}" for k in range(5)]), pa.large_string()),
            "d": pa.array(pick([datetime.date(2025, 1, 1 + k) for k in range(6)]), pa.date32()),
            "ts": pa.array(
                pick([datetime.datetime(2025, 1, 1, k) for k in range(5)]),
                pa.timestamp("us", tz="UTC"),
            ),
            "dec": pa.array(pick([decimal.Decimal(k) / 4 for k in range(6)]), pa.decimal128(10, 2)),
            "b": pa.array(pick([True, False]), pa.bool_()),
            "g": pa.array(pick([f"g{k}" for k in range(8)]), pa.string()).dictionary_encode(),
        }
    )


def decoded(t):
    """The table with dictionary columns turned into plain values (what sort_indices can order)."""
    cols = {
        f.name: t[f.name].cast(f.type.value_type) if pa.types.is_dictionary(f.type) else t[f.name]
        for f in t.schema
    }
    return pa.table(cols)


def reference_order(t, keys):
    return pc.sort_indices(decoded(t), sort_keys=[(k, "ascending") for k in keys])


def test_rank_order_equals_sort_indices_for_every_type(typed_table):
    cols = typed_table.column_names
    ranks = S._rank_table(typed_table, cols)
    key_sets = [["g", "i"], ["b", "d", "ts"], ["dec", "ls", "s", "i"]] + [[c] for c in cols]
    for keys in key_sets:
        assert S._order(ranks, keys).equals(reference_order(typed_table, keys)), keys


def test_nulls_sort_last(typed_table):
    order = S._order(S._rank_table(typed_table, ["s"]), ["s"])
    values = typed_table["s"].take(order).to_pylist()
    first_null = values.index(None)
    assert all(v is None for v in values[first_null:])
    assert values[:first_null] == sorted(values[:first_null])


def test_rank_size_equals_sort_by_size(typed_table):
    plain = typed_table.drop_columns(["g"])  # sort_by cannot order dictionary columns
    keys = ["d", "s"]
    with_ranks = S._encoded_size(plain, keys, "zstd", ranks=S._rank_table(plain, keys))
    assert with_ranks == S._encoded_size(plain, keys, "zstd")


def test_unsupported_rank_type_falls_back(typed_table, monkeypatch):
    real = pc.rank

    def fake(col, *args, **kwargs):
        if col.type == pa.date32():
            raise pa.ArrowNotImplementedError("rank not supported for this type")
        return real(col, *args, **kwargs)

    monkeypatch.setattr(S.pc, "rank", fake)
    ranks = S._rank_table(typed_table, ["d", "i"])
    assert ranks["d"].type == pa.date32()  # kept as the original values
    assert S._order(ranks, ["d", "i"]).equals(reference_order(typed_table, ["d", "i"]))


# --- cached, parallel scorer ----------------------------------------------

ORDERS = [
    ["region"],
    ["store", "item"],
    ["item", "store", "day"],
    ["day", "item"],
    ["region", "store", "item", "day"],
    ["store"],
]


@pytest.fixture
def small_sample():
    rnd = random.Random(3)
    rows = [(f"R{s % 4}", f"S{s:02d}", i, d) for s in range(40) for i in range(6) for d in range(1, 6)]
    rnd.shuffle(rows)
    cols = list(zip(*rows))
    return pa.table({"region": cols[0], "store": cols[1], "item": cols[2], "day": cols[3]})


def scorer_for(sample, threads=1, compression="zstd", **kw):
    return S._Scorer(sample, ["region", "store", "item", "day"], compression, threads, **kw)


def test_scorer_matches_plain_sort_by(small_sample):
    scorer = scorer_for(small_sample)
    for keys in ORDERS:
        assert scorer(keys) == S._encoded_size(small_sample, keys, "zstd"), keys
    assert scorer([]) == S._encoded_size(small_sample, [], "zstd")


def test_scorer_without_ranks_is_the_plain_path(small_sample):
    plain = scorer_for(small_sample, use_ranks=False)
    ranked = scorer_for(small_sample)
    assert [plain(k) for k in ORDERS] == [ranked(k) for k in ORDERS]


def test_prefetch_threads_give_same_sizes(small_sample):
    orders = ORDERS + [["store", "item"], ["day"], ["item"]]  # one duplicate
    serial, parallel = scorer_for(small_sample, threads=1), scorer_for(small_sample, threads=4)
    serial.prefetch(orders)
    parallel.prefetch(orders)
    assert serial.cache == parallel.cache
    assert serial.tried == parallel.tried == len({tuple(o) for o in orders})


def test_prefetch_computes_each_order_once(small_sample, monkeypatch):
    calls = []
    real = S._encoded_size

    def counting(table, keys, compression, ranks=None):
        calls.append(tuple(keys))
        return real(table, keys, compression, ranks)

    monkeypatch.setattr(S, "_encoded_size", counting)
    scorer = scorer_for(small_sample, threads=3)
    a, b = ["store", "item"], ["day"]
    scorer.prefetch([a, b, a])
    scorer(a)  # already cached: no new computation
    assert sorted(calls) == [("day",), ("store", "item")]


def test_prefetch_propagates_worker_errors(small_sample):
    before = threading.active_count()
    scorer = scorer_for(small_sample, threads=4, compression="no-such-codec")
    with pytest.raises(Exception):
        scorer.prefetch(ORDERS)
    assert scorer.tried == 0  # nothing half-cached
    assert threading.active_count() == before  # the pool was shut down


# --- wiring: threads, dictionary columns, error handling ---------------------


def test_resolve_threads_cases():
    r = S.resolve_threads
    assert r(3, 10**9, 1, cpus=16) == 3  # an explicit value wins
    assert r(0, 10**9, None, cpus=16) == 1 and r(-5, 10**9, None, cpus=16) == 1
    assert r(None, 10**6, 10**12, cpus=16) == 8  # capped at MAX_AUTO_THREADS
    assert r(None, 10**6, 10**12, cpus=2) == 2
    assert r(None, 10**6, None, cpus=16) == 4  # memory unknown
    sample = 10**6
    assert r(None, sample, int(3 * 1.5 * sample / 0.5), cpus=16) == 3  # memory cap
    assert r(None, sample, 1, cpus=16) == 1


def test_threads_do_not_change_the_result(sales):
    assert run(sales, threads=1) == run(sales, threads=4)


def dictionary_file(write):
    """Shuffled day-by-day rows (like `sales`), with the store column dictionary-encoded."""
    rnd = random.Random(2)
    rows = [(f"S{s:02d}", i, d) for s in range(50) for i in range(5) for d in range(8)] * 2
    rnd.shuffle(rows)
    stores, items, days = zip(*rows)
    t = pa.table(
        {"store": pa.array(stores).dictionary_encode(), "item": list(items), "day": list(days)}
    )
    return write(t, "dict.parquet", row_group_size=500)


def test_suggest_works_with_dictionary_columns(write):
    path = dictionary_file(write)
    assert pq.ParquetFile(path).schema_arrow.field("store").type == pa.dictionary(pa.int32(), pa.string())
    s = run(path)
    assert s.best.keys and set(s.best.keys) <= {"store", "item", "day"}


def test_suggest_samples_dictionary_entity_columns(write):
    # A sample smaller than the file takes the entity-subset path, which reads and
    # filters the (dictionary-typed) entity column.
    s = run(dictionary_file(write), sample_rows=600)
    assert s.sample_rows < 4000 and s.best.keys


def test_tiny_but_valid_input_is_fine(write):
    t = pa.table({"g": [i % 2 for i in range(12)], "h": [i % 3 for i in range(12)]})
    path = write(t, "tiny.parquet")
    for threads in (1, 8):
        assert isinstance(run(path, threads=threads), S.Suggestion)


def test_three_row_input_is_a_clean_error(write):
    path = write(pa.table({"g": [1, 1, 2]}), "three.parquet")
    with pytest.raises(S.SuggestError, match="no column is usable"):
        run(path, threads=4)


def test_bad_compression_is_a_clean_error(sales):
    with pytest.raises(S.SuggestError, match="no-such-codec"):
        run(sales, compression="no-such-codec")


def test_progress_reports_thread_count(sales):
    messages = []
    run(sales, threads=2, progress=messages.append)
    assert any("2 thread" in m for m in messages)


# --- CLI: --threads -------------------------------------------------------


def suggest_cli(sales, *extra):
    return main(["suggest-sort", str(sales), "--sample-rows", "15000", "--color", "never", *extra])


def test_cli_threads_option_runs(sales, capsys):
    assert suggest_cli(sales, "--threads", "2") == 0
    assert "2 thread" in capsys.readouterr().err


def test_cli_threads_one_equals_three(sales, capsys):
    assert suggest_cli(sales, "--threads", "1") == 0
    serial = capsys.readouterr().out
    assert suggest_cli(sales, "--threads", "3") == 0
    assert capsys.readouterr().out == serial


@pytest.mark.parametrize("value", ["0", "-1"])
def test_cli_rejects_non_positive_threads(sales, capsys, value):
    with pytest.raises(SystemExit) as exc:
        suggest_cli(sales, "--threads", value)
    assert exc.value.code == 2
    assert "--threads must be at least 1" in capsys.readouterr().err
