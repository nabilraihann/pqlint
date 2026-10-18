import random

import pyarrow as pa
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
