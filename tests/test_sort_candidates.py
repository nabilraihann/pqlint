import random

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pqlint import Config, lint
from pqlint.reader import read_local
from pqlint.rules import _overlap

NO_SIZE = Config(small_file_bytes=0, row_group_min_bytes=0)
N = 400  # 8 row groups of 50


def sort_finding(path, cfg=NO_SIZE):
    findings, score = lint(read_local(path), cfg)
    return next((f for f in findings if f.rule == "sort-candidates"), None), score


def shuffled(n=N, seed=0):
    values = list(range(n))
    random.Random(seed).shuffle(values)
    return values


@pytest.fixture
def make(write):
    def _make(cols, **kw):
        kw.setdefault("row_group_size", 50)
        kw.setdefault("compression", "zstd")
        return write(pa.table(cols), **kw)

    return _make


def test_overlap_extremes():
    disjoint = [(0, 9), (10, 19), (20, 29), (30, 39)]
    identical = [(0, 9)] * 4
    assert _overlap(disjoint) == 0
    assert _overlap(identical) == 1


def test_overlap_works_for_strings():
    assert _overlap([("a", "b"), ("c", "d"), ("e", "f")]) == 0
    assert _overlap([("a", "z")] * 3) == 1


def test_sorted_column_is_not_suggested(make):
    f, _ = sort_finding(make({"ts": list(range(N))}))
    assert f is None


def test_shuffled_column_is_suggested(make):
    f, _ = sort_finding(make({"ts": list(range(N)), "user": shuffled()}))
    assert f.columns == ("user",)
    assert f.severity.value == "hint"
    assert 0.5 <= f.details["user"] <= 1


def test_candidates_ranked_by_overlap(make):
    # "mild" is mostly clustered with some shuffling; "wild" is fully shuffled.
    mild = [i + random.Random(1).randint(0, 120) for i in range(N)]
    f, _ = sort_finding(make({"wild": shuffled(), "mild": mild}), Config(
        small_file_bytes=0, row_group_min_bytes=0, sort_overlap_threshold=0.1))
    assert f.columns[0] == "wild"


def test_string_column(make):
    names = [f"u{v:04d}" for v in shuffled()]
    f, _ = sort_finding(make({"name": names}))
    assert f.columns == ("name",)


def test_constant_column_is_skipped(make):
    f, _ = sort_finding(make({"c": [7] * N}))
    assert f is None


def test_low_cardinality_column_is_suggested(make):
    # Few distinct values spread across all row groups: sorting would cluster them.
    f, _ = sort_finding(make({"region": [i % 3 for i in range(N)]}))
    assert f.columns == ("region",)


def test_boolean_column_is_skipped(make):
    f, _ = sort_finding(make({"flag": [i % 2 == 0 for i in range(N)]}))
    assert f is None


def test_too_few_row_groups(make):
    f, _ = sort_finding(make({"x": shuffled(150)}, row_group_size=50))  # 3 groups
    assert f is None


def test_column_with_a_stats_gap_is_skipped(make):
    f, _ = sort_finding(make({"x": shuffled(), "y": shuffled(seed=2)}, write_statistics=["y"]))
    assert f.columns == ("y",)


def test_nan_values_do_not_break_judging(make):
    # pyarrow leaves NaN out of min/max, so the column is still judged normally.
    vals = [float(v) for v in shuffled()]
    vals[0] = float("nan")
    f, _ = sort_finding(make({"x": vals}))
    assert f.columns == ("x",)


def test_list_column_is_skipped(make):
    lists = [[v] for v in shuffled()]
    f, _ = sort_finding(make({"l": lists}))
    assert f is None


def test_declared_sorting_column_is_skipped(make):
    path = make({"a": shuffled(), "b": shuffled(seed=3)}, sorting_columns=[pq.SortingColumn(0)])
    f, _ = sort_finding(path)
    assert f.columns == ("b",)


def test_max_suggestions_cap(make):
    cols = {f"c{i}": shuffled(seed=i) for i in range(4)}
    f, _ = sort_finding(make(cols), Config(
        small_file_bytes=0, row_group_min_bytes=0, sort_max_suggestions=2))
    assert len(f.columns) == 2


def test_hint_does_not_lower_score(make):
    path = make({"user": shuffled()})
    f, score = sort_finding(path)
    assert f is not None
    assert score == 100


def test_empty_file(make):
    f, _ = sort_finding(make({"x": pa.array([], type=pa.int64())}))
    assert f is None
