import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pqlint.models import FileReport, Finding, Severity
from pqlint.report import lint_path
from pqlint.summary import partition_values, summarize


def report(path, size, rows, score, rules=()):
    return FileReport(
        path=path,
        size=size,
        num_rows=rows,
        score=score,
        findings=[Finding(r, Severity.WARNING, "m", "r") for r in rules],
    )


def test_totals_and_size_distribution():
    s = summarize([report("a", 10, 100, 100), report("b", 30, 50, 80), report("c", 20, 5, 90)])
    assert (s.files, s.errors, s.total_rows, s.total_bytes) == (3, 0, 155, 60)
    assert (s.min_bytes, s.median_bytes, s.max_bytes) == (10, 20, 30)
    assert s.mean_score == 90.0


def test_files_per_rule_counts_files_not_findings():
    s = summarize([
        report("a", 1, 1, 80, ["x", "x", "y"]),  # duplicate rule in one file counts once
        report("b", 1, 1, 90, ["x"]),
    ])
    assert s.files_per_rule == {"x": 2, "y": 1}


def test_worst_files_sorted_and_perfect_scores_excluded():
    s = summarize([report("a", 1, 1, 100), report("b", 1, 1, 70), report("c", 1, 1, 50)])
    assert s.worst_files == [("c", 50), ("b", 70)]


def test_errors_are_counted_and_excluded_from_stats():
    bad = FileReport(path="bad", error="boom")
    s = summarize([report("a", 10, 1, 100), bad])
    assert (s.files, s.errors, s.total_bytes) == (2, 1, 10)


def test_all_files_unreadable():
    s = summarize([FileReport(path="bad", error="boom")])
    assert (s.files, s.errors, s.mean_score, s.median_bytes) == (1, 1, None, None)


def test_empty_input():
    assert summarize([]).files == 0


@pytest.mark.parametrize(
    "path, expected",
    [
        ("/d/year=2024/month=01/f.parquet", {"year": "2024", "month": "01"}),
        ("/d/plain/f.parquet", {}),
        ("/d/=x/f.parquet", {}),
        ("/d/k=a=b/f.parquet", {"k": "a=b"}),
        ("/d/f=1.parquet", {}),  # the file name itself is not a partition
    ],
)
def test_partition_values(path, expected):
    assert partition_values(path) == expected


def test_partition_cardinality(tmp_path):
    t = pa.table({"a": [1]})
    for day in ("2024-01-01", "2024-01-02"):
        d = tmp_path / f"dt={day}" / "region=eu"
        d.mkdir(parents=True)
        for part in ("p0", "p1"):
            pq.write_table(t, d / f"{part}.parquet")
    s = summarize([lint_path(p) for p in sorted(tmp_path.rglob("*.parquet"))])
    assert s.files == 4
    assert s.partitions == {"dt": 2, "region": 1}
