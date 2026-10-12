import pyarrow as pa

from pqlint import Config, lint
from pqlint.reader import read_local

# Thresholds shrunk so tiny fixture files can trigger the size rules.
NO_SIZE_RULES = Config(small_file_bytes=0, row_group_min_bytes=0)


def rules_of(path, cfg=Config()):
    findings, score = lint(read_local(path), cfg)
    return {f.rule: f for f in findings}, score


def test_healthy_file_has_no_findings(write, table):
    found, score = rules_of(write(table, compression="zstd"), NO_SIZE_RULES)
    assert found == {}
    assert score == 100


def test_small_file(write, table):
    found, _ = rules_of(write(table, compression="zstd"))
    assert "small-file" in found


def test_small_file_threshold_is_configurable(write, table):
    found, _ = rules_of(write(table, compression="zstd"), Config(small_file_bytes=1))
    assert "small-file" not in found


def test_row_group_small_ignores_last_group(write, table):
    # 1000 rows / 400 -> groups of 400, 400, 200; the 200 remainder is not judged.
    path = write(table, compression="zstd", row_group_size=400)
    cfg = Config(small_file_bytes=0, row_group_min_bytes=1)
    found, _ = rules_of(path, cfg)
    assert "row-group-small" not in found
    cfg = Config(small_file_bytes=0, row_group_min_bytes=10**9)
    found, _ = rules_of(path, cfg)
    assert "row-group-small" in found
    assert "2 of 3" in found["row-group-small"].message


def test_single_row_group_in_small_file_is_not_double_reported(write, table):
    cfg = Config(row_group_min_bytes=10**9)
    found, _ = rules_of(write(table, compression="zstd"), cfg)
    assert "small-file" in found
    assert "row-group-small" not in found


def test_row_group_large(write, table):
    cfg = Config(small_file_bytes=0, row_group_min_bytes=0, row_group_max_bytes=1)
    found, _ = rules_of(write(table, compression="zstd"), cfg)
    assert "row-group-large" in found


def test_missing_stats_lists_columns(write, table):
    path = write(table, compression="zstd", write_statistics=["id"])
    found, _ = rules_of(path, NO_SIZE_RULES)
    assert found["missing-stats"].columns == ("name",)


def test_uncompressed(write, table):
    found, _ = rules_of(write(table, compression="none"), NO_SIZE_RULES)
    assert found["uncompressed"].columns == ("id", "name")


def test_all_null_column(write):
    t = pa.table({"a": pa.array([1, 2, 3]), "b": pa.array([None] * 3, type=pa.int64())})
    found, _ = rules_of(write(t, compression="zstd"), NO_SIZE_RULES)
    assert found["all-null-column"].columns == ("b",)


def test_all_null_requires_every_row_group(write):
    # Null in the first row group only: not an all-null column.
    t = pa.table({"b": pa.array([None, None, 1, 2], type=pa.int64())})
    found, _ = rules_of(write(t, compression="zstd", row_group_size=2), NO_SIZE_RULES)
    assert "all-null-column" not in found


def test_empty_table_is_not_flagged_all_null(write):
    t = pa.table({"b": pa.array([], type=pa.int64())})
    found, _ = rules_of(write(t, compression="zstd"), NO_SIZE_RULES)
    assert "all-null-column" not in found


def test_dictionary_ineffective_on_high_cardinality(write):
    n = 200_000
    t = pa.table({"uniq": [f"s{i}" for i in range(n)], "low": [i % 5 for i in range(n)]})
    # Uncompressed so the full dictionary stays above the 1 MiB limit on disk.
    found, _ = rules_of(write(t, compression="none"), NO_SIZE_RULES)
    assert found["dictionary-ineffective"].columns == ("uniq",)


def test_score_penalizes_by_severity(write, table):
    # uncompressed (warning) + small-file (warning)
    _, score = rules_of(write(table, compression="none"))
    assert score == 100 - 2 * Config().warning_penalty


def test_score_never_below_zero():
    from pqlint.models import Finding, Severity

    def noisy(info, cfg):
        for _ in range(20):
            yield Finding("x", Severity.WARNING, "m", "r")

    class Stub: ...

    _, score = lint(Stub(), rules=(noisy,))
    assert score == 0
