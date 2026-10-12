import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@pytest.fixture
def write(tmp_path):
    """Write a small table to a parquet file and return its path."""

    def _write(table, name="t.parquet", **kwargs):
        path = tmp_path / name
        pq.write_table(table, path, **kwargs)
        return path

    return _write


@pytest.fixture
def table():
    n = 1000
    return pa.table(
        {
            "id": pa.array(range(n)),
            "name": pa.array([f"n{i}" for i in range(n)]),
        }
    )
