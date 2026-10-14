"""Rewrite Parquet files with larger row groups and an optional sort.

Unlike the linter this reads and writes row data. Two modes:

- no sort: streams batches, memory stays near one row group;
- sort: loads everything into memory, so the peak is estimated from sampled
  row groups (footer sizes are encoded sizes and badly underestimate Arrow
  memory for dictionary-encoded strings) and compared with available RAM.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .reader import expand_paths

MiB = 1024 * 1024
TARGET_ROW_GROUP_BYTES = 64 * MiB  # in Parquet-encoded size, what the linter measures
MIN_AUTO_ROWS = 100_000
MAX_AUTO_ROWS = 10_000_000
MAX_ROW_GROUP_MEMORY = 1024 * MiB  # keep a single row group's Arrow footprint modest
SAMPLE_ROW_GROUPS = 8
SORT_PEAK_FACTOR = 2.2  # concatenated table + sorted copy + indices
STREAM_PEAK_FACTOR = 3.5  # buffered batches + concatenation + slice; measured ~3.3
STREAM_MEMORY_SHARE = 0.5  # auto row group size uses at most this share of free RAM


class CompactError(Exception):
    """A problem the user can act on; reported without a traceback."""


@dataclass(frozen=True)
class Plan:
    inputs: list[Path]
    schema: pa.Schema
    rows: int
    row_groups: int
    input_bytes: int
    sort_keys: list[tuple[str, str]]
    row_group_rows: int
    arrow_bytes_per_row: float
    est_peak_bytes: int
    available_bytes: int | None

    @property
    def output_row_groups(self) -> int:
        return -(-self.rows // self.row_group_rows)

    @property
    def fits_in_memory(self) -> bool:
        return self.available_bytes is None or self.est_peak_bytes <= self.available_bytes


def available_memory() -> int | None:
    """MemAvailable from /proc/meminfo; None where it cannot be determined."""
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return None


def parse_sort(spec: str) -> list[tuple[str, str]]:
    """'a,b:desc' -> [('a', 'ascending'), ('b', 'descending')]."""
    keys = []
    for item in filter(None, (part.strip() for part in spec.split(","))):
        name, _, direction = item.partition(":")
        if direction not in ("", "asc", "desc"):
            raise CompactError(f"bad sort direction {direction!r} for {name!r}; use asc or desc")
        keys.append((name, "descending" if direction == "desc" else "ascending"))
    if not keys:
        raise CompactError("--sort-by needs at least one column")
    return keys


def _sample_arrow_bytes_per_row(files: list[pq.ParquetFile]) -> float:
    spots = [(i, g) for i, f in enumerate(files) for g in range(f.metadata.num_row_groups)]
    step = max(1, len(spots) // SAMPLE_ROW_GROUPS)
    total = rows = 0
    for i, g in spots[::step][:SAMPLE_ROW_GROUPS]:
        t = files[i].read_row_group(g)
        total += t.nbytes
        rows += t.num_rows
    return total / rows if rows else 0.0


def make_plan(
    paths: list[str],
    sort_by: list[tuple[str, str]] | None = None,
    row_group_rows: int | None = None,
) -> Plan:
    inputs = list(expand_paths(paths))
    if not inputs:
        raise CompactError("no parquet files found")
    files = []
    for path in inputs:
        try:
            files.append(pq.ParquetFile(path))
        except Exception as exc:
            raise CompactError(f"{path}: {type(exc).__name__}: {exc}") from exc

    schema = files[0].schema_arrow
    for path, f in zip(inputs[1:], files[1:]):
        if not f.schema_arrow.equals(schema, check_metadata=False):
            raise CompactError(f"{path}: schema differs from {inputs[0]}; cannot merge")

    rows = sum(f.metadata.num_rows for f in files)
    if rows == 0:
        raise CompactError("input files contain no rows")
    encoded = sum(
        f.metadata.row_group(g).total_byte_size
        for f in files
        for g in range(f.metadata.num_row_groups)
    )
    arrow_bpr = _sample_arrow_bytes_per_row(files)

    sort_keys = sort_by or []
    for name, _ in sort_keys:
        if name not in schema.names:
            raise CompactError(f"unknown sort column {name!r}; columns: {', '.join(schema.names)}")

    if row_group_rows is None:
        by_target = TARGET_ROW_GROUP_BYTES / (encoded / rows) if encoded else MAX_AUTO_ROWS
        by_memory = MAX_ROW_GROUP_MEMORY / arrow_bpr if arrow_bpr else MAX_AUTO_ROWS
        limits = [max(by_target, MIN_AUTO_ROWS), MAX_AUTO_ROWS, by_memory]
        free = available_memory()
        if free and arrow_bpr and not sort_keys:
            limits.append(STREAM_MEMORY_SHARE * free / (arrow_bpr * STREAM_PEAK_FACTOR))
        row_group_rows = int(min(limits))
    row_group_rows = max(1, min(row_group_rows, rows))

    if sort_keys:
        peak = rows * arrow_bpr * SORT_PEAK_FACTOR
    else:
        peak = row_group_rows * arrow_bpr * STREAM_PEAK_FACTOR
    return Plan(
        inputs=inputs,
        schema=schema,
        rows=rows,
        row_groups=sum(f.metadata.num_row_groups for f in files),
        input_bytes=sum(os.path.getsize(p) for p in inputs),
        sort_keys=sort_keys,
        row_group_rows=row_group_rows,
        arrow_bytes_per_row=arrow_bpr,
        est_peak_bytes=int(peak),
        available_bytes=available_memory(),
    )


def plan_rows(plan: Plan) -> list[tuple[str, str]]:
    """Label/value pairs describing what a run would do."""
    gib = 1024 * MiB
    if plan.sort_keys:
        keys = ", ".join(f"{n} {'desc' if d == 'descending' else 'asc'}" for n, d in plan.sort_keys)
        sort = f"{keys} (in memory)"
    else:
        sort = "none (streaming)"
    avail = "unknown" if plan.available_bytes is None else f"{plan.available_bytes / gib:.1f} GiB"
    return [
        ("Input", f"{len(plan.inputs)} file(s), {plan.row_groups} row groups, "
                  f"{plan.input_bytes / MiB:.1f} MiB"),
        ("Rows", f"{plan.rows:,}"),
        ("Output", f"~{plan.output_row_groups} row group(s) of {plan.row_group_rows:,} rows"),
        ("Sort", sort),
        ("Peak memory", f"~{plan.est_peak_bytes / gib:.1f} GiB estimated, {avail} available"),
    ]


def _write_sorted(plan: Plan, tmp: Path, compression: str) -> None:
    table = pa.concat_tables([pq.ParquetFile(p).read() for p in plan.inputs])
    table = table.sort_by(plan.sort_keys)  # nulls last
    sorting = pq.SortingColumn.from_ordering(plan.schema, plan.sort_keys, null_placement="at_end")
    pq.write_table(
        table,
        tmp,
        row_group_size=plan.row_group_rows,
        compression=compression,
        sorting_columns=sorting,
    )


def _write_streaming(plan: Plan, tmp: Path, compression: str) -> None:
    size = plan.row_group_rows
    buffered: list[pa.RecordBatch] = []
    count = 0
    with pq.ParquetWriter(tmp, plan.schema, compression=compression) as writer:
        for path in plan.inputs:
            for batch in pq.ParquetFile(path).iter_batches(batch_size=min(size, 131072)):
                buffered.append(batch)
                count += batch.num_rows
                while count >= size:
                    table = pa.Table.from_batches(buffered)
                    writer.write_table(table.slice(0, size), row_group_size=size)
                    rest = table.slice(size)
                    buffered, count = rest.to_batches(), rest.num_rows
        if count:
            writer.write_table(pa.Table.from_batches(buffered, schema=plan.schema), row_group_size=size)


def compact(plan: Plan, output: str | os.PathLike[str], compression: str = "zstd",
            overwrite: bool = False, force: bool = False) -> Path:
    """Write the plan to `output`. Input files are never modified."""
    out = Path(output)
    if out.resolve() in {p.resolve() for p in plan.inputs}:
        raise CompactError("output is also an input; refusing to overwrite source data")
    if out.exists() and not overwrite:
        raise CompactError(f"{out} already exists; pass --overwrite to replace it")
    if not out.parent.is_dir():
        raise CompactError(f"output directory {out.parent} does not exist")
    if plan.sort_keys and not plan.fits_in_memory and not force:
        raise CompactError(
            f"sorting needs about {plan.est_peak_bytes / MiB / 1024:.1f} GiB but only "
            f"{plan.available_bytes / MiB / 1024:.1f} GiB is available. Free memory, run on a "
            "larger machine, or drop --sort-by to compact without sorting (streaming). "
            "--force attempts it anyway and may exhaust memory."
        )

    tmp = out.with_name(f".{out.name}.{os.getpid()}.tmp")
    try:
        (_write_sorted if plan.sort_keys else _write_streaming)(plan, tmp, compression)
        written = pq.ParquetFile(tmp)
        if written.metadata.num_rows != plan.rows:
            raise CompactError(
                f"verification failed: wrote {written.metadata.num_rows} rows, expected {plan.rows}"
            )
        if not written.schema_arrow.equals(plan.schema, check_metadata=False):
            raise CompactError("verification failed: output schema differs from input")
        written = None
        os.replace(tmp, out)
    finally:
        tmp.unlink(missing_ok=True)
    return out
