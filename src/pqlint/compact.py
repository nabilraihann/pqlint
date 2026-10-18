"""Rewrite Parquet files with larger row groups and an optional sort.

Unlike the linter this reads and writes row data. Three strategies:

- streaming (no sort): batches flow through, memory stays near one row group;
- memory (sort that fits): everything is loaded and sorted at once;
- bucketed (sort that does not fit): rows are split into temporary files by
  ranges of the first sort column, then each bucket is loaded and sorted on
  its own and appended in order. The result is identical to the in-memory
  sort; memory is bounded by the largest bucket instead of the whole input.

Memory is estimated from sampled row groups, because footer sizes are encoded
sizes and badly underestimate Arrow memory for dictionary-encoded strings.
"""

from __future__ import annotations

import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .fmt import human_size
from .reader import expand_paths

MiB = 1024 * 1024
GiB = 1024 * MiB
TARGET_ROW_GROUP_BYTES = 64 * MiB  # in Parquet-encoded size, what the linter measures
MIN_AUTO_ROWS = 100_000
MAX_AUTO_ROWS = 10_000_000
MAX_ROW_GROUP_MEMORY = 1024 * MiB  # keep a single row group's Arrow footprint modest
SAMPLE_ROW_GROUPS = 8
SORT_PEAK_FACTOR = 2.2  # loaded table + sorted copy + indices
STREAM_PEAK_FACTOR = 3.5  # buffered batches + concatenation + slice; measured ~3.3
STREAM_MEMORY_SHARE = 0.5  # auto row group size uses at most this share of free RAM
BUCKET_MEMORY_SHARE = 0.6  # a bucket's sort may use at most this share of free RAM
BUCKET_WRITER_FACTOR = 2.0  # output row group buffer next to a sorted bucket
PROCESS_BASELINE = 200 * MiB  # interpreter, pyarrow, writers; a 4.3M-row bucket measured
#                               2.63 GiB peak RSS against 2.1 GiB estimated before this
MAX_BUCKETS = 512  # pass 1 filters every batch once per bucket
MAX_BUCKET_KEYS = 1_000_000  # distinct values of the first sort column we are willing to count
BATCH_ROWS = 131_072

STREAMING, MEMORY, BUCKETED = "streaming", "memory", "bucketed"


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
    strategy: str = STREAMING
    # Bucketed only. bucket_values lists the distinct non-null values of the
    # first sort column in output order; bucket_index[i] is the bucket of
    # bucket_values[i]. Nulls go to one extra bucket written last.
    bucket_values: pa.Array | None = None
    bucket_index: pa.Array | None = None
    bucket_rows: tuple[int, ...] = ()
    null_rows: int = 0
    problem: str | None = None  # why the plan cannot run within memory, if it cannot
    memory_from_limit: bool = False  # available_bytes came from --memory-limit

    @property
    def output_row_groups(self) -> int:
        return -(-self.rows // self.row_group_rows)

    @property
    def fits_in_memory(self) -> bool:
        if self.problem:
            return False
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


def parse_size(text: str) -> int:
    """'512M', '2G', '1.5GiB', '1000000' -> bytes (binary units)."""
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([kmgt]?)(?:i?b)?\s*", text, re.IGNORECASE)
    if not m:
        raise CompactError(f"bad size {text!r}; use e.g. 512M or 2G")
    power = "bkmgt".index(m.group(2).lower() or "b")
    return int(float(m.group(1)) * 1024**power)


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


def _count_values(inputs: list[Path], column: str, order: str):
    """Distinct non-null values of `column` in output order, their counts, and the null count.

    Reads only that column. Returns None if there are too many distinct values.
    """
    counts: dict = {}
    nulls = 0
    value_type = None
    for path in inputs:
        for batch in pq.ParquetFile(path).iter_batches(columns=[column], batch_size=1 << 20):
            col = batch.column(0)
            value_type = col.type
            nulls += col.null_count
            for item in pc.value_counts(col).to_pylist():
                if item["values"] is not None:
                    counts[item["values"]] = counts.get(item["values"], 0) + item["counts"]
            if len(counts) > MAX_BUCKET_KEYS:
                return None
    values = pa.array(list(counts), type=value_type)
    # Arrow decides the order so it matches Table.sort_by exactly.
    ordered = values.take(pc.array_sort_indices(values, order=order))
    return ordered, [counts[v] for v in ordered.to_pylist()], nulls


def _bucket(plan_args: dict, inputs: list[Path], sort_keys, avail: int, bpr: float, rg_rows: int):
    """Fill the bucketed fields of a plan: group consecutive key values into buckets."""
    first, order = sort_keys[0]
    counted = _count_values(inputs, first, order)
    if counted is None:
        plan_args["problem"] = (
            f"{first!r} has more than {MAX_BUCKET_KEYS:,} distinct values, too many to split "
            "the sort into buckets; sort by a coarser column first"
        )
        return
    values, counts, nulls = counted
    overhead = rg_rows * bpr * BUCKET_WRITER_FACTOR + PROCESS_BASELINE
    max_rows = max(1, int((BUCKET_MEMORY_SHARE * avail - overhead) / (bpr * SORT_PEAK_FACTOR)))

    index: list[int] = []
    rows_per: list[int] = []
    for n in counts:
        if not rows_per or rows_per[-1] + n > max_rows:
            rows_per.append(0)
        rows_per[-1] += n
        index.append(len(rows_per) - 1)

    largest = max(rows_per + [nulls])
    plan_args.update(
        strategy=BUCKETED,
        bucket_values=values,
        bucket_index=pa.array(index, type=pa.int32()),
        bucket_rows=tuple(rows_per),
        null_rows=nulls,
        est_peak_bytes=int(largest * bpr * SORT_PEAK_FACTOR + overhead),
    )
    if overhead >= avail:
        plan_args["problem"] = (
            f"the memory limit ({human_size(avail)}) is below what the compaction needs before "
            f"sorting anything (~{human_size(overhead)} for the output buffer and the process)"
        )
    elif len(rows_per) > MAX_BUCKETS:
        plan_args["problem"] = (
            f"the sort would need {len(rows_per)} buckets (limit {MAX_BUCKETS}); "
            "free memory or raise --memory-limit"
        )
    elif largest * bpr * SORT_PEAK_FACTOR + overhead > avail:
        # max_rows only guides how values are grouped; a bucket larger than that is
        # still fine as long as the real peak fits in memory.
        if nulls == largest:
            what = f"the null values of {first!r}"
        else:
            what = f"the value {values[counts.index(largest)].as_py()!r} of {first!r}"
        plan_args["problem"] = (
            f"{what} alone has {largest:,} rows, about "
            f"{human_size(largest * bpr * SORT_PEAK_FACTOR + overhead)} to sort, more than "
            f"{human_size(avail)}, and one value cannot be split across buckets"
        )


def make_plan(
    paths: list[str],
    sort_by: list[tuple[str, str]] | None = None,
    row_group_rows: int | None = None,
    memory_limit: int | None = None,
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
    bpr = _sample_arrow_bytes_per_row(files)

    sort_keys = sort_by or []
    for name, _ in sort_keys:
        if name not in schema.names:
            raise CompactError(f"unknown sort column {name!r}; columns: {', '.join(schema.names)}")

    avail = memory_limit if memory_limit is not None else available_memory()
    full_sort_peak = rows * bpr * SORT_PEAK_FACTOR + PROCESS_BASELINE
    if not sort_keys:
        strategy = STREAMING
    elif avail is None or full_sort_peak <= avail:
        strategy = MEMORY
    else:
        strategy = BUCKETED

    if row_group_rows is None:
        by_target = TARGET_ROW_GROUP_BYTES / (encoded / rows) if encoded else MAX_AUTO_ROWS
        by_memory = MAX_ROW_GROUP_MEMORY / bpr if bpr else MAX_AUTO_ROWS
        limits = [max(by_target, MIN_AUTO_ROWS), MAX_AUTO_ROWS, by_memory]
        if avail and bpr and strategy != MEMORY:
            limits.append(STREAM_MEMORY_SHARE * avail / (bpr * STREAM_PEAK_FACTOR))
        row_group_rows = int(min(limits))
    row_group_rows = max(1, min(row_group_rows, rows))

    args = dict(
        inputs=inputs,
        schema=schema,
        rows=rows,
        row_groups=sum(f.metadata.num_row_groups for f in files),
        input_bytes=sum(os.path.getsize(p) for p in inputs),
        sort_keys=sort_keys,
        row_group_rows=row_group_rows,
        arrow_bytes_per_row=bpr,
        est_peak_bytes=int(
            full_sort_peak
            if strategy == MEMORY
            else row_group_rows * bpr * STREAM_PEAK_FACTOR + PROCESS_BASELINE
        ),
        available_bytes=avail,
        strategy=strategy,
        memory_from_limit=memory_limit is not None,
    )
    if strategy == BUCKETED:
        _bucket(args, inputs, sort_keys, avail, bpr, row_group_rows)
    return Plan(**args)


def plan_rows(plan: Plan) -> list[tuple[str, str]]:
    """Label/value pairs describing what a run would do."""
    keys = ", ".join(f"{n} {'desc' if d == 'descending' else 'asc'}" for n, d in plan.sort_keys)
    if plan.strategy == MEMORY:
        sort = f"{keys} (in memory)"
    elif plan.strategy == BUCKETED:
        n = len(plan.bucket_rows) + (1 if plan.null_rows else 0)
        largest = max(plan.bucket_rows + (plan.null_rows,))
        sort = (
            f"{keys} (bucketed by {plan.sort_keys[0][0]}: {n} bucket(s), "
            f"largest {largest:,} rows)"
        )
    else:
        sort = "none (streaming)"
    if plan.available_bytes is None:
        avail = "unknown available"
    elif plan.memory_from_limit:
        avail = f"{human_size(plan.available_bytes)} limit"
    else:
        avail = f"{human_size(plan.available_bytes)} available"
    rows = [
        ("Input", f"{len(plan.inputs)} file(s), {plan.row_groups} row groups, "
                  f"{plan.input_bytes / MiB:.1f} MiB"),
        ("Rows", f"{plan.rows:,}"),
        ("Output", f"~{plan.output_row_groups} row group(s) of {plan.row_group_rows:,} rows"),
        ("Sort", sort),
    ]
    if plan.strategy == BUCKETED:
        rows.append(("Temp disk", f"~{plan.input_bytes / MiB:.0f} MiB next to the output"))
    if plan.problem:
        rows.append(("Problem", plan.problem))
    rows.append(("Peak memory", f"~{human_size(plan.est_peak_bytes)} estimated, {avail}"))
    return rows


class _RowGroupWriter:
    """Feeds a ParquetWriter row groups of exactly `size` rows (the last may be shorter)."""

    def __init__(self, writer: pq.ParquetWriter, size: int) -> None:
        self.writer = writer
        self.size = size
        self.buffered: list[pa.RecordBatch] = []
        self.count = 0

    def add(self, data: pa.Table | pa.RecordBatch) -> None:
        self.buffered += data.to_batches() if isinstance(data, pa.Table) else [data]
        self.count += data.num_rows
        while self.count >= self.size:
            table = pa.Table.from_batches(self.buffered)
            self.writer.write_table(table.slice(0, self.size), row_group_size=self.size)
            rest = table.slice(self.size)
            self.buffered, self.count = rest.to_batches(), rest.num_rows

    def flush(self) -> None:
        if self.count:
            table = pa.Table.from_batches(self.buffered)
            self.writer.write_table(table, row_group_size=self.size)
            self.buffered, self.count = [], 0


def _sorting_columns(plan: Plan):
    return pq.SortingColumn.from_ordering(plan.schema, plan.sort_keys, null_placement="at_end")


def _write_sorted(plan: Plan, tmp: Path, compression: str) -> None:
    table = pa.concat_tables([pq.ParquetFile(p).read() for p in plan.inputs])
    table = table.sort_by(plan.sort_keys)  # nulls last
    pq.write_table(
        table,
        tmp,
        row_group_size=plan.row_group_rows,
        compression=compression,
        sorting_columns=_sorting_columns(plan),
    )


def _write_streaming(plan: Plan, tmp: Path, compression: str) -> None:
    with pq.ParquetWriter(tmp, plan.schema, compression=compression) as writer:
        out = _RowGroupWriter(writer, plan.row_group_rows)
        for path in plan.inputs:
            for batch in pq.ParquetFile(path).iter_batches(batch_size=BATCH_ROWS):
                out.add(batch)
        out.flush()


def _write_bucketed(plan: Plan, tmp: Path, compression: str) -> None:
    first = plan.sort_keys[0][0]
    workdir = tmp.with_name(tmp.name + ".buckets")
    workdir.mkdir()
    try:
        # Pass 1: route every row to the temporary file of its bucket. Filtering
        # keeps the input order, and Arrow's sort is stable, so sorting each
        # bucket reproduces the global sort exactly.
        null_bucket = len(plan.bucket_rows)
        paths = [workdir / f"{i:04d}.parquet" for i in range(null_bucket + 1)]
        writers: dict[int, pq.ParquetWriter] = {}
        try:
            for path in plan.inputs:
                for batch in pq.ParquetFile(path).iter_batches(batch_size=BATCH_ROWS):
                    key = batch.column(first)
                    ids = pc.take(plan.bucket_index, pc.index_in(key, value_set=plan.bucket_values))
                    if ids.null_count != key.null_count:
                        raise CompactError(f"{path} changed while compacting; rerun")
                    for b in pc.unique(ids).to_pylist():
                        mask = pc.is_null(key) if b is None else pc.equal(ids, b)
                        i = null_bucket if b is None else b
                        if i not in writers:
                            writers[i] = pq.ParquetWriter(paths[i], plan.schema, compression="zstd")
                        writers[i].write_batch(batch.filter(mask))
        finally:
            for w in writers.values():
                w.close()

        # Pass 2: sort one bucket at a time and append it to the output.
        with pq.ParquetWriter(
            tmp, plan.schema, compression=compression, sorting_columns=_sorting_columns(plan)
        ) as writer:
            out = _RowGroupWriter(writer, plan.row_group_rows)
            for i in sorted(writers):
                table = pq.ParquetFile(paths[i]).read().sort_by(plan.sort_keys)
                paths[i].unlink()
                out.add(table)
                del table
            out.flush()
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


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
        reason = plan.problem or (
            f"sorting needs about {human_size(plan.est_peak_bytes)} but only "
            f"{human_size(plan.available_bytes)} is available"
        )
        raise CompactError(
            f"{reason}. Free memory, run on a larger machine, or drop --sort-by to compact "
            "without sorting (streaming). --force attempts it anyway and may exhaust memory."
        )
    if plan.strategy == BUCKETED:
        free = shutil.disk_usage(out.parent).free
        if free < 2.5 * plan.input_bytes:
            raise CompactError(
                f"bucketed sort needs about {2.5 * plan.input_bytes / GiB:.1f} GiB of free disk "
                f"in {out.parent} (temporary buckets plus output), only {free / GiB:.1f} GiB free"
            )

    write = {STREAMING: _write_streaming, MEMORY: _write_sorted, BUCKETED: _write_bucketed}
    tmp = out.with_name(f".{out.name}.{os.getpid()}.tmp")
    try:
        write[plan.strategy](plan, tmp, compression)
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
