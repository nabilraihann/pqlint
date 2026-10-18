"""Suggest a sort order that makes the file small, by measuring a sample.

The footer cannot tell which order compresses best: that depends on run
lengths and on how columns depend on each other, which only the data shows.
So this reads data, unlike the linter.

Sampling matters. A sample of whole row groups (or random rows) breaks up the
groups that make sorted data compress well: in a file written day by day, each
(store, item) shows up for a day or two only, and every order looks alike.
Instead an *entity* column is picked (the candidate with the most distinct
values that is not near-unique, e.g. a store code) and all rows of a random
subset of its values are kept, so groups nested inside it stay whole. On a
33.6M-row file the estimates landed within 1-6% of real compacted sizes; a
row-group sample was off by 35% to 3x.

Each candidate order is scored by sorting the sample and writing it in
memory with the chosen codec. Whole orders are compared: start
from the best of a greedy order and the cardinality-ascending/-descending
orders, hill-climb by moving one column at a time, then drop keys that
barely matter (wherever they sit). Searching on a smaller subsample was tried and dropped: with
fewer stores, item-first orders compress unrealistically well (each item repeats
the same short list of stores), so the ranking flipped. Growing an order one best key at a time was tried first
and settled on item,store (15.3 MiB real) instead of dc,store,item,day
(12.9 MiB), because the better order only wins after three or four keys.
"""

from __future__ import annotations

import argparse
import random
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from . import render
from .compact import _count_values
from .reader import expand_paths

PROBE_ROW_GROUPS = 40  # spread row groups read to estimate cardinalities
NEAR_UNIQUE = 0.5  # distinct/rows in the probe above this: an id, useless as a sort key
DEFAULT_SAMPLE_ROWS = 1_000_000
MAX_CLIMB_ROUNDS = 10
MIN_CLIMB_GAIN = 0.005  # a move must shrink the search sample by this much
TRIM_TOLERANCE = 0.01  # drop keys whose removal grows the size by less than this


class SuggestError(Exception):
    pass


@dataclass
class Step:
    keys: list[str]
    sample_bytes: int
    est_bytes: int


@dataclass
class Suggestion:
    inputs: list[Path]
    rows: int
    file_bytes: int
    compression: str
    entity: str
    sample_rows: int
    unsorted: Step
    steps: list[Step]
    distinct: dict[str, int]  # in the probe
    skipped: dict[str, str] = field(default_factory=dict)  # column -> reason
    tried: int = 0

    @property
    def best(self) -> Step:
        return self.steps[-1] if self.steps else self.unsorted


def _probe(files: list[pq.ParquetFile]) -> pa.Table:
    spots = [(f, g) for f in files for g in range(f.metadata.num_row_groups)]
    if len(spots) > PROBE_ROW_GROUPS:
        step = (len(spots) - 1) / (PROBE_ROW_GROUPS - 1)
        spots = [spots[round(i * step)] for i in range(PROBE_ROW_GROUPS)]
    return pa.concat_tables([f.read_row_group(g) for f, g in spots])


def _candidates(schema: pa.Schema, probe: pa.Table, start: list[str]):
    distinct: dict[str, int] = {}
    skipped: dict[str, str] = {}
    keep: list[str] = []
    for f in schema:
        t = f.type
        if pa.types.is_floating(t):
            skipped[f.name] = "floating point (a measure, not a key)"
            continue
        if pa.types.is_nested(t) or pa.types.is_null(t):
            skipped[f.name] = "nested or null type"
            continue
        n = len(pc.unique(probe[f.name]))
        distinct[f.name] = n
        if f.name in start:
            continue
        if n <= 1:
            skipped[f.name] = "constant in the sample"
        elif n / max(probe.num_rows, 1) > NEAR_UNIQUE:
            skipped[f.name] = "nearly unique (an id): no repeats to compress"
        else:
            keep.append(f.name)
    # Columns that group rows identically (item code and item name) sort the same,
    # so trying both only doubles the search. Keep the first of each pair.
    for name in list(keep):
        for other in keep:
            if other == name or distinct[other] != distinct[name]:
                continue
            if keep.index(other) > keep.index(name):
                continue
            pairs = probe.group_by([other, name]).aggregate([]).num_rows
            if pairs == distinct[name]:
                skipped[name] = f"same grouping as {other!r}; sorting by it adds nothing"
                keep.remove(name)
                break
    return keep, distinct, skipped


def _entity_sample(inputs: list[Path], entity: str, fraction: float, seed: int) -> pa.Table:
    if fraction >= 1:
        return pa.concat_tables([pq.ParquetFile(p).read() for p in inputs])
    values, _, _ = _count_values(inputs, entity, "ascending")
    rnd = random.Random(seed)
    picked = [i for i in range(len(values)) if rnd.random() < fraction] or [0]
    chosen = values.take(pa.array(picked))
    parts = []
    for path in inputs:
        for batch in pq.ParquetFile(path).iter_batches(batch_size=1 << 20):
            parts.append(batch.filter(pc.is_in(batch.column(entity), value_set=chosen)))
    return pa.Table.from_batches(parts)


def _encoded_size(table: pa.Table, keys: list[str], compression: str) -> int:
    if keys:
        table = table.sort_by([(k, "ascending") for k in keys])
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression=compression, row_group_size=max(table.num_rows, 1))
    return sink.getvalue().size


def suggest(
    paths: list[str],
    sample_rows: int = DEFAULT_SAMPLE_ROWS,
    compression: str = "zstd",
    start_with: list[str] | None = None,
    max_keys: int = 6,
    min_gain: float = 0.02,
    seed: int = 0,
    progress=None,
) -> Suggestion:
    inputs = list(expand_paths(paths))
    if not inputs:
        raise SuggestError("no parquet files found")
    try:
        files = [pq.ParquetFile(p) for p in inputs]
    except Exception as exc:
        raise SuggestError(f"{type(exc).__name__}: {exc}") from exc
    schema = files[0].schema_arrow
    rows = sum(f.metadata.num_rows for f in files)
    if rows == 0:
        raise SuggestError("input files contain no rows")
    start = start_with or []
    for name in start:
        if name not in schema.names:
            raise SuggestError(f"unknown column {name!r}; columns: {', '.join(schema.names)}")

    probe = _probe(files)
    candidates, distinct, skipped = _candidates(schema, probe, start)
    pool = candidates + [c for c in start if c not in candidates]
    if not pool:
        raise SuggestError("no column is usable as a sort key (all floating, unique or constant)")
    entity = max(pool, key=lambda c: (distinct[c], c))
    fraction = min(1.0, sample_rows / rows)
    if progress:
        progress(f"sampling all rows of ~{fraction:.1%} of the {entity!r} values")
    sample = _entity_sample(inputs, entity, fraction, seed)
    scale = rows / max(sample.num_rows, 1)

    search = sample  # a smaller subsample biased the ranking; see the module docstring
    scores: dict[tuple[str, ...], int] = {}

    def score(keys: list[str]) -> int:
        """Encoded size of the (smaller) search sample sorted by `keys`."""
        key = tuple(keys)
        if key not in scores:
            scores[key] = _encoded_size(search, list(key), compression)
        return scores[key]

    # Whole orders are compared, not prefixes: the best order (coarse to fine,
    # e.g. dc, store, item, day) only pulls ahead after three or four keys, so
    # growing an order one best key at a time stopped on a worse one.
    movable = [c for c in candidates if c not in start]
    greedy: list[str] = []
    rest = list(movable)
    while rest:
        if progress:
            progress(f"greedy start: key {len(greedy) + 1} of {len(movable)}")
        pick = min(rest, key=lambda c: (score(start + greedy + [c]), c))
        greedy.append(pick)
        rest.remove(pick)
    by_card = sorted(movable, key=lambda c: (distinct[c], c))
    seeds = [greedy, by_card, by_card[::-1]]
    order = min(seeds, key=lambda o: (score(start + o), o))

    for round_ in range(MAX_CLIMB_ROUNDS):
        neighbours = []
        for i in range(len(order)):
            for j in range(len(order)):
                if i != j:
                    o = order[:i] + order[i + 1:]
                    o.insert(j, order[i])
                    neighbours.append(o)
        if not neighbours:
            break
        if progress:
            progress(f"improving: round {round_ + 1}, {len(neighbours)} neighbouring orders, "
                     f"current ~{score(start + order) * scale / 2**20:.1f} MiB")
        cand = min(neighbours, key=lambda o: (score(start + o), o))
        if score(start + cand) >= score(start + order) * (1 - MIN_CLIMB_GAIN):
            break
        order = cand

    # Drop keys that barely change the size (a store name after the store code, or
    # trailing keys): they only slow the compaction down. Start keys stay.
    final = start + order
    changed = True
    while changed:
        changed = False
        for k in final[len(start):]:
            shorter = [c for c in final if c != k]
            if shorter and score(shorter) <= score(final) * (1 + TRIM_TOLERANCE):
                final = shorter
                changed = True
                break
    final = final[: max(max_keys, len(start))]

    def measure(keys: list[str]) -> Step:
        size = score(keys)
        return Step(list(keys), size, int(size * scale))

    unsorted = measure([])
    steps = [measure(final[:n]) for n in range(max(len(start), 1), len(final) + 1)]
    if steps and steps[-1].sample_bytes >= unsorted.sample_bytes * (1 - min_gain):
        steps = []  # sorting does not pay off
    tried = len(scores)

    return Suggestion(
        inputs=inputs,
        rows=rows,
        file_bytes=sum(p.stat().st_size for p in inputs),
        compression=compression,
        entity=entity,
        sample_rows=sample.num_rows,
        unsorted=unsorted,
        steps=steps,
        distinct=distinct,
        skipped=skipped,
        tried=tried,
    )


def render_suggestion(st: render.Style, s: Suggestion, width: int | None = None) -> str:
    lines = [render.heading(st, "Sort suggestion (for file size)")]
    lines += render.kv(
        st,
        [
            ("Input", f"{len(s.inputs)} file(s), {s.rows:,} rows, {render.human_size(s.file_bytes)}"),
            (
                "Sample",
                f"{s.sample_rows:,} rows: every row of a random subset of {s.entity!r} values",
            ),
            ("Compression", s.compression),
            ("Tried", f"{s.tried} orders"),
        ],
    )
    lines.append("")
    rows = [["0", ("(unsorted)", ("dim",)), render.human_size(s.unsorted.est_bytes), "-"]]
    for i, step in enumerate(s.steps, 1):
        gain = 1 - step.est_bytes / s.unsorted.est_bytes if s.unsorted.est_bytes else 0
        style = ("green", "bold") if step is s.best else ()
        rows.append([str(i), (",".join(step.keys), style), render.human_size(step.est_bytes), f"{-gain:+.0%}"])
    lines += render.table(st, ["step", "sort keys", "est. size", "vs unsorted"], rows, "><>>", width)
    lines.append("")
    if s.steps:
        keys = ",".join(s.best.keys)
        target = s.inputs[0] if len(s.inputs) == 1 else "<inputs>"
        lines.append(f"  {st('Recommended', 'bold')}  {st('--sort-by ' + keys, 'green', 'bold')}")
        lines.append(
            f"  {st('Run', 'dim')}          pqlint compact {target} -o <output> "
            f"--sort-by {keys}" + ("" if s.compression == "zstd" else f" --compression {s.compression}")
        )
    else:
        lines.append("  " + st("No sort order shrinks the sample noticeably.", "yellow"))
    lines.append(
        "  "
        + st(
            "Sizes are scaled from the sample and were within about 5% of real compacted "
            "sizes in testing, so orders that close are a tie. If queries filter on one "
            "column, consider putting it first (--start-with) so engines can skip row groups.",
            "dim",
        )
    )
    if s.skipped:
        lines.append("")
        lines += render.table(
            st, ["skipped column", "reason"], [[c, (r, ("dim",))] for c, r in s.skipped.items()], "<", width
        )
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="pqlint suggest-sort",
        description="Suggest a --sort-by order that makes the file smallest, by sorting and "
        "compressing a sample. Reads row data (unlike the linter); writes nothing.",
    )
    parser.add_argument("inputs", nargs="+", help="Parquet files or directories")
    parser.add_argument(
        "--sample-rows", type=int, default=DEFAULT_SAMPLE_ROWS, metavar="N",
        help=f"approximate sample size (default {DEFAULT_SAMPLE_ROWS:,})",
    )
    parser.add_argument("--compression", default="zstd", help="codec to measure with (default zstd)")
    parser.add_argument(
        "--start-with", metavar="COLS",
        help="fixed leading keys, e.g. the column queries filter on most",
    )
    parser.add_argument("--max-keys", type=int, default=6, metavar="N", help="longest order to build")
    parser.add_argument("--seed", type=int, default=0, help="sampling seed (default 0)")
    parser.add_argument("--color", choices=["auto", "always", "never"], default="auto")
    args = parser.parse_args(argv)
    st = render.Style(render.use_color(args.color))
    start = [c.strip() for c in args.start_with.split(",") if c.strip()] if args.start_with else None

    def progress(msg: str) -> None:
        print(st(f"… {msg}", "dim"), file=sys.stderr)

    try:
        s = suggest(
            args.inputs, args.sample_rows, args.compression, start, args.max_keys,
            seed=args.seed, progress=progress,
        )
    except SuggestError as exc:
        print(f"{st('pqlint suggest-sort:', 'red', 'bold')} {exc}", file=sys.stderr)
        return 2
    print(render_suggestion(st, s))
    return 0
