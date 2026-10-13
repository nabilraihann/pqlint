# pqlint

Lint Parquet files using only their footer metadata. No row data is read.

```
uv sync
uv run pqlint path/to/file.parquet          # a file, or a directory (recursive)
uv run pqlint data/ --summary-only          # dataset-level summary only
uv run pqlint data/ --json                  # machine-readable: {"summary": ..., "files": [...]}
uv run pqlint data/ --fail-under 80         # exit 1 if any file scores below 80
```

Each file gets a score from 0 to 100 and a list of findings with recommendations.
When more than one file is scanned, a dataset summary follows: file count, total rows and
bytes, file size min/median/max, mean score, how many files each rule affects, the lowest
scoring files, and the number of distinct values per Hive-style partition key (`k=v`
directories). Unreadable files are counted and left out of the statistics.

Exit status: 0 on success, 1 if a file scores below `--fail-under`, 2 if any file could not
be read or no Parquet files were found.

| Rule | Severity | Meaning |
|---|---|---|
| `small-file` | warning | file is below 8 MiB |
| `row-group-small` / `row-group-large` | warning | row groups under 32 MiB *and* under 1M rows, or over 1 GiB (uncompressed) |
| `missing-stats` | warning | column chunks without min/max statistics |
| `uncompressed` | warning | column stored without a compression codec |
| `dictionary-ineffective` | info | dictionary page of 1 MiB or more (heuristic) |
| `all-null-column` | info | column contains only nulls |
| `sort-candidates` | hint | columns whose row group min/max ranges overlap heavily (see below); never lowers the score |

`sort-candidates` ranks columns by the fraction of row group pairs whose min/max ranges
intersect (0% = every row group holds its own range, so engines can skip them; 100% = every
pair overlaps, so a filter on that column skips nothing). A high percentage means sorting or clustering
by that column would help *if queries filter on it*; the footer cannot know that, so treat the
list as candidates. Skipped: columns without stats in every row group, constant and boolean
columns, list columns, columns already declared sorted in the footer, and files with fewer
than 4 row groups.

Thresholds live in `pqlint.Config`.

```
uv run pytest -q
```

## Compaction

Unlike linting, `compact` reads and writes row data. It merges files (same schema) into one
output file with larger row groups, optionally sorted. Inputs are never modified.

```
uv run pqlint compact data/ -o merged.parquet                       # streaming, low memory
uv run pqlint compact data/ -o merged.parquet --sort-by dc,store    # sort (in memory)
uv run pqlint compact in.parquet -o out.parquet --sort-by ts:desc --row-group-rows 1000000
uv run pqlint compact data/ -o merged.parquet --sort-by dc --dry-run   # plan only
```

- Without `--sort-by` batches are streamed, so memory stays near one row group, and the row
  group size shrinks automatically if free RAM is low.
- With `--sort-by` everything is loaded into memory (nulls last, `:desc` for descending). The
  peak is estimated from sampled row groups and compared with available RAM; the run is refused
  if it does not fit, unless `--force`. The sort is declared in the footer (`sorting_columns`).
- The output is written to a temporary file, checked for row count and schema, then renamed.
  An existing output needs `--overwrite`, and the output may not be one of the inputs.
- Other options: `--compression` (default `zstd`), `--row-group-rows`.

## License

MIT
