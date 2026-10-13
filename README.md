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
| `row-group-small` / `row-group-large` | warning | row groups under 32 MiB or over 1 GiB (uncompressed) |
| `missing-stats` | warning | column chunks without min/max statistics |
| `uncompressed` | warning | column stored without a compression codec |
| `dictionary-ineffective` | info | dictionary page of 1 MiB or more (heuristic) |
| `all-null-column` | info | column contains only nulls |
| `sort-candidates` | hint | columns whose row group min/max ranges overlap heavily (see below); never lowers the score |

`sort-candidates` ranks columns by how much their per-row-group min/max ranges overlap
(0% = row groups hold disjoint ranges, so engines can skip them; 100% = every row group
spans the same range, so filters skip nothing). A high percentage means sorting or clustering
by that column would help *if queries filter on it*; the footer cannot know that, so treat the
list as candidates. Skipped: columns without stats in every row group, constant and boolean
columns, list columns, columns already declared sorted in the footer, and files with fewer
than 4 row groups.

Thresholds live in `pqlint.Config`.

```
uv run pytest -q
```

## License

MIT
