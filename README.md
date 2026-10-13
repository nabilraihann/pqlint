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

Thresholds live in `pqlint.Config`.

```
uv run pytest -q
```

## License

MIT
