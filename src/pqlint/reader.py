from __future__ import annotations

import os
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq


@dataclass(frozen=True)
class FileInfo:
    """Footer metadata plus the file size, which the footer alone doesn't carry.

    Rules depend on this type instead of a path so a remote reader (S3/GCS)
    can supply the same data from a range request and a HEAD call.
    """

    path: str
    size: int
    metadata: pq.FileMetaData


def read_local(path: str | os.PathLike[str]) -> FileInfo:
    path = os.fspath(path)
    # pq.read_metadata fetches only the footer, never the row data.
    return FileInfo(path, os.path.getsize(path), pq.read_metadata(path))


def expand_paths(paths: Iterable[str | os.PathLike[str]]) -> Iterator[Path]:
    """Files as given; directories are searched recursively for *.parquet."""
    for p in map(Path, paths):
        if p.is_dir():
            yield from sorted(p.rglob("*.parquet"))
        else:
            yield p
