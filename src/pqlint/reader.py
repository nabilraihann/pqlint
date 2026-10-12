from __future__ import annotations

import os
from dataclasses import dataclass

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
