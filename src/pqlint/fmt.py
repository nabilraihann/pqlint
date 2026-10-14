from __future__ import annotations


def human_size(n: int | float) -> str:
    for unit, size in (("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)):
        if n >= size:
            return f"{n / size:.2f} {unit}"
    return f"{int(n)} B"


def plural(n: int, word: str) -> str:
    return f"{n:,} {word}" + ("" if n == 1 else "s")
