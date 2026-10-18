"""Generate demo data for docs/contoh-penggunaan.md.

Synthetic daily sales shaped like a typical retail extract: DC > store > item > day,
written the way many pipelines do it (one day after another, small row groups).

    uv run python examples/make_demo_data.py demo/

Creates:
    demo/sales.parquet             one file, 720,000 rows, 36 small row groups
    demo/daily/dt=2025-07-DD/...   a folder of small daily files, one uncompressed,
                                   plus one broken file
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

DCS = ["DC JAKARTA", "DC BANDUNG", "DC SURABAYA", "DC MEDAN", "DC MAKASSAR",
       "DC SEMARANG", "DC PALEMBANG", "DC DENPASAR"]
CATEGORIES = ["SUSU BAYI", "BISKUIT", "MINUMAN", "SNACK", "MIE INSTAN", "KOPI", "TEH", "SABUN"]
STORES = 400
ITEMS = 60
DAYS = 30


def sales_table(days: range, seed: int = 1) -> pa.Table:
    rnd = random.Random(seed)
    stores = [(DCS[s % len(DCS)], f"T{s:04d}", f"TOKO {s:04d}") for s in range(STORES)]
    items = [(CATEGORIES[i % len(CATEGORIES)], 10_000 + i, f"PRODUK {i:02d}", 5_000 + 750 * i)
             for i in range(ITEMS)]
    cols: dict[str, list] = {k: [] for k in
                             ["category", "dc", "kode_store", "store", "item_code", "item_desc",
                              "idr", "day", "month", "year"]}
    for day in days:
        block = [(s, i) for s in range(STORES) for i in range(ITEMS)]
        rnd.shuffle(block)  # within a day, rows arrive in no particular order
        for s, i in block:
            dc, code, name = stores[s]
            cat, icode, idesc, price = items[i]
            for key, value in (("category", cat), ("dc", dc), ("kode_store", code),
                               ("store", name), ("item_code", icode), ("item_desc", idesc),
                               ("idr", float(price * rnd.randint(1, 4))), ("day", day),
                               ("month", "JUL"), ("year", 2025)):
                cols[key].append(value)
    return pa.table(cols)


def main(out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    pq.write_table(sales_table(range(1, DAYS + 1)), out / "sales.parquet",
                   row_group_size=20_000, compression="snappy")

    daily = out / "daily"
    for day in range(1, 6):
        d = daily / f"dt=2025-07-{day:02d}"
        d.mkdir(parents=True, exist_ok=True)
        t = sales_table(range(day, day + 1), seed=day)
        pq.write_table(t, d / "part-0.parquet", row_group_size=5_000,
                       compression="none" if day == 3 else "snappy")
    broken = daily / "dt=2025-07-06"
    broken.mkdir(exist_ok=True)
    (broken / "part-0.parquet").write_bytes(b"not a parquet file")
    print(f"demo data written to {out}/")


if __name__ == "__main__":
    main(Path(sys.argv[1] if len(sys.argv) > 1 else "demo"))
