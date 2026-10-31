# Contoh penggunaan pqlint

Panduan ini memakai data contoh sintetis supaya setiap perintah bisa dijalankan ulang dan
memberi hasil yang sama. Strukturnya meniru ekstrak penjualan ritel pada umumnya: DC → toko →
item → hari, ditulis per hari dengan row group kecil, seperti yang sering dihasilkan pipeline.

Semua keluaran di bawah diambil dari menjalankan perintahnya sungguhan (lebar terminal 96
kolom, `--color never`). Di terminal interaktif, keluarannya berwarna.

## Daftar isi

1. [Persiapan](#1-persiapan)
2. [Memeriksa satu file](#2-memeriksa-satu-file)
3. [Memeriksa satu folder (dataset)](#3-memeriksa-satu-folder-dataset)
4. [Keluaran JSON dan pemakaian di CI](#4-keluaran-json-dan-pemakaian-di-ci)
5. [Mencari urutan sort terbaik](#5-mencari-urutan-sort-terbaik)
6. [Merapikan file dengan compact](#6-merapikan-file-dengan-compact)
7. [File lebih besar dari memori](#7-file-lebih-besar-dari-memori)
8. [Alur kerja yang disarankan](#8-alur-kerja-yang-disarankan)

## 1. Persiapan

```bash
git clone https://github.com/nabilraihann/pqlint.git
cd pqlint
uv sync
uv run python examples/make_demo_data.py demo
```

Perintah terakhir membuat:

| Path | Isi |
|---|---|
| `demo/sales.parquet` | 720.000 baris (400 toko × 60 item × 30 hari) dalam 36 row group kecil |
| `demo/daily/dt=2025-07-DD/part-0.parquet` | 5 file harian kecil; tanggal 3 tanpa kompresi |
| `demo/daily/dt=2025-07-06/part-0.parquet` | file rusak, untuk melihat penanganan error |

File `*.parquet` diabaikan git, jadi folder `demo/` aman dibuat di dalam repo.

Semua perintah di bawah ditulis `uv run pqlint ...`. Kalau virtualenv sudah aktif
(`source .venv/bin/activate`), cukup `pqlint ...`.

## 2. Memeriksa satu file

```bash
uv run pqlint demo/sales.parquet
```

```
demo/sales.parquet
  Score 80/100  ·  720,000 rows · 36 row groups · 4.04 MiB
  Writer parquet-cpp-arrow version 26.0.0

  LEVEL    RULE             FINDING
  ───────  ───────────────  ────────────────────────────────────────────────────────────────────
  WARNING  small-file       File is 4.04 MiB, below 8.00 MiB.
                            → Compact small files into larger ones to cut per-file open and
                              metadata overhead.
  WARNING  row-group-small  35 of 36 row groups are under 32.00 MiB uncompressed and 1,000,000
                            rows (smallest 120.31 KiB).
                            → Raise the writer's row group size; tiny row groups weaken
                              compression and stats.
  HINT     sort-candidates  Row group min/max ranges overlap heavily in 5 column(s), most
                            overlapping first.
                            overlap: category 100%, dc 100%, item_code 100%, item_desc 100%,
                            kode_store 100%
                            → If queries filter on one of these, sort by it first so engines can
                              skip row groups (this is about filter speed, not file size; for
                              size run `pqlint suggest-sort`).
```

Cara membacanya:

- **Hanya footer yang dibaca**, jadi pemeriksaan ini instan, bahkan untuk file ber-GB.
- **Score** 0–100. Setiap `WARNING` mengurangi 10, `INFO` mengurangi 3, `HINT` tidak mengurangi.
- **`row-group-small`:** 36 row group untuk 720 ribu baris terlalu kecil. Engine membaca
  row group satu per satu, dan statistik min/max per row group kurang berguna.
- **`sort-candidates`:** overlap 100% berarti setiap row group memuat hampir semua nilai kolom
  itu. Filter seperti `WHERE dc = 'DC MEDAN'` tidak bisa melewatkan row group mana pun.
  Ini soal **kecepatan filter**. Untuk **ukuran file**, lihat bagian 5.

## 3. Memeriksa satu folder (dataset)

```bash
uv run pqlint demo/daily
```

```
Files (6)
  SCORE    ROWS  ROW GROUPS        SIZE  FILE
  ─────  ──────  ──────────  ──────────  ──────────────────────────────────────────────────────
     80  24,000           5  165.34 KiB  demo/daily/dt=2025-07-01/part-0.parquet
                                         small-file, row-group-small, sort-candidates
     80  24,000           5  165.39 KiB  demo/daily/dt=2025-07-02/part-0.parquet
                                         small-file, row-group-small, sort-candidates
     70  24,000           5  198.16 KiB  demo/daily/dt=2025-07-03/part-0.parquet
                                         small-file, row-group-small, uncompressed,
                                         sort-candidates
     80  24,000           5  165.50 KiB  demo/daily/dt=2025-07-04/part-0.parquet
                                         small-file, row-group-small, sort-candidates
     80  24,000           5  165.47 KiB  demo/daily/dt=2025-07-05/part-0.parquet
                                         small-file, row-group-small, sort-candidates
    ERR                                  demo/daily/dt=2025-07-06/part-0.parquet
                                         ArrowInvalid: Parquet magic bytes not found in footer.
                                         Either the file is corrupted or this is not a parquet
                                         file.

Dataset summary
  Files       6  (1 unreadable)
  Rows        120,000
  Total size  859.87 KiB
  File size   min 165.34 KiB · median 165.47 KiB · max 198.16 KiB
  Mean score  78.0 / 100

  RULE             FILES AFFECTED  SHARE
  ───────────────  ──────────────  ─────
  row-group-small           5 / 5  100%
  small-file                5 / 5  100%
  sort-candidates           5 / 5  100%
  uncompressed              1 / 5  20%

  SCORE  LOWEST-SCORING FILES
  ─────  ───────────────────────────────────────
     70  demo/daily/dt=2025-07-03/part-0.parquet
     80  demo/daily/dt=2025-07-01/part-0.parquet
     ...

  PARTITION KEY  DISTINCT VALUES
  ─────────────  ───────────────
  dt             5
```

- Folder dibaca **rekursif**, dan hanya file `*.parquet` yang diambil.
- **File rusak tidak menghentikan pemeriksaan.** File itu ditandai `ERR` beserta pesannya,
  dan exit code menjadi `2`.
- **Partisi bergaya Hive** (`dt=...`) dikenali dari nama folder.
- File yang bermasalah langsung terlihat: tanggal 3 satu-satunya yang `uncompressed`.

Opsi tambahan:

```bash
uv run pqlint demo/daily --summary-only   # hanya ringkasan dataset
uv run pqlint demo/daily --details        # ikhtisar + tabel temuan lengkap untuk setiap file
```

## 4. Keluaran JSON dan pemakaian di CI

```bash
uv run pqlint demo/sales.parquet --json
```

```json
{
  "summary": {
    "files": 1,
    "errors": 0,
    "total_rows": 720000,
    "total_bytes": 4233417,
    "mean_score": 80.0,
    "files_per_rule": {"row-group-small": 1, "small-file": 1, "sort-candidates": 1},
    ...
  },
  "files": [
    {
      "path": "demo/sales.parquet",
      "size": 4233417,
      "num_rows": 720000,
      "num_row_groups": 36,
      "score": 80,
      "findings": [ { "rule": "small-file", "severity": "warning", ... }, ... ]
    }
  ]
}
```

Contoh mengambil data tertentu dengan `jq`:

```bash
# file dengan skor di bawah 80
uv run pqlint demo/daily --json | jq -r '.files[] | select(.score != null and .score < 80) | .path'

# kolom yang direkomendasikan untuk filter (sort-candidates)
uv run pqlint demo/sales.parquet --json | jq '.files[0].findings[] | select(.rule=="sort-candidates") | .details'
```

**Gerbang kualitas di CI/pipeline** memakai `--fail-under`:

```bash
uv run pqlint output/ --fail-under 90 --summary-only
echo $?   # 0 = semua file >= 90, 1 = ada yang di bawah 90, 2 = ada file rusak / tidak ada file
```

| Exit code | Arti |
|---|---|
| 0 | semua file lolos |
| 1 | ada file dengan skor di bawah `--fail-under` |
| 2 | ada file yang tidak terbaca, atau tidak ada file Parquet |

## 5. Mencari urutan sort terbaik

Seberapa kecil file setelah di-sort bergantung pada isi datanya (berapa sering nilai yang
sama berulang berturut-turut, dan kolom mana yang saling terkait). Footer tidak memuat
informasi itu, jadi `suggest-sort` **membaca sampel data**, mencoba berbagai urutan, dan
mengukur hasil kompresinya. Tidak ada file yang ditulis.

```bash
uv run pqlint suggest-sort demo/sales.parquet
```

```
… sampling all rows of ~100.0% of the 'kode_store' values
… searching with 8 thread(s)
… greedy start: key 1 of 5
…
… improving: round 3, 20 neighbouring orders, current ~0.4 MiB
Sort suggestion (for file size)
  Input        1 file(s), 720,000 rows, 4.04 MiB
  Sample       720,000 rows: every row of a random subset of 'kode_store' values
  Compression  zstd
  Tried        67 orders

  STEP  SORT KEYS              EST. SIZE  VS UNSORTED
  ────  ────────────────────  ──────────  ───────────
     0  (unsorted)              3.80 MiB            -
     1  item_code               2.15 MiB         -43%
     2  item_code,kode_store  412.82 KiB         -89%

  Recommended  --sort-by item_code,kode_store
  Run          pqlint compact demo/sales.parquet -o <output> --sort-by item_code,kode_store
  Sizes are scaled from the sample and were within about 5% of real compacted sizes in
  testing, so orders that close are a tie. If queries filter on one column, consider putting it
  first (--start-with) so engines can skip row groups.

  SKIPPED COLUMN  REASON
  ──────────────  ─────────────────────────────────────────────────────────
  idr             floating point (a measure, not a key)
  month           constant in the sample
  year            constant in the sample
  store           same grouping as 'kode_store'; sorting by it adds nothing
  item_desc       same grouping as 'item_code'; sorting by it adds nothing
```

- **Estimasi 412,82 KiB.** Setelah benar-benar di-compact (bagian 6), hasilnya 412,83 KiB.
  Pada file ini sampelnya mencakup seluruh data. Pada file besar galatnya sekitar ±5%.
- Kolom yang **tidak dicoba** dan alasannya ditampilkan: kolom nilai (`idr`), konstan, dan
  kolom yang pengelompokannya identik dengan kolom lain (`store` ↔ `kode_store`).
- Percobaan dijalankan **paralel** (jumlah thread menyesuaikan jumlah core dan memori bebas,
  maksimal 8; baris `searching with N thread(s)` menunjukkannya). Pada data demo ini sekitar
  8 detik; untuk file 33 juta baris sekitar 24 detik (sebelumnya ±86 detik). Rekomendasi dan
  estimasi ukurannya sama persis untuk jumlah thread berapa pun.

**Bila query sering memfilter kolom tertentu**, kunci kolom itu di depan dengan `--start-with`.
File sedikit lebih besar, tetapi filter pada kolom itu bisa melewatkan row group:

```bash
uv run pqlint suggest-sort demo/sales.parquet --start-with dc
```

```
  STEP  SORT KEYS                 EST. SIZE  VS UNSORTED
  ────  ───────────────────────  ──────────  ───────────
     0  (unsorted)                 3.80 MiB            -
     1  dc                         3.22 MiB         -15%
     2  dc,item_code               1.66 MiB         -56%
     3  dc,item_code,kode_store  453.58 KiB         -88%

  Recommended  --sort-by dc,item_code,kode_store
```

Opsi lain: `--sample-rows N` (bawaan 1 juta), `--compression gzip` (mengukur dengan codec
lain), `--max-keys N`, dan `--threads N` (bawaan otomatis; `--threads 1` = serial).

## 6. Merapikan file dengan compact

`compact` menulis ulang file menjadi **file baru**: row group lebih besar, kompresi, dan sort
opsional. Isi datanya tetap sama, dan file input tidak pernah diubah.

### Lihat rencananya dulu

```bash
uv run pqlint compact demo/sales.parquet -o out/sales_rapi.parquet \
    --sort-by item_code,kode_store --dry-run
```

```
Plan
  Input        1 file(s), 36 row groups, 4.0 MiB
  Rows         720,000
  Output       ~1 row group(s) of 720,000 rows
  Sort         item_code asc, kode_store asc (in memory)
  Peak memory  ~350.01 MiB estimated, 4.34 GiB available
```

### Jalankan

```bash
uv run pqlint compact demo/sales.parquet -o out/sales_rapi.parquet --sort-by item_code,kode_store
```

```
Wrote out/sales_rapi.parquet

                BEFORE       AFTER
  ──────────  ────────  ──────────
  Row groups        36           1
  Size        4.04 MiB  412.83 KiB
  Score             80          90
```

File menjadi **10× lebih kecil**. Bandingkan dengan compact **tanpa sort**, yang hanya
memperbesar row group:

```bash
uv run pqlint compact demo/sales.parquet -o out/sales_stream.parquet
```

```
  Sort         none (streaming)
  ...
                BEFORE     AFTER
  Row groups        36         1
  Size        4.04 MiB  3.80 MiB
```

Peringatan `small-file` yang tersisa wajar: data contohnya memang kecil.

### Menggabungkan banyak file kecil

```bash
uv run pqlint compact demo/daily/dt=2025-07-0[1-5] -o out/harian.parquet
```

```
Plan
  Input        5 file(s), 25 row groups, 0.8 MiB
  ...
Wrote out/harian.parquet

                  BEFORE       AFTER
  Row groups          25           1
  Size        859.87 KiB  653.42 KiB
  Score               78          90
```

Semua file input harus punya skema yang sama.

### Pengaman

```bash
# file rusak di input: berhenti, tidak ada file setengah jadi
uv run pqlint compact demo/daily -o out/x.parquet
# pqlint compact: demo/daily/dt=2025-07-06/part-0.parquet: ArrowInvalid: Parquet magic bytes not found ...
# (exit 2)

# output sudah ada: tidak ditimpa tanpa --overwrite
uv run pqlint compact demo/sales.parquet -o out/sales_rapi.parquet --sort-by item_code
# pqlint compact: out/sales_rapi.parquet already exists; pass --overwrite to replace it
# (exit 2)
```

- Hasil ditulis ke file sementara, diverifikasi (jumlah baris dan skema), baru kemudian
  diganti namanya.
- Output tidak boleh sama dengan salah satu input.
- Opsi lain: `--compression gzip|snappy|zstd`, `--row-group-rows N`, `--overwrite`.

## 7. File lebih besar dari memori

Sort butuh memori sekitar 2× ukuran data **saat dimuat**, yang bisa jauh lebih besar
daripada ukuran file karena string terkompresi di disk. Bila tidak muat, `compact` otomatis
memakai **sort per bucket**:

1. hitung nilai kolom sort pertama (hanya kolom itu yang dibaca);
2. kelompokkan nilai-nilai berurutan menjadi bucket yang muat di memori, lalu salin setiap
   baris ke file sementara bucket-nya;
3. sort bucket (beberapa sekaligus, lihat `--threads`) dan sambungkan sesuai urutan bucket.

Hasilnya identik dengan sort penuh. Contoh pada file nyata 33,6 juta baris (245 MiB di disk,
±10,5 GiB untuk sort penuh) di mesin dengan memori bebas 3,6 GiB:

```
Plan
  Input        1 file(s), 673 row groups, 245.0 MiB
  Rows         33,646,597
  Output       ~10 row group(s) of 3,669,022 rows
  Sort         dc asc, store asc (bucketed by dc: 36 bucket(s), 4 in parallel, largest 1,571,069 rows)
  Temp disk    ~245 MiB next to the output
  Peak memory  ~3.2 GiB estimated, 3.6 GiB available
```

Beberapa bucket di-sort sekaligus; tiap bucket mendapat bagian dari memori, jadi bucketnya
lebih banyak dan lebih kecil. Pada mesin ini hasilnya **tidak lebih cepat secara andal**:
tiga putaran bergantian memakan 64-74 detik dengan `--threads 1` (13 bucket) dan 56-76 detik
dengan `--threads 4` (selisih antar-run sekitar 10 detik). Memori puncak terukur sekitar
2,4 GiB. Isi file hasilnya identik baris demi baris. `--threads N` mengatur jumlah bucket
sekaligus (bawaan sampai 8); bila memori sempit jumlahnya otomatis diturunkan.

Membatasi memori secara manual:

```bash
uv run pqlint compact big.parquet -o out.parquet --sort-by dc,store --memory-limit 2G
```

Bila batasnya terlalu kecil, rencana menjelaskan alasannya dan run ditolak (gunakan
`--dry-run` untuk melihatnya tanpa menulis apa pun):

```bash
uv run pqlint compact demo/sales.parquet -o out/x.parquet \
    --sort-by item_code,kode_store --memory-limit 150M --dry-run
```

```
  Problem      the memory limit (150.00 MiB) is below what the compaction needs before
               sorting anything (~242.86 MiB for the output buffer and the process)
  Peak memory  ~245.36 MiB estimated, 150.00 MiB limit  (does not fit)
```

Kasus lain yang ditolak:

- **Satu nilai** kolom sort pertama terlalu besar untuk di-sort sendirian (satu nilai tidak
  bisa dipecah). Pilih kolom pertama yang lebih rinci, atau gunakan `--force`.
- Kolom sort pertama punya lebih dari 1 juta nilai berbeda (misalnya ID unik).
- Disk kosong kurang dari 2,5× ukuran input.

## 8. Alur kerja yang disarankan

```bash
# 1. periksa (instan, hanya footer)
uv run pqlint data.parquet

# 2. cari urutan sort untuk ukuran file (membaca sampel, beberapa menit untuk file besar)
uv run pqlint suggest-sort data.parquet
#    ...atau kunci kolom filter utama di depan:
uv run pqlint suggest-sort data.parquet --start-with dc

# 3. lihat rencana compact
uv run pqlint compact data.parquet -o data_rapi.parquet --sort-by <saran> --dry-run

# 4. jalankan
uv run pqlint compact data.parquet -o data_rapi.parquet --sort-by <saran>

# 5. periksa hasilnya
uv run pqlint data_rapi.parquet
```

Pilihan kompresi:

- **`zstd`** (bawaan): seimbang, cepat dibaca, dan pilihan yang baik untuk data yang sering
  di-query.
- **`gzip`:** kadang sedikit lebih kecil, tetapi lebih lambat ditulis dan dibaca.
- **`snappy`:** paling cepat, tetapi file lebih besar.

Untuk membandingkannya, ukur dulu dengan
`pqlint suggest-sort data.parquet --compression gzip`.
