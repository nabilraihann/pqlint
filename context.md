# CONTEXT: Ide Proyek dari Parquet Footer Metadata

Ringkasan diskusi dari claude.ai, untuk dilanjutkan di Claude Code.

## Referensi

Repo: https://github.com/vutrinh274/petquar

Petquar memvisualisasikan internal file Apache Parquet dengan membaca **hanya footer metadata** (tanpa scan data baris):
- schema, row group, statistik column chunk (min/max/null_count)
- encoding, kompresi, keberadaan bloom filter dan page index

Stack petquar:
- Rust extension (PyO3) membungkus crate `parquet` dari apache/arrow-rs
- Python: FastAPI + uvicorn, CLI `petquar path/to/file.parquet`
- Build dengan `uv` + `maturin`
- Frontend statis (schema tree, tabel row group/kolom)
- Server sengaja dibuat (bukan HTML statis) sebagai dasar playground predicate pushdown di masa depan
- Status: early scaffold, hanya file lokal, S3 baru stub di `reader.py`

## Daftar ide (dari metadata Parquet yang sama)

1. **Predicate pushdown simulator**: input filter (`WHERE tanggal = ...`), tampilkan row group/page yang bisa di-skip berdasarkan min/max stats. Sudah direncanakan di petquar, bisa jadi kontribusi.
2. **Parquet linter / health check**: deteksi small files, row group terlalu kecil/besar, kolom tanpa stats, dictionary encoding fallback, kolom yang sebaiknya disort. Output: skor + rekomendasi.
3. **Estimator biaya & ukuran**: ukuran per kolom (compressed vs uncompressed), rasio kompresi, estimasi byte yang di-scan per query (relevan untuk biaya BigQuery/Athena).
4. **Schema diff & drift detector**: bandingkan footer antar file/folder, tandai perubahan tipe, kolom hilang/baru, nullability.
5. **Dataset-level catalog scanner**: scan prefix S3, baca footer saja (range request di byte akhir), ringkas jumlah file, total baris, sebaran ukuran, kardinalitas kolom partisi.
6. **Clustering/sort quality analyzer**: cek overlap min/max antar row group per kolom, rekomendasikan kolom sort/Z-order untuk compaction.
7. **Writer comparison tool**: deteksi writer dari `created_by` (Spark, pyarrow, DuckDB, dll) dan bandingkan setting (row group size, kompresi, versi format).
8. **Data quality dari stats**: kolom all-null, nilai di luar rentang wajar, anomali antar file harian, tanpa menjalankan query.

## Rekomendasi

Kombinasi **#2 (linter) + #5 (S3 catalog scanner)**: scanner footer di S3 yang menghasilkan laporan kesehatan dataset. Ringan (hanya baca footer), praktis, dan relevan untuk pekerjaan migrasi data warehouse.

## Usulan langkah berikutnya di Claude Code

1. Buat prototipe Python dengan `pyarrow.parquet.ParquetFile(...).metadata` untuk membaca footer satu file.
2. Implementasikan aturan linter (row group size, stats hilang, small file, dll) dan keluarkan laporan.
3. Perluas ke S3: baca footer via range request (`s3fs`/`fsspec` atau `boto3`), loop semua file dalam prefix.
4. Agregasi ke laporan dataset (tabel/JSON, lalu opsional UI).

## Catatan

- Pilihan antara `pyarrow` (cepat dibuat) vs Rust/PyO3 (seperti petquar) belum diputuskan.
- Sumber S3 vs GCS (BigQuery/GCS) belum dikonfirmasi.