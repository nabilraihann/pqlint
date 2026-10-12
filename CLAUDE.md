# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Kondisi saat ini

Belum ada kode. Repo hanya berisi `context.md` (berbahasa Indonesia), ringkasan diskusi dari claude.ai yang menjadi satu-satunya sumber tujuan proyek. Belum ada source code, build system, test suite, maupun riwayat git, jadi belum ada perintah build/lint/test yang bisa didokumentasikan. Perbarui file ini setelah toolchain tersedia.

## Tujuan proyek

Tool yang membaca **hanya footer metadata Apache Parquet** (tanpa scan data baris) untuk menghasilkan laporan kesehatan dataset. Arah yang dipilih adalah gabungan dari:

- **Parquet linter / health check**: deteksi small files, row group terlalu kecil/besar, kolom tanpa stats, dictionary encoding fallback, kolom yang sebaiknya disort; output berupa skor plus rekomendasi.
- **Catalog scanner object store**: scan sebuah prefix, baca footer tiap file lewat range request di byte terakhir, lalu agregasi menjadi laporan level dataset (jumlah file, total baris, sebaran ukuran, kardinalitas kolom partisi).

Relevan dengan pekerjaan migrasi data warehouse user. Ide lain dari metadata yang sama (predicate pushdown simulator, estimator biaya/ukuran, schema diff, analyzer clustering/sort, perbandingan writer lewat `created_by`, data quality dari stats) tercatat di `context.md` tetapi bukan cakupan yang dipilih.

## Proyek referensi

[vutrinh274/petquar](https://github.com/vutrinh274/petquar) adalah inspirasinya: extension Rust (PyO3 membungkus crate `parquet` dari arrow-rs) dengan backend FastAPI, frontend statis, dan build `uv` + `maturin`. Statusnya masih early scaffold, hanya mendukung file lokal, dengan stub S3 di `reader.py`. Proyek ini bisa berkontribusi ke petquar atau meminjam pendekatannya, tetapi tidak ada kode di sini yang berasal dari petquar.

## Urutan implementasi yang direncanakan

1. Prototipe Python yang membaca footer satu file dengan `pyarrow.parquet.ParquetFile(...).metadata`.
2. Aturan linter (ukuran row group, stats hilang, small file, dll) beserta laporannya.
3. Perluas ke S3: baca footer lewat range request (`s3fs`/`fsspec` atau `boto3`), loop semua file dalam prefix.
4. Agregasi ke laporan level dataset (tabel/JSON, opsional UI di tahap berikutnya).

## Keputusan yang masih terbuka (belum diputuskan; tanyakan ke user sebelum berasumsi)

- `pyarrow` (cepat dibuat) vs Rust/PyO3 (seperti petquar).
- Object store: S3 vs GCS (konteks BigQuery/GCS belum dikonfirmasi).
