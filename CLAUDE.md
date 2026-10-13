# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Tujuan proyek

`pqlint` adalah linter Parquet yang membaca **hanya footer metadata** (tanpa scan data baris) dan menghasilkan skor 0–100 plus temuan beserta rekomendasi. Konteks dan daftar ide lain ada di `context.md` (berbahasa Indonesia). Rencana berikutnya: catalog scanner object store (S3/GCS) yang membaca footer lewat range request, lalu agregasi ke laporan level dataset. Proyek referensi: [vutrinh274/petquar](https://github.com/vutrinh274/petquar) (tidak ada kode yang disalin darinya).

## Perintah

Toolchain: `uv` + `pyarrow` (Python >=3.11), build backend `uv_build`.

```
uv sync                                   # install dependensi (termasuk dev)
uv run pytest -q                          # semua tes
uv run pytest tests/test_rules.py::test_uncompressed   # satu tes
uv run pqlint <file-atau-folder> [--json] [--fail-under N]
```

Exit code CLI: 0 normal, 1 jika ada skor di bawah `--fail-under`, 2 jika ada file yang gagal dibaca atau tidak ada file ditemukan.

## Arsitektur

Alur: `cli` → `report.lint_path` → `reader.read_local` → `rules.lint`; setelah semua file, `summary.summarize` mengagregasi `FileReport`.

- `reader.FileInfo(path, size, metadata)` adalah batas antara pembacaan dan aturan. Ukuran file dibawa terpisah karena footer tidak memuatnya. Aturan hanya bergantung pada `FileInfo`, jadi reader S3/GCS nanti cukup menghasilkan `FileInfo` yang sama (range request untuk footer + HEAD untuk ukuran) tanpa mengubah aturan.
- `rules.py`: tiap aturan adalah generator `(FileInfo, Config) -> Finding`. Menambah aturan = tulis fungsi lalu daftarkan di `DEFAULT_RULES`. Semua ambang dan bobot penalti ada di `Config`. Temuan diagregasi per file (satu temuan per aturan dengan daftar kolom), bukan per row group, supaya laporan tidak berisik. Skor = 100 dikurangi penalti per temuan (warning 10, info 3), minimum 0.
- `summary.py`: agregasi level dataset (total, sebaran ukuran, file per aturan, kardinalitas partisi Hive dari path). File error dihitung tetapi tidak ikut statistik. JSON keluaran CLI berbentuk `{"summary", "files"}`; `--summary-only` menghilangkan `files`.
- `report.lint_path` menangkap kegagalan baca dan kegagalan aturan ke `FileReport.error` alih-alih melempar, sehingga satu file rusak tidak menghentikan scan folder.

## Hal yang tidak jelas dari kode

- `row_group.total_byte_size` di Parquet adalah ukuran **tidak terkompresi**; ambang row group dibandingkan dengan nilai itu.
- Row group terakhir tidak dinilai oleh `row-group-small` (sisa wajar), dan file dengan satu row group yang sudah kena `small-file` tidak dilaporkan dua kali.
- `dictionary-ineffective` adalah heuristik: footer tidak bisa membedakan fallback ke PLAIN (`encodings` sama persis dengan kolom yang tidak fallback). Dipakai ukuran halaman dictionary on-disk >= 1 MiB; heuristik rasio sudah dicoba dan membuang kolom unik biasa seperti id, jadi jangan dikembalikan.
- Kolom bertipe logical `Null` (pandas/pyarrow menulisnya untuk kolom yang isinya None semua) tidak pernah punya stats; ditangani khusus: dilaporkan sebagai `all-null-column`, bukan `missing-stats`. Kolom di bawah repeated field (list) dilewati oleh `all-null-column` karena list kosong dihitung sebagai null di leaf.
- Tes memperkecil ambang lewat `Config` agar file fixture tetap kecil; hanya tes dictionary yang menulis ~2 MB tanpa kompresi.

## Keputusan yang masih terbuka (tanyakan ke user sebelum berasumsi)

- Rust/PyO3 (seperti petquar) vs tetap `pyarrow` jika performa jadi masalah.
- Object store: S3 vs GCS.
