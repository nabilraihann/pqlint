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
uv run pqlint <file-atau-folder> [--json] [--summary-only] [--fail-under N]
uv run pqlint compact <input...> -o <output> [--sort-by a,b:desc] [--memory-limit 2G] [--dry-run]
```

Exit code CLI: 0 normal, 1 jika ada skor di bawah `--fail-under`, 2 jika ada file yang gagal dibaca atau tidak ada file ditemukan.

## Arsitektur

Alur: `cli` → `report.lint_path` → `reader.read_local` → `rules.lint`; setelah semua file, `summary.summarize` mengagregasi `FileReport`.

- `reader.FileInfo(path, size, metadata)` adalah batas antara pembacaan dan aturan. Ukuran file dibawa terpisah karena footer tidak memuatnya. Aturan hanya bergantung pada `FileInfo`, jadi reader S3/GCS nanti cukup menghasilkan `FileInfo` yang sama (range request untuk footer + HEAD untuk ukuran) tanpa mengubah aturan.
- `rules.py`: tiap aturan adalah generator `(FileInfo, Config) -> Finding`. Menambah aturan = tulis fungsi lalu daftarkan di `DEFAULT_RULES`. Semua ambang dan bobot penalti ada di `Config`. Temuan diagregasi per file (satu temuan per aturan dengan daftar kolom), bukan per row group, supaya laporan tidak berisik. Skor = 100 dikurangi penalti per temuan (warning 10, info 3), minimum 0.
- `summary.py`: agregasi level dataset (total, sebaran ukuran, file per aturan, kardinalitas partisi Hive dari path). File error dihitung tetapi tidak ikut statistik. JSON keluaran CLI berbentuk `{"summary", "files"}`; `--summary-only` menghilangkan `files`.
- `render.py`: semua keluaran teks (tabel rata kolom, bungkus ke lebar terminal, warna opsional) tanpa dependensi; `fmt.py`: `human_size`/`plural`. Warna diterapkan setelah padding agar kode ANSI tidak merusak lebar kolom; `Style` jatuh ke ASCII bila stdout bukan UTF-8; warna hanya di TTY dan menghormati `NO_COLOR`/`TERM=dumb` (`--color always` mengalahkan keduanya). Kolom terakhir tabel adalah kolom yang dibungkus. Beberapa file: tabel ikhtisar saja, detail per file lewat `--details`. JSON tidak ikut berubah.
- `report.lint_path` menangkap kegagalan baca dan kegagalan aturan ke `FileReport.error` alih-alih melempar, sehingga satu file rusak tidak menghentikan scan folder.

## Compaction (`pqlint compact`)

`compact.py` (logika) + `compact_cli.py` (argparse); `cli.main` mengalihkan jika argumen pertama `compact`, jadi sebuah file bernama persis `compact` harus ditulis `./compact`. Berbeda dari linter, ini membaca dan menulis data baris. Tiga strategi (`Plan.strategy`): `streaming` tanpa `--sort-by` (memori ~satu row group); `memory` bila sort penuh muat (`pa.concat_tables` + `sort_by`); `bucketed` bila tidak muat. Null selalu di akhir, `sorting_columns` ditulis ke footer. Keputusan user: `pyarrow` murni dengan pengaman, bukan DuckDB; sort tetap untuk satu file (menggabung beberapa file juga didukung).
- Bucketed: pass 0 `_count_values` menghitung nilai kolom sort pertama (hanya kolom itu; urutan nilai ditentukan Arrow agar sama dengan `sort_by`), nilai berurutan dikelompokkan jadi bucket yang muat di `BUCKET_MEMORY_SHARE` memori; pass 1 menulis tiap baris ke file sementara bucket-nya (`index_in` + `take`, folder `.<out>.<pid>.tmp.buckets`); pass 2 sort tiap bucket dan menambahkannya lewat `_RowGroupWriter` (row group seragam lintas bucket). Filter menjaga urutan input dan sort Arrow stabil, jadi hasil identik dengan sort in-memory (dites di `test_bucketed_sort.py`). Satu nilai tidak bisa dipecah: bila satu nilai terlalu besar, `Plan.problem` diisi dan run ditolak kecuali `--force`. Batas: `MAX_BUCKET_KEYS`, `MAX_BUCKETS`, cek disk bebas 2,5x input. `--memory-limit` menggantikan `MemAvailable`.
- `PROCESS_BASELINE` (200 MiB) ikut estimasi bucket; hasil kalibrasi pada file nyata (estimasi 2,2 GiB vs terukur 2,22 GiB). Tes compaction menolkannya lewat fixture autouse karena fixture-nya berukuran KB.

- Estimasi memori **tidak boleh dari footer**: `total_byte_size` adalah ukuran ter-encode; file contoh 245 MiB berukuran 0,34 GiB di footer tetapi ~4,7 GiB sebagai Arrow (string dictionary). `_sample_arrow_bytes_per_row` membaca sampel row group. Faktor puncak: sort 2,2x, streaming 3,5x (terukur ~3,3x). Bila sort penuh tidak muat, otomatis bucketed; streaming dan bucketed mengecilkan ukuran row group otomatis sesuai memori.
- `Table.sort_by` pada kolom dictionary belum didukung pyarrow (`ArrowNotImplementedError`), jadi optimasi membaca string sebagai dictionary tidak bisa dipakai untuk sort.
- Penulisan lewat file sementara `.<nama>.<pid>.tmp` + verifikasi baris/skema + `os.replace`; tidak pernah menimpa input.
- Verifikasi bawaan hanya jumlah baris dan skema. Pada file nyata 33,6 juta baris isinya juga dibandingkan penuh secara streaming (identik).

## Hal yang tidak jelas dari kode

- `row_group.total_byte_size` di Parquet adalah ukuran **tidak terkompresi**; ambang row group dibandingkan dengan nilai itu. `row-group-small` juga mensyaratkan baris < 1 juta: data sempit dan repetitif ter-encode kecil walau berisi jutaan baris (hasil compaction 6 juta baris/row group sempat ditandai kecil sebelum aturan ini).
- Row group terakhir tidak dinilai oleh `row-group-small` (sisa wajar), dan file dengan satu row group yang sudah kena `small-file` tidak dilaporkan dua kali.
- `dictionary-ineffective` adalah heuristik: footer tidak bisa membedakan fallback ke PLAIN (`encodings` sama persis dengan kolom yang tidak fallback). Dipakai ukuran halaman dictionary on-disk >= 1 MiB; heuristik rasio sudah dicoba dan membuang kolom unik biasa seperti id, jadi jangan dikembalikan.
- Kolom bertipe logical `Null` (pandas/pyarrow menulisnya untuk kolom yang isinya None semua) tidak pernah punya stats; ditangani khusus: dilaporkan sebagai `all-null-column`, bukan `missing-stats`. Kolom di bawah repeated field (list) dilewati oleh `all-null-column` karena list kosong dihitung sebagai null di leaf.
- `sort-candidates` memakai `Severity.HINT` (penalti 0: saran, bukan cacat) dan `Finding.details` untuk angka per kolom. Metrik `_overlap` = fraksi pasangan row group yang rentang min/max-nya beririsan (dihitung lewat pasangan terpisah dengan bisect pada max terurut, O(n log n)), jadi berlaku untuk string/tanggal: 0 = semua terpisah, 1 = semua beririsan. Versi awal yang menghitung kedalaman di titik min memberi 50% untuk data teracak murni; tes di `test_compact.py` yang menangkapnya. Footer tidak tahu kolom mana yang sering difilter, jadi hasilnya hanya kandidat. pyarrow tidak memasukkan NaN ke min/max.
- `*.parquet` ada di `.gitignore` agar dataset yang dites di folder repo tidak ikut ter-commit.
- Tes memperkecil ambang lewat `Config` agar file fixture tetap kecil; hanya tes dictionary yang menulis ~2 MB tanpa kompresi.

## Keputusan yang masih terbuka (tanyakan ke user sebelum berasumsi)

- Rust/PyO3 (seperti petquar) vs tetap `pyarrow` jika performa jadi masalah.
- Object store: S3 vs GCS.
