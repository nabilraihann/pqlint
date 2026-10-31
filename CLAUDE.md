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
uv run pqlint compact <input...> -o <output> [--sort-by a,b:desc] [--memory-limit 2G] [--threads N] [--dry-run]
uv run pqlint suggest-sort <input...> [--start-with dc] [--sample-rows N]
uv run python examples/make_demo_data.py demo   # data sintetis untuk docs/contoh-penggunaan.md
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
- Bucketed: pass 0 `_count_values` menghitung nilai kolom sort pertama (hanya kolom itu; urutan nilai ditentukan Arrow agar sama dengan `sort_by`), nilai berurutan dikelompokkan jadi bucket yang muat di `BUCKET_MEMORY_SHARE` memori; pass 1 menulis tiap baris ke file sementara bucket-nya (`index_in` + `take`, folder `.<out>.<pid>.tmp.buckets`); pass 2 sort bucket dan menambahkannya lewat `_RowGroupWriter` (row group seragam lintas bucket), `Plan.workers` bucket sekaligus di `ThreadPoolExecutor` (pyarrow melepas GIL): hasil ditulis berurutan bucket, bucket berikutnya baru di-submit setelah yang sebelumnya ditulis, jadi paling banyak `workers` bucket termuat. `_bucket` membagi jatah memori per bucket dengan `workers` (bucket lebih banyak dan kecil), lalu menurunkan `workers` sampai `workers × bucket terbesar × 2,2 + overhead` muat, minimal 1 (= perilaku lama). Pass 1 tidak paralel. `--threads N` (bawaan `min(cpu, MAX_WORKERS=8)`, dibatasi memori; N<1 → exit 2). Filter menjaga urutan input dan sort Arrow stabil, jadi hasil identik dengan sort in-memory (dites di `test_bucketed_sort.py`). Satu nilai tidak bisa dipecah: bila satu nilai terlalu besar, `Plan.problem` diisi dan run ditolak kecuali `--force`. Batas: `MAX_BUCKET_KEYS`, `MAX_BUCKETS`, cek disk bebas 2,5x input. `BUCKET_MEMORY_SHARE` hanya mengatur pengelompokan; penolakan karena satu nilai terlalu besar dibandingkan dengan memori tersedia sebenarnya (sebelumnya keliru dibandingkan dengan jatah bucket sehingga plan yang muat ditolak). `--memory-limit` menggantikan `MemAvailable`.
- **Bagian B (bucket paralel) tidak terbukti lebih cepat.** File contoh 33,6 juta baris, `--sort-by dc,store`, 3,6 GiB bebas, tiga putaran bergantian: `--threads 1` 66/64/74 s, `--threads 4` 56/76/63 s. Selisihnya di dalam derau (±10 s) mesin ini (WSL2). Angka "83 s → 52 s" yang sempat tertulis di sini berasal dari satu run 83 s yang ternyata outlier; jangan dipakai. Isi hasil identik baris demi baris di semua jumlah thread. Penyebab dugaan: sort tiap bucket hanya sekitar 24 s dari ~65 s, sisanya pass 1 serial, penulisan, dan I/O. Sebelum mengklaim percepatan, ukur bergantian minimal 3 kali.
- `_sort_table` mengurutkan kolom kunci saja lalu menyalin kolom satu per satu sambil melepas aslinya (menyerahkan tabel tanpa nama lain yang memegangnya). Puncak alokasi Arrow fase sort turun 2,06× → 1,30× ukuran bucket (bucket 660 MiB, 4,7 juta baris) dan sort sedikit lebih cepat (0,85 → 0,72 s per 2 juta baris). **Tetapi puncak proses ditentukan fase tulis row group** (pass 2 lengkap: 2,36× bucket, RSS 2,4 GiB), jadi `SORT_PEAK_FACTOR` tetap 2,2: dicoba memakai faktor dinamis (1 + porsi kolom terbesar + indeks), tetapi estimasi 2,16 GiB berada di bawah RSS terukur 2,78 GiB, sehingga dibatalkan. Metadata skema (mis. pandas) harus dipertahankan saat membangun ulang tabel (`schema=`).
- Ukuran file: dari semua pengaturan writer pyarrow hanya level zstd yang berpengaruh (sampel 5 juta baris terurut, `category,dc,store,item_code_b2b`): default 2,43 MiB, level 9 2,22 (−9%), level 15 2,11 (−13%, tulis 2,5× lebih lambat). Tidak berpengaruh: `dictionary_pagesize_limit`, `data_page_size`, `data_page_version`. Lebih buruk: encoding DELTA_BINARY_PACKED untuk integer (+21%), BYTE_STREAM_SPLIT untuk `idr`, menonaktifkan dictionary. Row group lebih besar → file lebih kecil (122 ribu baris 2,91 MiB vs 5 juta 2,32 MiB). Kolom `idr` (2.093 nilai berbeda dari 5 juta baris) 45% ukuran. Kunci tie-break tambahan (`day`, `idr`) tidak membantu. Selisih dengan DuckDB (9,5 vs 12,9 MiB) belum terjelaskan oleh pengaturan ini.
- `PROCESS_BASELINE` (200 MiB) ikut estimasi bucket; hasil kalibrasi pada file nyata (estimasi 2,2 GiB vs terukur 2,22 GiB). Tes compaction menolkannya lewat fixture autouse karena fixture-nya berukuran KB.

- Estimasi memori **tidak boleh dari footer**: `total_byte_size` adalah ukuran ter-encode; file contoh 245 MiB berukuran 0,34 GiB di footer tetapi ~4,7 GiB sebagai Arrow (string dictionary). `_sample_arrow_bytes_per_row` membaca sampel row group. Faktor puncak: sort 2,2x, streaming 3,5x (terukur ~3,3x). Bila sort penuh tidak muat, otomatis bucketed; streaming dan bucketed mengecilkan ukuran row group otomatis sesuai memori.
- `Table.sort_by` pada kolom dictionary tidak didukung pyarrow (`ArrowNotImplementedError`). `compact._sort_table` mengurutkan lewat salinan kunci yang didekode lalu `take` ke tabel asli, sehingga tipe dictionary di output tetap. `_count_values` juga memakai tipe nilai (bukan dictionary) untuk `bucket_values`, karena `index_in` mendekode dictionary lebih dulu dan tipenya harus sama.
- Penulisan lewat file sementara `.<nama>.<pid>.tmp` + verifikasi baris/skema + `os.replace`; tidak pernah menimpa input.
- Verifikasi bawaan hanya jumlah baris dan skema. Pada file nyata 33,6 juta baris isinya juga dibandingkan penuh secara streaming (identik).

## Saran urutan sort (`pqlint suggest-sort`, `suggest.py`)

Untuk **ukuran file**, berbeda dari aturan `sort-candidates` yang untuk **kecepatan filter** (footer saja). Membaca data, tidak menulis apa pun. Pelajaran yang membentuk desainnya (diuji pada file nyata 33,6 juta baris; jangan diulang):
- Sampel row group/baris acak memecah kelompok (toko, item, hari) yang membuat data terurut kecil: estimasi meleset 35% sampai 3x. Dipakai **sampel entitas**: semua baris dari sebagian nilai kolom entitas (kolom kandidat dengan nilai unik terbanyak yang bukan ID). Entitas yang salah (item, 148 nilai) juga meleset; toko (20 ribu) akurat ±5%.
- Mencari pada sub-sampel yang lebih kecil membalik peringkat (sedikit toko membuat urutan item-dulu terlihat terlalu bagus), jadi pencarian memakai sampel penuh.
- Greedy/beam per prefiks berhenti di urutan yang lebih buruk (item,store 15,3 MiB vs dc,store,item,day 12,9 MiB) karena urutan bagus baru unggul setelah 3–4 kunci. Maka yang dibandingkan urutan lengkap: titik awal terbaik (greedy, kardinalitas naik/turun) lalu hill climbing (pindah satu kolom), lalu buang kunci yang tidak berpengaruh di posisi mana pun.
- Kolom float, hampir unik, konstan, dan yang pengelompokannya identik dengan kolom lain (dicek dengan `group_by` pada probe) tidak dicoba.
- Hasil pada file contoh: `category,dc,store,item_code_b2b` 13,4 MiB nyata (estimasi 12,9); terbaik manual 12,9 MiB. Galat ±5% berarti urutan sedekat itu dianggap seri.
- **Kecepatan** (file contoh 33,6 juta baris, mesin 16 core): 85,8 s → 53,2 s dengan `--threads 1` (peringkat angka) → **23,8 s** bawaan (6 thread). Langkah dan rekomendasi identik di ketiganya. Profiling menunjukkan Python bukan bottleneck (>99% waktu di C++ Arrow); sisa waktu bawaan kira-kira 16 s adalah dua pass pengambilan sampel (`_count_values` + filter entitas), belum dioptimalkan.
- `_rank_table`: kolom kunci diubah sekali menjadi peringkat dense (null terakhir), lalu tiap percobaan `sort_indices` + `take`. **Hanya di suggest-sort**: di `compact` untuk satu kali sort terukur lebih lambat (2,42 s vs 1,80 s). Kolom dictionary didekode dulu; tipe yang tidak didukung `rank` tetap berupa nilai aslinya.
- `_Scorer.prefetch` menghitung banyak urutan sekaligus di `ThreadPoolExecutor` (pyarrow melepas GIL), tetapi cache hanya ditulis di thread utama setelah semua worker selesai; pemilihan memakai tie-break yang sama, jadi hasil tidak bergantung jumlah thread (dites: `threads=1` == `threads=4`). `prefetch` dipanggil tepat sebelum tiap perbandingan: langkah greedy, tiga seed, tetangga hill-climb, varian trim, dan prefiks akhir. Karena trim menghitung semua variannya di awal, `Tried` sedikit lebih besar dari versi lama (101 → 104).
- Jumlah thread bawaan `resolve_threads`: `min(core, 8, 0,5 × memori tersedia / (1,5 × ukuran Arrow sampel))`, 4 bila memori tidak diketahui; `--threads N` dipakai apa adanya (min 1).
- Kolom bertipe **dictionary** (pandas `category`) sebelumnya membuat suggest-sort crash (`ArrowNotImplementedError`); kini berfungsi (peringkat mendekode nilainya, `_plain()` menyamakan tipe di filter entitas, `_probe` memanggil `unify_dictionaries()` karena ekspor pandas harian membawa kamus berbeda per file/row group dan `group_by` tidak bisa menyatukannya). `compact --sort-by` pada kolom dictionary juga sudah berfungsi (lihat `_sort_table`).

## Hal yang tidak jelas dari kode

- `row_group.total_byte_size` di Parquet adalah ukuran **tidak terkompresi**; ambang row group dibandingkan dengan nilai itu. `row-group-small` juga mensyaratkan baris < 1 juta: data sempit dan repetitif ter-encode kecil walau berisi jutaan baris (hasil compaction 6 juta baris/row group sempat ditandai kecil sebelum aturan ini).
- Row group terakhir tidak dinilai oleh `row-group-small` (sisa wajar), dan file dengan satu row group yang sudah kena `small-file` tidak dilaporkan dua kali.
- `dictionary-ineffective` adalah heuristik: footer tidak bisa membedakan fallback ke PLAIN (`encodings` sama persis dengan kolom yang tidak fallback). Dipakai ukuran halaman dictionary on-disk >= 1 MiB; heuristik rasio sudah dicoba dan membuang kolom unik biasa seperti id, jadi jangan dikembalikan.
- Kolom bertipe logical `Null` (pandas/pyarrow menulisnya untuk kolom yang isinya None semua) tidak pernah punya stats; ditangani khusus: dilaporkan sebagai `all-null-column`, bukan `missing-stats`. Kolom di bawah repeated field (list) dilewati oleh `all-null-column` karena list kosong dihitung sebagai null di leaf.
- `sort-candidates` memakai `Severity.HINT` (penalti 0: saran, bukan cacat) dan `Finding.details` untuk angka per kolom. Metrik `_overlap` = fraksi pasangan row group yang rentang min/max-nya beririsan (dihitung lewat pasangan terpisah dengan bisect pada max terurut, O(n log n)), jadi berlaku untuk string/tanggal: 0 = semua terpisah, 1 = semua beririsan. Versi awal yang menghitung kedalaman di titik min memberi 50% untuk data teracak murni; tes di `test_compact.py` yang menangkapnya. Footer tidak tahu kolom mana yang sering difilter, jadi hasilnya hanya kandidat; kolom float (ukuran/nilai) tidak ikut dinilai. Untuk ukuran file pakai `suggest-sort`. pyarrow tidak memasukkan NaN ke min/max.
- `docs/contoh-penggunaan.md` memuat keluaran nyata dari data `examples/make_demo_data.py`; bila format keluaran CLI berubah, jalankan ulang perintahnya dan perbarui dokumen itu. Jangan memakai file data milik user di dokumen (berisi nama toko/item nyata).
- Estimasi memori semua strategi memuat `PROCESS_BASELINE`; bila `--memory-limit` di bawah overhead tetap itu, `Plan.problem` menjelaskannya (dicek sebelum batas jumlah bucket, agar pesannya tidak menyesatkan).
- `*.parquet` ada di `.gitignore` agar dataset yang dites di folder repo tidak ikut ter-commit.
- Tes memperkecil ambang lewat `Config` agar file fixture tetap kecil; hanya tes dictionary yang menulis ~2 MB tanpa kompresi.

## Keputusan yang masih terbuka (tanyakan ke user sebelum berasumsi)

- Rust/PyO3 (seperti petquar) vs tetap `pyarrow` jika performa jadi masalah.
- Object store: S3 vs GCS.
