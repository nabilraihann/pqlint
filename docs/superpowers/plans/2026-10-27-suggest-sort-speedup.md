# Rencana Implementasi: Percepatan suggest-sort

> **Untuk pekerja agentik:** SUB-SKILL WAJIB: gunakan superpowers:subagent-driven-development (disarankan) atau superpowers:executing-plans untuk mengerjakan rencana ini tugas demi tugas. Langkah memakai kotak centang (`- [ ]`) untuk pelacakan.

**Tujuan:** Membuat `pqlint suggest-sort` minimal 2× lebih cepat pada file contoh 33,6 juta baris, dengan rekomendasi dan estimasi ukuran yang persis sama.

**Arsitektur:** Setiap urutan kandidat dinilai dengan mengurutkan sampel lalu menulisnya di memori. Dua perubahan membuat penilaian itu lebih murah: kunci sort diubah menjadi peringkat angka (dense rank) sekali per kolom, lalu tiap percobaan memakai `sort_indices` + `take`; dan percobaan di setiap langkah pencarian dijalankan di thread pool (pyarrow melepas GIL). Hasil dimasukkan ke cache di thread utama dan dipilih dengan tie-break yang sudah ada, sehingga keluaran tidak bergantung pada jumlah thread.

**Teknologi:** Python >=3.11, pyarrow (tanpa dependensi baru), `concurrent.futures.ThreadPoolExecutor`, pytest.

**Spesifikasi:** desain yang disepakati dalam percakapan pada 2026-10-27 (tidak ada file spesifikasi tertulis). Pengukuran yang mendasarinya: `/home/nabil/.claude/plans/saat-ini-core-processing-shimmying-emerson.md`. Bagian B (bucket paralel di `compact`) ditunda dan di luar cakupan.

## Batasan Global

- Hanya `src/pqlint/suggest.py`, `tests/test_suggest.py`, `README.md`, `docs/contoh-penggunaan.md`, dan `CLAUDE.md` yang berubah. `compact.py` tidak disentuh (sort dengan peringkat terukur lebih lambat untuk satu kali sort: 1,80 s lawan 2,42 s).
- Tanpa dependensi baru; hanya pyarrow.
- Rekomendasi (baris `Recommended`) dan tabel langkah (kunci dan estimasi ukuran) harus sama dengan baseline sebelum perubahan pada `asp-so-idr-df.parquet`. Jumlah `Tried N orders` boleh naik karena langkah trim sekarang menghitung semua kandidatnya di awal.
- `--threads 1` harus berjalan serial dan memberi hasil identik dengan jumlah thread berapa pun.
- Thread bawaan: `min(os.cpu_count(), 8, batas memori)`, dengan batas memori = `0,5 * tersedia / (1,5 * sample.nbytes)`, dan 4 bila memori tersedia tidak diketahui. Nilai `--threads N` eksplisit dipakai apa adanya.
- `uv run pytest -q` tetap hijau seluruhnya (saat ini 145 tes) setelah setiap tugas.
- Pesan commit diakhiri `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`. Jangan pernah meng-commit file `*.parquet` atau keluaran benchmark.

## Fokus Review

- **Kolom kunci bertipe dictionary (pandas `category`).** Saat ini `suggest` crash dengan `ArrowNotImplementedError` karena `Table.sort_by` dan `pc.rank` tidak mendukung array dictionary. Yang diharapkan: saran normal. Dikunci di Tugas 1 (peringkat dari nilai yang didekode) dan Tugas 3 (ujung ke ujung).
- **Nilai `--compression` yang salah.** Yang diharapkan: pesan galat bersih `pqlint suggest-sort: ...` dan exit 2, bukan traceback dari worker thread. Dikunci di Tugas 3.
- **`--threads 0` atau negatif.** Yang diharapkan: galat gaya argparse, exit 2. Dikunci di Tugas 4.
- **Input sangat kecil (1–3 baris, satu kolom yang bisa dipakai).** Yang diharapkan: tidak crash, tidak ada galat pool kosong. Dikunci di Tugas 3.
- **Kolom kunci dengan banyak null.** Yang diharapkan: null diurutkan terakhir, permutasi sama dengan `sort_by`. Dikunci di Tugas 1.

---

### Tugas 1: Fungsi bantu sort dengan peringkat angka

**Berkas:**
- Ubah: `src/pqlint/suggest.py` (tambahkan fungsi bantu di dekat `_encoded_size`)
- Tes: `tests/test_suggest.py`

**Antarmuka:**
- Menghasilkan:
  - `_rank_table(table: pa.Table, columns: list[str]) -> pa.Table`: satu kolom peringkat dense per nama di `columns` (uint64, null diberi peringkat terakhir). Kolom dictionary didekode lebih dulu ke tipe nilainya. Kolom yang `pc.rank`-nya melempar `pa.ArrowNotImplementedError` atau `pa.ArrowInvalid` dipertahankan sebagai nilai aslinya (yang sudah didekode), yang tetap bisa diurutkan oleh `sort_indices`.
  - `_order(ranks: pa.Table, keys: list[str]) -> pa.Array`: `pc.sort_indices(ranks, sort_keys=[(k, "ascending") for k in keys])`.
  - `_encoded_size(table: pa.Table, keys: list[str], compression: str, ranks: pa.Table | None = None) -> int`: tidak berubah bila `ranks is None`; selain itu mengurutkan dengan `table.take(_order(ranks, keys))`. `keys` kosong berarti tidak diurutkan pada kedua cara.

- [ ] **Langkah 1: Catat baseline (sebelum ada perubahan kode)**

Jalankan dengan kode yang sekarang, simpan di direktori scratch sesi ini:
`time uv run pqlint suggest-sort asp-so-idr-df.parquet --color never > <scratchpad>/suggest_baseline.txt 2> <scratchpad>/suggest_baseline.err`
Diharapkan: exit 0, waktu sekitar 1m50s, `Recommended  --sort-by category,dc,store,item_code_b2b`. Simpan waktunya sebagai catatan (sekitar 109 s).

- [ ] **Langkah 2: Tulis tes yang gagal**

Di `tests/test_suggest.py` tambahkan fixture `typed_table` (±600 baris, banyak nilai kembar, `None` disebar di setiap kolom) dengan kolom `i` (int64), `s` (string), `ls` (large_string), `d` (date32), `ts` (timestamp us, tz UTC), `dec` (decimal128(10,2)), `b` (bool), `g` (string ter-dictionary, yaitu `pa.array(...).dictionary_encode()`), serta tes berikut:
  - `test_rank_order_equals_sort_indices_for_every_type`: untuk setiap kolom dan daftar kunci `["g", "i"]`, `["b", "d", "ts"]`, `["dec", "ls", "s", "i"]`, `_order(_rank_table(t, all_cols), keys)` sama dengan `pc.sort_indices(t_decoded, sort_keys=...)`, dengan `t_decoded` mengubah kolom dictionary ke tipe nilainya.
  - `test_nulls_sort_last`: untuk kolom `s`, setiap baris bernilai null berada setelah semua baris non-null di `_order(...)`.
  - `test_rank_size_equals_sort_by_size`: untuk kolom non-dictionary dan kunci `["d", "s"]`, `_encoded_size(t, keys, "zstd", ranks=_rank_table(t, keys)) == _encoded_size(t, keys, "zstd")`.
  - `test_unsupported_rank_type_falls_back`: monkeypatch `S.pc.rank` dengan pembungkus yang melempar `pa.ArrowNotImplementedError` bila `col.type == pa.date32()` dan selain itu memanggil fungsi asli; pastikan `_rank_table(t, ["d"])["d"].type == pa.date32()` dan urutannya tetap sama dengan acuan.

- [ ] **Langkah 3: Jalankan tes, pastikan gagal**

Jalankan: `uv run pytest tests/test_suggest.py -q -k "rank or nulls_sort_last or falls_back"`
Diharapkan: GAGAL dengan `AttributeError: module 'pqlint.suggest' has no attribute '_rank_table'`.

- [ ] **Langkah 4: Implementasikan `_rank_table`, `_order`, dan parameter `ranks` pada `_encoded_size`**

Pakai `pc.rank(col, sort_keys="ascending", tiebreaker="dense")` per kolom; dekode dictionary dengan `col.cast(col.type.value_type)`.

- [ ] **Langkah 5: Jalankan seluruh suite**

Jalankan: `uv run pytest -q`
Diharapkan: semua lulus (tes lama tidak terpengaruh, tes baru hijau).

- [ ] **Langkah 6: Commit**

```bash
git add src/pqlint/suggest.py tests/test_suggest.py
git commit -m "Add rank-encoded sorting helpers for suggest-sort"   # ditambah trailer Co-Authored-By
```

---

### Tugas 2: Penghitung skor ber-cache dan paralel

**Berkas:**
- Ubah: `src/pqlint/suggest.py`
- Tes: `tests/test_suggest.py`

**Antarmuka:**
- Memakai: `_rank_table`, `_encoded_size` dari Tugas 1.
- Menghasilkan:
  - `class _Scorer` dengan `__init__(self, sample: pa.Table, columns: list[str], compression: str, threads: int = 1, use_ranks: bool = True)`; `__call__(self, keys: list[str]) -> int` (ukuran ter-encode sampel yang diurutkan menurut `keys`, di-cache menurut `tuple(keys)`); `prefetch(self, orders: list[list[str]]) -> None` (menghitung setiap urutan yang belum ada di cache, di `ThreadPoolExecutor` dengan `min(threads, jumlah urutan yang belum ada)` worker, atau serial bila hasilnya 1; urutan kembar dihitung sekali; cache ditulis di thread pemanggil setelah semua worker selesai; galat di worker diteruskan dan pool dimatikan); properti `tried` = jumlah urutan yang ada di cache.
  - `use_ranks=False` menilai dengan jalur `sort_by` biasa (dipertahankan sebagai acuan tes).

- [ ] **Langkah 1: Tulis tes yang gagal**

  - `test_scorer_matches_plain_sort_by`: pada sampel mirip `sales` (pakai `S._entity_sample` atau tabel kecil buatan tangan berisi region/store/item/day), untuk 6 daftar kunci berbeda `_Scorer(...)(keys) == _encoded_size(sample, keys, "zstd")`, dan `_Scorer(...)([]) == _encoded_size(sample, [], "zstd")`.
  - `test_prefetch_threads_give_same_sizes`: `_Scorer(..., threads=1)` dan `_Scorer(..., threads=4)` setelah `prefetch(orders)` memiliki cache yang sama untuk 8 urutan yang sama, termasuk satu urutan kembar.
  - `test_prefetch_computes_each_order_once`: monkeypatch `S._encoded_size` dengan pembungkus penghitung; setelah `prefetch([a, b, a])` lalu memanggil scorer pada `a`, pembungkus berjalan tepat dua kali.
  - `test_prefetch_propagates_worker_errors`: dengan `compression="no-such-codec"`, `prefetch` melempar galat (jenis apa pun) dan `threading.active_count()` sesudahnya tidak menunjukkan thread pool yang tersisa (bandingkan dengan jumlah sebelumnya).

- [ ] **Langkah 2: Jalankan tes, pastikan gagal**

Jalankan: `uv run pytest tests/test_suggest.py -q -k "scorer or prefetch"`
Diharapkan: GAGAL dengan `AttributeError: ... '_Scorer'`.

- [ ] **Langkah 3: Implementasikan `_Scorer`**

Bangun tabel peringkat sekali di `__init__` bila `use_ranks` benar dan `columns` tidak kosong. Worker memanggil metode murni yang hanya membaca `sample` dan tabel peringkat.

- [ ] **Langkah 4: Jalankan seluruh suite**

Jalankan: `uv run pytest -q`
Diharapkan: semua lulus.

- [ ] **Langkah 5: Commit**

```bash
git add src/pqlint/suggest.py tests/test_suggest.py
git commit -m "Add cached, thread-parallel order scorer"   # ditambah trailer
```

---

### Tugas 3: Pakai scorer di pencarian dan tentukan ukuran thread pool

**Berkas:**
- Ubah: `src/pqlint/suggest.py` (isi `suggest()`; `resolve_threads` baru; `_check_compression` baru)
- Tes: `tests/test_suggest.py`

**Antarmuka:**
- Memakai: `_Scorer` (Tugas 2); `pqlint.compact.available_memory() -> int | None`.
- Menghasilkan:
  - `resolve_threads(requested: int | None, sample_bytes: int, available: int | None, cpus: int | None = None) -> int`: `requested` menang bila diberikan (minimum 1); selain itu `max(1, min(cpus or os.cpu_count() or 1, MAX_AUTO_THREADS, cap))` dengan `cap = int(0.5 * available / (1.5 * sample_bytes))`, dan `4` menggantikan `cap` bila `available is None`. Konstanta `MAX_AUTO_THREADS = 8`, `THREAD_MEMORY_SHARE = 0.5`, `TRIAL_MEMORY_FACTOR = 1.5`.
  - `suggest(paths, sample_rows, compression, start_with, max_keys, min_gain, seed, threads: int | None = None, progress=None) -> Suggestion`: `threads` baru (keyword, setelah `seed`).
  - `_check_compression(codec: str) -> None`: menulis tabel satu baris dengan codec itu ke `pa.BufferOutputStream`; bila `ValueError` atau `pa.ArrowException`, melempar `SuggestError(f"unsupported compression codec {codec!r}")`. `suggest()` memanggilnya sebelum membaca data apa pun.

- [ ] **Langkah 1: Tulis tes yang gagal**

  - `test_resolve_threads_cases`: nilai eksplisit menang (`resolve_threads(3, 10**9, 1, cpus=16) == 3`); eksplisit 0 atau negatif dijepit ke 1; memori melimpah dan 16 cpu menghasilkan 8; 2 cpu menghasilkan 2; `available=None` dengan 16 cpu menghasilkan 4; `available = 3 * 1.5 * sample_bytes / 0.5` menghasilkan 3; memori tersedia sangat kecil menghasilkan 1.
  - `test_threads_do_not_change_the_result`: `run(sales, threads=1) == run(sales, threads=4)` (kesetaraan dataclass, termasuk `steps` dan `tried`).
  - `test_suggest_works_with_dictionary_columns`: tulis tabel dengan kolom dictionary gaya pandas `store` (string ter-dictionary) plus kolom int `item`; `run(...)` mengembalikan `Suggestion` yang `best.keys`-nya tidak kosong dan memuat `store` atau `item`.
  - `test_tiny_input_is_fine`: tabel 3 baris dengan satu kolom bernilai-sedikit mengembalikan `Suggestion` (steps boleh kosong) tanpa galat, untuk `threads=1` dan `threads=8`.
  - `test_bad_compression_is_a_clean_error`: `run(sales, compression="no-such-codec")` melempar `S.SuggestError` yang pesannya memuat `no-such-codec`.
  - `test_progress_reports_thread_count`: kumpulkan pesan `progress`; salah satunya memuat `thread`.

- [ ] **Langkah 2: Jalankan tes, pastikan gagal**

Jalankan: `uv run pytest tests/test_suggest.py -q -k "resolve_threads or threads_do_not or dictionary_columns or tiny_input or bad_compression or reports_thread"`
Diharapkan: GAGAL (`resolve_threads` tidak ada / argumen `threads` tidak dikenal).

- [ ] **Langkah 3: Implementasi**

Di `suggest()`: panggil `_check_compression` lebih dulu; setelah sampel ada, `threads = resolve_threads(threads, sample.nbytes, available_memory())`, kirim `progress(f"searching with {threads} thread(s)")`, dan ganti `scores`/`score` lokal dengan `scorer = _Scorer(sample, <kandidat + start tanpa duplikat>, compression, threads)`. Sebelum setiap tempat yang menilai banyak urutan, panggil `scorer.prefetch(...)` dengan tepat urutan yang kemudian dibandingkan oleh kode itu: tiap langkah greedy `start + greedy + [c] for c in rest`; ketiga `seeds` (masing-masing `start + o`); tetangga hill-climb per putaran (masing-masing `start + o`); varian `shorter` dari tiap pass trim (hanya yang tidak kosong); dan `[[]] + [final[:n] for n in range(max(len(start), 1), len(final) + 1)]` sebelum `measure`. Kode pemilihan (`min(..., key=(score, ...))`) dibiarkan. `tried = scorer.tried`.

- [ ] **Langkah 4: Jalankan seluruh suite**

Jalankan: `uv run pytest -q`
Diharapkan: semua lulus; tes `suggest` lama tetap menghasilkan rekomendasi yang sama.

- [ ] **Langkah 5: Commit**

```bash
git add src/pqlint/suggest.py tests/test_suggest.py
git commit -m "Run suggest-sort trials in parallel with rank-encoded sorting"   # ditambah trailer
```

---

### Tugas 4: Opsi `--threads`

**Berkas:**
- Ubah: `src/pqlint/suggest.py` (`main()`)
- Tes: `tests/test_suggest.py`

**Antarmuka:**
- Memakai: `suggest(..., threads=...)` dari Tugas 3.
- Menghasilkan: flag CLI `--threads N` (int, bawaan `None` = otomatis) pada `pqlint suggest-sort`; `N < 1` ditolak dengan `parser.error("--threads must be at least 1")` (argparse mencetak usage, status keluar 2).

- [ ] **Langkah 1: Tulis tes yang gagal**

  - `test_cli_threads_option_runs`: `main(["suggest-sort", str(sales), "--sample-rows", "15000", "--color", "never", "--threads", "2"]) == 0`, dan stderr memuat `2 thread`.
  - `test_cli_threads_one_equals_default_output`: stdout `--threads 1` sama dengan stdout `--threads 3` (laporan yang dicetak).
  - `test_cli_rejects_zero_threads`: `main([... "--threads", "0"])` melempar `SystemExit` berkode 2 dan stderr memuat `--threads must be at least 1`; sama untuk `-1`.

- [ ] **Langkah 2: Jalankan tes, pastikan gagal**

Jalankan: `uv run pytest tests/test_suggest.py -q -k "cli_threads or zero_threads"`
Diharapkan: GAGAL (`unrecognized arguments: --threads`).

- [ ] **Langkah 3: Implementasikan flag**

Tambahkan argumen dengan teks bantuan `"parallel trials (default: automatic, at most 8 and limited by free memory; 1 = serial)"`, validasi setelah `parse_args`, dan teruskan `threads=args.threads` ke `suggest()`.

- [ ] **Langkah 4: Jalankan seluruh suite**

Jalankan: `uv run pytest -q`
Diharapkan: semua lulus.

- [ ] **Langkah 5: Commit**

```bash
git add src/pqlint/suggest.py tests/test_suggest.py
git commit -m "Add --threads to suggest-sort"   # ditambah trailer
```

---

### Tugas 5: Benchmark pada file asli, perbarui dokumentasi, selesai

**Berkas:**
- Ubah: `README.md` (bagian suggest-sort), `docs/contoh-penggunaan.md` (bagian 5), `CLAUDE.md` (bagian suggest-sort)

**Antarmuka:**
- Memakai: CLI yang sudah jadi dari Tugas 4.

- [ ] **Langkah 1: Verifikasi kesetaraan dan kecepatan terhadap baseline**

Jalankan `time uv run pqlint suggest-sort asp-so-idr-df.parquet --color never > <scratchpad>/suggest_after.txt` dan hal yang sama dengan `--threads 1` ke `suggest_after_t1.txt`.
Lalu untuk masing-masing: `diff <(grep -E '^ +[0-9]+ +|Recommended' <scratchpad>/suggest_baseline.txt) <(grep -E '^ +[0-9]+ +|Recommended' <scratchpad>/suggest_after.txt)`.
Diharapkan: tidak ada diff (kunci, estimasi ukuran, dan urutan rekomendasi sama). Jalur bawaan minimal 2× lebih cepat dari baseline (sekitar 109 s); `--threads 1` juga lebih cepat dari baseline (karena peringkat angka). Bila ada diff, berhenti dan debug sebelum menyentuh dokumentasi; bila percepatan di bawah 2×, laporkan angka sebenarnya dan di mana waktunya habis (`cProfile`), jangan menyetel secara buta.

- [ ] **Langkah 2: Rekam ulang keluaran demo**

Buat ulang data demo (`uv run python examples/make_demo_data.py <scratchpad>/demo`) dan jalankan `pqlint suggest-sort <scratchpad>/demo/sales.parquet --color never`. Diharapkan: rekomendasi sama seperti yang terdokumentasi (`item_code,kode_store`).

- [ ] **Langkah 3: Perbarui dokumentasi**

README: tambahkan `--threads N` ke bagian suggest-sort dan satu kalimat berisi waktu sebelum/sesudah hasil ukur pada file contoh. `docs/contoh-penggunaan.md` bagian 5: tambahkan baris progres `… searching with N thread(s)` ke contoh keluaran, ganti kalimat waktu dengan hasil ukur ulang, dan sebut `--threads`. `CLAUDE.md` (bahasa Indonesia, di bagian suggest-sort): peringkat angka hanya untuk suggest-sort (di compact terukur lebih lambat); percobaan berjalan paralel tetapi di-cache dan dipilih di thread utama sehingga hasil tidak bergantung jumlah thread; rumus thread bawaan; kunci sort bertipe dictionary berfungsi di suggest-sort karena peringkat mendekodenya, sedangkan `compact --sort-by` pada kolom dictionary masih melempar `ArrowNotImplementedError` (celah yang diketahui, perbaikan terpisah); percepatan yang terukur.

- [ ] **Langkah 4: Jalankan seluruh suite**

Jalankan: `uv run pytest -q`
Diharapkan: semua lulus; `git status --short` hanya menampilkan berkas yang disebut di atas.

- [ ] **Langkah 5: Commit dan push**

```bash
git add README.md docs/contoh-penggunaan.md CLAUDE.md
git commit -m "Document suggest-sort speed-up and --threads"   # ditambah trailer
git push
```
