# Where Python is the ceiling: making migkit's hot paths as fast as the compiled tools (research as of 2026-09-28)

**Status: COMPLETE (2026-09-28).** Sections 0-15 written: migkit's hot paths and recorded numbers, runtimes, compiled extensions, decoders, Arrow paths, hashing, JSON, compression, I/O, allocation, a per-path table, docker measurement recipes, the decision seam, a ranked plan, sources. Nothing here is measured by this research; every "estimate" awaits section 12.

Scope: only the places where Python itself - not the database, not the network - sets the rate: decoding change logs, rendering and hashing rows for cross-engine digests, moving rows between drivers, collapsing batches. Everything here is public research plus a reading of migkit's own code and the measurements it already records. No database was connected to and nothing was run except printing library versions from `.venv`. Figures are marked **measured (migkit)**, **published** (with the source and who published it) or **estimate** (my arithmetic, to be measured before it is claimed).

---

## 0. The answer in brief

1. **On the MySQL tail the ceiling is most likely the GIL coupling decode and apply, not the speed of Python decoding.** migkit measured the binlog reader alone at 8.6 µs a change (116,000/s) and the whole tail at 15.3 µs a change (65,000/s). `_ReadAhead` is a *thread*: under the GIL the reader's Python work and the applier's Python work take turns on one core, so 15.3 ≈ 8.6 + ~6.7. If that holds (one recipe below settles it), moving the *same* pymysqlreplication decoder into a child process - no new dependency, no new decoder to trust - takes the tail toward ~150,000/s (estimate), and a compiled decoder adds nothing until the apply side's ~6.7 µs a change also comes down.
2. **Published compiled CDC numbers are not far ahead of migkit's Python decoder end to end:** Debezium PostgreSQL tuned 30,000 ops/s and a Rust/Arrow tool 35,000 ops/s in a sponsored benchmark; PeerDB's Go pgoutput consumer ~250,000 inserts/s; Debezium MySQL user reports 2,500-10,000/s. The gap is in *raw parsing* (Go rewrites of a pymysqlreplication tool: ~30x), which only matters once the GIL coupling and apply cost are gone.
3. **The PostgreSQL tail's ceiling is not Python at all:** three `psql` processes (three connections) per batch, and SQL-interface `peek` over `test_decoding` text. A persistent connection, then streaming `pgoutput`, are the levers - and they also remove a latent stall on text values holding newlines (section 1.2).
4. **Render+hash:** MD5 is pinned by the SQL digests; hashing is 13% of the in-process fold, rendering 80%. A compiled renderer is a fourth implementation of the canonical text and is worth it only where the fold is on a hot path (non-SQL sources, keyless tables, drilldowns). Specialise the Python renderer per column first.
5. **Not now:** free-threaded 3.14t (psycopg and psycopg2 re-enable the GIL), subinterpreters (PyO3, Cython, pyarrow and zstandard modules refuse to load), PyPy for the whole of migkit (numpy/pandas/pyarrow/duckdb block it; PyPy 3.11 is frozen as of 8.0.0), asyncio/uvloop/asyncpg rewrites, orjson in the canonical path, a faster hash on the cross-engine digest.

---

## 1. migkit's hot paths, as the code and its own measurements stand

**Environment read from `.venv` (2026-09-28):** CPython 3.12.13 (GIL build; `Py_GIL_DISABLED` unset), mysql-replication 1.0.17, psycopg 3.3.5 + psycopg-binary 3.3.5 (no psycopg-c), psycopg2-binary 2.9.13, pymongo 4.18.1, pyarrow 25.0.1, zstandard 0.25.0, lz4 4.4.5, fastavro 1.12.2, polars 1.43.2, duckdb 1.5.5, numpy 2.5.3, PyMySQL 1.2.3. **Not installed:** orjson, xxhash, blake3, msgspec, cramjam, uvloop, asyncpg, any ADBC driver, ConnectorX, psycopg-c. `pyproject.toml`: `requires-python >= 3.10`, every engine driver a hard dependency, plus pandas, pyarrow, duckdb, reladiff, datacompy. migkit is released on GitHub only (no PyPI wheels yet - `security-throughput-scorecard-2026-09-28.md`), which matters for anything compiled.

### 1.1 The paths, and what already measured them

| Path | Where | What Python does per row / change | Recorded measurement |
|---|---|---|---|
| MySQL binlog decode | `engines/mysql.py` `neutral_changes` (pymysqlreplication `BinLogStreamReader`, stream held open between calls; `_row_events`; `binlog_names`; `canon.change` per row) | pure-Python packet parse (PyMySQL socket reads), row image decode per column, a dict per row image, a dict per change | **measured (migkit), R2.6:** 300,000 changes decoded in 2.58 s (116,000/s) on their own; whole tail 320,000 in 4.9 s (65,000/s) |
| PostgreSQL slot read | `engines/postgres.py` `neutral_changes` + `pgslot.py` | three `psql` subprocesses per batch (`pg_replication_slot_advance`, `pg_current_wal_lsn()`, `pg_logical_slot_peek_changes`), then a hand-written scanner over `test_decoding` text, `canon.from_text` per value | no rate recorded. **Not pgoutput**: the module parses `test_decoding` text (a stock `postgres:16` ships only `pgoutput` and `test_decoding`) |
| Tail read-ahead | `engines/hetero.py` `_ReadAhead` | one *thread* reads the next batch while this one applies; MySQL only (`READS_AHEAD`) | **measured:** read 9.4 s + apply 6.1 s serial; with read-ahead and growing batches 7.7 s, mapping skipped 5.5 s, lanes 4.9 s (320,000 changes) |
| Collapse, net rows, lanes, runs | `engines/base.py` `_collapsed`, `_net_rows`, `_lanes`, `_apply_net` | per change `tuple(sorted((n, repr(v)) ...))`; per row sorted shape tuples and `str(v)` idents; lanes hash `repr` of every key value again | **measured:** 80,000 changes 10 ms away: 4,330 statements, 56 s before runs; 10.0 s and 6.0 s with runs and lanes |
| Column mapping | `hetero.py` `_mapped_change` | `dict(change)` + `_mapped_types` per part, only where rules exist | **measured:** skipping it where there is no mapping: 7.7 s -> 5.5 s |
| Neutral copy loop | `hetero.py` `_neutral_move`, `_write_checked`, `_move_in_ranges` | driver rows -> `list(r)` -> `canon.sql_rows` (non-`PLAIN` classes only) -> COPY `write_row` (PostgreSQL) / `executemany` or the LOAD path (MySQL) | **measured:** 1,000,000 rows MySQL->PostgreSQL 10.6 s in one process, 8.1 s in two, 7.0 s in four (2-CPU server): "threads gave it nothing; processes did" |
| Range workers | `ranges.py` `_Worker` | long-lived `python -m migkit.ranges` children, pickled `(fn, item)` over pipes | **measured:** 7.7 s in 8 ranges vs 11.2 s in 16 |
| Batch verify | `hetero.py` `_batch_digest`, `_verify_batch`, `_rows_differ` | range digest asked of both servers; row-by-row `canon.render_value` only on a mismatch | **measured:** folding the batch in Python took 7.8 of 32.7 s a million rows, so it moved to the servers |
| Row render + hash | `canon.render_value`, `rowtext.encode`, `canon.fold_rows`, `digest_step` | class dispatch per value, `Decimal` formatting, `strftime`, `json_text`; length-prefixed join; `hashlib.md5(...).hexdigest()`; `int(h[:15], 16)` summed | **measured, R18.3:** 200,000 rows in 0.45 s: render 0.36 s, MD5 0.06 s |
| Read sizing | `hetero.py` `_read_rows`, `sizing.py` | reads capped at 50,000 rows / 64 MB | **measured:** 486 MB held at 500,000 rows of ~250 B; 36 MB a copier on a million rows |
| Avro change stream | `avrostream.py` | `fastavro.schemaless_reader` per message, `parse_schema` per message | none |

### 1.2 What the code says before any web research

1. **The digest is pinned to MD5 of an exact text.** `digest_expr` makes MySQL and PostgreSQL compute `sum(first 15 hex digits of md5(row text))`; the in-process fold must give the same number. A faster hash (XXH3, BLAKE3) cannot replace MD5 on the cross-engine digest (R18.3 says so too). What can go faster is the rendering (80% of the fold) and the per-row call overhead of MD5.
2. **The rendering has three implementations held together by tests** (MySQL SQL, PostgreSQL SQL, `canon.render_value`). A compiled port is a fourth. The traps are already written into the code: `Decimal` printed with `format(v, "f")` (never `1E-10`); `-0.0` as `0.0`; a float through `repr` and then a 20-place quantize; instants in UTC with six places; `rowtext.encode` counts **code points**, not bytes, because `char_length`/`length` do; JSON object keys sorted by (UTF-8 length, bytes), numbers kept as the decimal they were written as.
3. **The binlog reader is pure Python end to end.** PyMySQL reads the socket in Python, pymysqlreplication decodes every column in Python. A public profile of the same library (pg_chameleon, python-mysql-replication issue #251, 2018) put 33.5% of time in the event loop and 48% in row decoding against 6.4% writing to the target.
4. **`_ReadAhead` is a thread.** Under the GIL, decode (all Python) and apply (Python building statements around the drivers' C code) take turns on one core; the read-ahead recovers only what socket waits and target round trips leave idle. The numbers fit this: 15.3 µs a change end to end ≈ 8.6 µs decode + ~6.7 µs apply. A decode in another *process* would overlap fully.
5. **The PostgreSQL tail's ceiling is mostly not Python.** Each batch runs three `psql` processes, each a new connection (and TLS handshake where configured); `pg_logical_slot_peek_changes` decodes again from the slot's position on every call, because the slot only moves when the next call hands the token back. A persistent connection removes the first; streaming `pgoutput` over the replication protocol removes both, and is where a compiled decoder would plug in.
6. **Found in passing (to verify in docker, not claimed):** `psql -At` prints a field raw, and `test_decoding` prints a text value with only its apostrophes doubled, so a value holding a newline arrives split across two output lines. `rows.splitlines()` (which also splits on `\x1c`-`\x1e`, `\x85`, U+2028, U+2029) then hands `parse_line` half a value and `_scan_fields` raises `unterminated value`. The tail stops there (peek never advances), so nothing is lost silently - but any row with a newline in a text column would stall the PostgreSQL tail. No test in `tests/test_pgslot_live.py` inserts one. Reading `lsn` and `data` as two columns through a driver, or a length-prefixed binary decoder (`pgoutput`), cannot fail this way.
7. **Per-change allocation is heavy on the apply side.** One change is a dict (`canon.change`) holding dicts (`key`, `values`, `before`); `_collapsed` builds `tuple(sorted((n, repr(v)) ...))` per change; `_net_rows` sorts key and value names per row; `_apply_net` builds `tuple(sorted((n, str(v)) ...))` per row; `_lanes` hashes `repr` of every value again. For a 20-column table that is on the order of a hundred small allocations and three or four sorts per change - on the side R2.6 names as the limit.

---

## 2. Runtimes: free-threading, subinterpreters, processes, PyPy, and CPython itself

### 2.1 CPython versions (no code change)

* **3.14's tail-calling interpreter:** first reported as 9-15% faster; most of that was a Clang/LLVM 19 regression in the baseline. Revised by the author to **3-5%**, 1-5% against a fair baseline (Ken Jin's correction; Nelson Elhage's analysis). LLVM 20.1.1 fixed the regression; the migkit venv's CPython 3.12.13 reports Clang 22, so it is not the slow build.
* **JIT:** 3.13/3.14's JIT was often slower than the interpreter; 3.15's measures 4-12% faster geometric mean (PEP 836), and JIT work was paused by the Steering Council in June 2026 pending PEP 836. Not a lever for migkit this year.
* **GC:** 3.14.0 regressed badly on heaps of small tuples (a published case: 15 M tuples 142.9 s with GC on vs 1.07 s off; fixed in 3.14.1), and the incremental GC was **reverted in 3.14.5**. Relevant because the tail allocates many small tuples and dicts per batch: `gc.freeze()` after start-up and a higher first threshold (`gc.set_threshold(50_000, ...)`) are cheap, measurable levers; a published web-app case saw 20% (Michael Kennedy). The batch structures are acyclic, so a `gc.disable()` around building a batch is safe in principle - measure before adopting.
* **Expected gain for migkit:** single-digit percent from upgrading the interpreter; the GC settings are the only part worth a measurement now.

### 2.2 Free-threaded CPython (3.13t, 3.14t)

* **Status:** 3.14 made the free-threaded build *supported* (PEP 779, phase II, accepted June 2025). Single-thread cost on pyperformance: ~1% (macOS arm64) to ~8% (x86-64 Linux) in 3.14 (official HOWTO), ~40% in 3.13. Independent: 9% (Miguel Grinberg), 33% on a Mandelbrot kernel. Meta's April 2026 run of 3.15a: ~9% x86-64, ~6% macOS arm64.
* **The catch that decides it for migkit:** a C extension not marked free-thread safe **re-enables the GIL for the whole process** on import. Status of what migkit imports: psycopg 3 - not supported (issue #1095, maintainers unsure when); psycopg2 - re-enables the GIL (issue #1810); pymongo - supported since 4.11 (not Windows, not In-Use Encryption), 3.14t from 4.14; pyarrow - 3.14t wheels (3.13t dropped June 2026); python-zstandard - declares the GIL unused and is tested in CI, but the project does not yet call it formally supported; cryptography - 3.14t since 46.0.0 (so PyMySQL's `caching_sha2` path is fine); orjson - unclear; PyMySQL and mysql-replication - pure Python, fine. Adoption across the top 360 extension packages: 180/360 on 19 February 2026 (Quansight).
* **Expected gain for migkit's paths:** none today for anything that touches PostgreSQL (psycopg re-enables the GIL). A MySQL-to-MySQL copy with PyMySQL on both ends could run ranges as threads instead of processes - the processes already work (R1), so the gain is memory, not speed.
* **Correctness risk:** migkit's own shared state (`self.__dict__` caches such as `_binlog_held`, `_ordered_of`, the origin lock) was written for the GIL; free-threading turns latent races into real ones.
* **Packaging:** a separate `cp314t` wheel per platform for anything compiled (abi3 does not load on 3.14t; `abi3t`, PEP 803, starts at 3.15).
* **Recipe:** `uv python install 3.14t`, a scratch venv, `python -c "import psycopg, pymysql, pymysqlreplication, pymongo, pyarrow, zstandard, duckdb, sys; print(sys._is_gil_enabled())"` - `True` ends the question for that combination.

### 2.3 Subinterpreters (PEP 734, `concurrent.interpreters` in 3.14)

* **Published:** a worker starts in ~9 ms against ~33 ms for a spawned process and ~0.04 ms for a thread (3.14.7, Apple M2); pure-Python CPU work on 8 cores: interpreters 0.52 s wall (5.6x parallel) vs processes 0.75 s vs threads 2.02 s. Anthony Shaw's PyCon 2024 summary: multiprocessing with less overhead and shared memory.
* **The catch:** third-party extensions must opt in. Fails to load today: numpy, pyarrow, Cython-built modules, PyO3-built modules (pydantic-core; PyO3 needs a redesign, issue #3451), python-zstandard (conda's experiment); psycopg loaded in the same experiment. pymysqlreplication and PyMySQL are pure Python and would load; `binlog_payload.py` imports `zstandard` for compressed transactions and would not (the stdlib `compression.zstd` in 3.14 would). Objects cross interpreters by pickling except `memoryview`/buffer-protocol data and `interpreters.Queue`.
* **Expected gain for migkit:** the same as the child-process decoder in section 2.4, minus ~20 ms of start-up per worker that migkit already amortises with long-lived workers. Not worth a second mechanism while `ranges._Worker` exists.

### 2.4 Processes, shared memory, Arrow IPC

* **What migkit already has:** long-lived `python -m migkit.ranges` children answering pickled requests (`ranges._Worker`), measured faster than threads for the copy.
* **Moving data between processes:** Arrow IPC in `multiprocessing.shared_memory` is zero-copy for numeric columns; string columns still become Python `str` per reader (a published migration from pickle to Arrow mmap found exactly that). Pickle protocol 5 with out-of-band buffers helps only for large buffers. For migkit's change records - small dicts of scalars - neither is zero-copy: the cost is building Python objects on the receiving side, which pickle does in C.
* **Estimate for the tail:** unpickling a change record is on the order of 1-3 µs against ~8.6 µs to decode it in Python, so a decoder process is a net win of several µs a change on the applier's core; a compact transport (a per-table column list sent once, then `(table_no, op, key_tuple, values_tuple)`) shrinks it further. `Decimal` pickles through its string (`__reduce__`), so decimal-heavy tables pay more - measure.
* **Correctness:** the sentinel `canon.ABSENT` survives pickling as itself because `_Absent.__new__` returns the singleton; `canon.Added` has `__slots__` and pickles by protocol 2+. Worth one test that sends every value kind across the boundary and checks `is` / `==` / canonical rendering.
* **Packaging:** none.

### 2.5 PyPy

* **State:** PyPy 8.0.0 (19 September 2026) ships PyPy3.11 - announced as its last 3.11 release barring security fixes - and PyPy3.12 as beta; HPy dropped; Linux builds need glibc 2.28. Headline speed about 3x CPython 3.11 (pypy.org), 4.3x on a new benchmark runner (June 2026 post); no published CPython 3.12/3.13 baseline. uv's docs now warn that PyPy is not actively developed (PyPy developers dispute the wording); numpy dropped PyPy with 2.5.
* **migkit's libraries on PyPy:** psycopg 3 only as the pure-Python (ctypes) implementation, which its own docs describe as much slower than the binary or C builds (neither exists for PyPy); psycopg2 via psycopg2cffi; pymongo works but has deprecated PyPy support; PyMySQL and pymysqlreplication are pure Python (pymysqlreplication's changelog carries a PyPy socket fix, so people run it there); orjson excludes PyPy by policy; pyarrow/pandas/numpy/duckdb/polars - not usable. **The whole of migkit cannot run on PyPy.**
* **Where it could still help:** as the interpreter of the decoder child only (section 2.4): `[pypy3, -m, migkit.decoder]` running pymysqlreplication + PyMySQL, handing records back by pickle. Byte-slicing, `struct` unpacking and small-object loops are what a tracing JIT is good at; nobody has published pymysqlreplication on PyPy. Estimate: 2-4x on decode, unmeasured.
* **Correctness risk:** PyPy's `decimal` and `datetime` are separate implementations; the decoded values must render identically (the shadow check in section 13 covers it).
* **Packaging:** the operator installs PyPy; migkit only detects it. Optional by construction.

---

## 3. Compiled code inside the process: PyO3/maturin, Cython, mypyc

### 3.1 What the published Rust extensions show

* **pydantic-core (PyO3):** Pydantic v2 is 5-50x faster than v1 per the project's announcement (the earlier plan said 4-50x, ~17x on a model of common fields; an independent check agreed on direction). The lesson for migkit: the gain came from moving a *per-value dispatch loop* (validation) out of the interpreter - the same shape as `canon.render_value`.
* **orjson (PyO3):** `dumps` ~10x and `loads` ~2x the stdlib on its README fixtures (loads can tie on string-heavy documents: github.json 0.23 ms both, v1.2.1 table).
* **polars, cramjam:** whole engines or thin codec bindings; cramjam's own advice is that passing `output_len` gives 1.5-3x by allocating once - allocation, not the codec, dominates small calls.
* **The ceiling of any extension called from Python:** each Python object it reads or builds still costs tens of nanoseconds. A Rust function handed a list of Python rows must walk `PyLong`, `PyUnicode`, `Decimal`, `datetime` objects; it wins by removing the interpreter's dispatch, attribute lookups and intermediate strings, not the objects themselves.

### 3.2 What a small extension for migkit's render+hash would cost

* **Scope:** one function, `fold_rows(classes, rows, total) -> (n, total)`, doing `render_value` + `rowtext.encode` + MD5 + the 60-bit sum in native code for the classes `integer`, `text`, `bytes`, `boolean`, `decimal`, `date`, `time`, `timestamp`; `float` and `json` call back into the Python renderer (the float band needs `repr`-exact shortest digits then a 20-place `ROUND_HALF_EVEN` quantize; the JSON rendering needs exact decimals and length-first key order - both are where a port goes wrong quietly).
* **Estimate:** ~0.3-0.5 µs a row of eight columns in native code against the measured 2.25 µs a row (0.45 s / 200,000), so **4-7x on the fold**. What it buys migkit end to end depends on where the fold runs (section 11): since R0 moved the batch digest to the servers, the fold is hot only for non-SQL sources (MongoDB, Kafka, files), keyless tables folded as they pass, and drilldowns.
* **Correctness risks, each already visible in the Python:** `Decimal` must print the digits it holds (`format(v, "f")`: `Decimal('1E-10')` is `0.0000000001`, trailing zeros kept); lengths are code points (a Rust `str.len()` is bytes - the classic port bug); `datetime` with a zone converted to UTC before formatting, six fractional digits always; MySQL `BIT` bytes read big-endian; `memoryview` and `bytearray` rendered as their bytes; `None` as the `N:` marker. **Guard:** a Hypothesis differential test (native vs Python on generated values of every class, including the edge values above) plus the existing cross-engine digest tests, and a runtime switch that falls back to Python for any class the native side does not claim.
* **Packaging burden (PyO3 + maturin):** `maturin generate-ci github` gives the usual 7 targets (manylinux and musllinux x86_64/aarch64, macOS arm64 and x86_64, Windows x86_64), aarch64 Linux cross-compiled inside maturin-action's containers without QEMU. One `abi3-py310` wheel per platform covers CPython 3.10+; free-threaded 3.14 needs its own `cp314t` wheel (abi3 does not load there), and `abi3t` (PEP 803) needs maturin >= 1.14 and CPython 3.15. PyPy needs its own wheels or the Python fallback. Because migkit publishes nothing to PyPI today, the realistic shape is a separate optional package (`migkit-speedups`, imported under `try:`), so the base install stays pure Python and a missing wheel is never an error. Effort: M for the code and tests, M for the release pipeline the first time.

### 3.3 Cython

* **Published:** compiling unmodified Python usually gains about 20-50% (Cython docs); typed code much more. Cython 3.1 (May 2025) added free-threading support (still experimental; modules opt in with `freethreading_compatible=True`). Cython-built modules do **not** load in subinterpreters.
* **For migkit:** `canon.render_value` spends its time in calls Cython cannot remove (`Decimal.__format__`, `datetime.strftime`, `hashlib`), so pure-mode Cython would land near the low end; a typed rewrite is as much work as the Rust one with a worse packaging story (a wheel per CPython version unless the Limited API mode is used). Not preferred.

### 3.4 mypyc

* **Published:** annotated code often runs 1.5-5x faster, code tuned for mypyc 5-10x (mypyc docs); mypy itself 4x; Black 2x (1.5x on files that change). Its README still calls it alpha.
* **For migkit:** the apply-side bookkeeping (`_collapsed`, `_net_rows`, `_apply_net`, `_lanes`) is plain dict/tuple/sort code of exactly the kind mypyc speeds up, with no second implementation to keep identical - the source stays the Python that runs when the compiled wheel is absent. Its catch is the same wheel matrix as Cython (per CPython version) and that compiled classes are stricter about monkeypatching (migkit's tests patch methods). A candidate only after the allocation diet in section 10, and only on a separate module boundary (e.g., a `migkit/_hot.py` that holds the collapse and run logic).

---

## 4. Change-log decoders

### 4.1 MySQL binlog

| Reader | Language | Published or measured rate | Notes |
|---|---|---|---|
| pymysqlreplication 1.0.17 (migkit's) | Python | **measured (migkit):** 116,000 changes/s decode only | pure Python on PyMySQL; issue #251 profile: 33.5% event loop, 48% row decode, 6.4% target write |
| binlog2sql -> binlog2sql_go | Python -> Go | **published:** 500 MB binlog in 12 min 59 s vs 23.7 s (~33x) | tool-level, includes SQL text generation; binlog2sql is built on python-mysql-replication. my2sql (Go): 1.1 GB in ~1.5 min; binlog2sql ~2 GB in ~54 min (ActionTech) |
| go-mysql (`replication`, `canal`) | Go | no published parse rate; go-mysql-transfer >4,000 TPS into Redis (includes the Redis write) | MIT; used by gh-ost; `EventCacheCount` 10,240 buffered events by default |
| mysql-binlog-connector-java (Debezium's) | Java | no parse-only number; Debezium MySQL users report 2,500-10,000/s; vendor (RisingWave) 50,000-100,000/s realistic, 200,000+/s best case; Debezium's Feb 2026 post: the source's writes, not Debezium, were the limit | single-task per connector |
| Rust: `mysql_common::binlog`, `mysql_binlog` (EasyPost), `mysql-binlog-connector-rust` (ApeCloud), `mysql-cdc-rs` | Rust | EasyPost's README claims orders of magnitude over go-mysql and python-mysql-replication (no benchmark shown; untested on MySQL 8.0) | `mysql_common` covers 8.0 compressed transaction payloads and optional table-map metadata (names, charsets, PK); ApeCloud returns CHAR/VARCHAR as bytes and unsigned as signed - exactly the traps migkit's `binlog_row_metadata=FULL` gate exists for. The only Rust parser with Python bindings (`mysqlbinlog-rs`) was last touched in 2018 |

**What it means for migkit:** a compiled parser is 10-30x faster at parsing (published cross-language ratios), but migkit's decode is 8.6 µs of a 15.3 µs change; the other ~6.7 µs are the applier's Python. A compiled decoder that hands back Python dicts still costs the applier the object construction (~1-2 µs a change, estimate). So the sequence is: decode out of the applier's process first (section 2.4), then the apply diet (section 10), then a compiled decoder - and only then does it pay the 10-30x.

### 4.2 PostgreSQL

| Reader | Language | Rate | Notes |
|---|---|---|---|
| migkit today: `psql` + `pg_logical_slot_peek_changes` + `test_decoding` text | Python + subprocess | not recorded | 3 process spawns and 3 connections per batch; peek re-decodes from the slot's position every call |
| psycopg2 `LogicalReplicationConnection` + `pgoutput` + a Python binary decoder | Python (driver in C) | none published; pypgoutput (a prototype, 2022, Pydantic per row) shows the shape | psycopg2 is already a migkit dependency; psycopg 3 has **no** replication protocol support (issue #71). The server moves `confirmed_flush_lsn` only on feedback (`send_feedback(flush_lsn=...)`); psycopg2 issue #940 (2019, wal2json) reported an advance without feedback - verify on the current version before relying on it |
| pglogrepl (jackc) | Go | PeerDB's consumer: ~250,000 inserts/s while the server decoded two large transactions (50 M inserts in 3-4 min, protocol v1) | MIT; supports protocol v2 streaming of in-progress transactions; used by PeerDB and pgstream (pgstream decodes wal2json only) |
| Supabase ETL (formerly pg_replicate) | Rust | no throughput published; README: 26 ms vs 848 ms tail latency and 13 MB vs 1,629 MB against Debezium Server at 200 changes/s | Apache-2.0; API not yet stable (pre-release); PG 14-18 |
| `pgwire-replication`, `pg_walstream` | Rust | none | transport only / transport plus parser (protocol 1-4, rustls) |
| Debezium PostgreSQL | Java | sponsored benchmark: 30,000 ops/s live tuned vs Supermetal (Rust, Arrow) 35,000 ops/s; snapshot 43 vs 123 MB/s average, 4 vCPU | Debezium profiling (July 2025) found a regex per record in `decoderbufs` type parsing: 0.768 -> 0.284 µs per op after caching |

**What it means for migkit:** the first 5-10x on PostgreSQL is not a compiled decoder; it is (a) a persistent driver connection for `advance`/`peek` and (b) streaming `pgoutput` with protocol v2, both in Python. A compiled pgoutput decoder (pglogrepl or Supabase ETL as a sidecar) matters after that, on the same terms as MySQL.

---

## 5. Arrow-native paths for moving rows

| Option | What it gives | Published numbers | Known losses (guards needed) | Packaging |
|---|---|---|---|---|
| ADBC PostgreSQL (`adbc-driver-postgresql`) | `fetch_arrow_table()` via binary COPY; bulk ingest | users: 5-10x psycopg+pandas on large reads; 40 M-row write 1 min 33 s vs 4 min 30 s SQLAlchemy (~430k rows/s) | NUMERIC comes back as text (R18); every read goes through binary COPY (fails on some wire-compatible engines); its own blocking IO | PyPI wheels |
| ADBC MySQL (ADBC Driver Foundry, Go) | Arrow reads from MySQL/MariaDB/TiDB/Vitess | none published | untested against migkit's MySQL traps (zero dates, unsigned, DECIMAL(65), binary vs text) | **not on PyPI**: installed by `dbc`; binaries for macOS arm64 and Linux amd64/arm64 |
| ConnectorX (Rust) | `read_sql(..., return_type="arrow")`, partitioned reads | VLDB 2022: 13x faster and 3x less memory than `pandas.read_sql` (TPC-H lineitem) | drops time zones (R18); whole result in memory unless `arrow_stream` | PyPI wheels |
| psycopg 3 binary COPY | `COPY ... (FORMAT BINARY)` with `write_row` / `read_row` | a user profile of the C build: Python/Cython processing <10% of COPY time; the docs expect binary to be faster in general; one user measured 22 s binary vs 25 s text | server does not cast (an `int` sent as `int2` into `int8` fails) - needs `set_types` from the catalogue | already a dependency (binary wheel) |
| pgpq (Rust, Arrow -> PostgreSQL binary COPY) | encodes an Arrow batch into COPY binary, sans-IO | encodes 1 M rows in under 1 s; encode + binary COPY cheaper than a CSV COPY; columnar encoding 25-50% faster than row-wise | must match the target's column types exactly | PyPI wheels |
| DuckDB `postgres`/`mysql` scanners | vectorised reads into DuckDB/Arrow; migkit already depends on DuckDB | none relevant | DuckDB DECIMAL tops out at width 38 (MySQL allows 65); a `mysql_query` bug once turned DECIMALs into NULL (issue #65, 0.10.2); extensions download at first use | in the base install; extension download at runtime |
| `pyarrow.compute` hashing | no element-wise MD5/xxhash kernel in a release (`hash32`/`hash64` in review, PR #45001) | - | - | - |

**What it means for migkit:** on the table copier the verification does not depend on Python objects any more (range digests run on the servers), so an Arrow row path could be added per table without touching what is verified. It is only safe per column: a table goes by Arrow only where every column's class is on a list proven by a differential test (Arrow path vs Python path, same docker table, same server-side digest), and falls back to Python rows otherwise. The published gains are for reads into dataframes; migkit's copy measured 10.6 µs a row MySQL->PostgreSQL in one process, and the split of that time (PyMySQL's pure-Python row decode vs everything else) has to be measured before an Arrow reader is worth its guards.

---

## 6. Hashing

* **Published (xxHash README, i7-9700K):** XXH3 31.5 GB/s, XXH64 19.4 GB/s, MD5 0.6 GB/s, SHA-1 0.8 GB/s; on small inputs ("small data velocity") XXH3 133.1 vs MD5 7.8. BLAKE3: ~12x SHA-256 single-threaded on x86 with wide SIMD, but on Apple Silicon hardware SHA-256 roughly matches or beats it single-threaded (Bazel's M3 test, BLAKE3 issue #315), and BLAKE3 is slower than the best SHA-256 below ~4 KB.
* **For migkit's rows (~100-300 bytes):** the measured 0.06 s for 200,000 MD5s is 0.3 µs a row. At 0.6 GB/s a 100-byte row is ~0.17 µs of MD5 itself, the rest Python's call, object creation and `hexdigest()` (the benchmark rows must have been short: 300 bytes would be 0.5 µs of MD5 alone). XXH3 from Python would save perhaps 0.1-0.2 µs a row - under 10% of the 2.25 µs fold, and not available on the cross-engine digest anyway.
* **Correctness:** the cross-engine digest is MD5 by construction (both servers compute it in SQL). A different hash is possible only for digests migkit computes on *both* sides itself (e.g., a key-set digest between two in-process folds) and must never be persisted where an old checkpoint could be compared against it (R18: never persist an engine's internal hash - polars' changes by version).
* **In-engine alternative worth knowing:** DuckDB (already a dependency) has `md5()` and `md5_number()`; `md5_number` reads the 16 digest bytes as a little-endian 128-bit integer (`md5_number_upper` is the first 8 bytes little-endian), so reproducing `int(md5hex[:15], 16)` needs a byte swap or a `substr` of `md5()` and a hex cast - doable in SQL, and vectorised, but the canonical *text* would then be rendered by DuckDB: a fourth renderer again.

## 7. JSON

* **orjson** is out for the canonical path: `loads` has no `parse_float` (floats become binary floats, not the decimal written - issue #21, closed), `dumps` does not take `Decimal`, `OPT_SORT_KEYS` sorts by UTF-8 bytes (migkit needs length first), and 3.12.0 changed float output to `1.2e+30` - a byte-exact rendering cannot depend on a formatter that changes between versions. It also does not support PyPy or subinterpreters, and its maintainers accept no public issues.
* **msgspec** can decode untyped floats as `Decimal` without loss (`msgspec.json.Decoder(float_hook=decimal.Decimal)`, or typed `Decimal` fields parsed straight from the bytes, trailing zeros kept) and is ~orjson speed on decode. It could replace the `json.loads(value, parse_float=Decimal, parse_int=int)` half of `canon.json_text`; the rendering half (length-first key order, `json.dumps(str, ensure_ascii=False)` escaping to match `jsonb`) stays Python. Worth it only if JSON columns show up in a profile.
* **Elsewhere:** `canon.sql_value` serialises a `dict`/`list` for the driver with `json.dumps(sort_keys=True, default=str)`; both servers normalise JSON on storage, so orjson could be used there if a profile ever shows it - with the float-format change in mind.

## 8. Compression

* **Published (zstd README, lzbench, i7-9700K, Silesia):** zstd -1 ratio 2.896 at 510 MB/s compress / 1,550 MB/s decompress; `--fast=4` 2.146 at 665 / 2,050; lz4 2.101 at 675 / 3,850; snappy 520 / 1,500; zlib -1 105 / 390.
* **Python bindings:** the stdlib `compression.zstd` (3.14, PEP 784) was *slower* than python-zstandard at decompression on 3.14 (output-buffer reassembly >50% of time); fixed in 3.15 (+25-30% decompression) where it becomes the fastest binding. python-build-standalone builds (what `uv` installs) measured 4-7x slower for the stdlib codec because build scripts overwrote optimisation flags. Small records: a project that moved to the stdlib saw ~30 µs vs ~3 µs per decompress of a 3.3 KB record because it created a new context per call - reuse one decompressor object per stream.
* **For migkit:** keep `zstandard` (already bundled with its own libzstd, tested free-threaded) for the relay, spill files and binlog payloads; compress per batch, not per row; level 1 or a negative level on a fast link, higher where the link is the limit. lz4 is already installed if a profile ever shows zstd -1 compress as the limit on a 10 Gb/s link (unlikely below ~500 MB/s per core).

## 9. I/O concurrency: threads vs asyncio

* **Published:** asyncpg's claim of 5x psycopg 3 on average comes from a June 2023 localhost microbenchmark under uvloop; application-level comparisons in 2024-2026 show ~20-25% or less (12,400 vs 10,100 req/s in one), sometimes none. psycopg 3 with its C build reached ~1.2 M rows/s fetching 1,000-integer rows (Tiger Data, 2024). uvloop's 2-4x is measured on an echo server; a Python 3.13 app saw ~15%; an aiohttp maintainer doubted in 2025 that it is still worth recommending; 0.22.1 (Oct 2025) added 3.14 and free-threading fixes.
* **For migkit:** the I/O waits (round trips to each server) already overlap through threads (drivers release the GIL while waiting) and processes (ranges). The CPU per row is the limit, and asyncio does not add a core. psycopg 3's **pipeline mode** (R19 lever 10) removes round trips without an event loop. An asyncio rewrite is not worth its cost.

## 10. Batch sizes and allocation patterns

* **Batch sizes already chosen by measurement:** the tail grows 1,000 -> 16,000 while behind; the copier reads 50,000 rows / 64 MB; a run of 1,000+ upserts goes COPY-into-a-temporary-table on PostgreSQL. pgpq's author found columnar encoding 25-50% faster mostly from fewer reallocations - the same rule applies to Python lists: build once, at the final size where known.
* **The allocation diet on the apply side (estimate 1.3-2x on apply CPU, to measure):**
  1. Compute a change's identity **once**, as the tuple of key values in the table's key-column order (from the catalogue), and reuse it in `_collapsed`, `_net_rows`, `_apply_net` and `_lanes` instead of rebuilding `tuple(sorted((n, repr(v)) ...))` and `tuple(sorted((n, str(v)) ...))` per step.
  2. Cache the **shape** of a row (sorted key names, sorted value names, the `Added` flag) per `(table, frozenset(names))`; most batches have one or two shapes per table.
  3. Keep `repr` only where equality and identity differ in a way that matters: `Decimal('1.0') == Decimal('1.00')` but their `repr`s differ; for a key column of one declared type the value's own hash is the right identity for `int`, `str`, `bytes`, `datetime`; keep `repr` for `Decimal` and `float` until a property test proves otherwise (`test_a_copy_and_its_changes_interleave_safely.py` is the gate).
  4. Carry changes as tuples with a per-table column list (the compact transport of section 2.4) where they cross a process boundary; convert to dicts only where the applier needs names.
  5. `gc.freeze()` after start-up and a higher first GC threshold for the tail and copier processes; measure the gen-0 collection count per batch before and after.
* **Correctness:** every one of these changes how rows are grouped or ordered, which is exactly what R2's lanes, runs and key moves depend on. The existing Hypothesis interleave test and the lane tests are the acceptance gate; nothing here changes what is written.

---

## 11. Per path: lever, expected gain, correctness risk, packaging

| migkit path | Lever | Expected gain | Correctness risk | Packaging | Recipe |
|---|---|---|---|---|---|
| MySQL tail (decode + apply, one GIL) | same pymysqlreplication decoder in a long-lived child process (the `ranges._Worker` pattern), compact tuple transport | **estimate** up to ~2.3x end to end (15.3 -> ~6.7 µs a change) *if* R-B shows the tail GIL-bound | low: same library and code; transport must keep `ABSENT`, `Added`, `Decimal`, `bytes`, zoned `datetime` identical; read-ahead dropped on token mismatch as today | none | R-A, R-B |
| MySQL tail, apply side | allocation diet (section 10), then mypyc on a `_hot` module if still the limit | **estimate** 1.3-2x on apply CPU; mypyc a further 1.5-4x (published range) | medium: grouping and ordering of rows (lanes, runs, key moves) - gated by the Hypothesis interleave test | none / per-version wheels for mypyc | R-B |
| MySQL tail, decode | PyPy as the child's interpreter | **estimate** 2-4x decode; nothing end to end until apply is faster | low-medium: PyPy's own `decimal`/`datetime`; shadow check | operator-installed PyPy | R-A |
| MySQL tail, decode | compiled sidecar: go-mysql (Go) or `mysql_common` (Rust) emitting framed records | **published ratio** 10-30x on parse; end to end bounded by apply | medium-high: a second decoder of every column type (JSON binary, DECIMAL, TIME fractional, charset, unsigned, `binlog_row_metadata`, compressed payloads, skip_rows positions) - shadow check mandatory | a static binary per platform (Go cross-compiles trivially) or maturin wheels | R-A, R-F |
| PostgreSQL tail | one persistent psycopg connection for advance / current LSN / peek; read `lsn` and `data` as columns, not split text | **estimate** removes ~3 process spawns + 3 connections per batch (tens of ms a batch; ~3-50 µs a change at 16,000-1,000 a batch) | improves it: no line splitting, so newline-bearing values no longer stall | none | R-C |
| PostgreSQL tail | streaming `pgoutput` (protocol v2) through psycopg2's replication connection, Python binary decoder; feedback only for applied positions | **estimate** removes the re-decode per peek and streams large transactions; Python parse cost similar to today's text scan | medium: slot advance must follow applied positions only (psycopg2 #940 to re-check); `pgoutput` omits unchanged TOAST values (a marker, not a NULL) | none (psycopg2 already a dependency) | R-C |
| PostgreSQL tail | compiled pgoutput decoder (pglogrepl sidecar or Supabase ETL's parser) | as MySQL's compiled rung | as MySQL's | as MySQL's | R-C, R-F |
| Table copier (MySQL -> PostgreSQL 10.6 µs a row) | Arrow reader per table where every column class is proven (ADBC PG; ADBC MySQL via `dbc`; ConnectorX) + binary COPY or pgpq on write | **published** reads 5-13x vs pandas-style paths; migkit's own split unknown | medium: per-column losses (NUMERIC as text, time zones, DECIMAL > 38, zero dates, unsigned) - whitelist per class; verification unchanged (server digests) | ADBC PG and ConnectorX on PyPI; ADBC MySQL not on PyPI | R-D |
| In-process fold (`fold_rows`) | render specialised per column (closures chosen once), then a native `fold_rows` (PyO3) with Python fallback for float/json | **estimate** 1.3-2x from specialising; 4-7x native | high for native (a fourth renderer); low for specialising | none / maturin wheels + cp314t | R-E |
| JSON columns in the fold | msgspec `float_hook=Decimal` for the parse half of `json_text` | **published** decode ~orjson speed; migkit gain unknown | low if the rendering half stays Python | PyPI wheels | R-E |
| Relay / spill / payload compression | stay on `zstandard`, reuse contexts, compress per batch | already near codec speed | none | none | - |

---

## 12. Measurement recipes (local docker only, one test at a time)

All under `python tools/with_docker_lock.py ...` so parallel checkouts do not collide, against throwaway containers only (the owner's rule: no real database). Profiles with `py-spy` (`record` for flame graphs, `top --gil` for the share of time holding the GIL); each run records the Python build (`python -VV`), the CPU, and the library versions printed in section 1.

* **R-A. Decode-only rate, per interpreter.** `mysql:8.4` with `--binlog-row-metadata=FULL --binlog-row-image=FULL`; write 300,000 row changes across the bench shapes (keyed, wide, lob, json) with `bench/run.py --engine mysql --rows ... --cdc-rate ...` or a seed script; then a script that calls `neutral_changes(limit=16000)` from the start token until caught up, discarding the changes. Report changes/s and CPU seconds per change. Run it under CPython 3.12 (today), 3.13, 3.14, 3.14t (checking `sys._is_gil_enabled()`), and PyPy 3.11 (the decoder script only). The same binlog window, decoded by each, must produce the same canonical change stream: hash `rowtext.encode` of every change's rendered key and values and compare the per-batch MD5 sums.
* **R-B. Is the tail GIL-bound?** Replay R2.6's backlog (320,000 changes, MySQL -> PostgreSQL): while the tail applies, `py-spy top --gil --pid <tail>` and the process's CPU (`ps -o %cpu`). ~100% of one core with decode and apply frames alternating under the GIL means GIL-bound. Then split it: record the decoded batches once (pickle), and time **apply only** from the recording. If decode-only (R-A) + apply-only ≈ the end-to-end time, the tail is serial on the GIL and the child-process decoder pays; if apply-only ≈ end to end, the target is the limit and only the apply levers pay.
* **R-C. PostgreSQL tail split.** `postgres:16 -c wal_level=logical`, a slot on `test_decoding`, 300,000 changes queued. Time separately: 100 `psql -c 'select 1'` spawns (spawn + connect); `select count(*) from pg_logical_slot_peek_changes(...)` from one persistent connection (server decode only); `pgslot.parse_line` + `pgslot.change` over the captured text (Python only). Then streaming: a `pgoutput` slot with a publication, `pg_recvlogical --start -o proto_version=2 -o publication_names=p -f - > /dev/null` for the server's streaming decode ceiling, and a psycopg2 `LogicalReplicationConnection` loop with a minimal binary decoder for the Python side. Add one change whose text holds `\n` and one holding U+2028 to confirm or refute section 1.2's stall.
* **R-D. Copier split.** `bench/run.py --engine mysql --rows 1000000` with `py-spy record` on one range worker: shares of PyMySQL row parsing, `list(r)`, `canon.sql_rows`, COPY `write_row`, and waiting on the read-back digest. Then, on the same table, time `adbc_driver_postgresql` `fetch_arrow_table()` and psycopg binary `COPY TO` into `/dev/null` for the PostgreSQL side, and ADBC MySQL / ConnectorX for the MySQL side, each followed by the server-side range digest on the target to prove equality.
* **R-E. Fold microbenchmark by class mix.** 200,000 generated rows per class mix (integers only; text; decimal(30,10); timestamptz; json; float) through `canon.fold_rows` with `timeit`; then the specialised-renderer variant; then (if built) the native one. Every variant's total must equal the Python one's on the same rows, and the SQL digests on MySQL and PostgreSQL containers holding the same rows.
* **R-F. Shadow check of a compiled decoder.** Point the candidate (a go-mysql or pglogrepl program emitting records) at the same window as R-A; compare batch by batch the canonical stream hash; report the first differing change, not a count. A candidate that differs on any change is not a rung.

---

## 13. The seam: choosing "compiled decoder" or "Python decoder" per source by measured rate

The decision engine in progress (wave 1, `decide.py` in the decision-engine agent's worktree, not merged) already has the shape this needs: a **rung** gives capabilities, needs facts, is **proved** before it is trusted, and is **costed** by a number measured per unit of work (`Costs.saw(name, units, seconds)`); the climb ranks by measured cost, a never-timed rung is tried once, footprint only breaks ties, and the chosen rung is remembered beside the tail's token so a restart climbs the same one.

**The part:** `read changes`, with rungs in this default order (the order the code's own measurements give):

| Rung | Gives | Needs | Proof | Footprint |
|---|---|---|---|---|
| `python-thread` (today's `_ReadAhead`) | exact positions (transaction ends, `skip_rows`), online | the engine's `neutral_changes` | none (the reference) | 0 |
| `python-process` | the same | `READS_AHEAD` (MySQL); pickle round trip of the value kinds | shadow: the first N batches decoded by both, canonical hashes equal | 1 (a child process) |
| `pypy-process` | the same | a `pypy3` on PATH that imports pymysqlreplication | shadow as above | 1 |
| `compiled-sidecar` (per engine: go-mysql / pglogrepl or a Rust parser) | the same, if it reports positions at transaction ends | the binary for this OS/arch; the source settings it relies on (`binlog_row_metadata=FULL`, a `pgoutput` publication) | shadow as above, on this source's own window | 2 (a binary, a second connection) |

**The contract every rung implements** (what `neutral_changes` already is): `read(token, limit) -> (changes, token)` where changes are `canon.change` records (or the compact tuples, turned into them at the applier), the token only ever lands where a source transaction ended (with `skip_rows` inside a large one), and a read ahead is used only if the token it started from is the one asked for. A rung's error ends that rung for the run: the tail goes back to the saved token on the rung below, which the idempotent applier makes safe.

**The cost that ranks them is not decode speed alone.** Measured per calibration window (the first few batches, or a timed window at tail start):
* `decode_cpu`: CPU seconds per change spent decoding, in whichever process does it;
* `applier_cpu`: CPU seconds per change in the applier's process (this is what a GIL-shared decode inflates);
* `wall`: seconds per change end to end;
* `source_rate`: changes per second the source is writing (binlog bytes or LSN advance per second, converted by the window's bytes per change).

Rank by `wall`; when two rungs tie (the decision engine's two-significant-figure rule), the smaller footprint wins - so a compiled sidecar is chosen only where it is measurably faster *on this source*, not because it exists. Re-climb when the lag grows for several minutes (the tail is behind a source faster than the chosen rung) and when the tail restarts on a different machine. The run's report names the rung and the numbers that chose it, in words ("changes are read in a process of their own: 2.1x faster on this source than beside the writer").

**The same seam on the copier** is the `read rows` part per table: `python-rows` (today) and `arrow` (a reader returning Arrow), where `arrow`'s `needs` are the column-class whitelist and its proof is one range copied both ways with equal server digests.

---

## 14. Ranked plan

Measure first - every lever below is conditional on a number.

| # | What | Why first | Effort |
|---|---|---|---|
| 1 | **R-B: is the MySQL tail GIL-bound?** plus R-A decode-only on CPython 3.12 | decides whether the cheapest lever (a child process) pays ~2x or nothing; R2.6's "the reader already runs beside it" is true for I/O, not for CPU | S |
| 2 | **R-C: PostgreSQL tail split**, including the newline case | the PG tail has no rate on record; spawns and re-decodes are likely the ceiling; the newline stall is a correctness question | S |
| 3 | **PostgreSQL tail on one persistent connection**, `lsn`/`data` read as columns | removes spawns and connections per batch and the line-splitting failure; no new dependency | S |
| 4 | **Decoder in a child process** (`python-process` rung) for MySQL, compact transport, value-kind round-trip test, shadow check | biggest expected end-to-end gain for the least new trust: same decoder code | M |
| 5 | **Apply-side allocation diet** (identity once, cached shapes, `repr` only where needed, `gc` settings) | the applier's ~6.7 µs a change becomes the ceiling after #4 | S-M |
| 6 | **R-D: copier split**, then an `arrow` rung for the classes that prove equal | the copier is 10.6 µs a row single-process; only worth guards if PyMySQL row decode dominates | M |
| 7 | **Streaming `pgoutput` (protocol v2) via psycopg2**, feedback after apply | removes per-peek re-decode, streams large transactions, and is the socket a compiled PG decoder plugs into | M |
| 8 | **R-E and a specialised Python renderer**; native `fold_rows` only if the fold shows on a hot path | four-renderer risk only where it buys time | S (specialise) / M-L (native + wheels) |
| 9 | **`pypy-process` rung** (optional, operator-installed) | free if #4 exists; measure with R-A | S |
| 10 | **`compiled-sidecar` rung** (go-mysql first, then pglogrepl), behind the shadow check | pays only once #4, #5 and #7 move the ceiling back to decoding | L |
| - | Not pursued now: free-threaded 3.14t (psycopg), subinterpreters (extension support), PyPy for all of migkit, asyncio/uvloop/asyncpg, orjson in the canonical path, XXH3/BLAKE3 on the cross-engine digest | reasons in sections 2, 6, 7, 9 | - |

What this changes in the backlog: R2.6 and R19 lever 9 say "decoding moves out of process only once the apply passes the reader"; under one GIL the two are not side by side, so the process move (#4) is worth measuring now, and the *compiled* decoder is the step that waits.

---

## 15. Sources

migkit (read, not changed): `migkit/engines/hetero.py` (`_neutral_move`, `_write_checked`, `_batch_digest`, `_move_in_ranges`, `_mapped_change`, `tail_apply`, `_ReadAhead`), `migkit/engines/base.py` (`_collapsed`, `_net_rows`, `_lanes`, `_apply_net`, `neutral_batches`), `migkit/pgslot.py`, `migkit/engines/postgres.py` (`_psql`, `neutral_changes`, `neutral_write`, `_copy_text`), `migkit/engines/mysql.py` (`neutral_changes`, `_row_events`, `binlog_names`, `neutral_read`, `neutral_write`), `migkit/canon.py`, `migkit/rowtext.py`, `migkit/ranges.py`, `migkit/sizing.py`, `migkit/avrostream.py`, `pyproject.toml`, `docs/backlog.md` (R2, R18, R19), `docs/research/security-throughput-scorecard-2026-09-28.md`, the decision-engine work in progress (`decide.py` in the wave-1 worktree), `tests/test_pgslot_live.py`.

Runtimes
- Free-threading HOWTO (3.14): https://docs.python.org/3/howto/free-threading-python.html
- PEP 779: https://peps.python.org/pep-0779/ ; PEP 803 (abi3t): https://peps.python.org/pep-0803/
- Compatibility tracking: https://py-free-threading.github.io/tracking/ ; wheels tracker: https://hugovk.dev/free-threaded-wheels/ ; Quansight, halfway: https://labs.quansight.org/blog/free-threaded-python-halfway
- psycopg free-threading: https://github.com/psycopg/psycopg/issues/1095 ; psycopg2: https://github.com/psycopg/psycopg2/issues/1810
- pymongo changelog: https://pymongo.readthedocs.io/en/stable/changelog.html ; python-zstandard news: https://python-zstandard.readthedocs.io/en/latest/news.html ; cryptography 46.0.0: https://cryptography.io/en/46.0.0/changelog/
- Miguel Grinberg on 3.14: https://blog.miguelgrinberg.com/post/python-3-14-is-here-how-fast-is-it
- PEP 734: https://peps.python.org/pep-0734/ ; `concurrent.interpreters`: https://docs.python.org/3/library/concurrent.interpreters.html ; LWN: https://lwn.net/Articles/985041/ ; start-up and CPU benchmark: https://github.com/nazmul284/python-subinterpreters-vs-processes ; conda experiment: https://github.com/conda/conda/issues/15481 ; PyO3 sub-interpreters: https://github.com/PyO3/pyo3/issues/3451
- Tail-call interpreter: https://blog.nelhage.com/post/cpython-tail-call/ ; https://fidget-spinner.github.io/posts/apology-tail-call.html ; JIT plan PEP 836: https://peps.python.org/pep-0836/
- Incremental GC revert: https://discuss.python.org/t/reverting-the-incremental-gc-in-python-3-14-and-3-15/107014 ; GC thresholds case: https://mkennedy.codes/posts/python-gc-settings-change-this-and-make-your-app-go-20pc-faster/
- PyPy 8.0.0: https://pypy.org/posts/2026/09/pypy-v800-release.html ; new runner: https://pypy.org/posts/2026/06/benchmarker2-for-pypy.html ; uv warning: https://github.com/astral-sh/uv/pull/17643 ; psycopg on PyPy: https://www.psycopg.org/psycopg3/docs/basic/install.html
- Arrow IPC: https://arrow.apache.org/docs/python/ipc.html ; PEP 574: https://peps.python.org/pep-0574/

Compiled extensions
- Pydantic v2: https://pydantic.dev/articles/pydantic-v2-alpha ; orjson: https://github.com/ijl/orjson and https://github.com/ijl/orjson/blob/master/CHANGELOG.md ; cramjam: https://github.com/milesgranger/cramjam
- maturin-action: https://github.com/PyO3/maturin-action ; PyO3 distribution: https://pyo3.rs/v0.29.2/building-and-distribution.html
- Cython free-threading: https://cython.readthedocs.io/en/latest/src/userguide/freethreading.html ; pure mode: https://cython.readthedocs.io/en/stable/src/tutorial/pure.html
- mypyc: https://mypyc.readthedocs.io/en/latest/introduction.html ; Black with mypyc: https://ichard26.github.io/blog/2022/05/compiling-black-with-mypyc-part-2/

Decoders
- python-mysql-replication profile: https://github.com/julien-duponchelle/python-mysql-replication/issues/251
- binlog2sql_go: https://github.com/354441703/binlog2sql_go ; my2sql vs binlog2sql (ActionTech): https://opensource.actionsky.com/20220829-binlog/
- go-mysql: https://github.com/go-mysql-org/go-mysql
- Debezium MySQL (Feb 2026): https://debezium.io/blog/2026/02/02/measuring-debezium-server-performance-mysql-streaming/ ; Debezium profiling (Jul 2025): https://debezium.io/blog/2025/07/07/quick-perf-check/ ; RisingWave tuning figures: https://risingwave.com/blog/debezium-performance-tuning-throughput-latency/
- Rust binlog parsers: https://docs.rs/mysql_common/latest/mysql_common/binlog/index.html ; https://crates.io/crates/mysql_binlog ; https://crates.io/crates/mysql-binlog-connector-rust
- psycopg 3 replication: https://github.com/psycopg/psycopg/issues/71 ; psycopg2 replication: https://www.psycopg.org/docs/extras.html ; https://github.com/psycopg/psycopg2/issues/940 ; pypgoutput: https://pypi.org/project/pypgoutput/
- pglogrepl: https://github.com/jackc/pglogrepl ; PeerDB on protocol versions: https://blog.peerdb.io/exploring-versions-of-the-postgres-logical-replication-protocol ; pgstream: https://github.com/xataio/pgstream
- Supabase ETL: https://github.com/supabase/etl ; pgwire-replication: https://github.com/vnvo/pgwire-replication ; pg_walstream: https://docs.rs/pg_walstream/latest/pg_walstream/
- Supermetal-sponsored CDC benchmark: https://supermetal.io/blog/cdc-benchmark-supermetal-debezium-flink

Arrow, hashing, JSON, compression, I/O
- ADBC PostgreSQL: https://arrow.apache.org/adbc/current/driver/postgresql.html ; benchmarks wanted: https://github.com/apache/arrow-adbc/issues/568 ; binary-to-Arrow for other drivers: https://github.com/apache/arrow-adbc/issues/3201 ; ADBC MySQL: https://github.com/adbc-drivers/mysql ; Driver Foundry: https://adbc-drivers.org/
- ConnectorX: https://github.com/sfu-db/connector-x ; paper: https://www.vldb.org/pvldb/vol15/p2994-wang.pdf
- psycopg COPY: https://www.psycopg.org/psycopg3/docs/basic/copy.html ; profile of binary COPY: https://github.com/psycopg/psycopg/discussions/1147 ; text vs binary: https://github.com/psycopg/psycopg/discussions/285
- pgpq: https://github.com/adriangb/pgpq
- DuckDB numeric limits: https://duckdb.org/docs/lts/sql/data_types/numeric ; DuckDB MySQL DECIMAL bug: https://github.com/duckdb/duckdb-mysql/issues/65 ; `md5_number`: https://duckdb.org/docs/stable/sql/functions/text
- Arrow `hash32`/`hash64`: https://github.com/apache/arrow/pull/45001
- xxHash: https://github.com/Cyan4973/xxHash ; BLAKE3 on Apple Silicon (Bazel): https://github.com/bazelbuild/bazel/discussions/22011
- orjson Decimal: https://github.com/ijl/orjson/issues/21 ; msgspec: https://msgspec.dev/api
- zstd: https://github.com/facebook/zstd ; PEP 784: https://peps.python.org/pep-0784/ ; stdlib decompression fix: https://emmatyping.dev/decompression-is-up-to-30-faster-in-cpython-315.html ; build flags: https://github.com/astral-sh/python-build-standalone/issues/761
- asyncpg: https://github.com/MagicStack/asyncpg ; psycopg2 vs 3 (Tiger Data): https://www.tigerdata.com/blog/psycopg2-vs-psycopg3-performance-benchmark ; uvloop: https://github.com/MagicStack/uvloop ; aiohttp on uvloop: https://github.com/aio-libs/aiohttp/discussions/10494

Limits of this research: no published parse-only rate exists for go-mysql, mysql-binlog-connector-java or Supabase ETL; the cross-language ratios come from tools (binlog2sql vs its Go rewrites) that do more than parse. Every "estimate" above is my arithmetic from migkit's own measurements and must be measured (section 12) before it is claimed.
