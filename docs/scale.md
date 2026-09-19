# What migkit does at scale, measured

Numbers, with the hardware beside them. Everything here was produced by
`bench/seed.py` against two PostgreSQL 16 containers and migkit's own
commands - no synthetic timings, no extrapolation.

## The machine

| | |
|---|---|
| Host | Apple M5, 10 cores, 16 GB |
| Container runtime | colima, **2 vCPU, 4 GiB, 20 GiB disk** |
| Databases | PostgreSQL 16.15, both in that one VM, `shared_buffers=256MB` |
| migkit | run on the host, talking to both over TCP |

Both databases share two cores and four gigabytes. That is the number to
keep in mind when reading the rest: this is a laptop VM, not a server.

## The table

`bench/seed.py` builds `bench_rows`: a bigint key, three short text columns
(name, email, city), a paragraph of free text, a tag, `numeric(14,4)`, an
integer, a boolean, a `timestamptz` and a `jsonb` payload. Every tenth row
has a NULL city and an empty note, because a table where every column is
populated is not a table anybody has. Text comes from a Faker-generated pool
of 20,000 values per column (2,000 for the paragraphs, 200 tags), so the
columns have realistic shape and cardinality without paying Faker's
per-row cost inside the measurement.

## Results

| | 1,000,000 rows | 10,000,000 rows |
|---|---|---|
| On disk | 278 MB | 2,777 MB |
| Seed (COPY from Python) | 6.9 s | 55.9 s |
| `migkit move --mode full --go` | **5.8 s** | **57.9 s** |
| move, migkit's own peak RSS | 30 MB | **30 MB** |
| `migkit check` (31 checks, all green) | **9.8 s** | **62.4 s** |
| check, peak RSS | 328 MB | **333 MB** |

### Large objects, and the two movers

The same measurements again on `bench_lobs`: 50,000 rows whose payload is a
`bytea` averaging 40 KB of **random** bytes (compressible filler would
measure the compressor, not the move) plus a TOAST-able text column -
2,093 MB, of which 2,051 MB is TOAST.

| 2,093 MB of LOB | wall | CPU on the host | rows |
|---|---|---|---|
| `migkit move` (`pg_dump -Fd -j2` \| `pg_restore -j2`) | 93.6 s | **82.9 s user** | 50,000 |
| `pgcopydb copy table-data --table-jobs 2` | **18.2 s** | 0.35 s user | 50,000 |

And on a plain table, 2,000,000 rows / 555 MB:

| 555 MB, no LOB | wall | CPU on the host |
|---|---|---|
| `migkit move` (dump/restore) | 11.7 s | 7.6 s user |
| `pgcopydb copy table-data` | **4.1 s** | 0.15 s user |

**pgcopydb is 2.9x faster on the plain table and 5.2x on the LOB table**,
and the CPU numbers say why: `pg_dump -Fd` compresses by default, so the
dump path spends 83 of its 94 seconds gzipping bytes that were random to
begin with. pgcopydb streams COPY between the two servers and spends
almost no host CPU at all.

LOBs cost about half the throughput per byte even on the fast path: 48 MB/s
for the plain table against 22 MB/s for the LOB table through dump/restore.

**Why migkit was not using it.** `movers.pgcopydb_available()` requires the
`dimitri/pgcopydb` container image rather than a local binary, because
Homebrew's pgcopydb 0.18 is compiled against PostgreSQL 18 and emits `SET
transaction_timeout = 0`, which PostgreSQL 16 has never heard of. That was
measured to make a whole-database `clone` move zero rows while reporting
each rejection separately.

Re-measured here, the rejection is real and the outcome is not: the target's
log shows `ERROR: unrecognized configuration parameter
"transaction_timeout"` during the pgcopydb run, and all 50,000 and then
2,000,000 rows arrived anyway. The difference is the subcommand - `copy
table-data` survives the failed SET where `clone` does not. That is a
narrower constraint than the current availability check enforces, and
worth 3-5x.

### What the numbers say

**Throughput is flat.** 172,000 rows/s at one million, 173,000 rows/s at ten
million - about 48 MB/s, on two shared cores with the source and target
competing for them. The move path here is `pg_dump -Fd -j2 | pg_restore -j2`;
see the note below.

**Memory does not follow the data.** Ten times the rows moved migkit's peak
RSS not at all (30 MB both times), and the verify's by 1.5% (328 → 333 MB).
That is the whole point of computing the digest inside each server: the rows
never travel to the client, so the client's memory is a function of the
schema, not of the table. A verifier that pulled both sides into a middle box
would have needed gigabytes here.

**Verify costs about 5.8 seconds per million rows**, plus roughly 4 seconds
of fixed work (schema dump comparison, atlas, liquibase, parameters). Solving
the two data points: 1M = 9.8 s and 10M = 62.4 s gives 5.84 s per million and
3.96 s fixed.

**Finding *which* rows differ is the expensive part.** With drift planted in
the ten-million-row table (100 rows changed, 40 deleted, 25 added), the
data check took 128 s and peak RSS rose to 814 MB, because the drilldown
reads keys rather than folding them into a number. That is the known cost of
naming rows instead of only counting them, and it is bounded by
`DRILL_CAP`, but 814 MB is worth watching: it is the one number here that
moved with the data.

## What the benchmark found

Running at ten million rows found a bug that no test in the suite could
have, because every table in the suite is small:

> `counts postgres: DIFF public.bench_rows src=2000000 dst=1999992`

about a table holding 10,000,000 rows on the source and 9,999,985 on the
target. A table large enough to be split into ranges reports, when one range
differs, that range's row count as the table's. `migkit check --only counts`
on its own was right; the number went wrong only when the counts rode along
with the checksum pass, which is the default.

Fixed, with `tests/test_chunked_counts_pg.py` forcing the chunk size down so
the same shape reproduces on 400 rows in under a second. The test was
confirmed to fail without the fix (it reported 100 for a 400-row table).

## Notes and open questions

- **The chooser now takes pgcopydb from a local binary, with a guard.**
  Measured end to end afterwards: the same 555 MB / 2,000,000-row move went
  from 11.7 s to **5.4 s** through `migkit move --go`, truncate, analyze and
  guard included. What made that safe rather than hopeful is
  `Engine.moved_nothing`: after any mover reports success, the tables the
  source has rows in must have rows on the target, or the command refuses to
  say it finished. Two things the local binary needed that the container had
  been hiding: its own `--dir` per run (pgcopydb keeps the exported snapshot
  under `/tmp/pgcopydb` by default, and the second run died on the first
  one's snapshot), and no `--dir` on `ping`, which answers with its usage
  and reads as a connectivity failure.
- **LOB numbers are above**, and the shape of the cost is clear: half the
  throughput per byte, and on the dump path most of the wall clock is host
  CPU spent compressing incompressible bytes.
- **Still no comparison against a managed service** (DMS, DTS) on the same
  hardware. Until that exists migkit makes no claim about being faster than
  anything except the two movers measured here.
- The 20 GiB VM disk is the practical ceiling for this harness: ten million
  rows on both sides plus WAL used about 12 GB.

## Reproducing

```
docker run -d --name bench-src -e POSTGRES_PASSWORD=test -p 15510:5432 \
  postgres:16 -c shared_buffers=256MB -c max_wal_size=4GB
docker run -d --name bench-dst -e POSTGRES_PASSWORD=test -p 15511:5432 \
  postgres:16 -c shared_buffers=256MB -c max_wal_size=4GB

python bench/seed.py --dsn postgresql://postgres:test@127.0.0.1:15510/postgres \
  --rows 10000000

MIGKIT_CONF=/path/to/bench-hops.yaml migkit move bench --mode full --go
MIGKIT_CONF=/path/to/bench-hops.yaml migkit check bench
```
