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

- **pgcopydb is installed and the plan did not use it.** For this hop migkit
  chose `pg_dump -Fd -j2` piped to `pg_restore -j2`. pgcopydb exists
  precisely because that pair cannot stream between two running servers, and
  it adds the snapshot-sharing and table-splitting described in
  [what a migration actually costs](what-a-migration-actually-costs.md).
  Whether the chooser should prefer it, and by how much it wins here, is the
  next measurement.
- **Nothing above involves LOBs**, which the research says is where full
  loads actually die. A bytea/large-object table is the next thing to add to
  `bench_rows`.
- **No comparison against another tool** on this hardware yet. Until that
  exists migkit makes no claim about being faster or slower than anything.
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
