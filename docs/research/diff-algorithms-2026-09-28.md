# Exact and fast table comparison: digests, trees, sketches, bisection (2026-09-28)

> **Status: COMPLETE (2026-09-28)** - sections 1-9 written. Research only:
> nothing here is measured yet (section 9.3 lists what to measure). Public
> sources only; no company data. The arithmetic claims in 2.1 and 2.4 are
> this document's own reasoning, marked as such.

Question from the owner: *validate the most exact and the fastest* - prove two
databases (same engine or not) hold the same rows, and find every difference,
reading and moving as little as possible.

---

## 1. What migkit does today (read from the code, 2026-09-28)

| Where | Row hash | Fold | Bits kept | Exact sum? | Ships on DIFF |
|---|---|---|---|---|---|
| `engines/postgres.py` `_row_hash_expr` / `_data_fast_native` | `md5(ROW(cols sorted by name)::text)` (or `jsonb_build_object`), PostGIS as EWKT | `count(*)` + `sum(('x'||substr(md5,1,16))::bit(64)::bigint::numeric)` + same over the PK (`_key_hash_expr`) | 64 (signed) | yes, `numeric` (no wrap) | chunk digests only (restartable ranges on an int key, `checkpoint.plan_ranges`) |
| `postgres.py` `_column_fingerprint` | md5 of one column | one `sum` per column in one scan | 64 | yes | one number per column |
| `postgres.py` `_drilldown_native` | `pk || md5(row)` per row | none - every (pk, 128-bit hash) crosses the wire; int-PK slices of `hop.slice` rows | 128 | - | **O(rows)** |
| `engines/mysql.py` `_checksum` / `_summed` | `crc32(expr)` and `md5(expr)` | `count` + `sum(crc32)` + `sum(conv(substr(md5,1,8),16,10))` (+ key) | 32 + 32 | yes, `DECIMAL` (was BIT_XOR - fixed 2026-09-27) | per range, then `_drilldown` ships (pk, md5) per row |
| `engines/mssql.py` `check_data` | `HASHBYTES('SHA2_256', (SELECT t.* FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER))` | `sum(cast(convert(bigint, substring(h,1,7)) as decimal(38,0)))` | 56 | yes | `_drilldown` ships (pk, sha256 hex) per row, capped 2,000,000, `WITH (NOLOCK)` |
| `engines/mongodb.py` `check_data` | `dbHash` (server md5 over each collection) | whole collection | 128 | - | `_drilldown`: per `_id` range, `$toHashedIndexKey: $$ROOT` (64-bit) per doc |
| `mongodb.py` `neutral_digest` (cross-engine) | canonical text, md5 in Python | `canon.digest_step` | 60 | yes (Python int) | **whole collection crosses the network** |
| `canon.py` `digest_expr` (cross-engine SQL) | md5 of the `rowtext` canonical encoding | `sum(prefix of md5)` | 60 (one term fits int64; sum as numeric / decimal(65,0)) | yes | - |
| `engines/hetero.py` `_bisect` | `neutral_digest` of `[lo,hi)` | binary halving (factor 2), leaf `BISECT_LEAF = 2000` rows, single-int keys only | 60 | yes | leaves walked row by row (`_localise`) |
| `base.py` `fenced_recheck` | `_compare_pks` on the suspect keys | - | - | - | two rounds: read the source position, `fence_wait`, re-compare the suspects |

Observations that matter for the rest of this document:

1. **The fold is already the right family.** Every engine folds with an
   *integer sum without wrap-around* plus a row count. Section 2 shows that
   this is a multiset hash (duplicates counted, no XOR cancellation) whose
   miss probability is at most 2^-k per compared pair for the k bits kept.
2. **Sums are additive across ranges** (`one_chunked` already relies on it).
   So every level of a Merkle-style tree can come out of **one**
   `GROUP BY bucket` scan (section 3.4). `hetero._bisect` instead re-scans
   each half at each level: with d scattered differences it reads the table
   about log2(d)+2 times per side, one query after another.
3. **Drilldown is O(rows) in bytes**: each engine ships one (key, hash) per
   row of the differing range, into an in-memory dict; the caps (20,000 /
   2,000,000) exist because of that. Section 4 (set reconciliation) is the
   family whose bytes scale with the *number of differences*.
4. **Two exactness defects found while reading** (details in 2.5 and 6.1):
   - **MongoDB drilldown can pass a changed document.** `$toHashedIndexKey`
     uses the hashed-index hasher, which converts every number to int64
     before hashing - 2.3 and 2.9 hash the same, and int/long/double of the
     same integral value hash the same. When `dbHash` differs, the drilldown
     can find "no difference" and the collection is reported `ok`.
   - **MySQL's `sum(crc32)` lane is weaker than 32 bits** against value
     swaps between rows: CRC is affine over GF(2), so a swap cancels in XOR
     exactly and in an integer sum with probability about 2^-16. The md5 lane
     beside it still carries 32 bits, so the fold is ~2^-48 for swaps, not
     2^-64. Widening the md5 lane fixes it at no cost.
5. `backlog.md` R18 already records "XOR is not a digest", the XXH3
   measurement (rendering dominates: 0.36 s of 0.45 s for 200k rows) and R18.4
   "no out-of-core key diff"; R19 lever 4 is "verify as it lands". The
   design in section 9 builds on all four.

---

## 2. Order-independent row-set digests

A table is a **multiset** of rows (a key-less table can hold a row twice), so
the digest must be a function of the multiset, computable as an aggregate in
any order, in parallel, and mergeable across ranges.

### 2.1 The families

| Fold | Definition | Duplicates | Accidental miss (random-oracle row hash, k bits) | Adversarial | In SQL? |
|---|---|---|---|---|---|
| XOR (`BIT_XOR`, `CHECKSUM_AGG`, `groupBitXor`) | xor of h(row) | **cancel**: any row present an even number of times vanishes; two equal changes to two rows offset | 1 for even-multiplicity differences; else 2^-k | trivial (linear algebra over GF(2)) | everywhere (PG 14+ native) |
| Sum mod 2^k (ClickHouse `sum(UInt64)`, 64-bit accumulators) | Σ h mod 2^k | counted, but multiplicity m with 2^v dividing m loses v bits | ≤ 2^(v-k) | Wagner k-tree, subexponential | everywhere |
| **Sum over the integers** (migkit today: numeric / DECIMAL / Python int) | Σ h exactly, plus count | counted exactly | **≤ 2^-k per compared pair, any sizes, any multiplicities** | weak (lattice short-vector) - irrelevant for migration | everywhere with an exact decimal sum |
| MSet-Add-Hash (Clarke et al. 2003) | keyed PRF, Σ mod large n | counted | ≤ 2^-k | set-collision resistant only | yes if the key is a salt |
| MSet-Mu-Hash / MuHASH (Bellare-Micciancio 1997) | Π H(x) in GF(p)* | counted | ≈ 1/p | multiset-collision resistant (discrete log) | **no**: SQL has no product aggregate mod p |
| LtHash (Bellare-Micciancio; Lewi et al., Facebook 2019) | vector of 1024 16-bit lanes, lane-wise add mod 2^16 | counted; one element added 2^16 times wraps to zero | negligible | ~200-bit security (lthash16) | would need 1024 aggregates; impractical |
| ECMH (Maitin-Shepard et al. 2016) | sum of curve points | counted | negligible | yes | no |
| Ordered hash (`md5(string_agg(... order by pk))`) | hash of the sorted concatenation | counted | 2^-128 | yes | yes, but needs a sort and no parallel merge |

Why the integer sum is safe for multisets (the argument, not a citation):
let A != B and d(x) = mult_A(x) - mult_B(x), non-zero for some x0. The
digests are equal iff the counts agree and Σ d(x)·h(x) = 0. Fix every h(x)
except h(x0); equality then needs h(x0) = one specific value, which a random
k-bit h hits with probability at most 2^-k. Nothing in that depends on the
row count or on how many copies of a row there are - which is why migkit's
reason for dropping XOR (R18, 2026-09-27) was right, and why reladiff's
belief that "limiting the summed up batches to 16K rows helps keep the risk
of collision low" does not apply to a wrap-free sum (it applies to overflow,
not collisions).

Adversarial resistance (LtHash, MuHash, ECMH) buys protection against someone
*crafting* colliding rows. In a migration the threat is bugs, not a forger,
and the only crafted-collision risk is structural (2.4). None of the
cryptographic multiset hashes can be computed inside SQL at a useful speed,
so they are not candidates for the in-server fold.

### 2.2 What k to keep

* One compared pair misses with probability ≤ 2^-k. With k = 64 and 10^6
  table-or-range comparisons in a lifetime of runs: ≤ 5·10^-14.
* A **second independent lane** (a different slice of the same md5, or a
  different function) makes it 2^-(k1+k2) for the cost of one more `sum` -
  but only if the lanes are independent (2.4 shows crc32 is not independent
  enough of the data's structure).
* **Salt per run** (hash `salt || row` with a fresh salt each run): a
  collision that happened in one run is independent of the next, so two runs
  compound to 2^-2k. Without a salt, a colliding pair of rows collides in
  every run forever. Cheap: a constant string prefix.

### 2.3 The count and the key lanes

A digest is (count, Σh(row), Σh(key)). The count is free and turns
"multiplicity lost" into a certainty rather than a probability; the key lane
tells *what kind* of difference it is before any drilldown
(`verdict.difference_kind`), and costs one short hash per row.

### 2.4 Structured hashes: CRC32 is not a random oracle

CRC is affine over GF(2): for equal-length inputs, crc(x) xor crc(y) =
L(x xor y). Consequences:

* **XOR of CRC32** (pt-table-checksum's and sync-diff-inspector's
  `BIT_XOR(CAST(CRC32(CONCAT_WS(...)) AS UNSIGNED))`): swapping a value
  between two equal-length rows, (1,'a'),(2,'b') -> (1,'b'),(2,'a'), leaves
  the fold **unchanged, always**, because (1|a) xor (2|b) = (1|b) xor (2|a).
* **Integer sum of CRC32** (migkit MySQL): the xor of the two sides is equal,
  so the sums are equal iff the AND-parts agree, which happens with
  probability 2^-(number of zero bits in the xor) ≈ 2^-16 on average - half
  the nominal bits.
* Percona's own advice is that CRC32 "provides no security" and md5 a better
  level of integrity; sync-diff-inspector's maintainers measured CRC32 at
  327.6 MB/s against md5's 282.6 MB/s on 2 TB (a 16% difference), and kept
  CRC32 for speed.

md5, sha-2, xxh3, cityHash, SipHash have no such linear structure for the
purposes here. (md5's known weakness is crafted collisions; irrelevant.)

### 2.5 Hashes that normalise values - the exactness trap

A row hash is only as exact as the bytes it is given. Two server-side
hashers quietly throw information away:

* **MongoDB `$toHashedIndexKey`** (and hashed indexes): the hasher converts
  every numeric type to int64 with `safeNumberLongForHash` before hashing,
  recursing into embedded documents with field names; MongoDB documents that
  "a hashed index uses the same hash to store the values 2.3, 2.2, and 2.9".
  Result: fractional changes, and int32/int64/double/decimal type changes of
  the same integral value, are invisible. mongodb-labs/migration-verifier's
  own docs say its `ToHashedIndexKey` mode "may miss certain type
  conversions between numeric types" and add the document length as a
  partial guard. migkit compares `$toHashedIndexKey($$ROOT)` alone.
* **ClickHouse `cityHash64`/`sipHash64`**: "may be the same for the same
  values even if types of arguments differ", and the documented example
  value of `cityHash64` changed between releases - fine inside one server,
  not across versions.
* **SQL Server `CHECKSUM`** is case-insensitive and ignores `N'-'`;
  `BINARY_CHECKSUM` stops at 255 characters of nvarchar(max) and skips
  xml/text/ntext/image (migkit already moved off it, R18).

---

## 3. Trees, chunks and bisection

### 3.1 Merkle trees over key ranges (anti-entropy)

* **Dynamo (2007)**: one Merkle tree per key range a node owns; replicas
  compare roots and descend only into differing children.
* **Cassandra** builds the trees at repair time with a "validation
  compaction" (a full read). Depth was fixed at 15 (32,768 leaves), so with
  a million partitions about 30 partitions are streamed per damaged one
  (*overstreaming*); depth 18-20 was found to cost little
  (CASSANDRA-5263), `repair_session_max_tree_depth` then
  `repair_session_space` (4.0+) bound the memory. Lesson: the leaf count is
  the knob between bytes shipped and precision.
* **ScyllaDB row-level repair (3.1)** dropped the tree: per-row hashes on a
  small range held in memory, set reconciliation between replicas, only the
  mismatched rows moved - 6.78x faster and <3.6% of the bytes of the old
  partition repair when little differed; it also moved from SHA-256 to a
  64-bit non-cryptographic hash.
* **pg_comparator** (Coelho): materialises a checksum table (key hash, tuple
  hash) on each side, then summary tables level by level with a
  `--folding-factor` of 2^7 by default (4-8 "reasonable"), `--aggregate sum`
  by default (xor needs an extension and has a signed/unsigned problem across
  engines), checksum size 8 bytes, Jenkins hash by default. Its analysis:
  network ≲ k·f·ceil(log n / log f)·(c + log n) for k differences; requests
  6 + ceil(log n / log f) when equal, at most 6 + 2·ceil(...); false-negative
  ≈ k·ceil(log n / log f)·2^-c; "the time is spent mainly on computing the
  initial checksum table" - and it suggests a **trigger-maintained tuple
  checksum** for frequent re-checks.
* **Prolly trees (Dolt)**: content-defined chunk boundaries (a hash of the
  key decides where a chunk ends) make the tree history-independent, so two
  independently built trees of the same data are identical and a diff costs
  O(changes); DoltHub measured single-row diffs "in effectively constant
  time" from 25k to 1M rows. Relevant idea for migkit: **boundaries chosen by
  hash of the key are the same on both sides without coordination.**

### 3.2 Segment bisection tools

| Tool | Segment digest | Split | Leaf | Notes |
|---|---|---|---|---|
| data-diff / reladiff | `count(*)`, `sum(md5 low 60 bits)` | `--bisection-factor` 32 (key min/max, uniform assumption; text/UUID keys mapped to integers; compound keys as an N-d grid) | `--bisection-threshold` 16,384 rows: below it the rows are **downloaded** | priority queue by depth over threads; `joindiff` (outer join) when both tables are in one database; MD5 on SQL Server ~100x slower than PG's, so reladiff dropped it there |
| sync-diff-inspector (TiDB) | `COUNT(*)`, `BIT_XOR(CRC32(CONCAT_WS(',', cols, CONCAT(ISNULL(col)...))))` | chunks from index statistics, default 50,000 rows, ≤10,000 chunks | on mismatch split **once** into halves; if both halves differ, go row by row; PingCAP's own write-up proposes bisecting to a 3,000-row minimum | XOR (2.1) and CRC (2.4) weaknesses both apply; floats compared at 6/15 significant digits |
| Vitess VDiff v2 | none - streams rows | per shard, ordered by PK, merge-sorted | row-by-row compare | resumable from last PK (a new snapshot each resume, so "rows compared match as of the snapshot taken when the comparison is performed"); a resume bug with PK column order fixed July 2026 (PR 20603) |
| Oracle Veridata | key literally + hash of non-key columns | external merge sort by key | every row's hash crosses | "maybe out-of-sync" rows re-read after a latency window (confirm-out-of-sync), value by value, then written to an OOS file |
| mongodb-labs migration-verifier | full document bytes (or `$toHashedIndexKey` + length) | 400 MB partitions | per document | change stream opened first; recheck generations until `writesOff`; metadata on the destination by default (`--metaURI` moves it) |

### 3.3 Cost model

N rows per side, d differing rows (scattered), L rows per leaf, s bytes per
(key, hash) pair, c bytes per digest (≈ 24: count + 64-bit sum as decimal
text is more on the wire - measure), RTT r.

| Method | Rows read per side | Bytes on the wire | Sequential round trips |
|---|---|---|---|
| Ship every (key, hash) (migkit drilldown, Veridata; VDiff ships whole rows) | N (plus a sort or an in-memory dict) | N·s | 1 (streamed) |
| Binary bisection with re-scans (migkit `hetero._bisect`) | ≈ N·(log2 d + 2), capped at N·log2(N/L) | ≈ 2c·d·log2(N/(d·L)) + d·L·s | ≈ log2(N/L) levels, one query per node (sequential in migkit) |
| f-ary bisection (reladiff, f = 32) | ≈ N per level that still has differing segments | ≈ c·f·d·log_f(N/L) + d·L·(row bytes: it downloads rows) | log_f(N/L) + 1 |
| Materialised checksum table + summary levels (pg_comparator) | N once, then small tables | ≈ k·f·log_f(n)·(c + log n) | 6 + log_f n .. 6 + 2·log_f n; **needs a writable temp table on each side** |
| **One scan, all leaves at once** (proposal 3.4) | N + d·L (index range reads of differing leaves) | c·N/L + d·L·s | 2 |
| **In-SQL IBLT** (section 4.3) | N (k× aggregation work) | ≈ (1.3..4)·d·cell + fixed m·cell | 1 if d ≤ capacity, else 2 |

### 3.4 One scan gives the whole tree

Because the fold is an exact sum, a parent's digest is the sum of its
children's. So

```sql
select bucket, count(*), sum(h), sum(kh) from (...) group by bucket
```

returns every leaf in one pass, and every coarser level is computed on the
client by adding. Nothing is re-read to "descend". With L = 1,000 and N = 10^9
this is 10^6 leaves ≈ 24-40 MB per side - two orders below shipping every
hash, and one scan instead of log2 d + 2. Choose L from a byte budget:
bytes ≈ c·N/L + d·L·s is minimised at L = sqrt(c·N/(d·s)); with d unknown,
pick L so that c·N/L is a fixed small fraction of the table (e.g. 0.1%) and
let the second pass be index range reads of only the differing leaves.

Bucket functions, all computable in the scan:

* **Range buckets on an indexed orderable key** - PG `width_bucket(key,
  thresholds_array)` (binary search over a sorted array; any sortable type),
  MySQL `INTERVAL(key, b1, b2, ...)` (documented binary search; integers
  only), integer division for int keys everywhere. Second pass = index range
  scans. Boundaries from the existing quantile planner (`ranges.bounds_sql`).
* **Hash buckets** - `bucket = h(canonical key) mod B`. No ordering, so **no
  collation problem across engines and any key type or composite key works**
  (today `hetero._bisect` refuses anything but a single integer key). Cost:
  the second pass is a filtered full scan (`where h(key) mod B in (...)`),
  still returning only d·L rows.

---

## 4. Set reconciliation: bytes proportional to the difference

Treat each side as a set of fixed-length elements e = (key-or-key-hash,
row-hash). Rows equal on both sides cancel; a changed row appears as two
elements with the same key part.

### 4.1 The schemes

| Scheme | Wire cost for d differences | Decode | Notes |
|---|---|---|---|
| IBLT / IBF (Goodrich-Mitzenmacher 2011; Eppstein-Goodrich-Uyeda-Varghese SIGCOMM 2011 "What's the Difference?") | m cells ≈ (1.2..10)·d, each (count, keySum, hashSum) | peeling, O(d); **can fail** if m too small (detected, not silent) | needs d estimated beforehand: the **Strata Estimator** (trailing-zero strata of small IBFs) or a first pass |
| **Rateless IBLT** (Yang-Gilad-Alizadeh, SIGCOMM 2024) | 1.72·d at d = 4, converging to **1.35·d** by d in the low hundreds; stream until decodable, no estimate | O(ℓ log d) per item to encode, O(ℓ log d) per difference to decode; 3.4 M differences/s on one 2016 core at d = 1000, ℓ = 8 bytes | 3-4x less communication than regular IBLT, 2-2000x less compute than PinSketch; on Ethereum state 5.6x faster and 4.4x fewer bytes than the production Merkle trie; Go library `riblt` (+ Rust/C++ ports); mapping probability ρ(i) = 1/(1 + i/2) needs a per-element pseudo-random walk |
| **Minisketch / PinSketch** (BCH; Bitcoin Erlay, BIP 330) | exactly b·c bits for capacity c of b-bit elements - the information-theoretic minimum | **guaranteed** when d ≤ c; O(d^2) | 49x faster than pinsketch at capacity 4096; beats sending a 2,500-element set on ≤ 1 Gbit/s at d = 20; extension sketches when decode fails; C library, MIT |
| CPISync (Minsky-Trachtenberg-Zippel 2003) | ≈ d·ℓ | cubic; 8000x slower than minisketch at the sizes above | characteristic polynomial evaluations = products mod p |
| Stuffed IBLTs (Klausen-Pagh-Walzer, arXiv Sept 2026) | within 1+ε of optimal | O(n), failure n^-c | linear sketch that recovers **multiplicities** - the multiset case (key-less tables) |

Merkle trees are the baseline all of these beat on bytes: O(d·log N) digests
over O(log N) round trips, against O(d) in one or two.

### 4.2 Can the sketch be computed *inside* the database?

This is the question that decides whether only kilobytes cross the network.

| Scheme | In SQL? | How / why not |
|---|---|---|
| Regular IBLT, **sum-cells** | **yes, portable** | per row, k cell indices from slices of one md5 of the key; `cross join (values (0),(1),(2))`; `group by cell` with `count(*)`, `sum(key_int or kh)`, `sum(rh)`, `sum(chk)` as exact decimals. Subtraction of two sketches is exact over the integers; a cell is pure when count = ±1 and chk(keySum, rhSum) = chkSum. The same arithmetic with count c and sums c·x decodes **multiset** elements (key-less tables) |
| Regular IBLT, xor-cells | PG 14+ (`bit_xor`), MySQL (`BIT_XOR`), ClickHouse (`groupBitXor`); not SQL Server | xor-cells cannot represent multiplicity; use sum-cells |
| Strata estimator | yes | `group by stratum, cell` where stratum = trailing zeros of a hash: 32 small IBLTs, ~tens of KB |
| Rateless IBLT | not practically | each element's cell indices come from a sequential pseudo-random walk (next index depends on the previous) - a recursive CTE per row. Belongs in a process beside the database |
| Minisketch / PinSketch | no | odd power sums over GF(2^b): carry-less multiplication, c of them per element |
| CPISync, MuHash | no (PG custom aggregate only) | product aggregate modulo a prime |

So: **a regular IBLT with sum-cells is the only reconciliation sketch every
SQL engine can build in one scan.** Where migkit runs a process beside each
server (R17d's reader/writer agents), that process can read (key, hash)
pairs at LAN speed and exchange a Rateless IBLT (or a minisketch for small d)
across the WAN - the best bytes-per-difference of all, with no SQL tricks.

### 4.3 In-SQL IBLT, concretely

* Element: (pk as an integer when the key is one integer - then the decoded
  element *is* the key), else (64-bit key hash, 64-bit row hash) and the
  decoded key hashes are resolved by one more filtered read (range-bounded
  when leaf buckets are known).
* Cells: m = 2^16 by default ≈ 2 MB per side as text; decodes about
  0.8·m ≈ 50,000 differences with k = 3 (asymptotic peeling threshold
  ~0.81; small m needs slack). Beyond that, R18.4's rule already holds: a
  table that different is re-copied by range, not repaired row by row.
* **Self-checking**: after peeling, the sum of the decoded elements' row
  hashes must equal ΔΣh from the whole-table digest, and the counts must
  match Δcount. A wrong decode (false pure cell, ~2^-64 each) is caught by
  this, so the IBLT never weakens the digest's guarantee.
* Cost: hashing is shared with the digest (one md5 per row; slices give the
  k indices), aggregation input grows k-fold. Whether the planner recomputes
  md5 per cross-join row (PG may flatten the subquery; MySQL computes a
  repeated `md5()` twice, as sync-diff's maintainers found) must be measured
  (section 8).

### 4.4 When reconciliation beats bisection

* **Bytes**: IBLT ≈ (1.3..4)·d·cell vs leaves c·N/L + d·L·s. At N = 10^8,
  L = 1,000, d = 100, s = 40: leaves ≈ 2.4 MB + 4 MB; IBLT (m = 4,096)
  ≈ 130 KB. At d = 10^5: leaves 2.4 MB + 4 GB of second-pass pairs (or a
  range re-copy); IBLT m = 2^17 ≈ 4 MB.
* **Round trips**: IBLT 1 (2 on decode failure); leaves 2; bisection
  log_f(N/L)+1 sequential.
* **Server reads**: IBLT N; leaves N + d·L (index); binary bisection
  N·(log2 d + 2).
* **CPU**: IBLT costs k aggregations per row; leaves cost one group-by.

Rule of thumb for the decision layer: link is the bottleneck (WAN, low
bandwidth, high RTT) and d is expected small (a re-verify after catch-up, a
rehearsal re-run) -> IBLT in the same scan as the digest; LAN or d unknown
and possibly large -> leaf digests; d·L large relative to the table -> range
re-copy.

---

## 5. Sampling, and streaming comparisons over sorted keys

* **Sampling cannot prove equality.** To see at least one bad row with
  probability 1-α when a fraction p is bad needs n ≈ ln(1/α)/p samples: a
  one-in-a-million corruption at 99% confidence needs ~4.6 million sampled
  rows. Sampling is a triage signal (DVT, Datafold offer it), never the
  proof. The digest reads every row once and is the cheaper proof.
* **Streaming merge over sorted key cursors** (VDiff, Veridata, reladiff's
  leaf download): both sides `order by key`, merge on the client, O(1)
  memory, no cap. Costs: an index-order scan (random heap I/O on a
  non-clustered PostgreSQL table, sequential on InnoDB/clustered), and
  **the two orders must agree**: text keys sort by collation, which differs
  across engines (and between PG's libc/ICU versions). Order by the canonical
  bytes (`COLLATE "C"` / `utf8mb4_bin` / binary) on both sides, or merge on
  a key hash with hash buckets. migkit's drilldowns build dicts; a sorted
  merge would remove the memory cap (not the byte cost).

---

## 6. Fast in-server row hashes, per engine

| Engine | Options | Cost / caveats |
|---|---|---|
| PostgreSQL | `md5(text)`; `sha256(bytea)` (11+); `hashtext`/`hashtextextended` (internal); `hash_record_extended(record, seed)` (14+, typed, no text rendering) | 2010 thread: hashtext 0.77 s vs md5 1.24 s per 1M rows. Internal hashes are **not stable across platforms or versions** (Tom Lane: "we've never thought that hash values are required to be consistent across platforms"; endianness is why `pg_dump --load-via-partition-root` exists) - only usable when both sides are the same major version on the same architecture, never persisted. The `::text` rendering usually costs more than md5 (migkit measured the same in Python, R18). `sum(...::numeric)` is slower than an int8 sum; worth measuring `sum(bigint)` (PG returns numeric for bigint sums anyway). PG 12+ prints floats shortest-exact (Ryu) by default |
| MySQL / MariaDB | `CRC32`, `MD5`, `SHA1`, `SHA2`; no xxhash | a repeated `md5(x)` in one SELECT is evaluated twice (derived table with NO_MERGE, or one slice, avoids it); `SUM` of integers is an exact DECIMAL; `conv(hex,16,10)` returns a string - cast to unsigned/decimal (migkit's canon notes the DOUBLE coercion trap). **FLOAT (single) text output is capped at 6 significant digits** (DOUBLE is shortest round-trip via dtoa) |
| SQL Server | `HASHBYTES('SHA2_256'|'SHA2_512'|'MD5', ...)`; `CHECKSUM`, `BINARY_CHECKSUM`, `CHECKSUM_AGG` | HASHBYTES input limited to 8,000 bytes up to 2014; MD5/SHA1 deprecated from 2016; reladiff found MD5 there ~100x slower than PostgreSQL's; `CHECKSUM_AGG` is XOR (cancels even multiplicities); `CONCAT_WS` skips NULLs (ambiguous) - migkit's `FOR JSON ... INCLUDE_NULL_VALUES` avoids that but renders `t.*` in physical column order (the same drop/add-column false alarm PG had) |
| ClickHouse | `cityHash64`, `sipHash64/128`, `xxHash64`, `xxh3` (64 and 128), `groupBitXor` | fastest of all; `sum(UInt64)` wraps mod 2^64 (fine as a digest with the count lane; mod-2^k caveat 2.1); type-blind and version-dependent hash values (2.5); the docs' own "table checksum" example is `groupBitXor(cityHash64(*))` - XOR |
| MongoDB | `dbHash` (md5 per collection); `$toHashedIndexKey` (64-bit, numbers normalised to int64); **`$hash` / `$hexHash` (8.3+: md5, sha256, xxh64 over a string or binData)** | `dbHash` is documented to take a shared database lock that blocks writes until it finishes (the current source takes an intent lock - check per version); before 8.3 there is no exact server-side per-document hash, so an exact per-document comparison must read documents (migkit's `_client_hashes` over raw BSON is exact) |
| Oracle / Snowflake / BigQuery (for completeness) | `STANDARD_HASH` / `ORA_HASH`; `HASH_AGG`; `FARM_FINGERPRINT` + `BIT_XOR` | engine-internal 64-bit hashes are not reproducible elsewhere; any XOR aggregate inherits 2.1 |

### 6.1 The MongoDB finding in full

`mongodb.py` `check_data` compares `dbHash`; on a mismatch `_drilldown`
compares `$toHashedIndexKey: "$$ROOT"` per `_id` and, finding no difference,
returns `ok` ("docs N compared by id hash"). Because the hasher normalises
every number to int64 (source: `src/mongo/db/hasher.cpp`, "Use
safeNumberLongForHash"; MongoDB's hashed-index docs), a document whose only
change is 2.3 -> 2.9, or NumberLong(5) -> 5.0, passes. Fix options, cheapest
first: (a) when `dbHash` differs and the id-hash walk finds nothing, walk the
same ranges again with `_client_hashes` (md5 of the raw BSON - exact, field
order and types included) instead of reporting ok; (b) on 8.3+, hash a
canonical string built in the pipeline with `$hexHash`; (c) add `$bsonSize`
to the id hash (catches int32 <-> 64-bit changes, not 2.3 -> 2.9).

---

## 7. Canonicalization: exactness is decided by the rendering

A digest is exact only if equal values render to equal bytes **and unequal
values never do**. The second half is where false "consistent" verdicts come
from; hash bits cannot fix a lossy rendering. migkit's `canon.py` already
handles banded floats, NaN/Infinity as uncomparable, booleans, bytes as hex,
microsecond timestamps in UTC, jsonb key order, `-0.0`, `YEAR`/`BIT` as
numbers, PostGIS as EWKT, and an injective row encoding (`rowtext`, with a
NULL distinct from every string). Checklist of what any cross-engine digest
must decide, with the known traps:

| Class | Trap | Rule |
|---|---|---|
| float | PG 12+ and MySQL 8 DOUBLE print shortest round-trip, but formats differ (`1e+20` vs `1e20`); MySQL FLOAT (single) prints 6 digits; `decimal(65,20)` saturates above 1e45 and zeroes below 1e-20 (canon.py measured) | banded rendering (migkit); render FLOAT via its exact double value, never via `cast(... as char)` |
| decimal | trailing zeros carry the scale: 1.10 vs 1.100 | decide explicitly: value-equal (normalise scale) or scale-equal (today: digits as sent, so a scale change is reported) |
| timestamp | precision (MySQL 6, SQL Server 7, Oracle 9, BSON 3 digits); some engines round and others truncate when narrowing (reladiff keeps both variants); `timestamptz` vs wall clock; SQL Server `datetime` 1/300 s | render at the **narrower** side's precision only when the move itself narrowed, and report it; always UTC for instants |
| text | collation equality (`'a' = 'A'`, PAD SPACE `'a' = 'a '`) affects keys and grouping, not md5 of bytes; `CHAR(n)` padding stripped by MySQL/PG, kept by SQL Server; Oracle `'' IS NULL`; NFC vs NFD are different bytes | hash the bytes (UTF-8), never normalise Unicode silently; report NFC/NFD and padding as findings; compare keys case-sensitively even when the source collation is not |
| json | key order (jsonb/MySQL sort by length then bytes; Mongo keeps order), duplicate keys, number spelling (`1` vs `1.0`) | jsonb's form (migkit) or RFC 8785 JCS (UTF-16 key sort, ECMAScript numbers) as the neutral spec |
| null / empty | NULL vs '' vs absent field (Mongo) | distinct tokens (rowtext does); Mongo "absent" is a third state |
| integer / bool / enum / uuid | tinyint(1) vs boolean; enum label vs ordinal; uuid case and dashes | render by class, lower-case uuid, enum by label |
| binary / geometry | hex case; PostGIS EWKB not stable across patch releases | upper hex; EWKT (migkit) |

---

## 8. Verification under writes

* **Snapshot-consistent read**: both sides read at one snapshot each
  (exported snapshot / REPEATABLE READ / `START TRANSACTION WITH CONSISTENT
  SNAPSHOT`) - exact for that instant, but the two instants differ unless
  writes are stopped.
* **Fences** (migkit `fenced_recheck`): read the source position (LSN,
  GTID set, oplog time), wait until the target has applied past it, then
  compare the suspects - two rounds for hot rows. Veridata's
  confirm-out-of-sync is the same idea with a time window instead of a
  position; a position is exact, a window is a guess.
* **Delta verification by changed keys** (migkit `delta_setup`): the change
  stream names the keys touched since the last proof; only those are
  re-compared. migration-verifier's **generations** formalise it: gen 0 is
  the full pass; each later generation re-checks what changed or failed in
  the previous one, until a final generation after writes stop.
* **Maintained digests**: the sum fold is incremental - D' = D - h(old) +
  h(new) (the property LtHash exists for, and pg_comparator's
  trigger-maintained checksum). migkit's tail already sees every change on
  the source, so per-range source digests can be kept current from the
  stream; the target side must still be *read* (a digest of what migkit
  applied proves migkit's bookkeeping, not the target), but only the ranges
  the stream marked dirty.
* **Resumed comparisons** compare different ranges at different instants
  (VDiff says so explicitly); a final fenced pass over the ranges touched
  since each was compared closes that gap.

---

## 9. Design for migkit: what the decision layer composes per table

No new modes or flags: every choice below is made by migkit from facts it
already reads (engines and versions, key shape and index, row count and
width, measured link bandwidth and RTT, whether a relay sits beside each
server, write activity and fence availability, CPU headroom from the
throttle, and the previous proof in `proof.json`).

### 9.1 The passes

**Pass A - prove (every table; one scan per side, both sides at once).**
* Row hash: md5 of the canonical text (byte-exact), salted per run, 64 bits
  folded by an exact integer sum; a second 64-bit lane (another md5 slice,
  same md5 call) when the table's proof is the final one before cutover.
  `hash_record_extended(row, salt)` (PG 14+) only between servers of the same
  major version and architecture, and reported as what it is: equality
  under each type's hash opclass (numeric 1.10 = 1.1, a nondeterministic
  collation's 'a' = 'A', -0 = 0), not byte identity.
* Lanes: count, Σh, Σh(key) (as today).
* **Leaves in the same scan** when shipping every pair would exceed a byte
  budget: `group by bucket`, L = max(1,000, c·N / budget). Range buckets
  (`width_bucket` / `INTERVAL` / integer division) when the key is one
  orderable column with an index and both sides order it identically
  (integers; text only under a binary collation on both); **hash buckets
  otherwise** - any key type, composite keys, and cross-engine text keys,
  which `hetero._bisect` refuses today.
* **An IBLT in the same scan** (sum-cells, m sized by the expected
  difference) when the link is the constraint - measured bandwidth low or
  RTT high - and the expected d is small: a re-verify after catch-up, a
  rehearsal re-run, a generation after the first.

**Pass B - localize (only what differs).**
1. IBLT decodes and passes the self-check -> the full list of differing
   keys, no further reads.
2. Otherwise the differing leaves: index range reads (range buckets) or one
   filtered scan (hash buckets), streaming (key, hash) sorted and merged on
   the client - no dict, no cap.
3. When the differing leaves hold more rows than re-copying them costs, re-copy
   those ranges (R18.4's rule, now per leaf).
4. Where a migkit process sits beside each server (R17d), the (key, hash)
   pairs are read there and reconciled across the WAN with a Rateless IBLT
   (or minisketch for very small d) - bytes ≈ 1.35-1.72·d·ℓ.

**Pass C - confirm and explain.**
* **Self-check (new, cheap):** the differences found must account exactly
  for the table digest's delta - Δcount and ΔΣh recomputed from the per-row
  hashes of the missing, extra and changed rows. If they do not, a leaf
  collided or a row was missed, and the pass widens. This makes
  localization provably complete up to 2^-k, and catches any IBLT
  mis-decode.
* `fenced_recheck` on the suspects (exists), then a literal value compare of
  the survivors (Veridata's confirm step, but at a position, not a time
  window) and the column fingerprint over the differing leaves only.

**Pass D - keep it proven under writes.** Store per-leaf digests with the
source position in `proof.json`; the change stream marks leaves dirty; the
next generation re-reads only dirty leaves on both sides (migration-verifier's
generations, with migkit's fences). The final generation is fenced after
writes stop.

### 9.2 Exactness guarantees

| Component | Guarantee |
|---|---|
| Exact-sum fold, k bits, with count | a differing table (or leaf) reads equal with probability ≤ 2^-k, for any sizes and multiplicities; with a per-run salt, independent across runs |
| Two independent 64-bit lanes | ≤ 2^-128 |
| Sum mod 2^k (ClickHouse native) | ≤ 2^(v-k), 2^v the largest power of two dividing the multiplicity difference; keep the count lane |
| XOR folds (CRC32 or not) | none for rows present an even number of times; never used |
| Sum of CRC32 | ≈ 2^-16 against value swaps between equal-length rows; drop or pair with md5 |
| Leaves | cannot weaken the table verdict (the table digest is still compared); localization completeness enforced by the self-check |
| IBLT | decode failure is visible, never silent; false pure cells (~2^-64 each) caught by the self-check |
| Minisketch | decode guaranteed when d ≤ capacity |
| Rendering | exact only within `canon`'s rules; values it cannot render are counted as uncomparable, never hashed equal |
| Under writes | exact at the fence position; resumed passes closed by a final fenced pass over touched leaves |

### 9.3 What to measure in docker to rank them

| # | Measure | Matrix | Metric |
|---|---|---|---|
| M1 | per-row hash cost | PG md5(text), sha256, hashtextextended, hash_record_extended, rendering alone; MySQL crc32/md5/sha2; SQL Server HASHBYTES over FOR JSON vs CONCAT; ClickHouse cityHash64/xxh3 - rows of 64 B / 512 B / 4 KB, 1 and 4 workers | rows/s/core |
| M2 | fold cost | PG `sum(x::numeric)` (today) vs `sum(x)` over bigint (PG sums int8 into a numeric result; check it uses its 128-bit accumulator); one lane vs two; MySQL md5 referenced twice vs once through a derived table | seconds per 10^7 rows |
| M3 | leaves in one scan | 1 / 10^3 / 6.5·10^4 / 10^6 buckets; width_bucket vs integer division vs hash-mod; MySQL INTERVAL | overhead vs the plain aggregate; bytes returned |
| M4 | IBLT in SQL | k = 3, m = 4,096 / 65,536; cross join vs unnest; does md5 run once per row (EXPLAIN VERBOSE and timing) | overhead vs the plain aggregate; decode success vs d; Python decode time |
| M5 | localization end to end | N = 10^7 (10^8 if disk allows); d = 0, 1, 10, 10^3, 10^5, scattered and clustered; current drilldown, `hetero._bisect`, reladiff (factor 32), leaves + range reads, leaves + hash buckets, IBLT; netem RTT 0 / 10 / 50 / 150 ms, bandwidth unbounded / 20 MB/s | wall time, source rows read (`pg_stat_user_tables`, `Handler_read_*`), bytes on the wire, queries issued |
| M6 | false-negative repros | Mongo 2.3 -> 2.9 and NumberLong -> Double under `$toHashedIndexKey`; MySQL value swap under `sum(crc32)`; duplicate rows under XOR; SQL Server drop/add column under `t.*`; PG numeric 1.10 vs 1.1 under `hash_record_extended` | caught or not (each becomes a regression test) |
| M7 | under writes | pgbench / sysbench running; full re-scan vs fenced generations over dirty leaves | time to a clean fenced proof |
| M8 | cross-engine rendering | canonical expression vs native text per engine and class | rows/s; share of time in rendering |

### 9.4 Effort

| Item | Effort | Why |
|---|---|---|
| MongoDB drilldown: never report ok when `dbHash` differs and the id hash finds nothing - re-walk those ranges with the exact raw-BSON hash (`_client_hashes`); `$bsonSize` beside the id hash | S | a false "ok" today |
| MySQL md5 lane widened to 64 bits (or two), crc32 lane dropped or kept only as extra; checkpoints keyed on the expression already refuse to mix | S | ~2^-48 -> 2^-64 on swaps, and faster without crc32 |
| PG: sum the bigint directly instead of casting every term to numeric | S (after M2) | likely a pure speed win, same exact answer |
| Per-run salt in every row hash (stored with checkpoints so resumed partials reuse it) | S | runs become independent |
| Self-check of localization against the digest delta | S | makes every drilldown provably complete |
| One scan returns all leaves; replace `hetero._bisect`'s re-scans and the per-range query loops | M | N·(log2 d + 2) reads -> N + d·L; sequential queries -> 2 round trips |
| Hash buckets for composite / text / cross-engine keys | M | localization for every keyed table, not only single-integer keys |
| Streaming sorted-merge drilldown (no dict, no cap) with binary ordering of text keys | M | removes the 20,000 / 2,000,000 caps as a memory limit |
| In-SQL IBLT (sum-cells) for PG, MySQL, SQL Server + Python peeling decoder + size choice (count delta, previous proof, or a strata estimator in the same scan) | M | O(d) bytes, one round trip on WAN legs |
| MongoDB 8.3+: `$hexHash` over a canonical string in the pipeline for the neutral digest | M | stops the collection crossing the network |
| Relay-side Rateless IBLT / minisketch across the WAN | L | depends on R17d; best bytes per difference |
| Maintained per-leaf digests from the tail + dirty-leaf generations | L | continuous verification without re-scans |
| Multiset (key-less) reconciliation beyond sum-cells (Stuffed IBLT) | M | only if M5 shows key-less tables at scale |

---

## Sources

* Clarke, Devadas, van Dijk, Gassend, Suh, *Incremental Multiset Hash
  Functions* (ASIACRYPT 2003): https://people.csail.mit.edu/devadas/pubs/mhashes.pdf
* Bellare, Micciancio, *A New Paradigm for Collision-free Hashing* (1997):
  https://eprint.iacr.org/1997/001
* Lewi, Kim, Maykov, Weis, *Securing Update Propagation with Homomorphic
  Hashing* (LtHash, 2019): https://eprint.iacr.org/2019/227
* Wagner, *A Generalized Birthday Problem* (2002):
  https://www.iacr.org/archive/crypto2002/24420288/24420288.pdf
* Maitin-Shepard et al., *Elliptic Curve Multiset Hash*: https://arxiv.org/abs/1601.06502
* Yang, Gilad, Alizadeh, *Practical Rateless Set Reconciliation* (SIGCOMM
  2024): https://arxiv.org/abs/2402.02668 ; code https://github.com/yangl1996/riblt
* Eppstein, Goodrich, Uyeda, Varghese, *What's the Difference?* (SIGCOMM
  2011): http://conferences.sigcomm.org/sigcomm/2011/papers/sigcomm/p218.pdf
* Klausen, Pagh, Walzer, *Stuffed IBLTs* (2026): https://arxiv.org/abs/2609.17487
* Minisketch: https://github.com/bitcoin-core/minisketch ; BIP 330:
  https://github.com/bitcoin/bips/blob/master/bip-0330.mediawiki
* reladiff technical explanation: https://reladiff.readthedocs.io/en/latest/technical-explanation.html ;
  https://eshsoft.com/blog/how-reladiff-works
* data-diff technical explanation: https://github.com/datafold/data-diff/blob/master/docs/technical-explanation.md
* sync-diff-inspector: https://docs.pingcap.com/tidb/stable/sync-diff-inspector-overview/ ;
  CRC32 vs MD5 measurement in https://github.com/pingcap/tidb-tools/pull/707
* Percona, CRC32 collisions: https://www.percona.com/blog/how-to-avoid-hash-collisions-when-using-mysqls-crc32-function/
* Vitess VDiff v2: https://vitess.io/blog/2022-11-22-vdiff-v2/ ; resume fix
  https://github.com/vitessio/vitess/pull/20603
* Oracle Veridata: https://blogs.oracle.com/dataintegration/oracle-goldengate-veridata-how-it-works
* migration-verifier: https://github.com/mongodb-labs/migration-verifier
* pg_comparator manual: https://manpages.debian.org/unstable/postgresql-comparator/pg_comparator.1.en.html
* ScyllaDB row-level repair: https://www.scylladb.com/2019/08/13/scylla-open-source-3-1-efficiently-maintaining-consistency-with-row-level-repair/ ;
  https://github.com/scylladb/scylladb/blob/master/docs/dev/row_level_repair.md
* Cassandra Merkle depth: https://issues.apache.org/jira/browse/CASSANDRA-5263 ;
  https://cassandra.apache.org/doc/latest/cassandra/managing/configuration/cass_yaml_file.html
* Dolt prolly trees: https://docs.dolthub.com/architecture/storage-engine/prolly-tree
* MongoDB: `dbHash` https://www.mongodb.com/docs/manual/reference/command/dbhash/ ;
  `$toHashedIndexKey` https://www.mongodb.com/docs/manual/reference/operator/aggregation/tohashedindexkey/ ;
  hashed indexes https://www.mongodb.com/docs/manual/core/indexes/index-types/index-hashed/ ;
  hasher https://github.com/mongodb/mongo/blob/master/src/mongo/db/hasher.cpp ;
  `$hash` (8.3) https://www.mongodb.com/docs/manual/reference/operator/aggregation/hash/
* SQL Server: https://learn.microsoft.com/en-us/sql/t-sql/functions/hashbytes-transact-sql ;
  https://learn.microsoft.com/en-us/sql/t-sql/functions/checksum-transact-sql
* ClickHouse hash functions: https://clickhouse.com/docs/sql-reference/functions/hash-functions ;
  `sumWithOverflow` https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/sumwithoverflow
* PostgreSQL float output (Ryu, 12+): https://www.postgresql.org/docs/current/datatype-numeric.html ;
  pg_dump `--load-via-partition-root`: https://www.postgresql.org/docs/current/app-pgdump.html
* MySQL floats and `INTERVAL()`: https://dev.mysql.com/doc/refman/8.4/en/floating-point-types.html ;
  https://mariadb.com/kb/en/library/interval
* RFC 8785 (JCS): https://www.rfc-editor.org/info/rfc8785/
