# Goals and modes

A hop says what it is for in one word, its `goal`. The commands that do
the work run in modes. This page says, for each, what happens on the
source and on the target and what migkit guarantees. `migkit init` says
the same when it proposes a goal, `migkit assess` and every dry run say it
for the hop at hand, all from one table in the code (`hopkeys.GOALS`,
`MODES`, `CUTOVER`).

## Goals

| Goal | On the source | On the target | Guaranteed |
|---|---|---|---|
| `copy` (the default) | read, never written: each table read as of one moment where the engine gives one; a row the application changes while it is read is not carried, and the check names it | the tables it lacks made and filled with the source's rows, indexes built after them; a table that already holds rows is refused unless the hop says `target_rows: refuse`, `replace` or `keep` | every row compared after the copy (`migkit check`): equal, or each row that differs named |
| `cutover` | read, and its log of changes followed from a position taken before the copy - the stream's own slot and publication on PostgreSQL, its binary log read on MySQL, a change stream on MongoDB - made by the stream and dropped when it is torn down; nothing else is written to it unless the hop turns `cutover:` on, and then only during the cutover | filled as a copy fills it, then kept level with the source change by change; kept from the application's writes until the cutover where the engine allows it (`protect_target`); after it, a stream back from it to the old source (`reverse: at_cutover`) | no change lost between the copy and the stream; each cycle's changes compared (`watch --verify --delta`); the stream torn down only once nothing writes to the source and the stream has passed where it stands |
| `verify` | read only | read only | equal, or every table and row that differs named; nothing written to either side |
| `two-way` | written: the target's changes carried to it by key, and its own told apart from what migkit wrote there, so nothing goes round | written the same way: the source's changes carried to it | a change made on either side arrives on the other once; where both sides changed one row, what only one side changed is kept and a column both changed is settled by its rule, alike on both sides (`two_way`: `on_conflict`, `tables`, `delete_vs_update`) |
| `keep-in-sync` | read, and its log of changes followed for as long as it runs; nothing else is written to it | a copy kept level with the source, the application kept from writing to it (`protect_target`) | each cycle's changes compared, the stream's lag said, a difference named |
| `rehearse` | a copy or a sample of the real run's source, read and followed as a cutover follows it; written only by a cutover the hop turns on, as the real run's would be | a target of its own, filled, followed and cut over to as the real run's will be; its own writes are timed there, rolled back | every phase measured and kept for the real run's forecast (the hop it names in `rehearses:`); the two sides proved equal before the real run is allowed to start |
| `upgrade` | read and followed as a cutover follows it; never upgraded where it runs - its downtime is the cutover's alone | a server of the new version or of a higher edition (SQL Server Web to Enterprise, MySQL Community to Enterprise) filled and followed, or a copy of the source's pages taken there - SQL Server's own backup restored (`physical: {backup: DIR}`), MySQL's clone, PostgreSQL's pages - upgraded by the new version's own upgrade where the version moves (`physical: {upgrade: ...}`), then followed from the copy's position | what the new version removes or changes, and what the new edition lacks of what the source uses, said before anything is written; the source's own statements planned, timed and held to the same answers on both versions |
| `subset` | read, never written: the rows chosen (`subset: {start: ...}`) and those they refer to | its tables in scope emptied and filled with the subset, masked on the way where the hop says (`mask_on_write`) | every reference in the copy resolves inside it; every row as the source's, its masked columns as the rule writes them, none beyond the subset |

**`goal: cutover` does not turn `cutover:` on.** The goal sets up the
stream, the stream back and the protection of the target; the cutover
migkit drives itself runs only where the hop's options say `cutover:`.
Without it, migkit writes nothing to the source at the cutover and the
operator does the steps by hand, with migkit proving around them (below).

## Modes

| Command | What it does |
|---|---|
| `move --mode full` | one copy of every table the hop carries: the source read, the target written |
| `move --mode cdc` | the stream of changes started, or resumed from where it stood: the source's log read, the target written change by change |
| `move --mode full+cdc` | the copy, then the stream from a position taken before the copy began, so nothing changed during it is lost |
| `move --mode cdc --drop` | the cutover: the stream torn down, and what else happens on each side is what `cutover:` says; run again once a cutover of the hop has flipped, its point of no return |
| `sync --mode reconcile` | the last check's differences shown; `--apply` writes the target to match the source |
| `sync --mode verify` | read only: every table compared as of one moment |
| `sync --mode seed` | the schema aligned, the copy, the differences repaired and a consistent check, one after another |
| `sync --mode stream` | the stream started, and each cycle's changes compared |
| `sync --mode migrate` | seed, then stream |

Without `--go` (or `--apply`) every one of them only shows its plan, and
starts with what each side will see.

## `cutover:` on or off

**Off** (no `cutover:` in the hop's options, whatever the goal): migkit
writes nothing to the source at the cutover. The operator, in order:

1. switch off on the source the jobs it runs (pg_cron jobs, MySQL
   events), so they write in one place
2. stop the application's writes to the source: stop it, or revoke its
   write privileges and end its sessions - ending them is what rolls back
   what they had not committed
3. `migkit move HOP --mode cdc --drop --go` - migkit proves nothing
   writes to the source and nothing is prepared there, waits for the
   stream to pass where the source stands, and only then tears it down
   (and starts the stream back, with `reverse: at_cutover`)
4. `migkit check HOP` - the final comparison
5. `migkit sync HOP --go --kind sequences --tag pre-cutover` - the
   target's counters past the source's
6. switch the jobs on on the target
7. point the application at the target
8. once the application is on the target for good, the point of no
   return: drop the stream back, if one runs, and give the old source
   back its writes

**On** (`cutover: true`, or its settings - [docs/cutover.md](cutover.md)):
migkit does all of that itself, and adds:

- a timed path: the write outage clocked from the pooler's pause or the
  freeze to the flip, against the hop's budget - past it, all of it is
  undone
- the freeze: migkit stops the source's writes itself, the way chosen
  from what the source allows - the whole server made read-only where it
  may, or the application's accounts kept from writing and their
  sessions ended - and reads it back; PostgreSQL, MySQL, MongoDB, Redis,
  SQL Server, Cassandra, Kafka and OpenSearch sources alike
- the drain: the source's position taken after the freeze, and the hop's
  own stream waited for until it passes it - or, where the stream keeps
  no position migkit can read, a mark written on the source after the
  freeze and waited for on the target; a streaming pipeline and a leg run
  from here drained by their own offsets and positions
- the fenced check: the rows changed since the last verified pass
  compared inside the outage, and the schema compared with the
  preflight's
- the counters: the target's sequences and auto-increments raised past
  every key the source handed out
- the stream back: from the new primary to the old source (`reverse:
  at_cutover`), made and proven before the freeze where its engine lets
  it wait idle, so the flip only starts it, and proven to reach the old
  source
- the undo: every step recorded before it acts and put back, the last
  first, on any failure, on the budget passing, or on the next run after
  one killed half way
- the outage measured: each run's outage and each step's time recorded,
  and the next cutover refused where the last one's was past its budget
- the point of no return: a step of its own behind an approval of its
  own - the forward stream ended for good, the old source given back its
  writes, the stream back dropped, what the cutover left taken away -
  after which going back is refused

With it on, the source is written during the cutover only - its jobs
switched off, its writes frozen - each step reversible and behind the
hop's approvals; after the flip it stays frozen until
`migkit rollback HOP --db DB --apply`, or until the point of no return
gives it back.

## The case between the two servers

A hop of one engine may still join two servers that differ, and migkit
reads how from the servers themselves - never from an address - and says
it as the first line of every plan (`the case: ...`). `migkit assess` and
the check before the first write hold to each part, every refusal with its
fix, all at once (`migkit/case.py`):

| Case | What migkit reads | What it holds to |
|---|---|---|
| **a fork change** - MySQL, MariaDB or Percona Server; PostgreSQL, Citus, YugabyteDB, CockroachDB, and Amazon Aurora or AlloyDB where the server says it is one; Redis, Valkey, KeyDB or Dragonfly; Elasticsearch or OpenSearch; MongoDB, Percona Server for MongoDB, FerretDB or Azure Cosmos DB; Kafka or another broker of its protocol | one brand a side, from what only that software says of itself (`migkit/variants.py`) | what the source uses that the target has not: types, collations, extensions, module types, the commands migkit sends, a view's pipeline or a validator asked of the target, field types of the other search engine - and each fork's own ways: MariaDB's transaction ids and its JSON kept as text, Citus's distributed tables, each search engine's own users and roles |
| **a version down** - a rollback plan, an older target | each side's version, compared only between two of one program | a copy of the pages refused up front (pages, a clone or a backup are read only by the version that wrote them or a newer one) and the logical copy chosen; what the newer source uses that the older target has not - identity and generated columns, partitioning, SQL-standard bodies, `NULLS NOT DISTINCT` (PostgreSQL); invisible columns, indexes on expressions, check constraints, `utf8mb4_0900_*` collations, SRIDs, window functions and `JSON_TABLE` in code (MySQL); types and collations of a later SQL Server, its compatibility level; time-series, clustered and encrypted collections (MongoDB); `BOOLEAN`, `VECTOR`, long names and extended strings (Oracle) |
| **an edition change** - SQL Server Express, Web, Standard, Enterprise, Developer; Oracle XE, Free, SE2, EE; MySQL Community, Enterprise; MongoDB Community, Enterprise | each side's edition (`SERVERPROPERTY('Edition')`, `v$version`, `@@license`, `buildInfo`) | what the source's databases use that the target's edition lacks (`sys.dm_db_persisted_sku_features`, Oracle's partitioned, compressed and encrypted tables, MySQL's enterprise plugins and what uses them, MongoDB's encrypted storage, auditing, LDAP and Kerberos users): *stripped* - the logical copy makes the tables without it, the rows the same; *lost* - encryption at rest, auditing, change data capture - refused unless the hop accepts it; *refused* where what uses it cannot be made there. The target edition's caps against the source: Express's database size, the memory and cores Web, Standard and SE2 use. An edition down is said as the licence-driven move it is |

The way is chosen by the same rungs as any hop's: a copy of the pages
(SQL Server's own copy-only backup, proved by its checksums and restored;
MySQL's clone; PostgreSQL's pages) holds only where the target's version
and edition take what the pages hold - a clone needs every plugin its
source runs on the target, a restore every feature the database holds -
and the logical copy carries the rest.

`goal: upgrade` takes an edition up as an upgrade: the target at the
source's version, of a higher edition, is moved into as it is, and a copy
of the pages needs no step after it. An edition down, or a version down,
is `goal: cutover` or `copy`.

What a change of edition may leave behind, the hop may accept by name:

    options:
      edition:
        accept: [TransparentDataEncryption, audit]   # as assess names them

## A change on the source a faster way needs

Some ways are faster only with a change on the source: MySQL's clone needs
its clone plugin there; a copy of PostgreSQL's pages needs a replication
right and room for its connections. migkit reads on the server whether the
change is there, whether its own user may make it, or only a DBA, or
nobody (a managed service that refuses it), and:

- where migkit may make it, the dry run offers it - what changes, that it
  is taken off again, and the minutes it saves and the load it spares the
  source - and `--go` at a terminal asks: **yes** (migkit makes it behind
  the hop's approvals, `source-setting`, records it, and takes it off
  again after the work or on any failure), **no** (the source is left as
  it is), or **dba** (the statements go to the DBA's script). The answer
  is recorded; a hop gives it ahead with
  `source_changes: {clone_plugin: yes | no | dba}` (or one word for every
  change).
- where only a DBA can, or nobody can, nothing is asked: migkit says so,
  says what not using the faster way means - the next way, its time and
  its load on the source - and goes on with it.

No is a normal answer: many teams allow no change on a production source.

## Limits the move keeps to

A hop may say what its move may take at most. Each is a hard limit: the
plan keeps to it wherever some way can, and where none can it says what
binds and what would help. A hop that sets none is planned for the
soonest end.

```yaml
options:
  deadline: 6h                  # or a moment: 2026-10-01T02:00
  limits:
    source_load: 30%            # the share of the source's reads
    replica_lag: 30             # seconds the source's replica may fall behind
    money: 50                   # in the currency transfer_price_per_gb is in
    bytes_billed: 500GB         # what a warehouse may bill, read as money
    outage: 60                  # seconds of write outage at the cutover
    target_connections: 40      # the target's connections the move may take
    target_memory: 4GB          # the target's memory the move may take
```

- **deadline**: the whole move - copy, index builds and check - planned to
  end within it.
- **source_load**: the source planned as if it read only that fast, and
  while the move runs the pace holds the move's reads at that share.
- **replica_lag**: the pace holds the move to it while the replica is read.
- **money** and **bytes_billed**: the ways that bill less are taken first
  where the budget would be passed.
- **outage**: the dry run says whether the cutover's predicted outage keeps
  within it (with `cutover:` on, the cutover's own budget is this limit).
- **target_connections** and **target_memory**: as many writers at once as
  they carry.

The dry run says the outcome - the end with its spread, what sets it,
whether it keeps within the limits. How migkit chose each table's way,
the order, the tables and workers at once, and what it stood on, is kept
in the run's decision record; `MIGKIT_EXPLAIN=1` shows it on the console
too.
