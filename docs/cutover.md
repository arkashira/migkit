# Cutover, in order

The moment a migration can go wrong without anyone seeing it is the
cutover: the application moves from the source to the target while
changes are still in flight. The cutover is one command,
`migkit move HOP --mode cdc --drop --go`. What it does depends on the
hop: with `cutover:` in its options migkit runs the whole path itself,
timed, and undoes it on any failure; without it migkit writes nothing to
the source, tells the operator what to do by hand, and proves around it.
Either way the same checks come first, and without `--go` the command
only shows them and the plan.

`goal: cutover` does not turn `cutover:` on: the goal sets up the stream,
the stream back and the protection of the target, and nothing else. A hop
with the goal and without `cutover:` is told, in `init`, `assess` and
every dry run, the steps it does by hand and what turning `cutover:` on
adds - the timed path, the freeze, the drain, the fenced check, the
counters, the stream back, the undo and the outage measured. What each
goal and mode does on each side is in [goals-and-modes.md](goals-and-modes.md).

Every command takes the hop's name; `--db` narrows one to a database.

## Before the day

1. **Assess both sides.** `migkit assess HOP`. Every `fail` is something
   that stops a cutover partway: a binlog that expires too soon, a slot
   limit, a packet too small for the largest row, another writer on the
   target. Each comes with the value to set.
2. **Rehearse on a copy.** Run the whole runbook against a rehearsal hop
   of the same engines. `migkit move HOP --mode full`, without `--go`,
   shows the plan table by table and how long the move takes, going by
   the rehearsal's own measured rates. The cutover of a rehearsal hop
   records how long each of its steps took; the next cutover of that hop
   is refused when the last one's outage was past the budget.
3. **Protect the target.** With `protect_target: true` in the hop's
   options, the application's roles cannot write to the target until
   cutover. A write that lands there before the flip is one the source
   never saw.
4. **Decide how the cutover runs**, in the hop's options:

   ```yaml
   options:
     protect_target: true
     reverse: at_cutover         # a stream back to the old source at the flip
     cutover:
       budget: 30                # seconds of write outage; past it, undone
       start_below: 1            # seconds the stream may be behind at the start
       freeze: auto              # auto | server | database | operator
       pooler: {kind: pgbouncer, host: 10.0.0.5, port: 6432, user: admin,
                password: CHANGE_ME, databases: [app]}
       flip: ./point-the-app-at-the-target.sh
       flip_undo: ./point-the-app-back.sh
       keys: exact               # exact | apart:<margin>
       jobs: move                # move | leave
       no_return: later          # later | at_flip
     origin_table: keep          # keep | drop, at the point of no return
     approvals: {steps: [cutover, rollback, point-of-no-return], count: 2,
                 signers: /etc/migkit/allowed_signers}
   ```

   `cutover: true` takes every default. A hop that turns it on lets
   migkit write to the source during the cutover - the freeze, the jobs
   switched off, a mark to drain by where the stream keeps no position -
   each step recorded first and undone from the record. With `freeze:
   operator` migkit still writes nothing to the source: it waits for the
   operator's stop and proves it.

   The freeze is one hold per engine (`freeze.Hold`), the way chosen from
   what the source allows, in the order of what each holds - a switch
   for the whole server before the application's accounts:

   | Source | The whole server (where every database on it is the hop's) | The hop's databases alone | Read back |
   |---|---|---|---|
   | PostgreSQL | `default_transaction_read_only` (ALTER SYSTEM), migkit's role excepted | grants revoked, the right to connect taken from owners, sessions ended | no session from before, no role that writes, nothing prepared |
   | MySQL | `super_read_only` (`read_only` on MariaDB), persisted | grants revoked, sessions ended | the switch on, no grant left, no XA or open write |
   | MongoDB | users' writes blocked (6.0), or the server locked | write roles swapped for read, sessions ended | the block on, no account that writes, no open transaction |
   | Redis | every client's writes held (`CLIENT PAUSE WRITE`, 6.2), not with a stream back | every account's writes refused (ACL), from 7.0 | each account refused a write (`ACL DRYRUN`), no connection from before, the count of writes still |
   | SQL Server | - | the database read-only (`WITH ROLLBACK IMMEDIATE`), or the application's users denied writes and their sessions ended | read-only on, or every write denied; no prepared or open write |
   | Cassandra, ScyllaDB | - | MODIFY revoked from the application's roles, the permission cache waited out | no role that writes the keyspace, the cache's time past |
   | Kafka | the topics' writes denied to every principal, and transactional producers kept from committing | the topics' writes denied to every principal | the denies on record, the topics' ends still |
   | OpenSearch | the whole cluster read-only | each index's write block, its writes in flight ended first | every block on, the count of writes still |

   Another source is frozen by its operator (`freeze: operator`); where
   migkit holds no writes of an engine, the preflight says why.

## Moving and following

5. **Copy and follow.** `migkit move HOP --mode full+cdc --go`. The change
   position is taken before the copy starts, so what changes during the
   copy is replayed on top of it. A tail that loses its connection resumes
   from its last saved position. One whose position belongs to another
   source (after a failover, or a rebuilt server) stops, rather than
   skipping what it cannot know it skipped.
6. **Watch it converge.** `migkit watch HOP --verify --delta` checks only
   the rows the log names, each cycle - and a cutover after it compares
   only what changed since its last clean cycle, inside the outage.
   `migkit watch HOP` samples counts and names the tables still being
   written to on the source.
   - Passes when: `SAFE TO CUT OVER` (the source stable for two cycles,
     and the target level with it).
   - Otherwise: `NOT SAFE ... source still HOT` names the tables whose
     writers are still running.

## The cutover

7. **See the plan.** `migkit move HOP --mode cdc --drop` checks, and
   writes nothing:
   - the stream's own position can be read, and the stream is less than
     `start_below` behind it (it is waited for, not estimated) - or,
     where the stream keeps no position migkit can hold it to (a MySQL
     replica without GTIDs, the streaming pipeline, a stream another
     tool runs), that a mark on the source can drain it: a hop with
     `cutover:`, a freeze of migkit's own that its own account writes
     through, and a pair a mark is written and read on (PostgreSQL,
     MySQL, MongoDB)
   - nothing prepared on either side (`pg_prepared_xacts`, `XA
     RECOVER`), and no write transaction on the source older than the
     budget: a prepared transaction commits whenever it is told, after
     any position a drain waited for
   - the schema the same on both sides; no trigger or index a load took
     off the target still off
   - who can write through the freeze: superusers, and on MySQL an
     account whose write privileges are server-wide; and what migkit's own
     account on the source would need to freeze it and give it back
     (ending other sessions, revoking and granting, owning the database)
   - the jobs each side runs, the counters the target has behind the
     source, other consumers reading the source, whether the stream back
     can run (wal_level on the target; binary log and GTIDs, and not a
     newer server replicating to an older one)
   - how long the last cutover of this hop took, against the budget
8. **Get it approved**, where the hop asks. The request carries the plan
   (budget, freeze, the stream back, the flip); a signature counts for
   that plan only, and a changed plan asks again.
9. **Cut over.** `migkit move HOP --mode cdc --drop --go`.

   **With `cutover:`**, one step after another, each recorded before it
   acts, with the clock running from the pooler's pause or the freeze:

   | Step | What happens | Proof |
   |---|---|---|
   | prepare | before the clock: the stream back made ahead and left idle where its engine lets it wait (a subscription made disabled on a slot of its own; a replica whose receiver runs and whose applier waits), and, for a drain by a mark, the mark's table made on the source | the stream back read back as ready (its subscription knows every published table, its slot held; its receiver signed in); a first mark found on the target - otherwise the stream back is made at the flip, or the cutover stops with nothing frozen |
   | jobs off | the jobs the source runs switched off there (pg_cron jobs, MySQL events) | recorded, to be switched on on the target |
   | pause | the pooler paused: the application's writes wait instead of failing | PAUSE returned within the budget |
   | freeze | chosen from what the source allows: the whole server where the hop's databases are all of it and migkit may change its settings (PostgreSQL `default_transaction_read_only` through ALTER SYSTEM, migkit's own role excepted; MySQL `super_read_only`, persisted); always, per database, the application's write grants revoked, the right to connect taken from a role that owns its tables, and every open session of the application ended, so what it had not committed is rolled back rather than committed late | read back: no application session left from before the freeze, no role that can still write, nothing holding a write transaction, nothing prepared, the server switch on |
   | drain | the source's position taken **after** the freeze - on PostgreSQL the WAL insert position, which is where every acknowledged commit is; where the stream keeps no position, a mark written on the source after the freeze; the leg run from here asked to end at the source's position of that moment | the hop's own stream (its slot by name, active or not; the target's executed GTIDs; migkit's tail's saved position) past it; or the mark on the target (in the pipeline's own topic), and then the stream's own offsets: the pipeline's sink applied to the end of every topic, the leg ended |
   | verify | the rows changed since the last verified pass compared, fenced - every table where no pass has run | the two the same, and the source's schema unchanged since the checks |
   | counters | the target's sequences and auto-increments past the source's (`keys: apart:<n>` adds a margin); a restore point `pre-cutover` kept first | the plan's undo recorded |
   | stream | the stream torn down (a pipeline's connectors paused, taken down once the flip is done); with `reverse: at_cutover` the stream back started from the new primary to the old source - made ahead, only started: its slot moved on to the new primary's position and the subscription enabled, or the replica's applier started | a statement a frozen MySQL source refuses (its replication account) is kept for when it is written again |
   | reverse | - | the stream back has reached the old source: its position on the new primary passed |
   | jobs on | the jobs the source ran switched on on the target | recorded |
   | flip | the target given back its writes; the flip command run; the pooler repointed and resumed. The clock stops | the command exited 0 |

   Any failure, or the clock passing the budget, puts every step back,
   the last first: the flip command's undo, the target held again, the
   jobs, the stream back dropped, the stream forward put back (before the
   source takes a write again where its new slot would start from now;
   after, where it starts from the target's own GTIDs), the counters, the
   freeze, the pooler resumed. The source is writable and has what it
   had. A cutover killed half way is undone by the next run of the
   command, which then stops and says so; `migkit doctor` names one left
   that way. Each run, flipped or undone, is recorded with the outage and
   each step's time in `cutover-runs.jsonl`, and every step in the
   changelog with who approved it.

   **Without `cutover:`** the command prints the steps to do by hand:
   switch the source's jobs off; stop the application's writes (revoke
   them and end its sessions - ending them is what rolls back what they
   had not committed); run the command; `migkit check HOP`; `migkit sync
   HOP --go --kind sequences --tag pre-cutover`; switch the jobs on on
   the target; point the application. Run with `--go`, it tears the
   stream down only once nothing is writing to the source - no open
   write transaction, and the server's own count of rows written not
   moving - and the stream has passed where the source stands. Otherwise
   the stream is left up and the command says why. `MIGKIT_CUTOVER=unproven`
   tears it down anyway, as an abandoned migration, and records that it
   did.

## After

10. **The old source stays frozen.** An application still pointed at it
    fails there rather than writing where nobody reads. With `reverse:
    at_cutover` it follows the new primary; `migkit check` on a hop with
    the two sides swapped proves it keeps up.
11. **The point of no return**, a step of its own behind an approval of
    its own (`approvals.steps: [point-of-no-return]`): `migkit move HOP
    --mode cdc --drop` again once the cutover has flipped shows it, and
    `--go` takes it - or at the flip itself, with `no_return: at_flip`,
    both approvals signed before the clock starts. In order, each step
    recorded before it acts:

    | Step | What happens |
    |---|---|
    | tail off | the forward tail, paused at the flip, ended for good |
    | thaw | the old source given back exactly what the freeze took |
    | back off | the stream back dropped: from here nothing the target takes reaches the old source |
    | left | what the cutover left on the old source run or taken away: the statements its frozen state refused, the mark's table |
    | origin | with `origin_table: drop`, migkit's own batch table taken off the target, once no tail of the hop runs |

    Up to the stream back's drop, a failure freezes the old source again
    and going back stays possible; a drop that failed half way leaves
    the old source frozen again and the rest to the next run, which goes
    on from the record; after it, a failure is said and kept. From then
    on `migkit rollback` is refused: nothing carries the target back.
    `origin_table: drop` takes the table away at a `--drop` without
    `cutover:` too, once no tail runs; a hop streaming both ways keeps it.
12. **If it has to go back** (before the point of no return): stop the
    application on the target first.
    `migkit rollback HOP --db DB` shows, and `--apply` does: read the new
    primary's counters as they stand, put the target's own back to the
    `pre-cutover` restore point, wait for the stream back to carry
    everything the target has (on MySQL the restore is itself a
    statement the stream carries), raise the old source's counters past
    every key the new primary handed out (only raise), and give the old
    source back its writes. Then point the application at the old
    source. The stream back still runs until it is dropped; a hop with
    neither `reverse` nor `cutover:` is told that nothing the target took
    after the flip is on the source, and nothing is written there.

A pooler in front (`cutover.pooler`) signs in to the target with what the
source's password hash gave it: the application's accounts have to be on
the target with the same hash, as `migkit users` carries them. An account
made again on the target with the same password is salted anew, and the
pooler's sign-in there is refused after the repoint.

## Measured

In docker, each side a container of its own on one machine, an
application writing one row at a time through the whole cutover and
recording every attempt (`tests/test_the_cutover_on_*.py`). The outage is
the application's own: the longest it went without an acknowledged
write. Every acknowledged row was on the side it wrote to at the end,
once; every step broken on purpose, by an error and by killing migkit at
it, left the source writable and as it was.

| Pair | How the application met the outage | Outage, two runs |
|---|---|---|
| PostgreSQL 16 to 16, native subscription, stream back | writes refused (31, 33) | 2.35 s, 2.50 s |
| the same, PgBouncer 1.25 in front, paused and repointed | writes waited, none refused | 2.80 s, 3.18 s |
| the same, the target paused at the freeze, budget 3 s | undone at 3.73-3.75 s, writes refused and then back on the source | 3.50 s, 3.57 s |
| MySQL 8.4 to 8.4, GTID replica, stream back, `super_read_only` | writes refused (9, 11) | 0.68 s, 0.90 s |
| the same, the server shared with another database, grants revoked | writes refused | 0.63 s, 0.52 s |

## What each step guards against

| Step | Goes wrong without it |
|---|---|
| protect the target | a write on the target the source never saw, overwritten or duplicated |
| watch | a flip while a worker still writes to the source |
| no prepared or long transaction | a commit that lands after the drain's position, on a stream already gone |
| freeze, read back | a session opened before the freeze that commits afterwards |
| drain from after the freeze, by the hop's own stream | an acknowledged commit past the position (asynchronous commit), or another consumer's position taken for the hop's |
| final check | a difference flipped over, with nothing left to compare against |
| counters | a key handed out twice on the first insert after the flip |
| stream back, proven | a rollback that loses what was written after the flip |
| stream back made ahead, its slot moved on at the flip | the making - a connection, a slot's wait for the target's open transactions - spent inside the outage; or what the target took before the flip carried back to the old source |
| a mark where the stream keeps no position | a stream torn down with no proof it had carried what the source committed before the freeze |
| a pipeline's offsets after its mark | a flip while its sink still applies another table's changes, which it applies apart from the mark's |
| old source's counters on rollback | the first insert on the old source taking an id the new primary already used |
| point of no return, approved on its own | an old source left frozen for ever, or given back while a stream back still writes into it, by nobody's decision |
| budget, undo | a half-flipped cutover, or an outage nobody decided to take |
