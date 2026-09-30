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
| `two-way` | written: the target's changes carried to it by key, and its own told apart from what migkit wrote there, so nothing goes round | written the same way: the source's changes carried to it | a change made on either side arrives on the other once; two changes to one row settled by the hop's rule (`on_conflict`, `newer_wins`) |
| `keep-in-sync` | read, and its log of changes followed for as long as it runs; nothing else is written to it | a copy kept level with the source, the application kept from writing to it (`protect_target`) | each cycle's changes compared, the stream's lag said, a difference named |

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
| `move --mode cdc --drop` | the cutover: the stream torn down, and what else happens on each side is what `cutover:` says |
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

**On** (`cutover: true`, or its settings - [docs/cutover.md](cutover.md)):
migkit does all of that itself, and adds:

- a timed path: the write outage clocked from the pooler's pause or the
  freeze to the flip, against the hop's budget - past it, all of it is
  undone
- the freeze: migkit stops the source's writes itself - the whole server
  made read-only where it may, or the application's grants revoked and
  its sessions ended - and reads it back
- the drain: the source's position taken after the freeze, and the hop's
  own stream waited for until it passes it
- the fenced check: the rows changed since the last verified pass
  compared inside the outage, and the schema compared with the
  preflight's
- the counters: the target's sequences and auto-increments raised past
  every key the source handed out
- the stream back: started from the new primary to the old source
  (`reverse: at_cutover`) and proven to reach it
- the undo: every step recorded before it acts and put back, the last
  first, on any failure, on the budget passing, or on the next run after
  one killed half way
- the outage measured: each run's outage and each step's time recorded,
  and the next cutover refused where the last one's was past its budget

With it on, the source is written during the cutover only - its jobs
switched off, its writes frozen - each step reversible and behind the
hop's approvals; after the flip it stays frozen until
`migkit rollback HOP --db DB --apply`.

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
