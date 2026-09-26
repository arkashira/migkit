# Cutover, in order

The moment a migration can go wrong without anyone seeing it is the
cutover: the application moves from the source to the target while
changes are still in flight. Each step below is a command migkit already
has. Each has a check that has to pass before the next step. Each says
what to do when it does not pass. The order matters more than any single
step.

Every command takes the hop's name; `--db` narrows one to a database.

## Before the day

1. **Assess both sides.** `migkit assess HOP`. Every `fail` is something
   that stops a cutover partway: a binlog that expires too soon, a slot
   limit, a packet too small for the largest row, another writer on the
   target. Each comes with the value to set.
2. **Rehearse on a copy.** Run the whole runbook against a rehearsal hop
   of the same engines. `migkit move HOP --mode full`, without `--go`,
   shows the plan table by table and how long the move takes, going by
   the rehearsal's own measured rates. Without a rehearsal it does not
   guess.
3. **Protect the target.** With `protect_target: true` in the hop's
   options, the application's roles cannot write to the target until
   cutover. A write that lands there before the flip is one the source
   never saw.

## Moving and following

4. **Copy and follow.** `migkit move HOP --mode full+cdc --go`. The change
   position is taken before the copy starts, so what changes during the
   copy is replayed on top of it. A tail that loses its connection resumes
   from its last saved position. One whose position belongs to another
   source (after a failover, or a rebuilt server) stops, rather than
   skipping what it cannot know it skipped.
5. **Watch it converge.** `migkit watch HOP --verify --delta` checks only
   the rows the log names, each cycle. `migkit watch HOP` samples counts
   and names the tables still being written to on the source.
   - Passes when: `SAFE TO CUT OVER` (the source stable for two cycles,
     and the target level with it).
   - Otherwise: `NOT SAFE ... source still HOT` names the tables whose
     writers are still running. The application is not stopped yet, or a
     queue or a worker writes on its own.

## The cutover

6. **Stop the application's writes to the source.** This step is outside
   migkit: stop the services, or revoke their write privileges.
7. **Prove the target caught up.** `migkit check HOP`. It waits for the
   change tail, or the native replication, to pass the source's current
   position before it compares (the fence). A difference still in flight
   is reported as such, never as wrong, and never as equal.
   - Passes when: the verdict is `same`.
   - Otherwise: `migkit sync HOP --kind rows` shows the repair plan, and
     `--apply` runs it, with the undo written first. Then check again.
8. **Certify at one instant.** `migkit check HOP --consistent` reads both
   sides at one snapshot each, bounded by the hop's `snapshot_limit`.
9. **Keep a restore point, then carry the counters.**
   `migkit sync HOP --go --kind sequences --tag pre-cutover` records the
   target's state first, then sets every sequence and auto-increment on
   the target past the source's. A target whose counters are behind hands
   out keys the source already used, on the first insert after the flip.
10. **Tear the stream down and give the target its writes.**
    `migkit move HOP --mode cdc --drop --go`. With `reverse: at_cutover`
    in the hop's options, the stream back to the old source starts at this
    moment, so what the application writes on the new primary reaches the
    old one, and a rollback loses nothing. The freeze from step 3 is
    lifted once the stream back runs.
11. **Point the application at the target.** This step is outside migkit:
    the connection strings, DNS, or the service's configuration.

## After

12. **Check again, from the new side.** With `reverse: at_cutover`, the
    old source is now the one following. `migkit check` on a hop with the
    two sides swapped proves it keeps up.
13. **If it has to go back:** `migkit rollback HOP --db DB --state
    pre-cutover` shows what the restore point holds and the row-level undo
    steps, and `--apply` puts the counters back. With the reverse stream,
    going back is pointing the application at the old source again: it
    has every write made since the flip.

## What each step guards against

| Step | Goes wrong without it |
|---|---|
| 3 protect the target | a write on the target the source never saw, overwritten or duplicated |
| 5 watch | a flip while a worker still writes to the source |
| 7 check with the fence | a difference still in flight called equal, or called wrong |
| 8 consistent check | a table compared at two different moments |
| 9 restore point, counters | nothing to go back to once the target has been written; a key handed out twice on the first insert |
| 10 reverse stream | a rollback that loses what was written after the flip |
