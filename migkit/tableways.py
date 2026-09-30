"""The way each table of a native bulk copy goes, climbed table by table
on what each way measured (backlog F7.dsbulk, F7.rfs, F7.ddbexp).

Cassandra and ScyllaDB, OpenSearch and DynamoDB each copy a table their
own way inside migkit (`native_bulk`: token ranges, a sliced scroll, a
parallel scan) and each has a way of its own servers' or tools' beside it
(the bulk loader, the snapshot reader, the service's export). One logic
chooses for all three: per table, the engine's rungs are climbed on its
facts and on the seconds a row each way took on this engine's earlier
tables and on the hop's earlier runs (`Engine._layered`) - a way never
timed is tried on a table first, then the one measured cheaper - and a
way that cannot run on a table gives it to the next, said, by the one
switch every choice of migkit's goes through (`decide.attempt`). The last
way, migkit's own, has none after it: a table no way carried stops the
copy with every way's reason, never a skip.
"""
import time

#: what a native copy's ways are timed as: seconds a row of a table
WORK = "native table copy, a row"


def _said(choice, facts):
    """Whether the choice is worth a line: more than one way held, or a
    way that is here was passed over for this table. A rung's first need
    is whether it is here at all, and a way this machine lacks is not news
    on every table."""
    if len(choice.ranked) > 1:
        return True
    for r, _ in choice.passed:
        try:
            if r.needs and r.needs[0].holds(facts):
                return True
        except Exception:  # noqa: BLE001 - not read: not said
            continue
    return False


def each(eng, db, tables, rungs, facts_for, run, log):
    """Every table of `tables` by the way climbed for it: `facts_for(t)`
    the facts its rungs read, `run(name, t)` the rows that way copied.
    Returns {table: way name}."""
    from .decide import attempt
    taken = {}
    for t in tables:
        facts = facts_for(t)
        # the table's facts are the ones `measured_ways` reads the choice
        # by; the costs are the engine's for every table alike
        costs = eng._layered(eng._work_key(WORK), rungs, facts,
                             "native way/")

        def one(choice, t=t, facts=facts, costs=costs):
            if log and _said(choice, facts):
                log(f"{t}: {choice.said} - {choice.reason}")
            began = time.monotonic()
            rows = run(choice.rung.name, t)
            costs.saw(choice.rung.name, max(1, int(rows or 0)),
                      time.monotonic() - began)
            return rows
        got, _ = attempt(t, rungs, one, facts, costs=costs, log=log)
        taken[t] = got.path
    return taken
