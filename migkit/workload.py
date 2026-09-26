"""Whether the target answers the source's own queries as well as the
source does (backlog 24, problems file G1).

Every other check asks whether the two sides hold the same data. A target
can pass all of them and still be slow: an index that did not come over,
statistics the planner has not got yet, a setting that changes its
choices. DMS and DTS do not look. The tools that do - Oracle's SQL
Performance Analyzer, Microsoft's Database Experimentation Assistant -
take the source's real statements, run them on both sides, and compare
plans and times. This does that, for the reads:

* the statements are the source's own busiest reads, from its statement
  statistics (`pg_stat_statements`, `performance_schema`)
* each is planned on both sides; a table the source reaches through an
  index and the target reads whole is a finding, whatever the timing says
* each one that can be run as it is - no parameters left in it - is run
  on both sides, read-only and under a time limit, after one run to warm
  each side's cache, alternating between them. It is slower only when
  its median is more than twice the source's and more than 20 ms beyond
  it, so noise on a fast query is not a finding

A finding here is `warn`, never `diff`: `diff` means the two sides do not
hold the same data, and a slow target does. With the hop option
`performance: gate`, a regression stops `check` all the same.

What is shown of a statement is its normalised text, never the literal
values a sample of it was run with.
"""
import statistics
import time

#: how many of the source's reads, and the budget for all of them
TOP = 20
BUDGET_SECONDS = 90
#: a statement is slower when both hold
FACTOR, FLOOR_MS = 2.0, 20.0
RUNS = 3


def compare(eng, db):
    """One `Result` for the database: ok, warn with the regressions, or
    skip saying why nothing was asked."""
    from .engines.base import Result
    reads = getattr(eng, "workload_reads", None)
    if reads is None:
        return Result("deep", f"{db} performance", "skip",
                      "this engine cannot read the source's statements")
    try:
        got = reads(db, TOP)
    except Exception as e:  # noqa: BLE001 - said, as the skip's reason
        return Result("deep", f"{db} performance", "skip",
                      f"the source's statements could not be read:"
                      f" {(str(e).strip().splitlines() or [''])[0][:100]}")
    if isinstance(got, str):
        return Result("deep", f"{db} performance", "skip", got)
    if not got:
        return Result("deep", f"{db} performance", "skip",
                      "the source has no reads on record yet to compare")
    began, slower, rescanned, timed, planned = time.time(), [], [], 0, 0
    for label, sql, runnable in got:
        if time.time() - began > BUDGET_SECONDS:
            break
        try:
            plans = [eng.workload_plan(side, db, sql)
                     for side in ("src", "dst")]
        except Exception:  # noqa: BLE001 - a statement the plan refuses
            plans = [None, None]
        if plans[0] is not None and plans[1] is not None:
            planned += 1
            # a table the target reads whole and the source does not: by
            # an index there, or not at all - measured on MySQL, `max(k)`
            # over an indexed column is answered before execution and its
            # plan names no table, while the target without the index
            # read the whole table for it
            lost = sorted(t for t, how in plans[1].items()
                          if how is None and plans[0].get(t, "") is not None)
            if lost:
                rescanned.append(
                    f"{label}: reads {', '.join(lost)} whole on the target"
                    " where the source uses "
                    + ", ".join(plans[0].get(t) or "an index alone,"
                                " without reading it" for t in lost))
        if not runnable:
            continue
        took = _timed(eng, db, sql)
        if took is None:
            continue
        timed += 1
        src, dst = took
        if dst > FACTOR * src and dst - src > FLOOR_MS:
            slower.append(f"{label}: {dst:,.0f} ms on the target against"
                          f" {src:,.0f} ms on the source")
    if not (planned or timed):
        return Result("deep", f"{db} performance", "skip",
                      f"none of the source's {len(got)} busiest reads could"
                      " be planned or run on both sides")
    asked = (f"{planned} of the source's busiest reads planned on both"
             f" sides, {timed} timed")
    if slower or rescanned:
        return Result("deep", f"{db} performance", "warn",
                      f"{len(slower) + len(rescanned)} reads are slower on"
                      f" the target ({asked}): "
                      + "; ".join((rescanned + slower)[:6])
                      + (" ..." if len(slower) + len(rescanned) > 6 else ""),
                      "", "compare the target's indexes and statistics with"
                          " the source's for those tables; `migkit check`"
                          " names what is missing")
    return Result("deep", f"{db} performance", "ok",
                  f"the target plans and answers the source's reads as well"
                  f" as the source does ({asked})")


def _timed(eng, db, sql):
    """(source ms, target ms): medians of alternating runs after a warm-up
    on each side, or None where a run fails."""
    times = {"src": [], "dst": []}
    try:
        for side in ("src", "dst"):
            eng.workload_time(side, db, sql)
        for _ in range(RUNS):
            for side in ("src", "dst"):
                took = eng.workload_time(side, db, sql)
                if took is None:
                    return None
                times[side].append(took)
    except Exception:  # noqa: BLE001 - a run that fails is not timed
        return None
    return statistics.median(times["src"]), statistics.median(times["dst"])


def gate(hop, results):
    """The regressions `performance: gate` stops `check` on."""
    if str((hop.options or {}).get("performance", "")).lower() != "gate":
        return []
    return [r for r in results
            if str(r.get("scope", "")).endswith(" performance")
            and r.get("status") == "warn"]
