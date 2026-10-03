"""Whether the target answers the source's own queries as well as the
source does (backlog 24, problems file G1; R23, 4).

Every other check asks whether the two sides hold the same data. A target
can pass all of them and still be slow: an index that did not come over,
statistics the planner has not got yet, a setting that changes its
choices. DMS and DTS do not look. The tools that do - Oracle's SQL
Performance Analyzer, Microsoft's Database Experimentation Assistant -
take the source's real statements, run them on both sides, and compare
plans and times. This does that:

* the statements are the source's own busiest reads and writes, from its
  statement statistics (`pg_stat_statements`, `performance_schema`)
* each is planned on both sides; a table the source reaches through an
  index and the target reads whole is a finding, whatever the timing says
* each read that can be run as it is - no parameters left in it - is run
  on both sides, read-only and under a time limit, after one run to warm
  each side's cache, alternating between them. It is slower only when
  its median is more than twice the source's and more than 20 ms beyond
  it, so noise on a fast query is not a finding

**After a change of engine, or of version** (R23, 4; the conversion
leapfrog's P10): the source's statements are in its own SQL, so each is
translated into the target's by the one translator migkit carries
(`storedcode.translate`), the hop's renames applied (`convert.localizer`),
and planned and timed on each side in its own SQL. A statement the
translator refuses is counted and said, never run in another meaning. A
read run on both sides - across engines, or across two major versions of
one - is held to the same answer too: its rows as values, as a multiset,
since a translated or upgraded query that answers otherwise is the
application answering otherwise; the agreement is weighted by how often
the application runs each.

**Writes** are the source's busiest inserts, updates and deletes. The
source is never written to, so its time is its own statistics' mean. Each
is planned on both sides - an update or delete the source finds through an
index and the target by reading the table whole is a finding, as a read
is - and planning writes nothing. It is timed on the target only where the
target is a rehearsal's own (goal rehearse), inside a transaction that is
rolled back, with a lock wait short enough never to hold up the change
tail; on any other target a check writes nothing.

**Advice before the cutover**: for each table read whole, the index the
source reaches it by and the statement that makes it on the target; the
tables whose statistics the target lacks; and between two servers of one
engine, the planner's settings that differ. Said in the finding's fix and
written as `for-the-target.sql` beside the reports, for a person to run -
migkit runs none of it.

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
#: the most rows of one answer held on each side to compare the two: a
#: read answering more is timed and planned, and its answer not compared
ANSWER_ROWS = 5000


def _sides(eng):
    """(source engine, target engine): a pair's own, the engine itself on
    both sides for a hop of one engine."""
    return (getattr(eng, "src_engine", None) or eng,
            getattr(eng, "dst_engine", None) or eng)


def _across(src, dst):
    return (getattr(src, "CANON_ENGINE", "") or type(src).__name__) != \
        (getattr(dst, "CANON_ENGINE", "") or type(dst).__name__)


def _versions_differ(eng, db):
    """Whether the two servers of one engine run different major versions
    - an upgrade - which may answer a query otherwise: each side's version
    as its server says it (`interlock.identity`)."""
    from . import interlock, upgrade
    hop = getattr(eng, "hop", None)
    if hop is None:
        return False
    try:
        a = interlock.identity(hop, eng, "src", db).get("version")
        b = interlock.identity(hop, eng, "dst", db).get("version")
    except Exception:  # noqa: BLE001 - not known: not held to answers
        return False
    ma, mb = upgrade.major(a), upgrade.major(b)
    return bool(ma and mb) and ma != mb


def _translator(eng, db, src, dst):
    """sql -> the target's SQL, or raises `translate.Refused`; the
    statement as it is where both sides speak one SQL. The hop's renames
    of tables and columns, and the columns it leaves out, are the one
    rewrite every carried statement takes (`statements.localized`)."""
    if not _across(src, dst):
        return lambda sql: sql
    from .storedcode.statements import localized
    from .storedcode.translate import translate
    local = localized(eng, db)
    return lambda sql: translate(sql, src.SQL_DIALECT, dst.SQL_DIALECT,
                                 local)


def _statements(src, db, what, most=TOP):
    """[(label, statement, runnable, source ms or None, calls)] from the
    source's `workload_reads` or `workload_writes`, or a reason (str)."""
    reads = getattr(src, f"workload_{what}", None)
    if reads is None:
        return "this engine cannot read the source's statements"
    got = reads(db, most)
    if isinstance(got, str):
        return got
    out = []
    for r in got or ():
        r = tuple(r) + (None, 1)
        out.append((r[0], r[1], r[2], r[3], int(r[4] or 1)))
    return out


def compare(eng, db):
    """One `Result` for the database: ok, warn with the regressions and the
    advice, or skip saying why nothing was asked."""
    from .engines.base import Result
    src, dst = _sides(eng)
    hop = getattr(eng, "hop", None)
    most = TOP
    if hop is not None:
        from .storedcode import corpus
        if corpus.turned_on(hop):
            # the corpus: every statement on the source's record within
            # the budget, an Oracle source through its shared pool
            src, most = corpus.source(src), corpus.TIMED
    across = _across(src, dst)
    scope = f"{db} performance"
    if across and not (getattr(src, "SQL_DIALECT", None)
                       and getattr(dst, "SQL_DIALECT", None)):
        return Result("deep", scope, "skip",
                      "one side has no SQL for the source's statements to"
                      " be translated into")
    if getattr(dst, "workload_plan", None) is None:
        return Result("deep", scope, "skip",
                      "the target's engine cannot plan the source's"
                      " statements")
    try:
        got = _statements(src, db, "reads", most)
    except Exception as e:  # noqa: BLE001 - said, as the skip's reason
        return Result("deep", scope, "skip",
                      f"the source's statements could not be read:"
                      f" {(str(e).strip().splitlines() or [''])[0][:100]}")
    if isinstance(got, str):
        return Result("deep", scope, "skip", got)
    try:
        writes = _statements(src, db, "writes", most)
    except Exception:  # noqa: BLE001 - the reads are compared still
        writes = []
    if isinstance(writes, str):
        writes = []
    if not got and not writes:
        return Result("deep", scope, "skip",
                      "the source has no reads on record yet to compare")
    to = _translator(eng, db, src, dst)
    answers = across or _versions_differ(eng, db)
    timed_writes = _writes_timed(getattr(eng, "hop", None))
    began = time.time()
    slower, rescanned, refused, differ = [], [], [], []
    #: the translated statements the target planned, for the indexes
    #: measured on its planner (`indexadvice`)
    translated = []
    planned, timed = {"read": 0, "write": 0}, {"read": 0, "write": 0}
    weight = {"alike": 0, "asked": 0}
    for kind, items in (("read", got), ("write", writes)):
        for label, sql, runnable, src_ms, calls in items:
            if time.time() - began > BUDGET_SECONDS:
                break
            try:
                there = to(sql)
            except Exception as e:  # noqa: BLE001 - the translator's words
                refused.append(f"{label}: {e}")
                continue
            plans = _plans(src, dst, db, sql, there)
            if plans is not None:
                planned[kind] += 1
                translated.append((there, calls, kind))
                lost = _read_whole(eng, db, plans, across)
                if lost:
                    rescanned.append((label, lost, plans[0]))
            if not runnable:
                continue
            if kind == "read":
                took = _timed(src, dst, db, sql, there)
                if answers:
                    same = _answered_alike(src, dst, db, sql, there)
                    if same is not None:
                        weight["asked"] += calls
                        if same is True:
                            weight["alike"] += calls
                        else:
                            differ.append(f"{label}: {same}")
            elif timed_writes and src_ms is not None:
                took = _write_timed(dst, db, there, src_ms)
            else:
                took = None
            if took is None:
                continue
            timed[kind] += 1
            a, b = took
            if b > FACTOR * a and b - a > FLOOR_MS:
                slower.append(f"{label}: {b:,.0f} ms on the target against"
                              f" {a:,.0f} ms on the source"
                              + (" (its own average there)" if kind ==
                                 "write" or getattr(src, "RECORDED_TIMES",
                                                    False) else ""))
    if not (sum(planned.values()) or sum(timed.values())):
        return Result("deep", scope, "skip",
                      f"none of the source's {len(got) + len(writes)}"
                      " busiest statements could be planned or run on both"
                      " sides" + (f" ({len(refused)} not translated: "
                                  + "; ".join(refused[:3]) + ")"
                                  if refused else ""))
    asked = (f"{planned['read']} of the source's busiest reads planned on"
             f" both sides, {timed['read']} timed"
             + (f"; {planned['write']} of its busiest writes planned on"
                " both sides, never run on the source"
                + (f", {timed['write']} timed on the target and rolled"
                   " back" if timed["write"] else "")
                if writes else "")
             + (f"; {len(refused)} not translated into the target's SQL"
                if refused else "")
             + (f"; {weight['alike']} of {weight['asked']} runs answered"
                " alike, weighted by how often each is run"
                if weight["asked"] else ""))
    whole = [f"{label}: reads {', '.join(lost)} whole on the target where"
             " the source uses "
             + ", ".join(src_plan.get(t) or "an index alone, without"
                         " reading it" for t in lost)
             for label, lost, src_plan in rescanned]
    found = whole + slower + differ
    if found:
        advice = advise(eng, db, rescanned, translated)
        return Result("deep", scope, "warn",
                      f"{len(found)} of the source's statements are slower"
                      f" or answer otherwise on the target ({asked}): "
                      + "; ".join(found[:6])
                      + (" ..." if len(found) > 6 else "")
                      + (f"; not translated: {'; '.join(refused[:2])}"
                         if refused else ""),
                      advice.get("file", ""),
                      advice.get("said") or
                      "compare the target's indexes and statistics with the"
                      " source's for those tables; `migkit check` names"
                      " what is missing")
    advice = advise(eng, db, [], translated) if _indexes_measured(hop) \
        else {}
    return Result("deep", scope, "ok",
                  f"the target plans and answers the source's statements as"
                  f" well as the source does ({asked})"
                  + (f"; not translated: {'; '.join(refused[:2])}"
                     if refused else "")
                  + (f"; measured to help further: {advice['said']}"
                     if advice.get("said") else ""),
                  advice.get("file", ""))


def _indexes_measured(hop):
    from .storedcode import indexadvice
    return hop is not None and indexadvice.turned_on(hop)


def _plans(src, dst, db, sql, there):
    """[the source's plan, the target's] or None where either refuses."""
    try:
        return [src.workload_plan("src", db, sql),
                dst.workload_plan("dst", db, there)]
    except Exception:  # noqa: BLE001 - a statement the plan refuses
        return None


def _read_whole(eng, db, plans, across):
    """The tables the target reads whole and the source does not: by an
    index there, or not at all - measured on MySQL, `max(k)` over an
    indexed column is answered before execution and its plan names no
    table, while the target without the index read the whole table for
    it. Across engines each side's table is matched by the name the hop
    gives it on the target, in one case."""
    src_plan, dst_plan = plans
    if not across:
        return sorted(t for t, how in dst_plan.items()
                      if how is None and src_plan.get(t, "") is not None)
    hop = eng.hop

    def there(t):
        return str(hop.target_table(db, str(t).split(".")[-1])) \
            .split(".")[-1].lower()
    mine = {there(t): how for t, how in src_plan.items()}
    return sorted(t for t, how in dst_plan.items()
                  if how is None and mine.get(str(t).split(".")[-1].lower(),
                                              "") is not None)


def _timed(src, dst, db, sql, there):
    """(source ms, target ms): medians of alternating runs after a warm-up
    on each side, or None where a run fails."""
    times = {"src": [], "dst": []}
    ask = {"src": (src, sql), "dst": (dst, there)}
    try:
        for side in ("src", "dst"):
            e, text = ask[side]
            e.workload_time(side, db, text)
        for _ in range(RUNS):
            for side in ("src", "dst"):
                e, text = ask[side]
                took = e.workload_time(side, db, text)
                if took is None:
                    return None
                times[side].append(took)
    except Exception:  # noqa: BLE001 - a run that fails is not timed
        return None
    return statistics.median(times["src"]), statistics.median(times["dst"])


def _writes_timed(hop):
    """Whether this hop's check may time the source's writes on its
    target: only a rehearsal's, whose target is its own, and only before
    its cutover - rolled back, it still takes locks the change tail may
    want."""
    if hop is None or (getattr(hop, "goal", "") or "") != "rehearse":
        return False
    from . import cutover
    last = cutover.last_run(hop)
    return not (last and last.get("state") == "flipped")


def _write_timed(dst, db, there, src_ms):
    """(the source's own average ms, the target's median ms) of a write
    run on the target inside a transaction rolled back; None where the
    engine cannot, or a run fails or waits on a lock."""
    run = getattr(dst, "workload_time_write", None)
    if run is None:
        return None
    got = []
    try:
        for _ in range(RUNS):
            took = run("dst", db, there)
            if took is None:
                return None
            got.append(took)
    except Exception:  # noqa: BLE001 - a lock waited on, a refusal
        return None
    return float(src_ms), statistics.median(got)


# --- the same answer ---------------------------------------------------------

def _value(v):
    """One value of an answer as text two engines agree on: a number by its
    digits without trailing zeros, a moment in UTC, bytes as hex, a
    boolean as 1 or 0."""
    import datetime
    import decimal
    if v is None:
        return "\x00NULL"
    if isinstance(v, bool):
        return str(int(v))
    if isinstance(v, int):
        return str(v)
    if isinstance(v, (float, decimal.Decimal)):
        try:
            d = decimal.Decimal(str(v)).normalize()
        except decimal.InvalidOperation:
            return str(v)
        return format(d, "f") if d == d.to_integral_value() or \
            abs(d.as_tuple().exponent) < 30 else str(d)
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).hex()
    if isinstance(v, datetime.datetime):
        if v.tzinfo is not None:
            v = v.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return v.isoformat()
    if isinstance(v, (datetime.date, datetime.time, datetime.timedelta)):
        return str(v)
    return str(v)


def answer(rows):
    """An answer as a multiset of its rows' text: order is left out, since
    two engines sort text by their own collations."""
    from collections import Counter
    return Counter(tuple(_value(v) for v in r) for r in rows)


def _answered_alike(src, dst, db, sql, there):
    """True where both sides answer the read with the same rows, a reason
    in words where they do not, None where it could not be asked or an
    answer is too long to hold (`ANSWER_ROWS`)."""
    rows = getattr(src, "workload_rows", None), \
        getattr(dst, "workload_rows", None)
    if None in rows:
        return None
    try:
        a = src.workload_rows("src", db, sql, ANSWER_ROWS + 1)
        b = dst.workload_rows("dst", db, there, ANSWER_ROWS + 1)
    except Exception:  # noqa: BLE001 - not asked, not a difference
        return None
    if a is None or b is None or len(a) > ANSWER_ROWS \
            or len(b) > ANSWER_ROWS:
        return None
    return alike(a, b)


def alike(a, b):
    """True where two answers hold the same rows, else what differs."""
    if len(a) != len(b):
        return f"{len(b):,} rows on the target against {len(a):,}"
    x, y = answer(a), answer(b)
    if x == y:
        return True
    missing = sum((x - y).values())
    return (f"{missing:,} of its {len(a):,} rows answered otherwise on the"
            " target")


# --- advice ------------------------------------------------------------------

def advise(eng, db, rescanned, translated=()):
    """{"said": the fix in words, "file": where the statements were
    written} for the findings: the index the source reaches each table
    read whole by, as the target makes it; with the hop option
    `index_advice`, the indexes the translated statements `translated`
    ([(the target's SQL, runs, kind)]) are measured on the target's own
    planner to need (`indexadvice`); the tables whose statistics the
    target lacks; the planner's settings that differ between two servers
    of one engine. {} where there is nothing to say."""
    src, dst = _sides(eng)
    hop = getattr(eng, "hop", None)
    statements, said = [], []
    if translated and _indexes_measured(hop):
        from .storedcode import indexadvice
        try:
            made, why = indexadvice.advise(dst, db, list(translated))
        except Exception as e:  # noqa: BLE001 - said, nothing advised
            made, why = [], ["no other index could be measured: "
                             + (str(e).strip().splitlines()
                                or [""])[0][:100]]
        statements += made
        said += [f"measured on the target's planner: {w}" if made else w
                 for w in why]
    for _, lost, src_plan in rescanned:
        for t in lost:
            got = _index_for(eng, db, t, src_plan)
            if got is None:
                continue
            stmt, why = got
            if stmt and stmt not in statements:
                statements.append(stmt)
            elif why and why not in said:
                said.append(why)
    stats = getattr(dst, "_planner_stats", None)
    if stats is not None:
        try:
            r = stats(db)
        except Exception:  # noqa: BLE001 - not asked, not said
            r = None
        if r is not None and r.status not in ("ok", "skip"):
            said.append(f"statistics: {r.detail}"
                        + (f" - {r.fix_hint}" if r.fix_hint else ""))
    if not _across(src, dst):
        said += settings_differ(src, dst, db)
    if not statements and not said:
        return {}
    out = {"said": "; ".join(
        ([f"make on the target the index the source reads by:"
          f" {'; '.join(statements[:4])}"] if statements else []) + said)}
    if statements and hop is not None:
        try:
            path = hop.report_dir(db) / "for-the-target.sql"
            path.write_text(
                "-- the indexes the source reads these tables by and the"
                " target lacks, and those measured on the target's own"
                " planner,\n-- written by migkit's check; migkit runs"
                " none of this\n" + ";\n".join(statements) + ";\n")
            out["file"] = str(path)
        except OSError:
            pass
    return out


def _index_for(eng, db, table, src_plan):
    """(the statement making on the target the index the source reads
    `table` by, or None; or why it cannot be written), or None where the
    source's plan names none."""
    src, dst = _sides(eng)
    across = _across(src, dst)
    hop = eng.hop
    mine = {str(t).split(".")[-1].lower(): (t, how)
            for t, how in src_plan.items()}
    at, how = mine.get(str(table).split(".")[-1].lower(), (None, None))
    if at is None and not across:
        at, how = table, src_plan.get(table)
    if not how or not str(how).startswith("index "):
        return None
    name = str(how)[len("index "):]
    try:
        indexes = src.neutral_indexes("src", db, at)
    except Exception:  # noqa: BLE001 - said as not written
        return None, f"the source's index {name} on {at} could not be read"
    one = next((i for i in indexes if i[0] == name), None)
    if one is None:
        return None
    _, unique, columns, plain = one
    if not plain:
        return None, (f"{at}'s index {name} is over an expression or a part"
                      " of its rows, which is made on the target by hand")
    target = str(hop.target_table(db, *str(at).split("."))) if across \
        else at
    if across:
        target = next((t for t in dst.neutral_tables("dst", db)
                       if str(t).split(".")[-1].lower()
                       == target.split(".")[-1].lower()), target)
    try:
        return dst.neutral_create_index_sql("dst", db, target, name, unique,
                                            columns), ""
    except Exception:  # noqa: BLE001 - said as not written
        return None, f"the index {name} cannot be written for the target"


def settings_differ(src, dst, db):
    """["<setting>: the source's A, the target's B"] for the planner's
    settings (`planner_settings`) two servers of one engine hold apart."""
    ask = getattr(src, "planner_settings", None)
    if ask is None:
        return []
    try:
        a = src.planner_settings("src", db)
        b = dst.planner_settings("dst", db)
    except Exception:  # noqa: BLE001 - not asked, not said
        return []
    out = [f"{k}: the source's {a[k]}, the target's {b.get(k)}"
           for k in sorted(a) if k in b and str(a[k]) != str(b[k])]
    return [f"settings the planner reads differ - {'; '.join(out[:6])}"] \
        if out else []


def gate(hop, results):
    """The regressions `performance: gate` stops `check` on."""
    if str((hop.options or {}).get("performance", "")).lower() != "gate":
        return []
    return [r for r in results
            if str(r.get("scope", "")).endswith(" performance")
            and r.get("status") == "warn"]
