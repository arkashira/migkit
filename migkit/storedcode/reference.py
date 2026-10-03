"""The source's answers kept as the reference once the target is live
(leapfrog research P19, backlog CV.P19).

Before the cutover the source is the oracle: every routine is held to it
by running both (`prove`). After it the application writes to the
target, the source is frozen or gone, and a target's routine can still
change under the application - edited by hand, the target's engine
upgraded, an emulated function (orafce's) changed with its extension.
With the hop option `code_reference_days`, the source's behaviour is kept
as the reference for that many days after the cutover: the calls the
proof made of each function whose answers do not depend on any table's
rows - a function of one expression that reads no table, or a MySQL one
that reads and writes none (`keep.pure`) - with the source's answer to
each (`code-reference.json` beside the reports, the values the proof
called with and the answers rendered in the source's classes; nothing is
kept under `mask: all`). After the cutover, every `check` calls the
target's version with each kept input, read-only, on the hop's own
target, and holds it to the source's kept answer: a call answered
otherwise is `diff`, naming the routine and the input. Past the period
the line says the reference has lapsed. A routine reading rows is not
kept: its answers were over the proof's fixture, not the target's rows.
"""
import json
import re
import time

from . import harness, inputs

#: calls kept per routine
KEEP = 20
#: the period when the option is `true`
DAYS = 30
FILE = "code-reference.json"
_READS = re.compile(r"(?i)\b(from|into|update|delete|insert|nextval|currval|"
                    r"rand|random|now|sysdate|current_\w+|localtime\w*|uuid"
                    r"\w*|newid|getdate|sys_guid|dbms_random)\b")


def days(hop):
    got = (hop.options or {}).get("code_reference_days")
    if got is True:
        return DAYS
    try:
        return max(0, int(got or 0))
    except (TypeError, ValueError):
        return 0


def turned_on(hop):
    return days(hop) > 0


def pure(r, engine):
    """Whether a function's answers are the same over any rows and at any
    time: one expression reading no table and no clock or chance, or a
    MySQL body that reads and writes no table (`keep.pure`)."""
    if r.kind != "function":
        return False
    if engine == "mysql" and r.body:
        from . import keep
        return keep.pure(r) and not _READS.search(r.body or "")
    one = r.settings.get("expression")
    return bool(one) and not _READS.search(one)


def after_cutover(hop):
    """The moment (epoch seconds) the hop's cutover flipped, where the
    target is live now - flipped and not rolled back; else None."""
    from .. import cutover
    last = cutover.last_run(hop, after=True)
    if not last or last.get("state") not in ("flipped", "no-return"):
        return None
    flipped = cutover.last_run(hop)
    at = (flipped or {}).get("ended") or last.get("ended")
    try:
        return float(at)
    except (TypeError, ValueError):
        return None


def record(pair, db, kept, classes):
    """The source's answers to the calls the proof made of each pure
    function (`kept`: {name: [(args, the source's rendered outcome)]}),
    beside the reports. `classes`: {name: (class of what it returns,
    whether a decimal is compared by its value)}."""
    from .. import masking
    hop = pair.hop
    if masking._rules(hop) is True:
        return 0
    out = {}
    for name, calls in kept.items():
        cls, loose = classes.get(name, (None, False))
        out[name] = {"returns": cls, "loose": loose, "calls": [
            {"args": [inputs._dump(a) for a in args],
             "value": o.value,
             "error": list(harness.error_key(o.error) or []) or None}
            for args, o in calls[:KEEP]]}
    if not out:
        return 0
    path = hop.report_dir(db) / FILE
    try:
        path.write_text(json.dumps(
            {"recorded": time.time(), "engine": pair.src_name,
             "routines": out}, indent=1, default=str))
    except OSError:
        return 0
    return len(out)


def check(pair, db):
    """The `code after cutover` line: the target's versions held to the
    source's kept answers, or None where the hop keeps no reference or
    its cutover has not flipped."""
    from ..engines.base import Result
    from .prove import _plain
    from .routines import by_key
    hop = pair.hop
    flipped = after_cutover(hop)
    if not turned_on(hop) or flipped is None:
        return None
    scope = f"{db} code after cutover"
    try:
        data = json.loads((hop.report_dir(db) / FILE).read_text())
    except (OSError, ValueError):
        return Result("schema", scope, "warn", "no reference of the"
                      " source's answers was kept before the cutover: the"
                      " target's routines are held to nothing now", "",
                      "keep the reference before the next cutover (hop"
                      " option code_reference_days, and a check before it)")
    age = (time.time() - flipped) / 86400
    if age > days(hop):
        return Result("schema", scope, "skip", f"the source's answers were"
                      f" the reference for {days(hop)} days after the"
                      f" cutover; {age:.0f} have passed")
    try:
        mine = by_key(pair.dst_engine.code_routines("dst", db))
    except Exception as e:  # noqa: BLE001 - said, as not asked
        return Result("schema", scope, "error", "the target's routines"
                      " could not be read: " + str(e).splitlines()[0][:100])
    try:
        runner = harness.runner(pair.dst_engine, db, {}, side="dst",
                                readonly=True).open()
    except harness.HarnessError as e:
        return Result("schema", scope, "error", f"the target could not be"
                      f" asked: {e}")
    calls, apart, gone = 0, [], []
    try:
        for name, kept in sorted(data.get("routines", {}).items()):
            t = mine.get(("function", name.lower()))
            if t is None:
                gone.append(name)
                continue
            for one in kept.get("calls", []):
                args = [inputs._load(a) for a in one.get("args", [])]
                got = runner.call(t, args, [])
                calls += 1
                value = harness.render(kept.get("returns"),
                                       _plain(got.value, kept.get("loose")))
                error = list(harness.error_key(got.error) or []) or None
                want_error = one.get("error")
                if error != want_error and not (
                        error and want_error and error[0] in
                        harness.UNKNOWN_CLASSES and want_error[0] in
                        harness.UNKNOWN_CLASSES):
                    apart.append(f"{name}({_args(args)}): error {error}"
                                 f" where the source answered"
                                 f" {want_error or 'without one'}")
                    break
                if not error and value != one.get("value"):
                    apart.append(f"{name}({_args(args)}): answers"
                                 f" {_said(value)} where the source answered"
                                 f" {_said(one.get('value'))}")
                    break
    finally:
        runner.close()
    left = f"; {days(hop) - age:.0f} of its {days(hop)} days left"
    if apart or gone:
        return Result("schema", scope, "diff", "; ".join(
            apart[:4] + [f"{g} is no longer on the target" for g in gone[:3]])
            + f" ({calls} calls asked against the source's answers kept"
              f" before the cutover{left})", "",
            "the target's routine changed after the cutover: compare it"
            " with the version migkit proved (schema --convert) and put it"
            " back, or prove the new one again")
    if not calls:
        return Result("schema", scope, "warn", "the reference kept no call"
                      " to ask" + left)
    return Result("schema", scope, "ok", f"{calls} calls of"
                  f" {len(data.get('routines', {}))} routines answered as"
                  f" the source did before the cutover{left}")


def _args(args):
    return ", ".join(repr(a) for a in args)[:80]


def _said(v):
    return "NULL" if v is None else repr(v)
