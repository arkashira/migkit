"""The cutover as one timed path that undoes itself.

`migkit move HOP --mode cdc --drop --go` has always been the cutover:
tearing the stream down is the moment the application moves. What came
before it was a runbook of steps an operator ran by hand, two of them
outside migkit - stopping the source's writes and pointing the
application at the target - with nothing timing the outage and nothing
undoing a step that failed half way.

A hop that turns `cutover:` on gets the whole path, run back to back
with a clock on it:

    options:
      cutover:
        budget: 30            # seconds of write outage; past it, undone
        start_below: 1        # seconds the stream may be behind at the start
        freeze: auto          # auto | server | database | operator
        pooler: {kind: pgbouncer, host: ..., port: 6432, user: ...,
                 password: ..., databases: [app]}
        flip: <command>       # run at the flip; none = the operator flips
        flip_undo: <command>  # run if the path is undone after the flip ran
        keys: exact           # exact | apart:<margin>
        jobs: move            # move | leave

1. preflight, nothing written: the stream's own position reachable and
   the stream less than `start_below` behind it; nothing prepared or
   long open on either side; the schema the same on both; nothing a load
   set aside left off; who can write through the freeze named; the jobs
   on each side; the counters; the stream back possible; the outage the
   last run of this hop measured against the budget; approvals signed
2. jobs the source runs switched off there (a frozen MySQL server refuses
   the statement afterwards)
3. the pooler paused, where the hop names one: writes wait instead of
   failing - the clock starts
4. the source frozen, chosen from what it allows (`freeze.scope`), and
   read back (`freeze.proof`) - the clock starts here without a pooler
5. the drain: a position taken *after* the freeze, reached by the hop's
   own stream
6. the final check of what changed, fenced, and the source's schema
   unchanged since the preflight
7. the target's counters past every key the source handed out
8. the stream torn down, and where the hop asks (`reverse: at_cutover`)
   the stream back started and proven to reach the old source
9. the jobs switched on on the target
10. the flip: the target given back its writes, the flip command run, the
    pooler repointed and resumed - the clock stops

Every step is written down before it acts. A failure anywhere, or the
clock passing the budget, undoes every step from the record, the last
first, and the source is where it was: writable, with nothing written
to it that it did not have. A run killed half way is undone by the next
one, which then stops and says so - never left half flipped. After the
flip the old source stays frozen, so an application still pointed at it
fails rather than writing where nobody reads; `rollback --apply` gives it
back.

A hop without `cutover:` gets the same preflight, the steps in migkit's
words to do by hand, and the proofs around them: the stream torn down
only once nothing is writing to the source, nothing is prepared there,
and the stream has passed where the source stands. Where one of those
fails the stream is left up; `MIGKIT_CUTOVER=unproven` tears it down
anyway and records that it did.
"""
import json
import os
import secrets
import subprocess
import threading
import time

from . import failpoint

#: in the order they run; each is a failpoint `cutover.<phase>`
PHASES = ("jobs_off", "pause", "freeze", "drain", "verify", "counters",
          "stream", "reverse", "jobs_on", "flip")

DEFAULTS = {"budget": 30, "start_below": 1, "freeze": "auto",
            "keys": "exact", "jobs": "move", "flip": "", "flip_undo": "",
            "pooler": None}

FREEZES = ("auto", "server", "database", "operator")
POOLERS = ("pgbouncer", "pgcat")


def settings(hop):
    """The hop's `cutover:` settings, or None where it does not turn the
    cutover on."""
    raw = (hop.options or {}).get("cutover")
    if raw is None or raw is False:
        return None
    if raw is True:
        raw = {}
    if not isinstance(raw, dict):
        raise SystemExit("cutover: is `true` or the path's settings"
                         f" ({', '.join(DEFAULTS)}), not {raw!r}")
    unknown = sorted(set(raw) - set(DEFAULTS))
    if unknown:
        raise SystemExit(f"cutover: {', '.join(unknown)} - not a setting of"
                         f" the cutover; it has {', '.join(DEFAULTS)}")
    got = {**DEFAULTS, **raw}
    try:
        got["budget"] = float(got["budget"])
        got["start_below"] = float(got["start_below"])
    except (TypeError, ValueError):
        raise SystemExit("cutover: budget and start_below are seconds")
    if got["budget"] <= 0:
        raise SystemExit("cutover: budget is the seconds the write outage"
                         " may last, more than 0")
    if str(got["freeze"]) not in FREEZES:
        raise SystemExit(f"cutover: freeze {got['freeze']} - one of"
                         f" {', '.join(FREEZES)}")
    margin(got)
    if str(got["jobs"]) not in ("move", "leave"):
        raise SystemExit("cutover: jobs is move or leave")
    pooler = got["pooler"]
    if pooler is not None:
        if not isinstance(pooler, dict) or \
                str(pooler.get("kind", "")).lower() not in POOLERS:
            raise SystemExit("cutover: pooler is {kind: pgbouncer or pgcat,"
                             " host, port, user, password, databases}")
    return got


def margin(conf):
    """The gap `keys: apart:<n>` asks between the source's last key and
    the target's first, 0 for `exact`."""
    keys = str(conf.get("keys") or "exact")
    if keys == "exact":
        return 0
    if keys.startswith("apart:") and keys[6:].isdigit() and int(keys[6:]):
        return int(keys[6:])
    raise SystemExit(f"cutover: keys {keys} - exact, or apart:<a whole"
                     " number above 0>")


def _engine(hop):
    from .engines import ALIASES
    return ALIASES.get(hop.engine, hop.engine)


def _reverse(hop):
    return str((hop.options or {}).get("reverse", "") or "").lower() \
        == "at_cutover"


# --- the stream, as the cutover sees it -----------------------------------

class Leg:
    """What carries the hop's changes now, and what the cutover does with
    it. Built by the command (`cli._stream_leg`), which knows the paths.

    kind         "native" (the servers' own replication), "tail" (migkit's
                 own), or what this path does not tear down ("follow",
                 "none"), with `why`
    fence        the engine whose `src_lsn`/`fence_wait` prove the drain
    down(d)      tear the stream down; returns source statements left for
                 when the source is written again
    up(d)        put it back, from where the target stands
    back_up(d), back_down(d), back_fence
                 the stream back (`reverse: at_cutover`) and the engine
                 that proves it reached the old source, or None
    after_thaw   the stream is put back only once the source is writable
                 (it starts from the target's own position, and setting it
                 up writes to the source)
    manual(go)   the teardown a hop without `cutover:` has always had
    """

    def __init__(self, kind, fence=None, down=None, up=None, back_up=None,
                 back_down=None, back_fence=None, after_thaw=False,
                 manual=None, why=""):
        self.kind, self.fence = kind, fence
        self.down, self.up = down, up
        self.back_up, self.back_down = back_up, back_down
        self.back_fence = back_fence
        self.after_thaw = after_thaw
        self.manual = manual
        self.why = why


# --- the record -------------------------------------------------------------

def _path(hop):
    return hop.report_dir() / "cutover.json"


def record(hop):
    """The cutover under way, or None."""
    try:
        return json.loads(_path(hop).read_text())
    except (OSError, ValueError):
        return None


def _save(hop, rec):
    tmp = _path(hop).with_suffix(".tmp")
    tmp.write_text(json.dumps(rec, indent=1, default=str))
    tmp.replace(_path(hop))


def _audit(hop, rec, entry):
    from . import audit
    audit.append(hop.report_dir() / "changelog.jsonl",
                 {"op": "cutover", "run": rec.get("id"),
                  "approved_by": rec.get("approvers") or [], **entry})


def _runs_path(hop):
    return hop.report_dir() / "cutover-runs.jsonl"


def last_run(hop):
    """The last cutover of this hop that ended, flipped or undone, with
    the seconds each phase took - what the next one is predicted from."""
    try:
        lines = _runs_path(hop).read_text().splitlines()
    except OSError:
        return None
    for line in reversed(lines):
        try:
            return json.loads(line)
        except ValueError:
            continue
    return None


def _close(hop, rec, how):
    rec["state"] = how
    rec["ended"] = time.time()
    clock = rec.get("clock") or {}
    if clock.get("began"):
        rec["outage"] = round((clock.get("stopped") or rec["ended"])
                              - clock["began"], 3)
    rec["took"] = {p: round(v["ended"] - v["began"], 3)
                   for p, v in (rec.get("phases") or {}).items()
                   if v.get("ended") and v.get("began")}
    with _runs_path(hop).open("a") as f:
        f.write(json.dumps(rec, default=str) + "\n")
    _path(hop).unlink(missing_ok=True)


# --- running a command at the flip --------------------------------------------

def _run_command(hop, command, timeout, say):
    """The hop's flip command, through the shell, bounded by what is left
    of the budget. Its last line is said without the hop's passwords."""
    from .util import without_secret
    env = {**os.environ, "MIGKIT_HOP": hop.name}
    wait = max(1.0, timeout)
    try:
        got = subprocess.run(["/bin/sh", "-c", command], capture_output=True,
                             text=True, timeout=wait, env=env)
    except subprocess.TimeoutExpired:
        return False, f"still running after {wait:.0f}s"
    out = (got.stdout + got.stderr).strip()
    for secret in (hop.source.password, hop.target.password):
        out = without_secret(out, secret)
    if out:
        say("    " + out.splitlines()[-1][:300])
    return got.returncode == 0, f"exited {got.returncode}"


# --- the pooler ---------------------------------------------------------------

def _pooler_conn(conf):
    import psycopg2
    p = conf["pooler"]
    conn = psycopg2.connect(host=p.get("host", "127.0.0.1"),
                            port=int(p.get("port", 6432)),
                            user=p.get("user"), password=p.get("password"),
                            dbname="pgbouncer", connect_timeout=5)
    conn.autocommit = True
    return conn


def _pooler_dbs(conf, dbs):
    return list((conf["pooler"] or {}).get("databases") or dbs)


def _pooler(conf, command, dbs, timeout):
    """`command` (PAUSE, RESUME, RECONNECT) for each of the pooler's
    databases, each bounded by `timeout`: PAUSE returns only once every
    transaction on its server connections has ended, and one long one
    would hold the cutover past its budget. A PAUSE that did not return is
    left to the undo's RESUME on a console of its own, never issued beside
    it on the same one."""
    conn = _pooler_conn(conf)
    try:
        for name in _pooler_dbs(conf, dbs):
            done = {}

            def ask(name=name):
                try:
                    conn.cursor().execute(f"{command} {name}")
                    done["ok"] = True
                except Exception as e:  # noqa: BLE001 - said below
                    done["error"] = str(e).strip()
            t = threading.Thread(target=ask, daemon=True)
            t.start()
            t.join(max(0.1, timeout))
            if t.is_alive():
                return False, (f"{command} {name} did not return in"
                               f" {timeout:.1f}s")
            error = done.get("error", "")
            # asked to resume what is not paused, or pause what is: the
            # state asked for is the state it is in
            if error and "not paused" not in error \
                    and "already" not in error:
                return False, f"{command} {name}: {error}"
        return True, ""
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001 - a paused console is let go
            pass


def pooler_ready(conf):
    """"" where the pooler's console answers, or why not."""
    try:
        conn = _pooler_conn(conf)
        conn.cursor().execute("show version")
        conn.close()
        return ""
    except Exception as e:  # noqa: BLE001 - said as the reason
        text = str(e).strip()
        return text.splitlines()[0] if text else repr(e)


# --- preflight ------------------------------------------------------------------

def preflight(hop, eng, dbs, leg, conf):
    """[(level, what)] - "fail" stops the cutover before anything is
    written; "warn" is said and does not. Reads only: the lag measure is
    a wait for the stream to pass a position, nothing more.

    Without `cutover:` most of these only inform, since the operator runs
    the steps: what stops the stream's teardown there is what would lose
    rows - a prepared transaction, a stream that has not caught up (said
    by `_manual` right before it tears down)."""
    from . import freeze
    out = []
    gate = "fail" if conf else "warn"

    def add(level, what):
        out.append((level, what))

    budget = conf["budget"] if conf else None
    top = str((hop.options or {}).get("topology", "") or "").lower()
    if top == "two_way" or (hop.options or {}).get("two_way"):
        add("fail", "the hop streams both ways (two_way), and a"
                    " cutover freezes one side: there is no one side to"
                    " move from")
        return out
    if leg.kind not in ("native", "tail"):
        add(gate, leg.why + (" - the cutover path tears down the servers'"
                             " own replication and migkit's tail"
                             if conf else " - nothing holds it to a"
                             " position, so the drain is not proven"))
        return out
    add("ok", "the stream: " + ("the servers' own replication"
                                if leg.kind == "native"
                                else "migkit's change tail"))
    kind = freeze.side_engine(hop, eng, "src")[0]
    how = conf["freeze"] if conf else "operator"
    if conf and how != "operator" and kind not in freeze.FREEZES:
        add("fail", f"a {hop.engine} source is not frozen by migkit:"
                    " `freeze: operator` has the operator stop its writes"
                    " and migkit prove they stopped")
    chosen = None
    if conf and how in ("auto", "server", "database") \
            and kind in freeze.FREEZES:
        chosen, why = freeze.scope(hop, eng, dbs)
        if how == "server" and chosen != "server":
            add("fail", f"freeze: server - {why}")
        elif how == "auto":
            add("ok", "the freeze: " + ("the whole server"
                                        if chosen == "server"
                                        else "per database") + f" ({why})")
        else:
            chosen = how
            add("ok", f"the freeze: {how}")
    for d in dbs:
        out += _position_rows(hop, leg, d, conf, gate)
        for side, name in (("src", "source"), ("dst", "target")):
            try:
                left = freeze.open_work(hop, eng, d, side,
                                        budget if side == "src" else None)
            except Exception as e:  # noqa: BLE001 - said as the reason
                left = [f"open transactions could not be read ({e})"]
            prepared = [x for x in left if x.startswith("prepared")]
            if prepared:
                add("fail", f"{d} {name}: " + "; ".join(prepared[:3])
                    + " - a prepared transaction outlives every session"
                      " and commits whenever it is told, after any"
                      " position a drain waits for")
            long = [x for x in left if not x.startswith("prepared")]
            if long and side == "src":
                add(gate, f"{d} source: " + "; ".join(long[:3])
                    + (f" - open longer than the {budget:g}s budget"
                       if conf else " - still being written"))
        if chosen:
            for line in freeze.cannot(hop, eng, d):
                add("fail", f"{d}: {line}")
            try:
                for line in freeze.bypass(hop, eng, d):
                    add("fail" if "per-database" in line
                        and chosen != "server" else "warn", f"{d}: {line}")
            except Exception as e:  # noqa: BLE001
                add("warn", f"{d}: who can write through the freeze could"
                            f" not be read ({e})")
        out += _shape_rows(hop, eng, d, gate)
        out += _set_aside_rows(hop, d, gate)
        if (hop.options or {}).get("protect_target"):
            if not freeze.state(hop, d):
                add("warn", f"{d}: protect_target is set and nothing holds"
                            " the target: the application may have written"
                            " there")
        else:
            add("warn", f"{d}: the target is not held from the"
                        " application's writes (protect_target); the final"
                        " check is what catches a row written there")
        out += _job_rows(hop, eng, d, conf)
        out += _counter_rows(hop, eng, d, conf, gate)
        out += _slot_rows(hop, eng, d)
        if _reverse(hop):
            out += _reverse_rows(hop, eng, d, gate)
    if conf:
        if conf.get("pooler"):
            why = pooler_ready(conf)
            add("fail" if why else "ok",
                f"the pooler's console: {why}" if why else
                "the pooler answers: writes wait there, not fail")
            out += _pooler_sign_in_rows(hop, eng, dbs)
        if conf.get("flip"):
            add("ok", f"the flip runs: {conf['flip']}")
        else:
            add("warn", "no flip command: the application is pointed at"
                        " the target by hand while the clock runs")
        last = last_run(hop)
        if last and last.get("state") == "flipped" \
                and last.get("outage") is not None:
            add("fail" if last["outage"] > conf["budget"] else "ok",
                f"the last cutover of this hop took {last['outage']:.2f}s"
                f" of outage; the budget is {conf['budget']:g}s")
        else:
            add("warn", "no cutover of this hop has been measured to the"
                        f" end: the {conf['budget']:g}s budget is the only"
                        " bound - rehearse on a copy first")
    return out


def _pooler_sign_in_rows(hop, eng, dbs):
    """Whether the pooler can sign the application in on the target once
    it is pointed there: it signs in with what the source's password hash
    gave it, and a hash the target salted anew refuses that - measured,
    PgBouncer 1.25 in front of PostgreSQL 16: `password authentication
    failed` for the first write after the repoint, the same password on
    both sides."""
    from . import freeze
    kind, src = freeze.side_engine(hop, eng, "src")
    if kind != "postgres":
        return []
    q = ("select rolname||chr(31)||coalesce(rolpassword, '') from pg_authid"
         " where rolcanlogin")
    try:
        a = dict(line.split("\x1f", 1) for line in
                 src._psql("src", dbs[0], q).splitlines() if line)
        b = dict(line.split("\x1f", 1) for line in
                 src._psql("dst", dbs[0], q).splitlines() if line)
        roles = freeze.app_roles(hop, eng, dbs[0], "src")
    except Exception as e:  # noqa: BLE001 - said as the reason
        return [("warn", f"the accounts' password hashes could not be"
                         f" compared ({str(e).strip()[:120]})")]
    differ = [r for r in roles if a.get(r) and b.get(r) != a.get(r)]
    if differ:
        return [("fail", "through the pooler these accounts cannot sign in"
                         " on the target: their password hash is not the"
                         f" source's ({', '.join(differ[:5])}) - carry them"
                         f" with `migkit users {hop.name}`")]
    return [("ok", "the application's accounts sign in on the target as on"
                   " the source (the same password hashes)")]


def _position_rows(hop, leg, d, conf, gate):
    """Whether the stream can be held to a position at all, and how far
    behind it is now: the wait for it to pass where the source stands."""
    from . import freeze
    try:
        pos = leg.fence.src_lsn(d)
    except Exception as e:  # noqa: BLE001 - said as the reason
        return [(gate, f"{d}: the source's position could not be read"
                       f" ({e})")]
    if pos is None:
        return [(gate, f"{d}: the source gives no position this stream can"
                       " be held to, so nothing proves the drain"
                       + (" (GTID is off)" if freeze.side_engine(
                           hop, leg.fence, "src")[0] == "mysql"
                          else ""))]
    wait = conf["start_below"] if conf else 1.0
    began = time.monotonic()
    got = leg.fence.fence_wait(d, pos, timeout=max(wait, 0.5))
    took = time.monotonic() - began
    if got is None:
        return [(gate, f"{d}: no stream of this hop's reads the source, so"
                       " nothing proves the drain")]
    if not got:
        return [(gate, f"{d}: the stream is more than {wait:g}s behind the"
                       " source" + (" (start_below)" if conf else ""))]
    return [("ok", f"{d}: the stream reached where the source stood in"
                   f" {took:.2f}s")]


def _shape_rows(hop, eng, d, gate):
    from . import drift
    if eng.maps_columns():
        return []
    watch = drift.reader(eng)
    dst_reader = getattr(eng, "dst_engine", None) or eng
    if watch is None or not hasattr(dst_reader, "column_catalog"):
        return []
    src = drift.shape(watch, "src", d)
    dst = drift.shape(dst_reader, "dst", d)
    if src is None or dst is None:
        return [("warn", f"{d}: the schema could not be read on both sides")]
    if getattr(eng, "src_engine", None) is not None:
        # across engines the types are named differently: tables only
        missing = sorted(set(src) - set(dst))
        return [(gate, f"{d}: not on the target: {', '.join(missing[:5])}")
                ] if missing else [("ok", f"{d}: every table is on the"
                                          " target")]
    said = drift.changes(dst, src)
    if said:
        return [(gate, f"{d}: the schema differs - " + "; ".join(said[:3])
                 + (" ..." if len(said) > 3 else ""))]
    return [("ok", f"{d}: the schema is the same on both sides")]


def _set_aside_rows(hop, d, gate):
    from .setaside import SetAside
    out = []
    for kind, what in (("dropped-triggers", "triggers"),
                       ("dropped-indexes", "indexes")):
        names = sorted({n for _, _, defs in SetAside(hop, d, kind).records()
                        if defs for n in defs})
        if names:
            out.append((gate, f"{d}: {what} a load took off the target are"
                              f" not back ({', '.join(names[:4])}) - one"
                              " left off after the flip changes nothing and"
                              " says nothing"))
    return out


def _job_rows(hop, eng, d, conf):
    from . import jobs
    out = []
    src = jobs.inventory(hop, eng, d, "src")
    dst = jobs.inventory(hop, eng, d, "dst")
    on_src = [n for n, on in (src or []) if on]
    on_dst = [n for n, on in (dst or []) if on]
    moving = conf and conf["jobs"] == "move"
    if on_src:
        out.append(("ok" if moving else "warn",
                    f"{d}: jobs running on the source:"
                    f" {', '.join(on_src[:5])}"
                    + (" - switched off there before the freeze and on on"
                       " the target at the flip" if moving else
                       " - they move to the target by hand")))
    if on_dst:
        out.append(("warn", f"{d}: jobs already running on the target:"
                            f" {', '.join(on_dst[:5])} - they write there"
                            " before the flip"))
    return out


def _counter_rows(hop, eng, d, conf, gate):
    try:
        plan = eng.repair_plan(d, "sequences")
    except Exception as e:  # noqa: BLE001 - said as the reason
        return [("warn", f"{d}: the counters could not be read ({e})")]
    out = []
    for a in plan:
        if "REFUSED" in (a.note or ""):
            out.append((gate, f"{d}: counters: {a.note} - a key the target"
                              " handed out that the source never did"))
        elif a.statements:
            out.append(("ok" if conf else "warn",
                        f"{d}: {len(a.statements)} counters on the target"
                        " are behind the source"
                        + (" - raised after the drain" if conf else
                           f" - `migkit sync {hop.name} --go --kind"
                           " sequences --tag pre-cutover` after the"
                           " drain")))
    return out


def _slot_rows(hop, eng, d):
    """Other consumers reading the source: after the flip they read a
    server that no longer changes."""
    if _engine(hop) != "postgres" or not hasattr(eng, "own_slots"):
        return []
    mine = set(eng.own_slots(d))
    try:
        got = eng._psql("src", d, "select slot_name from pg_replication_slots"
                                  f" where database = '{d}'"
                                  " and slot_name not like 'migkit\\_delta\\_%'"
                                  " and slot_name not like 'pg\\_%\\_sync\\_%'")
    except Exception:  # noqa: BLE001
        return []
    others = [s for s in got.split() if s not in mine]
    if not others:
        return []
    return [("warn", f"{d}: other consumers read the source"
                     f" ({', '.join(others[:4])}): after the flip they read"
                     " a server that no longer changes - make their slots"
                     " on the target before its writes resume")]


def _reverse_rows(hop, eng, d, gate):
    kind = _engine(hop)
    tdb = hop.target_db(d)
    out = []
    try:
        if kind == "postgres":
            level = eng._psql("dst", d, "show wal_level").strip()
            if level != "logical":
                out.append((gate, f"{tdb}: the stream back needs"
                                  f" wal_level logical on the target, it"
                                  f" is {level}"))
            else:
                out.append(("ok", f"{tdb}: the target can carry a stream"
                                  " back (wal_level logical)"))
        elif kind == "mysql":
            s, t = eng._brands()

            def major(b):
                return tuple(int(x) for x in str(b.version).split("-")[0]
                             .split(".")[:2])
            if s.name == t.name and s.version and t.version \
                    and major(t) > major(s):
                out.append((gate, f"the target ({t.version}) is newer than"
                                  f" the source ({s.version}), and a"
                                  " replica of a newer server is not"
                                  " supported: the stream back cannot run"))
            got = eng._q("dst", "select @@log_bin, @@gtid_mode")
            if not got or int(got[0][0]) != 1 or str(got[0][1]) != "ON":
                out.append((gate, f"{tdb}: the stream back needs the"
                                  " target's binary log and GTIDs on"))
            else:
                out.append(("ok", f"{tdb}: the target can carry a stream"
                                  " back (binary log, GTIDs)"))
    except Exception as e:  # noqa: BLE001
        out.append(("warn", f"{tdb}: whether the stream back can run could"
                            f" not be read ({e})"))
    return out


# --- the plan in words --------------------------------------------------------

def plan(hop, dbs, conf):
    """What the path does, step by step and side by side, as the dry run
    and the approvers see it; for a hop without `cutover:`, what the
    operator does by hand and what migkit proves around it."""
    from .hopkeys import CUTOVER
    back = _reverse(hop)
    name = hop.name
    if not conf:
        # the steps and what turning it on adds, as `hopkeys.CUTOVER`
        # writes them for every place that says them
        return ([
            "without `cutover:` in the hop's options migkit writes nothing"
            " to the source (no goal turns it on); by hand, in this"
            " order:"]
            + [f"  {i}. " + s.format(
                hop=name, back=" and starts the stream back" if back else "")
               for i, s in enumerate(CUTOVER["by hand"], 1)]
            + ["with `cutover:` migkit does all of it, timed, and undoes it"
               " on any failure - it adds:"]
            + [f"  - {a}" for a in CUTOVER["adds"]])
    how = conf["freeze"]
    steps = [f"the cutover, within {conf['budget']:g}s of write outage"
             " (undone past it):"]
    if conf["jobs"] == "move":
        steps.append("  source: the jobs it runs switched off (recorded)")
    if conf.get("pooler"):
        steps.append(f"  pooler: PAUSE {', '.join(_pooler_dbs(conf, dbs))}"
                     " - the clock starts")
    if how == "operator":
        steps.append("  source: nothing written - migkit waits for the"
                     " operator's stop and proves it")
    else:
        steps.append("  source: frozen "
                     + ("for the whole server" if how == "server" else
                        "per database" if how == "database" else
                        "as the server allows")
                     + " - the application's grants revoked (recorded),"
                       " its sessions ended; read back"
                     + ("" if conf.get("pooler") else " - the clock starts"))
    steps += [
        "  source: its position taken after the freeze; the hop's own"
        " stream waited for until it passes it",
        "  both: the rows changed since the last verified pass compared,"
        " fenced; the source's schema compared with the preflight's",
        "  target: counters past the source's"
        + (f", {margin(conf)} apart" if margin(conf) else ""),
        "  target: the stream torn down"
        + ("; the stream back to the old source started, and proven to"
           " reach it" if back else ""),
    ]
    if conf["jobs"] == "move":
        steps.append("  target: the jobs the source ran switched on")
    steps.append("  target: its writes given back"
                 + (f"; then `{conf['flip']}`" if conf.get("flip") else
                    "; then the operator points the application")
                 + ("; the pooler repointed and resumed"
                    if conf.get("pooler") else "") + " - the clock stops")
    steps.append("  after: the old source stays frozen; `migkit rollback"
                 f" {name} --db DB --apply` gives it back")
    return steps


# --- the path -----------------------------------------------------------------

class _Undo(Exception):
    """The path goes back: `why` is said and recorded."""

    def __init__(self, why):
        super().__init__(why)
        self.why = why


def run(hop, eng, dbs, go, leg, say, approve):
    """The cutover: the phased path for a hop with `cutover:`, the proved
    manual one without. `approve(detail)` asks for the hop's approvals of
    this plan and returns who approved."""
    conf = settings(hop)
    rec = record(hop)
    if rec and rec.get("state") == "flipped":
        # stopped between the flip and its record: it had cut over
        _flipped(hop, rec, say)
        return rec
    if rec and rec.get("state") in ("running", "undoing"):
        if not go:
            say(f"[yellow]a cutover of {hop.name} stopped at {_where(rec)}"
                " and was not undone; --go undoes it[/yellow]")
            return None
        say(f"[yellow]a cutover of {hop.name} stopped at {_where(rec)}"
            " without finishing - undoing everything it did[/yellow]")
        began = time.strftime("%F %T", time.localtime(rec.get("began", 0)))
        _undo(hop, eng, rec, leg, rec.get("settings") or conf or DEFAULTS,
              say, f"the run of {began} stopped at {_where(rec)}")
        raise SystemExit("the interrupted cutover was undone: the source is"
                         " writable and has what it had. Nothing was cut"
                         f" over; run `migkit move {hop.name} --mode cdc"
                         " --drop --go` again to cut over.")
    from .movers import DRIVEN
    from .wording import without_programs
    rows = [(lvl, without_programs(what, DRIVEN))
            for lvl, what in preflight(hop, eng, dbs, leg, conf)]
    _say_rows(rows, say)
    for line in plan(hop, dbs, conf):
        say(line)
    failed = [w for lvl, w in rows if lvl == "fail"]
    if not go:
        if not conf and leg.manual is not None:
            leg.manual(False)
        say("\ndry-run, add --go to cut over"
            + (f" ({len(failed)} of the checks above stop it)"
               if failed else ""))
        return None
    unproven = not conf and os.environ.get("MIGKIT_CUTOVER") == "unproven"
    if failed and not unproven:
        raise SystemExit(
            "the cutover did not start, nothing was written: "
            + "; ".join(failed[:4])
            + ("" if conf else
               ". To tear the stream down anyway, as an abandoned"
               " migration rather than a cutover: MIGKIT_CUTOVER=unproven"))
    detail = {"budget": conf["budget"] if conf else None,
              "freeze": conf["freeze"] if conf else "by hand",
              "reverse": _reverse(hop), "flip": (conf or {}).get("flip"),
              "dbs": list(dbs)}
    approvers = approve(detail)
    if not conf:
        return _manual(hop, eng, dbs, leg, say, approvers, failed, unproven)
    return _phased(hop, eng, dbs, leg, conf, say, approvers)


def _say_rows(rows, say):
    mark = {"ok": "[green]ok  [/green]", "warn": "[yellow]warn[/yellow]",
            "fail": "[red]fail[/red]"}
    for lvl, what in rows:
        say(f"  {mark[lvl]} {what}")


def _where(rec):
    began = [p for p in PHASES if (rec.get("phases") or {}).get(p)]
    return began[-1] if began else "the start"


def _manual(hop, eng, dbs, leg, say, approvers, failed, unproven):
    """The hop tears its stream down itself; migkit proves around it.
    `unproven`: MIGKIT_CUTOVER=unproven, which tears it down whatever a
    proof says, and records what the proofs said."""
    from . import audit
    unproven = list(failed) if unproven else None
    for d in dbs:
        got = pos = None
        if leg.fence is not None:
            busy = _quiet(hop, eng, d, 1.5)
            if busy and unproven is not None:
                unproven.append(f"{d} still being written - {busy}")
            elif busy:
                raise SystemExit(f"the stream was left up: {d} is still"
                                 f" being written - {busy}. Stop the"
                                 " application's writes (step 2) and run"
                                 " this again")
            pos = leg.fence.src_lsn(d)
            got = leg.fence.fence_wait(d, pos, timeout=float(
                (hop.options or {}).get("fence_timeout", 300))) \
                if pos is not None else None
            if got is False and unproven is not None:
                unproven.append(f"{d}: the stream short of {pos}")
            elif got is False:
                raise SystemExit(f"the stream was left up: it has not"
                                 f" carried everything the source has ({d}"
                                 f" at {pos}). Tearing it down now would"
                                 " lose that; run this again once it has"
                                 " caught up")
        say(f"  {d}: " + ("the stream passed where the source stands"
                          f" ({pos})" if got else
                          "nothing holds the stream to a position" if got
                          is None else "the stream did NOT reach where the"
                          f" source stands ({pos})"))
        audit.append(hop.report_dir() / "changelog.jsonl",
                     {"op": "cutover", "db": d, "by": "hand",
                      "drained": got, "position": pos,
                      "unproven": unproven or None,
                      "approved_by": approvers or []})
    if unproven:
        say("[yellow]torn down unproven, as MIGKIT_CUTOVER=unproven asks:"
            f" {'; '.join(unproven[:3])}[/yellow]")
    return leg.manual(True)


def _quiet(hop, eng, d, window):
    """"" where nothing writes to the source over `window` seconds, or
    what does: a transaction holding writes when it is looked at, and the
    server's own count of rows written in the database moving between the
    first look and the last. The count is what catches an application
    writing short transactions between two looks."""
    from . import freeze
    end = time.monotonic() + window
    seen = []
    first = freeze.writes_seen(hop, eng, d)
    while True:
        try:
            left = freeze.open_work(hop, eng, d, "src", None)
        except Exception:  # noqa: BLE001 - not readable is not proof
            left = []
        seen += [x for x in left if x not in seen]
        if time.monotonic() >= end:
            break
        time.sleep(0.25)
    last = freeze.writes_seen(hop, eng, d)
    if first is not None and last is not None and last > first:
        seen.insert(0, f"{last - first} rows written in {window:g}s")
    return "; ".join(seen[:3])


def _phased(hop, eng, dbs, leg, conf, say, approvers):
    from . import drift, freeze, jobs
    rec = {"id": secrets.token_hex(6), "began": time.time(), "dbs": dbs,
           "settings": conf, "approvers": approvers, "phases": {},
           "state": "running", "clock": {}, "left_on_source": {},
           "counters": {}, "stream": {}, "flip": {}}
    _save(hop, rec)
    _audit(hop, rec, {"event": "began", "budget": conf["budget"],
                      "dbs": dbs})
    budget = conf["budget"]
    how = conf["freeze"]
    if how == "auto":
        how = freeze.scope(hop, eng, dbs)[0]
    rec["freeze"] = how

    def left():
        clock = rec["clock"]
        if not clock.get("began"):
            return budget
        return budget - ((clock.get("stopped") or time.time())
                         - clock["began"])

    def phase(name):
        rec["phases"][name] = {"began": time.time()}
        _save(hop, rec)
        failpoint.hit(f"cutover.{name}")

    def done(name, **said):
        rec["phases"][name].update({"ended": time.time(), **said})
        _save(hop, rec)
        _audit(hop, rec, {"phase": name, "event": "done",
                          "seconds": round(rec["phases"][name]["ended"]
                                           - rec["phases"][name]["began"],
                                           3), **said})
        if left() < 0:
            raise _Undo(f"the {budget:g}s budget ran out after {name}")

    watch = drift.reader(eng)
    before = {d: drift.shape(watch, "src", d) if watch else None
              for d in dbs}
    try:
        if conf["jobs"] == "move":
            phase("jobs_off")
            n = 0
            for d in dbs:
                running = [nm for nm, on in (jobs.inventory(
                    hop, eng, d, "src") or []) if on]
                n += jobs.switch(hop, eng, d, "src", running, False, say)
            done("jobs_off", switched=n)
        if conf.get("pooler"):
            phase("pause")
            rec["clock"]["began"] = time.time()
            _save(hop, rec)
            ok, why = _pooler(conf, "PAUSE", dbs, left())
            if not ok:
                raise _Undo(f"the pooler did not pause: {why}")
            say("  pooler: paused - writes wait there")
            done("pause")
        phase("freeze")
        if not rec["clock"].get("began"):
            rec["clock"]["began"] = time.time()
            _save(hop, rec)
        if how == "operator":
            _operator_stop(hop, eng, dbs, left, say)
        else:
            freeze.freeze_source(hop, eng, dbs,
                                 lambda m: say(f"  {m}"), how)
            for d in dbs:
                held = freeze.proof(hop, eng, d, how)
                bad = [w for ok, w in held if not ok]
                if bad:
                    raise _Undo(f"{d}: the freeze did not hold - "
                                + "; ".join(bad[:3]))
                say(f"  {d}: the freeze holds - "
                    + "; ".join(w for _, w in held))
        done("freeze", how=how)
        phase("drain")
        at = {}
        for d in dbs:
            pos = _bounded(leg.fence.src_lsn, left(), d)
            if pos is None or pos is False:
                raise _Undo(f"{d}: the source gave no position to drain to"
                            + (" within the budget" if pos is False
                               else ""))
            at[d] = pos
            got = _bounded(leg.fence.fence_wait, left(), d, pos,
                           timeout=max(0.1, left()))
            if not got:
                raise _Undo(f"{d}: the stream did not pass {pos}, where the"
                            " source stood after the freeze"
                            + (", within the budget" if got is False
                               else " - it has no position"))
            say(f"  {d}: the stream passed {pos}, taken after the freeze")
        done("drain", positions=at)
        phase("verify")
        for d in dbs:
            _final_check(hop, eng, d, before.get(d), left, say)
        done("verify")
        phase("counters")
        for d in dbs:
            _counters(hop, eng, d, conf, rec, say)
        done("counters")
        phase("stream")
        rec["stream"] = {"down": [], "back": []}
        _save(hop, rec)
        for d in dbs:
            rec["stream"]["down"].append(d)
            _save(hop, rec)
            leftover = leg.down(d) or []
            if leftover:
                rec["left_on_source"][d] = leftover
                _save(hop, rec)
        if leg.back_up is not None:
            for d in dbs:
                rec["stream"]["back"].append(d)
                _save(hop, rec)
                leg.back_up(d)
        done("stream")
        if leg.back_up is not None:
            phase("reverse")
            rev = leg.back_fence
            for d in dbs:
                tdb = hop.target_db(d)
                pos = _bounded(rev.src_lsn, left(), tdb)
                got = _bounded(rev.fence_wait, left(), tdb, pos,
                               timeout=max(0.1, left())) \
                    if pos else pos
                if not got:
                    raise _Undo(f"{tdb}: the stream back did not reach the"
                                " old source"
                                + (" within the budget" if got is False
                                   else " - nothing proves it"))
                say(f"  {tdb}: the stream back reached the old source"
                    f" ({pos})")
            done("reverse")
        if conf["jobs"] == "move":
            phase("jobs_on")
            n = 0
            moved = [e for e in jobs.recorded(hop) if e["side"] == "src"]
            for d in dbs:
                names = [e["name"] for e in moved if e["db"] == d]
                n += jobs.switch(hop, eng, d, "dst", names, True, say)
            done("jobs_on", switched=n)
        phase("flip")
        if (hop.options or {}).get("protect_target"):
            rec["flip"]["thawed"] = True
            _save(hop, rec)
            for d in dbs:
                freeze.thaw(hop, eng, d, lambda m: say(f"  {m}"))
        if conf.get("flip"):
            rec["flip"]["ran"] = True
            _save(hop, rec)
            ok, why = _run_command(hop, conf["flip"], left(), say)
            if not ok:
                raise _Undo(f"the flip command failed: {why}")
        if conf.get("pooler"):
            for command in ("RECONNECT", "RESUME"):
                ok, why = _pooler(conf, command, dbs, max(1.0, left()))
                if not ok:
                    raise _Undo(f"the pooler did not {command.lower()}:"
                                f" {why}")
        rec["clock"]["stopped"] = time.time()
        rec["phases"]["flip"]["ended"] = time.time()
        rec["state"] = "flipped"
        _save(hop, rec)
    except _Undo as e:
        return _undone(hop, eng, rec, leg, conf, say, e.why)
    except KeyboardInterrupt:
        _undone(hop, eng, rec, leg, conf, say, "interrupted")
        raise
    except (Exception, SystemExit) as e:  # noqa: BLE001 - any failure goes back
        text = str(e).strip()
        return _undone(hop, eng, rec, leg, conf, say,
                       text.splitlines()[0] if text else type(e).__name__)
    return _flipped(hop, rec, say)


def _bounded(fn, within, *args, **kw):
    """`fn(*args, **kw)`, given no longer than `within` seconds (and a
    moment): a server that stops answering holds a call for as long as
    its connection's own timeouts and retries allow, which a clock with a
    budget cannot. What does not return in time is False - it only reads,
    and is let go."""
    got = {}

    def work():
        try:
            got["value"] = fn(*args, **kw)
        except BaseException as e:  # noqa: BLE001 - handed back below
            got["error"] = e
    t = threading.Thread(target=work, daemon=True)
    t.start()
    t.join(max(0.1, within) + 0.5)
    if t.is_alive():
        return False
    if "error" in got:
        raise got["error"]
    return got["value"]


def _operator_stop(hop, eng, dbs, left, say):
    """freeze: operator - migkit writes nothing and waits, within the
    budget, until nothing writes to the source."""
    for d in dbs:
        while True:
            busy = _quiet(hop, eng, d, 1.0)
            if not busy:
                say(f"  {d}: nothing writes to the source, nothing is"
                    " prepared there")
                break
            if left() <= 0:
                raise _Undo(f"{d}: the source was still being written when"
                            f" the budget ran out - {busy}")


def _final_check(hop, eng, d, before, left, say):
    """The rows changed since the last verified pass where a pass has run
    (`watch --verify --delta`), every table otherwise; and the source's
    schema against the preflight's. In a thread, bounded by the budget:
    what is still reading when it runs out is let go, it only reads."""
    from . import drift
    result = {}

    def work():
        try:
            if _has_delta(hop, eng, d):
                got = eng.delta_verify(d)
                result["how"] = ("the rows changed since the last verified"
                                 " pass")
            else:
                got = eng.check_data(d)
                result["how"] = "every table"
            # anything but a clean answer stops it: a flip on a check that
            # did not say "same" is the one mistake here with no undo
            result["bad"] = [r for r in got
                             if r.status not in ("ok", "skip")]
            result["skipped"] = [r.scope for r in got if r.status == "skip"]
        except Exception as e:  # noqa: BLE001 - a check that fails is a stop
            result["error"] = str(e).strip()[:300]
    t = threading.Thread(target=work, daemon=True)
    t.start()
    t.join(max(0.1, left()))
    if t.is_alive():
        raise _Undo(f"{d}: the final check was still reading when the"
                    " budget ran out")
    if "error" in result:
        raise _Undo(f"{d}: the final check failed - {result['error']}")
    if result["bad"]:
        r = result["bad"][0]
        raise _Undo(f"{d}: the final check found a difference - {r.scope}:"
                    f" {r.detail}")
    watch = drift.reader(eng)
    if watch is not None and before is not None:
        moved = drift.changes(before, drift.shape(watch, "src", d))
        if moved:
            raise _Undo(f"{d}: the source's schema changed during the"
                        f" cutover - {'; '.join(moved[:3])}")
    say(f"  {d}: the final check - {result['how']} - found the two the"
        " same" + (f"; not compared: {', '.join(result['skipped'][:4])}"
                   if result.get("skipped") else ""))


def _has_delta(hop, eng, d):
    """Whether a verified pass of `watch --verify --delta` stands to start
    from: only then does the delta check compare what changed, rather
    than record a starting point and compare nothing."""
    if not hasattr(eng, "delta_verify"):
        return False
    if hasattr(eng, "_delta_slot"):
        try:
            return eng._psql("src", d, "select count(*) from"
                                       " pg_replication_slots where"
                                       f" slot_name = '{eng._delta_slot(d)}'"
                             ).strip() == "1"
        except Exception:  # noqa: BLE001
            return False
    where = hop.report_dir(d)
    return any((where / f).exists() for f in (
        "delta-pos.json", "delta-token.json", "pair-delta-token.json"))


def _counters(hop, eng, d, conf, rec, say):
    """The target's counters past the source's, with the undo written
    first; `keys: apart` adds its margin (`SEQUENCE_BUFFER`, which both
    counters' plans read)."""
    gap = margin(conf)
    before = os.environ.get("SEQUENCE_BUFFER")
    try:
        if gap:
            os.environ["SEQUENCE_BUFFER"] = str(gap)
        plan = eng.repair_plan(d, "sequences")
    finally:
        if before is None:
            os.environ.pop("SEQUENCE_BUFFER", None)
        else:
            os.environ["SEQUENCE_BUFFER"] = before
    for a in plan:
        if "REFUSED" in (a.note or ""):
            raise _Undo(f"{d}: counters - {a.note}")
    # the restore point `rollback --state pre-cutover` reads, as the
    # runbook's `sync --go --kind sequences --tag pre-cutover` keeps one
    if hasattr(eng, "snapshot_state"):
        from .state import get_store
        point = get_store(hop).new_point(d, "pre-cutover")
        eng.snapshot_state(d, point.dir, "sequences")
        point.set_meta(op="cutover", db=d)
        point.commit(time.strftime("%F %T"))
    n = 0
    for a in plan:
        if not a.statements:
            continue
        rec["counters"].setdefault(d, []).extend(a.undo)
        _save(hop, rec)
        eng.apply(d, a)
        n += len(a.statements)
    say(f"  {d}: {n} counters on the target raised past the source's"
        + (f", {gap} apart" if gap else ""))


def _flipped(hop, rec, say):
    from . import jobs
    jobs.forget(hop)
    _close(hop, rec, "flipped")
    out = rec.get("outage") or 0.0
    budget = (rec.get("settings") or {}).get("budget")
    _audit(hop, rec, {"event": "flipped", "outage": out,
                      "took": rec.get("took"),
                      "left_on_source": rec.get("left_on_source") or None})
    say(f"[green]cut over: {out:.2f}s of write outage[/green] ("
        + ", ".join(f"{p} {s:.2f}s" for p, s in rec["took"].items()) + ")"
        + (f" - past the {budget:g}s budget, which the flip itself took"
           if budget and out > budget else ""))
    for d, stmts in (rec.get("left_on_source") or {}).items():
        say(f"  {d}: left on the old source until it is written again"
            f" (rollback --apply runs them): {'; '.join(stmts)}")
    say("  the old source stays frozen, so nothing is written to it by"
        f" mistake; `migkit rollback {hop.name} --db DB --apply` gives it"
        " back")
    return rec


def _undone(hop, eng, rec, leg, conf, say, why):
    # a server's own words are kept, the program that carried them is not
    from .movers import DRIVEN
    from .wording import without_programs
    why = without_programs(why, DRIVEN)
    say(f"[red]the cutover goes back: {why}[/red]")
    rec["why"] = why
    _undo(hop, eng, rec, leg, conf, say, why)
    raise SystemExit(f"the cutover was undone ({why}); the source is"
                     " writable and has what it had"
                     + (f" - {rec['outage']:.2f}s of write outage"
                        if rec.get("outage") is not None else ""))


def _undo(hop, eng, rec, leg, conf, say, why):
    """Every recorded step put back, the last first. A step that fails is
    said and the rest still run - the source is given back its writes
    whatever else could not be put back - and what failed is kept for the
    next run to try again."""
    from . import freeze, jobs
    rec["state"] = "undoing"
    _save(hop, rec)
    ph = rec.get("phases") or {}
    dbs = rec.get("dbs") or []
    errors = []

    def s(m):
        say(f"  {m}")

    def step(what, fn, *args):
        from .movers import DRIVEN
        from .wording import without_programs
        try:
            fn(*args)
        except BaseException as e:  # noqa: BLE001 - said, the rest go on
            text = without_programs(str(e).strip(), DRIVEN)[:200]
            errors.append(f"{what}: {text}")
            say(f"  [red]{what} could not be undone: {text}[/red]")

    flip = rec.get("flip") or {}
    if flip.get("ran"):
        if (conf or {}).get("flip_undo"):
            def unflip():
                ok, said = _run_command(hop, conf["flip_undo"], 60, say)
                if not ok:
                    raise RuntimeError(said)
                s("the flip undone")
            step("the flip", unflip)
        else:
            s("[yellow]the flip command ran and the hop gives no"
              " flip_undo: point the application back at the source by"
              " hand[/yellow]")
    if flip.get("thawed"):
        for d in dbs:
            step(f"{d}: the target's hold", freeze.freeze, hop, eng, d, s)
    if "jobs_on" in ph:
        step("the target's jobs", jobs.undo, hop, eng, s, "dst")
    stream = rec.get("stream") or {}
    if leg is not None and leg.back_down is not None:
        for d in stream.get("back", []):
            step(f"{d}: the stream back", leg.back_down, d)
    restore = list(stream.get("down", []))
    if leg is not None and restore and not leg.after_thaw:
        # put back before the source takes writes again: its new slot
        # starts from now, and a write before it would not be carried
        for d in restore:
            step(f"{d}: the stream", leg.up, d)
        restore = []
    for d, stmts in (rec.get("counters") or {}).items():
        if stmts:
            from .engines.base import RepairAction
            step(f"{d}: the target's counters", eng.apply, d,
                 RepairAction(d, "sequences", stmts, [], "cutover undone"))
    if "freeze" in ph:
        step("the source's freeze", freeze.thaw_source, hop, eng, dbs, s)
    if leg is not None:
        for d in restore:
            step(f"{d}: the stream", leg.up, d)
    if "jobs_off" in ph:
        step("the source's jobs", jobs.undo, hop, eng, s, "src")
    if "pause" in ph:
        def resume():
            ok, said = _pooler(conf, "RESUME", dbs, 30)
            if not ok:
                raise RuntimeError(said)
            s("pooler: resumed")
        step("the pooler", resume)
    clock = rec.setdefault("clock", {})
    if clock.get("began") and not clock.get("stopped"):
        clock["stopped"] = time.time()
    if errors:
        rec["undo_error"] = "; ".join(errors)
        _save(hop, rec)
        raise SystemExit("the cutover could not be fully undone: "
                         + "; ".join(errors) + ". What is left is recorded;"
                         f" `migkit move {hop.name} --mode cdc --drop --go`"
                         " tries it again")
    _close(hop, rec, "undone")
    _audit(hop, rec, {"event": "undone", "why": why,
                      "outage": rec.get("outage")})


# --- going back -----------------------------------------------------------------

def back_plan(hop, eng, db, say):
    """Where the hop streamed back (`reverse: at_cutover`) or cut over
    (`cutover:`), going back puts the old source's counters past every key
    the new primary handed out: the stream back carries rows, not
    counters, and the first insert on the old source took an id the new
    primary had already used. Read here, from the new primary as it
    stands - before `rollback` puts the target's own counters back to its
    restore point, which would hide the keys handed out since - and
    applied by `back_apply`. None, and nothing written to the source,
    where the hop has neither option; that is said."""
    from .engines import get_engine
    if not (_reverse(hop) or settings(hop)):
        say("  the old source's counters are left as they are: the hop has"
            " neither a stream back (`reverse: at_cutover`) nor `cutover:`,"
            " so migkit carried nothing the target took after the flip to"
            " the source - rows written on the target since are there"
            " only, and migkit writes nothing to the source")
        return None
    back = hop.reversed()
    rev = get_engine(back)
    tdb = hop.target_db(db)
    plan = rev.repair_plan(tdb, "sequences")
    for a in plan:
        say(f"  old source: {a.note}")
    for x in [x for a in plan for x in a.statements][:10]:
        say(f"    {x}")
    return {"rev": rev, "tdb": tdb, "plan": plan}


def back_apply(hop, eng, db, back, apply, say):
    """`back_plan`'s counters put on the old source (only raised), once the
    stream back has carried everything the target has - the target's own
    counters put back just before are among it, and on MySQL they are a
    statement the stream carries, which applied after the raise lowered
    the old source's counter again (measured on 8.4: the next key there
    was 256 where the new primary's counter stood at 5,000). Then the
    source's freeze, which the cutover left on it, given back, and the
    statements the cutover left for a writable source run."""
    from . import audit, freeze
    rev, tdb, plan = back["rev"], back["tdb"], back["plan"]
    stmts = [x for a in plan for x in a.statements]
    frozen = freeze.source_frozen(hop, hop.databases or [db])
    if not apply:
        if frozen:
            say("  the old source is still frozen by the cutover; --apply"
                " gives it back")
        return stmts
    pos = rev.src_lsn(tdb) if hasattr(rev, "src_lsn") else None
    got = rev.fence_wait(tdb, pos, timeout=float(
        (hop.options or {}).get("fence_timeout", 300))) \
        if pos is not None and hasattr(rev, "fence_wait") else None
    say("  the stream back " + (
        f"has carried everything the target has ({pos})" if got else
        "is not running - the counters are raised from what the target"
        " held" if got is None else
        f"has NOT reached where the target stands ({pos}): stop the"
        " application on the target first, or what it writes now misses"
        " the source"))
    # a MySQL server kept read-only refuses even migkit's statement:
    # given back first there, raised first everywhere else
    first = (frozen and freeze.side_engine(hop, eng, "src")[0] == "mysql"
             and freeze.server_state(hop))
    if first:
        freeze.thaw_source(hop, eng, hop.databases or [db],
                           lambda m: say(f"  {m}"))
    for a in plan:
        if a.statements:
            rev.apply(tdb, a)
    audit.append(hop.report_dir() / "changelog.jsonl",
                 {"op": "rollback-counters", "db": db, "side": "source",
                  "n": len(stmts), "undo": [u for a in plan for u in a.undo],
                  "stream_back": got})
    if stmts:
        say(f"  old source: {len(stmts)} counters raised past the new"
            " primary's")
    if frozen and not first:
        freeze.thaw_source(hop, eng, hop.databases or [db],
                           lambda m: say(f"  {m}"))
    for d, left in _left_on_source(hop).items():
        for x in left:
            try:
                eng.apply_replication_stmt("src", d, x)
            except Exception as e:  # noqa: BLE001 - said, not fatal
                say(f"  [yellow]{d}: {x} - {e}[/yellow]")
    return stmts


def _left_on_source(hop):
    """What the last flipped cutover could not run on the frozen source."""
    run = last_run(hop)
    if not run or run.get("state") != "flipped":
        return {}
    return run.get("left_on_source") or {}


def summary(hop, eng, dbs):
    """(level, what) for `assess` and `doctor`: what the cutover will do
    on each side, or that the operator does it by hand."""
    from . import freeze
    conf = settings(hop)
    back = _reverse(hop)
    if not conf:
        return ("warn", "no `cutover:` in the hop's options (no goal turns"
                        " it on): migkit writes nothing to the source at"
                        " cutover - the operator"
                        " stops its writes and its jobs, and raises the"
                        " counters, by hand, and migkit proves around it;"
                        f" `migkit move {hop.name} --mode cdc --drop` lists"
                        " the steps")
    how = conf["freeze"]
    if how in ("auto", "server", "database") and freeze.side_engine(
            hop, eng, "src")[0] in (
            freeze.FREEZES):
        scope, why = freeze.scope(hop, eng, dbs)
        how = (f"{'the whole server' if scope == 'server' else 'per database'}"
               f" ({why})")
    elif how != "operator":
        return ("fail", f"cutover: a {hop.engine} source is not frozen by"
                        " migkit - `freeze: operator`")
    return ("pass", f"cutover within {conf['budget']:g}s: the source frozen"
                    f" {how if how != 'operator' else 'by the operator, proved by migkit'};"
                    + (" a stream back to the old source" if back else
                       " no stream back - going back after the flip loses"
                       " what the target took")
                    + "; the flip "
                    + (f"runs `{conf['flip']}`" if conf.get("flip")
                       else "is the operator's"))


def said_by_doctor(hop):
    """Lines for `doctor`: a cutover stopped half way, a source the
    cutover left frozen."""
    from . import freeze
    out = []
    rec = record(hop)
    if rec and rec.get("state") in ("running", "undoing"):
        out.append(f"a cutover stopped at {_where(rec)}"
                   + (f" and could not be undone ({rec.get('undo_error')})"
                      if rec.get("undo_error") else "")
                   + f": `migkit move {hop.name} --mode cdc --drop --go`"
                     " undoes it")
    if freeze.source_frozen(hop, hop.databases or []):
        out.append("the source is kept frozen by the cutover (the old"
                   f" primary): `migkit rollback {hop.name} --db DB"
                   " --apply` gives it back")
    return out
