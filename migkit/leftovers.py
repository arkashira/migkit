"""What a mover added to the source and never took away.

Every other check in migkit compares the source with the target. This one
looks at the source on its own and asks a different question: what is in here
that the application did not put there?

Movers are not read-only. They create bookkeeping in the database they are
reading from - a schema to hold checksums, event triggers to capture DDL, a
publication, a replication slot - and several of them do not remove any of it
when the task ends. migkit's own playbook has said so in prose for a long
time: "after cutover remove the leftovers on both sides". Prose is a note to
remember something. This counts it.

Three reasons it matters more than tidiness:

- **A replication slot left behind pins WAL.** The source keeps every segment
  the slot has not consumed, and a source that runs out of disk is an outage
  on the system you already migrated away from.
- **Event triggers fire on DDL.** A trigger the mover installed is still there
  the next time anybody runs a migration on the source, long after the mover
  is gone.
- **It is evidence.** A leftover schema is proof of which mover touched this
  database and when, which matters when two of them ran and only one is
  admitted to.

The patterns are named by the mover that leaves them, so a finding says who
was here rather than just that something is odd. migkit's own artifacts are in
the list too - a tool that reports everyone else's litter and not its own is
not worth reading.
"""

class Named(str):
    """A whole name: the object is called exactly this. For the tools
    whose bookkeeping has a plain word for a name - a schema called
    `repack` - where a prefix or a suffix would also take in an
    application's `repackaging`."""


# (mover, [patterns]) - matched case-insensitively as a prefix or a whole
# name depending on the shape the mover uses. Kept as data so an engine only
# has to say what kind of object it found, never what it means.
SIGNATURES = [
    ("Tencent DTS", ["__tencentdb__", "tencentdb", "dts_",
                     "__dst__tencentdb__sync_"]),
    ("AWS DMS", ["awsdms_", "dms_"]),
    ("Debezium", ["debezium_", "dbz_"]),
    ("Maxwell", ["maxwell"]),
    ("Canal", ["canal_"]),
    # gh-ost and pt-osc wrap the original table name in BOTH a leading
    # underscore and a suffix. Matching the suffix alone would flag an
    # application's own `orders_new` as litter, which is the false positive
    # this module cannot afford. `_ghk` is where a paused gh-ost keeps its
    # checkpoint to resume from (`--checkpoint`)
    ("gh-ost", [("_", "_gho"), ("_", "_ghc"), ("_", "_del"),
                ("_", "_ghk")]),
    ("pt-online-schema-change", [("_", "_new"), "pt_osc_", ("_", "_old")]),
    # its checksums and its schema-change history: `percona.checksums`,
    # `percona.pt_osc_history`
    ("Percona Toolkit", [Named("percona")]),
    # a schema of its own on the source holding the sentinel its change
    # stream is steered by, and a slot of the same name. Named for what it
    # does rather than what it is called: migkit drives it too, and the
    # program is migkit's business (`tests/test_the_report_does_not_name_
    # its_tools.py`); the schema's name is the database's own word
    ("a PostgreSQL bulk copy", [Named("pgcopydb")]),
    # the extension's schema, and its slots `pgl_<db>_<provider>_<sub>`
    ("pglogical", [Named("pglogical"), "pgl_"]),
    # pgEdge's fork of it: `spock` schema and extension, `spk_` slots
    ("Spock", [Named("spock"), "spk_"]),
    # a schema in every database it replicates, holding the deltas its
    # triggers write, and the triggers themselves
    ("Bucardo", [Named("bucardo"), "bucardo_"]),
    # the extension's schema `repack` (a `log_<oid>` table per table being
    # rewritten) and the trigger it puts on that table while it runs
    ("pg_repack", [Named("repack"), Named("pg_repack"),
                   Named("repack_trigger")]),
    # its schema log (`pgstream.schema_log`), its slot and its DDL triggers
    ("pgstream", [Named("pgstream"), "pgstream_"]),
    # the loaders: tables and columns they add to what they load. A source
    # that was once a loader's destination carries them
    ("dlt", ["_dlt_"]),
    ("Sling", ["_sling_"]),
    ("PeerDB", ["_peerdb_", "peerflow_"]),
    ("Airbyte", ["_airbyte_", Named("airbyte_internal")]),
    ("migkit", ["migkit_"]),
]

#: Another tool's state and nothing else - never the application's rows, so
#: never copied, compared or repaired as them (`config.Hop.excluded` asks
#: `bookkeeping`). Stricter than what `whose` names: a finding may be
#: wrong at the cost of a line nobody acts on, an exclusion at the cost of
#: a table nobody compares, which is the worst answer migkit can give. So
#: only whole schema names and the loaders' own tables, and of gh-ost only
#: its changelog and checkpoint - its ghost and deleted tables hold
#: application rows. A column a loader added (`_sling_loaded_at`) is named
#: by `whose` and stays with its table: leaving it out would copy a table
#: into another shape than the source's.
NOT_DATA_SCHEMAS = {"percona": "Percona Toolkit",
                    "pgcopydb": "a PostgreSQL bulk copy",
                    "pglogical": "pglogical", "spock": "Spock",
                    "bucardo": "Bucardo", "repack": "pg_repack",
                    "pgstream": "pgstream",
                    "airbyte_internal": "Airbyte",
                    "_peerdb_internal": "PeerDB"}
#: (provider, the table shape it writes on the TARGET, what the table is):
#: a managed transfer service's own tables on the side it writes, as its
#: documentation and the field name them - never the operator's rows, so
#: left out of the scope, the counts, the check, a copy back and a repair
#: (`Hop.excluded` through `bookkeeping`), and said in one line
#: (`provider_table`). Tencent DTS's sync-back table was read as data in
#: a MySQL -> PostgreSQL run behind it (`__dst__tencentdb__sync_<id>`);
#: AWS DMS keeps its control tables under `awsdms_` on the target
PROVIDER_TABLES = [
    ("Tencent DTS", "__dst__tencentdb__sync_", "sync-back table"),
    ("AWS DMS", Named("awsdms_apply_exceptions"), "control table"),
    ("AWS DMS", Named("awsdms_status"), "control table"),
    ("AWS DMS", Named("awsdms_suspended_tables"), "control table"),
    ("AWS DMS", Named("awsdms_history"), "control table"),
    ("AWS DMS", Named("awsdms_validation_failures_v1"), "control table"),
]
#: managed transfer services whose own tables on the target are not
#: known here from their documentation (the shapes could not be read
#: from it when this was written): never guessed, so a table of theirs is
#: compared as data, and the check says it did not look (`not_checked`)
PROVIDERS_UNVERIFIED = ("Alibaba Cloud DTS", "Huawei Cloud DRS",
                        "Google Database Migration Service")
NOT_DATA_TABLES = [(mover, pat) for mover, pat, _ in PROVIDER_TABLES] + [
    ("gh-ost", ("_", "_ghc")), ("gh-ost", ("_", "_ghk")),
    ("dlt", Named("_dlt_loads")), ("dlt", Named("_dlt_pipeline_state")),
    ("dlt", Named("_dlt_version")),
    ("Airbyte", "_airbyte_raw_"), ("Airbyte", "_airbyte_tmp_"),
    ("PeerDB", "_peerdb_raw_"),
]


def _matches(low, pat):
    if isinstance(pat, Named):
        return low == pat
    if isinstance(pat, tuple):
        # both ends must match, so the original table name is wrapped
        pre, suf = pat
        return (low.startswith(pre) and low.endswith(suf)
                and len(low) > len(pre) + len(suf))
    return low.startswith(pat) or low.endswith(pat) or low == pat


def whose(name):
    """Which mover leaves an object with this name, or '' if none known."""
    low = str(name).lower()
    for mover, pats in SIGNATURES:
        if any(_matches(low, pat) for pat in pats):
            return mover
    return ""


def bookkeeping(*parts):
    """The tool whose own state the object named by `parts` is, or ''.

    `parts` as `Hop.excluded` takes them: a database alone, `(db, table)`,
    `(db, schema, table)`. A schema in the middle is matched whole; so is a
    database alone - the one a hop lists its databases through - but never
    the database a hop names and then asks a table of: the operator chose
    it. The last part of two or more is the table."""
    parts = [str(p).lower() for p in parts if p not in (None, "")]
    if not parts:
        return ""
    for name in (parts if len(parts) == 1 else parts[1:-1]):
        if name in NOT_DATA_SCHEMAS:
            return NOT_DATA_SCHEMAS[name]
    if len(parts) > 1:
        for mover, pat in NOT_DATA_TABLES:
            if _matches(parts[-1], pat):
                return mover
    return ""


def pg_dump_args():
    """The dump program's switches that leave another tool's schemas out
    whole (`-N`; one that matches nothing is no error): a dump of the
    whole database carried them to the target. Its tables elsewhere
    (`_dlt_loads`) are left out with the hop's own exclusions, wherever a
    move lists the source's tables (`movers.excluded_tables`)."""
    return [a for name in sorted(NOT_DATA_SCHEMAS) for a in ("-N", name)]


#: kinds whose name is qualified (`schema.table`, `schema.table.column`)
#: and matched by its last part
QUALIFIED = ("table", "column", "collection")


def group(found):
    """{mover: [labels]} from (kind, name) pairs, keeping only known movers.

    An unrecognised object is not reported. The source belongs to the
    application, and guessing that an unfamiliar schema must be litter would
    turn this from a finding into noise - the one thing a check nobody asked
    for cannot afford to be.
    """
    out = {}
    for kind, name in found:
        mover = whose(str(name).rsplit(".", 1)[-1] if kind in QUALIFIED
                      else name)
        if mover:
            out.setdefault(mover, []).append(f"{kind} {name}")
    return out


def describe(by_mover, show=4):
    parts = []
    for mover, items in sorted(by_mover.items()):
        items = sorted(set(items))
        shown = ", ".join(items[:show])
        if len(items) > show:
            shown += f" and {len(items) - show} more"
        parts.append(f"{mover}: {shown}")
    return "; ".join(parts)


def total(by_mover):
    return sum(len(set(v)) for v in by_mover.values())


# Leftovers that cost something while they sit there, as opposed to merely
# being untidy. Named by object kind, because every engine calls the object
# the same thing even when it spells the query differently.
COSTLY = {
    "slot": "pins WAL on the source until it is dropped - the source can run"
            " out of disk",
    "publication": "keeps the source publishing changes nobody reads",
    "event trigger": "still fires on the next DDL anyone runs on the source",
}


def urgent(by_mover):
    """The subset that is doing harm now, not just sitting there."""
    out = []
    # longest kind first: an object kind can be two words ("event trigger"),
    # and splitting on the first space matched "event" against nothing
    kinds = sorted(COSTLY, key=len, reverse=True)
    for items in by_mover.values():
        for label in set(items):
            for kind in kinds:
                if label.startswith(kind + " "):
                    out.append((label, COSTLY[kind]))
                    break
    return sorted(out)


def provider_table(name):
    """The line said for a table a managed transfer service writes on the
    target (`PROVIDER_TABLES`), or None where `name` is not one."""
    low = str(name).rsplit(".", 1)[-1].lower()
    for mover, pat, what in PROVIDER_TABLES:
        if _matches(low, pat):
            return (f"{name}: the provider's {what} ({mover}), not your"
                    " data: left out of scope, counts and the check")
    return None


def provider_tables(names):
    """The lines for those of `names` a provider writes (`provider_table`),
    in name order."""
    return [got for got in (provider_table(n) for n in sorted(names or ()))
            if got]


def not_your_data(names):
    """What the scope listing, the check and the report say a target
    holding `names` leaves out as a provider's own: the provider's tables
    among them (`provider_tables`). Only a table told apart is left out;
    a provider whose tables are not known here is said by
    `providers_untold`, not here, because its tables are not left out."""
    return provider_tables(names)


def providers_untold():
    """One line per provider whose sync-back and control tables are not
    known here by name (`PROVIDERS_UNVERIFIED`): a table of theirs on the
    target is not told apart from the user's own, so it is counted and
    checked as the user's data, not left out. Said beside the listing, and
    not a narrowing of the check: nothing the run set out to compare was
    skipped on their account."""
    return [f"a sync-back or control table of {p} is not told apart from"
            " your own here: one there is counted and checked as yours"
            for p in PROVIDERS_UNVERIFIED]


def is_replication_artifact(name, schema=None):
    if schema and (bookkeeping(schema) or whose(schema)):
        return True
    return bool(whose(name) or bookkeeping(name))


def artifact_origin(name, schema=None):
    if schema:
        origin = bookkeeping(schema) or whose(schema)
        if origin:
            return origin
    return whose(name) or bookkeeping(name)


# --- writes held and not given back -------------------------------------------

def _gone(pid):
    """True where the process that last wrote a hold's record no longer
    runs, False where it does, None where the record names none."""
    import os
    if not isinstance(pid, int) or pid <= 0:
        return None
    if pid == os.getpid():
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except OSError:
        return False
    return False


def holds(hop, dbs=None):
    """[{side, db, way, since, pid, gone}] of every hold of writes migkit
    recorded on a side of the hop and has not given back: the cutover's
    freeze of the source, a target kept from the application. `gone` is
    whether the process that wrote the record has died - a hold it left
    behind - or None where the record names no process. A target whose
    writes are only named (`freeze.UNNAMED`, `freeze.NAMED`) holds
    nothing and is not listed."""
    from . import freeze
    named = freeze.NAMED + (freeze.UNNAMED,)
    out = []
    for side in ("src", "dst"):
        rec = freeze.server_state(hop, side)
        if rec and rec.get("way") not in named:
            out.append({"side": side, "db": "", "way": rec.get("way"),
                        "since": rec.get("since"), "pid": rec.get("pid"),
                        "gone": _gone(rec.get("pid"))})
        for d in (dbs if dbs is not None else hop.databases or []):
            rec = freeze.state(hop, d, side)
            if rec and rec.get("way") not in named:
                out.append({"side": side, "db": d, "way": rec.get("way"),
                            "since": rec.get("since"),
                            "pid": rec.get("pid"), "gone": _gone(
                                rec.get("pid"))})
    return out


def _age(since, now):
    """Seconds since a record's `since` (local time, as `freeze` writes
    it), None where it cannot be read."""
    import time
    try:
        return now - time.mktime(time.strptime(str(since),
                                               "%Y-%m-%d %H:%M:%S"))
    except (TypeError, ValueError, OverflowError):
        return None


def longest_step(hop):
    """Seconds the longest step of the hop's last kept run took
    (`autonomous.Workflow`), None where none was kept."""
    from . import durable
    path = hop.report_dir() / "workflow.json"
    try:
        kept = durable.read_state(path, "state") if path.exists() else None
    except Exception:  # noqa: BLE001 - no run kept, nothing measured
        return None
    took = [u.get("took") for u in ((kept or {}).get("units") or {}).values()
            if isinstance(u, dict) and isinstance(u.get("took"),
                                                  (int, float))]
    return max(took) if took else None


def decided(hop, dbs=None, now=None):
    """[(hold, `decide.lift_hold` Choice)] of every hold `holds` finds."""
    import time
    from . import decide
    now = time.time() if now is None else now
    step = longest_step(hop)
    return [(h, decide.lift_hold({
        "holder gone": h["gone"], "age": _age(h["since"], now),
        "longest step": step, "side": h["side"]})) for h in holds(hop, dbs)]


def lift(hop, eng, say, dbs=None, force=False):
    """Every hold `holds` finds left behind given back, exactly as its
    record took it (`freeze.thaw_source`, `freeze.thaw`). A hold whose
    process still runs, or whose record names none, is left as it is
    unless `force`: it may be a cutover in progress - the choice is
    `decide.lift_hold`'s (`decided`). Returns the holds
    lifted and those left, each as `holds` lists them."""
    from . import freeze
    chosen = decided(hop, dbs)
    lifted = [h for h, c in chosen if force or c.path == "lift"]
    left = [h for h, c in chosen if not (force or c.path == "lift")]
    src = [h["db"] for h in lifted if h["side"] == "src" and h["db"]]
    if any(h["side"] == "src" for h in lifted):
        freeze.thaw_source(hop, eng, src, say)
    for h in lifted:
        if h["side"] == "dst" and h["db"]:
            freeze.thaw(hop, eng, h["db"], say, side="dst")
    return lifted, left


# --- dumps a killed run left ---------------------------------------------------

def dumps(hop, dbs=None, now=None):
    """[{path, owner, gone, age}] of every dump directory of a driven
    program (`evidence.PROGRAM_DIRS`) under the hop's report directory and
    its `at_rest.scratch`, for `dbs` (the hop's databases where not given,
    and every database found under the scratch). `owner` is the process
    recorded beside it (`evidence.owner_file`), `gone` whether it has died
    (None where none is recorded), `age` seconds since it was written."""
    import pathlib
    import time
    from . import evidence
    now = time.time() if now is None else now
    names = list(dbs if dbs is not None else hop.databases or [])
    bases = []
    for d in names or [None]:
        try:
            bases.append(pathlib.Path(hop.report_dir(d)))
        except Exception:  # noqa: BLE001 - no such directory: no dump in it
            continue
    where = evidence.scratch_of(hop)
    if where:
        top = pathlib.Path(where).expanduser() / str(hop.name)
        if names:
            bases += [top / str(d) for d in names]
        elif top.is_dir():
            bases += [p for p in top.iterdir() if p.is_dir()]
    out, seen = [], set()
    for base in bases:
        try:
            entries = sorted(base.iterdir()) if base.is_dir() else []
        except OSError:
            continue
        for p in entries:
            if p in seen or not p.is_dir() or not (
                    p.name in evidence.PROGRAM_DIRS
                    or p.name.startswith(evidence.PROGRAM_PREFIX)):
                continue
            seen.add(p)
            try:
                pid = int(evidence.owner_file(p).read_text().strip())
            except (OSError, ValueError):
                pid = None
            try:
                age = now - p.stat().st_mtime
            except OSError:
                age = None
            out.append({"path": p, "owner": pid, "gone": _gone(pid),
                        "age": age})
    return out


def clear_dumps(hop, dbs=None, say=None, now=None):
    """Every dump `dumps` finds removed where `decide.leftover_dump` says
    its owner is gone (or, with none recorded, that it is older than any
    step a run took), each with its owner record; one a run may still be
    writing is left, with why. Returns (removed, kept), each as `dumps`
    lists them. Nothing is removed for a hop with a run in progress."""
    import shutil
    from . import decide, evidence
    step = longest_step(hop)
    removed, kept = [], []
    for d in dumps(hop, dbs, now):
        c = decide.leftover_dump({"owner gone": d["gone"], "age": d["age"],
                                  "longest step": step})
        if c.path == "remove":
            shutil.rmtree(d["path"], ignore_errors=True)
            evidence.owner_file(d["path"]).unlink(missing_ok=True)
            removed.append(d)
            if say:
                say(f"removed the dump {d['path']} a killed run left")
        else:
            kept.append(d)
            if say:
                say(f"left the dump {d['path']}: {c.reason}")
    return removed, kept


# --- a target's setting a run raised -----------------------------------------

def _settings_file(hop):
    return hop.report_dir() / "target-settings.json"


def settings_left(hop):
    """[{name, old, new, db, pid, at}] of every setting a run changed on
    the target and has not put back - a run killed before its end leaves
    its entry (`settings_held`)."""
    import json
    try:
        got = json.loads(_settings_file(hop).read_text())
    except (OSError, ValueError):
        return []
    return [e for e in got if isinstance(e, dict) and e.get("name")]


def _keep_settings(hop, entries):
    import json
    import os
    path = _settings_file(hop)
    if not entries:
        path.unlink(missing_ok=True)
        return
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as fh:
        json.dump(entries, fh, indent=2)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def put_back_settings(hop, eng, say=None):
    """Every setting `settings_left` lists put back on the target to the
    value it had before the run, each entry dropped only once the server
    took it back; one the server would not take back is kept and said, so
    the next start and `leftovers` try again. Returns the entries put
    back."""
    back, kept = [], []
    for e in settings_left(hop):
        try:
            eng.set_target_setting(e["name"], e["old"])
        except Exception as x:  # noqa: BLE001 - kept for the next try
            kept.append(e)
            if say:
                say(f"the target's {e['name']} is still {e['new']}: it could"
                    f" not be put back to {e['old']} ({str(x)[-90:]}) -"
                    " migkit leftovers tries again")
            continue
        back.append(e)
        if say:
            say(f"put the target's {e['name']} back to {e['old']}")
    _keep_settings(hop, kept)
    return back


import contextlib as _contextlib  # noqa: E402 - the one block below


@_contextlib.contextmanager
def settings_held(hop, eng, db, say=None):
    """The target's settings a run needs (`eng.settings_for_run(db)`, each
    {name, old, new} chosen by its rung) set for the block and put back
    after. What a killed run left is put back first; each entry is written
    to disk before the server is asked, so a run killed between the two
    leaves an entry the next start puts back (`put_back_settings`)."""
    want = getattr(eng, "settings_for_run", None)
    if settings_left(hop):
        put_back_settings(hop, eng, say)
    if settings_left(hop):
        raise SystemExit("a setting an earlier run changed on the target"
                         " could not be put back; nothing more is changed"
                         " there until it is - migkit leftovers says which")
    import os
    import time
    entries = []
    try:
        for s in (want(db) if want else []):
            entries.append({"name": s["name"], "old": s["old"],
                            "new": s["new"], "db": db, "pid": os.getpid(),
                            "at": time.strftime("%Y-%m-%d %H:%M:%S")})
            _keep_settings(hop, entries)
            eng.set_target_setting(s["name"], s["new"])
            if say:
                say(f"{db}: the target's {s['name']} raised from {s['old']:,}"
                    f" to {s['new']:,} for this run: {s.get('why', '')}")
        yield entries
    finally:
        if entries:
            put_back_settings(hop, eng, say)
