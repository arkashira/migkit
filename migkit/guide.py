"""`migkit init` as the guide (backlog R21): two addresses and the
passwords, and migkit does the rest.

It asks where the data is and where it goes - an address each, or a file
another tool describes the task in (`migkit.imports`) - and the
passwords, which it keeps in this machine's keychain or leaves in
environment variables, as the operator chooses; nothing else. Then it
connects and says what it found, in plain words: each side's software and
version, who runs it (`migkit.clouds`), a primary or a replica, the
databases and their size, the tables without a key, the types that need
care across two engines, whether the source's changes can be followed,
and what the user may and may not do (`migkit.dba`). It proposes what the
hop is for - a target already holding the source's tables is proved
equal, a source whose changes can be followed is cut over with the least
downtime, any other is copied - with what migkit will do and how long it
may take, writes the script for the DBA where something is missing,
writes the smallest configuration that says it (`hopkeys.smallest`), and
says the next command.

Without a terminal it asks nothing: `MIGKIT_SOURCE` and `MIGKIT_TARGET`
are the two addresses, `MIGKIT_SOURCE_PASSWORD` and
`MIGKIT_TARGET_PASSWORD` the passwords (referenced as those variables in
the configuration, or kept in the keychain with
`MIGKIT_SECRETS=keychain`), `MIGKIT_GOAL` and `MIGKIT_HOP` the goal and
the hop's name where migkit's own choice is not wanted. Without either,
it writes the template, as it always has.

Measured, a PostgreSQL to MySQL hop from nothing in a terminal
(`tests/test_init_is_the_guide.py`): 5 questions (two addresses, two
passwords, where to keep them) and a hop of 4 lines (its name, the goal,
two addresses); two files are 2 questions and 3 lines. The template
`init` wrote before was a hop of 16 lines and asked the operator to know
the engine, the ports, the playbook and a worker count.
"""
import os
import re
from pathlib import Path

#: what the guide says it is asking, word for word
ASK_SOURCE = ("Where is the data now? An address like"
              " postgres://app@db1:5432/shop, or a task file from another"
              " tool")
ASK_TARGET = "Where should it go? An address like mysql://app@db2/shop"
ASK_KEEP = ("Keep the passwords in this machine's keychain (k), or read"
            " them from environment variables (e)?")


def _sign_in(engine):
    from .config import _NO_KEYCHAIN
    return engine not in _NO_KEYCHAIN


def _hop_name(raw, taken):
    from . import addresses
    base = ""
    for side in ("source", "target"):
        got = raw.get(side)
        if addresses.is_address(got):
            _, ep, dbs = addresses.parse(got, side)
            base = (dbs[0] if dbs else "") or base
            if not base and ep.get("path"):
                base = Path(str(ep["path"])).stem
            if not base:
                base = str(ep.get("host") or "").split(".")[0]
        if base:
            break
    base = re.sub(r"[^a-z0-9-]+", "-", base.lower()).strip("-") or "my-hop"
    name, n = base, 2
    while name in taken:
        name, n = f"{base}-{n}", n + 1
    return name


def _env_var(name, side):
    return re.sub(r"[^A-Z0-9]+", "_",
                  f"MIGKIT_{name}_{side}_PASSWORD".upper())


def _with_reference(text, ref):
    """An address with `ref` in its password's place."""
    scheme, _, rest = text.partition("://")
    authority, slash, path = rest.partition("/")
    user, at, host = authority.rpartition("@")
    user = user.split(":", 1)[0]
    return f"{scheme}://{user}:{ref}@{host}{slash}{path}"


def _has_password(raw_side):
    from . import addresses
    if addresses.is_address(raw_side):
        return bool(addresses.parse(raw_side)[1].get("password"))
    return isinstance(raw_side, dict) and bool(raw_side.get("password"))


def keychain_usable():
    try:
        import keyring
        from keyring.backends.fail import Keyring as Fail
        return not isinstance(keyring.get_keyring(), Fail)
    except Exception:  # noqa: BLE001 - no keychain here
        return False


def run(ask, say, path, force=False, env=None):
    """The guide. `ask(question, secret=False, default="")` returns the
    answer, or is None where nothing may be asked; `say(text)` prints.
    Returns the path written."""
    from . import addresses, hopkeys, imports
    from .config import build_hop, keychain_name, keychain_write
    env = dict(os.environ if env is None else env)
    asked = []

    def question(text, secret=False, default=""):
        asked.append(text)
        return ask(text, secret=secret, default=default)

    existing = _existing(path, force)
    imported = None
    src = env.get("MIGKIT_SOURCE") or ""
    for _ in range(3):
        if not src and ask is not None:
            src = question(ASK_SOURCE).strip()
        try:
            if imports.looks_like_a_file(src):
                imported = imports.read(src)
                break
            addresses.parse(src, "source")
            break
        except SystemExit as e:
            if ask is None or env.get("MIGKIT_SOURCE"):
                raise
            say(f"[yellow]{e}[/yellow]")
            src = ""
    raw = dict(imported.raw) if imported else {"source": src}
    if imported:
        say(f"read {src} as hop {imported.name}")
        for note in imported.notes:
            say(f"  - {note}")
    if not raw.get("source"):
        # a task that says only where its changes went
        raw["source"] = _address(question if ask else None, say, "",
                                 ASK_SOURCE, "source")
    if not raw.get("target"):
        raw["target"] = _address(question if ask else None, say,
                                 env.get("MIGKIT_TARGET", ""), ASK_TARGET,
                                 "target")
    # the same two places as a hop the file has: that hop, updated where
    # it is - it was added again as `shop-2`, and the `--force` suggested
    # rewrote every other hop out of the file
    same = None if force else _same_places(raw, existing)
    name = env.get("MIGKIT_HOP") or same \
        or (imported.name if imported else "") or _hop_name(raw, existing)
    if name in existing and not force and name != same:
        raise SystemExit(f"{path} already has a hop {name} going somewhere"
                         " else (MIGKIT_HOP=<another name> adds this one"
                         " beside it)")
    if same and name == same:
        say(f"{path} has a hop {name} between these two already: it is"
            " updated in place")
    # passwords: asked, then kept where the operator says
    typed = {}
    src_e, dst_e, _ = hopkeys.sides(raw)
    for side, e in (("source", src_e), ("target", dst_e)):
        if not _sign_in(e) or _has_password(raw[side]):
            continue
        var = f"MIGKIT_{side.upper()}_PASSWORD"
        if env.get(var) is not None:
            typed[side] = (env[var], var)
        elif ask is not None:
            who = _who(raw[side], side)
            got = question(f"Password for {who} (not shown; Enter if it"
                           " signs in without one)", secret=True)
            if got:
                typed[side] = (got, None)
    if typed:
        keep = str(env.get("MIGKIT_SECRETS") or "").lower()
        if not keep and ask is not None and keychain_usable():
            keep = question(ASK_KEEP, default="k").strip().lower() or "k"
            keep = "keychain" if keep.startswith("k") else "env"
        # without a terminal, the variables they came in stay where they
        # are: nothing is copied anywhere unasked
        keep = keep or "env"
        if keep == "keychain" and not keychain_usable():
            raise SystemExit("this machine has no keychain migkit can"
                             " use; keep the passwords in environment"
                             " variables instead (MIGKIT_SECRETS=env)")
        for side, (secret, var) in typed.items():
            if keep == "keychain":
                keychain_write(keychain_name(name, side), secret)
                if isinstance(raw[side], dict):
                    raw[side] = {**raw[side], "password":
                                 f"keyring:{keychain_name(name, side)}"}
                continue
            var = var or _env_var(name, side)
            os.environ[var] = secret
            if addresses.is_address(raw[side]):
                raw[side] = _with_reference(raw[side], f"env:{var}")
            else:
                raw[side] = {**raw[side], "password": f"env:{var}"}
        said = ("kept in this machine's keychain" if keep == "keychain"
                else "read from " + ", ".join(
                    sorted(v or _env_var(name, s) for s, (_, v)
                           in typed.items()))
                + " - set wherever migkit runs")
        say(f"passwords: {said}")
    hop = build_hop(name, raw)
    found = survey(hop, say, imported)
    if imported is not None and found.get("left_out"):
        left = found["left_out"]
        from .config import scope_patterns
        raw["exclude"] = scope_patterns(raw.get("exclude")) + left
        say(f"  the task took {len(found['tables']) - len(left)} of the"
            f" source's {len(found['tables'])} tables; left out:"
            f" {', '.join(left[:8])}{' ...' if len(left) > 8 else ''}")
    goal = env.get("MIGKIT_GOAL") or raw.get("goal") or propose(found)
    if goal not in hopkeys.GOALS:
        raise SystemExit(f"MIGKIT_GOAL={goal}: one of"
                         f" {', '.join(hopkeys.GOALS)}")
    raw["goal"] = goal
    hop = build_hop(name, raw)
    written = plan(hop, found, goal, say)
    small = hopkeys.smallest(name, raw)
    if goal == "copy":
        # what a hop with no goal does: said by leaving it out
        small.pop("goal", None)
    _write(path, name, small, force, replace=name in existing)
    lines = _lines(small)
    say(f"\nwrote hop {name} to {path} ({lines} lines)")
    for step in hopkeys.GOALS[goal]["next"]:
        say(f"  {step.format(hop=name)}")
    if written:
        say("  migkit doctor        - asks again once the DBA has run the"
            " script")
    return {"path": path, "name": name, "questions": asked, "goal": goal,
            "config": small, "found": found, "scripts": written}


def _address(question, say, given, text, side):
    from . import addresses
    got = given
    for _ in range(3):
        if not got and question is not None:
            got = question(text).strip()
        if not got:
            raise SystemExit(f"no {side} address: set MIGKIT_"
                             f"{side.upper()} or run `migkit init` in a"
                             " terminal")
        try:
            addresses.parse(got, side)
            return got
        except SystemExit as e:
            if question is None or given:
                raise
            say(f"[yellow]{e}[/yellow]")
            got = ""
    raise SystemExit(f"no {side} address migkit could read")


def _who(raw_side, side):
    """`user@host:port` of a side, as a password is asked for it."""
    from . import addresses
    ep = addresses.parse(raw_side, side)[1] if addresses.is_address(
        raw_side) else (raw_side if isinstance(raw_side, dict) else {})
    where = ep.get("host") or ep.get("hosts") or ep.get("account") or side
    if isinstance(where, (list, tuple)):
        where = ",".join(where)
    port = f":{ep['port']}" if ep.get("port") else ""
    return f"{ep.get('user', '')}@{where}{port} (the {side})"


# ---- what is there -------------------------------------------------------

def _looking_at(eng, hop, side):
    """(engine, side key) that asks one side's server and nothing else:
    across two engines, an engine pointed at that server on both of its
    sides (`HeteroEngine._one_side`) - asked of its other side, an engine
    waits on a server that speaks another protocol."""
    if getattr(eng, "src_engine", None) is not None:
        e = eng.src_engine if side == "source" else eng.dst_engine
        return eng._one_side(e, side), "src"
    return eng, ("src" if side == "source" else "dst")


def survey(hop, say, imported=None):
    """What each side is, said as it is found; the facts as a mapping."""
    from . import canon, clouds, dba, sizing
    from .engines import get_engine
    out = {"sides": {}, "tables": [], "left_out": [], "care": [],
           "reached": True}
    say("")
    try:
        eng = get_engine(hop)
        dbs = list(eng.databases())
    except SystemExit:
        raise
    except Exception as e:  # noqa: BLE001 - said, and the config written
        out["reached"] = False
        say(f"[red]could not reach the source[/red]: {dba._last(e)}")
        return out
    out["dbs"] = dbs
    out["eng"] = eng
    wheres = {}
    for side in ("source", "target"):
        ep = hop.source if side == "source" else hop.target
        e, key = _looking_at(eng, hop, side)
        names = [hop.target_db(d) if side == "target" else d for d in dbs]
        ask_dbs = names if key == "src" else dbs
        got = {"dbs": names}
        try:
            brands = e._brands()
            brand = brands[0 if key == "src" else 1]
            got["software"] = brand.label() if brand.identified else ""
            where = clouds.of(e, key, ep)
            wheres[side] = where
            got["where"] = where.as_dict()
            first = ask_dbs[0] if ask_dbs else ""
            cap = sizing.capacity(e, key, first)
            got["replica"] = cap.get("replica")
            facts, by_db = {}, {}
            for d in ask_dbs:
                try:
                    by_db[d] = e.table_facts(key, d) or {}
                    for t, f in by_db[d].items():
                        # qualified as the hop's exclude reads it: by
                        # schema where the engine has them, by database
                        # where the database is the schema
                        facts[t if "." in str(t) else f"{d}.{t}"] = f
                except Exception:  # noqa: BLE001 - a database not there
                    continue
            got["facts"] = facts
            # as each database answered, for the plan's time (`_eta`),
            # which read them all again
            got["by_db"] = by_db
        except SystemExit:
            raise
        except Exception as err:  # noqa: BLE001 - said
            got["error"] = dba._last(err)
            if side == "source":
                out["reached"] = False
        out["sides"][side] = got
    out["wheres"] = wheres
    src = out["sides"]["source"]
    if "error" not in src:
        facts = src.get("facts") or {}
        out["tables"] = sorted(facts)
        if imported is not None:
            out["left_out"] = imports_left_out(imported, facts)
        out["keyless"] = sorted(t for t, f in facts.items()
                                if isinstance(f, dict) and f.get("key") is
                                False and t not in out["left_out"])
        out.update(_sums(facts))
        out["carried"] = _sums({t: f for t, f in facts.items()
                                if t not in out["left_out"]})
    if hop.engine == "hetero" and "error" not in src:
        out["care"] = types_needing_care(hop, eng, dbs, canon)
    try:
        out["needs"] = dba.needs(hop, eng, "cutover", wheres,
                                 streaming=True)
        out["needs_asked"] = True
    except Exception as err:  # noqa: BLE001 - said
        out["needs"] = [dba.Need("source", "ask the server what it allows",
                                 None, dba._last(err))]
    out["link"] = _link(eng, hop, dbs)
    record(hop, out["sides"])
    _say_found(hop, out, say)
    return out


def record(hop, sides):
    """What each side was found to be - its software, who runs it, a
    primary or a replica - kept beside the hop's reports as facts
    (`facts.json`), each side's newest over what was found before."""
    import json
    path = hop.report_dir() / "facts.json"
    try:
        have = json.loads(path.read_text())
    except (OSError, ValueError):
        have = {}
    for side, got in sides.items():
        mine = {k: v for k, v in (got or {}).items()
                if k not in ("facts", "by_db")}
        have[side] = {**(have.get(side) or {}), **mine}
    path.write_text(json.dumps(have, indent=1, default=str))
    return path


def _sums(facts):
    """Tables, rows and sizes of `facts`; a size the catalogue does not
    give is None, never said as nothing."""
    facts = {t: f for t, f in facts.items() if isinstance(f, dict)}
    sized = [f for f in facts.values() if f.get("bytes") is not None]
    return {"count": len(facts),
            "rows": sum(int(f.get("rows") or 0) for f in facts.values()),
            "bytes": sum(int(f["bytes"]) for f in sized) if sized else None,
            "index_bytes": sum(int(f.get("index_bytes") or 0)
                               for f in sized) if sized else None}


def _size_words(sums):
    from .wording import human_bytes
    if sums.get("bytes") is None:
        return ""
    return (f", {human_bytes(sums['bytes'])} of tables and"
            f" {human_bytes(sums['index_bytes'])} of indexes")


def imports_left_out(imported, facts):
    from . import imports
    return imports.left_out(imported, list(facts))


#: how many tables' columns are read for the types that need care
CARE_TABLES = 200


def types_needing_care(hop, eng, dbs, canon):
    """[(table, column, declared, why)] of the source's columns that do
    not arrive as they are: no way to compare them across the two
    engines, no type for them on the target, a time that changes meaning,
    or a type the target makes narrower."""
    src = eng.src_engine
    s_name = src.CANON_ENGINE
    d_name = eng.dst_engine.CANON_ENGINE
    out = []
    seen = 0
    for db in dbs:
        for table in src.neutral_tables("src", db):
            seen += 1
            if seen > CARE_TABLES:
                return out
            for col, declared in src.neutral_columns("src", db, table):
                cls, why = canon.comparable(s_name, declared)
                if why:
                    out.append((table, col, declared,
                                "migkit cannot compare it across the two"))
                    continue
                try:
                    made = canon.ddl_type(d_name, cls,
                                          canon.params(declared))
                except ValueError:
                    out.append((table, col, declared,
                                f"{d_name} has no type for it"))
                    continue
                a = canon.time_meaning(s_name, declared)
                b = canon.time_meaning(d_name, made)
                if a and b and a != b:
                    out.append((table, col, declared,
                                f"becomes {made} - read as {a} on the"
                                f" source, as {b} on the target"))
                elif canon.narrower(canon.capacity(s_name, declared),
                                    canon.capacity(d_name, made)):
                    out.append((table, col, declared,
                                f"becomes {made}, which holds less"))
    return out


def _link(eng, hop, dbs):
    """The slowest measured rate of the links to the two sides, MiB/s, or
    None. Each carries every byte of the move, so whichever was measured
    bounds its time from below; a side not measured (a target database
    not made yet) bounds nothing."""
    rates = []
    for side in ("source", "target"):
        e, key = _looking_at(eng, hop, side)
        fn = getattr(e, "link_probe", None)
        if fn is None:
            continue
        names = [hop.target_db(d) if side == "target" and key == "src"
                 else d for d in dbs]
        try:
            got = fn(key, names[0] if names else "")
        except Exception:  # noqa: BLE001 - a rate, or none
            continue
        if got.get("mb_s"):
            rates.append(float(got["mb_s"]))
    return min(rates) if rates else None


def _say_found(hop, out, say):
    say("[bold]what is there[/bold]")
    for side in ("source", "target"):
        got = out["sides"].get(side) or {}
        if got.get("error"):
            say(f"  {side}: [red]could not be asked[/red] - {got['error']}")
            continue
        where = (got.get("where") or {}).get("label", "")
        role = {True: "a replica (read-only)", False: "a primary"}.get(
            got.get("replica"), "primary or replica: it would not say")
        software = got.get("software") or "software it would not name"
        say(f"  {side}: {software}, {where}, {role}")
    if "rows" in out:
        say(f"  {out['count']} tables in"
            f" {', '.join(map(str, out.get('dbs') or []))}: about"
            f" {out['rows']:,} rows{_size_words(out)}")
    keyless = out.get("keyless") or []
    if keyless:
        say(f"  {len(keyless)} without a key ({', '.join(keyless[:5])}"
            f"{' ...' if len(keyless) > 5 else ''}) - copied whole and"
            " compared whole; a change to one row cannot be told apart"
            " from another")
    care = out.get("care") or []
    part = (f" (of the first {CARE_TABLES} tables)"
            if out.get("count", 0) > CARE_TABLES else "")
    if care:
        say(f"  {len(care)} columns need care across the two"
            f" engines{part}:")
        for table, col, declared, why in care[:8]:
            say(f"    {table}.{col} {declared}: {why}")
    elif hop.engine == "hetero" and "rows" in out:
        say("  every column has a type on the target that keeps its"
            f" values{part}")
    follow = [n for n in out.get("needs") or []
              if n.side == "source" and n.follow]
    if follow:
        off = [n for n in follow if n.ok is False]
        say("  the source's changes can " + (
            "be followed" if not off else
            "not be followed yet: " + "; ".join(n.now for n in off)))
        # what that means, in the words the DBA's script gives it
        without = next((n.without for n in off if n.without), "")
        if without:
            say(f"    {without}")
    grants = [n for n in out.get("needs") or [] if n.ok is False
              and n not in follow]
    if grants:
        say("  the users lack: " + "; ".join(
            f"{n.side} - {n.what} ({n.now})" for n in grants[:6]))
    elif out.get("needs"):
        say("  the users may read and write what migkit needs")


def propose(found):
    """The goal the facts point to: a target that already holds the
    source's tables is proved equal; a source whose changes can be
    followed is cut over with the least downtime; any other is copied."""
    target = (found.get("sides") or {}).get("target") or {}
    have = {str(t).split(".")[-1] for t, f in (target.get("facts") or {})
            .items() if isinstance(f, dict) and f.get("rows")}
    tables = {str(t).split(".")[-1] for t in found.get("tables") or []}
    if tables and have and len(tables & have) * 2 >= len(tables):
        return "verify"
    follow = [n for n in found.get("needs") or [] if n.side == "source"
              and n.follow]
    if follow and all(n.ok for n in follow):
        return "cutover"
    return "copy"


def plan(hop, found, goal, say):
    """Says the plan; writes the DBA's scripts; returns {side: path}."""
    from . import dba, hopkeys
    from .wording import human_bytes
    say(f"\n[bold]the plan[/bold]: {goal} - {hopkeys.GOALS[goal]['what']}")
    # what the goal does on each side and guarantees, and for a cutover
    # what `cutover:` on or off means - the words `assess` and the dry run
    # say too
    for line in hopkeys.explain_goal(hop, goal)[1:]:
        say(line)
    carried = found.get("carried") or {}
    size = carried.get("bytes")
    rows = int(carried.get("rows") or 0)
    if goal != "verify" and carried:
        say(f"  carries {carried['count']} tables, about {rows:,} rows"
            + (f" and {human_bytes(size)}; the indexes"
               f" ({human_bytes(carried['index_bytes'])}) are built on"
               " the target" if size is not None else ""))
        rate = found.get("link")
        eta = _eta(hop, found)
        measured = None if eta else _measured(hop, rows)
        if eta:
            for line in eta:
                say(line)
        elif measured:
            say(f"  time: {measured}")
        elif rate and size:
            secs = size / (rate * 2 ** 20)
            say(f"  time: at least {_took(secs)} - {rate} MiB/s is what"
                " the slowest link measured carried; the first copy"
                " measures the real rate and every plan after it uses"
                " that")
        else:
            say("  time: no rate measured yet; the first copy measures it"
                " and every plan after it uses that")
    elif goal == "verify":
        say(f"  compares {len(found.get('tables') or [])} tables, about"
            f" {int(found.get('rows') or 0):,} rows")
    eng = found.get("eng")
    if eng is None:
        return {}
    try:
        # a cutover's needs are what the survey asked already - with the
        # change-following checks on - and were asked a second time
        needs = found["needs"] if goal == "cutover" \
            and found.get("needs_asked") else \
            dba.needs(hop, eng, goal, found.get("wheres"))
    except Exception as err:  # noqa: BLE001 - said
        say(f"  what the users may do could not be asked: {dba._last(err)}")
        return {}
    written = dba.write(hop, eng, needs, found.get("wheres"))
    lack = dba.missing(needs)
    unknown = [n for n in needs if n.ok is None]
    if lack:
        say(f"  before it can: {len(lack)} thing(s) the DBA must do -")
        for n in lack:
            say(f"    {n.side}: {n.what} ({n.now})")
        for side, p in written.items():
            say(f"  the {side}'s script, for the DBA: {p}")
    for n in unknown:
        say(f"  {n.side}: {n.what} - could not be asked ({n.now})")
    if not lack and not unknown:
        say("  nothing for the DBA to do first")
    return written


def _eta(hop, found):
    """The move's resources and each phase's time (`model`,
    `planner.phases`), a database at a time: which resource binds, how
    many at once carry it, and how long the copy, the index builds, the
    check, the catch-up and the cutover take. The probes read only here -
    nothing is written before the hop is even saved - so the target's
    writes are an earlier run's or are said not measured. [] where nothing
    was measured, now or before."""
    from . import model as resource_model
    from . import planner, sizing
    eng = found.get("eng")
    out = []
    seen = ((found.get("sides") or {}).get("source") or {}).get("by_db") \
        or {}
    for d in (found.get("dbs") or [])[:3]:
        try:
            facts = seen[d] if d in seen else eng.table_facts("src", d)
            m = resource_model.build(hop, eng, d, facts, write=False)
            n = sizing.estimate(hop, eng, d, None, model=m).start
        except Exception:  # noqa: BLE001 - that database is not said
            continue
        if not m.measured:
            continue
        head = f"  {d}: " if len(found.get("dbs") or []) > 1 else "  "
        out += [head + line for line in m.lines(n)]
        out += [head + line.strip() for line in planner.phase_lines(
            planner.phases(hop, m, facts, None, n))]
    return out


def _measured(hop, rows):
    """The planner's time for `rows`, from copies this hop or another of
    the same two engines made - on whichever way of copying has runs - or
    None where none has."""
    import json

    from . import config, planner
    paths = set()
    files = [planner._rates_path(hop)]
    try:
        files += [d / "throughput.json" for d in config.REPORTS.iterdir()
                  if d.is_dir()]
    except OSError:
        pass
    for f in files:
        try:
            paths |= set(json.loads(f.read_text()))
        except (OSError, ValueError):
            continue
    for path in sorted(paths):
        if planner._runs(planner._rates_path(hop), path) or \
                planner._elsewhere(hop, path)[0]:
            return planner.estimate(hop, path, rows)
    return None


def _took(secs):
    return (f"{secs / 3600:,.1f} hours" if secs >= 3600 else
            f"{secs / 60:,.0f} minutes" if secs >= 60 else
            f"{max(secs, 1):,.0f} seconds")


# ---- the file ------------------------------------------------------------

def _existing(path, force):
    """{name: the hop as written} of the file now, where the new one is
    added to it."""
    import yaml
    p = Path(path)
    if force or not p.exists():
        return {}
    try:
        data = yaml.safe_load(p.read_text()) or {}
    except yaml.YAMLError:
        raise SystemExit(f"{p} is not YAML migkit can read; use --force to"
                         " write a new one")
    if set(data) - {"hops"}:
        raise SystemExit(f"{p} holds more than hops; add the hop by hand,"
                         " or use --force to write a new file")
    return {str(k): v for k, v in (data.get("hops") or {}).items()}


def _place(raw_side, side):
    """Where a side is, whoever signs in: (engine, host, port, databases
    or file) - an address and a mapping read alike; None where it cannot
    be read without asking anything."""
    from . import addresses
    try:
        if addresses.is_address(raw_side):
            engine, ep, dbs = addresses.parse(raw_side, side)
        elif isinstance(raw_side, dict):
            engine, ep, dbs = raw_side.get("engine"), raw_side, \
                raw_side.get("databases") or []
        else:
            return None
    except SystemExit:
        return None
    where = ep.get("path") or ep.get("url") or ep.get("hosts") or \
        ep.get("host")
    return (str(engine or ""), str(where or "").lower(),
            str(ep.get("port") or ""), tuple(map(str, dbs or ())))


def _same_places(raw, existing):
    """The name of the hop in the file between the same two places as
    `raw`, or None."""
    want = (_place(raw.get("source"), "source"),
            _place(raw.get("target"), "target"))
    if None in want:
        return None
    for name, hop in existing.items():
        if isinstance(hop, dict) and (
                _place(hop.get("source"), "source"),
                _place(hop.get("target"), "target")) == want:
            return name
    return None


HEADER = """\
# migkit hops. A hop is one source -> target pair, written by `migkit init`
# as the fewest lines that say it; migkit decides everything else.
#   migkit doctor          what this machine can do, and whether hops connect
"""


def _dump(name, small):
    import yaml
    text = yaml.safe_dump({name: small}, sort_keys=False,
                          default_flow_style=False, width=1000)
    return "".join("  " + line + "\n" for line in text.splitlines())


def _lines(small):
    """The hop's lines in the file, its name included."""
    return len(_dump("x", small).splitlines())


def _replaced(text, name, body):
    """`text` with the hop `name`'s block - its line under `hops:` and the
    lines indented under it - replaced by `body`; None where it cannot be
    found as one block."""
    import re
    lines = text.splitlines(keepends=True)
    head = re.compile(rf"^(\s+){re.escape(name)}\s*:\s*(#.*)?$")
    for i, line in enumerate(lines):
        m = head.match(line.rstrip("\n"))
        if not m:
            continue
        indent = len(m.group(1))
        j = i + 1
        while j < len(lines):
            nxt = lines[j]
            # a line - a comment too - no deeper than the hop's name is
            # what comes after the hop
            if nxt.strip() and len(nxt) - len(nxt.lstrip()) <= indent:
                break
            j += 1
        return "".join(lines[:i]) + body + "".join(lines[j:])
    return None


def _write(path, name, small, force, replace=False):
    """The hop written: added to the file, or - `replace` - put in the
    place of the hop of that name, every other line of the file as it
    was."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = _dump(name, small)
    if p.exists() and not force:
        text = p.read_text()
        if not text.endswith("\n"):
            text += "\n"
        new = _replaced(text, name, body) if replace else None
        if new is None:
            if "hops:" not in text:
                text += "hops:\n"
            new = text + body
    else:
        new = HEADER + "hops:\n" + body
    tmp = p.with_suffix(p.suffix + ".tmp")
    from . import durable
    # 0600 from its first byte (SEC10)
    durable.write_atomic(tmp, new)
    # the file as written must load: a guide that wrote a configuration
    # migkit then refuses has handed the operator its own mistake
    from .config import load_hops
    try:
        load_hops(tmp)
    except SystemExit:
        tmp.unlink(missing_ok=True)
        raise
    except Exception as e:  # noqa: BLE001 - the file it was added to
        tmp.unlink(missing_ok=True)
        raise SystemExit(f"the hop could not be added to {p} as it is"
                         f" written ({type(e).__name__}); nothing was"
                         " changed - use --force to write a new file")
    tmp.replace(p)
