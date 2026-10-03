"""The case migkit recognised between a hop's two servers, said and held
to before anything is written (backlog P0-SCENARIOS).

A hop of one engine joins two servers that may still differ in three
ways, each read from the servers themselves and never from an address:

* **the brand** - MySQL, MariaDB or Percona Server; PostgreSQL, Citus,
  YugabyteDB or CockroachDB, and Amazon Aurora or AlloyDB where the server
  says it is one (`clouds`: its own role, its own function); Redis,
  Valkey, KeyDB or Dragonfly; Elasticsearch or OpenSearch; MongoDB,
  Percona Server for MongoDB, FerretDB or Azure Cosmos DB; Kafka or
  another broker of its protocol (`variants`, one brand fact a side).
  Two brands are **a fork change**: the version numbers of two programs
  are not compared, and what each lacks of the other is asked below;
* **the version** - up, the same, or **down**: a newer source's pages,
  clone or backup are read by no older server, so a copy of the pages is
  refused up front with the reason and the logical copy is the way; what
  the newer source uses that the older target has not is found first;
* **the edition** (`editions`) - up, down, or another: a move down an
  edition is a licence-driven one, and said so.

What the source uses that the target has not (`lacks`) is asked for a fork
change, a version down and an edition change; an upgrade's own checks
(`upgrade`) hold the version up. The plan names the case in its first line
(`plan_lines`), and `assess` and the gate before the first write hold to
every row (`ready`) - one run for all of them.
"""
from dataclasses import dataclass

from . import lacks

SCOPE = "the case"


def _row(level, item, detail, fix=""):
    return {"level": level, "scope": SCOPE, "item": item,
            "detail": detail + (f"  [fix] {fix}" if fix and level != "pass"
                                else "")}


def _last(e):
    return (str(e).strip().splitlines() or [type(e).__name__])[-1][:160]


#: the managed services whose engine is a distribution of its own - its
#: own storage under the same protocol - where the server says it is one
DISTRIBUTIONS = {"aurora": "Amazon Aurora", "alloydb": "AlloyDB"}

#: brands as an operator reads them
BRAND_WORDS = {"postgres": "PostgreSQL", "mysql": "MySQL",
               "mariadb": "MariaDB", "percona": "Percona Server",
               "mongodb": "MongoDB", "redis": "Redis", "valkey": "Valkey",
               "keydb": "KeyDB", "dragonfly": "Dragonfly",
               "cockroachdb": "CockroachDB", "yugabytedb": "YugabyteDB",
               "citus": "Citus", "greenplum": "Greenplum",
               "tidb": "TiDB", "vitess": "Vitess", "oceanbase": "OceanBase",
               "polardb": "PolarDB", "tdsql": "TDSQL",
               "ferretdb": "FerretDB", "cosmosdb": "Azure Cosmos DB",
               "percona-server-mongodb": "Percona Server for MongoDB",
               "elasticsearch": "Elasticsearch", "opensearch": "OpenSearch",
               "kafka": "Kafka", "mssql": "SQL Server",
               "oracle": "Oracle Database"}


def engine_of(eng):
    return getattr(getattr(eng, "src_engine", None) or eng,
                   "CANON_ENGINE", "") or ""


def applies(hop, eng):
    """Whether the hop joins two servers of one engine and writes: a hop
    of two engines is a change of engine, which canon holds; a hop that
    writes nothing has nothing to refuse."""
    from . import interlock
    from .hopkeys import GOALS
    goal = getattr(hop, "goal", "") or "copy"
    if hop.engine == "hetero" or getattr(eng, "src_engine", None):
        return False
    if goal in GOALS and not GOALS[goal]["writes"]:
        return False
    if not engine_of(eng):
        return False
    # a side nothing answers at is said by the step itself, soon, where
    # asking here would wait on each engine's own retries
    try:
        return interlock.reached(hop)
    except Exception:  # noqa: BLE001 - not known to answer: not asked
        return False


def _distribution(eng, hop, side):
    """The distribution the server itself says it is (`clouds.of`'s
    server evidence), or ""; an address alone names none."""
    from . import clouds
    try:
        where = clouds.of(eng, side, hop.source if side == "src"
                          else hop.target)
    except Exception:  # noqa: BLE001 - the server said nothing it could
        return ""
    if not any(str(e).startswith("the server") for e in where.evidence):
        return ""
    return where.service if where.service in DISTRIBUTIONS else ""


@dataclass
class Case:
    """What differs between the two servers of a hop."""
    engine: str
    src: lacks.Side
    dst: lacks.Side
    src_version: str = ""
    dst_version: str = ""
    version: str = None      #: "up", "same", "down"; None: not compared
    fork: bool = False
    editions: tuple = (None, None)
    edition: str = None      #: `editions.direction`

    def words(self):
        """The case in words, the first part the plan says."""
        from . import editions as ed
        out = []
        if self.fork:
            out.append(f"a fork change: {self.src.label} to"
                       f" {self.dst.label}")
        if self.version in ("down", "up"):
            out.append(f"a version {self.version}: {self.src_version} to"
                       f" {self.dst_version}")
        said = ed.words(*self.editions) if self.editions[0] else ""
        if said:
            out.append(said)
        if not out:
            same = (f"{self.src.label}, the same version"
                    + (f" and edition ({self.editions[0].label()})"
                       if self.edition == "same" else ""))
            out.append(f"one server to another of the same kind: {same}")
        return "; ".join(out)


#: brands whose version is numbered as their family's own software's -
#: YugabyteDB and Greenplum say the PostgreSQL they are built on, Percona
#: Server the MySQL or MongoDB release it follows - so a feature had "from
#: PostgreSQL 12" is had from their 12 as well; any other brand's number is
#: its own (CockroachDB's v23, FerretDB's 2.0) or a compatibility claim
FAMILY_NUMBERED = {"postgres", "yugabytedb", "greenplum", "mysql",
                   "percona", "mongodb", "percona-server-mongodb", "redis",
                   "opensearch", "elasticsearch"}


def _side_of(eng, hop, side, identity_version):
    """The `lacks.Side` of one server: its brand (the engine's name where
    the engine reads none), its own version, and whether that version is
    numbered as its family's own software's (`FAMILY_NUMBERED`)."""
    engine = engine_of(eng)
    family = getattr(eng, "ENGINE_FAMILY", "") or ""
    if family:
        b = eng._brands()[0 if side == "src" else 1]
        name = b.name
        text = b.version or identity_version or ""
        real = name in FAMILY_NUMBERED
    else:
        name, text, real = engine, identity_version or "", True
    dist = _distribution(eng, hop, side) if engine in ("postgres",
                                                       "mysql") else ""
    words = BRAND_WORDS.get(name, name if name != "unknown"
                            else "software migkit cannot name")
    label = f"{words} {text}".strip() + (f" ({DISTRIBUTIONS[dist]})"
                                         if dist else "")
    return lacks.Side(name, lacks.numbers(text, 3), real, label), dist, text


def _compare(engine, a, b):
    """-1, 0 or 1: `b`'s version against `a`'s - MySQL's forks by release
    (their features come with them), every other engine by major."""
    from .upgrade import major
    if a is None or b is None:
        return None
    if engine == "mysql":
        x, y = lacks._pad(a), lacks._pad(b)
    else:
        x = major(".".join(map(str, a)))
        y = major(".".join(map(str, b)))
    return (y > x) - (y < x)


def recognise(hop, eng, db=None):
    """The `Case` of this hop, read once for this engine."""
    cache = eng.__dict__.setdefault("_case", {})
    if "case" in cache:
        return cache["case"]
    from . import editions, upgrade
    engine = engine_of(eng)
    pair = (None, None)
    if engine in ("mssql", "oracle", "mysql", "mongodb", "redis"):
        try:
            pair = editions.pair(eng, db)
        except Exception:  # noqa: BLE001 - said by the edition's rows
            pair = (None, None)
    try:
        have = upgrade.versions(hop, eng, db) if db else (None, None)
    except Exception:  # noqa: BLE001 - the brands' own are read instead
        have = (None, None)
    # a server's version where its identity could not be read - a target
    # database not made yet - as its edition's reading says it
    have = tuple(v or (".".join(map(str, e.version)) if e is not None
                       and e.version else None)
                 for v, e in zip(have, pair))
    src, sdist, stext = _side_of(eng, hop, "src", have[0])
    dst, ddist, dtext = _side_of(eng, hop, "dst", have[1])
    fork = (src.brand != dst.brand or sdist != ddist) \
        and "unknown" not in (src.brand, dst.brand)
    version = None
    # one program's numbers are compared with its own, under any
    # distribution: Aurora's PostgreSQL 15 is PostgreSQL 15
    if src.brand == dst.brand and src.brand != "unknown":
        got = _compare(engine, src.version, dst.version)
        version = {1: "up", 0: "same", -1: "down"}.get(got)
    got = Case(engine, src, dst, stext, dtext, version, fork, pair,
               editions.direction(*pair) if pair[0] else None)
    cache["case"] = got
    return got


# --- what each case adds ---------------------------------------------------------------

def _mysql_fork(hop, eng, dbs, c):
    """MySQL's forks: two ways of numbering transactions, and MariaDB's
    JSON kept as text."""
    from .upgrade import _my
    out = []
    names = {c.src.brand, c.dst.brand}
    if "mariadb" in names and names - {"mariadb"}:
        out.append(_row(
            "pass", "transaction ids",
            "MariaDB numbers its transactions otherwise (domain, server,"
            " sequence) than MySQL's GTIDs: the changes are followed from"
            " the source's own log positions, and a stream back reads the"
            " target's own"))
    if c.dst.brand == "mariadb" and c.src.brand != "mariadb" and dbs:
        e = getattr(eng, "src_engine", None) or eng
        marks = ", ".join(["%s"] * len(dbs))
        try:
            cols = [str(r[0]) for r in _my(
                eng, "src", "select concat(table_schema, '.', table_name,"
                            " '.', column_name) from information_schema"
                            f".columns where table_schema in ({marks}) and"
                            " data_type = 'json'",
                tuple(e._d("src", d) for d in dbs))]
        except Exception as ex:  # noqa: BLE001 - unknown, said so
            return out + [_row("warn", "JSON as text",
                               f"could not be read: {_last(ex)} - unknown,"
                               " not clean")]
        if cols:
            out.append(_row(
                "warn", "JSON as text",
                f"{len(cols)} JSON columns arrive as text on MariaDB, kept"
                " valid by a check: " + lacks._said(cols) + " - MySQL keeps"
                " JSON as a binary document, so a comparison, an order or a"
                " JSON function that reads it may answer otherwise there",
                "run the application's statements that compare, sort or"
                " search JSON against the target before the cutover"))
    return out


def _pg_fork(hop, eng, dbs, c):
    """PostgreSQL's distributions: Citus distributes tables across
    workers; a copy into or out of it moves rows, not the distribution."""
    out = []
    if "citus" in (c.src.brand, c.dst.brand):
        from .upgrade import _pg
        dist = []
        if c.src.brand == "citus":
            for d in dbs:
                try:
                    dist += [f"{d}.{r[0]}" for r in _pg(
                        eng, "src", d, "select logicalrelid::regclass::text"
                                       " from pg_dist_partition") if r]
                except Exception:  # noqa: BLE001 - said below as unknown
                    dist = None
                    break
        if c.dst.brand == "citus":
            out.append(_row(
                "warn", "distribution",
                "the target distributes tables across its workers, and a"
                " table migkit makes there is a local one on its"
                " coordinator", "distribute each after the copy"
                               " (create_distributed_table), before the"
                               " application reaches it"))
        elif dist is None:
            out.append(_row("warn", "distribution",
                            "the source's distributed tables could not be"
                            " read - unknown, not clean"))
        elif dist:
            out.append(_row(
                "pass", "distribution",
                f"{len(dist)} distributed tables on the source are read"
                " whole through its coordinator and arrive as ordinary"
                " tables: " + lacks._said(dist)))
    return out


def _unnamed(c):
    """A side whose software migkit cannot name: said, never guessed."""
    out = []
    for side, s in (("source", c.src), ("target", c.dst)):
        if s.brand == "unknown":
            out.append(_row(
                "warn", f"the {side}'s software",
                f"migkit cannot say which software answers on the {side}:"
                " what it lacks is asked of it where it can be, and what"
                " cannot be asked is unknown, not clean"))
    return out


def _search_fork(hop, eng, dbs, c):
    if {c.src.brand, c.dst.brand} != {"elasticsearch", "opensearch"}:
        return []
    return [_row("pass", "users and roles",
                 "Elasticsearch and OpenSearch each keep their own users,"
                 " roles and API keys, in APIs of their own: migkit carries"
                 " the indexes' documents and mappings, and signs in to"
                 " each as the hop says")]


FORKS = {"mysql": _mysql_fork, "postgres": _pg_fork,
         "opensearch": _search_fork}


def _physical_refused(hop, c):
    """A version down: a copy of the pages the hop allows is refused up
    front, with the reason."""
    from . import physical
    if not physical.settings(hop):
        return []
    return [_row("warn", "a copy of the pages",
                 f"refused before anything runs: the target runs"
                 f" {c.dst_version}, older than the source's"
                 f" {c.src_version}, and pages, a clone or a backup are read"
                 " only by the version that wrote them or a newer one - the"
                 " logical copy carries it",
                 "nothing to do: the logical copy is the way; take"
                 " physical: out of the hop's options to say so")]


def _ms_level(eng, dbs, c):
    """SQL Server a version down: a database at a compatibility level the
    target has not runs at the target's highest there."""
    from .upgrade import major
    top = (major(c.dst_version) or (0,))[0] * 10
    if not top:
        return []
    out = []
    e = getattr(eng, "src_engine", None) or eng
    for d in dbs:
        (level,) = e._rows("src", d, "select compatibility_level from"
                                     " sys.databases where name ="
                                     " db_name()")[0]
        if int(level) > top:
            out.append(_row(
                "warn", "compatibility level",
                f"{d} runs at level {level} on the source; the target's"
                f" highest is {top}, and the database runs there at it -"
                " statements that need the higher level fail or plan"
                " otherwise", f"run the application's statements against"
                              f" the target at level {top} before the"
                              " cutover"))
    return out


def _mongo_fcv(eng, c):
    from .upgrade import major
    e = getattr(eng, "src_engine", None) or eng
    got = e._client("src").admin.command({"getParameter": 1,
                                          "featureCompatibilityVersion": 1})
    fcv = str((got.get("featureCompatibilityVersion") or {}).get("version")
              or "")
    if fcv and major(fcv) and major(c.dst_version) \
            and major(fcv) > major(c.dst_version):
        return [_row("pass", "feature compatibility",
                     f"the source runs with feature compatibility {fcv}"
                     f" and the target runs {c.dst_version}: what {fcv}"
                     " added is looked for below, collection by collection"
                     " and index by index")]
    return []


def _down(hop, eng, dbs, c):
    out = _physical_refused(hop, c)
    try:
        if c.engine == "mssql":
            out += _ms_level(eng, dbs, c)
        elif c.engine == "mongodb":
            out += _mongo_fcv(eng, c)
    except Exception as e:  # noqa: BLE001 - unknown, said so
        out.append(_row("warn", "what the older version changes",
                        f"could not be read: {_last(e)} - unknown, not"
                        " clean"))
    return out


def checks(hop, eng, dbs, quick=False):
    """assess rows of the case: the case named, what a fork change, a
    version down or an edition change adds, and what the source uses that
    the target has not - and the scenarios of the move itself, between
    any two engines (`scenario_rows`). `quick` is the gate's: the rows the
    engine's own `assess` gives it besides (`lacks.gate_only`) are asked
    here too."""
    return _servers(hop, eng, list(dbs or []), quick) + scenario_rows(
        hop, eng, list(dbs or []), quick)


def _servers(hop, eng, dbs, quick):
    if not applies(hop, eng):
        return []
    c = recognise(hop, eng, dbs[0] if dbs else None)
    out = [_row("pass", "what the two servers are", c.words())]
    if c.fork:
        out += _unnamed(c)
        part = FORKS.get(c.engine)
        if part is not None:
            try:
                out += part(hop, eng, dbs, c)
            except Exception as e:  # noqa: BLE001 - unknown, said so
                out.append(_row("warn", "the fork", f"could not be read:"
                                                    f" {_last(e)} -"
                                                    " unknown, not clean"))
    elif "unknown" in (c.src.brand, c.dst.brand):
        out += _unnamed(c)
    if c.version == "down":
        out += _down(hop, eng, dbs, c)
    from . import editions, upgrade
    differs = c.fork or c.version == "down" or c.edition not in (
        None, "same") or "unknown" in (c.src.brand, c.dst.brand)
    if differs:
        own = upgrade.settings(hop) is not None
        for d in dbs:
            try:
                out += [dict(r, scope=SCOPE) for r in lacks.checks(
                    hop, eng, d, c.src, c.dst, extensions=not own)]
            except Exception as e:  # noqa: BLE001 - unknown, said so
                out.append(_row("warn", "what the target has",
                                f"{d}: could not be asked: {_last(e)} -"
                                " unknown, not clean"))
    if quick:
        out += [dict(r, scope=SCOPE) for r in lacks.gate_only(hop, eng,
                                                              dbs)]
    if c.editions[0] is not None:
        out += [dict(r, scope=SCOPE) for r in editions.checks(hop, eng,
                                                              dbs)]
    return out


# --- the scenarios of the move itself -----------------------------------------------

#: what a move may change beside the servers, between any two engines: a
#: character set or a collation (`charsets`), a table's storage engine
#: (`tableengine`) - and the goals that place rows by rule: many sources
#: into one (`consolidate`), one into many (`split`), N into M by one
#: shard function (`reshard`), old rows to an archive (`archive`)
SCENARIOS = ("charsets", "tableengine", "consolidate", "split", "reshard",
             "archive")


def _scenario(name):
    import importlib
    return importlib.import_module(f"migkit.{name}")


def _wanted(hop, eng):
    """[(name, module, the hop's words for it or None)] of the scenarios
    that may be this move - asked of the hop and the engines alone, so a
    hop none of them is asks no server; then [] where a side does not
    answer (the step says why, soon)."""
    from . import interlock
    out = []
    for name in SCENARIOS:
        m = _scenario(name)
        try:
            if name in ("charsets", "tableengine"):
                if m.applies(hop, eng):
                    out.append((name, m, None))
            elif m.settings(hop) is not None:
                out.append((name, m, None))
        except SystemExit as e:
            out.append((name, m, str(e)))
    if not out:
        return []
    try:
        return out if interlock.reached(hop) else []
    except Exception:  # noqa: BLE001 - not known to answer: not asked
        return []


def scenario_rows(hop, eng, dbs, quick=False):
    """assess rows of the scenarios the move is, each with its fix: what a
    character set or engine change does, and what stops a consolidation,
    a split or an archive before anything is written."""
    out = []
    for name, m, said in _wanted(hop, eng):
        if said:
            out.append(_row("fail", f"options: {name}", said))
            continue
        if name in ("charsets", "tableengine"):
            try:
                out += [dict(r, scope=SCOPE) for r in m.checks(
                    hop, eng, dbs, quick)]
            except Exception as e:  # noqa: BLE001 - unknown, said so
                out.append(_row("warn", m.SCOPE, f"could not be read:"
                                                 f" {_last(e)} - unknown,"
                                                 " not clean"))
            continue
        for d in dbs:
            for why in m.problems(hop, eng, d):
                out.append(_row("fail", f"{d}: {name}", why))
            if name in ("split", "reshard") and not quick:
                out += [dict(r, scope=SCOPE) for r in m.rows(hop, eng, d)]
    return out


def scenario_lines(hop, eng, db):
    """The plan's lines of the scenarios the move is."""
    out = []
    for name, m, said in _wanted(hop, eng):
        if said:
            out.append(f"  {name}: {said}")
            continue
        try:
            out += m.plan_lines(hop, eng, db)
        except Exception as e:  # noqa: BLE001 - said, the plan goes on
            out.append(f"  {name}: could not be worked out: {_last(e)}")
    return out


# --- the plan ----------------------------------------------------------------------

def _way_lines(hop, eng, db, c):
    """The way the move goes where an edition or a version apart decides
    it: the copy of the pages the hop allows, climbed with the edition's
    and the version's needs among its rungs (`physical`), or the logical
    copy - with what the logical copy leaves behind."""
    from . import editions, physical, upgrade
    if upgrade.settings(hop) is not None:
        return []
    out = []
    if physical.settings(hop):
        try:
            rung, why = physical.choose(hop, eng, "full+cdc")
        except Exception as e:  # noqa: BLE001 - said, the plan goes on
            rung, why = None, f"it could not be worked out: {_last(e)}"
        out.append(f"  the way: {rung.said} - {why}" if rung is not None
                   else f"  the way: the logical copy - {why}")
    elif c.edition not in (None, "same") or c.version == "down" or c.fork:
        out.append("  the way: the logical copy - the hop allows no copy"
                   " of the pages (options: physical)")
    src_ed, dst_ed = c.editions
    if src_ed is not None and c.edition not in (None, "same") and db:
        try:
            gone = editions.stripped(eng, db, src_ed, dst_ed)
        except Exception:  # noqa: BLE001 - the rows say it
            gone = []
        if gone:
            out.append(f"  carried without {', '.join(gone)}:"
                       f" {dst_ed.label()} has none, and the rows arrive the"
                       " same")
    return out


def plan_lines(hop, eng, db):
    """The plan's lines of the case: what the two servers are, the way
    where the case decides it, and the scenarios the move is."""
    more = scenario_lines(hop, eng, db)
    if not applies(hop, eng):
        return (["the case: " + "; ".join(x.strip() for x in more)]
                if more else [])
    return _server_lines(hop, eng, db) + more


def _server_lines(hop, eng, db):
    c = recognise(hop, eng, db)
    out = [f"the case: {c.words()}"]
    if c.version == "down" and physical_allowed(hop):
        out.append("  a copy of the pages is refused: the target is older"
                   " than the source, and pages are read only by the"
                   " version that wrote them or a newer one")
    out += _way_lines(hop, eng, db, c)
    return out


def physical_allowed(hop):
    from . import physical
    return bool(physical.settings(hop))
