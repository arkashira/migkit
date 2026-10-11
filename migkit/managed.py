"""Every managed database service in one catalogue, read one way for any
pair (the owner, 2026-10-01: "any <-> any from the start").

`data/managed.yaml` lists the managed database services migkit knows,
provider by provider - the clouds' own and the vendors' - with the engine
migkit moves each as, how it is recognised, what the account the service
hands over may and may not do, how a setting is changed on it, what
following its changes needs, its restore points, its limits and its
defaults. Its header names the documentation each entry was read from.
Nothing here is a case of its own: a managed PostgreSQL on AWS into one
on Tencent Cloud is one instance of the same reading as every other pair,
and so is a server run by its owner on either side.

What comes from the catalogue and what from the servers:

* which service a side is: the server's own word first (`clouds.of`: a
  role, a variable, a function only that service has), the side's
  `managed: {service: ...}` where the server says nothing (a private
  address, a proxy), the address last - a hint, never the proof. Where
  the operator's word and the server's disagree it is said, and the
  server's stands.
* what the service's admin may do is the catalogue's word for the plan
  (`plan`), and every step asks the server itself before it acts
  (`freeze`, `dba`, `sourcechange`): a catalogue that is wrong costs a
  plan line, never a row.
* how a setting is changed - a parameter group, a database flag, a
  console - is the catalogue's, spelled in the provider's own words for
  the DBA (`spell`, which `dba._setting` asks).
* the instance behind a side, its facts and its restore points through
  the provider's API (`cloudapi`), where the side names it or its address
  finds it.

`plan` is the pair as one: the engine it runs through (the family's, or
the pair engine where the families differ), what following the source
needs in its provider's words, what the target's admin can and cannot
do, the way the source's writes are held at the cutover - the freeze
ladder over the facts the service's admin gives (`stand_in`) - and the
restore points each side offers. `items` is the same for `assess`, read
against the servers: the pair, the time zone, the character sets and
collations, the connections the target takes, the restore points.
"""
import re
import time
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

FILE = Path(__file__).parent / "data" / "managed.yaml"

#: what each engine family is called where an operator reads it
KINDS = {"postgres": "PostgreSQL", "mysql": "MySQL", "mongodb": "MongoDB",
         "mssql": "SQL Server", "oracle": "Oracle Database", "db2": "Db2",
         "redis": "Redis", "kafka": "Kafka", "opensearch": "OpenSearch",
         "dynamodb": "DynamoDB", "cassandra": "Cassandra",
         "redshift": "Redshift", "bigquery": "BigQuery",
         "clickhouse": "ClickHouse", "cockroachdb": "CockroachDB",
         "tidb": "TiDB", "dsql": "Aurora DSQL", "cosmosdb": "Cosmos DB",
         "snowflake": "Snowflake", "teradata": "Teradata",
         "kinesis": "a Kinesis stream",
         "pubsub": "a Pub/Sub topic"}

#: what `clouds` calls an endpoint of a service, and the service it is
ENDPOINTS = {"aurora-reader": "aurora", "rds-proxy": "rds"}

#: the placeholders a catalogue template may use (`fill`)
PLACEHOLDERS = ("name", "value", "after", "label", "apply", "kind",
                "restart_flag", "who", "id")


@lru_cache(maxsize=1)
def catalogue():
    """The catalogue as its file has it: {"providers": ..., "services":
    ...}. A missing file is an empty catalogue - every side is then a
    server run by its owner, as before the catalogue was."""
    import yaml
    try:
        got = yaml.safe_load(FILE.read_text()) or {}
    except OSError:
        got = {}
    return got if isinstance(got, dict) else {}


def providers():
    return catalogue().get("providers") or {}


@dataclass
class Service:
    """One managed service of the catalogue - or, with no `provider`, a
    server run by its owner."""
    id: str = ""
    provider: str = ""
    where: str = ""
    label: str = ""
    family: str = ""
    distinct: str = ""
    hosts: tuple = ()
    signs: dict = field(default_factory=dict)
    api: dict = field(default_factory=dict)
    admin: dict = field(default_factory=dict)
    change: dict = field(default_factory=dict)
    cdc: tuple = ()
    limits: dict = field(default_factory=dict)
    defaults: dict = field(default_factory=dict)
    notes: tuple = ()

    @property
    def managed(self):
        return bool(self.provider)

    @property
    def provider_label(self):
        return (providers().get(self.provider) or {}).get(
            "label", self.provider)

    @property
    def console(self):
        return (providers().get(self.provider) or {}).get(
            "console") or "the provider's console"

    @property
    def kind(self):
        """The engine it is, in words; "" for one migkit has no engine
        of (`distinct`)."""
        if self.family in ("", "none"):
            return ""
        return KINDS.get(self.family, self.family)

    @property
    def product(self):
        return (self.api or {}).get("product") or ""

    def said(self):
        """The side as the plan names it."""
        if not self.managed:
            return f"a {self.kind or 'database'} run by its owner"
        if not self.kind:
            return f"{self.label} on {self.provider_label}"
        return f"a managed {self.kind} on {self.provider_label} ({self.label})"


def _service(sid, raw):
    raw = raw or {}
    return Service(
        id=sid, provider=str(raw.get("provider") or ""),
        where=str(raw.get("where") or ""), label=str(raw.get("label") or sid),
        family=str(raw.get("family") or "none"),
        distinct=str(raw.get("distinct") or ""),
        hosts=tuple(raw.get("hosts") or ()), signs=dict(raw.get("signs") or {}),
        api=dict(raw.get("api") or {}), admin=dict(raw.get("admin") or {}),
        change=dict(raw.get("change") or {}), cdc=tuple(raw.get("cdc") or ()),
        limits=dict(raw.get("limits") or {}),
        defaults=dict(raw.get("defaults") or {}),
        notes=tuple(raw.get("notes") or ()))


@lru_cache(maxsize=1)
def services():
    """{id: Service} of every entry, in the catalogue's order."""
    return {sid: _service(sid, raw) for sid, raw in
            (catalogue().get("services") or {}).items()}


def get(sid):
    got = services().get(str(sid or ""))
    if got is None:
        raise SystemExit(f"{sid} is not a managed service migkit knows;"
                         " it knows " + ", ".join(sorted(services())[:12])
                         + " and more (`migkit/data/managed.yaml`)")
    return got


def family_of(name):
    """The engine family migkit moves `name` as."""
    from .engines import ALIASES
    name = str(name or "")
    return ALIASES.get(name, name)


def owner(family):
    """A server run by its owner, of this family."""
    return Service(family=family_of(family))


def find(cloud, where, family):
    """The catalogue's entry for a side `clouds` says `cloud`/`where` runs,
    of this engine family, or None."""
    where = ENDPOINTS.get(where, where)
    family = family_of(family)
    for s in services().values():
        if s.provider == cloud and s.where == where and s.family == family:
            return s
    return None


def distinct():
    """The services the catalogue lists that are engines migkit lacks."""
    return [s for s in services().values() if s.distinct]


# --- recognition -------------------------------------------------------------

def host_patterns():
    """(pattern, cloud, where) from the catalogue for addresses `clouds`
    does not name itself; hints only, after its own."""
    out, seen = [], set()
    for s in services().values():
        for p in s.hosts:
            if (p, s.provider) not in seen:
                seen.add((p, s.provider))
                out.append((p, s.provider, s.where))
    return out


def signs(kind):
    """{sign: (cloud, where)} of one kind - `pg_roles`, `pg_functions`,
    `my_variables`, `my_version_comment`, `mongo_build_info` - as the
    catalogue gives them; a sign two services share names the first."""
    out = {}
    for s in services().values():
        for sign in (s.signs or {}).get(kind) or ():
            out.setdefault(str(sign), (s.provider, s.where))
    return out


@dataclass
class Side:
    """One side of a hop as the catalogue and the server see it."""
    which: str
    service: Service
    where: object = None           #: `clouds.Where`
    how: str = ""                  #: how it is known, in words
    disagree: str = ""             #: the operator's word against the server's
    options: dict = field(default_factory=dict)   #: its `managed:` option


def _side_opts(ep):
    got = (ep.options or {}).get("managed")
    return dict(got) if isinstance(got, dict) else {}


def _family(e, hop):
    from .engines import ALIASES
    got = getattr(e, "CANON_ENGINE", "") or hop.engine
    return ALIASES.get(got, got)


def side(hop, eng, which):
    """The `Side` a hop's source or target is: the server's word
    (`clouds.of`), the side's `managed.service` where the server says
    nothing, its address otherwise. Read once a run."""
    which = "source" if which in ("src", "source") else "target"
    kept = hop.__dict__.setdefault("_managed_sides", {})
    if which in kept:
        return kept[which]
    from . import clouds
    from .guide import _looking_at
    ep = hop.source if which == "source" else hop.target
    opts = _side_opts(ep)
    try:
        e, key = _looking_at(eng, hop, which)
        family = _family(e, hop)
        where = clouds.of(e, key, ep)
    except Exception:  # noqa: BLE001 - the address still says
        e, family, where = None, family_of(hop.engine), clouds.from_host(
            ep.host)
    named = opts.get("service")
    by_server = [x for x in where.evidence if x.startswith("the server")]
    found = find(where.cloud, where.service, family) if where.cloud else None
    disagree = ""
    if named:
        mine = get(named)
        if mine.family != family:
            what = mine.kind or "an engine migkit does not have"
            raise SystemExit(f"the {which} is named {mine.label} ({what}),"
                             f" and it runs {KINDS.get(family, family)} -"
                             " name the service it is (managed: {service:"
                             " ...})")
        if found is None:
            found, how = mine, "the hop's managed option"
        elif mine.id == found.id:
            how = ("the server's own answer" if by_server
                   else "the hop's managed option")
        elif by_server:
            disagree = (f"the hop names {mine.label}, and the server says"
                        f" {found.label} ({', '.join(by_server)}): the"
                        " server's word stands")
            how = "the server's own answer"
        else:
            disagree = (f"the address looks like {found.label}; the hop"
                        f" names {mine.label}, which stands")
            found, how = mine, "the hop's managed option"
    else:
        how = ("the server's own answer" if by_server else
               "its address" if found is not None else "")
    if found is None and where.cloud:
        # a provider the catalogue has no entry for this family of
        found = Service(provider=where.cloud, where=where.service,
                        label=where.label, family=family)
    if found is not None and how == "the hop's managed option":
        # what the DBA's script and the ladders spell by is the service
        # the hop names, where nothing the server says stands against it
        where = clouds.Where(found.provider, found.where,
                             ["the hop's managed option"], where.region)
    got = Side(which, found or owner(family), where, how, disagree, opts)
    kept[which] = got
    return got


def pair_said(src, dst):
    """The pair as the plan names it."""
    return f"{src.said()} to {dst.said()}"


# --- a setting, in the service's own words ------------------------------------

def fill(template, values):
    """`template` with its `{placeholder}`s (`PLACEHOLDERS`) filled; any
    other brace - a JSON body in a command - left as it is."""
    names = "|".join(PLACEHOLDERS)
    return re.sub(r"\{(" + names + r")\}",
                  lambda m: str(values.get(m.group(1), m.group(0))),
                  str(template))


def service_of(where, engine):
    """The Service a `clouds.Where` stands for, of this engine, or None
    for a server run by its owner."""
    cloud = getattr(where, "cloud", "") or ""
    if not cloud:
        return None
    service = getattr(where, "service", "") or ""
    got = find(cloud, service, engine)
    if got is not None:
        return got
    return Service(provider=cloud, where=ENDPOINTS.get(service, service),
                   label=getattr(where, "label", "") or cloud,
                   family=family_of(engine))


def spell(engine, where, name, value, restart, label):
    """(statements run on the server, lines said about the cloud) that set
    `name` to `value` on a managed side, in its provider's words - or None
    where nobody's service runs it and the server takes the statement
    itself (`dba._setting`)."""
    s = service_of(where, engine)
    if s is None:
        return None
    alias = ((s.change.get("names") or {}).get(name) or {}).get(str(value))
    if alias:
        name, value = alias[0], alias[1]
    how = str(s.change.get("how") or "")
    if how == "configuration file":
        return None
    values = {"name": name, "value": value, "label": label,
              "after": " (the server restarts to take it)" if restart else "",
              "apply": "pending-reboot" if restart else "immediate",
              "kind": engine,
              "restart_flag": " --Forcerestart true" if restart else ""}
    words = s.change.get("words") or (
        f"{s.console}, the instance's settings: {{name}} = {{value}}{{after}}")
    lines = [fill(words, values)]
    if s.change.get("cli"):
        lines.append(fill(s.change["cli"], values))
    return [], lines


#: how a catalogue template that is a statement begins; any other is
#: words, said to the DBA rather than run
STATEMENTS = ("grant ", "revoke ", "alter ", "call ", "set ", "create ",
              "select ", "exec ", "db.")


def statement(template, who="", **more):
    """A catalogue template of a statement (`admin.replication`,
    `admin.binlog_retention`): (statements, lines said about the cloud) -
    a template in words ("console: ...") is said, not run."""
    text = fill(str(template or ""), {"who": who, **more}).strip()
    if not text:
        return [], []
    low = text.lower()
    if low.startswith(STATEMENTS):
        return [text], []
    return [], [text.split(":", 1)[1].strip()
                if low.startswith(("console:", "api:")) else text]


def undo_of(statement_):
    """The statement that takes a grant or a role's attribute off again,
    or None where there is none to write."""
    s = str(statement_).strip().rstrip(";")
    m = re.match(r"grant (.+) to (.+)$", s, re.I)
    if m:
        return f"revoke {m.group(1)} from {m.group(2)};"
    m = re.match(r"(alter (?:role|user) .+ with) replication$", s, re.I)
    if m:
        return f"{m.group(1)} noreplication;"
    return None


# --- the freeze a service's admin allows ---------------------------------------

#: the commands a MongoDB server lists that the holds lean on
MONGO_COMMANDS = ("setUserWriteBlockMode", "fsync", "fsyncUnlock",
                  "killSessions", "killOp", "currentOp", "grantRolesToUser",
                  "revokeRolesFromUser", "usersInfo", "listCommands")


def _version(text):
    try:
        return tuple(int(x) for x in str(text).split(".")[:2])
    except ValueError:
        return (0, 0)


def _procedure(template):
    """The name of the procedure a `kill` template calls, or None."""
    m = re.match(r"\s*call\s+([\w.`]+)\s*\(", str(template or ""), re.I)
    return m.group(1).replace("`", "") if m else None


def stand_in(service):
    """The facts a server with this service's admin would answer, as the
    freeze ladder of its family reads them (`freeze.Hold.readers`): what
    `plan` climbs, and what the tests hold every service to."""
    from .decide import IMPOSSIBLE, POSSIBLE, Stand
    a = service.admin or {}
    sup = bool(a.get("superuser"))
    f = service.family
    if f == "postgres":
        return {"others": [], "alter system": sup or bool(a.get(
                    "alter_system")),
                "owner roles": True,
                "connect revoke": sup or bool(a.get("owns_databases")),
                "alter roles": sup or bool(a.get("createrole")),
                "roles elsewhere": [],
                "signal": sup or bool(a.get("signal_backend"))}
    if f == "mysql":
        ends = ("kill" if sup or a.get("super") or a.get("connection_admin")
                or str(a.get("kill") or "").lower().startswith("kill")
                else _procedure(a.get("kill")))
        return {"others": [], "read only switch": sup or bool(
                    a.get("super") or a.get("system_variables_admin")),
                "kill": ends, "stream back": False,
                "grant option": sup or bool(a.get("grant_option")),
                "change param_read_only":
                    Stand(POSSIBLE) if a.get("read_only_by_api") and
                    service.product else
                    Stand(IMPOSSIBLE, "its read_only is not changed through"
                                      " an API migkit drives")}
    if f == "mongodb":
        missing = set(a.get("commands_missing") or ())
        actions = [x for x in ("setUserWriteBlockMode", "fsync")
                   if x not in missing]
        if "fsyncUnlock" not in missing and "fsync" in actions:
            actions.append("unlock")
        if "setUserWriteBlockMode" in actions:
            actions.append("bypassWriteBlockingMode")
        return {"others": [], "version": _version(a.get("version") or "0.0"),
                "actions": actions if sup or a.get("user_roles") else [],
                "commands": sorted(set(MONGO_COMMANDS) - missing),
                # a service checks who signs in, always
                "auth": True}
    if f == "redis":
        return {"others": [], "version": _version(a.get("version") or "0.0"),
                "acl": bool(a.get("acl")), "stream back": False,
                "one server": not a.get("cluster", False)}
    if f == "mssql":
        return {"others": [], "alter": bool(a.get("alter")),
                "alone in its group": not a.get("availability_group"),
                "control": bool(a.get("control")), "kill": bool(a.get("kill"))}
    if f == "kafka":
        return {"others": [], "authorizer": bool(a.get("authorizer"))}
    if f == "cassandra":
        return {"others": [], "authorizer": bool(a.get("authorizer", True)),
                "authorize": bool(a.get("authorize", True)), "cache": 2.0,
                "budget": 30.0}
    return {"others": []}


class _Stub:
    """A side engine of a family, for the ladder's rungs alone."""

    def __init__(self, family):
        self.CANON_ENGINE = family


def freeze_way(service, need=()):
    """(the rung the source's writes would be held by, why) - the freeze
    ladder of the service's family climbed over what its admin gives
    (`stand_in`); (None, why) where none holds or migkit holds no writes
    of the family (then the operator stops them, and migkit proves it)."""
    from . import decide, freeze
    cls = freeze.HOLDS.get(service.family)
    if cls is None:
        return None, freeze.not_held(service.family)
    rungs = cls(None, _Stub(service.family)).rungs()
    facts = decide.Facts(stand_in(service))
    got = decide.climb(f"holding the writes of {service.said()}", rungs,
                       facts, need, prove=False)
    return (got.rung.name if got.rung else None), got.reason


def freeze_offer(service):
    """The way a change on the source would buy, in words, or "": the
    ladder climbed again as if every change the admin may make were
    answered yes (`decide.offered`)."""
    from . import decide, freeze
    cls = freeze.HOLDS.get(service.family)
    if cls is None:
        return ""
    rungs = cls(None, _Stub(service.family)).rungs()
    got = decide.offered(f"holding the writes of {service.said()}", rungs,
                         decide.Facts(stand_in(service)))
    if got is None:
        return ""
    names = ", ".join(c.name for c in got.changes)
    return (f"{got.rung.said} is offered, needing {names} on the source"
            f" (source_changes: {{{names}: yes}})")


# --- the pair as one -----------------------------------------------------------

@dataclass
class Plan:
    """What a pair of services is, as one: `route` the engine it runs
    through, `source` what following the source needs, `target` what the
    target's admin can and cannot do, `freeze` the way the source's writes
    are held (`freeze_way` the rung, None where the operator stops them),
    `restore` what each side offers, `lacks` the engines migkit does not
    have among the two."""
    pair: str
    route: str
    source: list
    target: list
    freeze: str
    freeze_way: object
    offer: str
    restore: dict
    lacks: list


#: what following a family's changes needs, where a service says nothing
FOLLOWS = {
    "postgres": "a log that carries rows (wal_level logical), the right to"
                " read it, a free replication slot and WAL sender",
    "mysql": "the binary log on, in rows (binlog_format ROW,"
             " binlog_row_image FULL), kept a day or more, and the right to"
             " read it (replication slave, replication client)",
    "mongodb": "a replica set's change streams, and the right to read them",
    "mssql": "change tracking on the database and its tables",
}


def _source_lines(s):
    if s.distinct:
        return [f"{s.label} is {s.distinct}: migkit has no engine for it"]
    lines = list(s.cdc)
    if not lines:
        lines = [f"following its changes needs {FOLLOWS.get(s.family, 'what its engine keeps of them')}"
                 + (f", each changed through {s.change.get('how') or s.console}"
                    if s.managed else ", each set on the server itself")]
    return lines


def _target_lines(s):
    if s.distinct:
        return [f"{s.label} is {s.distinct}: migkit has no engine for it"]
    a = s.admin or {}
    out = []
    if not s.managed:
        out.append("run by its owner: its settings are the server's own,"
                   " changed on it")
        return out
    role = a.get("role") or ""
    if a.get("superuser"):
        out.append("its admin is a superuser")
    else:
        out.append("its admin is not a superuser - the service keeps that"
                   " for itself" + (f"; {role} stands in for it" if role
                                    else ""))
    if s.family == "postgres":
        out.append("its admin may create a subscription" if a.get(
            "subscribe") else "its admin may not create a subscription: the"
            " stream into it is migkit's own")
    if s.family == "mysql" and not (a.get("super") or a.get("superuser")):
        out.append("no SUPER: a global setting is changed through "
                   + (s.change.get("how") or s.console))
    how = s.change.get("how")
    if how:
        out.append(f"a setting is changed through its {how}"
                   f" ({s.console}) - offered, never assumed")
    for k in ("max_connections", "storage"):
        if s.limits.get(k):
            out.append(f"{k.replace('_', ' ')}: {s.limits[k]}")
    said = ", ".join(f"{k.replace('_', ' ')} {v}" for k, v in
                     s.defaults.items() if v)
    if said:
        out.append(f"it creates with {said}")
    return out


def _restore_words(s):
    if not s.managed:
        return "run by its owner: a restore point is the owner's backup"
    how = (s.api or {}).get("restore_point") or ""
    api = (providers().get(s.provider) or {}).get("api")
    if how and api and api != "none" and s.product and _drives(api, s):
        return (f"a {how} through {s.provider_label}'s API (cutover:"
                " {restore_points: [...]})")
    if how:
        return f"a {how}, taken in {s.console}"
    return f"none through an API: {s.console}"


def held(src):
    """(way, why, offer) of a source's freeze (`freeze_way`,
    `freeze_offer`), what `plan` says of it."""
    if src.distinct:
        return None, "", ""
    way, why = freeze_way(src)
    return way, why, freeze_offer(src)


def _drives(api, s):
    """Whether migkit's provider API layer takes this service's restore
    point itself (`cloudapi.offered`), where the catalogue says the
    service has one."""
    from . import cloudapi
    try:
        return cloudapi.offered(api, s.product, "restore_point")
    except (KeyError, SystemExit):
        return False


def plan(src, dst, holds=None):
    """The `Plan` of a pair of services (`Service`, or `Side`); `holds`
    the source's `held`, where it is known already."""
    src = getattr(src, "service", src)
    dst = getattr(dst, "service", dst)
    same = src.family == dst.family and not src.distinct \
        and not dst.distinct and src.family not in ("", "none")
    way, why, offer = holds or held(src)
    if src.distinct:
        words = "no engine of migkit's reads it"
    elif way:
        words = f"{_rung_said(src, way)} ({why})"
    else:
        words = (f"the operator stops the writes, and migkit proves it"
                 f" (freeze: operator) - {why}")
    return Plan(pair_said(src, dst), src.family if same else "hetero",
                _source_lines(src), _target_lines(dst), words, way, offer,
                {"source": _restore_words(src),
                 "target": _restore_words(dst)},
                [f"{s.label}: {s.distinct}" for s in (src, dst)
                 if s.distinct])


def _rung_said(service, name):
    from . import freeze
    cls = freeze.HOLDS.get(service.family)
    for r in cls(None, _Stub(service.family)).rungs() if cls else ():
        if r.name == name:
            return r.said
    return name


def matrix():
    """{(source id, target id): Plan} of every pair of catalogued services
    whose engines migkit has - what the tests hold the catalogue to."""
    usable = [s for s in services().values() if not s.distinct
              and s.family not in ("", "none")]
    ways = {a.id: held(a) for a in usable}
    return {(a.id, b.id): plan(a, b, ways[a.id]) for a in usable
            for b in usable}


# --- the provider's API for a side ---------------------------------------------

def api_conf(hop, eng, which):
    """(provider's API name, the adapter's configuration) of a side, or
    (None, why not)."""
    s = side(hop, eng, which)
    svc = s.service
    if not svc.managed:
        return None, ("it is run by its owner: no provider's API holds it")
    api = (providers().get(svc.provider) or {}).get("api") or ""
    if not api or api == "none":
        return None, (f"migkit drives no API of {svc.provider_label}'s:"
                      f" {svc.console} does it")
    if not svc.product:
        return None, f"{svc.provider_label}'s API offers {svc.label} nothing"
    conf = {k: v for k, v in s.options.items() if k != "service"}
    conf.setdefault("product", svc.product)
    return api, conf


def adapter(hop, eng, which, find=True):
    """The provider's API for a side (`cloudapi.Adapter`), its instance
    named by the side's `managed.id` or found by its address - SystemExit
    with why where there is none."""
    from . import cloudapi
    api, conf = api_conf(hop, eng, which)
    if api is None:
        raise SystemExit(f"the {which}: {conf}")
    got = cloudapi.adapter(api, conf)
    if conf.get("id") or not find:
        return got
    ep = hop.source if which in ("src", "source") else hop.target
    found = got.find(ep.host, ep.port)
    if not found:
        raise SystemExit(f"the {which}: {ep.host}:{ep.port} is no instance"
                         " the provider's API lists for these credentials -"
                         f" name it under the side's managed option (id)")
    return cloudapi.adapter(api, {**conf, "id": found})


def restore_ready(hop, eng, which):
    """"" where a restore point of the side can be taken through its
    provider's API, else why not. Asks the provider for the instance, and
    nothing is made."""
    from . import cloudapi
    api, conf = api_conf(hop, eng, which)
    if api is None:
        return conf
    if not cloudapi.offered(api, conf["product"], "restore_point"):
        return (f"{side(hop, eng, which).service.label} offers no restore"
                " point through its API")
    try:
        a = adapter(hop, eng, which)
        a.credentials()
    except SystemExit as e:
        return str(e)
    except Exception as e:  # noqa: BLE001 - said as the reason
        return _last(e)
    return ""


def _safe(name):
    return re.sub(r"[^a-z0-9-]+", "-", str(name).lower()).strip("-")[:40]


def restore_points(hop, eng, sides, say):
    """A restore point of each of `sides` taken through its provider's
    API and waited for: {side: what the provider made}. Before a cutover's
    clock (`cutover`), so the way back is a restore point the provider
    keeps, not only migkit's own stream back."""
    from . import audit
    out = {}
    for which in sides:
        which = "source" if which in ("src", "source") else "target"
        a = adapter(hop, eng, which)
        name = (f"migkit-{_safe(hop.name)}-{which}-"
                f"{time.strftime('%Y%m%d%H%M%S')}")
        got = a.restore_point(name, say)
        svc = side(hop, eng, which).service
        out[which] = {"id": got.id, "kind": got.kind, "at": got.at,
                      "service": svc.label, "provider": svc.provider}
        say(f"{which}: {got.kind} {got.id} taken through"
            f" {svc.provider_label}'s API - the way back to where it stands"
            " now")
        audit.append(hop.report_dir() / "changelog.jsonl",
                     {"op": "restore-point", "side": which, **out[which]})
    return out


# --- what the servers say, against the pair ------------------------------------

def _last(e):
    text = str(e).strip()
    return (text.splitlines() or [type(e).__name__])[-1][:160]


def _pg_one(e, key, db, sql):
    return e._psql(key, db, sql).strip()


def time_zone(e, key, family, db):
    """The time zone the server reads a timestamp without one in, or None
    where the family keeps none."""
    if family == "postgres":
        if hasattr(e, "_conn"):
            # a session of the server's own default: migkit's statements
            # through its client read every time in UTC, set per session
            conn = e._conn(key, db)
            try:
                with conn.cursor() as cur:
                    cur.execute("show timezone")
                    return str(cur.fetchone()[0])
            finally:
                conn.close()
        return _pg_one(e, key, db, "show timezone")
    if family == "mysql":
        tz, system = e._q(key, "select @@global.time_zone,"
                               " @@system_time_zone")[0]
        return str(system if str(tz).upper() == "SYSTEM" else tz)
    return None


def connections(e, key, family, db):
    """(the most the server takes, in use now), or None where it cannot
    be read."""
    if family == "postgres":
        most = int(_pg_one(e, key, db, "show max_connections"))
        now = int(_pg_one(e, key, db, "select count(*) from pg_stat_activity"
                                      " where backend_type ="
                                      " 'client backend'"))
        return most, now
    if family == "mysql":
        most = int(e._q(key, "select @@max_connections")[0][0])
        now = int(e._q(key, "select count(*) from"
                            " information_schema.processlist")[0][0])
        return most, now
    if family == "mongodb":
        got = e._client(key).admin.command("serverStatus").get(
            "connections") or {}
        now = int(got.get("current") or 0)
        return now + int(got.get("available") or 0), now
    if family == "mssql":
        most = int(e._rows(key, db, "select @@max_connections")[0][0])
        now = int(e._rows(key, db, "select count(*) from"
                                   " sys.dm_exec_sessions where"
                                   " is_user_process = 1")[0][0])
        return most, now
    return None


def used(e, key, family, db):
    """((its character set, its collation), {every collation its objects
    use}) of one database, or None where the family keeps none."""
    if family == "mysql":
        name = e._d(key, db) if hasattr(e, "_d") else db
        cs, coll = (e._q(key, "select default_character_set_name,"
                              " default_collation_name from"
                              " information_schema.schemata where"
                              " schema_name = %s", (name,)) or
                    [("", "")])[0]
        got = {str(r[0]) for r in e._q(
            key, "select distinct collation_name from"
                 " information_schema.columns where table_schema = %s and"
                 " collation_name is not null union select distinct"
                 " table_collation from information_schema.tables where"
                 " table_schema = %s and table_collation is not null",
            (name, name))}
        return (str(cs), str(coll)), got | ({str(coll)} if coll else set())
    if family == "postgres":
        enc, coll, ctype = _pg_one(
            e, key, db, "select pg_encoding_to_char(encoding)||chr(31)||"
                        "datcollate||chr(31)||datctype from pg_database where"
                        " datname = current_database()").split("\x1f")
        return (enc, coll), {coll, ctype}
    if family == "mssql":
        got = e._rows(key, db, "select convert(nvarchar(128),"
                               " databasepropertyex(db_name(),"
                               " 'Collation'))")[0]
        return ("", str(got[0])), {str(got[0])}
    return None


def made_with(e, key, family):
    """(character set, collation) a database the server makes without
    naming one is made with, or None where the family keeps none."""
    if family == "mysql":
        cs, coll = e._q(key, "select @@character_set_server,"
                             " @@collation_server")[0]
        return str(cs), str(coll)
    if family == "postgres":
        enc, coll = _pg_one(
            e, key, "postgres", "select pg_encoding_to_char(encoding)||"
                                "chr(31)||datcollate from pg_database where"
                                " datname = 'template1'").split("\x1f")
        return enc, coll
    if family == "mssql":
        got = e._rows(key, "master", "select convert(nvarchar(128),"
                                     " serverproperty('Collation'))")[0]
        return "", str(got[0])
    return None


def lacking(e, key, family, names):
    """The names of `names` the server has no collation or locale of."""
    if family == "mysql":
        have = {str(r[0]) for r in e._q(
            key, "select collation_name from information_schema.collations")}
        return sorted(n for n in names if n not in have)
    if family == "postgres":
        have = set(_pg_one(e, key, "postgres",
                           "select string_agg(distinct collcollate, chr(31))"
                           " from pg_collation where collcollate is not null"
                           ).split("\x1f"))
        norm = {re.sub(r"[-_.]", "", h.lower()) for h in have}
        return sorted(n for n in names if n not in ("C", "POSIX")
                      and re.sub(r"[-_.]", "", n.lower()) not in norm)
    if family == "mssql":
        have = {str(r[0]) for r in e._rows(
            key, "master", "select name from sys.fn_helpcollations()")}
        return sorted(n for n in names if n not in have)
    return []


def items(hop, eng, dbs, quick=False):
    """[ready.Item] of the pair: the services each side is and how that is
    known, the pair's plan, and what the servers say against it - the time
    zone, the character sets and collations, the connections the target
    takes, the restore points the hop asks for. With `quick`, only what
    would stop a copy: a collation the source's tables use that the target
    does not have."""
    from .ready import BEFORE, CUTOVER, Item
    out = []
    try:
        src, dst = side(hop, eng, "source"), side(hop, eng, "target")
    except SystemExit as e:
        return [Item("fail", BEFORE, "the services", str(e), gates=True)]
    except Exception as e:  # noqa: BLE001 - unknown, said so
        return [Item("warn", BEFORE, "the services",
                     f"could not be read: {_last(e)}")]
    for part in ((lambda: _plan_items(hop, eng, src, dst)) if not quick
                 else (lambda: []),
                 lambda: _server_items(hop, eng, dbs, src, dst, quick)):
        try:
            out += part()
        except (Exception, SystemExit) as e:  # noqa: BLE001 - said
            out.append(Item("warn", BEFORE, "the pair",
                            f"a part could not be read: {_last(e)}"))
    return out


def _plan_items(hop, eng, src, dst):
    from .ready import BEFORE, CUTOVER, Item
    p = plan(src, dst)
    out = [Item("pass", BEFORE, "the pair", p.pair + (
        "; through the pair engine, the two being different engines"
        if p.route == "hetero" else ""))]
    for s in (src, dst):
        if s.how and s.service.managed:
            out.append(Item("pass", BEFORE, f"{s.which}: the service",
                            f"{s.service.label}, known by {s.how}"))
        if s.disagree:
            out.append(Item("warn", BEFORE, f"{s.which}: the service",
                            s.disagree, "name the service the side is"
                                        " (managed: {service: ...}), or"
                                        " none"))
    for why in p.lacks:
        out.append(Item("fail", BEFORE, "the pair", why, gates=True))
    from .dba import STREAMS
    if (getattr(hop, "goal", "") or "copy") in STREAMS and src.service.managed:
        out.append(Item("pass", BEFORE, f"source: {src.service.label}",
                        "following its changes: " + "; ".join(p.source)))
    if dst.service.managed:
        out.append(Item("pass", BEFORE, f"target: {dst.service.label}",
                        "; ".join(p.target)))
    from . import cutover
    conf = cutover.settings(hop)
    if conf and str(conf.get("freeze")) != "operator":
        out.append(Item("pass", CUTOVER, "the source's freeze, as its"
                        " service allows", p.freeze + (
                            f"; {p.offer}" if p.offer else "")))
    for which in (conf or {}).get("restore_points") or ():
        why = restore_ready(hop, eng, which)
        out.append(Item("fail" if why else "pass", CUTOVER,
                        f"{which}: a restore point",
                        why or p.restore["source" if which in ("src",
                                                               "source")
                                         else "target"],
                        "name the side's instance and how its provider's"
                        " API is reached (managed: {id, region, profile})"
                        if why else ""))
    return out


def _server_items(hop, eng, dbs, src, dst, quick):
    from .guide import _looking_at
    from .ready import BEFORE, CUTOVER, Item
    out = []
    try:
        se, sk = _looking_at(eng, hop, "source")
        de, dk = _looking_at(eng, hop, "target")
    except Exception:  # noqa: BLE001 - nothing to ask
        return out
    sf, df = src.service.family, dst.service.family
    if sf != df:
        # across engines the pair engine maps every type and collation
        # itself (`canon`), and says what it cannot carry
        return out
    try:
        default = made_with(de, dk, df)
    except Exception:  # noqa: BLE001 - not said where not read
        default = None
    for d in dbs:
        try:
            got = used(se, sk, sf, d)
            lack = lacking(de, dk, df, got[1]) if got else []
        except Exception:  # noqa: BLE001 - not said where not read
            got, lack = None, []
        if lack:
            out.append(Item(
                "fail", BEFORE, f"{d}: collations",
                f"the target has no {', '.join(lack)}, which the source's"
                " database or tables use: a table made there would be"
                " refused, or sorted and compared another way",
                "a target of a version or an image that has it, or the"
                " tables' collation changed on the source first",
                gates=True))
        elif not quick and got and default and got[0][1] and \
                tuple(got[0]) != tuple(default):
            tdb = hop.target_db(d)
            cs, coll = got[0]
            out.append(Item(
                "warn", BEFORE, f"{d}: character set",
                f"the source's database is"
                f" {' / '.join(x for x in got[0] if x)}, and the target"
                f" makes one with {' / '.join(x for x in default if x)}"
                " unless told: a database made there without naming them"
                " sorts and compares text another way",
                {"postgres": f"create database {tdb} encoding '{cs}'"
                             f" lc_collate '{coll}' lc_ctype '{coll}'"
                             " template template0",
                 "mysql": f"create database `{tdb}` character set {cs}"
                          f" collate {coll}",
                 "mssql": f"create database [{tdb}] collate {coll}"
                 }.get(df, "")))
    if quick:
        return out
    try:
        sb, tb = eng._brands()
        older = _version(tb.version) < _version(sb.version) \
            and _version(tb.version) != (0, 0)
    except Exception:  # noqa: BLE001 - not said where not read
        older = False
    if older:
        out.append(Item("warn", BEFORE, "versions",
                        f"the target runs {tb.version}, older than the"
                        f" source's {sb.version}: what the source's schema"
                        " or data uses of the newer version may not be"
                        " there", "a target of the source's version or"
                        " newer, where the service offers one"))
    d = dbs[0] if dbs else ""
    try:
        tz = (time_zone(se, sk, sf, d),
              time_zone(de, dk, df, "postgres" if df == "postgres"
                        else hop.target_db(d)))
    except Exception:  # noqa: BLE001 - not said where not read
        tz = (None, None)
    if tz[0] and tz[1] and _tz(tz[0]) != _tz(tz[1]):
        out.append(Item("warn", CUTOVER, "time zone",
                        f"the source reads a time without a zone as"
                        f" {tz[0]}, the target as {tz[1]}: what the"
                        " application writes with now() or reads back"
                        " moves by the difference after the flip",
                        _fix(dst, df, "time_zone" if df == "mysql"
                             else "timezone", tz[0])))
    try:
        s_conn = connections(se, sk, sf, d)
        t_conn = connections(de, dk, df, "postgres" if df == "postgres"
                             else hop.target_db(d))
    except Exception:  # noqa: BLE001 - not said where not read
        s_conn = t_conn = None
    if s_conn and t_conn:
        need = s_conn[1] + 10
        if t_conn[0] < need:
            out.append(Item(
                "fail", CUTOVER, "connections",
                f"the target takes {t_conn[0]} connections, and the"
                f" source has {s_conn[1]} in use now: at the flip the"
                " application's are refused past the target's limit",
                "a larger class for the target, or its connection limit"
                " raised - " + _fix(dst, df, "max_connections", need)))
        elif t_conn[0] < s_conn[0]:
            out.append(Item(
                "warn", CUTOVER, "connections",
                f"the target takes {t_conn[0]}, the source {s_conn[0]}:"
                f" {s_conn[1]} are in use on the source now, but its"
                " busiest hour may take more than the target will",
                _fix(dst, df, "max_connections", s_conn[0])))
        else:
            out.append(Item("pass", CUTOVER, "connections",
                            f"the target takes {t_conn[0]}, the source has"
                            f" {s_conn[1]} in use"))
    return out


def _tz(name):
    return {"utc": "utc", "etc/utc": "utc", "+00:00": "utc", "gmt": "utc",
            "z": "utc", "universal": "utc", "zulu": "utc", "prc":
            "asia/shanghai"}.get(str(name).lower(), str(name).lower())


def _fix(side_, family, name, value):
    """How the target's setting is changed, in its provider's words."""
    s = side_.service
    got = spell(family, side_.where if s.managed else None, name, value,
                False, "the target")
    if got is None:
        return (f"set {name} = {value} on the target"
                + ("" if not s.managed else f" ({s.console})"))
    stmts, lines = got
    return "; ".join(stmts + lines)
