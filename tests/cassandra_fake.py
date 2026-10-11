"""An in-memory stand-in for a Cassandra cluster, for the tests that run
with no server.

It answers only the statements migkit's Cassandra engine sends and that
have been run against a real cluster in the docker tests of this
directory: the schema tables it reads (`system_schema.*`, `system.local`,
`system_views.settings`), a table read whole, a row read by its key, an
insert (with `using ttl ? and timestamp ?`), a delete, `truncate` and
`create table`. A statement it does not know raises, so a test never
passes on a statement nobody has run anywhere.

`install(monkeypatch, src, dst)` puts the driver's modules (`cassandra`,
`.query`, `.concurrent`) in place of the real ones and makes
`CassandraEngine._session` answer a `Cluster` per side.
"""
import re
import sys
import types

KEY, CLUSTERING, REGULAR, STATIC = ("partition_key", "clustering", "regular",
                                    "static")


class Unknown(Exception):
    """A statement this fake does not know."""


class Table:
    def __init__(self, name, columns, partition, clustering=()):
        self.name = name
        #: [(name, type, kind, position)]
        self.columns = []
        for i, k in enumerate(partition):
            self.columns.append((k, columns[k], KEY, i))
        for i, k in enumerate(clustering):
            self.columns.append((k, columns[k], CLUSTERING, i))
        for n, t in columns.items():
            if n not in partition and n not in clustering:
                self.columns.append((n, t, REGULAR, -1))
        self.partition, self.clustering = list(partition), list(clustering)
        self.key = self.partition + self.clustering
        #: key tuple -> {column: value}
        self.rows = {}
        #: (key tuple, column) -> (write time in microseconds, ttl seconds)
        self.times = {}
        self.options = {"default_time_to_live": 0, "gc_grace_seconds": 864000,
                        "compaction": {"class": "SizeTieredCompactionStrategy"}}
        self.indexes, self.triggers = {}, {}

    def names(self):
        return [c[0] for c in self.columns]


class Cluster:
    """One side's cluster: keyspaces of tables, and the schema objects the
    deep checks read."""

    def __init__(self, name="cluster", release="4.1.5", scylla=False,
                 replication=None):
        self.name, self.release, self.scylla = name, release, scylla
        self.keyspaces = {}
        self.replication = replication or {
            "class": "org.apache.cassandra.locator.SimpleStrategy",
            "replication_factor": "3"}
        self.views, self.types, self.functions, self.aggregates = \
            {}, {}, {}, {}
        self.roles, self.permissions = {}, []
        self.settings = {"partitioner": "Murmur3Partitioner"}
        self.log = []
        self.peers = []
        #: the schema version of this node, then of each peer
        self.schema_versions = ["v1"]
        self.clients, self.pools = 0, {}
        #: schema tables that answer with an error, and the beginnings of
        #: statements the cluster refuses
        self.broken, self.refuse = set(), []
        self.now_ms = 1_700_000_000_000
        self.metadata = _Metadata(self)

    def refresh_keyspace_metadata(self, ks):
        pass

    def refresh_table_metadata(self, ks, table):
        pass

    def keyspace(self, ks):
        self.keyspaces.setdefault(ks, {})
        return self.keyspaces[ks]

    def add_type(self, ks, name, fields):
        self.types[(ks, name)] = {
            "keyspace_name": ks, "type_name": name,
            "field_names": [f for f, _ in fields],
            "field_types": [t for _, t in fields]}

    def add_function(self, ks, name, args, returns, body="return 1;",
                     language="java"):
        self.functions[(ks, name)] = {
            "keyspace_name": ks, "function_name": name,
            "argument_names": [a for a, _ in args],
            "argument_types": [t for _, t in args], "return_type": returns,
            "language": language, "body": body,
            "called_on_null_input": False}

    def add_view(self, ks, name, base, where="id IS NOT NULL"):
        self.views[(ks, name)] = {
            "keyspace_name": ks, "view_name": name, "base_table_name": base,
            "where_clause": where, "include_all_columns": True}

    def add_role(self, role, grants=(), login=True, superuser=False):
        """A role, and its permissions as ("SELECT", "data/shop/orders")."""
        self.roles[role] = {"role": role, "can_login": login,
                            "is_superuser": superuser, "member_of": [],
                            "salted_hash": "hash-" + role}
        for perm, resource in grants:
            self.permissions.append({"role": role, "resource": resource,
                                     "permissions": {perm}})

    def table(self, ks, name, columns, partition, clustering=(), rows=(),
              at=1_000_000, ttl=None):
        """Make a table, and fill it with `rows` (dicts), every cell
        written at `at` microseconds (with `ttl` seconds to live)."""
        t = Table(name, dict(columns), partition, clustering)
        self.keyspace(ks)[name] = t
        for r in rows:
            self.put(t, r, at, ttl)
        return t

    @staticmethod
    def put(t, row, at=1_000_000, ttl=None):
        """One row written to a table, each cell at `at` microseconds."""
        key = tuple(row[k] for k in t.key)
        t.rows[key] = dict(row)
        for c in row:
            if c not in t.key:
                t.times[(key, c)] = (at, ttl)

    def session(self):
        return Session(self)


class Row(tuple):
    """A driver row: a tuple that also answers to its column names."""

    def __new__(cls, names, values):
        self = super().__new__(cls, values)
        self._names = tuple(names)
        return self

    def __getattr__(self, name):
        try:
            return self[self._names.index(name)]
        except ValueError:
            raise AttributeError(name)

    def _asdict(self):
        return dict(zip(self._names, self))


class ResultSet(list):
    def __init__(self, names, rows):
        super().__init__(Row(names, r) for r in rows)
        self.column_names = list(names)

    def one(self):
        return self[0] if self else None


class Prepared:
    def __init__(self, query):
        self.query_string = query


class Simple:
    def __init__(self, query_string, fetch_size=None, **_):
        self.query_string = query_string
        self.fetch_size = fetch_size


UNSET = object()


def token_of(partition):
    """A stand-in for the partitioner: a stable place on the ring for a
    partition key, spread over the whole of it."""
    import zlib
    h = zlib.crc32(repr(partition).encode())
    return (h - 2 ** 31) * 2 ** 31


def _unquote(name):
    name = name.strip()
    if name.startswith('"') and name.endswith('"'):
        return name[1:-1].replace('""', '"')
    return name.lower()


def _split(text, sep=","):
    """`text` split at the top-level `sep`: not inside quotes, <> or ()."""
    out, depth, cur, quoted = [], 0, [], False
    for ch in text:
        if ch == '"':
            quoted = not quoted
        if not quoted:
            if ch in "<(":
                depth += 1
            elif ch in ">)":
                depth -= 1
            elif ch == sep and depth == 0:
                out.append("".join(cur).strip())
                cur = []
                continue
        cur.append(ch)
    if "".join(cur).strip():
        out.append("".join(cur).strip())
    return out


def _target(text):
    """(keyspace, table) of `"ks"."t"` or `ks.t`."""
    parts = re.findall(r'"(?:[^"]|"")*"|[^.\s]+', text.strip())
    parts = [p for p in parts if p != "."]
    if len(parts) == 2:
        return _unquote(parts[0]), _unquote(parts[1])
    return None, _unquote(parts[0])


class Session:
    def __init__(self, cluster):
        self.cluster = cluster
        self.sent = []

    # ---- the driver's surface -------------------------------------------

    def prepare(self, query):
        return Prepared(query)

    def execute(self, stmt, params=None):
        query = stmt if isinstance(stmt, str) else stmt.query_string
        self.sent.append(" ".join(query.split()))
        return self._run(" ".join(query.split()), list(params or ()))

    # ---- the interpreter -------------------------------------------------

    def _run(self, q, params):
        low = q.lower()
        if any(low.startswith(x) for x in self.cluster.refuse):
            raise Unknown("refused: " + q)
        if low.startswith("select"):
            return self._select(q, params)
        if low.startswith("insert into"):
            return self._insert(q, params)
        if low.startswith("delete"):
            return self._delete(q, params)
        if low.startswith("truncate"):
            ks, name = _target(q.split(None, 1)[1])
            self._table(ks, name).rows.clear()
            self._table(ks, name).times.clear()
            return ResultSet([], [])
        if low.startswith("create table"):
            return self._create(q)
        if low.startswith(("drop index", "drop trigger", "create index",
                           "create custom index", "create trigger",
                           "create type", "alter table",
                           "create materialized view")):
            self.cluster.log.append(q)
            return self._ddl(q)
        raise Unknown(q)

    def _ddl(self, q):
        """The effect of the schema statements on the objects the deep
        checks read."""
        c = self.cluster
        m = re.match(r'(?i)create (custom )?index (\S+) on (\S+) '
                     r"\((.*?)\)(?: using '(.*?)')?$", q)
        if m:
            ks, t = _target(m.group(3))
            opts = {"target": m.group(4).strip('"')}
            if m.group(5):
                opts["class_name"] = m.group(5)
            self._table(ks, t).indexes[_unquote(m.group(2))] = {
                "kind": "CUSTOM" if m.group(1) else "COMPOSITES",
                "options": opts}
        m = re.match(r"(?i)create trigger (\S+) on (\S+) using '(.*?)'$", q)
        if m:
            ks, t = _target(m.group(2))
            self._table(ks, t).triggers[_unquote(m.group(1))] = {
                "class": m.group(3)}
        m = re.match(r"(?i)drop trigger (?:if exists )?(\S+) on (\S+)$", q)
        if m:
            ks, t = _target(m.group(2))
            self._table(ks, t).triggers.pop(_unquote(m.group(1)), None)
        m = re.match(r"(?i)drop index (?:if exists )?(\S+)$", q)
        if m:
            ks, name = _target(m.group(1))
            for t in c.keyspaces.get(ks, {}).values():
                t.indexes.pop(name, None)
        m = re.match(r'(?i)create type (\S+) \((.*)\)$', q)
        if m:
            ks, name = _target(m.group(1))
            fields = [f.strip().split(" ", 1) for f in _split(m.group(2))]
            c.types[(ks, name)] = {
                "keyspace_name": ks, "type_name": name,
                "field_names": [_unquote(f[0]) for f in fields],
                "field_types": [f[1] for f in fields]}
        m = re.match(r"(?is)create materialized view (\S+) as select .*?"
                     r"from (\S+) where (.*?) primary key", q)
        if m:
            ks, name = _target(m.group(1))
            base = _target(m.group(2))[1]
            c.views[(ks, name)] = {
                "keyspace_name": ks, "view_name": name,
                "base_table_name": base, "where_clause": m.group(3).strip(),
                "include_all_columns": True}
        m = re.match(r"(?i)alter table (\S+) add (\S+) (.*)$", q)
        if m:
            ks, t = _target(m.group(1))
            table = self._table(ks, t)
            table.columns.append((_unquote(m.group(2)), m.group(3).strip(),
                                  REGULAR, -1))
        return ResultSet([], [])

    def _table(self, ks, name):
        try:
            return self.cluster.keyspaces[ks][name]
        except KeyError:
            raise Unknown(f"no table {ks}.{name}")

    def _create(self, q):
        q = re.sub(r"(?is)\)\s+with\s+.*$", ")", q)
        m = re.match(r"(?i)create table (?:if not exists )?(\S+?)\s*\((.*)\)$",
                     q)
        ks, name = _target(m.group(1))
        defs = _split(m.group(2))
        cols, partition, clustering = {}, [], []
        for d in defs:
            if d.lower().endswith(" primary key") and not \
                    d.lower().startswith("primary key"):
                d, partition = d[:-len(" primary key")], None
            if partition is None:
                partition = [_unquote(d.split(" ", 1)[0])]
            if d.lower().startswith("primary key"):
                inner = d[len("primary key"):].strip()[1:-1]
                parts = _split(inner)
                head = parts[0]
                partition = [_unquote(x) for x in _split(
                    head[1:-1] if head.startswith("(") else head)]
                clustering = [_unquote(x) for x in parts[1:]]
            else:
                n, _, t = d.partition(" ")
                if n.startswith('"'):
                    n, _, t = d[:d.rindex('"') + 1], None, d[
                        d.rindex('"') + 1:]
                cols[_unquote(n)] = t.strip()
        self.cluster.keyspace(ks)[name] = Table(name, cols, partition,
                                                clustering)
        return ResultSet([], [])

    # ---- select ------------------------------------------------------------

    def _select(self, q, params):
        m = re.match(r"(?i)select (.*?) from (\S+)(?: where (.*?))?"
                     r"(?: limit (\d+))?$", q)
        if not m:
            raise Unknown(q)
        items, target, where, limit = m.groups()
        items = _split(items)
        conds, tokens = [], []
        if where:
            for c in re.split(r"(?i) and ", where):
                tk = re.match(r"(?i)token\((.*?)\) (>=|<=) (%s|\?)$",
                              c.strip())
                if tk:
                    tokens.append((_split(tk.group(1)), tk.group(2)))
                    continue
                mm = re.match(r"(\S+) = (%s|\?)$", c.strip())
                if not mm:
                    raise Unknown(q)
                conds.append(_unquote(mm.group(1)))
        if len(conds) + len(tokens) != len(params):
            raise Unknown(f"{q} with {params}")
        want = dict(zip(conds, params[:len(conds)]))
        bounds = params[len(conds):]
        ks, name = _target(target)
        if ks and ks.startswith(("system", "system_schema")):
            rows = self._system(ks, name, q)
            rows = [r for r in rows if all(r.get(k) == v
                                           for k, v in want.items())]
            return self._project(items, rows, [], limit)
        t = self._table(ks, name)
        rows = [(key, r) for key, r in t.rows.items()
                if all(r.get(k) == v for k, v in want.items())]
        for (cols, op), bound in zip(tokens, bounds):
            rows = [(k, r) for k, r in rows
                    if (token_of(tuple(r[_unquote(c)] for c in cols)) >= bound
                        if op == ">=" else
                        token_of(tuple(r[_unquote(c)] for c in cols))
                        <= bound)]
        return self._project(items, [r for _, r in rows], rows, limit, t)

    def _project(self, items, rows, keyed, limit, table=None):
        names, getters = [], []
        for it in items:
            if it == "*":
                keys = list(rows[0]) if rows else []
                for k in keys:
                    names.append(k)
                    getters.append(lambda r, k=k, **_: r.get(k))
                continue
            fn = re.match(r"(?i)(writetime|ttl)\((.+)\)$", it)
            if fn and table is not None:
                col, what = _unquote(fn.group(2)), fn.group(1).lower()
                names.append(f"{what}_{col}")
                getters.append(lambda r, col=col, what=what, **kw: (
                    (table.times.get((kw["key"], col)) or (None, None))
                    [0 if what == "writetime" else 1]))
                continue
            if it.lower().startswith("count("):
                names.append("count")
                getters.append(None)
                continue
            if it.lower().startswith("tounixtimestamp"):
                names.append("system_tounixtimestamp_system_now")
                getters.append(lambda r, **_: self.cluster.now_ms)
                continue
            col = _unquote(it)
            names.append(col)
            getters.append(lambda r, col=col, **_: r.get(col))
        if getters and getters[0] is None:
            return ResultSet(["count"], [(len(rows),)])
        if getters and getters[0] is not None and any(
                it.lower().startswith("tounixtimestamp") for it in items):
            return ResultSet(names, [[self.cluster.now_ms]])
        out = []
        pairs = keyed if keyed else [(None, r) for r in rows]
        for key, r in pairs:
            out.append([g(r, key=key) for g in getters])
        if limit:
            out = out[:int(limit)]
        return ResultSet(names, out)

    # ---- the schema tables -------------------------------------------------

    def _system(self, ks, name, q):
        c = self.cluster
        full = f"{ks}.{name}"
        if full in c.broken:
            raise Unknown(f"cannot read {full}")
        if full == "system_schema.keyspaces":
            return [{"keyspace_name": k, "replication": dict(c.replication),
                     "durable_writes": True} for k in c.keyspaces]
        if full == "system_schema.tables":
            out = []
            for k, tables in c.keyspaces.items():
                for n, t in tables.items():
                    out.append({"keyspace_name": k, "table_name": n,
                                "id": n, **t.options})
            return out
        if full == "system_schema.columns":
            return [{"keyspace_name": k, "table_name": n, "column_name": cn,
                     "type": ty, "kind": kind, "position": pos,
                     "clustering_order": getattr(t, "orders", {}).get(
                         cn, "none")}
                    for k, tables in c.keyspaces.items()
                    for n, t in tables.items()
                    for cn, ty, kind, pos in t.columns]
        if full == "system_schema.indexes":
            return [{"keyspace_name": k, "table_name": n, "index_name": i,
                     "kind": d.get("kind", "COMPOSITES"),
                     "options": d.get("options", {})}
                    for k, tables in c.keyspaces.items()
                    for n, t in tables.items() for i, d in t.indexes.items()]
        if full == "system_schema.triggers":
            return [{"keyspace_name": k, "table_name": n, "trigger_name": i,
                     "options": d}
                    for k, tables in c.keyspaces.items()
                    for n, t in tables.items() for i, d in t.triggers.items()]
        if full == "system_schema.views":
            return [dict(v) for v in c.views.values()]
        if full == "system_schema.types":
            return [dict(v) for v in c.types.values()]
        if full == "system_schema.functions":
            return [dict(v) for v in c.functions.values()]
        if full == "system_schema.aggregates":
            return [dict(v) for v in c.aggregates.values()]
        if full == "system_schema.broken":
            raise Unknown(q)
        if full == "system_schema.scylla_tables":
            if not c.scylla:
                raise Unknown("no such table")
            return []
        if full == "system_views.clients":
            return [{"address": f"10.0.0.{i}"} for i in range(c.clients)]
        if full == "system_views.thread_pools":
            return [{"name": n, "active_tasks": a}
                    for n, a in c.pools.items()]
        if full == "system.local":
            row = {"cluster_name": c.name, "host_id": "node-0",
                   "release_version": c.release,
                   "schema_version": c.schema_versions[0]}
            if c.scylla:
                row["scylla_version"] = c.release
            return [row]
        if full == "system.peers":
            return [{"host_id": h, "schema_version": v}
                    for h, v in zip(c.peers, c.schema_versions[1:])]
        if full == "system_views.settings":
            return [{"name": k, "value": v} for k, v in c.settings.items()]
        if full == "system_auth.roles":
            return [dict(v) for v in c.roles.values()]
        if full == "system_auth.role_permissions":
            return [dict(v) for v in c.permissions]
        raise Unknown(q)

    # ---- writes ------------------------------------------------------------

    def _insert(self, q, params):
        m = re.match(r"(?i)insert into (\S+) \((.*?)\) values \((.*?)\)"
                     r"(?: using (.*))?$", q)
        if not m:
            raise Unknown(q)
        ks, name = _target(m.group(1))
        t = self._table(ks, name)
        cols = [_unquote(c) for c in _split(m.group(2))]
        n = len(cols)
        vals, rest = list(params[:n]), list(params[n:])
        ttl = stamp = None
        if m.group(4):
            for part in re.split(r"(?i) and ", m.group(4)):
                word = part.split()[0].lower()
                v = rest.pop(0)
                if word == "ttl":
                    ttl = v
                elif word == "timestamp":
                    stamp = v
        key = tuple(vals[cols.index(k)] for k in t.key)
        row = t.rows.setdefault(key, {})
        for c, v in zip(cols, vals):
            if v is UNSET:
                continue
            row[c] = v
            t.times[(key, c)] = (stamp if stamp is not None
                                 else self.cluster.now_ms * 1000,
                                 ttl or None)
        return ResultSet([], [])

    def _delete(self, q, params):
        m = re.match(r"(?i)delete (.*?)\s*from (\S+)(?: using timestamp"
                     r" (%s|\?))? where (.*)$", q)
        if not m:
            raise Unknown(q)
        _, target, using, where = m.groups()
        ks, name = _target(target)
        t = self._table(ks, name)
        args = list(params)
        if using:
            args.pop(0)
        want = {}
        for c in re.split(r"(?i) and ", where):
            col = _unquote(c.split("=")[0])
            want[col] = args.pop(0)
        for key in [k for k, r in t.rows.items()
                    if all(r.get(c) == v for c, v in want.items())]:
            del t.rows[key]
        return ResultSet([], [])


# ---- the driver's metadata: a table as the driver exports it ----------------

class _Exported:
    def __init__(self, cluster, ks, table):
        self.cluster, self.ks, self.table = cluster, ks, table

    def export_as_string(self):
        t = self.cluster.keyspaces[self.ks][self.table.name]
        cols = ",\n    ".join(f"{n} {ty}" for n, ty, *_ in t.columns)
        part = ", ".join(t.partition)
        key = (f"(({part}), {', '.join(t.clustering)})" if t.clustering
               else f"(({part}))")
        out = [f"CREATE TABLE {self.ks}.{t.name} (\n    {cols},\n"
               f"    PRIMARY KEY {key}\n) WITH bloom_filter_fp_chance ="
               " 0.01 AND cdc = {'enabled': 'true'} AND comment = ''"]
        for name, d in t.indexes.items():
            tgt = d["options"].get("target", "")
            klass = d["options"].get("class_name")
            out.append(f"CREATE CUSTOM INDEX {name} ON {self.ks}.{t.name}"
                       f" ({tgt}) USING '{klass}'" if klass else
                       f"CREATE INDEX {name} ON {self.ks}.{t.name} ({tgt})")
        for name, d in t.triggers.items():
            out.append(f"CREATE TRIGGER {name} ON {self.ks}.{t.name} USING"
                       f" '{d['class']}'")
        for (ks, name), v in self.cluster.views.items():
            if ks == self.ks and v["base_table_name"] == t.name:
                out.append(f"\nCREATE MATERIALIZED VIEW {ks}.{name} AS\n"
                           f"    SELECT *\n    FROM {ks}.{t.name}\n"
                           f"    WHERE {v['where_clause']}\n"
                           "    PRIMARY KEY (id)")
        return ";\n".join(out) + ";"


class _Keyspace:
    def __init__(self, cluster, ks):
        self.tables = {n: _Exported(cluster, ks, t)
                       for n, t in cluster.keyspaces[ks].items()}


class _Metadata:
    def __init__(self, cluster):
        self.cluster = cluster

    @property
    def keyspaces(self):
        return {ks: _Keyspace(self.cluster, ks)
                for ks in self.cluster.keyspaces}


# ---- the driver's modules ---------------------------------------------------

def _execute_concurrent_with_args(session, stmt, params, concurrency=None,
                                  **_):
    out = []
    for p in params:
        try:
            out.append((True, session.execute(stmt, p)))
        except Exception as e:  # noqa: BLE001 - the driver's (False, error)
            out.append((False, e))
    return out


def _execute_concurrent(session, pairs, concurrency=None, **_):
    out = []
    for stmt, p in pairs:
        try:
            out.append((True, session.execute(stmt, p)))
        except Exception as e:  # noqa: BLE001
            out.append((False, e))
    return out


def modules():
    """The driver's modules, as far as migkit's Cassandra engine imports
    them."""
    root = types.ModuleType("cassandra")
    query = types.ModuleType("cassandra.query")
    query.SimpleStatement = Simple
    query.UNSET_VALUE = UNSET
    conc = types.ModuleType("cassandra.concurrent")
    conc.execute_concurrent_with_args = _execute_concurrent_with_args
    conc.execute_concurrent = _execute_concurrent
    root.query, root.concurrent = query, conc
    return {"cassandra": root, "cassandra.query": query,
            "cassandra.concurrent": conc}


def install(monkeypatch, src, dst):
    """The driver's modules replaced, and every Cassandra engine's session
    one of the two clusters: `src` for the source side, `dst` for the
    target's."""
    from migkit.engines.cassandra import CassandraEngine
    for name, mod in modules().items():
        monkeypatch.setitem(sys.modules, name, mod)
    sessions = {"src": src.session(), "dst": dst.session()}
    monkeypatch.setattr(CassandraEngine, "_session",
                        lambda self, side: sessions[side])
    return sessions


def rows_of(table):
    """A table's rows as a list of dicts, by key."""
    return [dict(r) for _, r in sorted(table.rows.items(),
                                       key=lambda kv: repr(kv[0]))]

