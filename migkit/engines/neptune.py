"""Amazon Neptune as a side of a pair (backlog P0-CLOUDS): its property
graph through Gremlin (or read through openCypher), its RDF store through
SPARQL, and both followed through Neptune Streams.

A hop's databases are `graph` (the property graph) and `rdf` (the RDF
store). The property graph is two tables: `vertices` - an element's id,
its label and its properties - and `edges` - the same, and the ids of the
vertices it goes from and to. Properties are one JSON value, each key
holding its values as [kind, text], sorted, so a set-valued property
compares whatever order a server hands its values in; a kind is the
value's as the driver reads it (`boolean`, `integer`, `double`, `string`,
`date`, `uuid`, `decimal`) - an Int32 and an Int64 of one number are one
`integer`, which a driver cannot tell apart. Elements are compared by id:
the id is the key.

Written by Gremlin's own traversals, each element whole: made where it is
not there, every property of it dropped and set again - set-valued ones
by `set` cardinality - so an element written again lands on itself; a
batch of them in one traversal. An edge is written once both of its
vertices are there, waited for while the vertices' copy runs
(`ENDS_WAIT`), and refused, named, where they never come. A Gremlin
server reached without Neptune's signature (a Gremlin Server of Apache
TinkerPop's) is the same side.

The RDF store is one table, `quads`: subject, predicate, object and graph
as N-Triples terms (the default graph as ""), all four the key. Read by
SPARQL's SELECT, written by INSERT DATA and DELETE DATA. A blank node is
its label, which another store gives its own: a store of blank nodes
reads as different after a copy, and says so.

Neptune signs its requests (`auth: aws_iam`, the region from its address
or `region`); without it, it is reached as its security groups allow.

Its changes are Neptune Streams' (`neptunedata`): one log for the whole
cluster, each change at its commit number and its place in the commit, in
order - so the fence is the log's position. A property graph's change
names an element, read back by its id as it is (Neptune reads no element
as it stood at a commit, and later changes level it again); an RDF change
is a quad added or removed, which is the whole of it. The streams must be
on for the cluster (`neptune_streams`); a Gremlin server has none.
"""
import base64
import datetime
import json
import time
import uuid
from decimal import Decimal

from .base import Engine, NeutralCopier

TABLES = {"graph": ("edges", "vertices"), "rdf": ("quads",)}
COLUMNS = {
    "vertices": [("id", "string"), ("label", "string"),
                 ("properties", "properties")],
    "edges": [("id", "string"), ("label", "string"), ("from", "string"),
              ("to", "string"), ("properties", "properties")],
    "quads": [("subject", "term"), ("predicate", "term"), ("object", "term"),
              ("graph", "term")],
}
KEYS = {"vertices": ["id"], "edges": ["id"],
        "quads": ["subject", "predicate", "object", "graph"]}
#: elements one traversal writes
BATCH = 50
#: seconds an edge waits for its vertices
ENDS_WAIT = 600
#: the graph Neptune keeps the default graph's quads in
NEPTUNE_DEFAULT = "<http://aws.amazon.com/neptune/vocab/v01/DefaultNamedGraph>"
XSD_STRING = "http://www.w3.org/2001/XMLSchema#string"
#: a place in the stream as one number: a commit's number, and room for
#: its operations after it
OPS = 10 ** 6


# ---- values -----------------------------------------------------------------

def typed(value):
    """A property value as [kind, text]."""
    if isinstance(value, bool):
        return ["boolean", "1" if value else "0"]
    if isinstance(value, int):
        return ["integer", str(int(value))]
    if isinstance(value, float):
        return ["double", repr(value)]
    if isinstance(value, Decimal):
        return ["decimal", format(value, "f")]
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(datetime.timezone.utc).replace(
                tzinfo=None)
        return ["date", value.isoformat(timespec="milliseconds")]
    if isinstance(value, uuid.UUID):
        return ["uuid", str(value)]
    if isinstance(value, (bytes, bytearray)):
        return ["bytes", base64.b64encode(bytes(value)).decode()]
    return ["string", str(value)]


def untyped(pair):
    """[kind, text] back as the value a driver sends."""
    kind, text = pair[0], pair[1]
    if kind == "boolean":
        return text == "1"
    if kind == "integer":
        from gremlin_python.statics import long
        n = int(text)
        return n if -2 ** 31 <= n < 2 ** 31 else long(n)
    if kind == "double":
        return float(text)
    if kind == "decimal":
        return Decimal(text)
    if kind == "date":
        return datetime.datetime.fromisoformat(text)
    if kind == "uuid":
        return uuid.UUID(text)
    if kind == "bytes":
        return base64.b64decode(text)
    return text


def properties(mapping):
    """{key: [value, ...]} as the properties JSON, each key's values
    sorted."""
    return {str(k): sorted(typed(v) for v in (vals if isinstance(
        vals, (list, tuple, set)) else [vals]))
        for k, vals in mapping.items()}


def term(binding):
    """A SPARQL JSON result's term as its N-Triples text."""
    if binding is None:
        return ""
    kind, value = binding.get("type"), binding.get("value", "")
    if kind == "uri":
        return f"<{value}>"
    if kind == "bnode":
        return f"_:{value}"
    text = '"' + (value.replace("\\", "\\\\").replace('"', '\\"')
                  .replace("\n", "\\n").replace("\r", "\\r")) + '"'
    if binding.get("xml:lang"):
        return f"{text}@{binding['xml:lang']}"
    dt = binding.get("datatype")
    if dt and dt != XSD_STRING:
        return f"{text}^^<{dt}>"
    return text


def nquad(line):
    """(subject, predicate, object, graph) of one N-Quads statement, the
    graph "" where it names none."""
    out, i, text = [], 0, str(line).strip()
    while i < len(text):
        ch = text[i]
        if ch.isspace():
            i += 1
            continue
        if ch == ".":
            break
        if ch == "<":
            j = text.index(">", i)
            out.append(text[i:j + 1])
            i = j + 1
        elif ch == "_":
            j = i
            while j < len(text) and not text[j].isspace():
                j += 1
            out.append(text[i:j])
            i = j
        elif ch == '"':
            j = i + 1
            while text[j] != '"':
                j += 2 if text[j] == "\\" else 1
            j += 1
            if j < len(text) and text[j] == "@":
                while j < len(text) and not text[j].isspace():
                    j += 1
            elif text.startswith("^^<", j):
                j = text.index(">", j) + 1
            out.append(text[i:j])
            i = j
        else:
            raise SystemExit(f"a stream statement migkit cannot read:"
                             f" {text[:80]}")
    if len(out) not in (3, 4):
        raise SystemExit(f"a stream statement of {len(out)} terms:"
                         f" {text[:80]}")
    g = out[3] if len(out) == 4 else ""
    return out[0], out[1], out[2], "" if g == NEPTUNE_DEFAULT else g


class NeptuneEngine(NeutralCopier, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "neptune"
    FOLDED_BECAUSE = "has no way to hash elements in the service"
    OVER_NETWORK = True
    RESUMES_BY_KEY = False
    CREATES_ON_WRITE = True
    CHANGE_POINT_READS_ONLY = True
    READS_AHEAD = False

    def _ep(self, side):
        return self.hop.source if side == "src" else self.hop.target

    def _d(self, side, db):
        return self.hop.target_db(db) if side == "dst" else db

    def databases(self):
        return list(self.hop.databases or ["graph"])

    def _model(self, side, db):
        name = self._d(side, db)
        if name not in TABLES:
            raise SystemExit(f"{name}: a Neptune side's databases are graph"
                             " (its property graph) and rdf (its RDF store)")
        return name

    def _tls(self, side):
        return self._ep(side).options.get("tls") is not False

    def _region(self, side):
        import re
        ep = self._ep(side)
        if ep.options.get("region"):
            return str(ep.options["region"])
        m = re.search(r"\.([a-z]{2}-[a-z]+-\d)\.neptune\.amazonaws\.com",
                      str(ep.host or ""))
        return m.group(1) if m else None

    def _signed(self, side, method, url, body=None, headers=None):
        """A request's headers, signed by Neptune's signature where the
        side signs in by it."""
        headers = dict(headers or {})
        if self._ep(side).options.get("auth") != "aws_iam":
            return headers
        import boto3
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest
        creds = boto3.Session(region_name=self._region(side)) \
            .get_credentials()
        req = AWSRequest(method=method, url=url, data=body, headers=headers)
        SigV4Auth(creds, "neptune-db", self._region(side)).add_auth(req)
        return dict(req.headers)

    # ---- the property graph through Gremlin --------------------------------

    def _g(self, side):
        held = self.__dict__.setdefault("_gs", {})
        if side not in held:
            from gremlin_python.driver.driver_remote_connection import \
                DriverRemoteConnection
            from gremlin_python.process.anonymous_traversal import traversal
            ep = self._ep(side)
            scheme = "wss" if self._tls(side) else "ws"
            url = f"{scheme}://{ep.host}:{ep.port or 8182}/gremlin"
            headers = self._signed(
                side, "GET", url.replace("wss://", "https://").replace(
                    "ws://", "http://"))
            conn = DriverRemoteConnection(url, "g", headers=headers or None)
            held[side] = (conn, traversal().with_remote(conn))
        return held[side][1]

    def _api(self, side):
        said = str(self._ep(side).options.get("graph_api") or "gremlin")
        if said not in ("gremlin", "opencypher"):
            raise SystemExit(f"graph_api: {said} - gremlin or opencypher")
        return said

    @staticmethod
    def _vertex(m):
        from gremlin_python.process.traversal import T
        props = {k: v for k, v in m.items() if not isinstance(k, T)}
        return [str(m[T.id]), str(m[T.label]), properties(props)]

    @staticmethod
    def _edge(m):
        from gremlin_python.process.traversal import Direction, T
        props = {k: v for k, v in m.items()
                 if not isinstance(k, (T, Direction))}
        return [str(m[T.id]), str(m[T.label]),
                str(m[Direction.OUT][T.id]), str(m[Direction.IN][T.id]),
                properties(props)]

    def _elements(self, side, table, ids=None):
        """Every vertex or edge - or those of `ids` - as rows."""
        if self._api(side) == "opencypher":
            yield from self._cypher_elements(side, table, ids)
            return
        from gremlin_python.process.traversal import WithOptions
        g = self._g(side)
        if table == "vertices":
            t = g.V(*ids) if ids else g.V()
            for m in t.value_map().with_(WithOptions.tokens):
                yield self._vertex(m)
        else:
            t = g.E(*ids) if ids else g.E()
            for m in t.element_map():
                yield self._edge(m)

    def _cypher(self, side, query, **params):
        import boto3
        ep = self._ep(side)
        client = boto3.client(
            "neptunedata", region_name=self._region(side),
            endpoint_url=f"{'https' if self._tls(side) else 'http'}://"
                         f"{ep.host}:{ep.port or 8182}")
        got = client.execute_open_cypher_query(
            openCypherQuery=query,
            **({"parameters": json.dumps(params)} if params else {}))
        return got.get("results") or []

    def _cypher_elements(self, side, table, ids=None):
        """The property graph read through openCypher, as Gremlin's reads
        give it: a vertex's labels joined by `::`, as Neptune names a
        vertex of several."""
        where = " where id(n) in $ids" if ids else ""
        if table == "vertices":
            for r in self._cypher(side, "match (n)" + where + " return id(n)"
                                  " as id, labels(n) as labels,"
                                  " properties(n) as props",
                                  **({"ids": list(ids)} if ids else {})):
                yield [str(r["id"]), "::".join(r.get("labels") or []),
                       properties(r.get("props") or {})]
        else:
            where = " where id(r) in $ids" if ids else ""
            for r in self._cypher(side, "match (a)-[r]->(b)" + where
                                  + " return id(r) as id, type(r) as label,"
                                    " id(a) as src, id(b) as dst,"
                                    " properties(r) as props",
                                  **({"ids": list(ids)} if ids else {})):
                yield [str(r["id"]), str(r["label"]), str(r["src"]),
                       str(r["dst"]), properties(r.get("props") or {})]

    # ---- the RDF store through SPARQL ---------------------------------------

    def _sparql_url(self, side, update=False):
        ep = self._ep(side)
        query = ep.options.get("sparql")
        said = ep.options.get("sparql_update") if update else None
        if said or query:
            return str(said or query)
        return (f"{'https' if self._tls(side) else 'http'}://{ep.host}:"
                f"{ep.port or 8182}/sparql")

    def _sparql(self, side, text, update=False):
        import urllib.parse
        import urllib.request
        url = self._sparql_url(side, update)
        body = urllib.parse.urlencode(
            {"update" if update else "query": text}).encode()
        headers = self._signed(side, "POST", url, body, {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/sparql-results+json"})
        req = urllib.request.Request(url, data=body, headers=headers,
                                     method="POST")
        with urllib.request.urlopen(req, timeout=600) as resp:
            got = resp.read()
        return None if update else json.loads(got)

    def _default_separate(self, side):
        """Whether the store keeps its default graph apart from its named
        ones (a Fuseki dataset), rather than as the union of them
        (Neptune)."""
        said = self._ep(side).options.get("rdf_default")
        if said:
            return str(said) == "separate"
        return "neptune.amazonaws.com" not in str(self._ep(side).host or "")

    def _quads(self, side, where=""):
        q = ("select ?s ?p ?o ?g where { graph ?g { ?s ?p ?o }" + where
             + " }")
        out = []
        for b in self._sparql(side, q)["results"]["bindings"]:
            g = term(b.get("g"))
            out.append([term(b["s"]), term(b["p"]), term(b["o"]),
                        "" if g == NEPTUNE_DEFAULT else g])
        if self._default_separate(side):
            q = "select ?s ?p ?o where { ?s ?p ?o" + where + " }"
            for b in self._sparql(side, q)["results"]["bindings"]:
                out.append([term(b["s"]), term(b["p"]), term(b["o"]), ""])
        return out

    def _quad_data(self, rows):
        by = {}
        for s, p, o, g in rows:
            by.setdefault(g, []).append(f"{s} {p} {o} .")
        parts = []
        for g, lines in by.items():
            body = "\n".join(lines)
            parts.append(f"graph {g} {{ {body} }}" if g else body)
        return "\n".join(parts)

    # ---- the neutral contract -----------------------------------------------

    def neutral_tables(self, side, db):
        return [t for t in TABLES[self._model(side, db)]
                if not self.hop.excluded(db, t)]

    def neutral_columns(self, side, db, table):
        return list(COLUMNS[table])

    def neutral_key(self, side, db, table):
        return list(KEYS[table])

    def foreign_keys(self, side, db):
        if self._model(side, db) != "graph":
            return []
        return [("edges", ["from"], "vertices", ["id"]),
                ("edges", ["to"], "vertices", ["id"])]

    def table_facts(self, side, db):
        return {t: {"rows": None, "bytes": None, "key": True}
                for t in self.neutral_tables(side, db)}

    def _rows(self, side, db, table, keys=None):
        if table == "quads":
            rows = self._quads(side)
            if keys is not None:
                wanted = {tuple(k) for k in keys}
                rows = [r for r in rows if tuple(r) in wanted]
            return rows
        return list(self._elements(side, table,
                                   [k[0] for k in keys] if keys else None))

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and a graph"
                             " takes none")
        names = [n for n, _ in columns]
        at = [n for n, _ in COLUMNS[table]]
        batch = []
        if table == "quads":
            source = self._quads(side)
        else:
            source = self._elements(side, table)
        for row in source:
            values = dict(zip(at, row))
            batch.append([values.get(n) for n in names])
            if len(batch) >= size:
                yield batch
                batch = []
        if batch:
            yield batch

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        rows = []
        for batch in self.neutral_batches(side, db, table, columns,
                                          limit or 1000, where):
            rows += batch
            if limit and len(rows) >= limit:
                return rows[:limit], None
        return rows, None

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        if not key or not keys:
            return {}
        names = [n for n, _ in columns]
        at = [n for n, _ in COLUMNS[table]]
        found = []
        keys = list(keys)
        for i in range(0, len(keys), 1000):
            for row in self._rows(side, db, table, keys[i:i + 1000]):
                values = dict(zip(at, row))
                found.append([values.get(n) for n in names])
        return self._by_key_map(columns, key, found)

    def neutral_digest(self, side, db, table, columns, where=None):
        from .. import canon
        return canon.fold_batches(columns, self.neutral_batches(
            side, db, table, columns, 5000, where))

    def neutral_create_sql(self, side, db, table, columns, key=()):
        if table not in COLUMNS:
            raise SystemExit(f"{table}: a graph holds vertices and edges, an"
                             " RDF store quads - another engine's table has"
                             " no place in either")
        return json.dumps({"table": table})

    def neutral_create(self, side, db, table, columns, key=()):
        """Nothing is made: a graph holds its vertices and edges, and an RDF
        store its quads, from the first written."""
        self._target_only(side, "create a table")
        return self.neutral_create_sql(side, db, table, columns, key)

    def _write_vertices(self, side, rows):
        from gremlin_python.process.graph_traversal import __
        from gremlin_python.process.traversal import Cardinality, T
        g = self._g(side)
        labels = {r[0]: r[1] for r in rows}
        # a vertex's label is not changed in place: one there under another
        # is made again
        wrong = [str(v["id"]) for v in g.V(*labels).project("id", "label")
                 .by(T.id).by(T.label).to_list()
                 if str(v["label"]) != labels.get(str(v["id"]))]
        if wrong:
            g.V(*wrong).drop().iterate()
        t = g.inject(0)
        for vid, label, props in rows:
            if isinstance(props, str):
                props = json.loads(props)
            up = __.V(vid).fold().coalesce(
                __.unfold(), __.addV(label).property(T.id, vid)).side_effect(
                __.properties().drop())
            for k, vals in (props or {}).items():
                for v in vals:
                    up = up.property(Cardinality.set_, k, untyped(v))
            t = t.side_effect(up)
        t.iterate()

    def _write_edges(self, side, rows):
        from gremlin_python.process.graph_traversal import __
        from gremlin_python.process.traversal import T
        g = self._g(side)
        ends = sorted({r[2] for r in rows} | {r[3] for r in rows})
        began = time.monotonic()
        while True:
            there = {str(x) for x in g.V(*ends).id_().to_list()}
            missing = [e for e in ends if e not in there]
            if not missing:
                break
            if time.monotonic() - began > ENDS_WAIT:
                raise SystemExit(
                    f"edges go from or to vertices the target does not hold"
                    f" ({', '.join(missing[:4])}): copy the vertices first")
            time.sleep(2)
        t = g.inject(0)
        for eid, label, frm, to, props in rows:
            if isinstance(props, str):
                props = json.loads(props)
            up = __.E(eid).fold().coalesce(
                __.unfold(), __.V(frm).addE(label).to(__.V(to)).property(
                    T.id, eid)).side_effect(__.properties().drop())
            for k, vals in (props or {}).items():
                if vals:
                    up = up.property(k, untyped(vals[0]))
            t = t.side_effect(up)
        t.iterate()

    def _write(self, side, db, table, rows):
        if table == "quads":
            for i in range(0, len(rows), 1000):
                self._sparql(side, "insert data { " + self._quad_data(
                    rows[i:i + 1000]) + " }", update=True)
            return
        if self._api(side) != "gremlin":
            raise SystemExit("a property graph is written through Gremlin:"
                             " give the target graph_api: gremlin")
        for i in range(0, len(rows), BATCH):
            (self._write_vertices if table == "vertices"
             else self._write_edges)(side, rows[i:i + BATCH])

    def neutral_write(self, side, db, table, columns, rows):
        self._target_only(side, "write rows")
        if not rows:
            return 0
        names = [n for n, _ in columns]
        at = [n for n, _ in COLUMNS[table]]
        self._write(side, db, table, [
            [dict(zip(names, r)).get(n) for n in at] for r in rows])
        return len(rows)

    def _apply_upserts(self, side, db, table, rows):
        at = [n for n, _ in COLUMNS[table]]
        self._write(side, db, table, [
            [{**values, **key}.get(n) for n in at] for key, values in rows])

    def _apply_upsert(self, side, db, table, key, values):
        self._apply_upserts(side, db, table, [(key, values)])

    def _apply_deletes(self, side, db, table, keys):
        keys = list(keys)
        if not keys:
            return
        if table == "quads":
            rows = [[k[c] for c in KEYS["quads"]] for k in keys]
            for i in range(0, len(rows), 1000):
                self._sparql(side, "delete data { " + self._quad_data(
                    rows[i:i + 1000]) + " }", update=True)
            return
        g = self._g(side)
        ids = [k["id"] for k in keys]
        for i in range(0, len(ids), 1000):
            (g.V if table == "vertices" else g.E)(*ids[i:i + 1000]) \
                .drop().iterate()

    def _apply_delete(self, side, db, table, key):
        self._apply_deletes(side, db, table, [key])

    def neutral_empty(self, side, db, table, where=None):
        self._target_only(side, "empty a table")
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and a graph"
                             " takes none")
        if table == "quads":
            self._sparql(side, "clear all", update=True)
            return None
        g = self._g(side)
        step = g.V if table == "vertices" else g.E
        gone = 0
        while True:
            n = step().limit(10000).count().next()
            if not n:
                return gone
            step().limit(10000).drop().iterate()
            gone += n

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)

    def repair_plan(self, db, kind):
        return self._as_pair().repair_plan(db, kind)

    def apply(self, db, action):
        return self._as_pair().apply(db, action)

    def settle_target(self, db, from_source=True):
        """The statistics Neptune plans queries by, refreshed from what was
        loaded: they are otherwise refreshed only once enough of the graph
        has changed, on the server's own schedule. A Gremlin server keeps
        none."""
        if "neptune.amazonaws.com" not in str(self._ep("dst").host or ""):
            return None
        client = self._data("dst")
        fn = (client.manage_propertygraph_statistics
              if self._model("dst", db) == "graph"
              else client.manage_sparql_statistics)
        try:
            fn(mode="refresh")
        except Exception as e:  # noqa: BLE001 - said, and nothing else
            return ("the target's statistics were not refreshed:"
                    f" {str(e).splitlines()[0][:100]}")
        return "the target's query statistics refreshed"

    # ---- who it is -----------------------------------------------------------

    def server_identity(self, side, db):
        ep = self._ep(side)
        out = {"name": str(ep.host), "id": f"{ep.host}:{ep.port or 8182}"}
        if "neptune.amazonaws.com" in str(ep.host or ""):
            try:
                got = self._data(side).get_engine_status()
                out["version"] = str(got.get("dbEngineVersion") or "")
                out["running"] = str(got.get("startTime") or "")
            except Exception:  # noqa: BLE001 - its status is optional here
                pass
        return out

    def stream_identity(self, side, db):
        return self.server_identity(side, db)

    # ---- Neptune Streams ---------------------------------------------------

    def _data(self, side):
        held = self.__dict__.setdefault("_data_clients", {})
        if side not in held:
            import boto3
            ep = self._ep(side)
            held[side] = boto3.client(
                "neptunedata", region_name=self._region(side),
                endpoint_url=f"{'https' if self._tls(side) else 'http'}://"
                             f"{ep.host}:{ep.port or 8182}")
        return held[side]

    def _follower(self, db):
        from .. import changetopic
        held = self.__dict__.setdefault("_followers", {})
        if db not in held:
            model = self._model("src", db)
            held[db] = changetopic.Follower(
                changetopic.NeptuneChanges(), NeptuneLanes(self, model),
                name=lambda row, db=db: row.table
                if row.table in self.neutral_tables("src", db) else None,
                shape=lambda t, row: self._shape(t, row),
                read_back=lambda t, rows, v, db=db: self._read_back(
                    db, t, rows))
        return held[db]

    @staticmethod
    def _shape(table, row):
        """A quad as the change carries it; an element read back."""
        if table != "quads":
            return None
        if row.op == "delete":
            return row.key, None
        return row.key, dict(row.key)

    def _read_back(self, db, table, rows):
        cols = COLUMNS[table]
        names = [n for n, _ in cols]
        ids = list({r.key["id"]: r for r in rows})
        found = {row[0]: row for row in self._elements("src", table, ids)}
        out = []
        for r in rows:
            got = found.get(r.key["id"])
            out.append((r, {"id": r.key["id"]},
                        None if got is None else dict(zip(names, got))))
        return out

    def change_point(self, side, db):
        lanes = self._follower(db).lanes
        return self._follower(db).point(fence=lanes.latest)

    def copy_point(self, side, db):
        return self.change_point(side, db)

    def log_position(self, side, db):
        return {"fence": self._follower(db).lanes.latest()}

    @staticmethod
    def position_reached(have, want):
        from ..changetopic import Follower
        return Follower.reached(have, want)

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Each element or quad the stream says changed, in the order it
        was committed."""
        if token is None:
            return [], self.change_point(side, db)
        return self._follower(db).changes(token, limit)

    def release_changes(self):
        for f in self.__dict__.pop("_followers", {}).values():
            f.close()

    def position_lost(self, side, db, token):
        return self._follower(db).lost(token) if token else None

    def tail_start(self, db, token_path):
        return self._as_pair().tail_start(db, token_path)

    def tail_apply(self, db, go, token_path, log):
        return self._as_pair().tail_apply(db, go, token_path, log)

    def src_lsn(self, db):
        return self._as_pair().src_lsn(db)

    def fence_wait(self, db, lsn, timeout=300):
        return self._as_pair().fence_wait(db, lsn, timeout)

    def delta_verify(self, db, limit=20000, log=None):
        return self._as_pair().delta_verify(db, limit, log)

    def _compare_pks(self, db, table, keys):
        return self._as_pair()._compare_pks(db, table, keys)

    def _write_pk_files(self, db, table, missing, extra, changed):
        return self._as_pair()._write_pk_files(db, table, missing, extra,
                                                changed)

    def reach_items(self, side):
        items = [{"level": "pass", "scope": "reach",
                  "item": "copied and verified",
                  "detail": "vertices, edges and quads compared by id and by"
                            " value"}]
        if side == "src":
            from ..changetopic import NeptuneChanges
            items.append({"level": "pass", "scope": "reach",
                          "item": "changes followed through Neptune Streams",
                          "detail": f"it loses {NeptuneChanges.LOSES}"})
        return items

    def _assess_extra(self):
        return self.reach_items("src")


class NeptuneLanes:
    """The cluster's one stream, read by commit and operation number. A
    read that reaches the stream's end marks everything it read."""
    KIND = "neptune"
    LANE = "stream"

    def __init__(self, engine, model):
        self.engine, self.model = engine, model

    def _get(self, **kw):
        from botocore.exceptions import ClientError
        client = self.engine._data("src")
        fn = (client.get_propertygraph_stream if self.model == "graph"
              else client.get_sparql_stream)
        try:
            return fn(**kw)
        except ClientError as e:
            code = (e.response.get("Error") or {}).get("Code", "")
            if "StreamRecordsNotFound" in code:
                return {"records": []}
            if "ExpiredStreamException" in code:
                raise SystemExit(
                    "Neptune Streams no longer holds the saved position: its"
                    " retention (neptune_streams_expiry_days) passed, and"
                    " what changed then is carried by nothing")
            raise

    def latest(self):
        """The stream's last change, as one number; 0 where it holds
        none."""
        got = self._get(iteratorType="LATEST", limit=1)
        last = got.get("lastEventId") or {}
        return int(last.get("commitNum") or 0) * OPS + int(
            last.get("opNum") or 0)

    def lanes(self):
        return [self.LANE]

    def ends(self):
        return {self.LANE: {"after": self.latest()}}

    def _record(self, rec):
        event = rec.get("eventId") or {}
        version = int(event.get("commitNum") or 0) * OPS + int(
            event.get("opNum") or 0)
        data = rec.get("data") or {}
        op = str(rec.get("op") or "").upper()
        if self.model == "rdf":
            s, p, o, g = nquad(data.get("stmt") or "")
            key = {"subject": s, "predicate": p, "object": o, "graph": g}
            return {"version": version, "table": "quads",
                    "op": "delete" if op == "REMOVE" else "upsert",
                    "key": key, "commit": int(event.get("commitNum") or 0)}
        kind = str(data.get("type") or "")
        return {"version": version,
                "table": "vertices" if kind in ("vl", "vp") else "edges",
                "op": "changed", "key": {"id": str(data.get("id"))},
                "commit": int(event.get("commitNum") or 0)}

    def read(self, at, most, seconds):
        out, pos = [], dict((at or {}).get(self.LANE) or {"after": 0})
        stop = time.monotonic() + seconds
        caught = False
        while len(out) < most and time.monotonic() < stop:
            after = int(pos.get("after") or 0)
            kw = {"limit": min(10000, most - len(out))}
            if after:
                kw.update(iteratorType="AFTER_SEQUENCE_NUMBER",
                          commitNum=after // OPS, opNum=after % OPS)
            else:
                kw["iteratorType"] = "TRIM_HORIZON"
            recs = self._get(**kw).get("records") or []
            if not recs:
                caught = True
                break
            for rec in recs:
                value = self._record(rec)
                out.append((self.LANE, {"after": value["version"] - 1}, None,
                            value, None))
                pos = {"after": value["version"]}
        if caught and pos.get("after"):
            out.append((self.LANE, dict(pos), None,
                        {"mark": int(pos["after"])}, None))
        return out, {self.LANE: pos}, caught

    def lost(self, at):
        return None

    def room(self, at):
        return None

    def close(self):
        return None


def _register():
    """What of the Gremlin driver and the Neptune data API is used, read
    off this file, and why the rest is not (`tooluse`)."""
    from .. import tooluse
    unused = tooluse.UNUSED
    tooluse.register("gremlinpython", licence="Apache-2.0", reads=(
        ("engines/neptune.py",),
        {("GraphTraversal", "value_map"): "value_map",
         ("GraphTraversal", "element_map"): "element_map",
         ("GraphTraversal", "coalesce"): "coalesce",
         ("GraphTraversal", "side_effect"): "side_effect",
         ("GraphTraversal", "drop"): "drop",
         ("Cardinality", "set_"): "set_",
         ("WithOptions", "tokens"): "tokens",
         ("DriverRemoteConnection", "headers"): "headers"}), options={
        ("GraphTraversal", "merge_v"): (
            unused, "a merge keeps properties the element holds that the"
                    " source's does not; an element written whole drops"
                    " them first"),
        ("Client", "submit"): (
            unused, "a script is text built from values; a traversal carries"
                    " them as values"),
    })
    tooluse.register("neptunedata", licence="Apache-2.0", reads=(
        ("engines/neptune.py",),
        {("stream", "get_propertygraph_stream"): "get_propertygraph_stream",
         ("stream", "get_sparql_stream"): "get_sparql_stream",
         ("query", "execute_open_cypher_query"): "execute_open_cypher_query",
         ("status", "get_engine_status"): "get_engine_status",
         ("statistics", "manage_propertygraph_statistics"):
             "manage_propertygraph_statistics",
         ("statistics", "manage_sparql_statistics"):
             "manage_sparql_statistics"}), options={
        ("loader", "start_loader_job"): (
            unused, "the bulk loader reads files from S3 the cluster's role"
                    " can reach, which a copy from another server has none"
                    " of"),
        ("query", "execute_gremlin_query"): (
            unused, "the Gremlin driver reads and writes by traversal over"
                    " one connection, where this sends text a request"),
    })


_register()
