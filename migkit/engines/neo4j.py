"""Neo4j as a side of a pair (backlog R16c), through its HTTP Cypher API.

A hop's databases are Neo4j's own (`neo4j` where it names none). Each is
two tables: `nodes` - a node's id, its labels and its properties - and
`relationships` - the same with its type, the ids of the nodes it goes
from and to and the first label of each, by which the target finds them
through an index. Properties are one JSON value; labels a sorted list.

A node's id is the value of its key property (`key_property`, default
`_source_id`) where it has one, else its element id: a copy writes the
source's id into the target's key property, so the target's nodes read
back under the source's ids and are compared by them. Read in ranges of
the server's internal id, `nodes` first, then `relationships` after them
(`foreign_keys`): a relationship waits for both of its nodes while their
copy runs and is refused, named, where they never come. A side's scope
is its `labels` and `types` when it names them.

Written by MERGE on the key property, each element whole: its properties
replaced, so an element written again lands on itself, a batch of them in
one statement, the key property indexed per label on the target first.

Its changes are Neo4j's change data capture (5.13 and later, with the
database's `txLogEnrichment` set): one log per database, each change at
its id, in commit order. A server without it says so, and the follow is
refused, naming the setting. A change id is opaque, so no fence is read
off it: the fence is not yet built (UNPROVED, capabilities).
"""
import base64
import json
import time
import urllib.error
import urllib.request

from .base import Engine, NeutralCopier, Result

TABLES = ("nodes", "relationships")
COLUMNS = {
    "nodes": [("id", "string"), ("labels", "json"), ("properties", "json")],
    "relationships": [("id", "string"), ("type", "string"),
                      ("from", "string"), ("from_label", "string"),
                      ("to", "string"), ("to_label", "string"),
                      ("properties", "json")],
}
KEYS = {"nodes": ["id"], "relationships": ["id"]}
KEY_PROPERTY = "_source_id"
#: elements one statement writes
BATCH = 500
#: what a database's change capture must be set to for its log to say
#: what a change made
ENRICHMENT = ("FULL", "DIFF")


def quoted(name):
    """A label, type or property name as Cypher takes it in a statement."""
    return "`" + str(name).replace("`", "``") + "`"


def labels_clause(labels):
    return "".join(":" + quoted(x) for x in labels)


class Neo4jEngine(NeutralCopier, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "neo4j"
    FOLDED_BECAUSE = "has no way to hash nodes and relationships in the server"
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
        return list(self.hop.databases or ["neo4j"])

    def _key(self, side):
        return str(self._ep(side).options.get("key_property") or KEY_PROPERTY)

    def _url(self, side, db):
        ep = self._ep(side)
        tls = ep.options.get("tls", True) not in (False, "false", "off")
        return (f"{'https' if tls else 'http'}://{ep.host}:"
                f"{ep.port or (7473 if tls else 7474)}"
                f"/db/{self._d(side, db)}/tx/commit")

    def _run(self, side, db, statement, **params):
        """The rows one statement returns, each a dict by column, or the
        server's error as the error."""
        ep = self._ep(side)
        body = json.dumps({"statements": [
            {"statement": statement, "parameters": params}]}).encode()
        req = urllib.request.Request(
            self._url(side, db), data=body, method="POST",
            headers={"Content-Type": "application/json",
                     "Accept": "application/json"})
        if ep.user:
            token = base64.b64encode(
                f"{ep.user}:{ep.password}".encode()).decode()
            req.add_header("Authorization", f"Basic {token}")
        try:
            with urllib.request.urlopen(req, timeout=float(
                    ep.options.get("timeout") or 120)) as resp:
                got = json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as e:
            raise SystemExit(f"{side} {self._d(side, db)}: the server"
                             f" answered {e.code} {e.reason}") from None
        if got.get("errors"):
            err = got["errors"][0]
            raise RuntimeError(f"{err.get('code', '')}:"
                               f" {err.get('message', '')}")
        res = (got.get("results") or [{}])[0]
        cols = res.get("columns") or []
        return [dict(zip(cols, d.get("row") or []))
                for d in res.get("data") or []]

    def _major(self, side, db):
        held = self.__dict__.setdefault("_majors", {})
        if side not in held:
            rows = self._run(side, db, "CALL dbms.components() YIELD name,"
                             " versions, edition RETURN name, versions,"
                             " edition")
            ver = str(((rows or [{}])[0].get("versions") or ["0"])[0])
            held[side] = int(ver.split(".")[0] or 0)
        return held[side]

    def _eid(self, side, db, var):
        return (f"elementId({var})" if self._major(side, db) >= 5
                else f"toString(id({var}))")

    def _scope(self, side, table):
        opts = self._ep(side).options
        return list(opts.get("labels" if table == "nodes" else "types") or [])

    # ---- the tables ---------------------------------------------------------

    def neutral_tables(self, side, db):
        return [t for t in TABLES if self.in_scope(db, t)]

    def neutral_columns(self, side, db, table):
        return list(COLUMNS[table])

    def neutral_key(self, side, db, table):
        return list(KEYS[table])

    def foreign_keys(self, side, db):
        return [("relationships", ["from"], "nodes", ["id"]),
                ("relationships", ["to"], "nodes", ["id"])]

    def _count(self, side, db, table):
        within = self._scope(side, table)
        if table == "nodes":
            where = ("WHERE any(l IN labels(n) WHERE l IN $within)"
                     if within else "")
            q = f"MATCH (n) {where} RETURN count(n) AS n"
        else:
            where = "WHERE type(e) IN $within" if within else ""
            q = f"MATCH ()-[e]->() {where} RETURN count(e) AS n"
        return int(self._run(side, db, q, within=within)[0]["n"])

    def table_facts(self, side, db):
        return {t: {"rows": self._count(side, db, t), "bytes": None,
                    "key": True} for t in self.neutral_tables(side, db)}

    def _select(self, side, db, table):
        k = quoted(self._key(side))
        if table == "nodes":
            return (f"coalesce(n.{k}, {self._eid(side, db, 'n')}) AS id,"
                    f" labels(n) AS labels, properties(n) AS properties")
        a, b = "startNode(e)", "endNode(e)"
        return (f"coalesce(e.{k}, {self._eid(side, db, 'e')}) AS id,"
                f" type(e) AS type,"
                f" coalesce({a}.{k}, {self._eid(side, db, a)}) AS from,"
                f" coalesce(head(labels({a})), '') AS from_label,"
                f" coalesce({b}.{k}, {self._eid(side, db, b)}) AS to,"
                f" coalesce(head(labels({b})), '') AS to_label,"
                f" properties(e) AS properties")

    def _shape(self, side, table, got):
        props = dict(got.get("properties") or {})
        props.pop(self._key(side), None)
        out = dict(got, id=str(got["id"]),
                   properties=json.dumps(props, sort_keys=True, default=str))
        if table == "nodes":
            out["labels"] = json.dumps(sorted(got.get("labels") or []))
        else:
            out["from"], out["to"] = str(got["from"]), str(got["to"])
        return [out.get(n) for n, _ in COLUMNS[table]]

    def _range(self, side, db, table, after, size):
        """The elements after internal id `after`, `size` of them at most,
        in the order of that id, and the last one's id."""
        within = self._scope(side, table)
        if table == "nodes":
            match = "MATCH (n) WHERE id(n) > $after"
            if within:
                match += " AND any(l IN labels(n) WHERE l IN $within)"
            at = "id(n)"
        else:
            match = "MATCH ()-[e]->() WHERE id(e) > $after"
            if within:
                match += " AND type(e) IN $within"
            at = "id(e)"
        rows = self._run(side, db, f"{match} RETURN {at} AS at,"
                         f" {self._select(side, db, table)} ORDER BY {at}"
                         f" LIMIT $size", after=after, within=within,
                         size=int(size))
        last = rows[-1]["at"] if rows else None
        return [self._shape(side, table, r) for r in rows], last

    def _all(self, side, db, table, size=BATCH):
        after = -1
        while True:
            rows, last = self._range(side, db, table, after, size)
            if rows:
                yield rows
            if last is None or len(rows) < size:
                return
            after = last

    def _by_ids(self, side, db, table, ids):
        k = quoted(self._key(side))
        var = "n" if table == "nodes" else "e"
        match = "MATCH (n)" if table == "nodes" else "MATCH ()-[e]->()"
        rows = self._run(side, db, f"UNWIND $ids AS i {match} WHERE"
                         f" coalesce({var}.{k}, {self._eid(side, db, var)})"
                         f" = i RETURN {self._select(side, db, table)}",
                         ids=list(ids))
        return [self._shape(side, table, r) for r in rows]

    def _rowfilter(self, table, where):
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and a graph"
                             " takes none: name labels or types instead")

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        self._rowfilter(table, where)
        names = [n for n, _ in columns]
        at = [n for n, _ in COLUMNS[table]]
        for rows in self._all(side, db, table, size or BATCH):
            yield [[dict(zip(at, r)).get(n) for n in names] for r in rows]

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        rows = []
        for batch in self.neutral_batches(side, db, table, columns,
                                          limit or BATCH, where):
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
        keys = [k[0] if isinstance(k, (list, tuple)) else k for k in keys]
        found = []
        for i in range(0, len(keys), BATCH):
            for r in self._by_ids(side, db, table, keys[i:i + BATCH]):
                found.append([dict(zip(at, r)).get(n) for n in names])
        return self._by_key_map(columns, key, found)

    def neutral_digest(self, side, db, table, columns, where=None):
        from .. import canon
        return canon.fold_batches(columns, self.neutral_batches(
            side, db, table, columns, 5000, where))

    def neutral_create_sql(self, side, db, table, columns, key=()):
        if table not in COLUMNS:
            raise SystemExit(f"{table}: a graph holds nodes and relationships"
                             " - another engine's table has no place in it")
        return json.dumps({"table": table})

    def neutral_create(self, side, db, table, columns, key=()):
        self._target_only(side, "create a table")
        self.neutral_create_sql(side, db, table, columns, key)
        return None

    # ---- writing --------------------------------------------------------------

    def _index(self, side, db, label):
        made = self.__dict__.setdefault("_indexed", set())
        if not label or (db, label) in made:
            return
        k = self._key(side)
        self._run(side, db, f"CREATE INDEX {quoted('migkit_' + label)} IF NOT"
                  f" EXISTS FOR (n:{quoted(label)}) ON (n.{quoted(k)})")
        made.add((db, label))

    def _write_nodes(self, side, db, rows):
        k = quoted(self._key(side))
        groups = {}
        for r in rows:
            got = dict(zip([n for n, _ in COLUMNS["nodes"]], r))
            labels = tuple(json.loads(got["labels"] or "[]"))
            groups.setdefault(labels, []).append(
                {"id": got["id"],
                 "properties": json.loads(got["properties"] or "{}")})
        for labels, batch in groups.items():
            for label in labels:
                self._index(side, db, label)
            self._run(side, db, f"UNWIND $rows AS r MERGE"
                      f" (n{labels_clause(labels)} {{{k}: r.id}})"
                      f" SET n = r.properties SET n.{k} = r.id", rows=batch)

    def _write_relationships(self, side, db, rows, wait=None):
        from .neptune import ENDS_WAIT
        k = quoted(self._key(side))
        groups = {}
        for r in rows:
            got = dict(zip([n for n, _ in COLUMNS["relationships"]], r))
            groups.setdefault((got["type"], got["from_label"],
                               got["to_label"]), []).append(
                {"id": got["id"], "from": got["from"], "to": got["to"],
                 "properties": json.loads(got["properties"] or "{}")})
        end = time.time() + (ENDS_WAIT if wait is None else wait)
        for (rtype, fl, tl), batch in groups.items():
            a = f"(a{labels_clause([fl] if fl else [])} {{{k}: r.from}})"
            b = f"(b{labels_clause([tl] if tl else [])} {{{k}: r.to}})"
            todo = batch
            while todo:
                done = {x["id"] for x in self._run(
                    side, db, f"UNWIND $rows AS r MATCH {a} MATCH {b} MERGE"
                    f" (a)-[e:{quoted(rtype)} {{{k}: r.id}}]->(b) SET e ="
                    f" r.properties SET e.{k} = r.id RETURN r.id AS id",
                    rows=todo)}
                todo = [x for x in todo if x["id"] not in done]
                if todo and time.time() >= end:
                    raise SystemExit(
                        f"{len(todo)} {rtype} relationships wait on nodes"
                        " the target never received, first: " + ", ".join(
                            f"{x['id']} ({x['from']} -> {x['to']})"
                            for x in todo[:5]))
                if todo:
                    time.sleep(2)

    def _write(self, side, db, table, rows):
        for i in range(0, len(rows), BATCH):
            (self._write_nodes if table == "nodes"
             else self._write_relationships)(side, db, rows[i:i + BATCH])

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
        ids = [k["id"] for k in keys]
        k = quoted(self._key(side))
        match = ("MATCH (x) WHERE x." if table == "nodes"
                 else "MATCH ()-[x]->() WHERE x.")
        drop = "DETACH DELETE x" if table == "nodes" else "DELETE x"
        for i in range(0, len(ids), BATCH):
            self._run(side, db, f"UNWIND $ids AS i {match}{k} = i {drop}",
                      ids=ids[i:i + BATCH])

    def _apply_delete(self, side, db, table, key):
        self._apply_deletes(side, db, table, [key])

    def neutral_empty(self, side, db, table, where=None):
        self._target_only(side, "empty a table")
        self._rowfilter(table, where)
        q = ("MATCH (x) WITH x LIMIT 10000 DETACH DELETE x RETURN count(*)"
             " AS n" if table == "nodes" else
             "MATCH ()-[x]->() WITH x LIMIT 10000 DELETE x RETURN count(*)"
             " AS n")
        gone = 0
        while True:
            n = int(self._run(side, db, q)[0]["n"])
            if not n:
                return gone
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

    # ---- the deep check ------------------------------------------------------

    def _constraints(self, side, db):
        rows = self._run(side, db, "SHOW CONSTRAINTS YIELD name, type,"
                         " labelsOrTypes, properties RETURN type,"
                         " labelsOrTypes, properties")
        return sorted(f"{r['type']} {','.join(r['labelsOrTypes'] or [])}"
                      f"({','.join(r['properties'] or [])})" for r in rows)

    def _indexes(self, side, db):
        k = self._key(side)
        rows = self._run(side, db, "SHOW INDEXES YIELD name, type,"
                         " labelsOrTypes, properties RETURN name, type,"
                         " labelsOrTypes, properties")
        return sorted(f"{r['type']} {','.join(r['labelsOrTypes'] or [])}"
                      f"({','.join(r['properties'] or [])})" for r in rows
                      if r.get("type") != "LOOKUP"
                      and list(r.get("properties") or []) != [k])

    def check_deep(self, db):
        """The source's constraints and indexes, each one the target must
        hold: a uniqueness the source keeps and the target does not lets
        the target take a duplicate the source would refuse."""
        out = []
        for what, read in (("constraints", self._constraints),
                           ("indexes", self._indexes)):
            try:
                src, dst = set(read("src", db)), set(read("dst", db))
            except RuntimeError as e:
                out.append(Result("deep", f"{db} {what}", "error",
                                  f"could not read the {what}:"
                                  f" {str(e).splitlines()[0][:100]}"))
                continue
            missing = sorted(src - dst)
            out.append(Result(
                "deep", f"{db} {what}", "fail" if missing else "ok",
                f"{len(missing)} of the source's {len(src)} {what} not on"
                f" the target: {', '.join(missing[:5])}" if missing else
                f"the source's {len(src)} {what} all on the target", "",
                "" if not missing else
                f"create the source's {what} on the target before cutover"))
        return out

    # ---- who it is -------------------------------------------------------------

    def server_identity(self, side, db):
        ep = self._ep(side)
        out = {"name": str(ep.host), "id": f"{ep.host}:{ep.port or 7474}"}
        try:
            rows = self._run(side, db, "CALL dbms.components() YIELD name,"
                             " versions, edition RETURN name, versions,"
                             " edition")
        except (RuntimeError, SystemExit, OSError):
            return out
        if rows:
            out["version"] = str((rows[0].get("versions") or [""])[0])
            out["edition"] = str(rows[0].get("edition") or "")
        return out

    def stream_identity(self, side, db):
        return self.server_identity(side, db)

    # ---- change data capture ---------------------------------------------------

    def _cdc_on(self, db):
        """Whether the source database's log says what each change made,
        or the reason it does not."""
        try:
            rows = self._run("src", db, "SHOW DATABASE $name YIELD options"
                             " RETURN options", name=db)
        except RuntimeError as e:
            return str(e).splitlines()[0][:120]
        got = str(((rows or [{}])[0].get("options") or {}).get(
            "txLogEnrichment") or "OFF").upper()
        if got not in ENRICHMENT:
            return (f"change capture is {got} on {db}: ALTER DATABASE {db}"
                    " SET OPTION txLogEnrichment 'FULL'")
        return None

    def change_point(self, side, db):
        why = self._cdc_on(db)
        if why:
            raise SystemExit(f"{db}: its changes cannot be followed - {why}")
        return {"cdc": self._run(side, db, "CALL db.cdc.current()")[0]["id"]}

    def copy_point(self, side, db):
        return self.change_point(side, db)

    def _change(self, db, ev):
        from .. import canon
        e = ev.get("event") or {}
        state = e.get("state") or {}
        after, before = state.get("after") or {}, state.get("before") or {}
        k = self._key("src")
        props = dict((after or before).get("properties") or {})
        key = {"id": str(props.pop(k, None) or e.get("elementId"))}
        table = "nodes" if e.get("eventType") == "n" else "relationships"
        op = {"c": "insert", "u": "update", "d": "delete"}[e["operation"]]
        if op == "delete":
            return canon.change(op, table, key, txn=ev.get("txId"))
        if table == "nodes":
            values = {"id": key["id"], "labels": json.dumps(sorted(
                after.get("labels") or e.get("labels") or [])),
                "properties": json.dumps(props, sort_keys=True, default=str)}
        else:
            start, end = e.get("start") or {}, e.get("end") or {}

            def end_id(node):
                keys = node.get("keys") or {}
                got = [v for ks in keys.values() for d in ks
                       for p, v in d.items() if p == k]
                return str(got[0] if got else node.get("elementId"))
            values = {"id": key["id"], "type": e.get("type"),
                      "from": end_id(start),
                      "from_label": (sorted(start.get("labels") or [""])
                                     or [""])[0],
                      "to": end_id(end),
                      "to_label": (sorted(end.get("labels") or [""])
                                   or [""])[0],
                      "properties": json.dumps(props, sort_keys=True,
                                               default=str)}
        return canon.change(op, table, key, values, txn=ev.get("txId"))

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Each node and relationship the log says changed after `token`,
        in commit order, and the change id to go on from."""
        if token is None:
            return [], self.change_point(side, db)
        try:
            rows = self._run(side, db, "CALL db.cdc.query($from) YIELD id,"
                             " txId, seq, event RETURN id, txId, seq, event"
                             " LIMIT $most", **{"from": token["cdc"],
                                                "most": int(limit)})
        except RuntimeError as e:
            if "Neo.ClientError.ChangeDataCapture" in str(e):
                raise SystemExit(f"{db}: the change log no longer holds"
                                 f" {token['cdc']}: {e}") from None
            raise
        within = {t: self._scope(side, t) for t in TABLES}
        out = []
        for ev in rows:
            got = self._change(db, ev)
            names = within[got["table"]]
            e = ev.get("event") or {}
            tags = (e.get("labels") or [] if got["table"] == "nodes"
                    else [e.get("type")])
            if self.in_scope(db, got["table"]) and (
                    not names or set(tags) & set(names)):
                out.append(got)
        last = rows[-1]["id"] if rows else token["cdc"]
        return out, {"cdc": last}

    def position_lost(self, side, db, token):
        """Why the log no longer holds `token`, or None: the server refuses
        a change id it has pruned."""
        if not token:
            return None
        try:
            self._run(side, db, "CALL db.cdc.query($from) YIELD id RETURN id"
                      " LIMIT 1", **{"from": token["cdc"]})
        except RuntimeError as e:
            if "ChangeDataCapture" in str(e):
                return str(e).splitlines()[0][:160]
            raise
        return None

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

    def reach_items(self, side):
        items = [{"level": "pass", "scope": "reach",
                  "item": "copied and verified",
                  "detail": "nodes and relationships compared by id and by"
                            " value"}]
        if side == "src":
            items.append({"level": "pass", "scope": "reach",
                          "item": "changes followed through the database's"
                                  " change capture",
                          "detail": "needs txLogEnrichment FULL or DIFF on"
                                    " the source database"})
        return items

    def _assess_extra(self):
        return self.reach_items("src")
