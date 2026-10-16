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
refused, naming the setting. A change id is opaque - the server
promises no order to read off it - so a position is counted: the changes
read since the copy point travel with the id (`change_position`), and the
changes still ahead of it are asked of the log (`changes_behind`).

A database can also move as a dump loaded on the target (`neo4j-admin`,
named by the side's `admin` option, through a directory both reach,
`dump_dir`). It stops both databases, so it is taken only where both
sides' `stop_database` option says it may be, the target holds nothing,
and a graph is large enough that the dump and load, timed on the disk
measured in `dump_dir`, beat the id ranges timed on the first one read.
Users are carried by name, status, home database and roles; their
passwords are not readable, so each is made with a password nobody knows
and must be reset.
"""
import base64
import json
import os
import secrets
import shlex
import shutil
import subprocess
import time
import uuid
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

    def tail_where(self, db, token):
        """What the tail's quiet line stands on (`tailwhere.where_from`): the
        tail's position and the source's now (`log_position`). The server keeps no
        count of its writes the tail could read in one bounded round, and lists
        no reader of the tail's: both are said unread, never assumed."""
        from .. import tailctl
        from .. import tailwhere
        return tailwhere.where_from(self, db, token)

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

    def neutral_batches(self, side, db, table, columns, size=None,
                        where=None):
        from .. import decide
        size = decide.cursor_rows(size)
        self._rowfilter(table, where)
        names = [n for n, _ in columns]
        at = [n for n, _ in COLUMNS[table]]
        for rows in self._all(side, db, table, size or BATCH):
            yield [[dict(zip(at, r)).get(n) for n in names] for r in rows]

    def neutral_read(self, side, db, table, columns, after=None, limit=None,
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
            side, db, table, columns, 5000, where), engine="neo4j")

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

    # ---- moving a database: id ranges or its dump ------------------------------

    def _admin(self, side):
        got = self._ep(side).options.get("admin")
        return shlex.split(got) if isinstance(got, str) else list(got or [])

    def _may_stop(self, side):
        return self._ep(side).options.get("stop_database") in (
            True, "true", "on", "yes")

    def _folder(self, side):
        o = self._ep(side).options
        return str(o.get("dump_dir") or self._ep("src").options.get(
            "dump_dir") or "")

    def _sizes(self, db):
        """What a copy by ranges costs, from the first range read: the
        elements, the bytes a stored element takes as the wire carries it
        and the seconds the whole graph would take at the rate that read
        ran at."""
        t0 = time.monotonic()
        rows, _ = self._range("src", db, "nodes", -1, BATCH)
        took = max(time.monotonic() - t0, 1e-6)
        per = (sum(len(json.dumps(r, default=str)) for r in rows)
               / len(rows)) if rows else 0
        n = sum(self._count("src", db, t) for t in TABLES)
        rate = (len(rows) * per) / took
        return {"elements": n, "bytes": n * per,
                "ranges_s": n * per / rate if rate else 0.0}

    def _disk_rate(self, folder):
        """Bytes a second the folder takes a written, synced file at,
        timed on a probe file that is removed."""
        path = os.path.join(folder, f".probe-{uuid.uuid4().hex}")
        blob = os.urandom(8 << 20)
        t0 = time.monotonic()
        try:
            with open(path, "wb") as f:
                f.write(blob)
                f.flush()
                os.fsync(f.fileno())
        finally:
            took = max(time.monotonic() - t0, 1e-6)
            if os.path.exists(path):
                os.unlink(path)
        return len(blob) / took

    def _move_ways(self):
        from .. import decide
        return (
            decide.Rung(
                "dump", "the database dumped and loaded on the target",
                needs=(
                    decide.Need(
                        "every element moved as it is",
                        lambda f: not f["changed"],
                        "{rung} carries the whole store, and a row filter,"
                        " mask, label or type scope changes what moves"),
                    decide.Need(
                        "both sides allowed to stop the database",
                        lambda f: f["stoppable"],
                        "{rung} stops the database on both sides, and the"
                        " `stop_database` option of one of them does not"
                        " allow it"),
                    decide.Need(
                        "an admin command and a shared directory",
                        lambda f: f["admin"],
                        "{rung} needs the `admin` command on both sides"
                        " (found on this machine) and a `dump_dir` they"
                        " both reach"),
                    decide.Need("a target holding nothing",
                                lambda f: f["empty"],
                                "{rung} replaces the target's database, and"
                                " the target holds elements"),
                    decide.Need(
                        "a graph the ranges would take longer over",
                        lambda f: f["sizes"]["ranges_s"] > 2 * f["sizes"][
                            "bytes"] / f["disk"],
                        lambda f: f"the ranges take about"
                        f" {f['sizes']['ranges_s']:.1f}s, no longer than a"
                        " dump and load on this disk")),
                because="the store copied whole, ids and indexes with it",
                leaves="a dump file in the shared directory", external=True),
            decide.Rung("ranges", "the elements in ranges of their id",
                        leaves="nothing"))

    def _program(self, cmd, log):
        done = subprocess.run(cmd, capture_output=True, text=True)
        if done.returncode:
            from .. import decide
            raise decide.Blocked(
                (done.stderr or done.stdout or "the command failed"
                 ).strip().splitlines()[-1][:160])
        log(f"{' '.join(cmd[-3:-1])}: done")

    def _dump_load(self, db, log):
        """The source's database dumped while stopped and loaded on the
        target while stopped; both are started again whatever happened."""
        from .. import decide
        sdb, tdb = self._d("src", db), self._d("dst", db)
        folder = self._folder("src")
        known = {r["name"] for r in self._run(
            "dst", "system", "SHOW DATABASES YIELD name RETURN name")}
        self._run("src", "system", f"STOP DATABASE {quoted(sdb)} WAIT")
        try:
            self._program(self._admin("src") + [
                "database", "dump", f"--to-path={folder}", sdb], log)
        finally:
            self._run("src", "system", f"START DATABASE {quoted(sdb)} WAIT")
        if tdb in known:
            self._run("dst", "system", f"STOP DATABASE {quoted(tdb)} WAIT")
        try:
            self._program(self._admin("dst") + [
                "database", "load", f"--from-path={self._folder('dst')}",
                "--overwrite-destination=true", tdb], log)
        finally:
            self._run("dst", "system", f"CREATE DATABASE {quoted(tdb)} IF NOT"
                      " EXISTS WAIT" if tdb not in known else
                      f"START DATABASE {quoted(tdb)} WAIT")
        for t in TABLES:
            a, b = self._count("src", db, t), self._count("dst", db, t)
            if a != b:
                raise decide.Blocked(f"the loaded database holds {b:,}"
                                     f" {t}, the source {a:,}")

    def move_table(self, db, sch, tbl, chunk, ck, log):
        from .. import decide
        if tbl != "nodes":
            return super().move_table(db, sch, tbl, chunk, ck, log)
        src, dst = self._admin("src"), self._admin("dst")

        def changed():
            try:
                return any(self.hop.row_filter(db, t) or self.hop.masks(db, t)
                           or self._scope("src", t) or self._scope("dst", t)
                           or not self.in_scope(db, t) for t in TABLES)
            except SystemExit:
                return True
        facts = decide.Facts(
            {"changed": changed(), "stoppable": self._may_stop("src") and self._may_stop("dst"),
             "admin": bool(src and dst and self._folder("src")
                           and self._folder("dst")
                           and shutil.which(src[0]) and shutil.which(dst[0])
                           and os.path.isdir(self._folder("src")))},
            empty=lambda: not any(self._count("dst", db, t) for t in TABLES),
            sizes=lambda: self._sizes(db),
            disk=lambda: self._disk_rate(self._folder("src")))

        def run(choice):
            if choice.rung.name == "dump":
                self._dump_load(db, log)
                for t in TABLES:
                    st = ck.setdefault(self.move_key(db, "", t), {})
                    st["done"], st["way"] = True, "dump"
                ck.save()
                return None
            return super(Neo4jEngine, self).move_table(db, sch, tbl, chunk,
                                                       ck, log)
        return self._attempt(f"moving {db}", self._move_ways(), run, facts,
                             log=log)[1]

    # ---- users ----------------------------------------------------------------

    def _users(self, side):
        try:
            rows = self._run(side, "system", "SHOW USERS YIELD user, roles,"
                             " suspended, home RETURN user, roles,"
                             " suspended, home")
        except RuntimeError:
            rows = self._run(side, "system", "SHOW USERS YIELD user,"
                             " suspended, home RETURN user, suspended, home")
        return [r for r in rows if r.get("user")]

    def carry_database_objects(self, db, log=None):
        """The source's users made on the target: name, status, home
        database and roles. A password cannot be read, so each is made with
        one nobody knows and must be changed at first sign-in."""
        say = log or (lambda m: None)
        have = {u["user"] for u in self._users("dst")}
        made, unroled = [], []
        for u in self._users("src"):
            if u["user"] in have:
                continue
            name = quoted(u["user"])
            home = (f" SET HOME DATABASE {quoted(u['home'])}"
                    if u.get("home") else "")
            self._run("dst", "system", f"CREATE USER {name} IF NOT EXISTS"
                      f" SET PASSWORD $pw CHANGE REQUIRED SET STATUS"
                      f" {'SUSPENDED' if u.get('suspended') else 'ACTIVE'}"
                      f"{home}", pw=secrets.token_urlsafe(24))
            for role in u.get("roles") or []:
                try:
                    self._run("dst", "system",
                              f"GRANT ROLE {quoted(role)} TO {name}")
                except RuntimeError:
                    unroled.append(f"{u['user']}:{role}")
            made.append(u["user"])
        if made:
            say(f"{len(made)} users made on the target with a password"
                " nobody knows: reset each one's password before it signs in")
        if unroled:
            say("roles the target does not have, not granted: "
                + ", ".join(unroled[:5]))
        return made

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
                "deep", f"{db} {what}", "diff" if missing else "ok",
                f"{len(missing)} of the source's {len(src)} {what} not on"
                f" the target: {', '.join(missing[:5])}" if missing else
                f"the source's {len(src)} {what} all on the target", "",
                "" if not missing else
                f"create the source's {what} on the target before cutover"))
        return out

    # ---- the server's settings, and the statistics after a load ----------------

    #: the settings that bear on a move, by what each kind holds: the
    #: server lists its settings by name, and a name under one of these
    #: prefixes is compared (paths, ports and hostnames are the server's own
    #: and always differ)
    SETTING_KINDS = (
        ("memory", ("server.memory.", "dbms.memory.")),
        ("transaction", ("db.transaction.", "dbms.transaction.",
                         "db.tx_log.", "db.lock.", "db.checkpoint.")),
        ("import", ("db.import.", "dbms.import.", "server.import.",
                    "dbms.security.allow_csv_import_from_file_urls")),
        ("cypher", ("dbms.cypher.", "db.cypher.", "server.cypher.")),
        ("statistics", ("db.index_sampling.", "db.stats.")),
        ("storage", ("db.format", "db.recovery.")),
    )

    @classmethod
    def _setting_kind(cls, name):
        for kind, prefixes in cls.SETTING_KINDS:
            if any(name.startswith(p) for p in prefixes):
                return kind
        return ""

    def _settings(self, side, db):
        """The settings of the kinds that bear on a move, as the server
        lists them, by name."""
        listed = ("SHOW SETTINGS YIELD name, value RETURN name, value"
                  if self._major(side, db) >= 5 else
                  "CALL dbms.listConfig() YIELD name, value RETURN name,"
                  " value")
        return {r["name"]: r["value"] for r in self._run(side, "system",
                                                        listed)
                if self._setting_kind(r["name"])}

    def check_params(self, db):
        def pull(side):
            try:
                return self._settings(side, db)
            except (RuntimeError, SystemExit) as e:
                return {self.UNREADABLE: str(e).splitlines()[-1][:80]}
        src, dst = pull("src"), pull("dst")
        return self._param_result(
            db, src, dst, tuple(set(src) | set(dst)),
            "set the target's memory, transaction, import and cypher"
            " settings to the source's, or to what the graph needs")

    def _index_states(self, db):
        """(name, state) of every index of the target a copy may have
        waited on: the lookup indexes the server keeps are not."""
        return [(r["name"], r["state"]) for r in self._run(
            "dst", db, "SHOW INDEXES YIELD name, state, type WHERE type <>"
            " 'LOOKUP' RETURN name, state")]

    def _settle_facts(self, db, from_source):
        """What the way to refresh the statistics is chosen from, read from
        the target and the server it is on now."""
        def listed(statement, column):
            try:
                return [r[column] for r in self._run("dst", db, statement)]
            except RuntimeError:
                return []
        rows = sum(self._count("dst", db, t) for t in TABLES)
        loaded = rows
        if from_source:
            loaded = min(rows, sum(self._count("src", db, t)
                                   for t in TABLES))
        sampling = None
        for r in self._run(
                "dst", "system",
                "SHOW SETTINGS YIELD name, value WHERE name ="
                " 'db.index_sampling.update_percentage' RETURN value"
                if self._major("dst", db) >= 5 else
                "CALL dbms.listConfig('db.index_sampling.update_percentage')"
                " YIELD value RETURN value"):
            try:
                sampling = float(r["value"])
            except (TypeError, ValueError):
                sampling = None
        from .. import decide
        return decide.Facts({
            "indexes": len(self._index_states(db)), "rows": rows,
            "loaded": loaded, "sampling": sampling,
            "procedures": listed(
                "SHOW PROCEDURES YIELD name WHERE name STARTS WITH"
                " 'db.resample' OR name = 'db.prepareForReplanning'"
                " RETURN name", "name")})

    def settle_choice(self, db):
        """The last refresh's `Choice`, or None before one."""
        return self.__dict__.get("_settled", {}).get(db)

    def settle_target(self, db, from_source=True):
        """Bring the target's indexes online and refresh their statistics
        after a load.

        The server samples an index on its own once more than its
        `db.index_sampling.update_percentage` of it changed; a load changes
        far more at once, and the planner then picks plans on a sample of a
        graph that was empty. How far to go - resample every index, the ones
        the server finds outdated, or only wait for them to come online - is
        `decide.settle_way`'s, from the indexes and the rows loaded."""
        from .. import decide
        ddb = self._d("dst", db)
        timeout = int(float(self.hop.target.options.get("timeout") or 120))
        try:
            facts = self._settle_facts(db, from_source)
            choice = decide.settle_way(facts)
            self.__dict__.setdefault("_settled", {})[db] = choice
            if not facts["indexes"]:
                return None
            self._run("dst", db, "CALL db.awaitIndexes($t)", t=timeout)
            if choice.path == "resample":
                if "db.prepareForReplanning" in facts["procedures"]:
                    self._run("dst", db, "CALL db.prepareForReplanning()")
                else:
                    for name, _ in self._index_states(db):
                        self._run("dst", db, "CALL db.resampleIndex($n)",
                                  n=name)
            elif choice.path == "outdated":
                self._run("dst", db, "CALL db.resampleOutdatedIndexes()")
            states = self._index_states(db)
        except (RuntimeError, SystemExit) as e:
            return (f"could not refresh the statistics of {ddb}:"
                    f" {str(e).splitlines()[-1][:80]}")
        down = sorted(n for n, st in states if st != "ONLINE")
        said = (f"{choice.said} after {facts['loaded']:,} of"
                f" {facts['rows']:,} elements loaded")
        if down:
            return (f"{len(down)} of {len(states)} indexes of {ddb} not"
                    f" online ({', '.join(down[:5])}); the statistics wait"
                    f" for them ({said})")
        return (f"refreshed the target's statistics: {len(states)} indexes"
                f" online, {said}")

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
        return {"cdc": self._run(side, db, "CALL db.cdc.current()")[0]["id"],
                "seen": 0}

    @staticmethod
    def change_position(token):
        """Changes read since the copy point: the server's change id says
        nothing of order, so the position is counted beside it."""
        return int((token or {}).get("seen") or 0)

    def changes_behind(self, side, db, token, most=100_000):
        """(changes in the log after `token`, whether that is all of them):
        counted up to `most`, which is what a fence waits to see at zero."""
        rows = self._run(side, db, "CALL db.cdc.query($from) YIELD id RETURN"
                         " id LIMIT $most", **{"from": token["cdc"],
                                               "most": int(most)})
        return len(rows), len(rows) < int(most)

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

    def neutral_changes(self, side, db, token=None, limit=None):
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
        return out, {"cdc": last, "seen": self.change_position(token)
                     + len(rows)}

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
        """Where the source's log is now, while a tail follows it: the
        change id `db.cdc.current()` names, for `fence_wait` to count
        from."""
        from .. import tailctl
        from .. import tailwhere
        if not tailctl.alive(self.hop.report_dir(db), self.hop):
            return None
        try:
            return {"cdc": self._run("src", db,
                                     "CALL db.cdc.current()")[0]["id"]}
        except (RuntimeError, IndexError, KeyError):
            return None

    def fence_reached(self, db, have, want):
        """Whether a tail at `have` has read as far as `want`: True, False,
        or None where the log no longer holds the tail's position.

        A change id says nothing of order, so the two are compared by what
        the log holds after each (`changes_behind`): a tail at or past
        `want` has no more changes ahead of it than `want` has. The ones
        after `want` are counted first, so a change committed between the
        two counts can only make the tail look further behind; the tail's
        count stops one past that, all the answer needs."""
        if have.get("cdc") == want.get("cdc"):
            return True
        try:
            ahead, _ = self.changes_behind("src", db, want)
            behind, whole = self.changes_behind("src", db, have, ahead + 1)
        except RuntimeError:
            if self.position_lost("src", db, have):
                return None
            raise
        return whole and behind <= ahead

    def fence_wait(self, db, lsn, timeout=300):
        """Wait until the tail's saved position has reached `lsn`: True,
        False on timeout, None where there is nothing to wait on or the
        log has dropped the tail's position. Each look counts the log on
        the source, so the next one waits at least twice as long as the
        last count took."""
        from .. import tailctl
        from .. import tailwhere
        if lsn is None:
            return None
        path = tailctl.token_path(self.hop, db)
        began = time.monotonic()
        while time.monotonic() - began < timeout:
            try:
                have = tailctl.saved(path)
            except SystemExit:
                return None
            took = 0.0
            if have is not None:
                at = time.monotonic()
                try:
                    reached = self.fence_reached(db, have, lsn)
                except RuntimeError:
                    # a count the server did not answer proves nothing:
                    # not reached, looked at again
                    reached = False
                took = time.monotonic() - at
                if reached is None:
                    return None
                if reached:
                    return True
            time.sleep(max(tailctl.POLL, 2 * took))
        return False

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
                                    " the source database; change ids are"
                                    " opaque, so positions are counted"
                                    " changes, not read from the id"})
        return items

    def _assess_extra(self):
        return self.reach_items("src")
