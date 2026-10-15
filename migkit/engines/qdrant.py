"""Qdrant as a side of a pair (backlog R16b), through its HTTP API.

A hop's database is the server (`qdrant` where it names none); its tables
are its collections, each one table of points: a point's id, its vectors
and its payload. Vectors are one JSON value - a list for a collection of
one unnamed vector, an object of named ones (sparse ones as their indices
and values) - and the payload another, both with their keys sorted. A
point is compared by id: the id is the key, a number or a UUID as the
server holds it.

Read by scroll, in the order of the id, each page from the last one's
offset, so a copy goes in ranges and a stopped one resumes at a page.
Written by upsert, each point whole, a batch of them in one request,
waited for until the server has applied it. A collection is made on the
target as the source's is: its vectors' size and distance, its sparse
vectors, its payload indexes, then filled.

Qdrant keeps no change log a client can read, so its changes are not
followed: a move is copied and verified, and its last changes are caught
by a second pass of the copy and check before a switch.
"""
import json
import urllib.error
import urllib.request

from .base import Engine, NeutralCopier, Result

COLUMNS = [("id", "string"), ("vector", "json"), ("payload", "json")]
KEY = ["id"]
#: points one request writes
BATCH = 256


def point_id(text):
    """A point's id as the server takes it: a number where it is one, else
    the UUID it was read as."""
    text = str(text)
    return int(text) if text.isdigit() else text


class QdrantEngine(NeutralCopier, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "qdrant"
    FOLDED_BECAUSE = "has no way to hash points in the server"
    OVER_NETWORK = True
    RESUMES_BY_KEY = False
    CREATES_ON_WRITE = False
    READS_AHEAD = False

    def _ep(self, side):
        return self.hop.source if side == "src" else self.hop.target

    def databases(self):
        return list(self.hop.databases or ["qdrant"])

    def _call(self, side, method, path, body=None):
        """The `result` of one request, or the server's status as the
        error."""
        ep = self._ep(side)
        tls = ep.options.get("tls", True) not in (False, "false", "off")
        url = (f"{'https' if tls else 'http'}://{ep.host}:"
               f"{ep.port or 6333}{path}")
        req = urllib.request.Request(
            url, method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json"})
        key = ep.password or ep.options.get("api_key")
        if key:
            req.add_header("api-key", str(key))
        try:
            with urllib.request.urlopen(req, timeout=float(
                    ep.options.get("timeout") or 120)) as resp:
                got = json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as e:
            said = ""
            try:
                said = json.loads(e.read() or b"{}").get("status", {})
                said = said.get("error", "") if isinstance(said, dict) \
                    else str(said)
            except ValueError:
                said = ""
            raise RuntimeError(f"{method} {path}: {e.code} {said or e.reason}"
                               ) from None
        return got.get("result")

    def _info(self, side, table):
        return self._call(side, "GET", f"/collections/{table}") or {}

    # ---- the tables ---------------------------------------------------------

    def neutral_tables(self, side, db):
        got = self._call(side, "GET", "/collections") or {}
        names = sorted(c["name"] for c in got.get("collections") or [])
        return [n for n in names if self.in_scope(db, n)]

    def neutral_columns(self, side, db, table):
        return list(COLUMNS)

    def neutral_key(self, side, db, table):
        return list(KEY)

    def _count(self, side, table):
        got = self._call(side, "POST", f"/collections/{table}/points/count",
                         {"exact": True}) or {}
        return int(got.get("count") or 0)

    def table_facts(self, side, db):
        return {t: {"rows": self._count(side, t), "bytes": None, "key": True}
                for t in self.neutral_tables(side, db)}

    @staticmethod
    def _row(point):
        return [str(point["id"]),
                json.dumps(point.get("vector"), sort_keys=True),
                json.dumps(point.get("payload") or {}, sort_keys=True)]

    def _page(self, side, table, offset, size):
        """One page of points from `offset` on, in id order, and the offset
        the next page starts at (None after the last)."""
        body = {"limit": int(size), "with_payload": True, "with_vector": True}
        if offset is not None:
            body["offset"] = offset
        got = self._call(side, "POST", f"/collections/{table}/points/scroll",
                         body) or {}
        return ([self._row(p) for p in got.get("points") or []],
                got.get("next_page_offset"))

    def _rowfilter(self, table, where):
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and a collection"
                             " takes none")

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        self._rowfilter(table, where)
        names = [n for n, _ in columns]
        at = [n for n, _ in COLUMNS]
        offset = None
        while True:
            rows, offset = self._page(side, table, offset, size or BATCH)
            if rows:
                yield [[dict(zip(at, r)).get(n) for n in names] for r in rows]
            if offset is None:
                return

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
        at = [n for n, _ in COLUMNS]
        ids = [point_id(k[0] if isinstance(k, (list, tuple)) else k)
               for k in keys]
        found = []
        for i in range(0, len(ids), BATCH):
            got = self._call(side, "POST", f"/collections/{table}/points", {
                "ids": ids[i:i + BATCH], "with_payload": True,
                "with_vector": True}) or []
            found += [[dict(zip(at, self._row(p))).get(n) for n in names]
                      for p in got]
        return self._by_key_map(columns, key, found)

    def neutral_digest(self, side, db, table, columns, where=None):
        from .. import canon
        return canon.fold_batches(columns, self.neutral_batches(
            side, db, table, columns, 1000, where))

    # ---- making a collection ---------------------------------------------------

    def _shape(self, side, table):
        """What a collection is made with and holds to: its vectors, sparse
        vectors and payload indexes, as the server says them."""
        info = self._info(side, table)
        params = (info.get("config") or {}).get("params") or {}
        indexes = {name: (v.get("data_type") if isinstance(v, dict) else v)
                   for name, v in (info.get("payload_schema") or {}).items()}
        return {"vectors": params.get("vectors"),
                "sparse_vectors": params.get("sparse_vectors"),
                "indexes": indexes}

    def neutral_create_sql(self, side, db, table, columns, key=()):
        got = self._shape("src", table)
        if not got["vectors"] and not got["sparse_vectors"]:
            raise SystemExit(f"{table}: a collection is made from the"
                             " source's vectors, and the source has no"
                             " collection of that name")
        return json.dumps(got, sort_keys=True)

    def neutral_create(self, side, db, table, columns, key=()):
        self._target_only(side, "create a table")
        want = json.loads(self.neutral_create_sql(side, db, table, columns,
                                                  key))
        body = {"vectors": want["vectors"]}
        if want["sparse_vectors"]:
            body["sparse_vectors"] = want["sparse_vectors"]
        self._call("dst", "PUT", f"/collections/{table}", body)
        for name, kind in sorted(want["indexes"].items()):
            self._call("dst", "PUT", f"/collections/{table}/index?wait=true",
                       {"field_name": name, "field_schema": kind})
        return None

    # ---- writing --------------------------------------------------------------

    def _write(self, table, rows):
        for i in range(0, len(rows), BATCH):
            self._call("dst", "PUT", f"/collections/{table}/points?wait=true",
                       {"points": [{"id": point_id(r[0]),
                                    "vector": json.loads(r[1]),
                                    "payload": json.loads(r[2] or "{}")}
                                   for r in rows[i:i + BATCH]]})

    def neutral_write(self, side, db, table, columns, rows):
        self._target_only(side, "write rows")
        if not rows:
            return 0
        names = [n for n, _ in columns]
        at = [n for n, _ in COLUMNS]
        self._write(table, [[dict(zip(names, r)).get(n) for n in at]
                            for r in rows])
        return len(rows)

    def _apply_upserts(self, side, db, table, rows):
        at = [n for n, _ in COLUMNS]
        self._write(table, [[{**values, **key}.get(n) for n in at]
                            for key, values in rows])

    def _apply_upsert(self, side, db, table, key, values):
        self._apply_upserts(side, db, table, [(key, values)])

    def _apply_deletes(self, side, db, table, keys):
        ids = [point_id(k["id"]) for k in keys]
        for i in range(0, len(ids), BATCH):
            self._call("dst", "POST",
                       f"/collections/{table}/points/delete?wait=true",
                       {"points": ids[i:i + BATCH]})

    def _apply_delete(self, side, db, table, key):
        self._apply_deletes(side, db, table, [key])

    def neutral_empty(self, side, db, table, where=None):
        self._target_only(side, "empty a table")
        self._rowfilter(table, where)
        gone = 0
        while True:
            rows, _ = self._page("dst", table, None, 1000)
            if not rows:
                return gone
            self._apply_deletes("dst", db, table, [{"id": r[0]}
                                                   for r in rows])
            gone += len(rows)

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

    def check_deep(self, db):
        """Each collection's vectors and payload indexes held to the
        source's: a vector of another size or distance ranks every search
        differently, and a payload index missing makes a filtered search a
        full scan."""
        out = []
        for t in self.neutral_tables("src", db):
            try:
                src, dst = self._shape("src", t), self._shape("dst", t)
            except RuntimeError as e:
                out.append(Result("deep", f"{db} {t} vectors", "error",
                                  str(e).splitlines()[0][:120], table=t))
                continue
            for what in ("vectors", "sparse_vectors"):
                same = src[what] == dst[what]
                out.append(Result(
                    "deep", f"{db} {t} {what.replace('_', ' ')}",
                    "ok" if same else "fail",
                    "as the source's" if same else
                    f"source {json.dumps(src[what], sort_keys=True)[:120]},"
                    f" target {json.dumps(dst[what], sort_keys=True)[:120]}",
                    "", "" if same else
                    "make the target collection again from the source's",
                    table=t))
            missing = sorted(set(src["indexes"].items())
                             - set(dst["indexes"].items()))
            out.append(Result(
                "deep", f"{db} {t} indexes", "fail" if missing else "ok",
                f"payload indexes missing or of another kind:"
                f" {', '.join(f'{n} ({k})' for n, k in missing)}" if missing
                else f"the source's {len(src['indexes'])} payload indexes all"
                     " on the target", "",
                "" if not missing else "create the payload indexes on the"
                                       " target", table=t))
        return out or [Result("deep", db, "ok", "no collections in scope")]

    # ---- who it is -------------------------------------------------------------

    def server_identity(self, side, db):
        ep = self._ep(side)
        out = {"name": str(ep.host), "id": f"{ep.host}:{ep.port or 6333}"}
        try:
            got = self._call(side, "GET", "/cluster") or {}
        except (RuntimeError, OSError):
            return out
        if got.get("peer_id") is not None:
            out["id"] = str(got["peer_id"])
        return out

    def reach_items(self, side):
        items = [{"level": "pass", "scope": "reach",
                  "item": "copied and verified",
                  "detail": "points compared by id, vectors and payload"}]
        if side == "src":
            items.append({"level": "warn", "scope": "reach",
                          "item": "changes not followed",
                          "detail": "Qdrant keeps no change log a client can"
                                    " read: copy and check again before the"
                                    " switch"})
        return items

    def _assess_extra(self):
        return self.reach_items("src")
