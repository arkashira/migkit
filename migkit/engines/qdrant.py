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

A collection is made with the source's HNSW graph and quantization as well
as its vectors, and the deep check holds the target to them. Two ways move
a collection: its points in pages, or the server's own snapshot of it
(made, downloaded, restored); which one is decided from the collection's
measured size and the link's measured round trip and rate, and a snapshot
that does not restore to the source's count is dropped and the pages go on.
After a copy the target's search is held to the source's: the same sampled
queries are asked of both, each answer scored against the source's exact
nearest points (recall at k), and a target that finds significantly fewer
of them than the source does fails the check.
"""
import json
import math
import os
import shutil
import statistics
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

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

    def _req(self, side, method, path, data=None, headers=None):
        """The request to `path` on a side, with the key it signs in with."""
        ep = self._ep(side)
        tls = ep.options.get("tls", True) not in (False, "false", "off")
        url = (f"{'https' if tls else 'http'}://{ep.host}:"
               f"{ep.port or 6333}{path}")
        req = urllib.request.Request(
            url, method=method, data=data,
            headers=headers or {"Content-Type": "application/json"})
        key = ep.password or ep.options.get("api_key")
        if key:
            req.add_header("api-key", str(key))
        return req

    @staticmethod
    def _refused(method, path, e):
        said = ""
        try:
            said = json.loads(e.read() or b"{}").get("status", {})
            said = said.get("error", "") if isinstance(said, dict) \
                else str(said)
        except ValueError:
            said = ""
        return RuntimeError(f"{method} {path}: {e.code} {said or e.reason}")

    def _call(self, side, method, path, body=None, timeout=None):
        """The `result` of one request, or the server's status as the
        error."""
        req = self._req(side, method, path, None if body is None
                        else json.dumps(body).encode())
        try:
            with urllib.request.urlopen(req, timeout=timeout or float(
                    self._ep(side).options.get("timeout") or 120)) as resp:
                got = json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as e:
            raise self._refused(method, path, e) from None
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
            side, db, table, columns, 1000, where), engine="qdrant")

    # ---- making a collection ---------------------------------------------------

    def _shape(self, side, table):
        """What a collection is made with and holds to: its vectors, sparse
        vectors and payload indexes, as the server says them."""
        info = self._info(side, table)
        params = (info.get("config") or {}).get("params") or {}
        indexes = {name: (v.get("data_type") if isinstance(v, dict) else v)
                   for name, v in (info.get("payload_schema") or {}).items()}
        conf = info.get("config") or {}
        return {"vectors": params.get("vectors"),
                "sparse_vectors": params.get("sparse_vectors"),
                "hnsw": conf.get("hnsw_config"),
                "quantization": conf.get("quantization_config"),
                "on_disk_payload": params.get("on_disk_payload"),
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
        for field, name in (("hnsw", "hnsw_config"),
                            ("quantization", "quantization_config"),
                            ("on_disk_payload", "on_disk_payload")):
            if want.get(field) is not None:
                body[name] = want[field]
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
                    "ok" if same else "diff",
                    "as the source's" if same else
                    f"source {json.dumps(src[what], sort_keys=True)[:120]},"
                    f" target {json.dumps(dst[what], sort_keys=True)[:120]}",
                    "", "" if same else
                    "make the target collection again from the source's",
                    table=t))
            for what, label in (("hnsw", "HNSW graph"),
                                ("quantization", "quantization")):
                held = self._holds(src[what], dst[what])
                a = json.dumps(src[what], sort_keys=True)[:120]
                b = json.dumps(dst[what], sort_keys=True)[:120]
                out.append(Result(
                    "deep", f"{db} {t} {what}", "ok" if held else "diff",
                    f"the source's {label} settings are the target's" if held
                    else f"source {a}, target {b}", "",
                    "" if held else f"make the target collection again with"
                                    f" the source's {label} settings",
                    table=t))
            missing = sorted(set(src["indexes"].items())
                             - set(dst["indexes"].items()))
            out.append(Result(
                "deep", f"{db} {t} indexes", "diff" if missing else "ok",
                f"payload indexes missing or of another kind:"
                f" {', '.join(f'{n} ({k})' for n, k in missing)}" if missing
                else f"the source's {len(src['indexes'])} payload indexes all"
                     " on the target", "",
                "" if not missing else "create the payload indexes on the"
                                       " target", table=t))
        out += self.check_recall(db)
        return out or [Result("deep", db, "ok", "no collections in scope")]

    @staticmethod
    def _holds(src, dst):
        """Whether the target holds every setting the source states: a
        setting the source leaves unset is the target's to default, and a
        newer server may state more than an older one did."""
        if not src:
            return True
        if not isinstance(src, dict) or not isinstance(dst, dict):
            return src == dst
        return all(dst.get(k) == v for k, v in src.items()
                   if v is not None)

    # ---- recall at k -------------------------------------------------------------

    @staticmethod
    def recall_sample(count, k):
        """How many queries resolve a recall to one hit in `k`: the
        sample a proportion needs for a 95% margin of 1/k, corrected for a
        collection too small to need it."""
        if count <= 0:
            return 0
        wide = math.ceil(1.96 ** 2 * 0.25 * k * k)
        return min(count, math.ceil(wide / (1 + (wide - 1) / count)))

    @staticmethod
    def _query_vector(vector):
        """(name, query, kind) a point is searched by: its first dense
        vector - the name None for a collection of one unnamed vector -
        else its first sparse one, searched through the sparse index by
        its indices and values; (None, None, None) where it holds none."""
        if isinstance(vector, list):
            return None, vector, "dense"
        named = sorted((vector or {}).items())
        for name, v in named:
            if isinstance(v, list):
                return name, v, "dense"
        for name, v in named:
            if isinstance(v, dict) and v.get("indices"):
                return name, {"indices": v["indices"],
                              "values": v["values"]}, "sparse"
        return None, None, None

    def _sample_points(self, side, table, n):
        """`n` points with their vectors: drawn at random by the server
        where it can, else the leading ones by id (said as such)."""
        from .. import decide

        def run(choice):
            if choice.rung.name == "random":
                try:
                    got = self._call(side, "POST", f"/collections/{table}"
                                     "/points/query", {
                                         "query": {"sample": "random"},
                                         "limit": n, "with_vector": True,
                                         "with_payload": False}) or {}
                except RuntimeError as e:
                    raise decide.Blocked(str(e).splitlines()[0][:160]) \
                        from e
                return got.get("points") or []
            got, offset = [], None
            while len(got) < n:
                page = self._call(side, "POST", f"/collections/{table}"
                                  "/points/scroll", {
                                      "limit": n - len(got),
                                      "offset": offset, "with_vector": True,
                                      "with_payload": False}) or {}
                got += page.get("points") or []
                offset = page.get("next_page_offset")
                if offset is None:
                    break
            return got
        rungs = (decide.Rung("random", "a random sample the server draws"),
                 decide.Rung("leading", "the leading points by id"))
        choice, got = self._attempt(f"sampling {table}", rungs, run,
                                    decide.Facts({}))
        return got, choice.rung.name

    def _nearest(self, side, table, using, vector, k, exact):
        body = {"query": vector, "limit": int(k), "with_payload": False,
                "params": {"exact": bool(exact)}}
        if using:
            body["using"] = using
        got = self._call(side, "POST", f"/collections/{table}/points/query",
                         body) or {}
        return {str(p["id"]) for p in got.get("points") or []}

    def recall_of(self, db, table):
        """The recall at k the source's search and the target's reach on
        queries sampled from the source: each answer scored against the
        source's exact nearest points. {"k", "n", "source", "target",
        "behind", "slack", "sampled"} or a reason string."""
        ep = self._ep("dst")
        count = self._count("src", table)
        k = min(int(ep.options.get("recall_k") or 10), count)
        if k < 1:
            return "the collection holds no points"
        want = self.recall_sample(count, k)
        points, how = self._sample_points("src", table, want)
        src, dst, kinds = [], [], set()
        for p in points:
            using, vec, kind = self._query_vector(p.get("vector"))
            if vec is None:
                continue
            kinds.add(kind)
            truth = self._nearest("src", table, using, vec, k, True)
            src.append(len(truth & self._nearest(
                "src", table, using, vec, k, False)) / k)
            dst.append(len(truth & self._nearest(
                "dst", table, using, vec, k, False)) / k)
        if not src:
            return "the collection has no vector to search by"
        gap = [a - b for a, b in zip(src, dst)]
        slack = (statistics.stdev(gap) / math.sqrt(len(gap))
                 if len(gap) > 1 else 0.0)
        return {"k": k, "n": len(src), "source": statistics.fmean(src),
                "target": statistics.fmean(dst),
                "behind": statistics.fmean(gap), "slack": slack,
                "sampled": how, "by": " and ".join(sorted(kinds))}

    def check_recall(self, db):
        """Recall at k of the target against the source, per collection:
        failed where the target finds significantly fewer of the exact
        nearest points than the source's own index does; a warning while
        the target is still building its index, which lowers recall for a
        while."""
        out = []
        for t in self.neutral_tables("src", db):
            try:
                got = self.recall_of(db, t)
                building = (self._info("dst", t).get("status") or
                            "green") != "green"
            except RuntimeError as e:
                out.append(Result("deep", f"{db} {t} recall", "error",
                                  str(e).splitlines()[0][:120], table=t))
                continue
            if isinstance(got, str):
                out.append(Result("deep", f"{db} {t} recall", "skip", got,
                                  table=t))
                continue
            worse = got["behind"] > 2 * got["slack"] and got["behind"] > 0
            says = (f"recall at {got['k']} over {got['n']} queries"
                    f" ({'random' if got['sampled'] == 'random' else 'leading'}"
                    f" points, {got.get('by') or 'dense'} vectors):"
                    f" source {got['source']:.3f}, target"
                    f" {got['target']:.3f}")
            out.append(Result(
                "deep", f"{db} {t} recall",
                "ok" if not worse else "warn" if building else "diff",
                says if not worse else
                says + (", the target is still indexing" if building else
                        f", behind by {got['behind']:.3f}"),
                "", "" if not worse else
                "wait for the target's index to finish and check again" if
                building else "make the target collection again with the"
                              " source's HNSW and quantization settings",
                table=t))
        return out

    # ---- moving a collection: pages or its snapshot -----------------------------

    def _transfer_facts(self, table):
        """What moving `table` costs either way, from what was measured
        now: the round trip, the rate of one real page, and the bytes a
        page and a snapshot carry per point."""
        rows = self._count("src", table)
        t0 = time.monotonic()
        self._call("src", "GET", "/collections")
        rtt = time.monotonic() - t0
        t0 = time.monotonic()
        page, _ = self._page("src", table, None, BATCH)
        took = time.monotonic() - t0
        if not page:
            return {"rows": rows, "rtt": rtt, "scroll_s": 0.0,
                    "snapshot_s": float("inf")}
        wire = sum(len(c) for r in page for c in r) / len(page)
        floats = sum(r[1].count(",") + 1 for r in page) / len(page)
        raw = floats * 4 + sum(len(r[2]) for r in page) / len(page)
        rate = sum(len(c) for r in page for c in r) / max(took - rtt, 1e-6)
        pages = math.ceil(rows / BATCH)
        return {"rows": rows, "rtt": rtt, "rate": rate,
                "scroll_s": pages * 2 * rtt + 2 * rows * wire / rate,
                "snapshot_s": 4 * rtt + 3 * rows * raw / rate}

    def _move_ways(self):
        from .. import decide
        return (
            decide.Rung(
                "snapshot", "the server's snapshot of the collection,"
                            " restored on the target",
                needs=(
                    decide.Need("the points moved as they are",
                                lambda f: not f["changed"],
                                "{rung} carries points as stored, and a"
                                " mask or row filter changes them on the"
                                " way"),
                    decide.Need("nothing of it on the target yet",
                                lambda f: f["fresh"],
                                "{rung} restores a whole collection, and the"
                                " target already holds this one"),
                    decide.Need("no earlier copy to resume",
                                lambda f: not f["done"],
                                "an earlier run finished this collection:"
                                " its copy is checked, not made again"),
                    decide.Need("a collection large enough that one stream"
                                " beats its pages",
                                lambda f: f["transfer"]["snapshot_s"]
                                < f["transfer"]["scroll_s"],
                                lambda f: "the collection's pages are"
                                f" quicker here: about"
                                f" {f['transfer']['scroll_s']:.1f}s against"
                                f" {f['transfer']['snapshot_s']:.1f}s for a"
                                " snapshot")),
                because="one stream of the stored segments, with the HNSW"
                        " graph and quantization they were built with",
                leaves="a snapshot on the source until it is dropped"),
            decide.Rung("pages", "the points in pages by their id",
                        leaves="nothing"))

    def move_table(self, db, sch, tbl, chunk, ck, log):
        from .. import decide
        key = self.move_key(db, sch, tbl)

        def changed():
            try:
                return bool(self.hop.masks(db, tbl) or
                            self.hop.row_filter(db, tbl))
            except SystemExit:
                return True
        facts = decide.Facts(
            {"changed": changed(), "done": bool((ck.get(key) or {}).get(
                "done")), "fresh": tbl not in self.neutral_tables("dst", db)},
            transfer=lambda: self._transfer_facts(tbl))

        def run(choice):
            if choice.rung.name == "snapshot":
                return self._snapshot_move(tbl, key, ck, log,
                                           facts["transfer"]["snapshot_s"])
            return super(QdrantEngine, self).move_table(
                db, sch, tbl, chunk, ck, log)
        return self._attempt(f"moving {tbl}", self._move_ways(), run, facts,
                             log=log)[1]

    def _snapshot_move(self, table, key, ck, log, expected):
        """The server's snapshot of `table` made, carried and restored as
        the target's collection; the source's snapshot is dropped at the
        end, and a restore that lands on another count is removed so the
        pages start from nothing."""
        from .. import decide
        wait = max(120.0, 4 * expected)
        made = self._call("src", "POST",
                          f"/collections/{table}/snapshots?wait=true",
                          timeout=wait) or {}
        name = made.get("name")
        if not name:
            raise decide.Blocked("the server named no snapshot")
        base = f"/collections/{table}/snapshots/{name}"
        try:
            with tempfile.TemporaryDirectory(prefix="migkit-snap-") as tmp:
                body = os.path.join(tmp, "upload")
                boundary = "migkit" + uuid.uuid4().hex
                head = (f"--{boundary}\r\nContent-Disposition: form-data;"
                        f' name="snapshot"; filename="'
                        f'{urllib.parse.quote(name)}"\r\nContent-Type:'
                        " application/octet-stream\r\n\r\n").encode()
                with open(body, "wb") as out:
                    out.write(head)
                    try:
                        with urllib.request.urlopen(
                                self._req("src", "GET", base),
                                timeout=wait) as resp:
                            shutil.copyfileobj(resp, out)
                    except urllib.error.HTTPError as e:
                        raise decide.Blocked(str(self._refused(
                            "GET", base, e))) from None
                    out.write(f"\r\n--{boundary}--\r\n".encode())
                with open(body, "rb") as data:
                    req = self._req(
                        "dst", "POST", f"/collections/{table}/snapshots/"
                        "upload?wait=true&priority=snapshot", data, {
                            "Content-Type": "multipart/form-data;"
                                            f" boundary={boundary}",
                            "Content-Length": str(os.path.getsize(body))})
                    try:
                        urllib.request.urlopen(req, timeout=wait).close()
                    except urllib.error.HTTPError as e:
                        raise decide.Blocked(str(self._refused(
                            "POST", "snapshots/upload", e))) from None
            want, got = self._count("src", table), self._count("dst", table)
            if got != want:
                self._call("dst", "DELETE", f"/collections/{table}")
                raise decide.Blocked(f"the restored collection holds"
                                     f" {got:,} points, the source {want:,}")
        finally:
            try:
                self._call("src", "DELETE", base + "?wait=true")
            except RuntimeError:
                log(f"{table}: the snapshot {name} is still on the source:"
                    " drop it there")
        st = ck.setdefault(key, {})
        st["done"], st["way"] = True, "snapshot"
        ck.save()
        log(f"{table}: {got:,} points restored from the server's snapshot")
        return None

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
                  "detail": "points compared by id, vectors and payload;"
                            " search quality held to the source's by recall"
                            " at k, index settings by name"}]
        if side == "src":
            items.append({"level": "warn", "scope": "reach",
                          "item": "changes not followed",
                          "detail": "Qdrant keeps no change log a client can"
                                    " read: copy and check again before the"
                                    " switch"})
        return items

    def _assess_extra(self):
        return self.reach_items("src")
