"""OpenSearch (and Elasticsearch) as one side of a pair (backlog 34).

A database is a prefix on index names (`<db>.` by default, the endpoint's
`index_prefix` otherwise, with `{db}` in it), and a table is an index. A
document's id is the canonical text of the row's key, so a row written
again replaces itself. What each column was is kept in the index
mapping's own `_meta`, and a document's `_source` keeps each value as it
was sent: a decimal is sent as its text and comes back with its scale,
where a JSON number would have lost it. An index migkit did not create is
described by its mapping, and keyed by `_id`.

Documents come back in no order, so an index is read in one pass through
a scroll, and looked up by key through `_mget`. What was written is made
visible to reads before migkit reads the target (a refresh), never on the
source.
"""
import base64
import json
import ssl
import urllib.error
import urllib.request

from .base import Engine, NeutralCopier, Result

#: mapping for each class, and the kinds of field a mapping names
MAPPING = {
    "integer": {"type": "long"},
    "float": {"type": "double"},
    "boolean": {"type": "boolean"},
    "text": {"type": "text",
             "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
    "bytes": {"type": "binary"},
    "date": {"type": "date", "format": "yyyy-MM-dd"},
    "timestamp": {"type": "date_nanos",
                  "format": "yyyy-MM-dd HH:mm:ss.SSSSSS"},
    "time": {"type": "keyword"},
}


#: the class a field of an index migkit did not make holds, by its type;
#: a `date` there is a moment, a timestamp
FIELDS = {"long": "integer", "integer": "integer", "short": "integer",
          "byte": "integer", "unsigned_long": "integer",
          "double": "float", "float": "float", "half_float": "float",
          "scaled_float": "float", "keyword": "text", "text": "text",
          "match_only_text": "text", "wildcard": "text",
          "constant_keyword": "text", "boolean": "boolean",
          "date": "timestamp", "date_nanos": "timestamp",
          "binary": "bytes"}


class OpenSearchEngine(NeutralCopier, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "opensearch"
    FOLDED_BECAUSE = "has no way to hash documents in the service"
    OVER_NETWORK = True
    RESUMES_BY_KEY = False

    # ---- the wire ---------------------------------------------------------

    def _call(self, side, method, path, body=None, ndjson=False,
              missing_ok=False):
        ep = self.hop.source if side == "src" else self.hop.target
        base = str(ep.options.get("url") or
                   f"{'https' if ep.options.get('tls') else 'http'}://"
                   f"{ep.host}:{ep.port or 9200}").rstrip("/")
        data = None
        if body is not None:
            data = (body if ndjson else json.dumps(body)).encode()
        req = urllib.request.Request(base + path, data=data, method=method)
        req.add_header("Content-Type", "application/x-ndjson" if ndjson
                       else "application/json")
        if ep.user:
            token = base64.b64encode(f"{ep.user}:{ep.password}".encode())
            req.add_header("Authorization", "Basic " + token.decode())
        ctx = None
        if base.startswith("https") and ep.options.get("verify") is False:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        try:
            with urllib.request.urlopen(req, timeout=60, context=ctx) as r:
                raw = r.read()
        except urllib.error.HTTPError as e:
            if missing_ok and e.code == 404:
                return None
            raise RuntimeError(f"{method} {path.split('?')[0]}: {e.code}"
                               f" {e.read()[:300].decode(errors='replace')}")
        return json.loads(raw) if raw else {}

    def _prefix(self, side, db):
        ep = self.hop.source if side == "src" else self.hop.target
        name = self.hop.target_db(db) if side == "dst" else db
        return str(ep.options.get("index_prefix") or "{db}.").format(
            db=name).lower()

    def _index(self, side, db, table):
        return self._prefix(side, db) + str(table).lower()

    def databases(self):
        return list(self.hop.databases or ["default"])

    # ---- what an index is -------------------------------------------------

    def neutral_tables(self, side, db):
        pre = self._prefix(side, db)
        got = self._call(side, "GET", "/_cat/indices?format=json&h=index")
        return sorted(i["index"][len(pre):] for i in got or []
                      if i["index"].startswith(pre)
                      and not self.hop.excluded(db, i["index"][len(pre):]))

    def _mapping(self, side, db, table):
        index = self._index(side, db, table)
        got = self._call(side, "GET", f"/{index}/_mapping")
        return got[index]["mappings"]

    def _described(self, side, db, table):
        meta = (self._mapping(side, db, table).get("_meta") or {}).get(
            "migkit")
        if meta:
            return [tuple(c) for c in meta["columns"]], list(meta["key"])
        return None

    def neutral_columns(self, side, db, table):
        described = self._described(side, db, table)
        if described is not None:
            return described[0]
        # an index migkit did not make: its mapping, by the class each
        # kind of field holds, and its ids as its key. An object, a nested
        # document or anything else stays unmapped.
        props = self._mapping(side, db, table).get("properties") or {}
        return [("_id", "text")] + sorted(
            (n, FIELDS.get(p.get("type", "object"), p.get("type", "object")))
            for n, p in props.items())

    def neutral_key(self, side, db, table):
        described = self._described(side, db, table)
        return described[1] if described is not None else ["_id"]

    # ---- values -----------------------------------------------------------

    @staticmethod
    def _to_source(declared, value):
        from decimal import Decimal

        from .. import canon
        if value is None:
            return None
        cls = canon.type_class("opensearch", declared)
        if cls == "decimal":
            # the text, scale and all: a JSON number keeps neither
            return format(value if isinstance(value, Decimal)
                          else Decimal(str(value)), "f")
        if cls == "integer":
            return int(value)
        if cls == "float":
            return float(value)
        if cls == "boolean":
            return bool(value)
        if cls == "bytes":
            return base64.b64encode(bytes(value)).decode()
        if cls in ("date", "timestamp", "time"):
            return canon.render_value(cls, value)
        return str(value)

    @staticmethod
    def _from_source(declared, value):
        import datetime
        from decimal import Decimal

        from .. import canon
        if value is None:
            return None
        cls = canon.type_class("opensearch", declared)
        if cls == "decimal":
            return Decimal(str(value))
        if cls == "integer":
            return int(value)
        if cls == "float":
            return float(value)
        if cls == "bytes":
            return base64.b64decode(value)
        if cls == "timestamp" and isinstance(value, str):
            return datetime.datetime.fromisoformat(value.replace("T", " "))
        if cls == "date" and isinstance(value, str):
            return datetime.date.fromisoformat(value[:10])
        if cls == "time" and isinstance(value, str):
            return datetime.time.fromisoformat(value)
        return value

    def _row(self, declared, names, hit):
        src = hit.get("_source") or {}
        return [hit["_id"] if n == "_id" else
                self._from_source(declared.get(n, ""), src.get(n))
                for n in names]

    def _doc_id(self, columns, key, row):
        from .. import rowtext
        return rowtext.encode(list(self._key_of(columns, key, row)))

    def _refresh(self, side, db, table):
        if side == "dst":
            self._call(side, "POST",
                       f"/{self._index(side, db, table)}/_refresh")

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        if where:
            raise SystemExit(f"{table} moves under a row filter, which is"
                             " SQL, and OpenSearch takes none")
        self._refresh(side, db, table)
        declared = dict(self.neutral_columns(side, db, table))
        names = [n for n, _ in columns]
        index = self._index(side, db, table)
        got = self._call(side, "POST", f"/{index}/_search?scroll=2m",
                         {"size": int(size), "sort": ["_doc"],
                          "_source": [n for n in names if n != "_id"]})
        scroll = got.get("_scroll_id")
        try:
            while True:
                hits = got["hits"]["hits"]
                if not hits:
                    return
                yield [self._row(declared, names, h) for h in hits]
                got = self._call(side, "POST", "/_search/scroll",
                                 {"scroll": "2m", "scroll_id": scroll})
                scroll = got.get("_scroll_id", scroll)
        finally:
            if scroll:
                self._call(side, "DELETE", "/_search/scroll",
                           {"scroll_id": scroll}, missing_ok=True)

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        """Documents come back in no order: the copier reads an index in
        one pass (`RESUMES_BY_KEY`). This serves the callers that ask
        whether an index holds anything."""
        rows = []
        for batch in self.neutral_batches(side, db, table, columns,
                                          limit or 1000, where):
            rows += batch
            if limit and len(rows) >= limit:
                return rows[:limit], None
        return rows, None

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        from .. import canon, rowtext
        if not key or not keys:
            return {}
        self._refresh(side, db, table)
        declared = dict(self.neutral_columns(side, db, table))
        cls = dict(columns)
        names = [n for n, _ in columns]
        ids = [kk[0] if key == ["_id"] else rowtext.encode(
            [canon.render_value(cls[k], v) for k, v in zip(key, kk)])
            for kk in keys]
        found = []
        index = self._index(side, db, table)
        for i in range(0, len(ids), 500):
            got = self._call(side, "POST", f"/{index}/_mget",
                             {"ids": ids[i:i + 500]})
            found += [self._row(declared, names, d)
                      for d in got.get("docs", []) if d.get("found")]
        return self._by_key_map(columns, key, found)

    def neutral_digest(self, side, db, table, columns, where=None):
        from .. import canon, rowtext
        classes = [c for _, c in columns]
        total, n = 0, 0
        for batch in self.neutral_batches(side, db, table, columns, 2000,
                                          where):
            for row in batch:
                total = canon.digest_step(total, rowtext.encode(
                    [canon.render_value(c, v) for c, v in zip(classes, row)]))
                n += 1
        return (n, str(total))

    def table_facts(self, side, db):
        pre = self._prefix(side, db)
        got = self._call(side, "GET", "/_cat/indices?format=json&bytes=b"
                                      "&h=index,docs.count,store.size")
        return {i["index"][len(pre):]: {
                    "rows": int(i.get("docs.count") or 0),
                    "bytes": int(i.get("store.size") or 0), "key": True}
                for i in got or [] if i["index"].startswith(pre)}

    # ---- writing ----------------------------------------------------------

    def neutral_create_sql(self, side, db, table, columns, key=()):
        if not key:
            raise SystemExit(f"{table} has no key, and a document's id is"
                             " its row's key: without one a row written"
                             " again would be a second document. Give the"
                             " table a key, or leave it out of this hop")
        props, cols = {}, []
        for col in columns:
            name, cls, numbers = col[0], col[1], tuple(col[2] or ())
            if cls == "decimal":
                scale = numbers[1] if len(numbers) == 2 else None
                props[name] = ({"type": "scaled_float",
                                "scaling_factor": 10 ** scale}
                               if scale is not None else {"type": "double"})
                declared = "decimal" + (f"({numbers[0]},{numbers[1]})"
                                        if scale is not None else "")
            elif cls in MAPPING:
                props[name] = MAPPING[cls]
                declared = cls
            else:
                raise SystemExit(f"a {cls} column ({name}) has no mapping"
                                 " migkit writes; leave it out with"
                                 " mapping.columns")
            cols.append([name, declared])
        return json.dumps({"mappings": {
            "_meta": {"migkit": {"columns": cols, "key": list(key)}},
            "dynamic": "strict", "properties": props}})

    def neutral_create(self, side, db, table, columns, key=()):
        self._target_only(side, "create an index")
        if str(table).lower() in self.neutral_tables(side, db):
            raise SystemExit(f"{table} already exists on the target -"
                             " migkit will not alter or replace an index that"
                             " is already there")
        body = self.neutral_create_sql(side, db, table, columns, key)
        self._call(side, "PUT", f"/{self._index(side, db, table)}",
                   json.loads(body))
        return body

    def neutral_write(self, side, db, table, columns, rows):
        """One document per row, its id the canonical text of the row's
        key, so a row written again replaces itself."""
        self._target_only(side, "write rows")
        if not rows:
            return 0
        declared = dict(self.neutral_columns(side, db, table))
        key = self.neutral_key(side, db, table)
        names = [n for n, _ in columns]
        index = self._index(side, db, table)
        lines = []
        for r in rows:
            doc = {n: self._to_source(declared.get(n, "text"), v)
                   for n, v in zip(names, r) if n != "_id"}
            lines.append(json.dumps({"index": {
                "_index": index, "_id": self._doc_id(columns, key, r)}}))
            lines.append(json.dumps(doc))
        got = self._call(side, "POST", "/_bulk", "\n".join(lines) + "\n",
                         ndjson=True)
        if got.get("errors"):
            bad = [i for i in got.get("items", [])
                   if i.get("index", {}).get("error")]
            first = bad[0]["index"]["error"] if bad else {}
            raise SystemExit(f"{index}: the target refused {len(bad)}"
                             f" documents: {first.get('type')}:"
                             f" {str(first.get('reason'))[:120]}")
        return len(rows)

    def neutral_empty(self, side, db, table, where=None):
        self._target_only(side, "empty an index")
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and OpenSearch"
                             " takes none")
        got = self._call(side, "POST", f"/{self._index(side, db, table)}"
                                       "/_delete_by_query?refresh=true",
                         {"query": {"match_all": {}}})
        return int(got.get("deleted", 0))

    # ---- the checks, through the pair every engine compares by --------

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)

    def check_deep(self, db):
        """The target's indexes' health: a red index is missing shards,
        and what a check reads from it is not all there is."""
        pre = self._prefix("dst", db)
        try:
            got = self._call("dst", "GET",
                             "/_cat/indices?format=json&h=index,health")
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("deep", f"{db} target index health", "error",
                           "could not read the target's indexes:"
                           f" {str(e).splitlines()[0][:90]}")]
        red = [i["index"] for i in got or []
               if i["index"].startswith(pre) and i.get("health") == "red"]
        if red:
            return [Result("deep", f"{db} target index health", "diff",
                           f"{len(red)} indexes missing shards:"
                           f" {', '.join(red[:6])}", "",
                           "bring the shards back before trusting a check")]
        return [Result("deep", f"{db} target index health", "ok",
                       "no index is missing a shard")]
