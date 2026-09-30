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
import posixpath
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

    def _base(self, side):
        ep = self.hop.source if side == "src" else self.hop.target
        return str(ep.options.get("url") or
                   f"{'https' if ep.options.get('tls') else 'http'}://"
                   f"{ep.host}:{ep.port or 9200}").rstrip("/")

    def _call(self, side, method, path, body=None, ndjson=False,
              missing_ok=False):
        ep = self.hop.source if side == "src" else self.hop.target
        base = self._base(side)
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

    #: documents and bytes one bulk request carries at most
    BULK_DOCS, BULK_BYTES = 1000, 32 * 2 ** 20
    #: times a document the cluster pushed back (429) is sent again, the
    #: wait doubling from one second up to a minute
    BULK_RETRIES = 8

    def _os(self, side):
        """The cluster's client, one a side, shared by the threads of a
        copy. OpenSearch's own: Elasticsearch's refuses any server that is
        not Elasticsearch."""
        clients = self.__dict__.setdefault("_clients", {})
        if side not in clients:
            from opensearchpy import OpenSearch
            ep = self.hop.source if side == "src" else self.hop.target
            checked = ep.options.get("verify") is not False
            clients[side] = OpenSearch(
                hosts=[self._base(side)],
                http_auth=(ep.user, ep.password) if ep.user else None,
                verify_certs=checked, ssl_assert_hostname=checked,
                ssl_show_warn=False, timeout=120,
                pool_maxsize=max(10, int(self.hop.workers or 1) * 2))
        return clients[side]

    def _bulk(self, side, actions):
        """(written, left because the target holds that version or a newer
        one) for a stream of bulk actions, sent in requests of
        `BULK_DOCS` documents or `BULK_BYTES`, one after another - a
        document the cluster pushes back for being busy is sent again
        after a wait (`BULK_RETRIES`), which parallel sending does not do.
        A delete of a document that is not there is what was wanted; any
        other refusal stops the copy, naming the first."""
        from opensearchpy.helpers import streaming_bulk
        written, kept, refused = 0, 0, []
        for ok, item in streaming_bulk(
                self._os(side), actions, chunk_size=self.BULK_DOCS,
                max_chunk_bytes=self.BULK_BYTES, raise_on_error=False,
                raise_on_exception=True, max_retries=self.BULK_RETRIES,
                initial_backoff=1, max_backoff=60, yield_ok=True):
            op, got = next(iter(item.items()))
            status = int(got.get("status") or 0)
            if ok:
                written += 1
            elif status == 409:
                kept += 1
            elif not (op == "delete" and status == 404):
                refused.append(got)
        if refused:
            first = refused[0].get("error") or {}
            raise SystemExit(f"{refused[0].get('_index')}: the target refused"
                             f" {len(refused)} documents:"
                             f" {first.get('type')}:"
                             f" {str(first.get('reason'))[:120]}")
        return written, kept

    @staticmethod
    def _versioned(hit, index):
        """A document read from the source, as the bulk action that writes
        it to `index` with the source's own place in its shard's history
        as its version: its primary term above its sequence number, so a
        later change always carries a larger one.

        `external`: the target keeps a write only when its version is
        larger than the one it holds, so a change carried twice, or an
        older one carried after a newer, is left (a conflict, counted, not
        an error). Not `external_gte`, which also takes an equal version -
        the documentation's own warning is that it can lose data - and not
        `if_seq_no`/`if_primary_term`, which hold a write to the target's
        own sequence numbers, which the copy neither knows nor shares with
        the source."""
        action = {"_op_type": "index", "_index": index, "_id": hit["_id"],
                  "_source": hit.get("_source") or {}}
        if hit.get("_routing") is not None:
            action["routing"] = hit["_routing"]
        term, seq = hit.get("_primary_term"), hit.get("_seq_no")
        if term is not None and seq is not None and int(term) < 2 ** 20:
            action["version"] = (int(term) << 43) | int(seq)
            action["version_type"] = "external"
        return action

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
        from .. import canon
        classes = [c for _, c in columns]
        total, n = 0, 0
        for batch in self.neutral_batches(side, db, table, columns, 2000,
                                          where):
            k, total = canon.fold_rows(classes, batch, total)
            n += k
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
        from .. import canon
        props, cols = {}, []
        for col in columns:
            name, cls, numbers = col[0], col[1], tuple(col[2] or ())
            if cls == canon.OWN:
                # OpenSearch to OpenSearch, row by row: a field as its own
                # type. Measured before: every such index was refused, on
                # its `_id`, the document's own id and no field at all
                own = str(numbers[0]) if numbers else ""
                if name == "_id":
                    cols.append([name, "text"])
                    continue
                if own in FIELDS:
                    props[name] = {"type": own}
                    cols.append([name, FIELDS[own]])
                    continue
                raise SystemExit(
                    f"{name} is a field of type {own or 'object'}, which is"
                    f" not copied row by row; the move's own path carries"
                    f" {table} whole")
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
        self._bulk(side, ({
            "_op_type": "index", "_index": index,
            "_id": self._doc_id(columns, key, r),
            "_source": {n: self._to_source(declared.get(n, "text"), v)
                        for n, v in zip(names, r) if n != "_id"}}
            for r in rows))
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

    # ---- settings -------------------------------------------------------

    #: settings that change what a search finds or whether a write lands:
    #: an analyzer is how text is split into what is searched, a field
    #: limit refuses a document with one more field
    CRITICAL_PARAMS = ("action.auto_create_index",
                       "index.mapping.total_fields.limit",
                       "index.mapping.depth.limit",
                       "index.mapping.nested_fields.limit",
                       "index.max_result_window",
                       "indices.query.bool.max_clause_count",
                       "search.max_buckets")

    #: per-index settings that are the index's own, not a choice to carry
    OWN_SETTINGS = ("index.uuid", "index.creation_date", "index.version",
                    "index.provided_name", "index.routing",
                    "index.resize", "index.history")

    def _settings(self, side, db):
        got = self._call(side, "GET", "/_cluster/settings?include_defaults"
                                      "=true&flat_settings=true")
        out = {}
        for part in ("defaults", "persistent", "transient"):
            out.update({str(k): str(v) for k, v in
                        (got.get(part) or {}).items()})
        for t in self.neutral_tables(side, db):
            index = self._index(side, db, t)
            got = self._call(side, "GET", f"/{index}/_settings"
                                          "?flat_settings=true")
            for k, v in ((got.get(index) or {}).get("settings") or {}
                         ).items():
                if not str(k).startswith(self.OWN_SETTINGS):
                    out[f"{t}:{k}"] = str(v)
        return {k: v for k, v in out.items()
                if not any(w in k.lower() for w in ("password", "secret",
                                                    "keystore"))}

    def check_params(self, db):
        """The cluster's settings and each index's own, both sides, through
        the report every engine's settings go through. An index's analysis
        (its analyzers, tokenizers, filters) counts as behaviour: the same
        text is found by other words under another."""
        def pull(side):
            try:
                return self._settings(side, db)
            except Exception as e:  # noqa: BLE001 - said by the report
                return {self.UNREADABLE: str(e).splitlines()[0][:80]}
        src, dst = pull("src"), pull("dst")
        crit = sorted({n for n in set(src) | set(dst)
                       if n.split(":")[-1] in self.CRITICAL_PARAMS
                       or ".analysis." in n})
        return self._param_result(
            db, src, dst, crit,
            "give the target's indexes the source's analysis and limits"
            " before cutover")

    # ---- snapshot ---------------------------------------------------------

    def snapshot_state(self, db, state_dir, kind="all"):
        """The target's indexes snapshotted by the cluster itself into a
        file repository under its own `path.repo`, where it has one; where
        it has none, each index's mapping, settings and count are kept,
        and why no snapshot was taken is said."""
        indexes = [self._index("dst", db, t)
                   for t in self.neutral_tables("dst", db)]
        out = {"indexes": {}}
        for index in indexes:
            got = self._call("dst", "GET", f"/{index}")
            count = self._call("dst", "GET", f"/{index}/_count")
            out["indexes"][index] = {"definition": got.get(index),
                                     "count": count.get("count")}
        nodes = self._call("dst", "GET", "/_nodes/settings")
        repos = sorted({r for n in (nodes.get("nodes") or {}).values()
                        for r in ((n.get("settings") or {}).get("path") or {})
                        .get("repo") or []})
        if not repos:
            out["no_snapshot"] = ("the target cluster has no path.repo, so"
                                  " there is no file repository to take one"
                                  " into")
        elif indexes:
            repo = f"migkit-{self.hop.name}".lower()
            self._call("dst", "PUT", f"/_snapshot/{repo}", {
                "type": "fs", "settings": {
                    "location": posixpath.join(repos[0], repo)}})
            name = f"migkit-{state_dir.name}".lower()
            got = self._call("dst", "PUT",
                             f"/_snapshot/{repo}/{name}"
                             "?wait_for_completion=true",
                             {"indices": ",".join(indexes),
                              "include_global_state": False})
            snap = got.get("snapshot") or {}
            out["snapshot"] = {"repository": repo, "name": name,
                               "state": snap.get("state"),
                               "shards": snap.get("shards")}
        (state_dir / "dst-indexes.json").write_text(
            json.dumps(out, indent=2, sort_keys=True, default=str))

    # ---- verifying only what changed ----------------------------------------

    def _shard_marks(self, side, db, table):
        """{primary shard: (highest sequence number, deleted docs)}: every
        write to a shard takes the next sequence number."""
        index = self._index(side, db, table)
        got = self._call(side, "GET", f"/{index}/_stats/docs"
                                      "?level=shards")
        out = {}
        for n, copies in ((got.get("indices") or {}).get(index, {})
                          .get("shards") or {}).items():
            for c in copies:
                if (c.get("routing") or {}).get("primary"):
                    out[str(n)] = [int((c.get("seq_no") or {})
                                       .get("max_seq_no", -1)),
                                   int((c.get("docs") or {})
                                       .get("deleted", 0))]
        return out

    def _written_since(self, db, table, before, now):
        """The ids of the source's documents written after `before` in each
        shard, asked of that shard by sequence number."""
        index = self._index("src", db, table)
        ids = set()
        for shard, (top, _) in now.items():
            was = int((before.get(shard) or [-1])[0])
            if top <= was:
                continue
            for hits in self._scroll(
                    "src", index, {"size": 1000, "_source": False,
                                   "sort": ["_doc"],
                                   "query": {"range": {"_seq_no": {
                                       "gt": was}}}},
                    f"preference=_shards:{shard}"):
                ids.update(h["_id"] for h in hits)
        return ids

    def _sources(self, side, db, table, ids):
        """{id: the document as sorted JSON} of these ids that are there -
        between two OpenSearch clusters a document is compared as it is
        held, not through a rendering."""
        index, out, ids = self._index(side, db, table), {}, sorted(ids)
        for i in range(0, len(ids), 500):
            got = self._call(side, "POST", f"/{index}/_mget",
                             {"ids": ids[i:i + 500]})
            out.update({d["_id"]: json.dumps(d.get("_source"),
                                             sort_keys=True)
                        for d in got.get("docs", []) if d.get("found")})
        return out

    def _ids(self, side, db, table):
        ids = set()
        for hits in self._scroll(side, self._index(side, db, table),
                                 {"size": 5000, "_source": False,
                                  "sort": ["_doc"]}):
            ids.update(h["_id"] for h in hits)
        return ids

    def delta_verify(self, db, limit=20000, log=None):
        """Only the documents written since the last clean run: each
        primary shard's highest sequence number is kept, and the documents
        above it are asked of that shard. A delete leaves nothing to find
        by number, so an index whose deleted count moved has its ids
        compared whole. The written documents are compared on both sides
        as the check compares them; the marks move only on a clean run."""
        state = self.hop.report_dir(db) / "delta-seqno.json"
        prev = json.loads(state.read_text()) if state.exists() else None
        now = {t: self._shard_marks("src", db, t)
               for t in self.neutral_tables("src", db)}
        if prev is None:
            state.write_text(json.dumps(now))
            return [Result("delta", db, "ok", "baseline recorded, writes"
                           " are tracked from this point on")]
        res, clean, total = [], True, 0
        for t, marks in sorted(now.items()):
            was = prev.get(t, {})
            written = self._written_since(db, t, was, marks)
            deleted = any(marks[s][1] != (was.get(s) or [0, 0])[1]
                          for s in marks)
            gone = set()
            if deleted:
                self._refresh("dst", db, t)
                gone = self._ids("dst", db, t) - self._ids("src", db, t)
            if not written and not gone:
                continue
            total += len(written) + len(gone)
            self._refresh("dst", db, t)
            a = self._sources("src", db, t, written)
            b = self._sources("dst", db, t, written)
            bad = sorted(gone | {i for i in written if a.get(i) != b.get(i)})
            clean = clean and not bad
            res.append(Result(
                "delta", f"{db}.{t}", "diff" if bad else "ok",
                f"{len(written)} written, {len(gone)} deleted on the source"
                f" and still on the target" + (f": {len(bad)} differ" if bad
                                               else ", all the same")))
            if log:
                log(f"{t}: {'DIFF' if bad else 'ok'}")
        if clean:
            state.write_text(json.dumps(now))
        res.insert(0, Result("delta", db, "ok" if clean else "diff",
                             f"{total} documents compared, marks"
                             f" {'advanced' if clean else 'NOT advanced'}"))
        return res

    # ---- documents copied as they are -------------------------------------

    def _scroll(self, side, index, body, where=""):
        """Pages of hits of one search, read through to the end with a
        scroll that is let go of however the reading stops. `where` is
        more of the request's query string (`preference=_shards:0`)."""
        got = self._call(side, "POST", f"/{index}/_search?scroll=2m"
                                       + (f"&{where}" if where else ""), body)
        scroll = got.get("_scroll_id")
        try:
            while got["hits"]["hits"]:
                yield got["hits"]["hits"]
                got = self._call(side, "POST", "/_search/scroll",
                                 {"scroll": "2m", "scroll_id": scroll})
                scroll = got.get("_scroll_id", scroll)
        finally:
            if scroll:
                self._call(side, "DELETE", "/_search/scroll",
                           {"scroll_id": scroll}, missing_ok=True)

    def _table_ways(self):
        """How an index goes (`tableway.each`): out of the source's own
        snapshot where the hop names one and the reader can carry it
        (`fromsnapshot`), by migkit's sliced scroll always."""
        from .. import fromsnapshot
        from ..decide import Need, Rung
        return (
            Rung("snapshot", "the source's own snapshot, the cluster itself"
                             " left unread",
                 gives=frozenset({"resume by chunk"}),
                 needs=(Need("the snapshot reader built here",
                             lambda f: f["reader"],
                             "the snapshot reader is not built here"),
                        Need("an index it can carry",
                             lambda f: f["unfit"] is None,
                             lambda f: f["unfit"])),
                 prove=fromsnapshot.prove,
                 because="the snapshot's shards read on this machine, no"
                         " search on the source"),
            Rung("scroll", "a sliced scroll of the source",
                 because="each document written with the source's own"
                         " version of it"),
        )

    def native_bulk(self, db, tables, go, log, shape_only=()):
        """Each index's documents copied as the source holds them - the
        `_source`, the `_id` and the routing unchanged - by the way climbed
        for it (`_table_ways`): out of a snapshot of the source the hop
        names, or by a sliced scroll, a slice per worker, each written
        back through `_bulk` with the source's own version of it
        (`_versioned`). An index the target lacks is made with the
        source's mappings and analysis; while it loads it keeps no replica
        and does not refresh, and gets both back after (`_loading`).

        What the target already holds is not emptied first: a document it
        holds at the source's version or a newer one is left, an older one
        replaced, and - where the index held documents before the copy -
        one the source does not hold is taken away after. Emptied, each
        deleted document's version stays behind for a minute
        (`index.gc_deletes`), and the copy's own write of it, at the same
        version, would be refused and the document lost."""
        steps = [f"{t}: documents copied as they are, by a sliced scroll"
                 for t in tables]
        if not go:
            return steps
        from .. import fromsnapshot, tableway
        from ..decide import Facts
        slices = max(1, int(getattr(self.hop, "workers_most", 0)
                            or self.hop.workers or 1))
        for t in list(shape_only) + list(tables):
            if t.lower() not in self.neutral_tables("dst", db):
                self._made_like(db, t)

        def run(way, t):
            dst = self._index("dst", db, t)
            self._refresh("dst", db, t)
            held = int(self._call("dst", "GET", f"/{dst}/_count")
                       .get("count", 0))
            with self._loading(dst):
                if way == "snapshot":
                    moved = fromsnapshot.copy_index(self, db, t, slices, log)
                    how = "out of the source's snapshot"
                else:
                    moved = self._copy_by_scroll(db, t, slices)
                    how = f"in {slices} slices"
            gone, refused = self._strays(db, t) if held else (0, 0)
            if refused:
                raise SystemExit(
                    f"{t}: {refused:,} documents deleted on the target in"
                    " the last minute refuse the source's version of them"
                    " for a minute more - move again after it")
            if log:
                log(f"{t}: {moved:,} documents copied {how}"
                    + (f", {gone:,} the source does not hold taken from"
                       " the target" if gone else ""))
            return moved
        tableway.each(self, db, tables, self._table_ways(),
                      lambda t: Facts(
                          reader=lambda: fromsnapshot.program() is not None,
                          unfit=lambda: fromsnapshot.unfit(self, db, t)),
                      run, log)
        return steps

    def _loading(self, index):
        """While `index` loads on the target: no replica and no refresh,
        each put back as it was after, however the load ends."""
        import contextlib

        @contextlib.contextmanager
        def window():
            kept = self._call("dst", "GET", f"/{index}/_settings")
            index_settings = ((kept.get(index) or {}).get("settings")
                              or {}).get("index", {})
            restore = {"number_of_replicas": index_settings.get(
                           "number_of_replicas", "1"),
                       "refresh_interval": index_settings.get(
                           "refresh_interval", "1s")}
            self._call("dst", "PUT", f"/{index}/_settings",
                       {"index": {"number_of_replicas": 0,
                                  "refresh_interval": "-1"}})
            try:
                yield
            finally:
                self._call("dst", "PUT", f"/{index}/_settings",
                           {"index": restore})
                self._call("dst", "POST", f"/{index}/_refresh")
        return window()

    def _copy_by_scroll(self, db, t, slices):
        """One index by a sliced scroll of the source, each document
        written with the source's own version of it; the documents
        copied."""
        from concurrent.futures import ThreadPoolExecutor
        src, dst = self._index("src", db, t), self._index("dst", db, t)

        def one(i):
            body = {"size": 1000, "sort": ["_doc"],
                    "seq_no_primary_term": True}
            if slices > 1:
                body["slice"] = {"id": i, "max": slices}
            moved = 0
            for hits in self._scroll("src", src, body):
                self._bulk("dst", (self._versioned(h, dst) for h in hits))
                moved += len(hits)
            return moved
        with ThreadPoolExecutor(max_workers=slices) as pool:
            return sum(pool.map(one, range(slices)))

    def _strays(self, db, table):
        """(deleted, refused): the target's documents of one index the
        source does not hold, deleted; the source's the target lacks,
        written with their versions - and how many of those the target
        still refused. A document deleted on the target keeps its delete's
        version for a minute (`index.gc_deletes`), and until then refuses
        the source's version of it. Both are found by a search of their ids
        rather than asked by id: a document written with a routing of its
        own lives in the shard its routing names, where a lookup by id
        alone does not look."""
        self._refresh("dst", db, table)
        there = self._ids("dst", db, table)
        held = self._ids("src", db, table)
        gone = sorted(there - held)
        dst = self._index("dst", db, table)
        for at in range(0, len(gone), 500):
            self._call("dst", "POST", f"/{dst}/_delete_by_query"
                                      "?refresh=true&conflicts=proceed",
                       {"query": {"ids": {"values": gone[at:at + 500]}}})
        lacking = sorted(held - there)
        src = self._index("src", db, table)
        refused = 0
        for at in range(0, len(lacking), 500):
            got = self._call("src", "POST", f"/{src}/_search",
                             {"size": 500, "seq_no_primary_term": True,
                              "query": {"ids": {
                                  "values": lacking[at:at + 500]}}})
            refused += self._bulk("dst", (
                self._versioned(h, dst)
                for h in (got.get("hits") or {}).get("hits", [])))[1]
        return len(gone), refused

    def _made_like(self, db, table):
        """The target index made with the source's mappings and analysis,
        its own identity left out."""
        src = self._index("src", db, table)
        got = (self._call("src", "GET", f"/{src}").get(src) or {})
        settings = ((got.get("settings") or {}).get("index") or {})
        keep = {k: v for k, v in settings.items()
                if k in ("number_of_shards", "number_of_replicas",
                         "analysis", "mapping", "max_result_window",
                         "refresh_interval", "codec", "sort")}
        self._call("dst", "PUT", f"/{self._index('dst', db, table)}",
                   {"settings": {"index": keep},
                    "mappings": got.get("mappings") or {}})

    # ---- following the source by its shards' sequence numbers --------------

    #: seconds between two looks at the source once caught up
    FOLLOW_EVERY = 1
    CHANGE_POINT_READS_ONLY = True

    def _copies(self, side, index):
        """(the index's id, {primary shard: [global checkpoint, highest
        sequence number]}, {copy of a shard: times it has made writes
        visible to a search}). Every write to a shard takes its next
        sequence number, a delete too; up to the global checkpoint every
        copy of the shard holds it."""
        got = self._call(side, "GET", f"/{index}/_stats/refresh"
                                      "?level=shards")
        mine = (got.get("indices") or {}).get(index) or {}
        marks, seen = {}, {}
        for n, copies in (mine.get("shards") or {}).items():
            for c in copies:
                routing = c.get("routing") or {}
                refresh = c.get("refresh") or {}
                seen[f"{n}/{routing.get('node')}/{routing.get('primary')}"] \
                    = int(refresh.get("external_total",
                                      refresh.get("total", 0)))
                if routing.get("primary"):
                    seq = c.get("seq_no") or {}
                    marks[str(n)] = [int(seq.get("global_checkpoint", -1)),
                                     int(seq.get("max_seq_no", -1))]
        return str(mine.get("uuid") or ""), marks, seen

    def _refresh_every(self, side, index):
        """Seconds between the index's refreshes, None where it refreshes
        only when asked (`-1`)."""
        got = self._call(side, "GET", f"/{index}/_settings?include_defaults"
                                      "=true&flat_settings=true")
        mine = got.get(index) or {}
        text = str((mine.get("settings") or {}).get("index.refresh_interval")
                   or (mine.get("defaults") or {}).get(
                       "index.refresh_interval") or "1s")
        if text.startswith("-"):
            return None
        for unit, scale in (("ms", 0.001), ("s", 1), ("m", 60), ("h", 3600)):
            if text.endswith(unit) and text[:-len(unit)].replace(
                    ".", "", 1).isdigit():
                return float(text[:-len(unit)]) * scale
        return 1.0

    def _visible(self, side, waits):
        """Wait until every write up to the checkpoints read with each
        index's `before` is within a search's reach - {index: (before,
        seconds between its refreshes)}, all waited on together. A copy of
        a shard that has made writes visible twice since - the second time
        begun after the first ended, so after `before` was read - has them.
        One that has not by twice its interval either has nothing left to
        make visible (a copy refreshes on its interval whenever it has), or
        has gone unsearched a while and makes writes visible only when
        searched - and a search waits for it to (measured on 2.19: after
        35 s without a search, a shard had not made a new write visible 3 s
        later; a count on it made it visible at once). So the index is
        searched then, and its wait is over."""
        import time
        began, left = time.monotonic(), dict(waits)
        while left:
            for index, (before, every) in list(left.items()):
                _, _, now = self._copies(side, index)
                if all(now.get(c, 0) >= n + 2 for c, n in before.items()):
                    del left[index]
                elif time.monotonic() - began > 2 * every:
                    self._call(side, "POST", f"/{index}/_count",
                               {"query": {"match_all": {}}})
                    del left[index]
            if left:
                time.sleep(0.2)

    @staticmethod
    def _never_refreshes(index):
        return (f"{index} on the source refreshes only when asked, so what"
                " it takes is seen by no search until then - a copy and a"
                " follow cannot see it either. Give it a refresh interval,"
                " or stop writes and move it")

    def stream_identity(self, side, db):
        return {"cluster": str(self._call(side, "GET", "/")
                               .get("cluster_uuid") or "")}

    def change_point(self, side, db):
        """Now, in each index's shards: the global checkpoint of each,
        taken once every copy has made the writes up to it visible - so a
        copy read after it holds everything up to it, and a follow begun
        from it misses nothing between. It reads only, and waits."""
        out, waits = {}, {}
        for t in self.neutral_tables(side, db):
            index = self._index(side, db, t)
            uuid, marks, seen = self._copies(side, index)
            every = self._refresh_every(side, index)
            if every is None:
                raise SystemExit(self._never_refreshes(index))
            waits[index] = (seen, every)
            out[t] = {"uuid": uuid,
                      "shards": {s: m[0] for s, m in marks.items()}}
        self._visible(side, waits)
        return {"indexes": out}

    def log_position(self, side, db):
        """Each index's shards' highest sequence numbers now: every write
        acknowledged so far is at or under them."""
        out = {}
        for t in self.neutral_tables(side, db):
            uuid, marks, _ = self._copies(side, self._index(side, db, t))
            out[t] = {"uuid": uuid,
                      "shards": {s: m[1] for s, m in marks.items()}}
        return {"indexes": out}

    @staticmethod
    def position_reached(have, want):
        """Whether the follow at `have` has carried, and checked for
        deletes, every write each shard of `want` holds."""
        if not have or not want:
            return None
        for t, w in (want.get("indexes") or {}).items():
            h = (have.get("indexes") or {}).get(t)
            if not h or h.get("uuid") != w.get("uuid"):
                return False
            if any(int((h.get("shards") or {}).get(s, -1)) < int(n)
                   for s, n in (w.get("shards") or {}).items()):
                return False
        return True

    def tail_start(self, db, token_path):
        """The position the follow begins at, taken before the copy it
        follows (`change_point`)."""
        from .. import tailctl
        if token_path.exists():
            return False
        tailctl.begin(self, db, token_path, self.change_point("src", db))
        return True

    def tail_seed(self, db, token_path, point):
        from .. import tailctl
        tailctl.begin(self, db, token_path, point)

    def _follow_index(self, db, table, was, log):
        """(the index's position after one look, documents carried, whether
        it settled) - every write each shard took since the position, read
        as the documents it left and written with their versions; then,
        since a delete leaves nothing to find by its number, the two sides
        counted, and where they differ, their ids compared and the
        target's extra taken away (`_strays`).

        The counts are of both sides as they stand: a delete in this look
        and a document written just after the checkpoint was read can
        cancel out in them for one look, and the next look, whose counts
        differ, takes the document away. Once writes stop, the counts are
        exact - which is when a fence decides a cutover."""
        src, dst = self._index("src", db, table), self._index("dst", db,
                                                              table)
        uuid, marks, seen = self._copies("src", src)
        if was and was.get("uuid") != uuid:
            raise SystemExit(
                f"{src} on the source was made again since the saved"
                " position (another index id): what the position counted is"
                " not in it. Move it again with --mode full+cdc")
        done = {s: int((was or {}).get("shards", {}).get(s, -1))
                for s in marks}
        upto = {s: m[0] for s, m in marks.items()}
        if was and all(upto[s] <= done[s] for s in upto):
            return was, 0, True
        every = self._refresh_every("src", src)
        if every is None:
            raise SystemExit(self._never_refreshes(src))
        self._visible("src", {src: (seen, every)})
        if table.lower() not in self.neutral_tables("dst", db):
            self._made_like(db, table)
        wrote = 0
        for s, top in sorted(upto.items()):
            if top <= done[s]:
                continue
            for hits in self._scroll(
                    "src", src, {"size": 1000, "sort": ["_doc"],
                                 "seq_no_primary_term": True,
                                 "query": {"range": {"_seq_no": {
                                     "gt": done[s], "lte": top}}}},
                    f"preference=_shards:{s}"):
                wrote += self._bulk("dst", (self._versioned(h, dst)
                                            for h in hits))[0]
        self._refresh("dst", db, table)
        count = [int(self._call(side, "GET", f"/{index}/_count")
                     .get("count", 0)) for side, index in (("src", src),
                                                           ("dst", dst))]
        gone, refused = (self._strays(db, table) if count[0] != count[1]
                         else (0, 0))
        if refused:
            # looked at again from the same position, once the target
            # takes the source's version of them
            return was, wrote + gone, False
        return {"uuid": uuid, "shards": upto}, wrote + gone, True

    def tail_apply(self, db, go, token_path, log):
        """Keep the target following the source by each primary shard's
        sequence numbers: each look, the documents written since the saved
        position are read from the shard that took them and written with
        the source's version (`_versioned`), so a look repeated, or a
        change carried after a newer one, is left by the target; deletes
        are found by the counts (`_follow_index`). The position is saved
        after each look; the fence waits on it (`position_reached`)."""
        from .. import tailctl

        def look(token):
            indexes = dict((token or {}).get("indexes") or {})
            went, settled = 0, True
            for t in self.neutral_tables("src", db):
                indexes[t], n, ok = self._follow_index(db, t,
                                                       indexes.get(t), log)
                went += n
                settled = settled and ok
            return {"indexes": indexes}, went, settled

        def behind(token):
            now = self.log_position("src", db)["indexes"]
            have = (token or {}).get("indexes") or {}
            ahead = {t: sum(max(0, int(n) - int((have.get(t) or {}).get(
                "shards", {}).get(s, -1))) for s, n in w["shards"].items())
                for t, w in now.items()}
            return (f"{sum(ahead.values()):,} writes on the source past the"
                    " saved position" + "".join(
                        f"; {t} {n:,}" for t, n in sorted(ahead.items())
                        if n))
        tailctl.follow(self, db, go, token_path, log, "documents", look,
                       behind, self.FOLLOW_EVERY)

    # the fence and the confirm are the pair's, through the position above

    def src_lsn(self, db):
        return self._as_pair().src_lsn(db)

    def fence_wait(self, db, lsn, timeout=300):
        return self._as_pair().fence_wait(db, lsn, timeout)

    def _compare_pks(self, db, table, keys):
        return self._as_pair()._compare_pks(db, table, keys)

    def _write_pk_files(self, db, table, missing, extra, changed):
        return self._as_pair()._write_pk_files(db, table, missing, extra,
                                                changed)
