"""An OpenSearch cluster in memory, for tests that run without a server.

It answers the requests `OpenSearchEngine` makes through `_call`, and the
bulk requests its `_bulk` sends through the client library's
`streaming_bulk`, from a dictionary of indexes - mappings, settings,
aliases and documents with the version, sequence number and primary term a
real shard gives them. It is a model of what the engine asks, not of the
server: a request it does not know raises, so a test never passes over a
call nobody wrote an answer for.

    cluster = Cluster()
    cluster.index("shop.orders", {"id": {"type": "long"}}, docs)
    with installed(src=cluster, dst=other):
        engine.check_deep("shop")

`installed` stands the cluster in for the wire (`OpenSearchEngine._call`)
and for the client library, which is not installed where these tests run.
"""
import contextlib
import json
import sys
import types
import zlib
from urllib.parse import parse_qs, urlsplit

PRIMARY_TERM = 1


class Cluster:
    def __init__(self, distribution="opensearch", number="2.19.1",
                 uuid="cluster-1"):
        self.distribution, self.number, self.uuid = distribution, number, uuid
        self.indexes = {}
        self.cluster_settings = {}
        self.refreshes = {}
        self.scrolls = {}
        self.calls = []
        #: {path-prefix: answer} for requests a test wants answered by
        #: hand, the longest prefix first; None answers 404. A bare
        #: cluster has no templates, no pipelines, no policies and runs
        #: no security plugin
        self.answers = {"/_template": {},
                        "/_index_template": {"index_templates": []},
                        "/_ingest/pipeline": None, "/_ilm": None,
                        "/_plugins/_ism": None, "/_ccr": None,
                        "/_plugins/_replication": None,
                        "/_tasks": {"nodes": {}},
                        "/_data_stream": {"data_streams": []},
                        "/_plugins/_security/api": None}
        #: the requests made, as (method, path) - for tests of what was
        #: and was not asked
        self.log = []
        self._n = 0

    # ---- building a cluster ----

    def index(self, name, properties=None, docs=(), shards=1, replicas=0,
              settings=None, mappings=None, aliases=None, meta=None):
        """An index of `docs` ({id: source} or [(id, source)]), written the
        way a bulk write is: each takes the next sequence number of its
        shard."""
        maps = dict(mappings or {})
        if properties is not None:
            maps["properties"] = properties
        if meta is not None:
            maps["_meta"] = meta
        body = {"number_of_shards": str(shards),
                "number_of_replicas": str(replicas),
                "uuid": f"uuid-{name}-{self._counter()}"}
        body.update(settings or {})
        idx = {"settings": body, "mappings": maps,
               "aliases": dict(aliases or {}), "docs": {},
               "seq": [-1] * shards, "deleted": [0] * shards}
        self.indexes[name] = idx
        for doc_id, source in (docs.items() if isinstance(docs, dict)
                               else docs):
            self.write(name, str(doc_id), source)
        return idx

    def _counter(self):
        self._n += 1
        return self._n

    def shard_of(self, name, doc_id, routing=None):
        shards = len(self.indexes[name]["seq"])
        return zlib.crc32(str(routing or doc_id).encode()) % shards

    def write(self, name, doc_id, source, version=None, routing=None):
        """(status, version) of one write. With `version` it is an external
        one: kept only where larger than the document's own."""
        idx = self.indexes[name]
        doc = idx["docs"].get(doc_id)
        if version is not None:
            if doc is not None and doc["_version"] >= version:
                return 409, doc["_version"]
            new = int(version)
        else:
            new = (doc["_version"] + 1) if doc else 1
        shard = self.shard_of(name, doc_id, routing)
        idx["seq"][shard] += 1
        idx["docs"][doc_id] = {
            "_source": json.loads(json.dumps(source)), "_version": new,
            "_seq_no": idx["seq"][shard], "_primary_term": PRIMARY_TERM,
            "_shard": shard, "_routing": routing}
        return (200 if doc else 201), new

    def delete(self, name, doc_id):
        idx = self.indexes[name]
        doc = idx["docs"].pop(doc_id, None)
        if doc is None:
            return 404
        idx["seq"][doc["_shard"]] += 1
        idx["deleted"][doc["_shard"]] += 1
        return 200

    def sources(self, name):
        return {i: d["_source"] for i, d in self.indexes[name]["docs"].items()}

    # ---- the wire ----

    def handle(self, method, path, body=None, ndjson=False, missing_ok=False):
        """What `OpenSearchEngine._call` returns for this request."""
        self.log.append((method, urlsplit(path).path))
        url = urlsplit(path)
        query = {k: v[-1] for k, v in parse_qs(url.query).items()}
        parts = [p for p in url.path.split("/") if p]
        try:
            for prefix, answer in sorted(self.answers.items(),
                                         key=lambda kv: -len(kv[0])):
                if url.path.startswith(prefix) and (
                        not isinstance(answer, tuple) or answer[0] == method):
                    got = answer[1] if isinstance(answer, tuple) else answer
                    if isinstance(got, Exception):
                        raise got
                    if got is None:
                        raise _Missing(url.path)
                    return got(method, url.path, body, query) \
                        if callable(got) else got
            return self._route(method, parts, query, body)
        except _Missing as e:
            if missing_ok:
                return None
            raise RuntimeError(f"{method} {url.path}: 404 {e}")

    def _index_of(self, name):
        if name not in self.indexes:
            raise _Missing(f"no such index [{name}]")
        return self.indexes[name]

    def _route(self, method, parts, q, body):
        body = body if isinstance(body, dict) else (body or {})
        if not parts:
            version = {"number": self.number}
            if self.distribution == "opensearch":
                version["distribution"] = "opensearch"
            else:
                version["build_flavor"] = "default"
            out = {"cluster_uuid": self.uuid, "version": version}
            if self.distribution != "opensearch":
                out["tagline"] = "You Know, for Search"
            return out
        head = parts[0]
        if head == "_cat" and parts[1] == "indices":
            return self._cat_indices(q)
        if head == "_cluster":
            if parts[1] == "settings":
                return {"defaults": {}, "persistent": self.cluster_settings,
                        "transient": {}}
            if parts[1] == "health":
                return self._health(parts[2] if len(parts) > 2 else None, q)
        if head == "_search" and parts[1] == "scroll":
            return self._scroll(method, body)
        if head == "_nodes":
            raise _Missing("nodes")
        if head.startswith("_"):
            raise RuntimeError(f"fake cluster: no answer for"
                               f" {method} /{'/'.join(parts)}")
        name, rest = head, parts[1:]
        if not rest:
            if method == "PUT":
                return self._create(name, body)
            if method == "GET":
                idx = self._index_of(name)
                return {name: {"aliases": idx["aliases"],
                               "mappings": idx["mappings"],
                               "settings": {"index": self._nested(idx)}}}
            if method == "DELETE":
                self._index_of(name)
                del self.indexes[name]
                return {"acknowledged": True}
        what = rest[0]
        idx = self._index_of(name)
        if what == "_mapping":
            return {name: {"mappings": idx["mappings"]}}
        if what == "_settings":
            if method == "PUT":
                for k, v in ((body.get("index") or body).items()):
                    if v is None:
                        idx["settings"].pop(k, None)
                    else:
                        idx["settings"][k] = str(v)
                return {"acknowledged": True}
            flat = q.get("flat_settings") == "true"
            index = {f"index.{k}": v for k, v in idx["settings"].items()} \
                if flat else {"index": self._nested(idx)}
            out = {name: {"settings": index}}
            if q.get("include_defaults") == "true":
                out[name]["defaults"] = ({"index.refresh_interval": "1s"}
                                         if flat else {})
            return out
        if what == "_refresh":
            self.refreshes[name] = self.refreshes.get(name, 0) + 1
            return {"_shards": {"failed": 0}}
        if what == "_count":
            hits = self._matching(name, (body or {}).get("query"))
            return {"count": len(hits)}
        if what == "_search":
            return self._search(name, q, body)
        if what == "_mget":
            return self._mget(name, body)
        if what == "_delete_by_query":
            hits = self._matching(name, body.get("query"))
            for i, _ in hits:
                self.delete(name, i)
            return {"deleted": len(hits)}
        if what == "_stats":
            return self._stats(name, rest[1] if len(rest) > 1 else "docs")
        raise RuntimeError(f"fake cluster: no answer for {method}"
                           f" /{'/'.join(parts)}")

    @staticmethod
    def _nested(idx):
        out = {}
        for k, v in idx["settings"].items():
            node = out
            keys = k.split(".")
            for part in keys[:-1]:
                node = node.setdefault(part, {})
            node[keys[-1]] = v
        return out

    def _create(self, name, body):
        if name in self.indexes:
            raise RuntimeError(f"PUT /{name}: 400 resource_already_exists")
        settings = ((body.get("settings") or {}).get("index")
                    or body.get("settings") or {})
        flat = {}

        def walk(prefix, node):
            for k, v in node.items():
                if isinstance(v, dict):
                    walk(f"{prefix}{k}.", v)
                else:
                    flat[f"{prefix}{k}"] = v
        walk("", settings)
        self.index(name, mappings=body.get("mappings") or {},
                   shards=int(flat.pop("number_of_shards", 1)),
                   replicas=int(flat.pop("number_of_replicas", 1)),
                   settings=flat, aliases=body.get("aliases"))
        return {"acknowledged": True, "index": name}

    def _cat_indices(self, q):
        rows = []
        for name, idx in sorted(self.indexes.items()):
            size = sum(len(json.dumps(d["_source"]))
                       for d in idx["docs"].values())
            rows.append({"index": name, "health": idx.get("health", "green"),
                         "status": idx.get("status", "open"),
                         "docs.count": str(len(idx["docs"])),
                         "store.size": str(size),
                         "pri.store.size": str(idx.get("bytes", size)),
                         "pri": idx["settings"]["number_of_shards"],
                         "rep": idx["settings"]["number_of_replicas"]})
        return rows

    def _health(self, names, q):
        out = {"status": "green", "relocating_shards": 0,
               "initializing_shards": 0, "unassigned_shards": 0,
               "number_of_data_nodes": 1,
               "active_shards": sum(int(i["settings"]["number_of_shards"])
                                    for i in self.indexes.values())}
        if q.get("level") == "indices":
            wanted = names.split(",") if names else sorted(self.indexes)
            out["indices"] = {}
            for n in wanted:
                idx = self._index_of(n)
                out["indices"][n] = {
                    "status": idx.get("health", "green"),
                    "number_of_shards": int(idx["settings"][
                        "number_of_shards"]),
                    "relocating_shards": idx.get("relocating", 0),
                    "initializing_shards": idx.get("initializing", 0),
                    "unassigned_shards": idx.get("unassigned", 0)}
        return out

    # ---- queries ----

    def _matching(self, name, query):
        """[(id, doc)] a query selects: match_all, ids, a range over the
        sequence number, exists, term."""
        idx = self._index_of(name)
        query = query or {"match_all": {}}
        (kind, spec), = query.items()
        out = []
        for i, d in idx["docs"].items():
            if kind == "match_all":
                ok = True
            elif kind == "ids":
                ok = i in spec["values"]
            elif kind == "range":
                (field, cond), = spec.items()
                value = d["_seq_no"] if field == "_seq_no" else None
                ok = value is not None
                for op, bound in cond.items():
                    ok = ok and {"gt": lambda: value > bound,
                                 "gte": lambda: value >= bound,
                                 "lt": lambda: value < bound,
                                 "lte": lambda: value <= bound}[op]()
            elif kind == "exists":
                ok = d["_source"].get(spec["field"]) is not None
            elif kind == "term":
                (field, want), = spec.items()
                ok = d["_source"].get(field) == want
            else:
                raise RuntimeError(f"fake cluster: query {kind}")
            if ok:
                out.append((i, d))
        return out

    def _hit(self, name, doc_id, d, body):
        hit = {"_index": name, "_id": doc_id, "_score": None}
        src = body.get("_source", True)
        if src is not False:
            source = d["_source"]
            if isinstance(src, list):
                source = {k: v for k, v in source.items() if k in src}
            hit["_source"] = source
        if body.get("seq_no_primary_term"):
            hit["_seq_no"] = d["_seq_no"]
            hit["_primary_term"] = d["_primary_term"]
        if body.get("version"):
            hit["_version"] = d["_version"]
        if body.get("docvalue_fields"):
            hit["fields"] = {}
            for spec in body["docvalue_fields"]:
                name = spec["field"] if isinstance(spec, dict) else spec
                value = d["_source"]
                for part in name.split("."):
                    value = value.get(part) if isinstance(value, dict) \
                        else None
                if value is not None:
                    if isinstance(spec, dict) and spec.get(
                            "format") == "epoch_millis":
                        value = _millis(value)
                    hit["fields"][name] = [value]
        if d["_routing"] is not None:
            hit["_routing"] = d["_routing"]
        return hit

    def _search(self, name, q, body):
        hits = self._matching(name, body.get("query"))
        shard = q.get("preference", "")
        if shard.startswith("_shards:"):
            want = int(shard.split(":")[1])
            hits = [(i, d) for i, d in hits if d["_shard"] == want]
        sl = body.get("slice")
        if sl:
            hits = [(i, d) for i, d in hits
                    if zlib.crc32(i.encode()) % int(sl["max"])
                    == int(sl["id"])]
        hits.sort(key=lambda h: (h[1]["_shard"], h[1]["_seq_no"]))
        size = int(body.get("size", 10))
        if "scroll" in q:
            key = f"scroll-{self._counter()}"
            self.scrolls[key] = (name, body, hits[size:])
            return {"_scroll_id": key, "hits": {"hits": [
                self._hit(name, i, d, body) for i, d in hits[:size]]}}
        return {"hits": {"total": {"value": len(hits)}, "hits": [
            self._hit(name, i, d, body) for i, d in hits[:size]]}}

    def _scroll(self, method, body):
        if method == "DELETE":
            self.scrolls.pop(body.get("scroll_id"), None)
            return {"succeeded": True}
        key = body["scroll_id"]
        name, sbody, left = self.scrolls[key]
        size = int(sbody.get("size", 10))
        self.scrolls[key] = (name, sbody, left[size:])
        return {"_scroll_id": key, "hits": {"hits": [
            self._hit(name, i, d, sbody) for i, d in left[:size]]}}

    def _mget(self, name, body):
        idx = self._index_of(name)
        docs = []
        for i in body.get("ids", []):
            d = idx["docs"].get(i)
            if d is None:
                docs.append({"_index": name, "_id": i, "found": False})
            else:
                docs.append({"_index": name, "_id": i, "found": True,
                             "_version": d["_version"],
                             "_seq_no": d["_seq_no"],
                             "_primary_term": d["_primary_term"],
                             "_source": d["_source"]})
        return {"docs": docs}

    def _stats(self, name, kind):
        idx = self._index_of(name)
        shards = {}
        for n, top in enumerate(idx["seq"]):
            total = self.refreshes.get(name, 0) + 3 * self._counter()
            shards[str(n)] = [{
                "routing": {"node": "n1", "primary": True},
                "refresh": {"total": total, "external_total": total},
                "seq_no": {"global_checkpoint": top, "max_seq_no": top},
                "docs": {"deleted": idx["deleted"][n]}}]
        return {"indices": {name: {"uuid": idx["settings"]["uuid"],
                                   "shards": shards}}}

    # ---- bulk ----

    def bulk_action(self, action):
        """(ok, {op: item}) for one action of a bulk stream, as the client
        library's `streaming_bulk` yields it."""
        action = dict(action)
        op = action.pop("_op_type", "index")
        name, doc_id = action.pop("_index"), str(action.pop("_id"))
        self._index_of(name)
        if op == "delete":
            status = self.delete(name, doc_id)
            item = {"_index": name, "_id": doc_id, "status": status}
            return status < 300, {"delete": item}
        version = action.get("version")
        if version is not None and action.get("version_type") not in (
                None, "external"):
            raise RuntimeError("fake cluster: version_type")
        status, got = self.write(name, doc_id, action.get("_source") or {},
                                 version, action.get("routing"))
        item = {"_index": name, "_id": doc_id, "status": status,
                "_seq_no": self.indexes[name]["docs"].get(doc_id, {}).get(
                    "_seq_no", -1)}
        if status == 409:
            item["error"] = {"type": "version_conflict_engine_exception",
                             "reason": "version conflict"}
        return status < 300, {op: item}


class _Missing(Exception):
    pass


def _millis(value):
    """A date as the cluster's `epoch_millis` format gives it: text."""
    import datetime
    if isinstance(value, (int, float)):
        return str(int(value))
    when = datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if when.tzinfo is None:
        when = when.replace(tzinfo=datetime.timezone.utc)
    return str(int(when.timestamp() * 1000))


def _fake_library(clusters):
    """A stand-in for the client library: `OpenSearch(hosts=...)` and
    `helpers.streaming_bulk`, over the cluster a host names."""
    lib = types.ModuleType("opensearchpy")
    helpers = types.ModuleType("opensearchpy.helpers")

    class OpenSearch:
        def __init__(self, hosts=None, **kw):
            self.host = hosts[0]
            self.kw = kw

    def streaming_bulk(client, actions, chunk_size=500,
                       max_chunk_bytes=0, raise_on_error=True,
                       raise_on_exception=True, max_retries=0,
                       initial_backoff=2, max_backoff=600, yield_ok=True,
                       **kw):
        cluster = clusters[client.host]
        for action in actions:
            ok, item = cluster.bulk_action(action)
            if ok and not yield_ok:
                continue
            yield ok, item
    lib.OpenSearch = OpenSearch
    lib.helpers = helpers
    helpers.streaming_bulk = streaming_bulk
    return lib, helpers


@contextlib.contextmanager
def installed(src, dst=None):
    """The clusters stood in for the wire and the client library, until the
    block ends: `src` for the source side and `dst` for the target (the
    same cluster where none is given, as a hop within one cluster is)."""
    from migkit.engines.opensearch import OpenSearchEngine
    dst = dst or src
    by_side = {"src": src, "dst": dst}
    hosts = {"http://src.example.com:9200": src,
             "http://dst.example.com:9200": dst}
    real_call, real_base = OpenSearchEngine._call, OpenSearchEngine._base

    def call(self, side, method, path, body=None, ndjson=False,
             missing_ok=False):
        return by_side[side].handle(method, path, body, ndjson, missing_ok)

    def base(self, side):
        return f"http://{side}.example.com:9200"
    lib, helpers = _fake_library(hosts)
    saved = {k: sys.modules.get(k) for k in ("opensearchpy",
                                             "opensearchpy.helpers")}
    OpenSearchEngine._call, OpenSearchEngine._base = call, base
    sys.modules["opensearchpy"], sys.modules["opensearchpy.helpers"] = \
        lib, helpers
    try:
        yield
    finally:
        OpenSearchEngine._call, OpenSearchEngine._base = real_call, real_base
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v
