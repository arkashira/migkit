"""An in-memory stand-in for the Azure Cosmos DB SDK (`azure.cosmos`), shaped
like the calls migkit makes of it - accounts by URL, databases, containers
with a partition key, items kept by id within a partition key value, feed
ranges, a change feed in both modes with a continuation per range,
transactional batches, a conflicts feed - so the engine's own logic runs
against something that answers as the service does.

It is a model of what migkit asks, not of the service: no request units, no
indexing, no consistency levels. What it does not hold is said where a test
needs it. Use `installed(monkeypatch)` to put it where `azure.cosmos` is
imported from.
"""
import base64
import json
import sys
import threading
import types
import zlib

URL = "http://cosmos.example.com:8081/"
KEY = "CHANGE_ME"
RANGES = 2


class CosmosHttpResponseError(Exception):
    def __init__(self, message="", status_code=400):
        super().__init__(message)
        self.status_code = status_code


class CosmosResourceNotFoundError(CosmosHttpResponseError):
    def __init__(self, message="Entity with the specified id does not exist"):
        super().__init__(message, 404)


class CosmosResourceExistsError(CosmosHttpResponseError):
    def __init__(self, message="Entity with the specified id already exists"):
        super().__init__(message, 409)


class PartitionKey:
    def __init__(self, path, kind="Hash", version=2):
        self.paths = [path] if isinstance(path, str) else list(path)
        self.kind = kind


_ACCOUNTS = {}


def reset():
    _ACCOUNTS.clear()


def _range_of(partition):
    return zlib.crc32(json.dumps(partition, sort_keys=True,
                                 default=str).encode()) % RANGES


def _part(paths, body):
    return [body.get(p.strip("/")) for p in paths]


class _Feed:
    """What `query_items_change_feed` returns: iterated whole (until it is
    caught up), or by page, each page setting the continuation header."""

    def __init__(self, container, kw):
        self.c, self.kw = container, kw

    def by_page(self):
        return _Pages(self)

    def __iter__(self):
        for page in self.by_page():
            yield from page


class _Pages:
    def __init__(self, feed):
        self.feed, self.done = feed, False

    def __iter__(self):
        return self

    def __next__(self):
        if self.done:
            raise StopIteration
        page = self.feed.c._feed_page(self.feed.kw)
        if not page:
            self.done = True
        return page


class Container:
    def __init__(self, db, id_, key, props=None):
        self.id, self.db = id_, db
        self.lock = threading.RLock()
        self.items = {}
        self.log = []                      # (lsn, range, op, body, meta)
        self.lsn = 0
        self.conflicts = []
        self.props = {"id": id_, "_rid": f"rid-{db.id}-{id_}",
                      "partitionKey": {"paths": list(key.paths),
                                       "kind": key.kind, "version": 2},
                      "indexingPolicy": {
                          "indexingMode": "consistent", "automatic": True,
                          "includedPaths": [{"path": "/*"}],
                          "excludedPaths": [{"path": '/"_etag"/?'}]},
                      **(props or {})}
        self.scripts = _Scripts()
        self.throughput = None
        self.expired_before = 0
        self.client_connection = types.SimpleNamespace(
            last_response_headers={})
        self.calls = []

    # ---- items -----------------------------------------------------------

    def _paths(self):
        return self.props["partitionKey"]["paths"]

    def _key(self, body):
        return (json.dumps(_part(self._paths(), body), default=str),
                body["id"])

    def upsert_item(self, body, **kw):
        with self.lock:
            k = self._key(body)
            op = "replace" if k in self.items else "create"
            self.lsn += 1
            stored = {**body, "_rid": f"{self.id}-{self.lsn}",
                      "_etag": f"e{self.lsn}", "_ts": 0, "_lsn": self.lsn,
                      "_self": "", "_attachments": ""}
            self.items[k] = stored
            self.log.append((self.lsn, _range_of(json.loads(k[0])), op,
                             stored, {"id": body["id"],
                                      "partition": _part(self._paths(),
                                                         body)}))
            return stored

    def execute_item_batch(self, ops, partition_key=None, **kw):
        with self.lock:
            return [self.upsert_item(args[0]) for op, args in ops
                    if op == "upsert"]

    def _find(self, item_id, partition_key):
        part = partition_key if isinstance(partition_key, list) \
            else [partition_key]
        k = (json.dumps(part, default=str), item_id)
        return k, self.items.get(k)

    def read_item(self, item, partition_key, **kw):
        with self.lock:
            _, got = self._find(item, partition_key)
            if got is None:
                raise CosmosResourceNotFoundError()
            return dict(got)

    def delete_item(self, item, partition_key, **kw):
        with self.lock:
            k, got = self._find(item, partition_key)
            if got is None:
                raise CosmosResourceNotFoundError()
            del self.items[k]
            self.lsn += 1
            self.log.append((self.lsn, _range_of(json.loads(k[0])), "delete",
                             None, {"id": item, "partition": json.loads(k[0])}))

    def read_feed_ranges(self):
        return [{"Range": {"min": f"{i:02X}", "max": f"{i + 1:02X}",
                           "isMinInclusive": True, "isMaxInclusive": False}}
                for i in range(RANGES)]

    @staticmethod
    def _index(feed_range):
        return int(feed_range["Range"]["min"], 16)

    def query_items(self, query, feed_range=None, max_item_count=None,
                    enable_cross_partition_query=None, priority=None, **kw):
        self.calls.append(("query_items", query, priority))
        with self.lock:
            rows = [dict(v) for k, v in sorted(self.items.items())
                    if feed_range is None
                    or _range_of(json.loads(k[0])) == self._index(
                        feed_range)]
        return iter(rows)

    def read(self, **kw):
        return dict(self.props)

    def get_throughput(self, **kw):
        if self.throughput is None:
            raise CosmosHttpResponseError("no throughput of its own", 404)
        return types.SimpleNamespace(offer_throughput=self.throughput,
                                     auto_scale_max_throughput=None)

    def list_conflicts(self, **kw):
        return iter(list(self.conflicts))

    # ---- the change feed -------------------------------------------------

    @staticmethod
    def _token(rng, lsn, mode):
        return base64.b64encode(json.dumps(
            {"range": rng, "token": f'"{lsn}"', "mode": mode}).encode()
        ).decode()

    def query_items_change_feed(self, feed_range=None, start_time=None,
                                mode=None, continuation=None,
                                max_item_count=None, priority=None, **kw):
        self.calls.append(("change_feed", mode, priority))
        if mode == "AllVersionsAndDeletes" and not self.db.account.avad:
            raise CosmosHttpResponseError(
                "AllVersionsAndDeletes needs continuous backup", 400)
        return _Feed(self, {"feed_range": feed_range, "start": start_time,
                            "mode": mode, "continuation": continuation,
                            "max": max_item_count})

    def _feed_page(self, kw):
        with self.lock:
            cont = kw["continuation"]
            if cont:
                body = json.loads(base64.b64decode(cont))
                rng, after = body["range"], int(body["token"].strip('"'))
                kw["mode"] = body.get("mode")
                if after < self.expired_before:
                    raise CosmosHttpResponseError(
                        "the continuation is past the feed's retention", 410)
            else:
                rng = (self._index(kw["feed_range"])
                       if kw["feed_range"] else None)
                after = self.lsn if kw["start"] == "Now" else 0
            ranges = [rng] if rng is not None else range(RANGES)
            out, last = [], after
            for lsn, r, op, body, meta in self.log:
                if lsn <= after or r not in ranges:
                    continue
                if kw["max"] and len(out) >= kw["max"]:
                    break
                out.append(self._shaped(lsn, op, body, meta, kw["mode"]))
                last = lsn
            if not out:
                last = max(self.lsn, after) if kw["start"] == "Now" \
                    or cont else after
            if rng is not None:
                kw["continuation"] = self._token(rng, last, kw["mode"])
                kw["start"] = None
                self.client_connection.last_response_headers["etag"] = \
                    kw["continuation"]
            return out

    def _shaped(self, lsn, op, body, meta, mode):
        if mode == "AllVersionsAndDeletes":
            out = {"metadata": {"operationType": op, "lsn": lsn,
                                "id": meta["id"]}}
            if op == "delete":
                out["current"] = {}
                out["metadata"]["partitionKey"] = {
                    p.strip("/"): v for p, v in
                    zip(self._paths(), meta["partition"])}
            else:
                out["current"] = dict(body)
            return out
        return dict(body) if body is not None else None


class _Scripts:
    """A container's stored procedures, functions and triggers."""

    def __init__(self):
        self.sprocs, self.udfs, self.triggers = {}, {}, {}

    def list_stored_procedures(self, **kw):
        return iter([{"id": k, "body": v} for k, v in
                     sorted(self.sprocs.items())])

    def list_user_defined_functions(self, **kw):
        return iter([{"id": k, "body": v} for k, v in
                     sorted(self.udfs.items())])

    def list_triggers(self, **kw):
        return iter([{"id": k, "triggerType": t, "triggerOperation": o,
                      "body": b} for k, (t, o, b) in
                     sorted(self.triggers.items())])


class Database:
    def __init__(self, account, id_):
        self.account, self.id = account, id_
        self.containers = {}

    def read(self, **kw):
        return {"id": self.id, "_rid": f"db-{self.id}"}

    def list_containers(self, **kw):
        return iter([{"id": n} for n in sorted(self.containers)])

    def get_container_client(self, name):
        return _Lazy(self, name)

    def create_container(self, id=None, partition_key=None,
                         default_ttl=None, unique_key_policy=None,
                         indexing_policy=None, **kw):
        if id in self.containers:
            raise CosmosResourceExistsError()
        props = {k: v for k, v in (("defaultTtl", default_ttl),
                                   ("uniqueKeyPolicy", unique_key_policy),
                                   ("indexingPolicy", indexing_policy))
                 if v is not None}
        c = Container(self, id, partition_key or PartitionKey("/id"),
                      props)
        self.containers[id] = c
        return c

    create_container_if_not_exists = create_container

    def replace_container(self, container, partition_key=None,
                          indexing_policy=None, default_ttl=None,
                          conflict_resolution_policy=None, **kw):
        c = self.containers[container.id]
        if indexing_policy is not None:
            c.props["indexingPolicy"] = indexing_policy
        if default_ttl is not None:
            c.props["defaultTtl"] = default_ttl
        if conflict_resolution_policy is not None:
            c.props["conflictResolutionPolicy"] = conflict_resolution_policy
        return c


class _Lazy:
    """A container client for a name; the container is looked up when it is
    used, as the service answers a missing one then."""

    def __init__(self, db, name):
        object.__setattr__(self, "_db", db)
        object.__setattr__(self, "_name", name)

    def _c(self):
        got = self._db.containers.get(self._name)
        if got is None:
            raise CosmosResourceNotFoundError()
        return got

    def __getattr__(self, name):
        return getattr(self._c(), name)

    @property
    def id(self):
        return self._name


class Account:
    def __init__(self, avad):
        self.databases = {}
        self.avad = avad
        self.multi_write = False


class CosmosClient:
    def __init__(self, url, credential=None, connection_verify=True, **kw):
        self.account = _ACCOUNTS.setdefault(url, Account(avad=True))

    def get_database_account(self, **kw):
        return types.SimpleNamespace(
            EnableMultipleWritableLocations=self.account.multi_write)

    def list_databases(self, **kw):
        return iter([{"id": n} for n in sorted(self.account.databases)])

    def create_database(self, id, **kw):
        if id in self.account.databases:
            raise CosmosResourceExistsError()
        self.account.databases[id] = Database(self.account, id)
        return self.account.databases[id]

    def delete_database(self, id, **kw):
        self.account.databases.pop(id, None)

    def get_database_client(self, id):
        return self.account.databases.get(id) or Database(self.account, id)


def account(url=URL, avad=True):
    """The account at `url`, made if it is not there."""
    got = _ACCOUNTS.setdefault(url, Account(avad=avad))
    got.avad = avad
    return got


def installed(monkeypatch):
    """Put the model where `azure.cosmos` is imported from."""
    reset()
    pkg = types.ModuleType("azure")
    cosmos = types.ModuleType("azure.cosmos")
    exceptions = types.ModuleType("azure.cosmos.exceptions")
    cosmos.CosmosClient, cosmos.PartitionKey = CosmosClient, PartitionKey
    cosmos.exceptions = exceptions
    for n in ("CosmosHttpResponseError", "CosmosResourceNotFoundError",
              "CosmosResourceExistsError"):
        setattr(exceptions, n, globals()[n])
    pkg.cosmos = cosmos
    monkeypatch.setitem(sys.modules, "azure", pkg)
    monkeypatch.setitem(sys.modules, "azure.cosmos", cosmos)
    monkeypatch.setitem(sys.modules, "azure.cosmos.exceptions", exceptions)
    return CosmosClient(URL, KEY)


def hop(tmp_path, **kw):
    """Cosmos DB to Cosmos DB: the database `csrc` copied to `cdst`, both
    at the model's one account."""
    from migkit.config import Endpoint, Hop
    ep = dict(host="cosmos.example.com", port=8081, user="", password=KEY,
              options={"url": URL})
    kw.setdefault("databases", ["csrc"])
    kw.setdefault("db_map", {"csrc": "cdst"})
    h = Hop(name="cosmos", engine="cosmosdb", source=Endpoint(**ep),
            target=Endpoint(**{**ep, "options": dict(ep["options"])}), **kw)
    h.report_dir = lambda db=None: tmp_path
    return h


def engine(tmp_path, **kw):
    from migkit.engines.cosmosdb import CosmosEngine
    return CosmosEngine(hop(tmp_path, **kw))


def put(container, id, **fields):
    """An item with an id and the fields given."""
    return container.upsert_item({"id": str(id), **fields})


def items(container):
    """{id: item without the service's own fields}, as migkit reads them."""
    from migkit.changetopic import COSMOS_SYSTEM
    return {i["id"]: {k: v for k, v in i.items() if k not in COSMOS_SYSTEM
                      and k != "id"}
            for i in container.query_items("SELECT * FROM c")}


def copy(eng, db="csrc"):
    """Every container in scope made on the target and copied."""
    from migkit.cli import _Checkpoint
    eng.create_missing(db, lambda m: None)
    ck = _Checkpoint(eng.hop.report_dir(db) / "move.json")
    for sch, t in eng.list_move_tables(db):
        eng.move_table(db, sch, t, 10, ck, lambda m: None)
    eng.finish_created(db)
