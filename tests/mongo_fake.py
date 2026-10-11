"""A MongoDB deployment in memory, for the tests that hold what migkit asks
of the server without one (the way `tests/kafka_fake.py` stands for Kafka).

It answers the calls the engine makes - the client, its databases and
collections, the admin commands, the oplog, `$currentOp` - with what the
server answers them with, as measured: an index specification as
`index_information` and `list_indexes` give it, a view as `listCollections`
lists it, `hello` with the replica set's name and its cluster time. It is
no query engine: `find` takes equality, `$in` and `_id` ranges, which is
all the engine asks of it. A call the engine makes that this does not
answer raises, so a test never passes on a guess.
"""
import copy
import re

from bson import SON
from bson.timestamp import Timestamp
from pymongo.errors import OperationFailure


class Cursor:
    def __init__(self, docs):
        self.docs = list(docs)

    def sort(self, key, direction=1):
        if key == "$natural":
            self.docs = self.docs if direction == 1 else self.docs[::-1]
        else:
            self.docs.sort(key=lambda d: d.get(key), reverse=direction == -1)
        return self

    def limit(self, n):
        self.docs = self.docs[:n] if n else self.docs
        return self

    def __iter__(self):
        return iter(self.docs)


def _matches(doc, flt):
    for field, want in (flt or {}).items():
        have = doc.get(field)
        if isinstance(want, dict):
            for op, v in want.items():
                if op == "$in" and have not in v:
                    return False
                if op == "$gt" and not (have is not None and have > v):
                    return False
                if op == "$exists" and (field in doc) != bool(v):
                    return False
        elif have != want:
            return False
    return True


class Coll:
    """A collection - or a view, which has no documents and no indexes."""

    def __init__(self, db, name, **options):
        self.db, self.name = db, name
        self.options = dict(options)
        self.docs = []
        self.indexes = {"_id_": {"v": 2, "key": [("_id", 1)]}}

    @property
    def is_view(self):
        return "viewOn" in self.options

    def _index_check(self):
        if self.is_view:
            raise OperationFailure(
                f"Namespace {self.db.name}.{self.name} is a view, not a"
                " collection", 166)

    def index_information(self):
        self._index_check()
        return copy.deepcopy(self.indexes)

    def list_indexes(self):
        self._index_check()
        for name, spec in self.indexes.items():
            spec = dict(spec)
            yield {**spec, "key": SON(spec["key"]), "name": name}

    def create_index(self, keys, name=None, **spec):
        self._index_check()
        keys = [(keys, 1)] if isinstance(keys, str) else list(keys)
        name = name or "_".join(f"{f}_{d}" for f, d in keys)
        if name in self.indexes and self.indexes[name]["key"] != keys:
            raise OperationFailure(
                f"An index named {name} already exists with a different"
                " key", 86)
        self.indexes[name] = {"v": 2, "key": keys, **spec}
        return name

    def insert_one(self, doc):
        self.docs.append(dict(doc))

    def insert_many(self, docs, **kw):
        self.docs += [dict(d) for d in docs]

    def find(self, flt=None, projection=None, sort=None, **kw):
        found = [d for d in self.docs if _matches(d, flt)]
        cur = Cursor(found)
        for key, direction in (sort or []):
            cur.sort(key, direction)
        return cur

    def find_one(self, flt=None, projection=None, sort=None):
        return next(iter(self.find(flt, projection, sort)), None)

    def count_documents(self, flt):
        return len(list(self.find(flt)))

    def delete_many(self, flt):
        keep = [d for d in self.docs if not _matches(d, flt)]
        gone, self.docs = len(self.docs) - len(keep), keep

        class Done:
            deleted_count = gone
        return Done


class DB:
    def __init__(self, server, name):
        self.server, self.name = server, name
        self.colls = {}

    def __getitem__(self, name):
        if name not in self.colls:
            self.colls[name] = Coll(self, name)
            self.colls[name].implicit = True
        return self.colls[name]

    get_collection = __getitem__

    def _made(self):
        """The collections that exist: one only read from does not."""
        return {n: c for n, c in self.colls.items()
                if not getattr(c, "implicit", False) or c.docs
                or len(c.indexes) > 1}

    def list_collection_names(self, **kw):
        return sorted(self._made())

    def list_collections(self, filter=None, **kw):
        want = (filter or {}).get("type")
        for name, c in sorted(self._made().items()):
            kind = "view" if c.is_view else c.options.get("kind", "collection")
            if want and kind != want:
                continue
            opts = {k: v for k, v in c.options.items() if k != "kind"}
            yield {"name": name, "type": kind, "options": opts}

    def create_collection(self, name, **options):
        if name in self._made():
            raise OperationFailure(f"Collection {self.name}.{name} already"
                                   " exists.", 48)
        self.colls[name] = Coll(self, name, **options)
        return self.colls[name]

    def command(self, name, *args, **kw):
        if name == "listCollections":
            got = list(self.list_collections(kw.get("filter") and {
                "type": (kw["filter"] or {}).get("type")}))
            if kw.get("filter", {}) and "name" in kw["filter"]:
                got = [c for c in got if c["name"] == kw["filter"]["name"]]
            return {"cursor": {"firstBatch": got}}
        if name == "collMod":
            # `command("collMod", coll, index={"name": ..., ...})`
            held = self.server.refuse_collmod
            if held:
                raise OperationFailure(held, 13)
            ix = self[args[0]].indexes[kw["index"]["name"]]
            ix["expireAfterSeconds"] = kw["index"]["expireAfterSeconds"]
            self.server.collmods.append((args[0], kw["index"]["name"],
                                         kw["index"]["expireAfterSeconds"]))
            return {"ok": 1}
        raise OperationFailure(f"no such command: '{name}'", 59)


class Admin:
    def __init__(self, server):
        self.server = server

    def command(self, name, *args, **kw):
        s = self.server
        if name == "hello":
            out = {"operationTime": s.now}
            if s.replica_set:
                out["setName"] = s.replica_set
                out["lastWrite"] = {"majorityOpTime": {"ts": s.now}}
            if s.sharded:
                out["msg"] = "isdbgrid"
            return out
        if name == "buildInfo":
            if s.version is None:
                raise OperationFailure("not authorized on admin", 13)
            return {"version": s.version, "gitVersion": "0" * 40,
                    "storageEngines": ["wiredTiger"]}
        if name == "getLastRequestStatistics":
            raise OperationFailure("no such command", 59)
        if name == "top":
            if s.top is None:
                raise OperationFailure("not authorized on admin", 13)
            return {"totals": s.top()}
        if name == "replSetGetConfig":
            return {"config": {"settings": {"replicaSetId": "a1"}}}
        raise OperationFailure(f"no such command: '{name}'", 59)

    def aggregate(self, pipeline, **kw):
        if "$currentOp" not in pipeline[0]:
            raise OperationFailure("only $currentOp is answered here", 59)
        if self.server.running is None:
            raise OperationFailure("not authorized on admin", 13)
        rx = re.compile(pipeline[1]["$match"]["ns"]["$regex"])
        return iter([op for op in self.server.running()
                     if rx.match(op.get("ns", ""))])


class Client:
    def __init__(self, server):
        self.server = server
        self.admin = Admin(server)
        self.local = DB(server, "local")
        oplog = self.local["oplog.rs"]
        oplog.implicit = False
        oplog.docs = [{"ts": t} for t in server.oplog]

    def __getitem__(self, name):
        return self.server.dbs.setdefault(name, DB(self.server, name))

    def list_database_names(self):
        return sorted(self.server.dbs)


class Server:
    """One deployment: its databases, its version, its replica set (None
    for a standalone), the oplog's first entry and what is running."""

    def __init__(self, version="7.0.5", replica_set="rs0", sharded=False,
                 oplog_from=1_000, now=8_200):
        self.version, self.replica_set, self.sharded = (
            version, replica_set, sharded)
        self.now = Timestamp(now, 1)
        self.oplog = [Timestamp(oplog_from, 1)] if replica_set else []
        self.dbs = {}
        #: callables answering `$currentOp` and `top`; None refuses
        self.running = lambda: []
        self.top = lambda: {}
        #: every `collMod` of an index it was asked, and a reason to refuse
        self.collmods, self.refuse_collmod = [], ""

    def client(self):
        return Client(self)

    def db(self, name):
        return self.dbs.setdefault(name, DB(self, name))
