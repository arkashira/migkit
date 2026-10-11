"""A MongoDB server in memory, for the tests that prove what migkit does
with a server's answers and have none to ask.

Only what migkit's MongoDB engine asks is modelled, and modelled as the
server answers it: a query's operators match by BSON's type brackets
(`$gt` of a number never takes a string), a missing field and a null one
both match `null`, `find` is read as the bytes the server sends when the
caller asks for raw documents, `dbHash` is a digest of the stored bytes,
and `$type` names the BSON type of a value. An aggregation is answered
only for the one shape the engine's own `field_types` asks; any other
pipeline answers nothing, as a server with an empty collection would -
so a test that depends on one says so with its own `aggregate`.

Every read and write of a collection is logged in `FakeClient.touched`
as (database, collection), which is how a test proves that a collection
the hop leaves out was not so much as looked at.
"""
import copy
import hashlib
import re

import bson
import pytest
from bson.codec_options import CodecOptions
from bson.decimal128 import Decimal128
from bson.int64 import Int64
from bson.objectid import ObjectId
from bson.raw_bson import RawBSONDocument

_MISSING = object()


def type_name(value):
    """The name `$type` gives a value."""
    import datetime
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, Int64):
        return "long"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "double"
    if isinstance(value, Decimal128):
        return "decimal"
    if isinstance(value, str):
        return "string"
    if isinstance(value, datetime.datetime):
        return "date"
    if isinstance(value, ObjectId):
        return "objectId"
    if isinstance(value, bson.Binary):
        return "binData"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, (list, tuple)):
        return "array"
    if isinstance(value, bson.Timestamp):
        return "timestamp"
    if isinstance(value, bson.Regex):
        return "regex"
    return type(value).__name__


def _bracket(value):
    """The BSON type bracket a comparison operator stays inside."""
    kind = type_name(value)
    return "number" if kind in ("int", "long", "double", "decimal") else kind


def _rank(value):
    """A total order across types in the way BSON sorts them."""
    order = ["null", "number", "string", "object", "array", "binData",
             "objectId", "bool", "date"]
    kind = _bracket(value)
    at = order.index(kind) if kind in order else len(order)
    if kind == "number" and isinstance(value, Decimal128):
        value = float(value.to_decimal())
    return (at, value if kind not in ("object", "array") else repr(value))


def _held(doc, field):
    return field in doc, doc.get(field)


def matches(flt, doc):
    """Whether `doc` is one `flt` takes, by the server's rules."""
    for key, want in (flt or {}).items():
        if key == "$and":
            if not all(matches(f, doc) for f in want):
                return False
        elif key == "$or":
            if not any(matches(f, doc) for f in want):
                return False
        elif key == "$expr":
            if not _expr(want, doc):
                return False
        elif isinstance(want, dict) and want and all(
                k.startswith("$") for k in want):
            if not _ops(doc, key, want):
                return False
        else:
            has, got = _held(doc, key)
            if want is None:
                if has and got is not None:
                    return False
            elif not has or got != want or \
                    _bracket(got) != _bracket(want):
                return False
    return True


def _expr(node, doc):
    if isinstance(node, bool):
        return node
    if isinstance(node, dict) and len(node) == 1:
        (op, args), = node.items()
        if op in ("$gt", "$gte", "$lt", "$lte", "$eq", "$ne"):
            a, b = (_value(x, doc) for x in args)
            ra, rb = _rank(a), _rank(b)
            return {"$gt": ra > rb, "$gte": ra >= rb, "$lt": ra < rb,
                    "$lte": ra <= rb, "$eq": ra == rb, "$ne": ra != rb}[op]
    raise AssertionError(f"$expr {node!r} is not modelled")


def _value(node, doc):
    if isinstance(node, str) and node.startswith("$") \
            and not node.startswith("$$"):
        return doc.get(node[1:])
    if isinstance(node, dict) and set(node) == {"$literal"}:
        return node["$literal"]
    return node


def _ops(doc, field, ops):
    has, got = _held(doc, field)
    for op, arg in ops.items():
        if op == "$eq":
            ok = (not has or got is None) if arg is None else (
                has and got == arg and _bracket(got) == _bracket(arg))
        elif op == "$ne":
            ok = not ((not has or got is None) if arg is None else (
                has and got == arg and _bracket(got) == _bracket(arg)))
        elif op in ("$gt", "$gte", "$lt", "$lte"):
            ok = has and got is not None and _bracket(got) == _bracket(arg) \
                and {"$gt": lambda: _rank(got) > _rank(arg),
                     "$gte": lambda: _rank(got) >= _rank(arg),
                     "$lt": lambda: _rank(got) < _rank(arg),
                     "$lte": lambda: _rank(got) <= _rank(arg)}[op]()
        elif op == "$in":
            ok = (has and any(got == a and _bracket(got) == _bracket(a)
                              for a in arg if a is not None)) or (
                None in arg and (not has or got is None))
        elif op == "$nin":
            ok = not _ops(doc, field, {"$in": arg})
        elif op == "$exists":
            ok = has == bool(arg)
        elif op == "$type":
            ok = has and (type_name(got) == arg)
        elif op == "$regex":
            ok = has and isinstance(got, str) and re.search(
                arg, got, re.S) is not None
        elif op == "$options":
            ok = True
        elif op == "$not":
            ok = not _ops(doc, field, arg)
        else:
            raise AssertionError(f"{op} is not modelled")
        if not ok:
            return False
    return True


def _project(doc, projection):
    if not projection:
        return copy.deepcopy(doc)
    keep = {k for k, v in projection.items() if v}
    out = {}
    if projection.get("_id", 1):
        out["_id"] = doc.get("_id")
    for k in keep - {"_id"}:
        if k in doc:
            out[k] = copy.deepcopy(doc[k])
    return out


class Cursor:
    def __init__(self, docs, raw):
        self.docs, self.raw, self.closed = docs, raw, False

    def sort(self, key, direction=1):
        if isinstance(key, list):
            key, direction = key[0]
        self.docs = sorted(self.docs, key=lambda d: _rank(d.get(key)),
                           reverse=direction < 0)
        return self

    def limit(self, n):
        if n:
            self.docs = self.docs[:n]
        return self

    def __iter__(self):
        for d in self.docs:
            yield RawBSONDocument(bson.encode(d)) if self.raw else d

    def close(self):
        self.closed = True


class FakeCollection:
    codec_options = CodecOptions()

    def __init__(self, db, name, docs=(), indexes=None, options=None,
                 view=False, raw=False):
        self.db, self.name = db, name
        self.docs = [copy.deepcopy(d) for d in docs]
        self.indexes = indexes if indexes is not None else {}
        self.options = options or {}
        self.view, self.raw = view, raw

    # --- reads
    def _touch(self):
        self.db.client.touched.add((self.db.name, self.name))

    def with_options(self, codec_options=None, **kw):
        twin = FakeCollection(self.db, self.name, raw=bool(
            codec_options is not None
            and codec_options.document_class is RawBSONDocument))
        twin.docs, twin.indexes, twin.options = \
            self.docs, self.indexes, self.options
        twin.view = self.view
        return twin

    def find(self, flt=None, projection=None, sort=None, limit=0, **kw):
        self._touch()
        got = [_project(d, projection) for d in self.docs
               if matches(flt, d)]
        cursor = Cursor(got, self.raw)
        if sort:
            cursor.sort(sort)
        return cursor.limit(limit)

    def find_one(self, flt=None, projection=None, sort=None, **kw):
        got = list(self.find(flt, projection, sort=sort, limit=1))
        return got[0] if got else None

    def count_documents(self, flt=None, **kw):
        self._touch()
        return sum(1 for d in self.docs if matches(flt, d))

    def aggregate(self, pipeline, **kw):
        self._touch()
        first = pipeline[0] if pipeline else {}
        sample = first.get("$limit")
        pipe = pipeline[1:] if sample else pipeline
        if pipe and "$project" in pipe[0] and "kv" in pipe[0]["$project"] \
                and "$objectToArray" in str(pipe[0]["$project"]["kv"]):
            return self._types(self.docs[:sample] if sample else self.docs,
                               grouped="$addToSet" in str(pipe))
        return self.answer(pipeline)

    def answer(self, pipeline):
        """What a pipeline this fake does not model gives: nothing."""
        return []

    @staticmethod
    def _types(docs, grouped):
        stats = {}
        for d in docs:
            for k, v in d.items():
                t, present, nulls = stats.setdefault(k, [set(), 0, 0])
                t.add(type_name(v))
                stats[k][1] += 1
                stats[k][2] += v is None
        if grouped:
            return [{"_id": k, "types": sorted(t), "present": n,
                     "nulls": z} for k, (t, n, z) in sorted(stats.items())]
        hist = {}
        for d in docs:
            for k, v in d.items():
                hist[(k, type_name(v))] = hist.get((k, type_name(v)), 0) + 1
        return [{"_id": {"f": k, "t": t}, "n": n}
                for (k, t), n in hist.items()]

    def list_indexes(self):
        self._touch()
        out = [{"v": 2, "key": {"_id": 1}, "name": "_id_",
                **({"collation": self.options["collation"]}
                   if "collation" in self.options else {})}]
        for name, spec in self.indexes.items():
            out.append({"v": 2, "name": name, **spec})
        return iter(out)

    def index_information(self):
        out = {"_id_": {"v": 2, "key": [("_id", 1)]}}
        for name, spec in self.indexes.items():
            out[name] = {"v": 2, **{k: (list(v.items()) if k == "key" else v)
                                    for k, v in spec.items()}}
        return out

    # --- writes
    def create_index(self, keys, name=None, **spec):
        self._touch()
        self.indexes[name] = {"key": dict(keys), **spec}
        return name

    def insert_many(self, docs, ordered=True):
        self._touch()
        for d in docs:
            d = bson.decode(d.raw) if isinstance(d, RawBSONDocument) else d
            if any(x["_id"] == d["_id"] for x in self.docs):
                from pymongo.errors import BulkWriteError
                raise BulkWriteError({"writeErrors": [{"code": 11000}]})
            self.docs.append(copy.deepcopy(d))

    def delete_many(self, flt):
        self._touch()
        keep = [d for d in self.docs if not matches(flt, d)]
        gone = len(self.docs) - len(keep)
        self.docs[:] = keep
        return type("R", (), {"deleted_count": gone})()

    def delete_one(self, flt):
        self._touch()
        for i, d in enumerate(self.docs):
            if matches(flt, d):
                del self.docs[i]
                return

    def replace_one(self, flt, doc, upsert=False):
        self._touch()
        for i, d in enumerate(self.docs):
            if matches(flt, d):
                self.docs[i] = copy.deepcopy(doc)
                return
        if upsert:
            self.docs.append(copy.deepcopy(doc))

    def bulk_write(self, ops, ordered=True):
        for op in ops:
            doc = getattr(op, "_doc", None)
            flt = getattr(op, "_filter", None)
            self.replace_one(flt, bson.decode(doc.raw)
                             if isinstance(doc, RawBSONDocument) else doc,
                             upsert=True)


class FakeDatabase:
    def __init__(self, client, name):
        self.client, self.name = client, name
        self.colls = {}

    def add(self, name, docs=(), **kw):
        self.colls[name] = FakeCollection(self, name, docs, **kw)
        return self.colls[name]

    def __getitem__(self, name):
        if name not in self.colls:
            # a collection comes into being on its first write, as it does
            self.colls[name] = FakeCollection(self, name)
            self.colls[name].created_empty = True
        return self.colls[name]

    def get_collection(self, name, codec_options=None):
        return self[name].with_options(codec_options=codec_options)

    def list_collection_names(self):
        return [n for n, c in self.colls.items()
                if not getattr(c, "created_empty", False) or c.docs]

    def list_collections(self, filter=None, **kw):
        return iter([{"name": n, "type": "view" if c.view else "collection",
                      "options": c.options}
                     for n, c in self.colls.items()
                     if n in self.list_collection_names()
                     and all(("view" if c.view else "collection") == v
                             for k, v in (filter or {}).items()
                             if k == "type")])

    def command(self, cmd, *args, **kw):
        if isinstance(cmd, dict):
            name, kw = next(iter(cmd)), {**kw, **{k: v for k, v in
                                                  cmd.items()}}
        else:
            name = cmd
        if name == "dbHash":
            if self.client.no_dbhash:
                from pymongo.errors import OperationFailure
                raise OperationFailure("dbHash is not allowed")
            names = kw.get("collections") or self.list_collection_names()
            out = {}
            for n in names:
                if n in self.colls and n in self.list_collection_names():
                    body = b"".join(bson.encode(d) for d in sorted(
                        self.colls[n].docs, key=lambda d: _rank(d["_id"])))
                    out[n] = hashlib.md5(body).hexdigest()
            return {"collections": out}
        if name == "listCollections":
            want = kw.get("filter", {}).get("name")
            first = [c for c in self.list_collections()
                     if want is None or c["name"] == want]
            return {"cursor": {"firstBatch": first}}
        if name in ("hello", "isMaster"):
            return {"msg": "standalone", "operationTime": None}
        if name == "serverStatus":
            return {"connections": {"current": 1, "available": 100,
                                    "active": 1}}
        if name == "rolesInfo":
            return {"roles": self.client.roles.get(self.name, [])}
        if name == "usersInfo":
            return {"users": [u for u in self.client.accounts]}
        if name == "buildInfo":
            return {"version": "7.0.0"}
        raise AssertionError(f"the command {name} is not modelled")

    def watch(self, *a, **kw):
        return self.client.stream


class FakeClient:
    """One server: its databases, the accounts and roles it holds, and
    what was touched."""
    codec_options = CodecOptions()

    def __init__(self):
        self.dbs = {}
        self.touched = set()
        self.no_dbhash = False
        self.roles = {}
        self.accounts = []
        self.stream = None

    def db(self, name):
        if name not in self.dbs:
            self.dbs[name] = FakeDatabase(self, name)
        return self.dbs[name]

    def __getitem__(self, name):
        return self.db(name)

    @property
    def admin(self):
        return self.db("admin")

    @property
    def local(self):
        return self.db("local")

    def list_database_names(self):
        return sorted(self.dbs)

    def close(self):
        pass


class FakeStream:
    """A change stream: the events it holds, read one at a time."""

    def __init__(self, events):
        self.events = list(events)
        self.resume_token = {"_data": "8200"}

    def try_next(self):
        return self.events.pop(0) if self.events else None

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


#: the fake servers by the host a hop's endpoint names them with, which
#: is how `migkit.users` finds the one it was handed an endpoint of
SERVERS = {}


@pytest.fixture(autouse=True)
def no_real_server(monkeypatch):
    """No test that imports this module reaches a server: the accounts
    are read through the fakes, by the endpoint's host."""
    from migkit import users
    SERVERS.clear()
    monkeypatch.setattr(users, "_mongo_client", lambda ep: SERVERS[ep.host])
    yield
    SERVERS.clear()


def engine(hop_kw=None, tmp_path=None, source=None, target=None):
    """(a MongoDB engine on a hop between two fake servers, source, target):
    the engine's `_client` answers with the fakes, and its reports go to
    `tmp_path`."""
    from migkit.config import Endpoint, Hop
    from migkit.engines.mongodb import MongoEngine
    src, dst = source or FakeClient(), target or FakeClient()
    SERVERS.update({"10.0.0.1": src, "10.0.0.2": dst})
    kw = dict(name="m", engine="mongodb", databases=["shop"],
              source=Endpoint(host="10.0.0.1", port=27017, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=27017, user="u",
                              password="CHANGE_ME"))
    kw.update(hop_kw or {})
    hop = Hop(**kw)
    if tmp_path is not None:
        hop.report_dir = lambda db=None: tmp_path
    eng = MongoEngine(hop)
    eng._client = lambda side: src if side == "src" else dst
    return eng, src, dst
