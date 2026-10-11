"""Google Cloud Firestore as a side of a pair (backlog P0-CLOUDS), in both of
its modes: native (`FirestoreEngine`) and Datastore mode
(`DatastoreEngine`), each through its own SDK.

A hop's database is a Firestore database (`(default)` unless named);
the side's host is the project (`firestore://project/(default)`,
`emulator: host:port` for the emulator).

Native mode. A table is a collection group - every collection of that id,
at any depth - its top-level collections by default, and deeper ones
where the side names them (`collection_groups: [orders, lines]`). A row
is a document: `__name__` its path in the database, which it is keyed and
ordered by, and its top-level fields as columns, described by the kinds
their values hold (a field holding several kinds is JSON). A reference is
its document's path, a point its latitude and longitude, a map or an array
JSON in which a timestamp, bytes, a reference, a point, a vector or a whole
double is kept as `{"$timestamp" | "$bytes" | "$ref" | "$geo" | "$vector"
| "$double": ...}` - so a
document copied to another Firestore database comes back field for field
and kind for kind (a reference stays a reference, an integer is never a
double). A group is read by the service's own partitions, side by side,
for a digest; written by the bulk writer, its failures after its own
retries collected and said.

Firestore has no change feed. What it has is a listener: a query's
snapshots, each consistent as of its read time, and between two of them
every document added, changed or removed. The follow is a listener on
each group, begun where the follow begins - before the copy - and what it
proves holds while this process holds the listener: every change up to
the last snapshot's read time is carried, in order, and the fence is that
read time. A listener cannot be resumed by another process: a follow
started again from a saved position begins a new listener, whose first
snapshot is every document as it is - each written again, and each on the
target that the snapshot lacks deleted - so it ends equal all the same,
at the price of writing the group again. A document changed and changed
back between two snapshots is one change, which a copy does not need.

Datastore mode. A table is a kind (in the side's `namespace`), a row an
entity: `__key__` its key's path as JSON (`["Parent", 12, "Child",
"x"]`), read in key order; embedded entities are JSON as maps are. It has
no listener and no change feed at all: nothing can follow it, and a move
of a Datastore database is a copy made while nothing writes, proved by
check.
"""
import base64
import concurrent.futures
import datetime
import json
import queue
import threading
import time
from decimal import Decimal

from .base import Engine, NeutralCopier, Result

NAME = "__name__"
#: documents written by one batch of Datastore's, the service's most
PUT = 500
#: an entity's text or bytes indexed at most this long; longer is kept
#: out of the indexes, as the service requires
INDEXED_MOST = 1500


def _nanotime(v):
    from .spanner import _nanotime as nt
    return nt(v)


# ---- values -----------------------------------------------------------------

def fs_kind(value):
    """A Firestore value's kind, as the SDK hands it over."""
    from google.cloud.firestore_v1 import DocumentReference, GeoPoint
    from google.cloud.firestore_v1.vector import Vector
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "double"
    if isinstance(value, str):
        return "string"
    if isinstance(value, bytes):
        return "bytes"
    if isinstance(value, datetime.datetime):
        return "timestamp"
    if isinstance(value, DocumentReference):
        return "reference"
    if isinstance(value, GeoPoint):
        return "geopoint"
    if isinstance(value, Vector):
        return "vector"
    if isinstance(value, dict):
        return "map"
    if isinstance(value, (list, tuple)):
        return "array"
    return "string"


def ds_kind(value):
    """A Datastore value's kind."""
    from google.cloud.datastore import Entity, Key
    from google.cloud.datastore.helpers import GeoPoint
    if isinstance(value, Key):
        return "key"
    if isinstance(value, GeoPoint):
        return "geopoint"
    if isinstance(value, Entity):
        return "entity"
    if isinstance(value, bytes):
        return "blob"
    return fs_kind(value)


def to_ext(value):
    """A value inside a map or an array as JSON, a kind JSON has none of
    marked (`$timestamp`, `$bytes`, `$ref`, `$geo`, `$vector`, `$key`, and
    `$double` for a double JSON would read as an integer)."""
    kind = ds_kind(value)
    if kind in ("map", "entity"):
        return {str(k): to_ext(v) for k, v in dict(value).items()}
    if kind == "array":
        return [to_ext(v) for v in value]
    if kind == "timestamp":
        from .. import canon
        return {"$timestamp": canon.render_value("timestamp",
                                                 _nanotime(value))}
    if kind in ("bytes", "blob"):
        return {"$bytes": base64.b64encode(bytes(value)).decode()}
    if kind == "reference":
        return {"$ref": value.path}
    if kind == "key":
        return {"$key": list(value.flat_path)}
    if kind == "geopoint":
        return {"$geo": [value.latitude, value.longitude]}
    if kind == "vector":
        return {"$vector": list(value)}
    if kind == "double" and value.is_integer():
        # JSON's numbers are compared by value, where 3 and 3.0 are one:
        # a whole double is marked, so an integer made a double is seen
        return {"$double": repr(value)}
    return value


def from_ext(value, ref=None, key=None):
    """`to_ext` undone: `ref(path)` makes a document reference, `key(path)`
    an entity key."""
    if isinstance(value, list):
        return [from_ext(v, ref, key) for v in value]
    if not isinstance(value, dict):
        return float(value) if isinstance(value, Decimal) else value
    if len(value) == 1:
        (mark, got), = value.items()
        if mark == "$timestamp":
            from .. import nanotime
            v = nanotime.parse(got)
            return v.replace(tzinfo=datetime.timezone.utc) \
                if v.tzinfo is None else v
        if mark == "$bytes":
            return base64.b64decode(got)
        if mark == "$double":
            return float(got)
        if mark == "$ref" and ref is not None:
            return ref(got)
        if mark == "$key" and key is not None:
            return key(got)
        if mark == "$geo":
            from google.cloud.firestore_v1 import GeoPoint
            return GeoPoint(float(got[0]), float(got[1]))
        if mark == "$vector":
            from google.cloud.firestore_v1.vector import Vector
            return Vector([float(x) for x in got])
    return {k: from_ext(v, ref, key) for k, v in value.items()}


class _Documents(NeutralCopier, Engine):
    """What both modes share: a table described by the kinds its values
    hold, or - one migkit made from another engine - by the classes kept
    for it (`migkit.described`); compared, repaired and followed by the
    pair machinery every engine shares."""
    checks = ("schema", "counts", "data")
    FOLDED_BECAUSE = "has no way to hash documents in the service"
    OVER_NETWORK = True
    RESUMES_BY_KEY = True
    CREATES_ON_WRITE = True
    EXPRESSES_ABSENT = True
    CHANGE_POINT_READS_ONLY = True
    KEY = NAME

    def _ep(self, side):
        return self.hop.source if side == "src" else self.hop.target

    def _d(self, side, db):
        return self.hop.target_db(db) if side == "dst" else db

    def databases(self):
        return list(self.hop.databases or ["(default)"])

    def _where(self, side, db):
        ep = self._ep(side)
        return (f"{ep.options.get('emulator') or ''}/"
                f"{ep.options.get('project') or ep.host}/{self._d(side, db)}"
                f"/{ep.options.get('namespace') or ''}")

    def _described(self, side, db, table):
        from .. import described
        return described.read(self.CANON_ENGINE, self._where(side, db), table)

    def neutral_columns(self, side, db, table):
        from .couchbase import document_columns
        got = self._described(side, db, table)
        if got is not None:
            return got[0]
        cols = document_columns(self, (side, db, table),
                                lambda: self._bodies(side, db, table),
                                kind=self.kind)
        return [(self.KEY, "string")] + [c for c in cols if c[0] != self.KEY]

    def neutral_key(self, side, db, table):
        got = self._described(side, db, table)
        return got[1] if got is not None else [self.KEY]

    def _classes(self, side, db, table):
        from .. import canon
        return {n: canon.type_class(self.CANON_ENGINE, t) or "json"
                for n, t in self.neutral_columns(side, db, table)}

    def table_facts(self, side, db):
        return {t: {"rows": None, "bytes": None, "key": True}
                for t in self.neutral_tables(side, db)}

    def _row(self, side, db, table, names, ident, body):
        """A document as a row: its identity, then each column - a map, an
        array or a field of several kinds as `to_ext`'s JSON."""
        from .. import described as desc
        got = self._described(side, db, table)
        if got is not None:
            cols, key = got
            classes = dict(cols)
            values = desc.key_values(classes, key, self._key_text(ident))
            for n, cls in cols:
                if n not in key:
                    values[n] = self._described_value(cls, body.get(n))
            return [values.get(n) for n in names]
        kinds = dict(self.neutral_columns(side, db, table))
        out = []
        for n in names:
            if n == self.KEY:
                out.append(ident)
                continue
            v = body.get(n)
            k = kinds.get(n)
            if v is None:
                out.append(None)
            elif k == "timestamp":
                out.append(_nanotime(v))
            elif k in ("reference",):
                out.append(v.path)
            elif k == "key":
                out.append(json.dumps(list(v.flat_path)))
            elif k == "geopoint":
                out.append({"latitude": v.latitude, "longitude": v.longitude})
            elif k == "vector":
                out.append([float(x) for x in v])
            elif k in ("map", "array", "entity", "json"):
                out.append(to_ext(v))
            else:
                out.append(v)
        return out

    @staticmethod
    def _described_value(cls, v):
        """A column of a described table as it was written (`_store`)."""
        from .. import described as desc
        if v is None:
            return None
        if cls in ("integer", "boolean", "text") and not isinstance(v, str):
            return v
        if cls == "float" and isinstance(v, float):
            return v
        if cls == "bytes" and isinstance(v, bytes):
            return v
        if cls == "json" and not isinstance(v, str):
            return from_ext(v)
        return desc.value_of(cls, v)

    def _stored(self, side, db, table, names, row):
        """A row as the document's fields: a described table's columns as
        `_described_value` reads them back; any other's by the kind the
        source's field holds, a same-engine copy keeping each kind."""
        from .. import canon
        from . import same_engine
        got = self._described(side, db, table)
        values = dict(zip(names, row))
        if got is not None:
            cols, key = got
            out = {}
            for n, cls in cols:
                if n in key or n not in values:
                    continue
                v = values[n]
                if v is None or cls in ("integer", "boolean", "text",
                                        "float", "bytes"):
                    out[n] = v if cls != "text" or v is None else str(v)
                elif cls == "json":
                    out[n] = self._from_json(side, db, v)
                else:
                    out[n] = canon.render_value(cls, v)
            return out
        kinds = dict(self.neutral_columns("src", db, table)) \
            if same_engine(self) else {}
        out = {}
        for n, v in values.items():
            if n == self.KEY:
                continue
            out[n] = self._field(side, db, kinds.get(n), v)
        return out

    def _from_json(self, side, db, v):
        if isinstance(v, str):
            v = json.loads(v, parse_float=Decimal)
        return from_ext(v, **self._makers(side, db))

    def _makers(self, side, db):
        return {}

    def _field(self, side, db, kind, v):
        """A value on its way into a field of the kind the source's held."""
        if v is None:
            return None
        if kind in ("map", "array", "entity", "json") or isinstance(
                v, (dict, list)):
            return self._from_json(side, db, v)
        if kind == "timestamp" or isinstance(v, datetime.datetime):
            return v if v.tzinfo else v.replace(
                tzinfo=datetime.timezone.utc)
        if kind == "geopoint" and isinstance(v, dict):
            from google.cloud.firestore_v1 import GeoPoint
            return GeoPoint(float(v["latitude"]), float(v["longitude"]))
        if kind == "vector":
            from google.cloud.firestore_v1.vector import Vector
            return Vector([float(x) for x in v])
        if isinstance(v, Decimal):
            return float(v)
        if isinstance(v, (bytearray, memoryview)):
            return bytes(v)
        return v

    def neutral_create_sql(self, side, db, table, columns, key=()):
        if not key:
            raise SystemExit(f"{table} has no key, and a document is kept by"
                             " its name: a table without one has nowhere to"
                             " go")
        return json.dumps({"collection": table, "key": list(key),
                           "columns": [[c[0], c[1]] for c in columns]})

    def neutral_create(self, side, db, table, columns, key=()):
        """Nothing is made on the service - a collection is there once a
        document is - but a table from another engine is described on this
        machine, so its rows are read back by their columns' classes."""
        from .. import described
        from . import same_engine
        self._target_only(side, "create a table")
        spec = self.neutral_create_sql(side, db, table, columns, key)
        if not same_engine(self):
            described.keep(self.CANON_ENGINE, self._where(side, db), table,
                           [(c[0], c[1]) for c in columns], list(key))
        return spec

    def _key_text(self, ident):
        return ident

    def _ident_of(self, side, db, table, key_values):
        """A row's identity (a path) from its key's values."""
        from .. import described as desc
        got = self._described(side, db, table)
        if got is None:
            return key_values[0]
        cols, key = got
        return self._ident_from_text(table, desc.key_text(
            dict(cols), key, dict(zip(key, key_values))))

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and a"
                             " collection takes none")
        names = [n for n, _ in columns]
        start = (self._ident_of(side, db, table, list(after))
                 if after is not None else None)
        rows = [self._row(side, db, table, names, ident, body)
                for ident, body in self._scan(side, db, table, start, limit)]
        key = self.neutral_key(side, db, table)
        if not rows or not all(k in names for k in key):
            return rows, None
        return rows, tuple(rows[-1][names.index(k)] for k in key)

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and a"
                             " collection takes none")
        names = [n for n, _ in columns]
        batch = []
        for ident, body in self._scan(side, db, table):
            batch.append(self._row(side, db, table, names, ident, body))
            if len(batch) >= size:
                yield batch
                batch = []
        if batch:
            yield batch

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        if not key or not keys:
            return {}
        names = [n for n, _ in columns]
        idents = [self._ident_of(side, db, table, list(k)) for k in keys]
        found = [self._row(side, db, table, names, ident, body)
                 for ident, body in self._get(side, db, table, idents)]
        return self._by_key_map(columns, key, found)

    def neutral_digest(self, side, db, table, columns, where=None):
        from .. import canon
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and a"
                             " collection takes none")
        classes = [c for _, c in columns]
        names = [n for n, _ in columns]

        def one(part):
            total, n, batch = 0, 0, []
            for ident, body in part:
                batch.append(self._row(side, db, table, names, ident, body))
                if len(batch) >= 5000:
                    k, total = canon.fold_rows(classes, batch, total)
                    n, batch = n + k, []
            if batch:
                k, total = canon.fold_rows(classes, batch, total)
                n += k
            return n, total
        parts = self._parts(side, db, table)
        n, total = 0, 0
        with concurrent.futures.ThreadPoolExecutor(
                max(1, min(8, len(parts)))) as pool:
            for k, t in pool.map(one, parts):
                n, total = n + k, total + t
        return n, str(total)

    def _parts(self, side, db, table):
        return [self._scan(side, db, table)]

    def neutral_write(self, side, db, table, columns, rows):
        """Each document set whole - which replaces one there - so a batch
        written again lands on itself."""
        from .couchbase import forget_columns
        self._target_only(side, "write rows")
        if not rows:
            return 0
        forget_columns(self)
        names = [n for n, _ in columns]
        key = self.neutral_key(side, db, table)
        self._put(side, db, table, [
            (self._ident_of(side, db, table,
                            [r[names.index(k)] for k in key]),
             self._stored(side, db, table, names, r)) for r in rows])
        return len(rows)

    def _apply_upserts(self, side, db, table, rows):
        from .couchbase import forget_columns
        forget_columns(self)
        key = self.neutral_key(side, db, table)
        out = []
        for k, values in rows:
            row = {**values, **k}
            names = list(row)
            out.append((self._ident_of(side, db, table, [row[c] for c in key]),
                        self._stored(side, db, table, names,
                                     list(row.values()))))
        self._put(side, db, table, out)

    def _apply_upsert(self, side, db, table, key, values):
        self._apply_upserts(side, db, table, [(key, values)])

    def _apply_deletes(self, side, db, table, keys):
        from .couchbase import forget_columns
        forget_columns(self)
        pk = self.neutral_key(side, db, table)
        self._remove(side, db, table, [
            self._ident_of(side, db, table, [k[c] for c in pk]) for k in keys])

    def _apply_delete(self, side, db, table, key):
        self._apply_deletes(side, db, table, [key])

    def neutral_empty(self, side, db, table, where=None):
        from .couchbase import forget_columns
        self._target_only(side, "empty a collection")
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and a"
                             " collection takes none")
        forget_columns(self)
        idents = [ident for ident, _ in self._scan(side, db, table)]
        self._remove(side, db, table, idents)
        return len(idents)

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

    # ---- the database's settings, indexes and time to live -----------------
    #
    # Both modes keep them in the one admin API: a database's settings, its
    # composite indexes and the fields a time-to-live policy deletes
    # documents by. The emulator serves none of it, and says so.

    def _admin(self, side):
        held = self.__dict__.setdefault("_admins", {})
        if side not in held:
            from google.cloud import firestore_admin_v1

            from . import gcp
            held[side] = firestore_admin_v1.FirestoreAdminClient(
                credentials=gcp.credentials(self._ep(side)))
        return held[side]

    def _db_name(self, side, db):
        from . import gcp
        return (f"projects/{gcp.project(self._ep(side))}/databases/"
                f"{self._d(side, db) or '(default)'}")

    def _indexes(self, side, db):
        """{(group, scope, its fields): state} of the composite indexes of
        the collection groups in scope."""
        out = {}
        for ix in self._admin(side).list_indexes(
                parent=f"{self._db_name(side, db)}/collectionGroups/-"):
            group = ix.name.split("/collectionGroups/", 1)[1].split("/")[0]
            if not self.in_scope(db, group):
                continue
            fields = tuple(
                (f.field_path, f.order.name if f.order else
                 f.array_config.name if f.array_config else "vector")
                for f in ix.fields)
            out[(group, ix.query_scope.name, fields)] = ix.state.name
        return out

    def _ttl_fields(self, side, db):
        """{group.field: its time-to-live policy's state} of the fields a
        policy deletes documents by, in scope."""
        out = {}
        for f in self._admin(side).list_fields(
                parent=f"{self._db_name(side, db)}/collectionGroups/-",
                filter="ttlConfig:*"):
            group, _, field = f.name.split("/collectionGroups/", 1)[1] \
                .partition("/fields/")
            if self.in_scope(db, group):
                out[f"{group}.{field}"] = f.ttl_config.state.name
        return out

    #: what changes what is kept, how it is read and what deletes it
    CRITICAL_PARAMS = ("type", "concurrency_mode",
                       "point_in_time_recovery_enablement")

    def _settings(self, side, db):
        try:
            d = self._admin(side).get_database(name=self._db_name(side, db))
            out = {k: getattr(getattr(d, k), "name", str(getattr(d, k)))
                   for k in ("type", "concurrency_mode",
                             "point_in_time_recovery_enablement",
                             "app_engine_integration_mode",
                             "delete_protection_state", "location_id")}
            out.update({f"{k} ttl": v for k, v in
                        self._ttl_fields(side, db).items()})
        except Exception as e:  # noqa: BLE001 - said, as the error
            return {self.UNREADABLE: str(e).strip().splitlines()[0][:120]}
        return out

    def check_params(self, db):
        """The database's own settings - its mode, how it holds
        transactions, its point-in-time recovery - and every field a
        time-to-live policy deletes documents by, both sides: a target
        without the source's policy keeps what the source deletes, and one
        with a policy the source lacks deletes what it keeps."""
        src, dst = self._settings("src", db), self._settings("dst", db)
        critical = self.CRITICAL_PARAMS + tuple(
            k for k in set(src) | set(dst) if k.endswith(" ttl"))
        return self._param_result(db, src, dst, critical,
                                  "give the target the source's settings and"
                                  " time-to-live policies")

    def check_deep(self, db):
        """Every composite index of the source's on the target, over the
        same fields in the same order, and ready: a query the source
        answers from one is refused by a target without it."""
        try:
            src, dst = self._indexes("src", db), self._indexes("dst", db)
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("deep", f"{db} indexes", "error",
                           "could not read the indexes:"
                           f" {str(e).strip().splitlines()[0][:90]}")]
        lacking = [k for k in src if k not in dst]
        building = [k for k, state in dst.items()
                    if k in src and state != "READY"]

        def said(k):
            return f"{k[0]}({', '.join(f for f, _ in k[2])})"
        return [Result(
            "deep", f"{db} indexes", "diff" if lacking or building else "ok",
            (f"{len(lacking)} of the source's indexes are not on the target:"
             f" {', '.join(said(k) for k in lacking[:4])}" if lacking else
             "indexes not ready on the target: " + ", ".join(
                 f"{said(k)} {dst[k]}" for k in building[:4]) if building
             else f"every one of the source's {len(src)} indexes is on the"
                  " target, ready"), "",
            "create them on the target before the cutover" if lacking
            else "wait for them to be built" if building else "")]

    # ---- a table by the service's own export and import ---------------------

    #: how long an export or an import is waited for
    EXPORT_WAIT = 6 * 3600

    def _bucket(self):
        """The bucket prefix the hop gives the service's export, or None."""
        got = str((self.hop.options or {}).get("export_bucket") or "")
        return got.rstrip("/") if got.startswith("gs://") else None

    def _export_unfit(self, db, table):
        """Why the service's own export and import cannot carry the table,
        or None."""
        from . import same_engine
        if not same_engine(self):
            return "the target is another engine"
        if self._bucket() is None:
            return ("the hop names no bucket the export is written to"
                    " (export_bucket: gs://bucket/prefix)")
        if any(self._ep(s).options.get("emulator") for s in ("src", "dst")):
            return "the emulator exports nothing"
        if self._described("src", db, table) is not None:
            return ("migkit made the table from another engine's rows, and"
                    " describes it here")
        return None

    def _where_to(self, db, table):
        safe = "".join(c if c.isalnum() or c in "-_" else "-" for c in
                       f"{self.hop.name}-{db}-{table}")
        return f"{self._bucket()}/{safe}-{time.strftime('%Y%m%d%H%M%S')}"

    def _held_to(self, side, db, table):
        """The database, the collection group or kind, and - in Datastore
        mode - the namespace an export or an import is held to: both modes
        are exported by the one admin API."""
        out = {"name": self._db_name(side, db), "collection_ids": [table]}
        space = self._ep(side).options.get("namespace")
        if space:
            out["namespace_ids"] = [space]
        return out

    def _exported(self, db, table):
        """The collection group or kind exported by the service as it
        stands, under a prefix of its own; the prefix."""
        got = self._admin("src").export_documents(request={
            **self._held_to("src", db, table),
            "output_uri_prefix": self._where_to(db, table)}).result(
            self.EXPORT_WAIT)
        return got.output_uri_prefix

    def _imported(self, db, table, uri):
        self._admin("dst").import_documents(request={
            **self._held_to("dst", db, table),
            "input_uri_prefix": uri}).result(self.EXPORT_WAIT)

    def _table_ways(self):
        """How a table goes (`tableway.each`): the service's own export of
        it, imported into the emptied target, where the hop names a bucket
        for it; by the table copier always."""
        from ..decide import Need, Rung
        return (
            Rung("export", "the service's own export, imported into the"
                           " target",
                 needs=(Need("a table the export carries",
                             lambda f: f["unfit"] is None,
                             lambda f: f["unfit"]),),
                 because="every document as the service keeps it, read and"
                         " written by the service"),
            Rung("copier", "the table copier, by key ranges",
                 because="every document read and written by migkit"),
        )

    def native_bulk(self, db, tables, go, log, shape_only=()):
        """Each collection carried by the service's own export and import
        through the bucket the hop names, the target's emptied first, where
        it can; by the table copier otherwise. The check that follows
        compares them."""
        steps = [f"{t}: exported and imported by the service, or copied by"
                 " key ranges" for t in tables]
        if not go:
            return steps
        from .. import tableway
        from ..decide import Facts
        held = []

        def run(way, t):
            if way != "export":
                return self.copy_by_copier(db, t, log, held)
            uri = self._exported(db, t)
            if t in self.neutral_tables("dst", db):
                self.neutral_empty("dst", db, t)
            self._imported(db, t, uri)
            if log:
                log(f"{t}: exported and imported by the service ({uri})")
            return None
        tableway.each(self, db, tables, self._table_ways(),
                      lambda t: Facts(unfit=lambda t=t: self._export_unfit(
                          db, t)), run, log)
        return steps

    def snapshot_state(self, db, state_dir, kind="all"):
        """The moment the target stood at before a repair, with whether
        point-in-time recovery keeps it seven days - a copy can be made of
        the database as of then (`cloudapi`) - or only an hour, which is
        said."""
        out = {"at": datetime.datetime.now(datetime.timezone.utc)
               .strftime("%Y-%m-%dT%H:%M:%SZ")}
        try:
            d = self._admin("dst").get_database(
                name=self._db_name("dst", db))
            pitr = d.point_in_time_recovery_enablement.name
            out["kept"] = ("seven days" if pitr.endswith("_ENABLED")
                           else "an hour")
        except Exception as e:  # noqa: BLE001 - kept as the reason
            out["no_point"] = str(e).strip().splitlines()[0][:160]
        (state_dir / "dst-firestore.json").write_text(
            json.dumps(out, indent=2, sort_keys=True))


class FirestoreEngine(_Documents):
    CANON_ENGINE = "firestore"
    READS_AHEAD = False

    kind = staticmethod(fs_kind)

    def _client(self, side, db):
        held = self.__dict__.setdefault("_clients", {})
        at = (side, self._d(side, db))
        if at not in held:
            from google.cloud import firestore

            from . import gcp
            ep = self._ep(side)
            held[at] = gcp.client(firestore.Client, "FIRESTORE_EMULATOR_HOST",
                                  ep, project=gcp.project(ep),
                                  database=at[1])
        return held[at]

    def _groups(self, side, db):
        said = self._ep(side).options.get("collection_groups") or []
        if isinstance(said, str):
            said = [s.strip() for s in said.split(",") if s.strip()]
        top = [c.id for c in self._client(side, db).collections()]
        return sorted(set(top) | set(said))

    def neutral_tables(self, side, db):
        return [t for t in self._groups(side, db)
                if self.in_scope(db, t)]

    def _makers(self, side, db):
        client = self._client(side, db)
        return {"ref": client.document}

    def _field(self, side, db, kind, v):
        if kind == "reference" and isinstance(v, str):
            # a reference stays one: its path's text is another kind
            return self._client(side, db).document(v)
        return super()._field(side, db, kind, v)

    def _ident_from_text(self, table, text):
        return f"{table}/{text}"

    def _key_text(self, ident):
        return ident.split("/")[-1]

    def _query(self, side, db, table):
        from google.cloud.firestore_v1.field_path import FieldPath
        return self._client(side, db).collection_group(table).order_by(
            FieldPath.document_id())

    def _bodies(self, side, db, table):
        for snap in self._client(side, db).collection_group(table).stream():
            yield snap.to_dict() or {}

    def _scan(self, side, db, table, start=None, limit=None, query=None):
        from google.cloud.firestore_v1.field_path import FieldPath
        q = query if query is not None else self._query(side, db, table)
        if start is not None:
            q = q.start_after({FieldPath.document_id():
                               self._client(side, db).document(start)})
        if limit:
            q = q.limit(int(limit))
        for snap in q.stream():
            yield snap.reference.path, snap.to_dict() or {}

    def _parts(self, side, db, table):
        """The group's partitions as the service splits it, each read on
        its own."""
        group = self._client(side, db).collection_group(table)
        return [self._scan(side, db, table, query=p.query())
                for p in group.get_partitions(8)]

    def _get(self, side, db, table, idents):
        client = self._client(side, db)
        for snap in client.get_all([client.document(i) for i in idents]):
            if snap.exists:
                yield snap.reference.path, snap.to_dict() or {}

    def _put(self, side, db, table, docs):
        client = self._client(side, db)
        failed = []

        def on_error(error, writer):
            if error.attempts >= 10:
                failed.append(error)
                return False
            return True
        bw = client.bulk_writer()
        bw.on_write_error(on_error)
        for ident, body in docs:
            bw.set(client.document(ident), body)
        bw.close()
        if failed:
            raise SystemExit(f"{table}: the target refused {len(failed)}"
                             f" documents: {str(failed[0].message)[:150]}")

    def _remove(self, side, db, table, idents):
        client = self._client(side, db)
        failed = []

        def on_error(error, writer):
            if error.attempts >= 10:
                failed.append(error)
                return False
            return True
        bw = client.bulk_writer()
        bw.on_write_error(on_error)
        for ident in idents:
            bw.delete(client.document(ident))
        bw.close()
        if failed:
            raise SystemExit(f"{table}: the target refused {len(failed)}"
                             " deletes")

    # ---- who it is -----------------------------------------------------------

    def server_identity(self, side, db):
        ep = self._ep(side)
        return {"name": f"{ep.options.get('project') or ep.host}/"
                        f"{self._d(side, db)}",
                "id": f"{ep.options.get('emulator') or 'firestore'}:"
                      f"{ep.options.get('project') or ep.host}/"
                      f"{self._d(side, db)}"}

    def stream_identity(self, side, db):
        return self.server_identity(side, db)

    # ---- the listener ----------------------------------------------------------

    def _follower(self, db):
        from .. import changetopic
        held = self.__dict__.setdefault("_followers", {})
        if db not in held:
            tables = self.neutral_tables("src", db)
            made = [t for t in tables
                    if self._described("src", db, t) is not None]
            if made:
                raise SystemExit(
                    f"{db}: {', '.join(made[:4])} were made by migkit from"
                    " another engine's rows, and are followed from that"
                    " engine, not from here")
            held[db] = changetopic.Follower(
                _Listened(), listening(self, db, tables),
                name=lambda row: row.table,
                shape=lambda t, row, db=db: self._shape(db, t, row))
        return held[db]

    def _shape(self, db, table, row):
        names = [n for n, _ in self.neutral_columns("src", db, table)]
        if row.op == "delete":
            return {NAME: row.key[NAME]}, None
        values = dict(zip(names, self._row("src", db, table, names,
                                           row.key[NAME], row.values)))
        return {NAME: row.key[NAME]}, values

    def change_point(self, side, db):
        return self._follower(db).point()

    def copy_point(self, side, db):
        return self.change_point(side, db)

    def log_position(self, side, db):
        """Now, as the follow's fence: this machine's clock past the
        slack, for every group."""
        now = time.time_ns() + CLOCK_SLACK * 10 ** 9
        return {"at": {t: {"at": now} for t in self.neutral_tables(side, db)},
                "fence": None}

    @staticmethod
    def position_reached(have, want):
        """Every group's listener handed on a snapshot consistent as of a
        time at or past the fence's."""
        if not have or not want:
            return None
        for lane, pos in (want.get("at") or {}).items():
            got = (have.get("at") or {}).get(lane)
            if not got or int(got.get("at", 0)) < int(pos.get("at", 0)):
                return False
        return True

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Each document the listeners say was added, changed or removed,
        in the order of their snapshots."""
        if token is None:
            return [], self.change_point(side, db)
        return self._follower(db).changes(token, limit)

    def release_changes(self):
        for f in self.__dict__.pop("_followers", {}).values():
            f.close()

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

    def _compare_pks(self, db, table, keys):
        return self._as_pair()._compare_pks(db, table, keys)

    def _write_pk_files(self, db, table, missing, extra, changed):
        return self._as_pair()._write_pk_files(db, table, missing, extra,
                                                changed)

    def reach_items(self, side):
        items = [{"level": "pass", "scope": "reach",
                  "item": "copied and verified",
                  "detail": "collection groups read by the service's own"
                            " partitions, documents set whole, each field's"
                            " kind kept"}]
        if side == "src":
            items.append({"level": "warn", "scope": "reach",
                          "item": "changes followed by a listener",
                          "detail": _Listened.LOSES})
        return items

    def _assess_extra(self):
        return self.reach_items("src")


class _Listened:
    """What a listener hands on: a document added or changed with its
    fields, or removed, at its snapshot's read time; a snapshot's read
    time with nothing changed is a mark."""
    name = "firestore-listener"
    from ..changetopic import LANE as ORDER
    READ_BACK = False
    LOSES = ("the follow holds while this process holds its listeners;"
             " begun again from a saved position, it writes every document"
             " of each group again and deletes what the source no longer"
             " holds - equal again, at the price of a copy")

    def decode(self, key, value, topic=None):
        from ..changetopic import Row
        if not value or value.get("kind") == "mark":
            return []
        return [Row(None, topic, value["op"], {NAME: value["path"]},
                    value.get("body"), int(value["at"]))]


#: the listeners this process holds, by the source and groups they
#: listen to: kept past the engine that began them - each pair a hop is
#: carried by is made anew - for as long as the process runs
_LISTENING = {}
_LISTENING_LOCK = threading.Lock()


def listening(engine, db, tables):
    """This process's listeners on these groups of this source, begun
    where there are none yet."""
    ep = engine.hop.source
    at = (str(ep.options.get("emulator") or ""), str(
        ep.options.get("project") or ep.host), engine._d("src", db),
        tuple(tables))
    with _LISTENING_LOCK:
        if at not in _LISTENING:
            _LISTENING[at] = ListenerLanes(engine, db, tables)
        return _LISTENING[at]


#: seconds a follow's fence stands past this machine's clock: a snapshot
#: consistent as of a later time holds every write this machine saw end
#: before it, however far the two clocks are apart within that
CLOCK_SLACK = 2


class ListenerLanes:
    """A listener on each collection group, begun when the follow's point
    is taken and kept for as long as this process follows. Each snapshot
    the listener makes consistent - with changes or without - is a mark
    at its read time (the listener's own `push`, which calls back only
    where something changed, is watched for it). A lane's position is its
    last such read time, in nanoseconds; a lane this process holds no
    listener for (a follow begun again) is read from a new listener's first
    snapshot, which holds every document - each written, and every
    document of the target's the snapshot lacks deleted."""
    KIND = "firestore"

    def __init__(self, engine, db, tables):
        self.engine, self.db, self.tables = engine, db, tables
        self.queue = queue.Queue()
        self.watches = {}
        self.began = set()
        self.lock = threading.Lock()

    def _listen(self, table, before_copy=False):
        """A listener on one group. Its first snapshot is the whole group:
        begun by the follow's point, before the copy, it is what the copy
        reads and is not carried; begun on a follow's restart, it is
        carried whole."""
        from . import same_engine
        from .spanner import ns_of
        if not before_copy and not same_engine(self.engine):
            raise SystemExit(
                f"{table}: a Firestore listener cannot be resumed by another"
                " process, and the documents the target holds that the"
                " source no longer does can be found only on a Firestore"
                " target. Move the group again (full and follow) in one"
                " run")
        client = self.engine._client("src", self.db)
        ready = threading.Event()

        def on(snapshot, changes, read_time):
            at = ns_of(read_time)
            with self.lock:
                first = table not in self.began
                self.began.add(table)
            if first and not before_copy:
                paths = set()
                for snap in snapshot:
                    paths.add(snap.reference.path)
                    self.queue.put((table, at, {
                        "op": "upsert", "path": snap.reference.path,
                        "body": snap.to_dict() or {}, "at": at}))
                for ident, _ in self.engine._scan("dst", self.db, table):
                    if ident not in paths:
                        self.queue.put((table, at, {
                            "op": "delete", "path": ident, "at": at}))
            elif not first:
                for ch in changes:
                    kind = str(ch.type.name).upper()
                    self.queue.put((table, at, {
                        "op": "delete" if kind == "REMOVED" else "upsert",
                        "path": ch.document.reference.path,
                        "body": None if kind == "REMOVED"
                        else ch.document.to_dict() or {}, "at": at}))
            ready.set()
        watch = client.collection_group(table).on_snapshot(on)
        own = watch.push

        def push(read_time, next_resume_token):
            got = own(read_time, next_resume_token)
            if read_time is not None:
                self.queue.put((table, ns_of(read_time), None))
            return got
        watch.push = push
        self.watches[table] = watch
        if not ready.wait(120):
            raise SystemExit(f"{table}: the listener sent no first snapshot"
                             " in two minutes")

    def lanes(self):
        return list(self.tables)

    def ends(self):
        """Now: a listener on each group, begun here before the copy, and
        this machine's clock past the slack its first snapshot is held
        to."""
        for t in self.tables:
            if t not in self.watches:
                self._listen(t, before_copy=True)
        now = time.time_ns() + CLOCK_SLACK * 10 ** 9
        return {t: {"at": now} for t in self.tables}

    def _drain(self):
        out = []
        while True:
            try:
                out.append(self.queue.get_nowait())
            except queue.Empty:
                return out

    def read(self, at, most, seconds):
        for t in self.tables:
            if t not in self.watches:
                self._listen(t)
        stop = time.monotonic() + seconds
        out, nxt = [], dict(at or {})
        while time.monotonic() < stop and len(out) < most:
            got = self._drain()
            for table, when, rec in got:
                was = int((nxt.get(table) or {}).get("at", 0))
                if rec is not None:
                    out.append((table, {"at": when}, None, rec, table))
                nxt[table] = {"at": max(was, when)}
            if not got:
                time.sleep(0.2)
            elif self.queue.empty():
                break
        return out, nxt, self.queue.empty()

    def lost(self, at):
        return None

    def room(self, at):
        return None

    def close(self):
        for w in self.watches.values():
            try:
                w.unsubscribe()
            except Exception:  # noqa: BLE001 - closing what is going
                pass
        self.watches.clear()


class DatastoreEngine(_Documents):
    CANON_ENGINE = "datastore"
    KEY = "__key__"

    kind = staticmethod(ds_kind)

    def _client(self, side, db):
        held = self.__dict__.setdefault("_clients", {})
        at = (side, self._d(side, db))
        if at not in held:
            from google.cloud import datastore

            from . import gcp
            ep = self._ep(side)
            kw = {"project": gcp.project(ep),
                  "namespace": ep.options.get("namespace") or None}
            if at[1] and at[1] != "(default)":
                kw["database"] = at[1]
            held[at] = gcp.client(datastore.Client,
                                  "DATASTORE_EMULATOR_HOST", ep, **kw)
        return held[at]

    def neutral_tables(self, side, db):
        client = self._client(side, db)
        q = client.query(kind="__kind__")
        q.keys_only()
        return sorted(e.key.name for e in q.fetch()
                      if e.key.name and not e.key.name.startswith("__")
                      and self.in_scope(db, e.key.name))

    def _makers(self, side, db):
        client = self._client(side, db)
        return {"key": lambda path: client.key(*path)}

    def _key(self, side, db, ident):
        return self._client(side, db).key(*json.loads(ident))

    def _ident_from_text(self, table, text):
        return json.dumps([table, text])

    def _key_text(self, ident):
        return json.loads(ident)[-1]

    def _bodies(self, side, db, table):
        for e in self._client(side, db).query(kind=table).fetch():
            yield dict(e)

    def _scan(self, side, db, table, start=None, limit=None):
        from google.cloud.datastore.query import PropertyFilter
        client = self._client(side, db)
        q = client.query(kind=table, order=["__key__"])
        if start is not None:
            q.add_filter(filter=PropertyFilter(
                "__key__", ">", self._key(side, db, start)))
        for e in q.fetch(limit=int(limit) if limit else None):
            yield json.dumps(list(e.key.flat_path)), dict(e)

    def _get(self, side, db, table, idents):
        client = self._client(side, db)
        idents = list(idents)
        for i in range(0, len(idents), 1000):
            for e in client.get_multi([self._key(side, db, x)
                                       for x in idents[i:i + 1000]]):
                yield json.dumps(list(e.key.flat_path)), dict(e)

    def _put(self, side, db, table, docs):
        """Entities by `put_multi`, 500 a call; a text or bytes past what
        the service indexes, and an embedded entity, kept out of the
        indexes, as the service requires."""
        from google.cloud import datastore
        client = self._client(side, db)
        out = []
        for ident, body in docs:
            unindexed = tuple(
                n for n, v in body.items()
                if isinstance(v, (datastore.Entity, dict))
                or (isinstance(v, (str, bytes)) and len(
                    v.encode("utf-8") if isinstance(v, str) else v)
                    > INDEXED_MOST))
            e = datastore.Entity(key=self._key(side, db, ident),
                                 exclude_from_indexes=unindexed)
            e.update({n: self._entity_value(client, v)
                      for n, v in body.items()})
            out.append(e)
        for i in range(0, len(out), PUT):
            client.put_multi(out[i:i + PUT])

    @staticmethod
    def _entity_value(client, v):
        from google.cloud import datastore
        if isinstance(v, dict) and not isinstance(v, datastore.Entity):
            e = datastore.Entity()
            e.update({k: DatastoreEngine._entity_value(client, x)
                      for k, x in v.items()})
            return e
        if isinstance(v, list):
            return [DatastoreEngine._entity_value(client, x) for x in v]
        return v

    def _field(self, side, db, kind, v):
        if kind == "key" and isinstance(v, str):
            return self._client(side, db).key(*json.loads(v))
        if kind == "geopoint" and isinstance(v, dict):
            from google.cloud.datastore.helpers import GeoPoint
            return GeoPoint(float(v["latitude"]), float(v["longitude"]))
        return super()._field(side, db, kind, v)

    def _remove(self, side, db, table, idents):
        client = self._client(side, db)
        idents = list(idents)
        for i in range(0, len(idents), PUT):
            client.delete_multi([self._key(side, db, x)
                                 for x in idents[i:i + PUT]])

    def server_identity(self, side, db):
        ep = self._ep(side)
        return {"name": f"{ep.options.get('project') or ep.host}/"
                        f"{self._d(side, db)}",
                "id": f"{ep.options.get('emulator') or 'datastore'}:"
                      f"{ep.options.get('project') or ep.host}/"
                      f"{self._d(side, db)}/"
                      f"{ep.options.get('namespace') or ''}"}

    def reach_items(self, side):
        items = [{"level": "pass", "scope": "reach",
                  "item": "copied and verified",
                  "detail": "kinds read in key order, entities put 500 a"
                            " call, each value's kind kept"}]
        if side == "src":
            items.append({"level": "warn", "scope": "reach",
                          "item": "changes are not followed",
                          "detail": "Datastore mode has no change feed and no"
                                    " listener: move it while nothing writes,"
                                    " and prove it with migkit check"})
        return items

    def _assess_extra(self):
        return self.reach_items("src")


def _register():
    """What of the SDKs is used, read off this file, and why the rest is
    not (`tooluse`)."""
    from .. import tooluse
    unused = tooluse.UNUSED
    tooluse.register("google-cloud-firestore", licence="Apache-2.0", reads=(
        ("engines/firestore.py",),
        {("Query", "stream"): "stream",
         ("Query", "start_after"): "start_after",
         ("Query", "on_snapshot"): "on_snapshot",
         ("CollectionGroup", "get_partitions"): "get_partitions",
         ("Client", "collection_group"): "collection_group",
         ("Client", "collections"): "collections",
         ("Client", "get_all"): "get_all",
         ("Client", "bulk_writer"): "bulk_writer",
         ("BulkWriter", "on_write_error"): "on_write_error",
         ("BulkWriter", "set"): "set",
         ("BulkWriter", "delete"): "delete",
         ("FirestoreAdminClient", "get_database"): "get_database",
         ("FirestoreAdminClient", "list_indexes"): "list_indexes",
         ("FirestoreAdminClient", "list_fields"): "list_fields",
         ("FirestoreAdminClient", "export_documents"): "export_documents",
         ("FirestoreAdminClient", "import_documents"): "import_documents"}),
        options={
        ("Client", "recursive_delete"): (
            unused, "it deletes the documents under each document too, where"
                    " a table is one collection group"),
        ("Client", "batch"): (
            unused, "the bulk writer sends the same writes side by side with"
                    " its own retries, where a batch is one at a time"),
        ("Query", "read_time"): (
            unused, "a read at a past time reaches back an hour, or seven days"
                    " with point-in-time recovery on: a copy longer than that"
                    " could not be read at one time"),
    })
    tooluse.register("google-cloud-datastore", licence="Apache-2.0", reads=(
        ("engines/firestore.py",),
        {("Client", "put_multi"): "put_multi",
         ("Client", "get_multi"): "get_multi",
         ("Client", "delete_multi"): "delete_multi",
         ("Query", "keys_only"): "keys_only",
         ("Query", "add_filter"): "add_filter",
         ("Entity", "exclude_from_indexes"): "exclude_from_indexes"}),
        options={
        ("Client", "transaction"): (
            unused, "a copy puts whole entities, which land on themselves"
                    " again; a transaction would hold a group's writes for"
                    " nothing"),
        ("Query", "run_aggregation_query"): (
            unused, "a count alone proves nothing a digest of every entity"
                    " does not"),
    })


_register()
