"""Couchbase as a side of a pair (backlog ER4), through its Python SDK.

A hop's database is a bucket, and a table a collection, named
`scope.collection` (one made from another side's table, which the pair
names by its name alone, in the bucket's `_default` scope). A document
is a row: its key, and its body's top-level
fields as columns. A collection migkit did not make is described by the
JSON kinds its documents hold, read from all of them (a field holding more
than one kind is JSON, compared with its kind), and keyed by its document
key as the column `_key`. One migkit made keeps each column's class and
its key's columns beside the endpoint on this machine (as a DynamoDB table
made without tags does), so a decimal comes back at its scale from the
text it was written as, a time from its ISO text and bytes from base64 -
and its document key is its key's value (a key of several columns, their
values as one JSON array).

Rows are read by the key-value service's own range scan - no query
service, no index - in one pass (it returns no order to resume by), and
written by `upsert_multi`, which replaces a document of the same key: a
batch written again lands on itself.

Its changes come through Couchbase's Kafka connector, published with its
default source handler (each change's vBucket, UUID, sequence number and
body), read by the change-topic reader (`changetopic`): a vBucket's
changes are in order in one partition, and one read twice is dropped by
its sequence number. The fence is the server's own: each active
vBucket's high sequence number, asked of each data node in memcached's
binary protocol (`STAT vbucket-seqno`), which the connector publishes
up to and the tail has read up to.
"""
import base64
import datetime
import hashlib
import hmac
import json
import os
import socket
import struct
from decimal import Decimal

from .base import Engine, NeutralCopier, Result

#: documents written by one `upsert_multi`
WRITE_BATCH = 500
#: the field a document's key is read into, for a collection migkit did
#: not make
KEY = "_key"
#: memcached's binary protocol, as far as the fence needs it
MAGIC_REQ, MAGIC_RES = 0x80, 0x81
STAT, SASL_LIST, SASL_AUTH, SASL_STEP, SELECT_BUCKET = (
    0x10, 0x20, 0x21, 0x22, 0x89)
AUTH_CONTINUE = 0x21


class CouchbaseEngine(NeutralCopier, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "couchbase"
    FOLDED_BECAUSE = "has no way to hash documents in the service"
    OVER_NETWORK = True
    RESUMES_BY_KEY = False
    CREATES_ON_WRITE = False
    CHANGE_POINT_READS_ONLY = True
    READS_AHEAD = False

    # ---- signing in ------------------------------------------------------

    def _ep(self, side):
        return self.hop.source if side == "src" else self.hop.target

    def _cluster(self, side):
        held = self.__dict__.setdefault("_clusters", {})
        if side not in held:
            from couchbase.auth import PasswordAuthenticator
            from couchbase.cluster import Cluster
            from couchbase.options import ClusterOptions
            ep = self._ep(side)
            url = ep.options.get("url")
            if not url:
                scheme = "couchbases" if ep.options.get("tls") else "couchbase"
                url = f"{scheme}://{ep.host}" + (f":{ep.port}" if ep.port
                                                 else "")
            extra = {}
            if ep.options.get("network"):
                extra["network"] = str(ep.options["network"])
            if ep.options.get("tls_insecure"):
                extra["tls_verify"] = "none"
            elif ep.options.get("tls_ca_file"):
                extra["trust_store_path"] = str(ep.options["tls_ca_file"])
            if extra:
                url += ("&" if "?" in url else "?") + "&".join(
                    f"{k}={v}" for k, v in extra.items())
            cluster = Cluster(url, ClusterOptions(PasswordAuthenticator(
                ep.user, ep.password)))
            cluster.wait_until_ready(datetime.timedelta(seconds=30))
            held[side] = cluster
        return held[side]

    def _bucket(self, side, db):
        return self._cluster(side).bucket(self.hop.target_db(db)
                                          if side == "dst" else db)

    def _collection(self, side, db, table):
        scope, coll = self._split(table)
        return self._bucket(side, db).scope(scope).collection(coll)

    @staticmethod
    def _split(table):
        scope, _, coll = str(table).rpartition(".")
        return scope or "_default", coll

    def _rest(self, side, path):
        """The cluster manager's answer to GET `path`, as JSON."""
        import base64 as b64
        import ssl
        import urllib.request
        ep = self._ep(side)
        tls = bool(ep.options.get("tls"))
        port = ep.options.get("mgmt_port") or (18091 if tls else 8091)
        req = urllib.request.Request(
            f"{'https' if tls else 'http'}://{ep.host}:{port}{path}",
            headers={"Authorization": "Basic " + b64.b64encode(
                f"{ep.user}:{ep.password}".encode()).decode()})
        ctx = None
        if tls:
            ctx = ssl.create_default_context(
                cafile=ep.options.get("tls_ca_file") or None)
            if ep.options.get("tls_insecure"):
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(req, timeout=15, context=ctx) as got:
            return json.loads(got.read())

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        return sorted(b["name"] for b in self._rest("src",
                                                    "/pools/default/buckets"))

    # ---- what a collection is ----------------------------------------------

    def neutral_tables(self, side, db):
        out = []
        for scope in self._bucket(side, db).collections().get_all_scopes():
            if scope.name.startswith("_system"):
                continue
            for c in scope.collections:
                name = f"{scope.name}.{c.name}"
                if not self.hop.excluded(db, scope.name, c.name) and \
                        not self.hop.excluded(db, name):
                    out.append(name)
        return sorted(out)

    def _kept_here(self, side):
        """Descriptions of the collections migkit made, one file an
        endpoint, whichever hop made them."""
        from pathlib import Path

        from .. import config
        ep = self._ep(side)
        where = str(ep.options.get("url") or f"{ep.host}:{ep.port}")
        d = Path(config.REPORTS) / "_couchbase"
        d.mkdir(parents=True, exist_ok=True)
        return d / (hashlib.sha1(where.encode(), usedforsecurity=False)
                    .hexdigest()[:16] + ".json")

    def _described(self, side, db, table):
        """(columns, key) of a collection migkit made, or None."""
        scope, coll = self._split(table)
        name = f"{self._d(side, db)}.{scope}.{coll}"
        try:
            got = json.loads(self._kept_here(side).read_text()).get(name)
        except (OSError, ValueError):
            got = None
        if got:
            return [tuple(c) for c in got["columns"]], list(got["key"])
        return None

    def _d(self, side, db):
        return self.hop.target_db(db) if side == "dst" else db

    def _scan(self, side, db, table, ids_only=False):
        from couchbase.kv_range_scan import RangeScan
        from couchbase.options import ScanOptions
        return self._collection(side, db, table).scan(
            RangeScan(), ScanOptions(ids_only=ids_only,
                                     batch_item_limit=500))

    def neutral_columns(self, side, db, table):
        described = self._described(side, db, table)
        if described is not None:
            return described[0]
        return [(KEY, "string")] + document_columns(
            self, (side, db, table),
            lambda: (got.content_as[dict]
                     for got in self._scan(side, db, table)))

    def neutral_key(self, side, db, table):
        described = self._described(side, db, table)
        return described[1] if described is not None else [KEY]

    def _classes(self, side, db, table):
        from .. import canon
        return {n: canon.type_class("couchbase", t) or "json"
                for n, t in self.neutral_columns(side, db, table)}

    # ---- values ------------------------------------------------------------

    def _doc_id(self, key, classes, values):
        """The document key of a row of a collection migkit made: its key's
        value, or the values of several as one JSON array."""
        from .. import canon
        texts = [canon.render_value(classes.get(k) or "text", values[k])
                 if not isinstance(values[k], str) else values[k]
                 for k in key]
        return texts[0] if len(texts) == 1 else json.dumps(texts)

    def _key_of_id(self, doc_id, key, classes):
        """A row's key back from its document key (`_doc_id`)."""
        from .. import changetopic
        texts = [doc_id] if len(key) == 1 else json.loads(doc_id)
        return {k: changetopic.from_text(classes.get(k) or "text", t,
                                         "base64")
                for k, t in zip(key, texts)}

    @staticmethod
    def _to_json(cls, value):
        from .. import canon
        if value is None:
            return None
        if cls == "decimal":
            return canon.render_value("decimal", value)
        if cls in ("timestamp", "date", "time"):
            return canon.render_value(cls, value)
        if cls == "bytes":
            return base64.b64encode(bytes(value)).decode()
        if cls == "uuid":
            return str(value)
        if cls == "json" and isinstance(value, str):
            try:
                value = json.loads(value)
            except ValueError:
                return value
        return _plain(value)

    @staticmethod
    def _from_json(cls, value):
        from .. import changetopic
        if value is None:
            return None
        if cls in ("decimal", "bytes", "date", "timestamp", "time") and \
                isinstance(value, str):
            return changetopic.from_text(cls, value, "base64")
        if cls == "integer" and isinstance(value, float) and \
                value.is_integer():
            return int(value)
        return value

    def _row(self, names, classes, doc_id, body):
        body = body if isinstance(body, dict) else {}
        return [doc_id if n == KEY and n not in body else
                self._from_json(classes.get(n), body.get(n)) for n in names]

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        if where:
            raise SystemExit(f"{table} moves under a row filter, which is"
                             " SQL, and a collection is read by its keys")
        classes = self._classes(side, db, table)
        names = [n for n, _ in columns]
        batch = []
        for got in self._scan(side, db, table):
            batch.append(self._row(names, classes, got.id,
                                   got.content_as[dict]))
            if len(batch) >= size:
                yield batch
                batch = []
        if batch:
            yield batch

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        """Documents come back in no order: a collection is read in one
        pass (`RESUMES_BY_KEY`). This serves the callers that ask whether
        a collection holds anything."""
        rows = []
        for batch in self.neutral_batches(side, db, table, columns,
                                          limit or 1000, where):
            rows += batch
            if limit and len(rows) >= limit:
                return rows[:limit], None
        return rows, None

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        if not key or not keys:
            return {}
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and a"
                             " collection takes none")
        classes = self._classes(side, db, table)
        names = [n for n, _ in columns]
        ids = {}
        for k in keys:
            values = dict(zip(key, k))
            ids[values[KEY] if key == [KEY] else
                self._doc_id(key, classes, values)] = k
        found = []
        coll = self._collection(side, db, table)
        items = list(ids)
        for i in range(0, len(items), WRITE_BATCH):
            got = coll.get_multi(items[i:i + WRITE_BATCH],
                                 return_exceptions=True)
            for doc_id, res in got.results.items():
                found.append(self._row(names, classes, doc_id,
                                       res.content_as[dict]))
        return self._by_key_map(columns, key, found)

    def neutral_digest(self, side, db, table, columns, where=None):
        from .. import canon
        classes = [c for _, c in columns]
        total, n = 0, 0
        for batch in self.neutral_batches(side, db, table, columns, 5000,
                                          where):
            k, total = canon.fold_rows(classes, batch, total)
            n += k
        return (n, str(total))

    def table_facts(self, side, db):
        out = {}
        for t in self.neutral_tables(side, db):
            out[t] = {"rows": None, "bytes": None, "key": True}
        return out

    # ---- writing -----------------------------------------------------------

    def neutral_create_sql(self, side, db, table, columns, key=()):
        if not key:
            raise SystemExit(
                f"{table} has no key, and a document is kept by its key: a"
                " table without one has no collection to go to")
        cols = [[c[0], c[1] + (f"({c[2][0]},{c[2][1]})"
                               if c[1] == "decimal" and len(c[2] or ()) == 2
                               else "")] for c in columns]
        return json.dumps({"collection": f"{self._d(side, db)}.{table}",
                           "key": list(key), "columns": cols})

    def neutral_create(self, side, db, table, columns, key=()):
        self._target_only(side, "create a collection")
        if table in self.neutral_tables(side, db) or \
                f"_default.{table}" in self.neutral_tables(side, db):
            raise SystemExit(f"{table} already exists on the target -"
                             " migkit will not alter or replace a collection"
                             " that is already there")
        spec = json.loads(self.neutral_create_sql(side, db, table, columns,
                                                  key))
        scope, coll = self._split(table)
        mgr = self._bucket(side, db).collections()
        if scope not in {s.name for s in mgr.get_all_scopes()}:
            mgr.create_scope(scope)
        mgr.create_collection(scope, coll)
        path = self._kept_here(side)
        try:
            kept = json.loads(path.read_text())
        except (OSError, ValueError):
            kept = {}
        kept[f"{self._d(side, db)}.{scope}.{coll}"] = {
            "columns": spec["columns"], "key": spec["key"]}
        path.write_text(json.dumps(kept, indent=1))
        return json.dumps(spec)

    def _upsert_docs(self, side, db, table, docs):
        forget_columns(self)
        coll = self._collection(side, db, table)
        items = list(docs.items())
        for i in range(0, len(items), WRITE_BATCH):
            got = coll.upsert_multi(dict(items[i:i + WRITE_BATCH]),
                                    return_exceptions=True)
            if not got.all_ok:
                bad = next(iter(got.exceptions.items()))
                raise SystemExit(f"{table}: the document {bad[0]} was not"
                                 f" written ({bad[1]})")

    def _docs(self, side, db, table, names, rows):
        classes = self._classes(side, db, table)
        key = self.neutral_key(side, db, table)
        out = {}
        for r in rows:
            values = dict(zip(names, r))
            doc_id = (str(values[KEY]) if key == [KEY]
                      else self._doc_id(key, classes, values))
            out[doc_id] = {n: self._to_json(classes.get(n), v)
                           for n, v in values.items() if n != KEY}
        return out

    def neutral_write(self, side, db, table, columns, rows):
        """Each document upserted by its key, which replaces one already
        there: a batch written again after a restart lands on itself."""
        self._target_only(side, "write rows")
        if not rows:
            return 0
        self._upsert_docs(side, db, table, self._docs(
            side, db, table, [n for n, _ in columns], rows))
        return len(rows)

    def _apply_upserts(self, side, db, table, rows):
        by = {}
        for key, values in rows:
            row = {**key, **values}
            by.update(self._docs(side, db, table, list(row),
                                 [list(row.values())]))
        self._upsert_docs(side, db, table, by)

    def _apply_upsert(self, side, db, table, key, values):
        self._apply_upserts(side, db, table, [(key, values)])

    def _apply_deletes(self, side, db, table, keys):
        classes = self._classes(side, db, table)
        key = self.neutral_key(side, db, table)
        forget_columns(self)
        ids = [str(k[KEY]) if key == [KEY] else
               self._doc_id(key, classes, k) for k in keys]
        coll = self._collection(side, db, table)
        for i in range(0, len(ids), WRITE_BATCH):
            coll.remove_multi(ids[i:i + WRITE_BATCH], return_exceptions=True)

    def _apply_delete(self, side, db, table, key):
        self._apply_deletes(side, db, table, [key])

    def neutral_empty(self, side, db, table, where=None):
        self._target_only(side, "empty a collection")
        forget_columns(self)
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and a"
                             " collection takes none")
        ids = [got.id for got in self._scan(side, db, table, ids_only=True)]
        coll = self._collection(side, db, table)
        for i in range(0, len(ids), WRITE_BATCH):
            coll.remove_multi(ids[i:i + WRITE_BATCH], return_exceptions=True)
        return len(ids)

    # ---- the checks, through the pair ------------------------------------

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

    def check_deep(self, db):
        """Every vBucket of the target's bucket with an active copy, and
        its memory quota not full: a vBucket with none has lost what it
        held, and a full quota answers writes with temporary failures and
        evicts what the check then reads from disk."""
        try:
            got = self._rest("dst", f"/pools/default/buckets/"
                                    f"{self.hop.target_db(db)}")
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("deep", f"{db} target bucket", "error",
                           f"could not ask the target's cluster manager:"
                           f" {str(e).splitlines()[0][:90]}")]
        vbmap = (got.get("vBucketServerMap") or {}).get("vBucketMap") or []
        orphan = sum(1 for v in vbmap if not v or v[0] < 0)
        used = float((got.get("basicStats") or {}).get("quotaPercentUsed")
                     or 0)
        out = [Result("deep", f"{db} target vBuckets",
                      "diff" if orphan else "ok",
                      f"{orphan} vBuckets have no active copy" if orphan
                      else f"all {len(vbmap)} vBuckets active", "",
                      "fail over or rebalance the target cluster" if orphan
                      else "")]
        if used >= 90:
            out.append(Result("deep", f"{db} target bucket", "warn",
                              f"{used:.0f}% of the bucket's memory quota is"
                              " used: writes are refused for a while and"
                              " documents evicted", "",
                              "raise the bucket's memory quota before the"
                              " load"))
        return out

    # ---- following, through the connector's topic -------------------------

    def _changes_spec(self, side="src"):
        spec = self._ep(side).options.get("changes")
        return dict(spec) if isinstance(spec, dict) else None

    def _follower(self, db):
        from .. import changetopic
        held = self.__dict__.setdefault("_followers", {})
        if db not in held:
            spec = self._changes_spec()
            if not spec:
                raise SystemExit(
                    "Couchbase hands its changes out through its Kafka"
                    " connector: say which topic it publishes to in the"
                    " source's options - changes: {servers: host:9092,"
                    " topic: <topic>} - with the connector's default source"
                    " handler")
            held[db] = changetopic.Follower(
                changetopic.CouchbaseDcp(), changetopic.KafkaLanes(
                    spec, f"{self.hop.name}-changes"),
                name=lambda row, db=db: self._named(db, row),
                shape=lambda t, row, db=db: self._shape(db, t, row))
        return held[db]

    def _named(self, db, row):
        if row.db is not None and str(row.db) != str(db):
            return None
        tables = self.__dict__.setdefault("_scope", {})
        if db not in tables or row.table not in tables[db]:
            tables[db] = set(self.neutral_tables("src", db))
        return row.table if row.table in tables[db] else None

    def _shape(self, db, table, row):
        classes = self._classes("src", db, table)
        key = self.neutral_key("src", db, table)
        doc_id = row.key[KEY]
        ident = ({KEY: doc_id} if key == [KEY]
                 else self._key_of_id(doc_id, key, classes))
        if row.values is None:
            return ident, None
        names = [n for n, _ in self.neutral_columns("src", db, table)]
        values = dict(zip(names, self._row(names, classes, doc_id,
                                           row.values)))
        return ident, values

    def seqnos(self, side, db):
        """{"vb<n>": [uuid, high seqno]} of every active vBucket of the
        bucket, from each data node that holds it."""
        bucket = self._d(side, db)
        got = self._rest(side, f"/pools/default/buckets/{bucket}")
        vbmap = got["vBucketServerMap"]
        servers = vbmap["serverList"]
        external = str(self._ep(side).options.get("network") or "")
        place = {}
        for node in got.get("nodes", []):
            host = node["hostname"].rsplit(":", 1)[0]
            alt = ((node.get("alternateAddresses") or {}).get(external)
                   if external else None)
            if alt:
                place[host] = (alt["hostname"],
                               int((alt.get("ports") or {}).get(
                                   "kv", node["ports"]["direct"])))
            else:
                place[host] = (host, int(node["ports"]["direct"]))
        out = {}
        ep = self._ep(side)
        for idx, server in enumerate(servers):
            host = server.rsplit(":", 1)[0]
            addr = place.get(host, (host, int(server.rsplit(":", 1)[1])))
            stats = _kv_stats(addr, ep.user, ep.password, bucket,
                              "vbucket-seqno", ep.options)
            for vb, owners in enumerate(vbmap["vBucketMap"]):
                if owners and owners[0] == idx:
                    uuid = stats.get(f"vb_{vb}:uuid")
                    high = stats.get(f"vb_{vb}:high_seqno")
                    if high is not None:
                        out[f"vb{vb}"] = [int(uuid) if uuid else None,
                                          int(high)]
        return out

    def change_point(self, side, db):
        """Now: the topic's ends, then every vBucket's high sequence
        number - what the connector publishes after the ends is past
        them or was committed before a copy begun after this reads it."""
        f = self._follower(db)
        return f.point(seen=lambda: self.seqnos(side, db))

    def copy_point(self, side, db):
        return self.change_point(side, db)

    def log_position(self, side, db):
        return {"seen": self.seqnos(side, db)}

    @staticmethod
    def position_reached(have, want):
        from ..changetopic import Follower
        return Follower.reached(have, want)

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Each change the connector published, in its vBucket's order."""
        if token is None:
            return [], self.change_point(side, db)
        return self._follower(db).changes(token, limit)

    def release_changes(self):
        for f in self.__dict__.pop("_followers", {}).values():
            f.close()

    def position_lost(self, side, db, token):
        return self._follower(db).lost(token) if token else None

    def stream_room(self, side, db, token):
        return self._follower(db).room(token)

    def tail_start(self, db, token_path):
        return self._as_pair().tail_start(db, token_path)

    #: its change tail is the pair's (`Engine.tail_masks`)
    TAILS_THROUGH_PAIR = True

    def tail_apply(self, db, go, token_path, log):
        return self._as_pair().tail_apply(db, go, token_path, log)

    def src_lsn(self, db):
        return self._as_pair().src_lsn(db)

    def fence_wait(self, db, lsn, timeout=300):
        return self._as_pair().fence_wait(db, lsn, timeout)

    def _compare_pks(self, db, table, keys):
        return self._as_pair()._compare_pks(db, table, keys)

    def _write_pk_files(self, db, table, missing, extra, changed):
        return self._as_pair()._write_pk_files(db, table, missing, extra,
                                                changed)

    def delta_verify(self, db, limit=20000, log=None):
        return self._as_pair().delta_verify(db, limit, log)

    # ---- the server ----------------------------------------------------------

    def server_identity(self, side, db):
        pools = self._rest(side, "/pools")
        out = {"id": str(pools.get("uuid") or ""),
               "version": str(pools.get("implementationVersion") or "")}
        try:
            got = self._rest(side, f"/pools/default/buckets/{self._d(side, db)}")
            out["name"] = str(got.get("uuid") or "")
        except Exception:  # noqa: BLE001 - a bucket not there yet
            out["absent"] = True
        return out

    def stream_identity(self, side, db):
        got = self._rest(side, f"/pools/default/buckets/{self._d(side, db)}")
        spec = self._changes_spec(side) or {}
        return {"bucket": str(got.get("uuid") or ""),
                "changes": str(spec.get("topic") or spec.get("topics"))}

    def reach_items(self, side):
        from ..changetopic import CouchbaseDcp
        items = [{"level": "pass", "scope": "reach",
                  "item": "copied and verified",
                  "detail": "collections read by the key-value range scan,"
                            " documents upserted by key, compared document"
                            " by document"}]
        if side == "src":
            spec = self._changes_spec()
            items.append(
                {"level": "pass" if spec else "warn", "scope": "reach",
                 "item": "changes followed through the Kafka connector"
                 if spec else "changes are not followed",
                 "detail": f"in each vBucket's order, fenced on the"
                           f" server's own sequence numbers; it loses"
                           f" {CouchbaseDcp.LOSES}" if spec else
                           "name the topic Couchbase's Kafka connector"
                           " publishes to (the source's `changes`)"})
        return items

    def _assess_extra(self):
        return self.reach_items("src")


def _register():
    """What of the SDK is used, read off this file, and why the rest is
    not (`tooluse`)."""
    from .. import tooluse
    unused = tooluse.UNUSED
    tooluse.register("couchbase", licence="Apache-2.0", reads=(
        tuple(f"engines/couchbase.py::{fn}" for fn in (
            "_scan", "_upsert_docs", "_apply_deletes", "neutral_empty",
            "neutral_rows_by_key", "neutral_tables", "neutral_create",
            "_cluster")),
        {("Collection", "scan"): "scan",
         ("scan", "RangeScan"): "RangeScan",
         ("ScanOptions", "ids_only"): "ids_only",
         ("ScanOptions", "batch_item_limit"): "batch_item_limit",
         ("Collection", "upsert_multi"): "upsert_multi",
         ("Collection", "remove_multi"): "remove_multi",
         ("Collection", "get_multi"): "get_multi",
         ("CollectionManager", "get_all_scopes"): "get_all_scopes",
         ("CollectionManager", "create_scope"): "create_scope",
         ("CollectionManager", "create_collection"): "create_collection",
         ("connection string", "network"): "network",
         ("Cluster", "wait_until_ready"): "wait_until_ready"}), options={
        ("scan", "SamplingScan"): (
            unused, "a collection's columns are read from every document:"
                    " a sample misses a field only the others hold"),
        ("scan", "PrefixScan"): (
            unused, "a collection is read whole, whatever its keys begin"
                    " with"),
        ("Cluster", "query"): (
            unused, "the range scan reads a collection with no query"
                    " service and no index"),
        ("Collection", "mutate_in"): (
            unused, "a change carries its document's whole body, which a"
                    " change of some paths would not"),
        ("ScanOptions", "consistent_with"): (
            unused, "a scan reads what each vBucket holds when it gets"
                    " there; the tail's position is the seam, not a"
                    " mutation token"),
    })


_register()


#: seconds the columns read from a collection's documents stand for it:
#: a field one of them gains after that is read into the next reading
COLUMNS_KEPT = 60


def document_columns(eng, at, docs):
    """[(field, kind)] of every top-level field the documents `docs()`
    hold, all of them read - a field holding several kinds is JSON, a
    number that is sometimes whole a number. Kept a minute for `at` on
    `eng`, and forgotten when `eng` writes (`forget_columns`): a change
    tail asks for each change."""
    import time
    known = eng.__dict__.setdefault("_columns_of", {})
    got = known.get(at)
    if got is None or time.monotonic() - got[0] > COLUMNS_KEPT:
        seen = {}
        for body in docs():
            if not isinstance(body, dict):
                continue
            for name, value in body.items():
                seen.setdefault(name, set()).add(_kind(value))
        cols = []
        for name, kinds in sorted(seen.items()):
            kinds.discard("null")
            if kinds == {"integer", "number"}:
                kinds = {"number"}
            cols.append((name, kinds.pop() if len(kinds) == 1
                         else "json" if kinds else "string"))
        got = known[at] = (time.monotonic(), cols)
    return got[1]


def forget_columns(eng):
    eng.__dict__.pop("_columns_of", None)


def _plain(value):
    """A value as the SDK's JSON writes it: a decimal inside a document as
    the number it is (whole, or a float - a JSON number is read as one)."""
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() \
            else float(value)
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


def _kind(value):
    """A JSON value's kind, a whole number apart from any other."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, (float, Decimal)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    return "object"


# ---- memcached's binary protocol, for a data node's statistics -----------

def _packet(opcode, key=b"", value=b"", extras=b""):
    body = extras + key + value
    return struct.pack(">BBHBBHIIQ", MAGIC_REQ, opcode, len(key),
                       len(extras), 0, 0, len(body), 0, 0) + body


def _answer(sock):
    head = _exact(sock, 24)
    magic, _, keylen, extlen, _, status, bodylen, _, _ = struct.unpack(
        ">BBHBBHIIQ", head)
    if magic != MAGIC_RES:
        raise SystemExit("a Couchbase data node answered in a protocol"
                         " migkit does not read")
    body = _exact(sock, bodylen)
    return status, body[extlen:extlen + keylen], body[extlen + keylen:]


def _exact(sock, n):
    out = b""
    while len(out) < n:
        got = sock.recv(n - len(out))
        if not got:
            raise ConnectionError("the data node closed the connection")
        out += got
    return out


def _scram(sock, mech, user, password):
    """SASL SCRAM (RFC 5802) with the hash `mech` names."""
    name = {"SCRAM-SHA512": "sha512", "SCRAM-SHA256": "sha256",
            "SCRAM-SHA1": "sha1"}[mech]
    who = user.replace("=", "=3D").replace(",", "=2C")
    nonce = base64.b64encode(os.urandom(18)).decode()
    bare = f"n={who},r={nonce}"
    sock.sendall(_packet(SASL_AUTH, mech.encode(), f"n,,{bare}".encode()))
    status, _, first = _answer(sock)
    if status != AUTH_CONTINUE:
        raise SystemExit(f"the data node refused the sign-in ({status:#x})")
    fields = dict(p.split("=", 1) for p in first.decode().split(","))
    salted = hashlib.pbkdf2_hmac(name, password.encode(),
                                 base64.b64decode(fields["s"]),
                                 int(fields["i"]))
    client_key = hmac.new(salted, b"Client Key", name).digest()
    stored = hashlib.new(name, client_key).digest()
    bound = f"c=biws,r={fields['r']}"
    signature = hmac.new(stored, f"{bare},{first.decode()},{bound}".encode(),
                         name).digest()
    proof = bytes(a ^ b for a, b in zip(client_key, signature))
    sock.sendall(_packet(SASL_STEP, mech.encode(),
                         f"{bound},p={base64.b64encode(proof).decode()}"
                         .encode()))
    status, _, _ = _answer(sock)
    if status != 0:
        raise SystemExit(f"the data node refused the sign-in ({status:#x})")


def _kv_stats(addr, user, password, bucket, group, options):
    """{name: value} of one statistics group of a bucket on one data
    node, signed in by SCRAM (PLAIN only over TLS)."""
    import ssl
    sock = socket.create_connection(addr, timeout=15)
    try:
        if options.get("tls"):
            ctx = ssl.create_default_context(
                cafile=options.get("tls_ca_file") or None)
            if options.get("tls_insecure"):
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
            sock = ctx.wrap_socket(sock, server_hostname=addr[0])
        sock.sendall(_packet(SASL_LIST))
        _, _, mechs = _answer(sock)
        offered = mechs.decode().split()
        mech = next((m for m in ("SCRAM-SHA512", "SCRAM-SHA256",
                                 "SCRAM-SHA1") if m in offered), None)
        if mech:
            _scram(sock, mech, user, password)
        elif "PLAIN" in offered and options.get("tls"):
            sock.sendall(_packet(SASL_AUTH, b"PLAIN",
                                 f"\0{user}\0{password}".encode()))
            if _answer(sock)[0] != 0:
                raise SystemExit("the data node refused the sign-in")
        else:
            raise SystemExit("the data node offers no sign-in migkit makes"
                             f" ({', '.join(offered)})")
        sock.sendall(_packet(SELECT_BUCKET, bucket.encode()))
        if _answer(sock)[0] != 0:
            raise SystemExit(f"the data node has no bucket {bucket} this"
                             " user may read")
        sock.sendall(_packet(STAT, group.encode()))
        out = {}
        while True:
            status, key, value = _answer(sock)
            if status != 0:
                raise SystemExit(f"the data node would not give its"
                                 f" {group} statistics ({status:#x})")
            if not key:
                return out
            out[key.decode()] = value.decode()
    finally:
        sock.close()
