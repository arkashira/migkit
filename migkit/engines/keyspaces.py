"""Amazon Keyspaces as a side of a pair (backlog P0-CLOUDS): the Cassandra
engine (`cassandra`), with what Keyspaces does differently.

* It is reached over TLS only (port 9142), signed in to by AWS's own
  signature (`auth: aws_iam`, the SigV4 plugin, credentials as the AWS
  CLI resolves them) or by the service's own user and password.
* Every write is at LOCAL_QUORUM, the only level it takes one at; reads
  are too, so a row just written is the row read.
* A keyspace is replicated by `SingleRegionStrategy` unless the target
  says otherwise, and a table, like a keyspace, is made in the background:
  it is used once the service says it is ACTIVE (`system_schema_mcs`).
* It has no TRUNCATE: a table is emptied row by row.
* Its changes are its CDC stream's (`keyspacesstreams`): a table's
  stream, shard by shard - each shard a lane, its records in the order
  its keys changed - a record carrying the row's key and, for a stream of
  new images, the row as it became. Where the stream carries keys only,
  the row is read back as it is. A stream says no operation: a record
  with no new image in a stream of new images is a delete. A shard with no
  position yet is read from the start of what the stream keeps (a day),
  which carries again changes a copy read - each lands on itself in the
  order the shard holds them. The fence is time: every shard read to its
  end after the moment asked, and `stream_lag` seconds past it (the
  stream's own delay behind a write; 10 unless the source says).
"""
import datetime
import time

from .cassandra import CassandraEngine

#: seconds the stream is taken to trail a write
STREAM_LAG = 10


def cell_value(cell):
    """A stream's cell as the Cassandra driver would hand the value over."""
    import uuid
    from decimal import Decimal
    if not cell:
        return None
    v = cell.get("value") if "value" in cell else cell
    if not isinstance(v, dict) or not v:
        return None
    (kind, raw), = [(k, x) for k, x in v.items()][:1]
    if kind in ("asciiT", "textT", "varcharT", "inetT"):
        return raw
    if kind in ("bigintT", "intT", "smallintT", "tinyintT", "varintT",
                "counterT"):
        return int(raw)
    if kind == "blobT":
        return bytes(raw)
    if kind == "boolT":
        return bool(raw)
    if kind == "decimalT":
        return Decimal(str(raw))
    if kind in ("doubleT", "floatT"):
        return float(raw)
    if kind in ("uuidT", "timeuuidT"):
        return str(uuid.UUID(str(raw)))
    if kind == "dateT":
        text = str(raw)
        if text.lstrip("-").isdigit():
            return datetime.date(1970, 1, 1) + datetime.timedelta(
                days=int(text))
        return datetime.date.fromisoformat(text[:10])
    if kind == "timestampT":
        text = str(raw)
        if text.lstrip("-").isdigit():
            return datetime.datetime(1970, 1, 1) + datetime.timedelta(
                milliseconds=int(text))
        got = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
        return got.astimezone(datetime.timezone.utc).replace(tzinfo=None) \
            if got.tzinfo else got
    if kind == "timeT":
        from .. import nanotime
        return nanotime.parse(str(raw))
    if kind == "durationT":
        from .. import intervals
        return intervals.parse(str(raw))
    if kind in ("listT", "setT", "tupleT"):
        return [cell_value(x) for x in raw]
    if kind == "mapT":
        return {str(cell_value(x.get("key"))): cell_value(x.get("value"))
                for x in raw}
    if kind == "udtT":
        return {k: cell_value(x) for k, x in raw.items()}
    return raw


class KeyspacesEngine(CassandraEngine):
    #: its values are Cassandra's in every way `canon` reads them; its pair
    #: is made of Keyspaces sides
    CANON_ENGINE = "cassandra"
    PAIR_ENGINE = "keyspaces"
    _sessions = {}

    def _session(self, side):
        from cassandra import ConsistencyLevel
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, \
            ExecutionProfile
        ep = self.hop.source if side == "src" else self.hop.target
        at = (ep.host, ep.port or 9142, ep.user)
        if at not in self._sessions:
            if ep.options.get("auth") == "aws_iam" or not ep.user:
                import boto3
                from cassandra_sigv4.auth import SigV4AuthProvider
                auth = SigV4AuthProvider(boto3.Session(
                    region_name=self._region(ep)))
            else:
                auth = PlainTextAuthProvider(ep.user, ep.password)
            tls = ep.cassandra_tls()
            if not tls and not ep.options.get("tls_insecure"):
                import ssl
                ctx = ssl.create_default_context()
                tls = {"ssl_context": ctx,
                       "ssl_options": {"server_hostname": ep.host}}
            profile = ExecutionProfile(
                consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            cluster = Cluster([ep.host], port=int(ep.port or 9142),
                              auth_provider=auth, connect_timeout=15,
                              execution_profiles={
                                  EXEC_PROFILE_DEFAULT: profile}, **tls)
            self._sessions[at] = cluster.connect()
        return self._sessions[at]

    @staticmethod
    def _region(ep):
        import re
        said = ep.options.get("region")
        if said:
            return str(said)
        m = re.search(r"cassandra\.([a-z0-9-]+)\.amazonaws\.com",
                      str(ep.host or ""))
        return m.group(1) if m else None

    def _await_active(self, side, what, where, args):
        """Wait until the service says a keyspace or table it is making is
        ACTIVE: it is made in the background, and used before that it
        refuses."""
        for _ in range(300):
            got = list(self._session(side).execute(
                f"select status from system_schema_mcs.{what} where {where}",
                args))
            if got and str(got[0].status).upper() == "ACTIVE":
                return
            time.sleep(2)
        raise SystemExit(f"the {what[:-1]} {args} did not become ACTIVE in"
                         " ten minutes")

    def prepare_target(self, db):
        """The keyspace, where it is missing: replicated as the target says,
        or in its one region (`SingleRegionStrategy`), and waited for."""
        if not self.target_missing(db):
            return None
        how = self.hop.target.options.get("replication") or \
            "{'class': 'SingleRegionStrategy'}"
        ks = self._ks("dst", db)
        self._session("dst").execute(f"create keyspace {self._q(ks)} with"
                                     f" replication = {how}")
        self._await_active("dst", "keyspaces", "keyspace_name = %s", (ks,))
        return f"created keyspace {ks}"

    def neutral_create(self, side, db, table, columns, key=()):
        ddl = super().neutral_create(side, db, table, columns, key)
        self._await_active(side, "tables", "keyspace_name = %s and"
                                           " table_name = %s",
                           (self._ks(side, db), table))
        return ddl

    def neutral_empty(self, side, db, table, where=None):
        """Every row deleted by its key: Keyspaces takes no TRUNCATE."""
        from cassandra.concurrent import execute_concurrent_with_args
        self._target_only(side, "empty a table")
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and Keyspaces"
                             " takes none")
        key = self.neutral_key(side, db, table)
        session = self._session(side)
        q = self._qualified(side, db, table)
        stmt = session.prepare(f"delete from {q} where "
                               + " and ".join(f"{self._q(k)} = ?"
                                              for k in key))
        keys = [tuple(r) for r in session.execute(
            f"select {', '.join(self._q(k) for k in key)} from {q}")]
        for ok, got in execute_concurrent_with_args(session, stmt, keys,
                                                    concurrency=64):
            if not ok:
                raise got
        return len(keys)

    def server_identity(self, side, db):
        """The service's endpoint and region, which say which Keyspaces it
        is - its nodes are the endpoint's, not a cluster's own - and
        whether the keyspace is there yet."""
        ep = self.hop.source if side == "src" else self.hop.target
        out = {"name": str(ep.host), "id": f"keyspaces:{self._region(ep)}"
                                           f":{ep.host}"}
        if not list(self._session(side).execute(
                "select keyspace_name from system_schema.keyspaces where"
                " keyspace_name = %s", (self._ks(side, db),))):
            out["absent"] = True
        return out

    def stream_identity(self, side, db):
        return {"streams": ",".join(sorted(
            self._stream_arn(db, t) or "" for t in
            self.neutral_tables(side, db)))}

    # ---- the CDC stream ------------------------------------------------------

    def _streams(self):
        held = self.__dict__.get("_streams_client")
        if held is None:
            import boto3
            ep = self.hop.source
            held = self.__dict__["_streams_client"] = boto3.client(
                "keyspacesstreams", region_name=self._region(ep),
                **({"endpoint_url": ep.options["endpoint_url"]}
                   if ep.options.get("endpoint_url") else {}))
        return held

    def _stream_arn(self, db, table):
        got = self._streams().list_streams(keyspaceName=db, tableName=table)
        arns = sorted((s.get("streamLabel") or "", s["streamArn"])
                      for s in got.get("streams", []))
        return arns[-1][1] if arns else None

    def _logged(self, db):
        """The tables in scope, each with its CDC stream on - or the reason
        a table cannot be followed, which is its stream's to be turned on
        (`ALTER TABLE ... WITH cdc = true`), not migkit's."""
        out = []
        for t in self.neutral_tables("src", db):
            if self._stream_arn(db, t) is None:
                raise SystemExit(
                    f"{db}.{t} has no CDC stream, and turning one on is a"
                    " change to the source's table, which migkit does not"
                    " make: alter table ... with cdc = true (a view of new"
                    " images, or of new and old), then move again with"
                    " --mode full+cdc")
            out.append(t)
        return out

    def _follower(self, db):
        from .. import changetopic
        from .bigtable import _Tables
        held = self.__dict__.setdefault("_followers", {})
        if db not in held:
            tables = self._logged(db)
            held[db] = changetopic.Follower(
                changetopic.KeyspacesChanges(),
                _Tables({t: KeyspacesLanes(self, db, t) for t in tables}),
                name=lambda row: row.table,
                shape=lambda t, row, db=db: self._shape(db, t, row),
                read_back=lambda t, rows, v, db=db: self._read_back(
                    db, t, rows))
        return held[db]

    def _shape(self, db, table, row):
        if row.op == "delete":
            return row.key, None
        if row.values is None:
            return None
        return row.key, {**row.values, **row.key}

    def _read_back(self, db, table, rows):
        from .. import canon
        cols = [(n, canon.type_class("cassandra", t) or canon.OWN)
                for n, t in self.neutral_columns("src", db, table)]
        key = self.neutral_key("src", db, table)
        names = [n for n, _ in cols]
        now = self.neutral_rows_by_key(
            "src", db, table, cols, key,
            [tuple(r.key[k] for k in key) for r in rows])
        cls = dict(cols)
        out = []
        for r in rows:
            ident = tuple(canon.render_value(cls[k], r.key[k]) for k in key)
            got = now.get(ident)
            out.append((r, {k: r.key[k] for k in key},
                        None if got is None else dict(zip(names, got))))
        return out

    def change_point(self, side, db):
        return self._follower(db).point()

    def copy_point(self, side, db):
        return self.change_point(side, db)

    def log_position(self, side, db):
        lag = float((self.hop.options or {}).get("stream_lag", STREAM_LAG))
        return {"time": time.time() + lag}

    @staticmethod
    def position_reached(have, want):
        """Every shard read to its end at or after the moment asked."""
        if not have or not want or "time" not in want:
            return None
        at = have.get("at") or {}
        if not at:
            return False
        return all(float((p or {}).get("caught") or 0) >= float(want["time"])
                   for p in at.values())

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Each row the CDC stream says changed, shard by shard in its
        order, as the stream's image gives it or as the table holds it."""
        if token is None:
            return [], self.change_point(side, db)
        return self._follower(db).changes(token, limit)

    def release_changes(self):
        for f in self.__dict__.pop("_followers", {}).values():
            f.close()

    def position_lost(self, side, db, token):
        return self._follower(db).lost(token) if token else None

    def stream_room(self, side, db, token):
        return None

    def tail_apply(self, db, go, token_path, log):
        return self._as_pair().tail_apply(db, go, token_path, log)

    def tail_start(self, db, token_path):
        return self._as_pair().tail_start(db, token_path)

    def reach_items(self, side):
        items = [{"level": "pass", "scope": "reach",
                  "item": "copied and verified",
                  "detail": "rows read by token and written at LOCAL_QUORUM,"
                            " compared row by row"}]
        if side == "src":
            from ..changetopic import KeyspacesChanges
            items.append({"level": "pass", "scope": "reach",
                          "item": "changes followed through its CDC stream",
                          "detail": f"shard by shard; it loses"
                                    f" {KeyspacesChanges.LOSES}"})
        return items

    def _assess_extra(self):
        return self.reach_items("src")


class KeyspacesLanes:
    """One table's CDC stream's shards: a parent read before its children,
    each by sequence number, a shard with no position from its start. A
    shard read to its end says when (`caught`), which the fence is held
    to."""
    KIND = "keyspaces"

    def __init__(self, engine, db, table):
        self.engine, self.db, self.table = engine, db, table
        self.arn = engine._stream_arn(db, table)
        self.view = None

    def _shards(self):
        client = self.engine._streams()
        out, token = [], None
        while True:
            kw = {"streamArn": self.arn}
            if token:
                kw["nextToken"] = token
            got = client.get_stream(**kw)
            self.view = got.get("streamViewType")
            out += got.get("shards", [])
            token = got.get("nextToken")
            if not token:
                break
        ids = {s["shardId"] for s in out}
        placed, ordered = set(), []
        while len(ordered) < len(out):
            before = len(ordered)
            for s in out:
                parents = [p for p in s.get("parentShardIds") or []
                           if p in ids]
                if s["shardId"] not in placed and all(p in placed
                                                      for p in parents):
                    ordered.append(s)
                    placed.add(s["shardId"])
            if len(ordered) == before:
                raise SystemExit(f"the shards of {self.table}'s stream name"
                                 " each other as parents")
        return ordered

    def lanes(self):
        return [s["shardId"] for s in self._shards()]

    def ends(self):
        return {shard: {"from_start": True} for shard in self.lanes()}

    def _iterator(self, shard, pos):
        kw = {"streamArn": self.arn, "shardId": shard}
        if isinstance(pos, dict) and pos.get("after"):
            kw.update(shardIteratorType="AFTER_SEQUENCE_NUMBER",
                      sequenceNumber=pos["after"])
        else:
            kw.update(shardIteratorType="TRIM_HORIZON")
        return self.engine._streams().get_shard_iterator(
            **kw)["shardIterator"]

    def _record(self, rec):
        key = {k: cell_value(c) for k, c in
               {**(rec.get("partitionKeys") or {}),
                **(rec.get("clusteringKeys") or {})}.items()}
        image = rec.get("newImage")
        if image:
            values = {k: cell_value(c) for k, c in
                      {**(image.get("valueCells") or {}),
                       **(image.get("staticCells") or {})}.items()}
            op = "upsert"
        else:
            values = None
            op = "delete" if self.view in ("NEW_IMAGE",
                                           "NEW_AND_OLD_IMAGES") else "changed"
        return {"table": self.table, "key": key, "image": values, "op": op,
                "sequence": rec["sequenceNumber"]}

    def read(self, at, most, seconds):
        client = self.engine._streams()
        out, nxt, caught = [], dict(at or {}), True
        stop = time.monotonic() + seconds
        for s in self._shards():
            shard = s["shardId"]
            pos = dict(nxt.get(shard) or {})
            it = self._iterator(shard, pos)
            began = time.time()
            while it:
                if len(out) >= most or time.monotonic() > stop:
                    caught = False
                    break
                got = client.get_records(shardIterator=it,
                                         maxResults=min(1000, most - len(out)))
                recs = got.get("changeRecords", [])
                for r in recs:
                    out.append((shard, {"at": r["sequenceNumber"]}, None,
                                self._record(r), self.table))
                    pos = {"after": r["sequenceNumber"]}
                it = got.get("nextShardIterator")
                if not recs:
                    pos["caught"] = began
                    break
            nxt[shard] = pos
        return out, nxt, caught

    def lost(self, at):
        return None

    def room(self, at):
        return None

    def close(self):
        return None


def _register():
    """What of the SigV4 plugin is used (`tooluse`)."""
    from .. import tooluse
    tooluse.register("cassandra-sigv4", licence="Apache-2.0", reads=(
        ("engines/keyspaces.py",),
        {("auth", "SigV4AuthProvider"): "SigV4AuthProvider"}), options={})


_register()
