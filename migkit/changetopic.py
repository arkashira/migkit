"""One reader for a source that hands its changes out as a feed in a known
envelope (backlog ER4, ER5): TiDB through TiCDC, CockroachDB through its
changefeeds, Aurora DSQL through its CDC stream, Couchbase through its
connector's topic, Cosmos DB through its change feed.

None of these sources gives a reader its log. Each publishes what changed
somewhere else - a Kafka topic, a Kinesis stream, a feed the server
streams to a query - in an envelope of its own, with a position of its own
for when a change was committed. What differs between them is small: how a
message is read (the envelope), where the messages are (the lanes: a
topic's partitions, a stream's shards, a container's feed ranges), and
which of two orders the source promises:

* one clock for every lane (`GLOBAL`): each change carries its commit time
  on the source's own clock - TiDB's TSO, CockroachDB's HLC, DSQL's commit
  nanoseconds - and a lane can be behind the others. A change is handed on
  only once every lane has said nothing at or below its time is still to
  come (a mark: TiCDC's watermark, CockroachDB's resolved timestamp, and
  for DSQL, whose stream says neither, the stream's own measure of how far
  behind it publishes). Handed on in commit order, a key's changes can
  never overtake each other, and a message delivered twice - all of these
  deliver at least once - is at or below what was handed on already and
  is dropped;
* one order per lane (`LANE`): a key's changes are in one lane, in order,
  and nothing orders two lanes (Couchbase's vBuckets through the
  connector's partitions, Cosmos DB's feed ranges). Handed on as read; a
  change below what its vBucket already handed on is a delivery again,
  and dropped.

The same buffer serves both, so a fix to the order is a fix for every
source that publishes its changes.

Where the source can be read as it stood at a moment - TiDB at a TSO,
CockroachDB `AS OF SYSTEM TIME` - the envelope is read for which keys
changed and when, never for the values: the rows are read back from the
source as they stood at the batch's own fence (`READ_BACK`), through the
engine's own driver, so a value arrives exactly as the copy read it
whatever the envelope made of it (TiCDC's canal-json writes every value
as a string and binary as ISO-8859-1 characters; CockroachDB's JSON writes
decimals as numbers). What was read back is checked against what the feed
said: a key the feed says was written and is not there, or deleted and is
there, is a key migkit cannot read back as the feed names it, and stops the
tail - applying the absence as a delete would lose the row without a word.
Where the source no longer keeps the moment (a garbage collector past it)
the rows are read as they are now, which later changes bring level again.

The position saved is, per lane, the first message not yet handed on - so
a tail stopped with changes held back reads them again - with the fence
(`GLOBAL`) or each sub-lane's last version (`LANE`) handed on, which drop
what is read twice. What a reader holds in memory belongs to the position
it last handed back: any other position (a tail restarted from its saved
one after a lost connection) is read again from the lanes.
"""
import base64
import datetime
import json
import time
from decimal import Decimal

GLOBAL, LANE = "global", "lane"

#: changes held back waiting for a mark before reading stops until one
#: comes: past this a tail that is not sent marks would grow without bound
HOLD = 200_000
#: seconds rows may be held with no lane's mark moving before the reader
#: says the feed sends none
STALL = 600
#: seconds a stream read from a moment starts before it: this machine's
#: clock against the stream's
SINCE = 300


class Row:
    """One change as an envelope reads it: `key` and `values` as the
    message gave them (text, numbers), `version` its place in the source's
    order, `sub` the sub-lane a version is compared within (`LANE`) and
    `subid` that sub-lane's incarnation (a vBucket's UUID). `op` is
    `upsert`, `delete`, or `changed` where the message names only the key.
    """
    __slots__ = ("db", "table", "op", "key", "values", "version", "sub",
                 "subid", "lane", "at", "txn")

    def __init__(self, db, table, op, key, values, version, sub=None,
                 subid=None, txn=None):
        self.db, self.table, self.op = db, table, op
        self.key, self.values, self.version = key, values, version
        self.sub, self.subid, self.txn = sub, subid, txn
        self.lane, self.at = None, None

    def ident(self):
        return (self.table, tuple(sorted((k, str(v))
                                         for k, v in self.key.items())))


class Mark:
    """Nothing at or below `version` is still to come on this lane."""
    __slots__ = ("version",)

    def __init__(self, version):
        self.version = int(version)


# ---- envelopes ------------------------------------------------------------

def _json(raw):
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray, memoryview)):
        raw = bytes(raw).decode("utf-8")
    return json.loads(raw, parse_float=Decimal) if raw else None


class Envelope:
    #: what the hop names it by (`changes: {format: ...}`)
    name = ""
    ORDER = GLOBAL
    #: whether rows are read back from the source at the fence
    READ_BACK = False
    #: what a change of this envelope cannot carry, said by `assess`
    LOSES = ""

    def decode(self, key, value, topic=None):
        raise NotImplementedError


class TicdcCanalJson(Envelope):
    """TiCDC's canal-json with `enable-tidb-extension=true`: each row
    event's `_tidb.commitTs` and the `TIDB_WATERMARK` events a partition
    is sent - without the extension neither is there, and nothing can be
    put in commit order. A primary key an UPDATE changed is read from
    `old` as a second key; a DDL event is the tail's shape check's."""
    name = "canal-json"
    READ_BACK = True
    LOSES = ("nothing: the rows are read back from TiDB as they stood at"
             " each watermark")

    def decode(self, key, value, topic=None):
        msg = _json(value)
        if not msg:
            return []
        kind = str(msg.get("type") or "").upper()
        tidb = msg.get("_tidb") or {}
        if kind == "TIDB_WATERMARK":
            return [Mark(tidb["watermarkTs"])]
        if msg.get("isDdl") or kind not in ("INSERT", "UPDATE", "DELETE"):
            return []
        if "commitTs" not in tidb:
            raise SystemExit(
                "the TiCDC changefeed's messages carry no commit timestamp,"
                " and without it no change can be put in the order it was"
                " committed: create the changefeed's sink with"
                " enable-tidb-extension=true")
        ts = int(tidb["commitTs"])
        names = list(msg.get("pkNames") or [])
        out = []
        for i, row in enumerate(msg.get("data") or []):
            op = "delete" if kind == "DELETE" else "upsert"
            out.append(Row(msg.get("database"), msg.get("table"), op,
                           {k: row.get(k) for k in names} or dict(row),
                           None, ts, txn=ts))
            old = (msg.get("old") or [None] * (i + 1))[i] \
                if kind == "UPDATE" else None
            if old and names:
                was = {k: old.get(k, row.get(k)) for k in names}
                if was != out[-1].key:
                    out.append(Row(msg.get("database"), msg.get("table"),
                                   "delete", was, None, ts, txn=ts))
        return out


class TicdcSimple(Envelope):
    """TiCDC's simple protocol: `commitTs` on every row event and
    `WATERMARK` events of its own."""
    name = "simple"
    READ_BACK = True
    LOSES = TicdcCanalJson.LOSES

    def decode(self, key, value, topic=None):
        msg = _json(value)
        if not msg:
            return []
        kind = str(msg.get("type") or "").upper()
        if kind == "WATERMARK":
            return [Mark(msg["commitTs"])]
        if kind not in ("INSERT", "UPDATE", "DELETE"):
            return []
        ts = int(msg["commitTs"])
        row = msg.get("old") if kind == "DELETE" else msg.get("data")
        out = [Row(msg.get("database"), msg.get("table"),
                   "delete" if kind == "DELETE" else "upsert", dict(row or {}),
                   None, ts, txn=ts)]
        if kind == "UPDATE" and msg.get("old"):
            out.append(Row(msg.get("database"), msg.get("table"), "delete",
                           dict(msg["old"]), None, ts, txn=ts))
        return out


def hlc(text):
    """A CockroachDB timestamp - `1701102296662969433.0000000012`, wall
    nanoseconds and a logical count - as one integer that orders as the
    timestamps do: the wall time shifted past room for the logical part."""
    wall, _, logical = str(text).partition(".")
    return int(wall) * 10 ** 10 + int(logical or 0)


def hlc_text(value):
    """`hlc` back to CockroachDB's own text."""
    wall, logical = divmod(int(value), 10 ** 10)
    return f"{wall}.{logical:010d}"


class CrdbChangefeed(Envelope):
    """A CockroachDB changefeed's JSON, from a Kafka sink or streamed to a
    query: the message key is the primary key as a JSON array, the value
    `{"after": ..., "updated": ...}` (`after` null for a delete), and a
    resolved timestamp `{"resolved": ...}` with no key. The feed has to be
    made `WITH updated, resolved`."""
    name = "changefeed-json"
    READ_BACK = True
    LOSES = ("nothing: the rows are read back from CockroachDB as they"
             " stood at each resolved timestamp")

    def decode(self, key, value, topic=None):
        body = _json(value)
        if body is None:
            return []
        if "resolved" in body:
            return [Mark(hlc(body["resolved"]))]
        if "payload" in body and isinstance(body["payload"], dict):
            body = body["payload"]
        if "updated" not in body:
            raise SystemExit(
                "the changefeed's messages carry no commit timestamp: make"
                " it WITH updated, resolved")
        ts = hlc(body["updated"])
        pk = _json(key) if key is not None else body.get("key")
        after = body.get("after")
        return [Row(None, topic or body.get("topic"),
                    "delete" if after is None else "upsert",
                    {str(i): v for i, v in enumerate(pk or [])}, None, ts,
                    txn=ts)]


class CouchbaseDcp(Envelope):
    """What Couchbase's Kafka connector publishes with its default source
    handler: a document's `event` (mutation, deletion, expiration), its
    vBucket (`partition`) and that vBucket's UUID, its sequence number
    there (`bySeqno`), its key, and the body as bytes (`content`, base64
    in JSON) - with or without the converter's `schema` around it. A
    vBucket's sequence numbers are the order of its keys' changes; a
    vBucket that failed over and rolled back starts a new UUID."""
    name = "couchbase-dcp"
    ORDER = LANE
    LOSES = ("extended attributes and the expiry time a document carries:"
             " the body and its key are what the connector publishes")

    def decode(self, key, value, topic=None):
        msg = _json(value)
        if not msg:
            return []
        if isinstance(msg.get("payload"), dict):
            msg = msg["payload"]
        event = str(msg.get("event") or "").lower()
        if event not in ("mutation", "deletion", "expiration"):
            raise SystemExit(
                "the Couchbase topic's messages are not the connector's"
                " default envelope (no mutation/deletion event): set the"
                " connector's source handler to its default, which carries"
                " each change's vBucket and sequence number")
        table = f"{msg.get('scope') or '_default'}." \
                f"{msg.get('collection') or '_default'}"
        body = None
        if event == "mutation":
            raw = msg.get("content")
            if isinstance(raw, str):
                raw = base64.b64decode(raw)
            body = _json(raw) if raw else {}
        return [Row(msg.get("bucket"), table,
                    "upsert" if event == "mutation" else "delete",
                    {"_key": msg.get("key")}, body, int(msg["bySeqno"]),
                    sub=f"vb{int(msg['partition'])}",
                    subid=msg.get("vBucketUuid"))]


class DsqlCdc(Envelope):
    """Aurora DSQL's CDC records: `op` c, u or d, the row after (all its
    columns) or, for a delete, its key before, `source.ts_ns` the commit
    time. A record over 9 MiB comes as a `chunked` record and its
    `fragment`s, in any order on any shard: the fragments are held by
    chunk until the record is whole, and its CRC32C checked. Records are
    compacted per transaction - one per row - so `txId` is not needed for
    order. The stream sends no mark: the engine says how far it has
    published (`Follower(delivered=...)`)."""
    name = "dsql"
    LOSES = ("an interval's months and days (sent as approximate"
             " microseconds), a timetz's offset, and the infinities of a"
             " date or timestamp: rows of tables holding those are read"
             " back from DSQL as they are when the change is read")

    def __init__(self):
        self.pieces = {}
        self.waiting = {}

    def decode(self, key, value, topic=None):
        msg = _json(value)
        if not msg:
            return []
        kind = msg.get("type", "full")
        if kind == "fragment":
            self.pieces.setdefault(msg["chunk_id"], {})[int(msg["index"])] \
                = msg["data"]
            return self._whole()
        if kind == "chunked":
            self.waiting[id(msg)] = msg
            return self._whole()
        return [self._row(msg)]

    def _whole(self):
        """The chunked records whose fragments have all come."""
        out = []
        for ident, msg in list(self.waiting.items()):
            parts = msg.get("chunked") or {}
            if any(len(self.pieces.get(c["chunk_id"], {}))
                   < int(c["total_fragments"]) for c in parts.values()):
                continue
            for side, c in parts.items():
                got = self.pieces.pop(c["chunk_id"])
                text = "".join(got[i] for i in range(int(c["total_fragments"])))
                crc = _crc32c(text.encode("utf-8"))
                if str(crc) != str(c["crc32c"]):
                    raise SystemExit(
                        f"a DSQL change to {msg['source'].get('table')} came"
                        " in pieces that do not add up to what it says it"
                        " holds (CRC32C): nothing of it is applied")
                msg[side] = json.loads(text, parse_float=Decimal)
            del self.waiting[ident]
            out.append(self._row(msg))
        return out

    @staticmethod
    def _row(msg):
        src = msg.get("source") or {}
        op = msg.get("op")
        ts = int(src["ts_ns"])
        if op == "d":
            if not msg.get("before"):
                raise SystemExit(
                    f"a delete from {src.get('table')} names no key: DSQL"
                    " sends a deleted row's key only for a table with a"
                    " primary key, and without one the row cannot be found"
                    " on the target")
            return Row(src.get("db"), f"{src.get('schema')}.{src.get('table')}",
                       "delete", dict(msg["before"]), None, ts,
                       txn=src.get("txId"))
        # c, u, and an operation this version does not know: the row after
        # it is its state, as AWS says an unknown op is to be read
        after = dict(msg.get("after") or {})
        return Row(src.get("db"), f"{src.get('schema')}.{src.get('table')}",
                   "upsert", after, after, ts, txn=src.get("txId"))


def _crc32c(data):
    import google_crc32c
    return google_crc32c.value(data)


#: what Cosmos DB adds to an item of its own, which no other side holds
COSMOS_SYSTEM = ("_rid", "_self", "_etag", "_attachments", "_ts", "_lsn")


class CosmosFeed(Envelope):
    """A Cosmos DB container's change feed, as its SDK reads it by feed
    range: in all-versions-and-deletes mode an item's `current` and its
    `metadata` (`operationType` create, replace or delete - a delete
    naming the item's `id` and `partitionKey`, `lsn` its place); in
    latest-version mode the item as it now is, `_lsn` its place. The
    container's name comes with each read."""
    name = "cosmos-feed"
    ORDER = LANE
    LOSES = ("nothing within the account's continuous-backup window, in"
             " all-versions-and-deletes mode; in latest-version mode every"
             " delete and every expiry by time to live - found only by"
             " check - and the versions between two reads, which a copy"
             " does not need")

    def __init__(self, key_fields=None):
        #: {container: [the fields its partition key is read from]}
        self.key_fields = key_fields or {}

    def decode(self, key, value, topic=None):
        item = value if isinstance(value, dict) else _json(value)
        if not item:
            return []
        meta = item.get("metadata")
        if isinstance(meta, dict) and "operationType" in meta:
            version = int(meta["lsn"])
            if meta["operationType"] == "delete":
                ident = {"id": meta.get("id")}
                ident.update(meta.get("partitionKey") or {})
                return [Row(None, topic, "delete", ident, None, version)]
            body = {k: v for k, v in (item.get("current") or {}).items()
                    if k not in COSMOS_SYSTEM}
        else:
            version = int(item.get("_lsn") or 0)
            body = {k: v for k, v in item.items() if k not in COSMOS_SYSTEM}
        ident = {"id": body.get("id")}
        for f in self.key_fields.get(topic, ()):
            ident[f] = body.get(f)
        return [Row(None, topic, "upsert", ident, body, version)]


ENVELOPES = {e.name: e for e in (TicdcCanalJson, TicdcSimple,
                                 CrdbChangefeed, CouchbaseDcp, DsqlCdc,
                                 CosmosFeed)}


def envelope(name):
    try:
        return ENVELOPES[name]()
    except KeyError:
        raise SystemExit(f"format: {name} - migkit reads"
                         f" {', '.join(sorted(ENVELOPES))}") from None


# ---- lanes: where the messages are ----------------------------------------

class KafkaLanes:
    """A topic's (or several topics') partitions, read by offset with no
    group: nothing is committed to the cluster, and the position is the
    tail's own. Signed in to as the Kafka engine signs in, from the
    `changes` options (`servers`, `security_protocol`, `sasl_mechanism`,
    `user`, `password`, `ssl_cafile`)."""
    KIND = "kafka"

    def __init__(self, spec, name="changes"):
        from .config import Endpoint, Hop, _secret
        from .engines.kafka import KafkaEngine
        servers = spec.get("servers") or spec.get("hosts") or []
        if isinstance(servers, str):
            servers = [s.strip() for s in servers.split(",") if s.strip()]
        if not servers:
            raise SystemExit("changes: name the Kafka cluster the changes are"
                             " published to (servers: host:9092)")
        self.topics = list(spec.get("topics") or [spec.get("topic")])
        if not all(self.topics):
            raise SystemExit("changes: name the topic the changes are"
                             " published to (topic:, or topics: [...])")
        opts = {k: spec[k] for k in ("security_protocol", "sasl_mechanism",
                                     "ssl_cafile") if spec.get(k)}
        ep = Endpoint(user=str(spec.get("user") or ""),
                      password=str(_secret(spec.get("password") or "")),
                      options={"hosts": servers, **opts})
        self.kafka = KafkaEngine(Hop(name=name, engine="kafka", source=ep,
                                     target=Endpoint()))
        self._consumer = None
        self._tps = None

    def _open(self):
        if self._consumer is None:
            from kafka import TopicPartition
            self._consumer = self.kafka._consumer("src")
            tps = []
            for t in self.topics:
                parts = self._consumer.partitions_for_topic(t)
                if not parts:
                    raise SystemExit(f"the change topic {t} is not on the"
                                     " cluster the hop names")
                tps += [TopicPartition(t, p) for p in sorted(parts)]
            self._tps = tps
            self._consumer.assign(tps)
        return self._consumer

    @staticmethod
    def _lane(tp):
        return f"{tp.topic}:{tp.partition}"

    def lanes(self):
        self._open()
        return [self._lane(tp) for tp in self._tps]

    def ends(self):
        c = self._open()
        return {self._lane(tp): int(o)
                for tp, o in c.end_offsets(self._tps).items()}

    def begins(self):
        c = self._open()
        return {self._lane(tp): int(o)
                for tp, o in c.beginning_offsets(self._tps).items()}

    def read(self, at, most, seconds):
        """([(lane, offset, key, value, topic)], next offsets, caught up):
        every partition from `at` (its beginning where `at` has none - a
        partition added since) until it reaches the end it had when the
        read began, or `most` messages, or `seconds`."""
        c = self._open()
        ends = self.ends()
        begins = self.begins()
        nxt = {}
        for tp in self._tps:
            lane = self._lane(tp)
            pos = at.get(lane)
            pos = begins[lane] if pos is None else int(pos)
            nxt[lane] = pos
            c.seek(tp, pos)
        out = []
        stop = time.monotonic() + seconds
        while len(out) < most and time.monotonic() < stop:
            if all(nxt[lane] >= ends[lane] for lane in nxt):
                break
            got = c.poll(timeout_ms=500, max_records=min(5000,
                                                         most - len(out)))
            for tp, records in got.items():
                lane = self._lane(tp)
                for r in records:
                    out.append((lane, r.offset, r.key, r.value, tp.topic))
                    nxt[lane] = r.offset + 1
        return out, nxt, all(nxt[lane] >= ends[lane] for lane in nxt)

    def lost(self, at):
        begins = self.begins()
        gone = [lane for lane, pos in at.items()
                if lane in begins and int(pos) < begins[lane]]
        if gone:
            return (f"the change topic no longer holds what follows the"
                    f" saved position on {', '.join(gone[:4])}: its"
                    " retention removed it, and what changed then is"
                    " carried by nothing")
        return None

    def room(self, at):
        """{"seconds": ...}: how long the oldest message not yet read is
        kept, from the topic's retention and that message's time."""
        c = self._open()
        try:
            from kafka.admin import ConfigResource, ConfigResourceType
            admin = self.kafka._admin("src")
            got = admin.describe_configs([ConfigResource(
                ConfigResourceType.TOPIC, t) for t in self.topics])
            keep = None
            for res in got:
                for entry in getattr(res, "resources", []):
                    for item in entry[4]:
                        if item[0] == "retention.ms" and int(item[1]) > 0:
                            keep = min(keep or int(item[1]), int(item[1]))
        except Exception:  # noqa: BLE001 - for /metrics only
            return None
        if keep is None:
            return None
        oldest = None
        for tp in self._tps:
            pos = at.get(self._lane(tp))
            if pos is None:
                continue
            c.seek(tp, int(pos))
        got = c.poll(timeout_ms=500, max_records=len(self._tps) * 2)
        for records in got.values():
            for r in records[:1]:
                oldest = min(oldest or r.timestamp, r.timestamp)
        if oldest is None:
            return {"seconds": keep / 1000}
        return {"seconds": max(0.0, (oldest + keep) / 1000 - time.time())}

    def close(self):
        if self._consumer is not None:
            try:
                self._consumer.close()
            finally:
                self._consumer = None


class KinesisLanes:
    """A Kinesis stream's shards, a parent read before its children,
    by sequence number; a shard with no position yet from the time the
    tail began (`AT_TIMESTAMP`), or from its first record where it began
    after that. Signed in to as the Kinesis engine signs in (`region`,
    `endpoint_url`, `user`/`password` as the access key)."""
    KIND = "kinesis"

    def __init__(self, spec, name="changes"):
        from .config import Endpoint, Hop, _secret
        from .engines.kinesis import KinesisEngine
        self.stream = spec.get("stream")
        if not self.stream:
            raise SystemExit("changes: name the Kinesis stream the changes"
                             " are published to (stream: name or ARN)")
        opts = {k: spec[k] for k in ("region", "endpoint_url") if spec.get(k)}
        ep = Endpoint(user=str(spec.get("user") or ""),
                      password=str(_secret(spec.get("password") or "")),
                      options=opts)
        self.client = KinesisEngine(Hop(name=name, engine="kinesis",
                                        source=Endpoint(), target=ep))._client()
        self._by = ({"StreamARN": self.stream}
                    if str(self.stream).startswith("arn:")
                    else {"StreamName": self.stream})

    def _shards(self):
        out, token = [], None
        while True:
            got = self.client.list_shards(
                **({"NextToken": token} if token else self._by))
            out += got.get("Shards", [])
            token = got.get("NextToken")
            if not token:
                break
        ids = {s["ShardId"] for s in out}
        placed, ordered = set(), []
        while len(ordered) < len(out):
            before = len(ordered)
            for s in out:
                parent = s.get("ParentShardId")
                if s["ShardId"] not in placed and (
                        not parent or parent not in ids or parent in placed):
                    ordered.append(s)
                    placed.add(s["ShardId"])
            if len(ordered) == before:
                raise SystemExit(f"the shards of {self.stream} name each"
                                 " other as parents")
        return ordered

    def lanes(self):
        return [s["ShardId"] for s in self._shards()]

    def _iterator(self, shard, pos):
        kw = dict(self._by, ShardId=shard)
        if isinstance(pos, dict) and pos.get("at"):
            kw.update(ShardIteratorType="AT_SEQUENCE_NUMBER",
                      StartingSequenceNumber=pos["at"])
        elif isinstance(pos, dict) and pos.get("after"):
            kw.update(ShardIteratorType="AFTER_SEQUENCE_NUMBER",
                      StartingSequenceNumber=pos["after"])
        elif isinstance(pos, dict) and pos.get("since"):
            kw.update(ShardIteratorType="AT_TIMESTAMP",
                      Timestamp=datetime.datetime.fromtimestamp(
                          float(pos["since"]), datetime.timezone.utc))
        else:
            kw.update(ShardIteratorType="TRIM_HORIZON")
        return self.client.get_shard_iterator(**kw)["ShardIterator"]

    def read(self, at, most, seconds):
        out, nxt, caught = [], dict(at), True
        stop = time.monotonic() + seconds
        for s in self._shards():
            shard = s["ShardId"]
            pos = at.get(shard)
            it = self._iterator(shard, pos)
            while it:
                if len(out) >= most or time.monotonic() > stop:
                    caught = False
                    break
                got = self.client.get_records(ShardIterator=it,
                                              Limit=min(10000,
                                                        most - len(out)))
                for r in got.get("Records", []):
                    out.append((shard, {"at": r["SequenceNumber"]},
                                r.get("PartitionKey"), r["Data"], None))
                    nxt[shard] = {"after": r["SequenceNumber"]}
                it = got.get("NextShardIterator")
                if not got.get("Records") and \
                        int(got.get("MillisBehindLatest") or 0) == 0:
                    break
            if shard not in nxt and pos is not None:
                nxt[shard] = pos
        return out, nxt, caught

    def ends(self):
        """Now, for every shard: the records that arrive from this moment,
        less `SINCE` seconds for this machine's clock against the
        stream's. What is read twice by that is dropped by the fence the
        caller reads after this."""
        since = time.time() - SINCE
        return {shard: {"since": since} for shard in self.lanes()}

    def lost(self, at):
        """Why a saved position is gone: a shard whose oldest record is
        past the one the position starts at."""
        for shard, pos in at.items():
            seq = isinstance(pos, dict) and (pos.get("at")
                                             or pos.get("after"))
            if not seq:
                continue
            try:
                first = self.client.get_records(
                    ShardIterator=self._iterator(shard, None),
                    Limit=1).get("Records", [])
            except Exception:  # noqa: BLE001 - a shard gone with its parent
                continue
            if first and int(first[0]["SequenceNumber"]) > int(seq) + (
                    0 if pos.get("at") else 1):
                return (f"the stream no longer holds what follows the saved"
                        f" position on {shard}: its retention passed, and"
                        " what changed then is carried by nothing")
        return None

    def room(self, at):
        try:
            got = self.client.describe_stream_summary(**self._by)
            hours = got["StreamDescriptionSummary"]["RetentionPeriodHours"]
        except Exception:  # noqa: BLE001 - for /metrics only
            return None
        return {"seconds": float(hours) * 3600}

    def close(self):
        return None


# ---- the buffer that orders what the lanes carry --------------------------

class Follower:
    """The changes of one source database, from its lanes through its
    envelope, in the order the envelope's source promises.

    `name(row)` gives the engine's name for a row's table, or None where
    the table is not this database's or not in the hop's scope.
    `shape(table, row)` gives (key, values) as the engine's own reads give
    them, from what the message held. `read_back(table, rows, version)`
    gives, for rows whose values the source is asked for, [(row, key,
    values or None)] - None where the row is not there. `delivered()`
    gives, for a stream with no marks, the version below which everything
    has been published, asked before a read that goes to every lane's
    end."""

    def __init__(self, envelope, lanes, *, name, shape=None, read_back=None,
                 delivered=None, log=None):
        self.env, self.lanes = envelope, lanes
        self.name, self.shape = name, shape or (lambda t, r: (r.key,
                                                               r.values))
        self.read_back, self.delivered = read_back, delivered
        self.log = log or (lambda m: None)
        self._state = None

    # -- positions --

    def point(self, fence=None, seen=None):
        """Now: every lane at its end, and then `fence()` (the source's own
        now: a change published before the ends committed before it) or
        `seen()` - what changes at or below are in what a copy begun after
        this reads. Asked in that order; the other way round, a change
        committed between the two would be past the fence and before the
        ends, and read by nothing."""
        at = self.lanes.ends()
        return {"via": self.lanes.KIND, "at": at,
                "fence": None if fence is None else int(fence()),
                "marks": {}, "seen": dict(seen() if seen else {})}

    @staticmethod
    def reached(have, want):
        """Whether a tail at `have` has handed on everything `want` (the
        source's position, `point`) names."""
        if not have or not want:
            return None
        if want.get("fence") is not None:
            got = have.get("fence")
            return got is not None and int(got) >= int(want["fence"])
        for sub, (ident, version) in (want.get("seen") or {}).items():
            got = (have.get("seen") or {}).get(sub)
            if got is None:
                if int(version):
                    return False
                continue
            if got[0] != ident or int(got[1]) < int(version):
                return False
        return True

    def lost(self, token):
        return self.lanes.lost((token or {}).get("at") or {})

    def room(self, token):
        return self.lanes.room((token or {}).get("at") or {})

    def close(self):
        self.lanes.close()
        self._state = None

    # -- reading --

    def _fresh(self, token):
        return {"key": json.dumps(token, sort_keys=True, default=str),
                "read": dict(token.get("at") or {}),
                "fence": token.get("fence"),
                "marks": {k: int(v) for k, v in
                          (token.get("marks") or {}).items()},
                "seen": {k: list(v) for k, v in
                         (token.get("seen") or {}).items()},
                "held": [], "ids": set(), "moved": time.monotonic()}

    def changes(self, token, limit=1000):
        """(canon change records, the position after them)."""
        from . import canon
        st = self._state
        if st is None or st["key"] != json.dumps(token, sort_keys=True,
                                                 default=str):
            st = self._state = self._fresh(token)
        before = None
        if len(st["held"]) < HOLD:
            if self.delivered is not None:
                before = self.delivered()
            got, st["read"], caught = self.lanes.read(
                st["read"], max(limit * 4, 5000), 2.0)
            self._take(st, got)
            if before is not None and caught:
                # everything published below `before` was there when the
                # read began, and the read went to every lane's end
                for lane in self.lanes.lanes():
                    was = st["marks"].get(lane)
                    if was is None or before > was:
                        st["marks"][lane] = int(before)
        ready = self._ready(st, limit)
        out = self._records(ready, canon)
        token = self._token(st)
        st["key"] = json.dumps(token, sort_keys=True, default=str)
        self._stalled(st)
        return out, token

    def _take(self, st, got):
        for lane, pos, key, value, topic in got:
            for ev in self.env.decode(key, value, topic):
                if isinstance(ev, Mark):
                    was = st["marks"].get(lane)
                    if was is None or ev.version > was:
                        st["marks"][lane] = ev.version
                    continue
                ev.lane, ev.at = lane, pos
                if ev.sub is None:
                    ev.sub = lane
                table = self.name(ev)
                if table is None:
                    continue
                ev.table = table
                if self.env.ORDER == GLOBAL:
                    if st["fence"] is not None and \
                            ev.version <= int(st["fence"]):
                        continue    # handed on already, or in the copy
                    ident = (ev.ident(), ev.version)
                    if ident in st["ids"]:
                        continue    # delivered twice while held
                    st["ids"].add(ident)
                st["held"].append(ev)

    def _fence_now(self, st):
        lanes = self.lanes.lanes()
        if not lanes or any(lane not in st["marks"] for lane in lanes):
            return st["fence"]
        low = min(st["marks"][lane] for lane in lanes)
        return low if st["fence"] is None else max(low, int(st["fence"]))

    def _ready(self, st, limit):
        """The held rows that may go now, at most `limit` - never half of
        one commit time's - taken out of what is held."""
        if self.env.ORDER == LANE:
            ready, keep = [], []
            for ev in st["held"]:
                seen = st["seen"].get(ev.sub)
                if seen is not None and seen[0] != ev.subid:
                    self.log(f"{ev.sub} was failed over and began again"
                             " (a new UUID): its changes are read on from"
                             " there")
                    seen = None
                # below what the sub-lane handed on is a delivery again;
                # at it, one of several changes sharing a version (a Cosmos
                # DB batch's items), handed on again, which lands on itself
                if seen is not None and ev.version < int(seen[1]):
                    continue
                if len(ready) >= limit:
                    keep.append(ev)
                    continue
                ready.append(ev)
                st["seen"][ev.sub] = [ev.subid, ev.version]
            st["held"] = keep
            return ready
        fence = self._fence_now(st)
        if fence is None:
            return []
        due = sorted((ev for ev in st["held"] if ev.version <= fence),
                     key=lambda ev: ev.version)
        if len(due) > limit:
            cut = due[limit - 1].version
            due = [ev for ev in due if ev.version <= cut]
            fence = cut
        taken = {id(ev) for ev in due}
        st["held"] = [ev for ev in st["held"] if id(ev) not in taken]
        st["ids"] = {i for i in st["ids"] if i[1] > fence}
        if st["fence"] is None or fence > int(st["fence"]):
            st["moved"] = time.monotonic()
        st["fence"] = fence
        return due

    def _records(self, ready, canon):
        """The rows as change records. Read back from the source where the
        envelope's values are not the source's own (`READ_BACK`), or where
        the engine says a message cannot carry a row exactly (`shape`
        answers None): a key's last change in the batch standing for it,
        its row read as it stood at the batch's fence. Otherwise each
        change as the message holds it, in order."""
        if not ready:
            return []
        out, back = [], []
        for r in ready:
            got = None if self.env.READ_BACK else self.shape(r.table, r)
            if got is None:
                back.append(r)
                continue
            key, values = got
            if r.op == "delete":
                out.append(canon.change("delete", r.table, key, txn=r.txn))
            else:
                out.append(canon.change("update", r.table, key, values,
                                        txn=r.txn))
        if back:
            if self.read_back is None:
                raise SystemExit(
                    f"a change to {back[0].table} cannot be carried as the"
                    " feed wrote it, and its source cannot be read back")
            last = {}
            for r in back:
                last[r.ident()] = r
            by_table = {}
            for r in last.values():
                by_table.setdefault(r.table, []).append(r)
            version = max(r.version for r in back)
            for table, rows in by_table.items():
                for r, key, values in self.read_back(table, rows, version):
                    op = "delete" if values is None else "update"
                    out.append(canon.change(op, table, key, values,
                                            txn=r.txn))
        return out

    def _token(self, st):
        """Each lane from its first held row, or where the read stopped."""
        at = dict(st["read"])
        for ev in st["held"]:
            first = at.get(ev.lane)
            if first is None or _before(ev.at, first):
                at[ev.lane] = ev.at
        return {"via": self.lanes.KIND, "at": at, "fence": st["fence"],
                "marks": dict(st["marks"]),
                "seen": {k: list(v) for k, v in st["seen"].items()}}

    def _stalled(self, st):
        """Changes held while the fence has not moved for `STALL` seconds:
        a lane that says nothing of how far it has come holds every other
        lane's changes back with it, which a tail would otherwise wait on
        without a word."""
        if self.env.ORDER != GLOBAL or not st["held"]:
            return
        if time.monotonic() - st["moved"] < STALL:
            return
        lanes = [lane for lane in self.lanes.lanes()
                 if lane not in st["marks"]]
        raise SystemExit(
            f"{len(st['held']):,} changes are held and the feed has not"
            f" said for {STALL // 60} minutes that anything older is past"
            + (f" (nothing from {', '.join(lanes[:4])})" if lanes else "")
            + ": without its marks a change cannot be put in commit order. "
            + MARKS_WANTED.get(self.env.name, ""))


#: how each source is asked to send the marks the order needs
MARKS_WANTED = {
    "canal-json": "Create the TiCDC changefeed with"
                  " enable-tidb-extension=true.",
    "simple": "Check the TiCDC changefeed is running.",
    "changefeed-json": "Make the changefeed WITH resolved.",
    "dsql": "Check the DSQL stream is ACTIVE.",
}


def _before(a, b):
    """Whether lane position `a` comes before `b`: offsets, or Kinesis
    sequence numbers, where starting at a record comes before starting
    after it and a moment before either."""
    if isinstance(a, dict) or isinstance(b, dict):
        def start(p):
            if isinstance(p, dict) and p.get("at"):
                return (int(p["at"]), 0)
            if isinstance(p, dict) and p.get("after"):
                return (int(p["after"]), 1)
            return (-1, 0)
        return start(a) < start(b)
    try:
        return int(a) < int(b)
    except (TypeError, ValueError):
        return False


# ---- values out of text ---------------------------------------------------

def from_text(cls, text, binary="latin-1"):
    """A value of canonical class `cls` from the text a feed wrote it as -
    the value the engine's own read would give. `binary`: how the feed
    wrote bytes (`latin-1`: a character a byte, as TiCDC's canal-json;
    `base64`; `hex`: `\\x...`)."""
    from . import canon, nanotime
    if text is None:
        return None
    if cls == "bytes":
        if isinstance(text, (bytes, bytearray)):
            return bytes(text)
        if binary == "base64":
            return base64.b64decode(text)
        if binary == "hex" and str(text).startswith("\\x"):
            return bytes.fromhex(str(text)[2:])
        return str(text).encode("latin-1")
    if cls in ("integer", "float", "decimal", "boolean"):
        if isinstance(text, bool) and cls == "boolean":
            return text
        return canon.from_text(cls, str(text))
    if cls == "date":
        return datetime.date.fromisoformat(str(text)[:10])
    if cls in ("timestamp", "time"):
        try:
            return nanotime.parse(str(text).replace("T", " ").rstrip("Z"))
        except ValueError:
            return str(text)
    return text


def key_text(cls, value):
    """A key's value as one text, for matching what was read back to what
    the feed named."""
    from . import canon
    if value is None:
        return None
    try:
        return canon.render_value(cls or "text", value)
    except Exception:  # noqa: BLE001 - a class the renderer does not take
        return str(value)


# ---- the cloud-native services' own feeds (P0-CLOUDS part 2) ---------------
#
# Each engine's lanes hand these a record already read off the service's
# own client, as a dict; `{"mark": version}` is a lane saying nothing at or
# below it is still to come.

class SpannerChanges(Envelope):
    """A Spanner change stream's data change records, as `SpannerLanes`
    reads them from either dialect: the commit timestamp in nanoseconds
    (TrueTime, one clock for every partition), the table, what was done,
    and each changed row's key. A partition's records come in commit
    order, so each is also a mark just below its own time; its heartbeats
    and the end of a window it was read through are marks too."""
    name = "spanner-change-stream"
    READ_BACK = True
    LOSES = ("nothing: each changed row is read back from Spanner as it"
             " stood at its batch's commit timestamp, within the database's"
             " version retention")

    def decode(self, key, value, topic=None):
        if not value:
            return []
        if "mark" in value:
            return [Mark(value["mark"])]
        ts = int(value["commit_ns"])
        op = "delete" if value.get("mod_type") == "DELETE" else "upsert"
        out = [Row(None, value.get("table_name"), op, dict(m["keys"]), None,
                   ts, txn=ts) for m in value.get("mods") or []]
        return out + [Mark(ts - 1)]


class BigtableChanges(Envelope):
    """A Bigtable change stream's data changes, as `BigtableLanes` reads
    them: the row's key (base64), its commit time in microseconds, and the
    partition's low watermark - below which every change of the partition
    has been sent - as the partition's mark. A garbage collector's
    deletion is a change as any other. The rows are read back as they are
    when the change is read: Bigtable reads no row as it stood at a
    moment, and a later change brings it level again."""
    name = "bigtable-change-stream"
    READ_BACK = True
    LOSES = ("nothing a later change does not bring level again: each row"
             " is read back as it is when its change is read")

    def decode(self, key, value, topic=None):
        if not value:
            return []
        if "mark" in value:
            return [Mark(value["mark"])]
        ts = int(value["commit_us"])
        return [Row(None, topic, "changed", {"row_key": value["row_key"]},
                    None, ts, txn=ts), Mark(ts - 1)]


class KeyspacesChanges(Envelope):
    """An Amazon Keyspaces CDC stream's records, as `KeyspacesLanes` reads
    them from the stream's shards: a row's key columns, its new image
    where the stream carries one, the operation, and its sequence number
    in the shard - the order of a key's changes, which one shard holds.
    A stream of keys only has its rows read back as they are."""
    name = "keyspaces-cdc"
    ORDER = LANE
    LOSES = ("with a stream of keys only, the values between two reads,"
             " which a copy does not need; with new images, nothing")

    def decode(self, key, value, topic=None):
        if not value:
            return []
        op = "delete" if value.get("op") == "delete" else "upsert"
        return [Row(None, value.get("table"), op, dict(value["key"]),
                    value.get("image") if op == "upsert" else None,
                    int(value["sequence"]))]


class NeptuneChanges(Envelope):
    """Neptune's stream, as `NeptuneLanes` reads it: one log for the whole
    cluster, each change at its commit and its place in it (`commitNum`
    and `opNum` as one number). A property graph's change names an
    element - a vertex or an edge - which is read back as it is; an RDF
    change is a quad added or removed, which is the whole of it."""
    name = "neptune-stream"
    LOSES = ("between two reads, the states of an element a later change"
             " brings level again: Neptune reads no element as it stood at"
             " a commit")

    def decode(self, key, value, topic=None):
        if not value:
            return []
        if "mark" in value:
            return [Mark(value["mark"])]
        version = int(value["version"])
        return [Row(None, value["table"], value["op"], dict(value["key"]),
                    value.get("values"), version, txn=value.get("commit")),
                Mark(version - 1)]


ENVELOPES.update({e.name: e for e in (SpannerChanges, BigtableChanges,
                                      KeyspacesChanges, NeptuneChanges)})
