"""Two Kafka clusters in memory, enough of the client for the topic copier.

No broker is reached. What is modelled is what the offsets depend on, so a
test can say what a real log would do:

* a partition's log is a list of records at consecutive offsets; a
  transaction's commit or abort takes an offset of its own (a control
  record), an aborted write keeps its offset and is never given to a reader
  of committed messages, and a write that is still open stops the last
  stable offset;
* `DeleteRecords` moves a partition's first offset up, never down, and
  never past its end; it can be refused;
* a producer with a `transactional_id` that starts again ends what the
  last one left open (`init_transactions`), as the broker does;
* a crash is a `Crash` (a `BaseException`, so nothing the engine catches
  on a failure runs - the process is gone and the transaction stays open);
* a consumer asked to read from an offset the log no longer holds is moved
  to the log's end, as the real client does by default (`auto_offset_reset`
  is `latest`), unless it was made with `earliest`;
* the admin calls the assessment asks: a broker's and a topic's settings
  (`describe_configs`), a topic's replicas (`describe_topics`), the
  producers a partition holds state for (`describe_producers`) and the
  brokers' log directories (`describe_log_dirs`), each answering what the
  test put on the cluster, or refusing where the test says it does.

What it does not model, and a test here therefore does not prove: the
broker's wire protocol, replication, leader changes, producer epochs
beyond the one rule above, compaction running by itself, and the real
client's timing. `tests/` has docker-gated tests for those; this file is
how the arithmetic is held without one.
"""
import collections
import json
import sys
import types

TopicPartition = collections.namedtuple("TopicPartition",
                                        "topic partition")
OffsetAndMetadata = collections.namedtuple("OffsetAndMetadata",
                                           "offset metadata leader_epoch")
OffsetAndTimestamp = collections.namedtuple("OffsetAndTimestamp",
                                            "offset timestamp")
Msg = collections.namedtuple(
    "Msg", "topic partition offset key value headers timestamp")
Meta = collections.namedtuple("Meta", "topic partition offset")


class Crash(BaseException):
    """The process is gone: nothing after this point runs."""


class Rec:
    def __init__(self, offset, key, value, headers, timestamp, kind="data",
                 state="committed", txn=None):
        self.offset, self.key, self.value = offset, key, value
        self.headers, self.timestamp = headers, timestamp
        self.kind, self.state, self.txn = kind, state, txn


class Log:
    def __init__(self):
        self.recs = []
        self.start = 0

    @property
    def end(self):
        return len(self.recs)

    @property
    def stable(self):
        for r in self.recs:
            if r.state == "open":
                return r.offset
        return len(self.recs)

    def append(self, **kw):
        r = Rec(len(self.recs), **kw)
        self.recs.append(r)
        return r


class Cluster:
    def __init__(self, name="cluster"):
        self.name = name
        self.topics = {}
        self.open = {}          # transactional id -> (token, partitions)
        self.groups = {}        # group -> {TopicPartition: offset}
        self.deny_delete = False
        self.hooks = collections.defaultdict(list)
        self.deleted = []       # every DeleteRecords asked: (tp, offset)
        self.created = []
        self.producers = []     # the config of every producer made
        self.refuse = set()     # topics whose sends the broker refuses
        self.sequence = 0       # transactions begun, to tell them apart
        self.broker_ids = [1]
        self.cluster_id = f"id-{name}"
        self.broker_configs = {}    # setting -> value, of every broker
        self.topic_settings = {}    # topic -> {setting: (value, source)}
        self.replication = {}       # topic -> copies of each partition
        self.producer_state = {}    # (topic, partition) -> [producer]
        self.log_dirs = None        # what describe_log_dirs answers
        self.api_version = None     # what the client library infers
        self.refuse_calls = set()   # admin calls the cluster refuses
        self.no_producer_state = False  # the client has no such call

    # -- building a log the way a producer did ----
    def topic(self, name, partitions=1):
        self.topics.setdefault(name, [Log() for _ in range(partitions)])
        return self.topics[name]

    def log(self, topic, p=0):
        return self.topics[topic][p]

    def put(self, topic, p, key, value, ts=1_700_000_000_000, headers=None):
        return self.log(topic, p).append(
            key=key, value=value, headers=headers or [],
            timestamp=ts).offset

    def put_many(self, topic, p, n, prefix=b"m", ts=1_700_000_000_000):
        for i in range(n):
            self.put(topic, p, b"k%d" % i, prefix + b"%d" % i, ts + i)

    def hole(self, topic, p, n=1):
        """A compaction hole: offsets that hold no message."""
        for _ in range(n):
            self.log(topic, p).append(key=None, value=None, headers=[],
                                      timestamp=0, kind="control")

    def transaction(self, topic, p, n, commit=True, prefix=b"t"):
        """A transaction written: its messages, then its marker."""
        log = self.log(topic, p)
        state = "committed" if commit else "aborted"
        for i in range(n):
            log.append(key=b"t%d" % i, value=prefix + b"%d" % i, headers=[],
                       timestamp=1_700_000_100_000 + i, state=state)
        log.append(key=None, value=None, headers=[], timestamp=0,
                   kind="control")

    def fire(self, name, *args):
        for hook in list(self.hooks[name]):
            hook(*args)

    def messages(self, topic, p=0, committed=True):
        """(offset, key, value) of what a reader is given."""
        log = self.log(topic, p)
        out = []
        for r in log.recs[log.start:]:
            if r.kind != "data":
                continue
            if committed and (r.state != "committed"
                              or r.offset >= log.stable):
                continue
            out.append((r.offset, r.key, r.value))
        return out


class Future:
    def __init__(self, rec=None, error=None):
        self.rec, self.error = rec, error

    def get(self, timeout=None):
        if self.error:
            raise self.error
        return Meta(None, None, self.rec.offset)

    def failed(self):
        return self.error is not None

    @property
    def exception(self):
        return self.error


class Producer:
    def __init__(self, cluster, **cfg):
        self.cluster = cluster
        self.cfg = cfg
        cluster.producers.append(cfg)
        self.tid = cfg.get("transactional_id")
        self.token = None
        self.inside = False
        self.touched = []
        self.started = False

    def send(self, topic, value=None, key=None, headers=None, partition=None,
             timestamp_ms=None):
        log = self.cluster.log(topic, partition)
        if self.tid and not self.inside:
            raise RuntimeError("send outside a transaction")
        if topic in self.cluster.refuse:
            return Future(error=RuntimeError("MessageSizeTooLargeError"))
        r = log.append(key=key, value=value, headers=list(headers or []),
                       timestamp=timestamp_ms or 0,
                       state="open" if self.inside else "committed",
                       txn=self.token if self.inside else None)
        if self.inside and (topic, partition) not in self.touched:
            self.touched.append((topic, partition))
        self.cluster.fire("send", self, topic, partition, r)
        return Future(r)

    def flush(self):
        self.cluster.fire("flush", self)

    def init_transactions(self):
        if not self.tid:
            raise RuntimeError("no transactional id")
        token, touched = self.cluster.open.pop(self.tid, (None, ()))
        for topic, p in touched:
            self._end(topic, p, "aborted", only=token)
        self.started = True

    def begin_transaction(self):
        assert self.started and not self.inside
        self.inside = True
        self.touched = []
        self.cluster.sequence += 1
        self.token = f"{self.tid}#{self.cluster.sequence}"
        self.cluster.open[self.tid] = (self.token, self.touched)

    def _end(self, topic, p, state, only):
        log = self.cluster.log(topic, p)
        for r in log.recs:
            if r.state == "open" and r.txn == only:
                r.state = state
        log.append(key=None, value=None, headers=[], timestamp=0,
                   kind="control")

    def commit_transaction(self):
        assert self.inside
        self.cluster.fire("commit", self)
        for topic, p in self.touched:
            self._end(topic, p, "committed", self.token)
        self.cluster.open.pop(self.tid, None)
        self.inside = False

    def abort_transaction(self):
        assert self.inside
        self.cluster.fire("abort", self)
        for topic, p in self.touched:
            self._end(topic, p, "aborted", self.token)
        self.cluster.open.pop(self.tid, None)
        self.inside = False

    def close(self):
        pass


class _Brokers:
    def __init__(self, cluster):
        self.cluster = cluster
        self.cluster_id = cluster.cluster_id

    def brokers(self):
        return [types.SimpleNamespace(node_id=i)
                for i in self.cluster.broker_ids]


class Consumer:
    def __init__(self, cluster, isolation_level="read_uncommitted", **cfg):
        self.cluster = cluster
        self.committed = isolation_level == "read_committed"
        self.reset = cfg.get("auto_offset_reset", "latest")
        self.pos = {}
        self.tps = []
        self._client = types.SimpleNamespace(cluster=_Brokers(cluster))

    def topics(self):
        return set(self.cluster.topics)

    def partitions_for_topic(self, topic):
        return set(range(len(self.cluster.topics.get(topic, [])))) or None

    def assign(self, tps):
        self.tps = list(tps)
        for tp in self.tps:
            self.pos.setdefault(tp, self.cluster.log(*tp).start)

    def seek(self, tp, offset):
        self.pos[tp] = offset

    def seek_to_beginning(self, tp):
        self.pos[tp] = self.cluster.log(*tp).start

    def position(self, tp):
        log = self.cluster.log(*tp)
        return max(self.pos.get(tp, log.start), log.start)

    def end_offsets(self, tps):
        out = {}
        for tp in tps:
            log = self.cluster.log(*tp)
            out[tp] = log.stable if self.committed else log.end
        return out

    def beginning_offsets(self, tps):
        return {tp: self.cluster.log(*tp).start for tp in tps}

    def offsets_for_times(self, wanted):
        out = {}
        for tp, ts in wanted.items():
            log = self.cluster.log(*tp)
            hit = [r for r in log.recs[log.start:]
                   if r.kind == "data" and r.timestamp >= ts]
            out[tp] = OffsetAndTimestamp(hit[0].offset, hit[0].timestamp) \
                if hit else None
        return out

    def poll(self, timeout_ms=0, max_records=500):
        out = {}
        for tp in self.tps:
            log = self.cluster.log(*tp)
            limit = log.stable if self.committed else log.end
            at = self.pos.get(tp, log.start)
            if at < log.start:
                # out of range: the client's reset, by default to the end
                at = log.start if self.reset == "earliest" else limit
            got = []
            while at < limit and len(got) < max_records:
                r = log.recs[at]
                at += 1
                if r.kind != "data":
                    continue
                if self.committed and r.state != "committed":
                    continue
                got.append(Msg(tp.topic, tp.partition, r.offset, r.key,
                               r.value, r.headers, r.timestamp))
            self.pos[tp] = at
            if got:
                out[tp] = got
        return out

    def close(self):
        pass


class Admin:
    def __init__(self, cluster):
        self.cluster = cluster
        self.config = {"api_version": cluster.api_version}
        self._client = types.SimpleNamespace(cluster=_Brokers(cluster))

    def _asked(self, call):
        if call in self.cluster.refuse_calls:
            raise PermissionError(f"{call}: ClusterAuthorizationFailedError")

    def create_topics(self, topics):
        for t in topics:
            self.cluster.created.append(t)
            self.cluster.topic(t.name, t.num_partitions)
            self.cluster.replication[t.name] = t.replication_factor
            self.cluster.topic_settings.setdefault(t.name, {}).update(
                {k: (v, "DYNAMIC_TOPIC_CONFIG")
                 for k, v in t.topic_configs.items()})

    def describe_configs(self, resources, include_synonyms=False,
                         config_filter="modified"):
        self._asked("describe_configs")
        out = {}
        for r in resources:
            if r.resource_type == ConfigResourceType.BROKER:
                got = {k: (v, "STATIC_BROKER_CONFIG")
                       for k, v in self.cluster.broker_configs.items()}
            else:
                got = dict(self.cluster.topic_settings.get(r.name, {}))
                if config_filter == "all":
                    for k, v in TOPIC_DEFAULTS.items():
                        got.setdefault(k, (v, "DEFAULT_CONFIG"))
                else:
                    got = {k: x for k, x in got.items()
                           if x[1] == "DYNAMIC_TOPIC_CONFIG"}
            out.setdefault(r.resource_type, {})[r.name] = {
                k: {"value": v, "config_source": src, "is_sensitive": False}
                for k, (v, src) in got.items()}
        return out

    def describe_topics(self, topics=None):
        self._asked("describe_topics")
        out = []
        for t in topics or sorted(self.cluster.topics):
            if t not in self.cluster.topics:
                continue
            copies = self.cluster.replication.get(t, 1)
            out.append({"name": t, "partitions": [
                {"partition_index": i, "leader_id": 1,
                 "replica_nodes": list(range(1, copies + 1))}
                for i in range(len(self.cluster.topics[t]))]})
        return out

    def describe_producers(self, partitions):
        self._asked("describe_producers")
        if self.cluster.no_producer_state:
            raise AttributeError("describe_producers")
        return {tp: types.SimpleNamespace(
            active_producers=list(self.cluster.producer_state.get(
                (tp.topic, tp.partition), ()))) for tp in partitions}

    def describe_log_dirs(self):
        self._asked("describe_log_dirs")
        if self.cluster.log_dirs is None:
            raise AttributeError("describe_log_dirs")
        return self.cluster.log_dirs

    def delete_records(self, wanted):
        out = {}
        for tp, offset in wanted.items():
            if self.cluster.deny_delete:
                raise PermissionError("TopicAuthorizationFailedError")
            log = self.cluster.log(*tp)
            if offset > log.end:
                raise ValueError("OffsetOutOfRange")
            self.cluster.deleted.append((tp, offset))
            log.start = max(log.start, offset)
            out[tp] = {"low_watermark": log.start}
        return out

    def list_groups(self):
        return [{"group_id": g} for g in self.cluster.groups]

    def list_group_offsets(self, group):
        held = self.cluster.groups.get(group, {})
        return {group: {tp: OffsetAndMetadata(o, "", -1)
                        for tp, o in held.items()}}

    def alter_group_offsets(self, group, offsets):
        """Commit offsets for a group, making it where it is new; the
        answer per partition, `None` being no error."""
        held = self.cluster.groups.setdefault(group, {})
        for tp, meta in offsets.items():
            held[tp] = meta.offset
        return {tp: None for tp in offsets}

    def delete_consumer_groups(self, groups):
        for group in groups:
            self.cluster.groups.pop(group, None)

    def close(self):
        pass


#: what a topic reports of the settings it does not set itself
TOPIC_DEFAULTS = {"cleanup.policy": "delete", "retention.ms": "604800000",
                  "retention.bytes": "-1", "max.message.bytes": "1048588",
                  "min.insync.replicas": "1", "compression.type": "producer",
                  "delete.retention.ms": "86400000", "segment.ms": "604800000",
                  "segment.bytes": "1073741824"}


def producer(producer_id, last_timestamp):
    """What the brokers hold of one active producer of a partition."""
    return types.SimpleNamespace(producer_id=producer_id, producer_epoch=0,
                                 last_sequence=0,
                                 last_timestamp=last_timestamp,
                                 coordinator_epoch=0,
                                 current_transaction_start_offset=-1)


def log_dir(topics, usable, total=None):
    """One broker's answer to DescribeLogDirs: `topics` is {topic:
    {partition: bytes}}, `usable` the bytes free."""
    return {"log_dirs": [{
        "log_dir": "/data", "error_code": 0, "total_bytes": total or usable,
        "usable_bytes": usable,
        "topics": [{"name": t, "partitions": [
            {"partition_index": p, "partition_size": n}
            for p, n in sorted(parts.items())]}
            for t, parts in sorted(topics.items())]}]}


class ConfigResourceType:
    TOPIC = "topic"
    BROKER = "broker"


class ConfigResource:
    def __init__(self, resource_type, name, configs=None):
        self.resource_type, self.name = resource_type, name
        self.configs = configs


class NewTopic:
    def __init__(self, name, num_partitions=1, replication_factor=1,
                 topic_configs=None):
        self.name, self.num_partitions = name, num_partitions
        self.replication_factor = replication_factor
        self.topic_configs = topic_configs or {}


class TransactionalProducer:
    """The client class the engine asks whether it can run transactions."""
    def init_transactions(self):
        pass


class PlainProducer:
    """A client that cannot."""


def install(monkeypatch, transactions=True):
    """The `kafka` package replaced by the stand-ins, for one test."""
    kafka = types.ModuleType("kafka")
    kafka.TopicPartition = TopicPartition
    kafka.KafkaProducer = TransactionalProducer if transactions \
        else PlainProducer
    admin = types.ModuleType("kafka.admin")
    admin.NewTopic = NewTopic
    structs = types.ModuleType("kafka.structs")
    structs.OffsetAndMetadata = OffsetAndMetadata
    admin.ConfigResource = ConfigResource
    admin.ConfigResourceType = ConfigResourceType
    kafka.admin, kafka.structs = admin, structs
    monkeypatch.setitem(sys.modules, "kafka", kafka)
    monkeypatch.setitem(sys.modules, "kafka.admin", admin)
    monkeypatch.setitem(sys.modules, "kafka.structs", structs)


def engine(src, dst, real_settings=False, where=None, **options):
    """A KafkaEngine whose clients are the in-memory clusters. The topics'
    settings are not asked of them (`{}`) unless `real_settings`; `where`
    is the directory the hop keeps its files in."""
    from migkit.config import Endpoint, Hop
    from migkit.engines.kafka import KafkaEngine
    hop = Hop(name="k", engine="kafka",
              source=Endpoint(host="10.0.0.1", port=9092, user="",
                              password=""),
              target=Endpoint(host="10.0.0.2", port=9092, user="",
                              password=""),
              options=dict(options))
    eng = KafkaEngine(hop)
    side = {"src": src, "dst": dst}
    eng._consumer = lambda s, **kw: Consumer(side[s], **kw)
    eng._producer = lambda **kw: Producer(dst, **kw)
    eng._admin = lambda s: Admin(side[s])
    if not real_settings:
        eng._topic_configs = lambda s, topics: {}
    if where is not None:
        hop.report_dir = lambda db=None: where
    return eng


class Copy:
    """A topic copied round by round with the state saved the way the
    copier saves it: only what `save()` was called with survives a crash."""

    def __init__(self, src, dst, topic="orders", **options):
        self.src, self.dst, self.topic, self.options = src, dst, topic, options
        self.saved = "{}"
        self.state = {}
        self.said = []
        self.restart()

    def restart(self):
        """A new process: a new engine, the state as last saved."""
        self.eng = engine(self.src, self.dst, **self.options)
        self.state = json.loads(self.saved)

    def save(self):
        self.saved = json.dumps(self.state, sort_keys=True)

    def run(self, chunk=5000):
        return self.eng._copy_topic(self.topic, self.state, chunk, self.save,
                                    self.said.append)

    def partition(self, p=0):
        return self.state["parts"][str(p)]
