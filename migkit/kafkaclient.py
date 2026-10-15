"""The stream client a Kafka target is written through, and the codec its
messages go in (backlog R18.7).

Two clients can write a topic: a compiled one, about twice the messages a
second, and the pure-Python one migkit has always used. Which one is a
`decide` climb over facts (installed, how the side signs in) with the
hop's own measured rate a client (`HopCosts`), never an import tried and
dropped in silence. The codec is a second climb, over what was measured:
the payload's compressibility, read off the first messages to be sent,
and whether the target's brokers take zstd (the Produce request's version
they answer with, else the version they run). zstd, then lz4, then none;
the choice and why are kept in the run's decision record.
"""
import collections
import logging
import uuid

from . import decide

#: a sample that compresses to more than this share of its size is sent
#: as it is: the brokers' and consumers' work buys nothing
COMPRESSION_PAYS = 0.9
#: the oldest broker that takes zstd, and the Produce request's version
#: that came with it
ZSTD_BROKER = (2, 1)
ZSTD_PRODUCE = 7
#: messages and bytes of a sample, at most
SAMPLE_MESSAGES = 200
SAMPLE_BYTES = 1 << 20

#: the compiled client's own lines go here, not to the operator's terminal
LOG = logging.getLogger("migkit.stream")


def native_missing():
    try:
        import confluent_kafka  # noqa: F401
    except ImportError:
        return "the compiled stream client is not installed here"
    return None


def pure_missing():
    try:
        import kafka  # noqa: F401
    except ImportError:
        return "the pure-Python stream client is not installed here"
    return None


def _client_rungs():
    from .decide import Need, Rung
    return (
        Rung("native", "the compiled stream client",
             gives=frozenset({"exact"}),
             needs=(Need("the compiled stream client",
                         lambda f: f["native_missing"] is None,
                         lambda f: f["native_missing"]),
                    Need("a sign-in it carries",
                         lambda f: not f["cloud_sign_in"],
                         "the side signs in with the cloud's own"
                         " credentials, which {rung} does not carry")),
             because="it batches and compresses in compiled code, about"
                     " twice the messages a second"),
        Rung("pure", "the pure-Python stream client",
             gives=frozenset({"exact"}),
             needs=(Need("the pure-Python stream client",
                         lambda f: f["pure_missing"] is None,
                         lambda f: f["pure_missing"]),),
             because="the stream client installed here"),
    )


CLIENTS = _client_rungs()


def client_way(cloud_sign_in=False, costs=None, native=None, pure=None):
    """The `Choice` of the client a target is written through. `native`
    and `pure` stand in for the installation checks (tests)."""
    facts = decide.Facts(
        {"cloud_sign_in": bool(cloud_sign_in)},
        native_missing=native or native_missing,
        pure_missing=pure or pure_missing)
    got = decide.climb("stream client", CLIENTS, facts, need=("exact",),
                       costs=costs)
    if got.rung is None:
        raise SystemExit("no stream client can write the target here ("
                         + got.reason + "): migkit doctor --install puts"
                         " it in place")
    return got


def sample_of(values):
    """The first values to be sent, up to `SAMPLE_MESSAGES` and
    `SAMPLE_BYTES`."""
    out, size = [], 0
    for v in values:
        if not v:
            continue
        v = bytes(v)
        out.append(v)
        size += len(v)
        if len(out) >= SAMPLE_MESSAGES or size >= SAMPLE_BYTES:
            break
    return out


def ratios(sample):
    """{codec: compressed size / size} of the sample, measured; {} where
    there is nothing to measure."""
    data = b"".join(sample_of(sample))
    if not data:
        return {}
    out = {}
    try:
        import zstandard
        out["zstd"] = len(zstandard.ZstdCompressor(level=1).compress(data)
                          ) / len(data)
    except ImportError:
        pass
    try:
        import lz4.frame
        out["lz4"] = len(lz4.frame.compress(data)) / len(data)
    except ImportError:
        # lz4 compresses less than zstd: what zstd measured is its best
        if "zstd" in out:
            out["lz4"] = min(1.0, out["zstd"] * 1.25)
    return out


def target_takes_zstd(produce_version=None, broker_version=None):
    """True, False, or None where neither the Produce request's version
    the brokers answer with nor the version they run is known."""
    if produce_version is not None:
        return int(produce_version) >= ZSTD_PRODUCE
    if broker_version is not None:
        return tuple(broker_version)[:2] >= ZSTD_BROKER
    return None


def _pays(codec):
    def holds(f):
        got = f["ratios"].get(codec)
        return got is not None and got <= COMPRESSION_PAYS

    def why(f):
        got = f["ratios"].get(codec)
        if got is None:
            return "no message was there to measure how well it compresses"
        return (f"the first messages compress only to {got:.0%} of their"
                " size")
    return decide.Need("a payload that compresses", holds, why)


def _taken(codec):
    """The target has not refused `codec` at send time (`Stepping`)."""
    return decide.Need(f"a target that takes {codec} when sent",
                       lambda f: codec not in f["refused"],
                       lambda f: "the target refused it at send time: "
                       + f["refused"][codec].replace("{", "(").replace(
                           "}", ")"))


def _codec_rungs():
    from .decide import Need, Rung
    return (
        Rung("zstd", "zstd compression",
             needs=(Need("zstd in the client", lambda f: f["has_zstd"],
                         "the stream client here has no zstd"),
                    _taken("zstd"),
                    Need("brokers that take zstd",
                         lambda f: f["zstd_target"] is True,
                         lambda f: "the target's brokers are older than"
                         " 2.1, which zstd needs" if f["zstd_target"] is
                         False else "the target's brokers did not say a"
                         " version, and zstd needs 2.1"),
                    _pays("zstd")),
             because="the smallest on the wire for what it costs"),
        Rung("lz4", "lz4 compression",
             needs=(Need("lz4 in the client", lambda f: f["has_lz4"],
                         "the stream client here has no lz4"),
                    _taken("lz4"),
                    _pays("lz4")),
             because="every broker that carries headers takes it"),
        Rung("none", "messages sent as they are",
             because="nothing to gain from compressing"),
    )


CODECS = _codec_rungs()


def codec(sample, zstd_target, has_zstd=True, has_lz4=True, costs=None,
          refused=None):
    """The `Choice` of the codec a producer sends in. `refused` is
    {codec: the target's words} for codecs it refused at send time."""
    facts = decide.Facts({"ratios": ratios(sample),
                          "zstd_target": zstd_target,
                          "has_zstd": bool(has_zstd),
                          "has_lz4": bool(has_lz4),
                          "refused": dict(refused or {})})
    return decide.climb("message compression", CODECS, facts, costs=costs)


def codecs_of(way):
    """(has zstd, has lz4) of a client way."""
    if way == "native":
        # the compiled client's wheels are built with both
        return True, True
    try:
        from kafka import codec as c
        return bool(c.has_zstd()), bool(c.has_lz4())
    except ImportError:
        return False, False


def can_transact(way):
    if way == "native":
        return True
    try:
        from kafka import KafkaProducer
    except ImportError:
        return False
    return callable(getattr(KafkaProducer, "init_transactions", None))


class StreamError(Exception):
    """A send the compiled client reports as failed, with its error's
    name (`code`)."""

    def __init__(self, err):
        self.code = str(getattr(err, "name", lambda: err)()
                        if callable(getattr(err, "name", None)) else err)
        super().__init__(str(getattr(err, "str", lambda: err)()
                             if callable(getattr(err, "str", None))
                             else err))

    @property
    def codec_refused(self):
        return "UNSUPPORTED_COMPRESSION" in self.code.upper()


class Refused(SystemExit):
    """A sign-in the cluster refused, said as one."""


class Sent:
    """One send's outcome, as the copier reads it: `failed()`,
    `exception`, and `get()` for where it landed (`.offset`)."""

    def __init__(self, owner):
        self._owner = owner
        self.done = False
        self.exception = None
        self.offset = self.partition = None

    def _landed(self, err, msg):
        self.done = True
        if err is not None:
            self.exception = StreamError(err)
        else:
            self.offset, self.partition = msg.offset(), msg.partition()

    def failed(self):
        return self.done and self.exception is not None

    def succeeded(self):
        return self.done and self.exception is None

    def get(self, timeout=None):
        if not self.done:
            self._owner.flush()
        if self.exception is not None:
            raise self.exception
        return self


#: the pure client's settings, as the compiled client names them
_KEYS = {"acks": "acks", "request_timeout_ms": "request.timeout.ms",
         "max_in_flight_requests_per_connection":
             "max.in.flight.requests.per.connection",
         "enable_idempotence": "enable.idempotence",
         "transactional_id": "transactional.id",
         "compression_type": "compression.type", "linger_ms": "linger.ms"}


def native_sign_in(conn):
    """The compiled client's settings for where a cluster is and how it
    is signed in to, from the connection the pure client is given."""
    conf = {"bootstrap.servers": ",".join(conn["bootstrap_servers"]),
            "logger": LOG}
    if conn.get("security_protocol"):
        conf["security.protocol"] = conn["security_protocol"]
    if conn.get("sasl_mechanism"):
        conf["sasl.mechanism"] = conn["sasl_mechanism"]
    if conn.get("sasl_plain_username") is not None:
        conf["sasl.username"] = conn["sasl_plain_username"]
        conf["sasl.password"] = conn.get("sasl_plain_password") or ""
    if conn.get("ssl_cafile"):
        conf["ssl.ca.location"] = conn["ssl_cafile"]
    return conf


def native_config(conn, **kw):
    """The compiled client's settings from the connection and the
    producer's settings as the pure client names them. Keys go to their
    partition by the same hash the pure client and Java's use, so a key's
    messages land where they always did."""
    conf = {**native_sign_in(conn), "partitioner": "murmur2_random",
            "message.timeout.ms": 120000}
    for k, v in kw.items():
        if k == "max_request_size":
            conf["message.max.bytes"] = min(int(v), 1_000_000_000)
        elif k == "buffer_memory":
            conf["queue.buffering.max.kbytes"] = max(1, int(v) // 1024)
        elif k in _KEYS and v is not None:
            conf[_KEYS[k]] = v
    return conf


class _SignIn:
    """A compiled client's sign-in, said as refused where the cluster
    refused it: the client retries a refused handshake and reports it
    only through its error callback."""

    def _error(self, err):
        if "AUTHENTICATION" in str(getattr(err, "name", lambda: "")()):
            self._auth.append(str(err.str()) if callable(
                getattr(err, "str", None)) else str(err))

    def _check(self):
        if self._auth:
            raise Refused(
                f"the {self._side} cluster refused the sign-in:"
                f" {self._auth[-1]}. Check the endpoint's user and password,"
                " and its sasl_mechanism")


class NativeProducer(_SignIn):
    """The compiled client's producer, answering as the copier asks of a
    producer: `send` returning a `Sent`, `flush`, `close` and the
    transaction calls."""

    def __init__(self, conn, side="target", **kw):
        from confluent_kafka import Producer
        self._side = side
        self._auth = []
        conf = native_config(conn, **kw)
        conf["error_cb"] = self._error
        self._p = Producer(conf)

    def send(self, topic, value=None, key=None, headers=None,
             partition=None, timestamp_ms=None):
        sent = Sent(self)
        kw = {"value": value, "key": key, "on_delivery": sent._landed}
        if headers:
            kw["headers"] = list(headers)
        if partition is not None:
            kw["partition"] = int(partition)
        if timestamp_ms is not None and int(timestamp_ms) > 0:
            kw["timestamp"] = int(timestamp_ms)
        while True:
            try:
                self._p.produce(topic, **kw)
                break
            except BufferError:
                # the queue is full: deliveries made room once served
                self._p.poll(0.1)
        self._p.poll(0)
        return sent

    def flush(self, timeout=None):
        while self._p.flush(30 if timeout is None else timeout) > 0:
            self._check()
            if timeout is not None:
                break
        self._check()

    def close(self, timeout=None):
        self.flush()
        closing = getattr(self._p, "close", None)
        if callable(closing):
            closing()

    def init_transactions(self, timeout=30):
        self._p.init_transactions(timeout)

    def begin_transaction(self):
        self._p.begin_transaction()

    def commit_transaction(self, timeout=60):
        self._p.commit_transaction(timeout)

    def abort_transaction(self, timeout=60):
        self._p.abort_transaction(timeout)


def codec_refused(sent):
    """Whether a send failed because the target refused its codec, on
    either client."""
    if not (callable(getattr(sent, "failed", None)) and sent.failed()):
        return False
    err = getattr(sent, "exception", None)
    if isinstance(err, StreamError):
        return err.codec_refused
    name = type(err).__name__
    return name in ("UnsupportedCodecError",
                    "UnsupportedCompressionTypeError") \
        or "UNSUPPORTED_COMPRESSION" in str(err).upper()


class Handle:
    """A send's outcome as the caller holds it; `Stepping` points it at
    the send that carried the message again."""

    def __init__(self, sent):
        self.current = sent

    def __getattr__(self, name):
        if name == "current":
            raise AttributeError(name)
        return getattr(self.current, name)


class Stepping:
    """A producer that, where the target refuses its codec at send time,
    steps down the codec ladder (`step(codec, why)` gives the next one,
    the reason recorded) and sends again what was refused, in the order
    it was sent: a refusal never stops the copy. A refused message with a
    landed one after it in the same topic cannot be sent again without
    reordering its partition, and stops the copy there instead.
    `make(codec)` makes the producer underneath."""

    def __init__(self, make, codec, step):
        self._make, self._step, self.codec = make, step, codec
        self._p = make(codec)
        self._held = []

    def send(self, topic, **kw):
        held = Handle(self._p.send(topic, **kw))
        self._held.append((held, topic, kw))
        return held

    def _in_order(self):
        """Stop where a message of a topic landed after one refused."""
        first = {}
        for i, (held, topic, _) in enumerate(self._held):
            if codec_refused(held.current):
                first.setdefault(topic, i)
            elif i > first.get(topic, len(self._held)):
                raise SystemExit(
                    f"{topic}: the target refused {self.codec} for a"
                    " message and took one sent after it; sending it again"
                    " would put it out of order. Nothing past the last"
                    " saved position is counted as copied")

    def flush(self, timeout=None):
        if timeout is None:
            self._p.flush()
        else:
            self._p.flush(timeout)
        while True:
            refused = [x for x in self._held if codec_refused(x[0].current)]
            if not refused:
                break
            self._in_order()
            why = str(refused[0][0].current.exception)
            nxt = self._step(self.codec, why)
            if nxt == self.codec:
                raise SystemExit(f"the target refused {self.codec}: {why}")
            self._p.close()
            self.codec, self._p = nxt, self._make(nxt)
            for held, topic, kw in refused:
                held.current = self._p.send(topic, **kw)
            self._p.flush()
        self._held = []

    def close(self, timeout=None):
        self.flush()
        self._p.close()

    def __getattr__(self, name):
        if name == "_p":
            raise AttributeError(name)
        return getattr(self._p, name)


#: a partition and an offset as the pure client keys them: a tuple of the
#: same fields, so either client's keys find the other's in a dict
TopicPartition = collections.namedtuple("TopicPartition", "topic partition")
OffsetAndMetadata = collections.namedtuple(
    "OffsetAndMetadata", "offset metadata leader_epoch")
OffsetAndTimestamp = collections.namedtuple(
    "OffsetAndTimestamp", "offset timestamp leader_epoch")
#: a message read, with the pure client's fields
Record = collections.namedtuple(
    "Record", "topic partition offset timestamp timestamp_type key value"
              " headers checksum serialized_key_size serialized_value_size"
              " serialized_header_size leader_epoch")


def _ctp(tp, offset=None):
    from confluent_kafka import TopicPartition as T
    return T(tp[0], int(tp[1])) if offset is None \
        else T(tp[0], int(tp[1]), int(offset))


def _wait(futures, timeout=30):
    """The results of the compiled admin's futures, {key: result}; the
    first error raised as a `StreamError`."""
    out = {}
    for key, fut in dict(futures).items():
        try:
            out[key] = fut.result(timeout)
        except Exception as e:  # noqa: BLE001 - said as the cluster's error
            raise StreamError(e.args[0] if e.args else e) from None
    return out


class NativeConsumer(_SignIn):
    """The compiled client's consumer, answering as the copier asks of a
    consumer (the pure client's calls, keys and records). Positions are
    kept here and set on the compiled client before the next fetch, as an
    assignment with offsets: it refuses a seek on a partition that is not
    fetching yet."""

    #: the pure client's settings, as the compiled client names them
    KEYS = {"isolation_level": "isolation.level",
            "enable_auto_commit": "enable.auto.commit",
            "auto_offset_reset": "auto.offset.reset",
            "fetch_max_bytes": "fetch.max.bytes",
            "max_partition_fetch_bytes": "max.partition.fetch.bytes",
            "group_id": "group.id"}

    def __init__(self, conn, side="target", consumer_timeout_ms=None,
                 request_timeout_ms=None, **kw):
        from confluent_kafka import Consumer
        self._side, self._auth = side, []
        self._timeout = (request_timeout_ms or 30000) / 1000
        self._idle = consumer_timeout_ms
        conf = native_sign_in(conn)
        # never joined nor committed under: assignments only
        conf.update({"group.id": f"migkit-read-{uuid.uuid4().hex[:12]}",
                     "enable.auto.commit": False,
                     "enable.auto.offset.store": False,
                     "auto.offset.reset": "latest",
                     "error_cb": self._error})
        for k, v in kw.items():
            if k in self.KEYS and v is not None:
                conf[self.KEYS[k]] = v
        self._reset = conf["auto.offset.reset"]
        self._c = Consumer(conf)
        self._assigned, self._pos, self._dirty = [], {}, False

    def _meta(self, topic=None):
        try:
            got = self._c.list_topics(topic, timeout=self._timeout)
        finally:
            self._check()
        return got

    def topics(self):
        return set(self._meta().topics)

    def partitions_for_topic(self, topic):
        got = self._meta(topic).topics.get(topic)
        if got is None or got.error is not None:
            return None
        return set(got.partitions)

    def brokers(self):
        return sorted(self._meta().brokers)

    def cluster_id(self):
        return self._meta().cluster_id

    def _marks(self, tps):
        out = {}
        for tp in tps:
            tp = TopicPartition(tp[0], int(tp[1]))
            out[tp] = self._c.get_watermark_offsets(
                _ctp(tp), timeout=self._timeout, cached=False)
        self._check()
        return out

    def end_offsets(self, tps):
        return {tp: hi for tp, (_, hi) in self._marks(tps).items()}

    def beginning_offsets(self, tps):
        return {tp: lo for tp, (lo, _) in self._marks(tps).items()}

    def offsets_for_times(self, wanted):
        """{tp: OffsetAndTimestamp, or None past the last message}; the
        compiled client does not say the found message's time."""
        got = self._c.offsets_for_times(
            [_ctp(tp, ts) for tp, ts in wanted.items()],
            timeout=self._timeout)
        out = {}
        for c in got:
            if c.error is not None:
                raise StreamError(c.error)
            out[TopicPartition(c.topic, c.partition)] = \
                OffsetAndTimestamp(c.offset, None, None) \
                if c.offset >= 0 else None
        return out

    def assign(self, tps):
        self._assigned = [TopicPartition(t[0], int(t[1])) for t in tps]
        self._dirty = True

    def assignment(self):
        return set(self._assigned)

    def seek(self, tp, offset):
        self._pos[TopicPartition(tp[0], int(tp[1]))] = int(offset)
        self._dirty = True

    def seek_to_beginning(self, *tps):
        for tp, lo in self.beginning_offsets(tps or self._assigned).items():
            self.seek(tp, lo)

    def seek_to_end(self, *tps):
        for tp, hi in self.end_offsets(tps or self._assigned).items():
            self.seek(tp, hi)

    def position(self, tp):
        tp = TopicPartition(tp[0], int(tp[1]))
        if tp not in self._pos:
            lo, hi = self._marks([tp])[tp]
            self._pos[tp] = lo if self._reset == "earliest" else hi
            self._dirty = True
        held = self._pos[tp]
        if not self._dirty:
            # the compiled client's own position moves past what a reader
            # of committed messages is not given (markers, aborted writes)
            got = self._c.position([_ctp(tp)])
            if got and got[0].offset is not None and got[0].offset > held:
                held = self._pos[tp] = got[0].offset
        return held

    def _placed(self):
        if self._dirty:
            for tp in self._assigned:
                if tp not in self._pos:
                    self.position(tp)
            self._c.assign([_ctp(tp, self._pos[tp])
                            for tp in self._assigned])
            self._dirty = False

    def poll(self, timeout_ms=0, max_records=500):
        """{TopicPartition: [Record]}: what arrived within `timeout_ms`,
        at most `max_records`, returned as soon as there is any - the first
        message waited for, the rest taken only as already fetched."""
        self._placed()
        first = self._c.poll(max(0, timeout_ms) / 1000)
        msgs = [] if first is None else [first]
        if first is not None and max_records > 1:
            msgs += self._c.consume(num_messages=max_records - 1, timeout=0)
        self._check()
        out = {}
        for m in msgs:
            err = m.error()
            if err is not None:
                if "PARTITION_EOF" in str(err.name()):
                    continue
                raise StreamError(err)
            tp = TopicPartition(m.topic(), m.partition())
            kind, ts = m.timestamp()
            key, value = m.key(), m.value()
            headers = list(m.headers() or [])
            out.setdefault(tp, []).append(Record(
                tp.topic, tp.partition, m.offset(), ts, max(0, kind - 1),
                key, value, headers, None,
                -1 if key is None else len(key),
                -1 if value is None else len(value),
                sum(len(k) + len(v or b"") for k, v in headers), None))
            self._pos[tp] = m.offset() + 1
        return out

    def __iter__(self):
        while True:
            got = self.poll(timeout_ms=self._idle or 1000)
            if not got:
                return
            for msgs in got.values():
                yield from msgs

    def close(self):
        self._c.close()


#: an admin call by its name on one version of the pure client, and the
#: names it has had on the others - probed by attribute, never by the
#: version string
ALIASES = {"delete_groups": ("delete_groups", "delete_consumer_groups"),
           "list_groups": ("list_groups", "list_consumer_groups"),
           "list_group_offsets": ("list_group_offsets",
                                  "list_consumer_group_offsets"),
           "alter_group_offsets": ("alter_group_offsets",
                                   "alter_consumer_group_offsets")}


def call_of(client, name):
    """The client's call for `name`, under whichever name it has it
    (`ALIASES`); an AttributeError naming them all where it has none."""
    names = ALIASES.get(name, (name,))
    for each in names:
        got = getattr(client, each, None)
        if callable(got):
            return got
    raise AttributeError(f"{type(client).__name__} has none of "
                         + ", ".join(names))


class NativeAdmin(_SignIn):
    """The compiled client's admin, for the calls it answers, each under
    the pure client's name and in its answer's shape (`CALLS`);
    `lacks(name)` says why a call is not answered here."""

    #: the pure client's call -> the compiled admin's that answers it
    CALLS = {"create_topics": "create_topics",
             "delete_records": "delete_records",
             "list_groups": "list_consumer_groups",
             "list_group_offsets": "list_consumer_group_offsets",
             "alter_group_offsets": "alter_consumer_group_offsets",
             "delete_groups": "delete_consumer_groups",
             "brokers": "list_topics", "cluster_id": "list_topics"}
    #: calls the compiled admin has no answer to, and why
    PURE = {
        "api_versions": "the compiled admin does not say which request"
                        " versions the brokers answer",
        "describe_log_dirs": "the compiled admin has no call for the"
                             " brokers' log directories",
        "describe_producers": "the compiled admin has no call for the"
                              " producers a partition holds state for",
        "_manager": "the compiled admin has no call for client quotas",
        "_client": "the pure client's own connection is asked",
    }

    def __init__(self, conn, side="target", request_timeout_ms=None):
        from confluent_kafka.admin import AdminClient
        self._side, self._auth = side, []
        self._timeout = (request_timeout_ms or 30000) / 1000
        self._a = AdminClient({**native_sign_in(conn),
                               "error_cb": self._error})

    def lacks(self, name):
        """None where the compiled admin answers `name`, else why not."""
        if name in self.CALLS:
            if callable(getattr(self._a, self.CALLS[name], None)):
                return None
            return ("the compiled admin installed here has no"
                    f" {self.CALLS[name]}")
        return self.PURE.get(
            name, "its answer is read here only in the pure client's shape")

    def _done(self, futures):
        try:
            return _wait(futures, self._timeout)
        finally:
            self._check()

    def brokers(self):
        return sorted(self._a.list_topics(timeout=self._timeout).brokers)

    def cluster_id(self):
        return self._a.list_topics(timeout=self._timeout).cluster_id

    def create_topics(self, new_topics, timeout_ms=None, validate_only=False):
        from confluent_kafka.admin import NewTopic
        made = [NewTopic(t.name, num_partitions=int(t.num_partitions),
                         replication_factor=int(t.replication_factor),
                         config={str(k): str(v) for k, v in
                                 (getattr(t, "topic_configs", None)
                                  or {}).items()})
                for t in new_topics]
        return self._done(self._a.create_topics(
            made, validate_only=validate_only))

    def delete_records(self, wanted, timeout_ms=None):
        return self._done(self._a.delete_records(
            [_ctp(tp, offset) for tp, offset in wanted.items()]))

    def list_groups(self):
        got = self._a.list_consumer_groups().result(self._timeout)
        self._check()
        return [{"group_id": g.group_id,
                 "protocol_type": "" if getattr(
                     g, "is_simple_consumer_group", False) else "consumer",
                 "group_state": str(getattr(getattr(g, "state", None),
                                            "name", "") or "")}
                for g in got.valid]

    def list_group_offsets(self, group):
        from confluent_kafka import ConsumerGroupTopicPartitions as G
        got = self._done(self._a.list_consumer_group_offsets([G(group)]))
        out = {}
        for c in got[group].topic_partitions or []:
            if c.error is not None:
                raise StreamError(c.error)
            epoch = getattr(c, "leader_epoch", None)
            out[TopicPartition(c.topic, c.partition)] = OffsetAndMetadata(
                c.offset, c.metadata or "", -1 if epoch is None else epoch)
        return {group: out}

    def alter_group_offsets(self, group, offsets):
        """{TopicPartition: None, or the error the cluster answered}."""
        from confluent_kafka import ConsumerGroupTopicPartitions as G
        parts = []
        for tp, meta in offsets.items():
            c = _ctp(tp, getattr(meta, "offset", meta))
            if getattr(meta, "metadata", None):
                c.metadata = meta.metadata
            parts.append(c)
        got = self._done(self._a.alter_consumer_group_offsets(
            [G(group, parts)]))
        return {TopicPartition(c.topic, c.partition):
                None if c.error is None else StreamError(c.error)
                for c in got[group].topic_partitions or []}

    def delete_groups(self, groups):
        return self._done(self._a.delete_consumer_groups(list(groups)))

    def close(self):
        """The compiled admin holds no connection to end."""


class Unanswered(Exception):
    """An admin call the compiled admin does not answer, where the pure
    client that would is not installed."""


class Admin:
    """One side's admin: each call through the compiled admin where it
    answers it (`NativeAdmin`), else through the pure one, made on first
    need, the call and why said once to `said(call, why)`. Without a
    compiled admin every call goes the pure way, under whichever name the
    installed version has it (`ALIASES`)."""

    def __init__(self, native, pure, said=None):
        self._native, self._make, self._pure = native, pure, None
        self._said = said
        self.went_pure = {}

    def _pure_one(self, call=None):
        if self._pure is None:
            missing = pure_missing() if self._native is not None else None
            if missing is not None:
                raise Unanswered(f"{call}: {self.went_pure.get(call)}, and"
                                 f" {missing}")
            self._pure = self._make()
        return self._pure

    def __getattr__(self, name):
        if name.startswith("__") or name in (
                "_native", "_make", "_pure", "_said", "went_pure"):
            raise AttributeError(name)
        if self._native is not None:
            why = self._native.lacks(name)
            if why is None:
                return getattr(self._native, name)
            if name not in self.went_pure:
                self.went_pure[name] = why
                if self._said is not None:
                    self._said(name, why)
        if name in ALIASES:
            return call_of(self._pure_one(name), name)
        return getattr(self._pure_one(name), name)

    def brokers(self):
        if self._native is not None:
            return self._native.brokers()
        return brokers_of(self._pure_one())

    def close(self):
        if self._native is not None:
            self._native.close()
        if self._pure is not None:
            self._pure.close()


def brokers_of(client):
    """The node ids of a cluster's brokers, asked of a consumer or an
    admin of either client (the pure client's attribute is `node_id` on
    3.x and was `nodeId` on 2.x - probed, not read off the version)."""
    if isinstance(client, (NativeConsumer, NativeAdmin, Admin)):
        return list(client.brokers())
    return [b.node_id if hasattr(b, "node_id") else b.nodeId
            for b in client._client.cluster.brokers()]


def cluster_id_of(client):
    """The cluster's id, asked of a consumer of either client."""
    if isinstance(client, NativeConsumer):
        return client.cluster_id()
    return client._client.cluster.cluster_id


def error_name(err):
    """The name of the error a call answered with, None for none."""
    if err is None:
        return None
    if isinstance(err, StreamError):
        return err.code
    name = getattr(err, "__name__", None) or type(err).__name__
    return None if name == "NoError" else name
