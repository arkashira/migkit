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
import logging

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


def _codec_rungs():
    from .decide import Need, Rung
    return (
        Rung("zstd", "zstd compression",
             needs=(Need("zstd in the client", lambda f: f["has_zstd"],
                         "the stream client here has no zstd"),
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
                    _pays("lz4")),
             because="every broker that carries headers takes it"),
        Rung("none", "messages sent as they are",
             because="nothing to gain from compressing"),
    )


CODECS = _codec_rungs()


def codec(sample, zstd_target, has_zstd=True, has_lz4=True, costs=None):
    """The `Choice` of the codec a producer sends in."""
    facts = decide.Facts({"ratios": ratios(sample),
                          "zstd_target": zstd_target,
                          "has_zstd": bool(has_zstd),
                          "has_lz4": bool(has_lz4)})
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


def native_config(conn, **kw):
    """The compiled client's settings from the connection and the
    producer's settings as the pure client names them. Keys go to their
    partition by the same hash the pure client and Java's use, so a key's
    messages land where they always did."""
    conf = {"bootstrap.servers": ",".join(conn["bootstrap_servers"]),
            "partitioner": "murmur2_random", "logger": LOG,
            "message.timeout.ms": 120000}
    if conn.get("security_protocol"):
        conf["security.protocol"] = conn["security_protocol"]
    if conn.get("sasl_mechanism"):
        conf["sasl.mechanism"] = conn["sasl_mechanism"]
    if conn.get("sasl_plain_username") is not None:
        conf["sasl.username"] = conn["sasl_plain_username"]
        conf["sasl.password"] = conn.get("sasl_plain_password") or ""
    if conn.get("ssl_cafile"):
        conf["ssl.ca.location"] = conn["ssl_cafile"]
    for k, v in kw.items():
        if k == "max_request_size":
            conf["message.max.bytes"] = min(int(v), 1_000_000_000)
        elif k == "buffer_memory":
            conf["queue.buffering.max.kbytes"] = max(1, int(v) // 1024)
        elif k in _KEYS and v is not None:
            conf[_KEYS[k]] = v
    return conf


class NativeProducer:
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
