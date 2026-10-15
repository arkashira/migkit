"""The client a Kafka target is written through, and the codec its
messages go in, are chosen from facts (backlog R18.7): installed or not,
how the side signs in, how well the first messages compress, which
brokers the target runs, and again a topic at a time and after a codec
the target refused when sent; the consumer and the admin go through the
client chosen for their side, an admin call the compiled one lacks through
the pure one, said by name. The compiled client is driven through a
double: no server is reached."""
import collections
import os
import sys
import types

import pytest

from migkit import kafkaclient

TEXT = [b'{"order": %d, "status": "shipped", "city": "Bangkok"}' % i
        for i in range(200)]


def test_zstd_where_the_target_takes_it_and_the_messages_compress():
    got = kafkaclient.codec(TEXT, kafkaclient.target_takes_zstd(9, None))
    assert got.path == "zstd"


def test_lz4_where_the_brokers_are_older_than_zstd():
    got = kafkaclient.codec(TEXT, kafkaclient.target_takes_zstd(
        None, (2, 0)))
    assert got.path == "lz4"
    assert "2.1" in got.reason


def test_lz4_where_the_brokers_do_not_say_their_version():
    got = kafkaclient.codec(TEXT, kafkaclient.target_takes_zstd())
    assert got.path == "lz4"
    assert "did not say" in got.reason


def test_nothing_compressed_where_the_messages_do_not_compress():
    noise = [os.urandom(512) for _ in range(50)]
    got = kafkaclient.codec(noise, True)
    assert got.path == "none"
    assert "compress only" in got.reason


def test_nothing_compressed_where_the_client_has_neither_codec():
    got = kafkaclient.codec(TEXT, True, has_zstd=False, has_lz4=False)
    assert got.path == "none"


def test_the_produce_version_outranks_the_broker_version():
    assert kafkaclient.target_takes_zstd(6, (3, 7)) is False
    assert kafkaclient.target_takes_zstd(7, (2, 0)) is True
    assert kafkaclient.target_takes_zstd(None, (2, 1)) is True


def test_the_compiled_client_where_it_is_installed():
    got = kafkaclient.client_way(native=lambda: None, pure=lambda: None)
    assert got.path == "native"


def test_the_pure_client_where_the_compiled_one_is_missing():
    got = kafkaclient.client_way(native=lambda: "not installed here",
                                 pure=lambda: None)
    assert got.path == "pure"
    assert "not installed here" in got.reason


def test_the_pure_client_where_the_side_signs_in_by_the_cloud():
    got = kafkaclient.client_way(True, native=lambda: None,
                                 pure=lambda: None)
    assert got.path == "pure"
    assert "cloud" in got.reason


def test_no_client_is_a_stop_that_says_so():
    with pytest.raises(SystemExit) as e:
        kafkaclient.client_way(native=lambda: "a", pure=lambda: "b")
    assert "doctor --install" in str(e.value)


class _Err:
    def __init__(self, name):
        self._name = name

    def name(self):
        return self._name

    def str(self):
        return self._name.lower()


class _Msg:
    def __init__(self, offset, partition):
        self._o, self._p = offset, partition

    def offset(self):
        return self._o

    def partition(self):
        return self._p


class _Producer:
    """The compiled client's producer as far as the copier drives it:
    deliveries served on flush, a full queue once."""
    made = []

    def __init__(self, conf):
        self.conf = conf
        self.queue, self.next, self.full = [], 0, 1
        _Producer.made.append(self)

    def produce(self, topic, value=None, key=None, on_delivery=None,
                partition=None, timestamp=None, headers=None):
        if self.full:
            self.full -= 1
            raise BufferError("queue full")
        self.queue.append((topic, value, on_delivery, partition, headers))

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        for topic, value, cb, partition, _ in self.queue:
            if value == b"too big":
                cb(_Err("MSG_SIZE_TOO_LARGE"), None)
            elif value == b"zstd refused":
                cb(_Err("UNSUPPORTED_COMPRESSION_TYPE"), None)
            else:
                cb(None, _Msg(self.next, partition or 0))
                self.next += 1
        self.queue = []
        return 0


@pytest.fixture
def compiled(monkeypatch):
    monkeypatch.setitem(sys.modules, "confluent_kafka",
                        types.SimpleNamespace(Producer=_Producer))
    _Producer.made.clear()
    return _Producer


def test_the_compiled_producer_is_set_as_the_copier_asks(compiled):
    kafkaclient.NativeProducer(
        {"bootstrap_servers": ["10.0.0.1:9092", "10.0.0.2:9092"],
         "security_protocol": "SASL_SSL", "sasl_mechanism": "SCRAM-SHA-512",
         "sasl_plain_username": "u", "sasl_plain_password": "CHANGE_ME"},
        acks="all", max_request_size=2 * 10 ** 9, buffer_memory=64 << 20,
        enable_idempotence=True, transactional_id="migkit-copy.orders",
        compression_type="zstd", max_in_flight_requests_per_connection=1)
    conf = compiled.made[-1].conf
    assert conf["bootstrap.servers"] == "10.0.0.1:9092,10.0.0.2:9092"
    assert conf["partitioner"] == "murmur2_random"
    assert conf["compression.type"] == "zstd"
    assert conf["enable.idempotence"] is True
    assert conf["transactional.id"] == "migkit-copy.orders"
    assert conf["message.max.bytes"] == 1_000_000_000
    assert conf["queue.buffering.max.kbytes"] == 64 << 10
    assert conf["sasl.username"] == "u"
    assert conf["max.in.flight.requests.per.connection"] == 1


def test_a_send_says_where_it_landed_and_what_was_refused(compiled):
    p = kafkaclient.NativeProducer({"bootstrap_servers": ["10.0.0.1:9092"]})
    p._p.full = 0
    ok = p.send("orders", value=b"a", key=b"k", partition=2,
                headers=[("h", b"v")], timestamp_ms=1700000000000)
    bad = p.send("orders", value=b"too big", partition=2)
    codec = p.send("orders", value=b"zstd refused", partition=2)
    p.flush()
    assert ok.succeeded() and ok.get(15).offset == 0
    assert ok.partition == 2
    assert bad.failed() and not bad.exception.codec_refused
    assert codec.failed() and codec.exception.codec_refused
    with pytest.raises(kafkaclient.StreamError):
        bad.get(15)


def test_a_full_queue_is_waited_out_not_dropped(compiled):
    p = kafkaclient.NativeProducer({"bootstrap_servers": ["10.0.0.1:9092"]})
    sent = p.send("orders", value=b"a")
    p.flush()
    assert sent.succeeded()
    assert compiled.made[-1].next == 1


def test_a_refused_sign_in_is_said_as_one(compiled):
    p = kafkaclient.NativeProducer({"bootstrap_servers": ["10.0.0.1:9092"]})
    p._error(_Err("_AUTHENTICATION"))
    with pytest.raises(kafkaclient.Refused) as e:
        p.flush()
    assert "refused the sign-in" in str(e.value)


# --- a codec refused at send time is stepped down from ------------------

class _Landed:
    def __init__(self, err=None):
        self.exception = err

    def failed(self):
        return self.exception is not None


class _Refusing:
    """Refuses every send while its codec is in `refuses`, and a value of
    b"big" whatever the codec."""

    def __init__(self, codec, refuses, log):
        self.codec, self.refuses, self.log = codec, refuses, log
        self.closed = False

    def send(self, topic, value=None, key=None):
        if self.codec in self.refuses:
            return _Landed(kafkaclient.StreamError(
                _Err("UNSUPPORTED_COMPRESSION_TYPE")))
        if value == b"big":
            return _Landed(kafkaclient.StreamError(
                _Err("MSG_SIZE_TOO_LARGE")))
        self.log.append((self.codec, topic, value))
        return _Landed()

    def flush(self, timeout=None):
        pass

    def close(self, timeout=None):
        self.closed = True


def _stepping(refuses, ladder=("zstd", "lz4", None)):
    log, steps, made = [], [], []

    def make(codec):
        made.append(_Refusing(codec, refuses, log))
        return made[-1]

    def step(codec, why):
        steps.append((codec, why))
        return ladder[ladder.index(codec) + 1]
    return kafkaclient.Stepping(make, ladder[0], step), log, steps, made


def test_a_refused_zstd_is_sent_again_in_lz4_in_order():
    p, log, steps, made = _stepping({"zstd"})
    held = [p.send("orders", value=v) for v in (b"1", b"2", b"3")]
    p.flush()
    assert [v for _, _, v in log] == [b"1", b"2", b"3"]
    assert {c for c, _, _ in log} == {"lz4"}
    assert steps == [("zstd", "unsupported_compression_type")]
    assert all(not h.failed() for h in held)
    assert made[0].closed and p.codec == "lz4"


def test_both_codecs_refused_ends_with_messages_sent_as_they_are():
    p, log, steps, _ = _stepping({"zstd", "lz4"})
    p.send("orders", value=b"1")
    p.flush()
    assert [c for c, _ in steps] == ["zstd", "lz4"]
    assert log == [(None, "orders", b"1")]


def test_a_failure_that_is_not_the_codec_is_left_for_the_copier():
    p, log, steps, _ = _stepping(set())
    bad = p.send("orders", value=b"big")
    p.flush()
    assert steps == [] and bad.failed()
    assert not bad.exception.codec_refused


def test_a_landed_message_after_a_refused_one_stops_rather_than_reorders():
    p, _, _, made = _stepping({"zstd"})
    p.send("orders", value=b"1")
    made[0].refuses = set()
    p.send("orders", value=b"2")
    with pytest.raises(SystemExit, match="out of order"):
        p.flush()


def test_a_ladder_that_does_not_step_stops_and_says_why():
    p, _, _, _ = _stepping({"zstd"})
    p.send("orders", value=b"1")
    p._step = lambda codec, why: codec
    with pytest.raises(SystemExit, match="refused zstd"):
        p.flush()


def test_the_codec_ladder_takes_the_refusal_as_a_fact(monkeypatch):
    monkeypatch.setattr(kafkaclient, "ratios",
                        lambda s: {"zstd": 0.2, "lz4": 0.3})
    assert kafkaclient.codec([b"x"], True).path == "zstd"
    got = kafkaclient.codec([b"x"], True,
                            refused={"zstd": "UNSUPPORTED {x}"})
    assert got.path == "lz4"
    assert "refused it at send time" in str(got.as_evidence())
    both = kafkaclient.codec([b"x"], True,
                             refused={"zstd": "no", "lz4": "no"})
    assert both.path == "none"


# --- an admin call found under the name the installed client has -------

class _Old:
    __version__ = "3.0.11"

    def __init__(self):
        self.deleted = []

    def delete_consumer_groups(self, groups):
        self.deleted += groups


class _New:
    def delete_groups(self, groups):
        return "new"

    def delete_consumer_groups(self, groups):
        return "old"


def test_the_delete_call_is_probed_by_attribute_not_by_version():
    old = _Old()
    kafkaclient.call_of(old, "delete_groups")(["billing"])
    assert old.deleted == ["billing"]
    assert kafkaclient.call_of(_New(), "delete_groups")([]) == "new"
    with pytest.raises(AttributeError, match="delete_consumer_groups"):
        kafkaclient.call_of(object(), "delete_groups")


class _NativeAdminDouble:
    def __init__(self):
        self.closed = False

    def lacks(self, name):
        return None if name == "list_groups" else f"no {name} here"

    def list_groups(self):
        return [{"group_id": "billing"}]

    def close(self):
        self.closed = True


class _PureAdminDouble:
    def __init__(self):
        self._manager = "quotas"
        self.closed = False

    def api_versions(self):
        return {0: (0, 9)}

    def delete_consumer_groups(self, groups):
        return groups

    def close(self):
        self.closed = True


def test_the_admin_goes_native_and_says_each_call_that_did_not():
    made, said = [], []
    admin = kafkaclient.Admin(
        _NativeAdminDouble(),
        lambda: made.append(_PureAdminDouble()) or made[-1],
        lambda call, why: said.append((call, why)))
    assert admin.list_groups() == [{"group_id": "billing"}]
    assert made == [] and said == []
    assert admin.api_versions() == {0: (0, 9)}
    assert admin.api_versions() == {0: (0, 9)}
    assert admin._manager == "quotas"
    assert len(made) == 1
    assert said == [("api_versions", "no api_versions here"),
                    ("_manager", "no _manager here")]
    admin.close()
    assert made[0].closed


def test_the_pure_admin_alone_answers_under_its_own_names():
    said = []
    admin = kafkaclient.Admin(None, _PureAdminDouble,
                              lambda c, w: said.append(c))
    assert admin.delete_groups(["g"]) == ["g"]
    assert said == []


def test_a_call_the_compiled_admin_lacks_without_the_pure_client(
        monkeypatch):
    monkeypatch.setattr(kafkaclient, "pure_missing", lambda: "not here")
    admin = kafkaclient.Admin(_NativeAdminDouble(), _PureAdminDouble)
    with pytest.raises(kafkaclient.Unanswered, match="api_versions"):
        admin.api_versions()


# --- the compiled consumer and admin, on doubles of the compiled client --

class _CTP:
    def __init__(self, topic, partition, offset=-1001):
        self.topic, self.partition, self.offset = topic, partition, offset
        self.error, self.metadata, self.leader_epoch = None, None, None


class _Fetched:
    def __init__(self, offset, value, err=None, partition=0):
        self._o, self._v, self._e, self._p = offset, value, err, partition

    def error(self):
        return self._e

    def topic(self):
        return "orders"

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def key(self):
        return b"k"

    def value(self):
        return self._v

    def headers(self):
        return [("h", b"v")]

    def timestamp(self):
        return (1, 1700000000000)


class _Consumer:
    made = []

    def __init__(self, conf):
        self.conf, self.assigned, self.queue = conf, None, []
        _Consumer.made.append(self)

    def assign(self, parts):
        self.assigned = [(c.topic, c.partition, c.offset) for c in parts]

    def get_watermark_offsets(self, tp, timeout=None, cached=False):
        return (3, 10)

    def offsets_for_times(self, parts, timeout=None):
        return [_CTP(c.topic, c.partition, 5 if c.offset < 100 else -1)
                for c in parts]

    def position(self, parts):
        return [_CTP(c.topic, c.partition, -1001) for c in parts]

    def poll(self, timeout=None):
        return self.queue.pop(0) if self.queue else None

    def consume(self, num_messages=1, timeout=None):
        got, self.queue = self.queue[:num_messages], self.queue[num_messages:]
        return got

    def list_topics(self, topic=None, timeout=None):
        meta = types.SimpleNamespace(partitions={0: None, 1: None},
                                     error=None)
        return types.SimpleNamespace(topics={"orders": meta},
                                     brokers={2: None, 1: None},
                                     cluster_id="c-1")

    def close(self):
        pass


class _Future:
    def __init__(self, value=None, err=None):
        self.value, self.err = value, err

    def result(self, timeout=None):
        if self.err is not None:
            raise Exception(self.err)
        return self.value


class _AdminClient:
    def __init__(self, conf):
        self.conf = conf

    def list_consumer_groups(self):
        g = types.SimpleNamespace(group_id="billing",
                                  is_simple_consumer_group=False,
                                  state=types.SimpleNamespace(name="EMPTY"))
        return _Future(types.SimpleNamespace(valid=[g]))

    def alter_consumer_group_offsets(self, requests):
        parts = requests[0].topic_partitions
        parts[1].error = _Err("UNKNOWN_MEMBER_ID")
        return {requests[0].group_id: _Future(types.SimpleNamespace(
            group_id=requests[0].group_id, topic_partitions=parts))}

    def delete_consumer_groups(self, groups):
        return {g: _Future(err=_Err("GROUP_ID_NOT_FOUND")) for g in groups}


@pytest.fixture
def compiled_reader(monkeypatch):
    ck = types.ModuleType("confluent_kafka")
    ck.Consumer, ck.TopicPartition = _Consumer, _CTP
    ck.ConsumerGroupTopicPartitions = lambda g, parts=None: \
        types.SimpleNamespace(group_id=g, topic_partitions=parts)
    admin = types.ModuleType("confluent_kafka.admin")
    admin.AdminClient = _AdminClient
    ck.admin = admin
    monkeypatch.setitem(sys.modules, "confluent_kafka", ck)
    monkeypatch.setitem(sys.modules, "confluent_kafka.admin", admin)
    _Consumer.made.clear()


#: the pure client's key, as the copier makes it
Key = collections.namedtuple("TopicPartition", "topic partition")


def test_the_compiled_consumer_answers_as_the_copier_asks(compiled_reader):
    c = kafkaclient.NativeConsumer(
        {"bootstrap_servers": ["10.0.0.1:9092"]},
        isolation_level="read_committed", consumer_timeout_ms=10000,
        request_timeout_ms=15000, enable_auto_commit=False)
    raw = _Consumer.made[-1]
    assert raw.conf["isolation.level"] == "read_committed"
    assert raw.conf["enable.auto.commit"] is False
    assert raw.conf["group.id"].startswith("migkit-read-")
    tp = Key("orders", 0)
    c.assign([tp])
    c.seek(tp, 7)
    assert raw.assigned is None, "a seek waits for the next fetch"
    raw.queue = [_Fetched(7, b"a"), _Fetched(8, None, _Err("_PARTITION_EOF")),
                 _Fetched(8, b"b")]
    got = c.poll(timeout_ms=100, max_records=10)
    assert raw.assigned == [("orders", 0, 7)]
    assert [m.offset for m in got[tp]] == [7, 8]
    rec = got[tp][0]
    assert (rec.key, rec.value, rec.headers) == (b"k", b"a", [("h", b"v")])
    assert rec.timestamp_type == 0 and rec.serialized_value_size == 1
    assert c.position(tp) == 9
    assert c.end_offsets([tp]) == {tp: 10}
    assert c.beginning_offsets([tp]) == {tp: 3}
    assert c.offsets_for_times({tp: 50})[tp].offset == 5
    assert c.offsets_for_times({tp: 500})[tp] is None
    assert c.partitions_for_topic("orders") == {0, 1}
    assert c.partitions_for_topic("gone") is None
    assert kafkaclient.brokers_of(c) == [1, 2]
    assert kafkaclient.cluster_id_of(c) == "c-1"


def test_a_fetch_error_is_raised_not_skipped(compiled_reader):
    c = kafkaclient.NativeConsumer({"bootstrap_servers": ["10.0.0.1:9092"]})
    c.assign([Key("orders", 0)])
    _Consumer.made[-1].queue = [
        _Fetched(0, None, _Err("TOPIC_AUTHORIZATION_FAILED"))]
    with pytest.raises(kafkaclient.StreamError,
                       match="topic_authorization_failed"):
        c.poll(timeout_ms=10)


def test_an_unplaced_partition_starts_where_the_reset_says(compiled_reader):
    c = kafkaclient.NativeConsumer({"bootstrap_servers": ["10.0.0.1:9092"]},
                                   auto_offset_reset="earliest")
    c.assign([Key("orders", 1)])
    c.poll(timeout_ms=10)
    assert _Consumer.made[-1].assigned == [("orders", 1, 3)]
    late = kafkaclient.NativeConsumer(
        {"bootstrap_servers": ["10.0.0.1:9092"]})
    late.assign([Key("orders", 1)])
    late.poll(timeout_ms=10)
    assert _Consumer.made[-1].assigned == [("orders", 1, 10)]


def test_the_compiled_admin_answers_in_the_pure_clients_shape(
        compiled_reader):
    a = kafkaclient.NativeAdmin({"bootstrap_servers": ["10.0.0.1:9092"]})
    assert a.list_groups() == [{"group_id": "billing",
                                "protocol_type": "consumer",
                                "group_state": "EMPTY"}]
    got = a.alter_group_offsets("billing", {
        Key("orders", 0): kafkaclient.OffsetAndMetadata(4, "", -1),
        Key("orders", 1): kafkaclient.OffsetAndMetadata(9, "", -1)})
    assert kafkaclient.error_name(got[Key("orders", 0)]) is None
    assert kafkaclient.error_name(got[Key("orders", 1)]) == \
        "UNKNOWN_MEMBER_ID"
    with pytest.raises(kafkaclient.StreamError, match="group_id_not_found"):
        a.delete_groups(["gone"])
    assert a.lacks("list_groups") is None
    assert "no delete_records" in a.lacks("delete_records")
    assert "client quotas" in a.lacks("_manager")
    assert "log directories" in a.lacks("describe_log_dirs")


# --- the engine: a client a side, a codec a topic -----------------------

def _engine(where, src_options=None):
    from migkit.config import Endpoint, Hop
    from migkit.engines.kafka import KafkaEngine
    hop = Hop(name="k", engine="kafka",
              source=Endpoint(host="10.0.0.1", port=9092, user="",
                              password="", options=src_options or {}),
              target=Endpoint(host="10.0.0.2", port=9092, user="",
                              password=""),
              options={})
    hop.report_dir = lambda db=None: str(where)
    return KafkaEngine(hop)


def test_each_side_reads_through_its_own_client(monkeypatch, tmp_path):
    monkeypatch.setattr(kafkaclient, "native_missing", lambda: None)
    monkeypatch.setattr(kafkaclient, "pure_missing", lambda: None)
    eng = _engine(tmp_path, {"security_protocol": "SASL_SSL",
                             "sasl_mechanism": "AWS_MSK_IAM"})
    assert eng._client_way("src") == "pure"
    assert eng._client_way("dst") == "native"
    assert eng._client_way() == "native"


def test_the_codec_is_measured_a_topic_at_a_time(monkeypatch, tmp_path):
    monkeypatch.setattr(
        kafkaclient, "ratios",
        lambda s: {"zstd": 0.2, "lz4": 0.3} if s and s[0].startswith(b"a")
        else ({"zstd": 1.0, "lz4": 1.0} if s else {}))
    eng = _engine(tmp_path)
    eng.__dict__["_zstd"] = True
    eng._sampled("text", [b"aaaa" * 100])
    eng._sampled("random", [b"\x93\x11\xfe"])
    eng._sampled("text", [b"zz"])
    assert eng._codec("native", "text") == "zstd"
    assert eng._codec("native", "random") is None
    assert eng._codec("native", "unsampled") is None


def test_a_refusal_steps_that_topic_down_and_no_other(monkeypatch,
                                                       tmp_path):
    monkeypatch.setattr(kafkaclient, "ratios",
                        lambda s: {"zstd": 0.2, "lz4": 0.3} if s else {})
    eng = _engine(tmp_path)
    eng.__dict__["_zstd"] = True
    eng._sampled("a", [b"x"])
    eng._sampled("b", [b"x"])
    assert eng._stepped("native", "a", "zstd",
                        "UNSUPPORTED_COMPRESSION_TYPE") == "lz4"
    assert eng._codec("native", "a") == "lz4"
    assert eng._codec("native", "b") == "zstd"
