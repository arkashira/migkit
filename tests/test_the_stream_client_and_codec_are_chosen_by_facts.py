"""The client a Kafka target is written through, and the codec its
messages go in, are chosen from facts (backlog R18.7): installed or not,
how the side signs in, how well the first messages compress, which
brokers the target runs. The compiled client is driven through a double:
no server is reached."""
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
