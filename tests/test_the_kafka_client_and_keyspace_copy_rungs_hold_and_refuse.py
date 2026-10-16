"""The rungs the stream client, its compression and the keyspace copy's
source are chosen by, each held where its needs hold and refused with its
own reason where one does not, on fakes (backlog
AUD-Z.rungs-outside-owed): no client that is missing, no codec the
client, the brokers or the payload do not take, and no backup file the
hop does not name."""
import pytest

from migkit import decide, kafkaclient, redisshake
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ('kafkaclient', '_client_rungs', 'native', 'holds'),
    ('kafkaclient', '_client_rungs', 'native', 'refuses'),
    ('kafkaclient', '_client_rungs', 'pure', 'holds'),
    ('kafkaclient', '_client_rungs', 'pure', 'refuses'),
    ('kafkaclient', '_codec_rungs', 'lz4', 'holds'),
    ('kafkaclient', '_codec_rungs', 'lz4', 'refuses'),
    ('kafkaclient', '_codec_rungs', 'none', 'holds'),
    ('kafkaclient', '_codec_rungs', 'zstd', 'holds'),
    ('kafkaclient', '_codec_rungs', 'zstd', 'refuses'),
    ('redisshake', '_rungs', 'aof', 'holds'),
    ('redisshake', '_rungs', 'aof', 'refuses'),
    ('redisshake', '_rungs', 'rdb', 'holds'),
    ('redisshake', '_rungs', 'rdb', 'refuses'),
    ('redisshake', '_rungs', 'scan', 'holds'),
    ('redisshake', '_rungs', 'sync', 'holds'),
    ('redisshake', '_rungs', 'sync', 'refuses'),
]


def _rung(ladder, name):
    return next(r for r in ladder if r.name == name)


def _held_then_refused(rung, ok, bad):
    with watched() as seen:
        kept, _ = decide.prune([rung], decide.Facts(ok))
        _, out = decide.prune([rung], decide.Facts(bad))
    assert kept == [rung] and [r for r, _ in out] == [rung]
    assert branch(seen, rung.name) == [HOLDS, REFUSES], seen
    assert why(seen, rung.name) == out[0][1]
    return out[0][1]


CLIENT = {"native_missing": None, "cloud_sign_in": False,
          "pure_missing": None}
CODEC = {"has_zstd": True, "has_lz4": True, "refused": {},
         "zstd_target": True, "ratios": {"zstd": 0.3, "lz4": 0.5}}


@pytest.mark.parametrize("name,bad,said", [
    ("native", {"native_missing": "the compiled client is not installed"},
     "the compiled client is not installed"),
    ("native", {"cloud_sign_in": True}, "the cloud's own credentials"),
    ("pure", {"pure_missing": "the pure-Python client is not installed"},
     "the pure-Python client is not installed"),
])
def test_a_client_is_held_where_it_is_here_and_refused_where_not(
        name, bad, said):
    got = _held_then_refused(_rung(kafkaclient._client_rungs(), name),
                             CLIENT, dict(CLIENT, **bad))
    assert said in got


@pytest.mark.parametrize("name,bad,said", [
    ("zstd", {"has_zstd": False}, "has no zstd"),
    ("zstd", {"zstd_target": False}, "older than 2.1"),
    ("zstd", {"refused": {"zstd": "UNSUPPORTED_COMPRESSION_TYPE"}},
     "refused it at send time"),
    ("zstd", {"ratios": {"zstd": 0.97}}, "compress only to 97%"),
    ("lz4", {"has_lz4": False}, "has no lz4"),
    ("lz4", {"ratios": {}}, "no message was there to measure"),
])
def test_a_codec_is_held_where_it_pays_and_refused_where_not(name, bad,
                                                             said):
    got = _held_then_refused(_rung(kafkaclient._codec_rungs(), name),
                             CODEC, dict(CODEC, **bad))
    assert said in got


def test_messages_go_as_they_are_wherever_nothing_is_gained():
    with watched() as seen:
        kept, _ = decide.prune(kafkaclient._codec_rungs(),
                               decide.Facts(dict(CODEC, has_zstd=False,
                                                 has_lz4=False)))
    assert [r.name for r in kept] == ["none"]
    assert branch(seen, "none") == [HOLDS]


KEYSPACE = {"rdb_file": "/backups/dump.rdb", "aof_file": "/backups/a.aof",
            "replica_refused": None}


@pytest.mark.parametrize("name,bad,said", [
    ("rdb", {"rdb_file": None}, "names no backup file"),
    ("aof", {"aof_file": ""}, "names no append-only file"),
    ("sync", {"replica_refused": "the source refuses a replica's SYNC"},
     "refuses a replica's SYNC"),
])
def test_a_keyspace_source_is_held_where_the_hop_gives_it(name, bad, said):
    got = _held_then_refused(_rung(redisshake._rungs(), name), KEYSPACE,
                             dict(KEYSPACE, **bad))
    assert said in got


def test_the_scan_is_held_where_nothing_else_is():
    with watched() as seen:
        kept, _ = decide.prune(redisshake._rungs(), decide.Facts(
            {"rdb_file": None, "aof_file": None,
             "replica_refused": "refused"}))
    assert [r.name for r in kept] == ["scan"]
    assert branch(seen, "scan") == [HOLDS]
