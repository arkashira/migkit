"""Redis's own copy tool carries a keyspace only into Redis's own products.

Its licence (BSL 1.1, Redis Ltd.) grants production use with Redis
Community Edition, Redis Cloud and Redis Software, and nothing else: a
Valkey, KeyDB or Dragonfly target, or a managed service running Redis
under another name, takes migkit's own copier - and the reason is said,
without the tool's name. The addresses and passwords go in its
environment, never on its command line.
"""
import pytest

from migkit import movers
from migkit.config import Endpoint, Hop


class _Brand:
    def __init__(self, name):
        self.name, self.version = name, "7.4.0"


#: the two sides' passwords, which must never reach a command line
SECRETS = ("CHANGE_ME_SRC", "CHANGE_ME_DST")


def _hop(target_host="10.0.0.9", exclude=(), **target_options):
    return Hop(name="r", engine="redis",
               source=Endpoint(host="10.0.0.8", port=6379, user="",
                               password=SECRETS[0]),
               target=Endpoint(host=target_host, port=6379, user="app",
                               password=SECRETS[1],
                               options=target_options),
               databases=["0"], exclude=list(exclude))


@pytest.fixture
def brand(monkeypatch):
    from migkit.engines import redis as r
    said = {"dst": "redis"}
    monkeypatch.setattr(r.RedisEngine, "_brands",
                        lambda self: (_Brand("redis"), _Brand(said["dst"])))
    monkeypatch.setattr(movers, "which", lambda p: "/x/" + p)
    return said


def test_it_is_picked_for_redis_once_installed(monkeypatch):
    monkeypatch.setattr(movers, "which",
                        lambda p: "/x/" + p if p == "riotx" else None)
    assert movers.pick("redis") == "riotx"
    monkeypatch.setattr(movers, "which", lambda p: None)
    assert movers.pick("redis") == "builtin"


@pytest.mark.parametrize("product", ["community", "cloud", "software"])
def test_a_declared_redis_product_takes_it(brand, product):
    assert movers.fitted(_hop(redis_product=product), "redis",
                         "riotx") == ("riotx", None)


def test_redis_cloud_is_known_by_its_name(brand):
    hop = _hop("redis-12000.c1.us-east-1-2.ec2.redislabs.com")
    assert movers.fitted(hop, "redis", "riotx") == ("riotx", None)


def test_any_other_target_takes_migkits_copier_and_says_why(brand):
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    # a managed service running Redis under another name, undeclared
    via, why = movers.fitted(
        _hop("my-cache.abc123.use1.cache.amazonaws.com"), "redis", "riotx")
    assert via == "builtin" and "Redis Community Edition" in why, why
    # a fork, declared or not
    brand["dst"] = "valkey"
    via, why = movers.fitted(_hop(redis_product="community"), "redis",
                             "riotx")
    assert via == "builtin" and "valkey" in why, why
    # and one pattern of keys is all it takes
    brand["dst"] = "redis"
    via, why = movers.fitted(_hop(exclude=["cache:*"],
                                  redis_product="software"),
                             "redis", "riotx")
    assert via == "builtin" and "leaves keys out" in why, why
    for said in (why,):
        assert not [t for t in TOOLS if t in said.lower()], said


def test_no_address_or_password_on_its_command_line():
    steps = movers.riotx_move(_hop(redis_product="community"), "0", 4,
                              False, None)
    argv = steps[0].argv
    assert argv[:4] == ["riotx", "replicate", "--mode", "scan"], argv
    assert "--threads" in argv and "4" in argv
    line = " ".join(argv)
    assert not [s for s in SECRETS if s in line], line
    assert "10.0.0." not in line, line
    # what it runs is said in migkit's words
    assert "keyspace" in str(steps[0]) and "riotx" not in str(steps[0])
