"""The configuration `migkit init` writes is the smallest that says the
hop, and loads into the same decisions as the long form it stands for
(backlog R21: "only what differs from migkit's own decisions").
"""
import yaml

import pytest

from migkit import hopkeys
from migkit.config import build_hop

LONG = {
    "pg-to-mysql": {
        "engine": "hetero", "service": "native",
        "source": {"host": "db1.example.com", "port": 5432, "user": "app",
                   "password": "env:SRC_PW",
                   "options": {"sslmode": "verify-full",
                               "sslrootcert": "/etc/ca.pem"}},
        "target": {"host": "10.0.0.7", "port": 3306, "user": "app",
                   "password": "env:DST_PW", "ssl_ca": "/etc/rds.pem"},
        "databases": ["shop"], "db_map": {"shop": "shop_v2"},
        "big_rows": 5000000, "slice": 1000000,
        "options": {"source_engine": "postgres", "target_engine": "mysql"},
    },
    "same-engine": {
        "engine": "postgres",
        "source": {"host": "a.example.com", "port": 5432, "user": "u",
                   "password": "keyring:same-engine.source"},
        "target": {"host": "b.example.com", "port": 6432, "user": "u",
                   "password": "keyring:same-engine.target"},
        "databases": ["x", "y"], "db_map": {"x": "x", "y": "y2"},
        "exclude": ["x.public.tmp_*"],
        "options": {"mask": ["users.email"], "reverse": "at_cutover"},
    },
    "a-ceiling": {
        "engine": "mysql",
        "source": {"host": "m1", "user": "r", "password": "env:SRC_PW"},
        "target": {"host": "m2", "user": "r", "password": "env:DST_PW"},
        "workers": 4, "slice": 200000,
    },
    "mongo-replicas": {
        "engine": "mongodb",
        "source": {"user": "u", "password": "env:SRC_PW",
                   "hosts": "h1:27017,h2:27017",
                   "uri_options": "replicaSet=rs0"},
        "target": {"host": "h3", "user": "u", "password": "env:DST_PW"},
        "databases": ["app"],
    },
}


@pytest.fixture(autouse=True)
def _secrets(monkeypatch, tmp_path):
    import keyring
    from tests.keychain_for_tests import FileKeyring
    monkeypatch.setenv("SRC_PW", "s")
    monkeypatch.setenv("DST_PW", "d")
    monkeypatch.setenv("MIGKIT_TEST_KEYCHAIN", str(tmp_path / "k.json"))
    was = keyring.get_keyring()
    keyring.set_keyring(FileKeyring())
    for side in ("source", "target"):
        keyring.get_keyring().set_password("migkit", f"same-engine.{side}",
                                           side)
    yield
    keyring.set_keyring(was)


def _lines(name, raw):
    return len(yaml.safe_dump({name: raw}, sort_keys=False,
                              default_flow_style=False).splitlines())


@pytest.mark.parametrize("name", sorted(LONG))
def test_the_smallest_loads_into_the_same_decisions(name):
    raw = LONG[name]
    small = hopkeys.smallest(name, raw)
    assert hopkeys.decisions(build_hop(name, small)) == hopkeys.decisions(
        build_hop(name, raw)), small
    assert not [p for p in hopkeys.check({"hops": {name: small}})
                if p.level == "error"]
    assert _lines(name, small) < _lines(name, raw), small


def test_what_the_smallest_of_each_is():
    got = hopkeys.smallest("pg-to-mysql", LONG["pg-to-mysql"])
    assert got == {
        "source": "postgres://app:env:SRC_PW@db1.example.com/shop?"
                  "sslmode=verify-full&sslrootcert=%2Fetc%2Fca.pem",
        "target": "mysql://app:env:DST_PW@10.0.0.7/shop_v2?"
                  "ssl-mode=VERIFY_IDENTITY&ssl-ca=%2Fetc%2Frds.pem"}, got
    same = hopkeys.smallest("same-engine", LONG["same-engine"])
    # the keychain's own entries are the ones a bare address signs in with
    assert same["source"] == "postgres://u@a.example.com/x,y"
    assert same["target"] == "postgres://u@b.example.com:6432/x,y2"
    assert same["options"] == {"mask": ["users.email"],
                               "reverse": "at_cutover"}
    # a ceiling the hop set is a decision it made, and stays
    ceiling = hopkeys.smallest("a-ceiling", LONG["a-ceiling"])
    assert ceiling["workers"] == 4 and ceiling["slice"] == 200000


def test_the_comparison_can_say_different():
    """The property above is only worth something if a changed hop reads
    as changed: each of these differs from the long form in one decision."""
    raw = LONG["pg-to-mysql"]
    base = hopkeys.decisions(build_hop("pg-to-mysql", raw))
    small = hopkeys.smallest("pg-to-mysql", raw)
    for change in (
            {"target": small["target"].replace("shop_v2", "shop")},
            {"source": small["source"].replace("db1.", "db9.")},
            {"source": small["source"].replace("verify-full", "require")},
            {"goal": "cutover"},
            {"workers": 2}):
        other = build_hop("pg-to-mysql", {**small, **change})
        assert hopkeys.decisions(other) != base, change


def test_a_side_an_address_cannot_say_keeps_the_hop_long():
    raw = {"engine": "aurora-postgres",
           "source": {"host": "a", "user": "u", "password": "env:SRC_PW"},
           "target": {"host": "b", "user": "u", "password": "env:DST_PW"},
           "databases": ["d"]}
    got = hopkeys.smallest("alias", raw)
    # the engine is written as an alias the address has no scheme for
    assert got["engine"] == "aurora-postgres" and got["databases"] == ["d"]
    assert hopkeys.decisions(build_hop("alias", got)) == hopkeys.decisions(
        build_hop("alias", raw))
