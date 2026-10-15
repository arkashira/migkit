"""`migkit init` from an address that names no engine (backlog
P0-MINIMAL-INPUT): the server's own first words say what it is
(`migkit.engineat`), a server that does not is the one question asked
with its port's engine proposed, and what a user lacks is said with the
statements that grant it."""
import struct

import pytest

from migkit import engineat

MYSQL_GREETING = (b"\x0a" + b"8.4.0\0" + b"\x01\0\0\0" + b"abcdefgh\0"
                  + b"\xff\xf7" + b"\x21\x02\0\xff\xc1\x15" + b"\0" * 10)
SSL_REQUEST = struct.pack("!ii", 8, 80877103)


def _packet(body, seq=0):
    return len(body).to_bytes(3, "little") + bytes([seq]) + body


def _tds_reply(major, minor, build):
    payload = bytes([0x00, 0, 6, 0, 6, 0xff]) + bytes(
        [major, minor]) + build.to_bytes(2, "big") + b"\0\0"
    return struct.pack(">BBHHBB", 4, 1, 8 + len(payload), 0, 1, 0) \
        + payload


def test_each_protocol_is_told_by_its_own_answer():
    assert engineat._hear_mysql(_packet(MYSQL_GREETING)) == {
        "version": "8.4.0"}
    maria = MYSQL_GREETING.replace(b"8.4.0", b"5.5.5-11.8.2-MariaDB")
    assert engineat._hear_mysql(_packet(maria))["version"] == \
        "11.8.2-MariaDB"
    assert engineat._hear_mysql(_packet(MYSQL_GREETING)[:8]) is None
    assert engineat._hear_postgres(b"S") == {}
    assert engineat._hear_postgres(b"N") == {}
    assert engineat._hear_postgres(b"SSH-2.0") is None
    assert engineat._hear_tds(_tds_reply(16, 0, 4135)) == {
        "version": "16.0.4135"}
    assert engineat._hear_tds(b"\x04\x01\x00") is None
    doc = engineat._bson([("ok", 1), ("maxWireVersion", 21)])
    reply = struct.pack("<iiii", 16 + 5 + len(doc), 9, 7, 2013) + \
        struct.pack("<I", 0) + b"\0" + doc
    assert engineat._hear_mongo(reply) == {"note": "wire version 21"}
    other = reply[:8] + struct.pack("<i", 8) + reply[12:]
    assert engineat._hear_mongo(other) is None
    assert engineat._hear_redis(b"-NOAUTH Authentication required.\r\n") \
        == {}
    assert engineat._hear_redis(b"+PONG\r\n") == {}
    assert engineat._hear_redis(b"+OK POP3 ready\r\n") is None
    assert engineat._hear_kafka(struct.pack(">iih", 6, 0x6d6b, 0)) == {}
    assert engineat._hear_kafka(struct.pack(">iih", 6, 1, 0)) is None
    body = b'{"version": {"distribution": "opensearch", "number": "2.11.0"}}'
    assert engineat._hear_http(b"HTTP/1.1 200 OK\r\n\r\n" + body) == {
        "engine": "opensearch", "version": "2.11.0"}
    assert engineat._hear_http(
        b"HTTP/1.0 200 OK\r\nX-ClickHouse-Server-Display-Name: a\r\n\r\nOk."
    ) == {"engine": "clickhouse"}


def _servers(monkeypatch, answers):
    """`answers`: {port: reply to its own protocol's hello}; every other
    knock gets nothing back."""
    asked = []

    def exchange(host, port, hello, hear, whole=False, wait=0):
        asked.append((port, hello))
        got = answers.get(int(port), b"")
        return got if hear(got) is not None else b""
    monkeypatch.setattr(engineat, "_exchange", exchange)
    monkeypatch.setattr(engineat, "_reachable",
                        lambda h, p, wait=0: int(p) in answers)
    return asked


def test_the_engine_and_version_come_from_the_server(monkeypatch):
    knocked = _servers(monkeypatch, {5432: b"S",
                                     3306: _packet(MYSQL_GREETING)})
    said = []
    got = engineat.address("app@db1.example.com:5432/shop", None,
                           said.append)
    assert got == "postgres://app@db1.example.com:5432/shop"
    # the port's own engine was asked first, and nothing after it
    assert knocked == [(5432, SSL_REQUEST)]
    assert said == ["db1.example.com:5432 answers as postgres (the"
                    " server's own answer to a request for TLS)"]
    said.clear()
    # a host alone: every engine's own port is knocked on at once
    _servers(monkeypatch, {3306: _packet(MYSQL_GREETING)})
    assert engineat.address("app@db2.example.com/shop", None,
                            said.append) == \
        "mysql://app@db2.example.com:3306/shop"
    assert said == ["db2.example.com:3306 answers as mysql 8.4.0 (the"
                    " server's own greeting)"]


def test_a_server_that_does_not_say_is_one_question(monkeypatch):
    _servers(monkeypatch, {5432: b"\x00garbage"})
    questions = []

    def ask(text, secret=False, default=""):
        questions.append((text, default))
        return ""
    got = engineat.address("app@db1.example.com:5432/shop", ask,
                           lambda _: None)
    assert got == "postgres://app@db1.example.com:5432/shop"
    assert len(questions) == 1, questions
    text, default = questions[0]
    assert default == "postgres"
    assert text.startswith("Which engine answers at db1.example.com:5432?"
                           " migkit could not tell:"), text
    # without a terminal it stops with the address to write instead
    with pytest.raises(SystemExit) as e:
        engineat.address("app@db1.example.com:5432/shop", None,
                         lambda _: None)
    assert "postgres://app@db1.example.com:5432/shop" in str(e.value)
    _servers(monkeypatch, {})
    with pytest.raises(SystemExit) as e:
        engineat.address("db9.example.com", None, lambda _: None)
    assert "write the port" in str(e.value)


def test_two_bare_addresses_become_a_whole_hop(monkeypatch, tmp_path):
    import migkit.config as cfg
    from migkit import guide
    _servers(monkeypatch, {5432: b"S", 3306: _packet(MYSQL_GREETING)})
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "r")
    monkeypatch.setattr(guide, "survey", lambda hop, say, imported=None: {
        "sides": {}, "tables": [], "reached": False})
    monkeypatch.setattr(guide, "plan", lambda *a: {})
    monkeypatch.setenv("MIGKIT_SOURCE_PASSWORD", "CHANGE_ME")
    monkeypatch.setenv("MIGKIT_TARGET_PASSWORD", "CHANGE_ME")
    env = {"MIGKIT_SOURCE": "app@db1.example.com:5432/shop",
           "MIGKIT_TARGET": "app@db2.example.com:3306/shop",
           "MIGKIT_SOURCE_PASSWORD": "CHANGE_ME",
           "MIGKIT_TARGET_PASSWORD": "CHANGE_ME", "MIGKIT_SECRETS": "env"}
    said = []
    got = guide.run(None, said.append, tmp_path / "hops.yaml", env=env)
    assert got["questions"] == []
    assert got["config"] == {
        "source": "postgres://app:env:MIGKIT_SOURCE_PASSWORD"
                  "@db1.example.com/shop",
        "target": "mysql://app:env:MIGKIT_TARGET_PASSWORD"
                  "@db2.example.com/shop"}, got["config"]
    hop = cfg.load_hops(tmp_path / "hops.yaml")["shop"]
    assert hop.databases == ["shop"]


def test_the_address_s_databases_stay_in_the_hop():
    from migkit import hopkeys
    assert hopkeys.smallest("shop", {
        "source": "postgres://app:env:X@db1.example.com:5432/shop,crm",
        "target": "mysql://root@db2.example.com/shop", "goal": "cutover"
    }) == {"goal": "cutover",
           "source": "postgres://app:env:X@db1.example.com/shop,crm",
           "target": "mysql://root@db2.example.com/shop"}


def test_a_missing_grant_is_said_with_its_statement(monkeypatch):
    from migkit import dba, guide
    from migkit.config import build_hop
    hop = build_hop("shop", {
        "source": "postgres://reader@db1.example.com/shop",
        "target": "postgres://app@db2.example.com/shop"})
    lack = dba.Need("source", "read public.orders", False,
                    "no SELECT on it",
                    fix=["grant select on public.orders to reader;"])
    monkeypatch.setattr(dba, "needs", lambda *a, **k: [lack])
    monkeypatch.setattr(dba, "write", lambda *a, **k: {})
    said = []
    guide.plan(hop, {"eng": object()}, "copy", said.append)
    text = "\n".join(map(str, said))
    assert "source: read public.orders (no SELECT on it)" in text, text
    assert "      grant select on public.orders to reader;" in said, said


def test_init_is_routed_through_the_decision_engine():
    from migkit import decide
    assert decide.TASKS["init"][1] == ("routed", "guide", "engineat")
    assert not [t for t in decide.task_gaps() if t[0] == "init"]


@pytest.mark.docker
def test_a_postgres_server_says_what_it_is():
    """Against the shared PostgreSQL pair (`tests/servers.py` profile
    `pg`): its own answer names it, nothing is asked."""
    from tests import servers
    with servers.space("pg", __name__, own=False) as pair:
        said = []
        got = engineat.address(f"postgres@127.0.0.1:{pair.src.port}/x",
                               None, said.append)
        assert got == f"postgres://postgres@127.0.0.1:{pair.src.port}/x"
        assert said == [f"127.0.0.1:{pair.src.port} answers as postgres"
                        " (the server's own answer to a request for TLS)"]
