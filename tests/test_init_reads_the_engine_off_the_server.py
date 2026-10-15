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


def _servers(monkeypatch, answers, signed=None, signins=None):
    """`answers`: {port: reply to its own protocol's hello}; every other
    knock gets nothing back. `signed`: {port: what the server says of
    itself once signed in}; a port not in it says why it could not be
    asked. Each sign-in goes into `signins`: no test opens a connection."""
    asked = []

    def exchange(host, port, hello, hear, whole=False, wait=0):
        asked.append((port, hello))
        got = answers.get(int(port), b"")
        return got if hear(got) is not None else b""

    def said(family, host, port, user, password, db):
        if signins is not None:
            signins.append((family, host, int(port), user, password, db))
        got = (signed or {}).get(int(port))
        return got if got is not None else "nobody signs in here"
    monkeypatch.setattr(engineat, "_exchange", exchange)
    monkeypatch.setattr(engineat, "_reachable",
                        lambda h, p, wait=0: int(p) in answers)
    monkeypatch.setattr(engineat, "_said_after_sign_in", said)
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


def _whole_hop(monkeypatch, tmp_path, signed=None):
    import migkit.config as cfg
    from migkit import guide
    signins = []
    _servers(monkeypatch, {5432: b"S", 3306: _packet(MYSQL_GREETING)},
             signed, signins)
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "r")
    monkeypatch.setattr(guide, "survey", lambda hop, say, imported=None: {
        "sides": {}, "tables": [], "reached": False})
    monkeypatch.setattr(guide, "plan", lambda *a: {})
    env = {"MIGKIT_SOURCE": "app@db1.example.com:5432/shop",
           "MIGKIT_TARGET": "app@db2.example.com:3306/shop",
           "MIGKIT_SOURCE_PASSWORD": "CHANGE_ME",
           "MIGKIT_TARGET_PASSWORD": "CHANGE_ME", "MIGKIT_SECRETS": "env"}
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    said = []
    got = guide.run(None, said.append, tmp_path / "hops.yaml", env=env)
    return got, said, signins


def test_both_knocked_sides_are_asked_their_brand_once(monkeypatch,
                                                       tmp_path):
    got, said, signins = _whole_hop(monkeypatch, tmp_path)
    # each side read off the server is signed in to once, and a server
    # that says nothing more keeps the family it greeted as
    assert sorted((f, h, p) for f, h, p, *_ in signins) == [
        ("mysql", "db2.example.com", 3306),
        ("postgres", "db1.example.com", 5432)], signins
    assert {u for _, _, _, u, _, _ in signins} == {"app"}
    assert {d for *_, d in signins} == {"shop"}
    assert got["config"]["target"].startswith("mysql://")
    assert not [s for s in said if "signed in as" in str(s)], said


def test_a_mariadb_that_greets_as_mysql_is_written_as_mariadb(monkeypatch,
                                                              tmp_path):
    got, said, _ = _whole_hop(monkeypatch, tmp_path, signed={3306: {
        "version": "11.8.2-MariaDB-ubu2404",
        "version_comment": "mariadb.org binary distribution",
        "endpoint": "db2.example.com"}})
    assert got["config"]["target"].startswith("mariadb://app:"), got
    assert "@db2.example.com" in got["config"]["target"]
    assert got["config"]["source"].startswith("postgres://")
    assert any("db2.example.com:3306 signed in as mariadb" in str(s)
               for s in said), said


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


# --- Oracle, Cassandra, Db2 and Teradata, each by its own answer ----------

def _tns(kind, data=b""):
    body = struct.pack(">HH", 0, len(data)) + data
    return struct.pack(">HHBBH", 8 + len(body), 0, kind, 0, 0) + body


TNS_REFUSE = _tns(4, b"(DESCRIPTION=(TMP=)(VSNNUM=318767104)(ERR=12514)"
                     b"(ERROR_STACK=(ERROR=(CODE=12514)(EMFI=4))))")


def test_oracle_is_told_by_its_listener():
    assert engineat._hear_tns(TNS_REFUSE) == {"version": "19.0.0"}
    assert engineat._hear_tns(_tns(2)) == {}
    assert engineat._hear_tns(_tns(5, b"(ADDRESS=(HOST=x))")) == {}
    # a connect packet sent back, another family's answer, a short read
    assert engineat._hear_tns(engineat._tns_hello("h", 1521)) is None
    assert engineat._hear_tns(_packet(MYSQL_GREETING)) is None
    assert engineat._hear_tns(b"HTTP/1.1 400 Bad\r\n\r\n") is None
    assert engineat._hear_tns(TNS_REFUSE[:6]) is None


def _cql(op, body, version=0x84):
    return bytes([version, 0, 0, 0, op]) + struct.pack(">i", len(body)) \
        + body


def _multimap(d):
    out = struct.pack(">H", len(d))
    for k, vals in d.items():
        out += struct.pack(">H", len(k)) + k.encode() \
            + struct.pack(">H", len(vals))
        for v in vals:
            out += struct.pack(">H", len(v)) + v.encode()
    return out


def test_cassandra_is_told_by_its_supported_answer():
    supported = _cql(0x06, _multimap({
        "COMPRESSION": ["lz4", "snappy"], "CQL_VERSION": ["3.4.5"],
        "PROTOCOL_VERSIONS": ["3/v3", "4/v4", "5/v5-beta"]}))
    assert engineat._hear_cql(supported) == {"version": "3.4.5",
                                             "note": "CQL 3.4.5"}
    reason = b"Invalid or unsupported protocol version"
    older = _cql(0x00, struct.pack(">iH", 0x0a, len(reason)) + reason,
                 version=0x83)
    assert engineat._hear_cql(older) == {"note": "native protocol v3"}
    # no CQL version named, its own request sent back, cut short, another
    # family's answer
    assert engineat._hear_cql(_cql(0x06, _multimap({
        "COMPRESSION": ["lz4"]}))) is None
    assert engineat._hear_cql(engineat._cql_hello("h", 9042)) is None
    assert engineat._hear_cql(supported[:-3]) is None
    assert engineat._hear_cql(b"+PONG\r\n") is None
    assert engineat._hear_cql(struct.pack(">iih", 6, 0x6d6b, 0)) is None


def _excsatrd(level, cls="QDB2/LINUXX8664"):
    params = engineat._ddm(0x1147, cls.encode("cp500")) \
        + engineat._ddm(0x115a, level.encode("cp500"))
    ddm = engineat._ddm(0x1443, params)
    return struct.pack(">HBBH", 6 + len(ddm), 0xd0, 0x03, 1) + ddm


def test_db2_is_told_by_its_release_level():
    assert engineat._hear_drda(_excsatrd("SQL11055")) == {
        "engine": "db2", "version": "11.5.5", "note": "QDB2/LINUXX8664"}
    assert engineat._hear_drda(_excsatrd("DSN12015", "QDB2"))[
        "engine"] == "db2zos"
    assert engineat._hear_drda(_excsatrd("QSQ07050", "QAS"))[
        "engine"] == "db2i"
    # another server on the same protocol is no Db2; its own exchange sent
    # back, cut short, another family's answer
    assert engineat._hear_drda(_excsatrd("CSS10170", "Derby")) is None
    assert engineat._hear_drda(engineat._drda_hello("h", 50000)) is None
    assert engineat._hear_drda(_excsatrd("SQL11055")[:-4]) is None
    assert engineat._hear_drda(_tds_reply(16, 0, 4135)) is None


def _td(body, version=3, cls=1):
    head = bytearray(engineat._TD_HEADER)
    head[0], head[1], head[2] = version, cls, 14
    head[3:5] = (len(body) >> 16).to_bytes(2, "big")
    head[8:10] = (len(body) & 0xffff).to_bytes(2, "big")
    return bytes(head) + body


def test_teradata_is_told_by_its_gateways_answer():
    """The header's layout is read from the protocol, not yet heard from a
    real gateway (unverified against a server)."""
    assert engineat._hear_td(_td(b"\x00" * 40)) == {
        "note": "gateway message version 3"}
    assert engineat._hear_td(_td(b"\x01" * 9, version=4)) == {
        "note": "gateway message version 4"}
    # its own request sent back has no body: no gateway answers so
    assert engineat._hear_td(engineat._td_hello("h", 1025)) is None
    assert engineat._hear_td(_td(b"\x00" * 40)[:-1]) is None
    assert engineat._hear_td(_td(b"\x00" * 40, version=5)) is None
    assert engineat._hear_td(_td(b"\x00" * 40, cls=2)) is None
    assert engineat._hear_td(_packet(MYSQL_GREETING)) is None


@pytest.mark.parametrize("port,reply,want", [
    (1521, TNS_REFUSE, "oracle"),
    (9042, _cql(0x06, _multimap({"CQL_VERSION": ["3.4.7"]})), "cassandra"),
    (50000, _excsatrd("DSN13015", "QDB2"), "db2zos"),
    (1025, _td(b"\x00" * 40), "teradata")])
def test_each_new_wire_is_one_address(monkeypatch, port, reply, want):
    _servers(monkeypatch, {port: reply})
    said = []
    assert engineat.address(f"app@db5.example.com:{port}/x", None,
                            said.append) == \
        f"{want}://app@db5.example.com:{port}/x"
    assert len(said) == 1 and f"answers as {want}" in said[0], said


# --- the brand after sign-in ----------------------------------------------

def test_a_brand_on_a_shared_wire_is_said_after_sign_in(monkeypatch):
    signins, said = [], []
    _servers(monkeypatch, {3306: b"", 4000: b"", 26257: b"", 5432: b""},
             signed={
        3306: {"version": "11.8.2-MariaDB-ubu2404",
               "version_comment": "mariadb.org binary distribution"},
        4000: {"version": "8.0.11-TiDB-v8.5.1",
               "version_comment": "TiDB Server (Apache License 2.0)"},
        26257: {"version": "CockroachDB CCL v23.2.5 (x86_64-pc-linux-gnu)",
                "server_version": "13.0.0"},
        5432: {"version": "PostgreSQL 16", "server_version": "16",
               "endpoint": "abcdefgh.dsql.us-east-1.on.aws"}},
        signins=signins)
    got = engineat.rebrand("mysql://app:env:X@db1.example.com:3306/shop",
                           "CHANGE_ME", said.append)
    assert got == "mariadb://app:env:X@db1.example.com:3306/shop"
    assert signins == [("mysql", "db1.example.com", 3306, "app",
                        "CHANGE_ME", "shop")]
    assert said == ["db1.example.com:3306 signed in as mariadb"
                    " 11.8.2-MariaDB-ubu2404 (its own version text; it"
                    " greeted as mysql)"]
    assert engineat.rebrand("mysql://app@db1.example.com:4000/shop", None,
                            said.append).startswith("tidb://")
    assert engineat.rebrand("postgres://root@db3.example.com:26257/x",
                            None, said.append).startswith("cockroachdb://")
    assert engineat.rebrand(
        "postgres://admin@abcdefgh.dsql.us-east-1.on.aws/postgres", None,
        said.append).startswith("dsql://")


def test_a_server_that_says_nothing_more_keeps_its_family(monkeypatch):
    signins, said = [], []
    _servers(monkeypatch, {3306: b"", 3307: b"", 9092: b""}, signed={
        3307: {"version": "8.4.0",
               "version_comment": "MySQL Community Server - GPL"}},
        signins=signins)
    # the software itself, a sign-in refused, a server not reached, a wire
    # only one engine speaks: never a brand guessed
    for text in ("mysql://app@db1.example.com:3307/shop",
                 "mysql://app@db1.example.com:3306/shop",
                 "mysql://app@db1.example.com:3399/shop",
                 "kafka://db1.example.com:9092"):
        assert engineat.rebrand(text, "CHANGE_ME", said.append) == text
    assert said == []
    # only the two reached on a wire several brands speak were asked
    assert [p for _, _, p, *_ in signins] == [3307, 3306]
