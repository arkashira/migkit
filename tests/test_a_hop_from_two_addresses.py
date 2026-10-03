"""Two addresses are a hop (backlog R21).

`source: postgres://app@db1:5432/shop` and `target: mysql://app@db2/shop`
say the engine of each side, the pair, the ports, the databases and TLS;
the long form stays valid and means what it meant. A password is never in
an address; a side given as one signs in with what this machine's
keychain keeps for it.
"""
import json
import textwrap

import pytest

from migkit import addresses, clouds, config, hopkeys
from migkit.engines import NAMES, SCHEMES


@pytest.fixture
def keychain(tmp_path, monkeypatch):
    """A keychain in a file, never this machine's own."""
    import keyring
    from tests.keychain_for_tests import FileKeyring
    monkeypatch.setenv("MIGKIT_TEST_KEYCHAIN", str(tmp_path / "keys.json"))
    was = keyring.get_keyring()
    keyring.set_keyring(FileKeyring())
    yield keyring
    keyring.set_keyring(was)


def _load(tmp_path, text):
    conf = tmp_path / "hops.yaml"
    conf.write_text(textwrap.dedent(text))
    return config.load_hops(conf)


def test_every_engine_has_a_scheme():
    named = set(SCHEMES.values())
    assert set(NAMES) - {"hetero"} <= named, set(NAMES) - named


def test_two_addresses_make_a_cross_engine_hop(tmp_path, keychain):
    hop = _load(tmp_path, """
        hops:
          shop:
            source: postgres://app@db1:6543/shop?sslmode=verify-full&sslrootcert=/ca.pem
            target: mysql://root@db2/shop_new?ssl-mode=VERIFY_CA&ssl-ca=/rds.pem
    """)["shop"]
    assert hop.engine == "hetero"
    assert hop.options == {"source_engine": "postgres",
                           "target_engine": "mysql"}
    assert (hop.source.host, hop.source.port, hop.source.user) == (
        "db1", 6543, "app")
    # the engine's own port where the address names none
    assert hop.target.port == 3306
    assert hop.databases == ["shop"] and hop.db_map == {"shop": "shop_new"}
    assert hop.source.libpq_tls() == {"sslmode": "verify-full",
                                      "sslrootcert": "/ca.pem"}
    tls = hop.target.mysql_tls()
    assert tls["ssl_ca"] == "/rds.pem" and tls["ssl_verify_identity"] \
        is False


def test_the_same_engine_on_both_sides_is_one_engine(tmp_path, keychain):
    hops = _load(tmp_path, """
        hops:
          a:
            source: mongodb://u@h1:27017,h2:27018/app?replicaSet=rs0
            target: mongodb://u@h3/app
          b:
            source: kafka://b1:9092,b2:9093?security_protocol=SASL_SSL
            target: kafka://b3
          c:
            source: sqlite:///tmp/a.db
            target: sqlite:///tmp/b.db
          d:
            source: dynamodb://us-east-1/shop
            target: dynamodb://localhost:8000/shop
    """)
    a, b, c, d = (hops[k] for k in "abcd")
    assert a.engine == "mongodb" and a.source.options == {
        "hosts": "h1:27017,h2:27018", "uri_options": "replicaSet=rs0"}
    assert b.engine == "kafka" and b.source.options["hosts"] == [
        "b1:9092", "b2:9093"]
    assert c.engine == "sqlite" and c.source.options == {"path": "/tmp/a.db"}
    assert d.source.options == {"region": "us-east-1"}
    assert d.target.options == {"endpoint_url": "http://localhost:8000"}


@pytest.mark.parametrize("address,why", [
    ("postgres://app:hunter2@db1/shop", "holds a password"),
    ("postgress://app@db1/shop", "did you mean postgres://"),
    ("postgres://app@db1/shop?sslmod=require", "did you mean sslmode?"),
    ("postgres://app@db1/shop?sslmode=verify", "one of disable"),
    ("mysql://app@db1/shop?ssl-mode=VERIFY_IDENTITY", "ssl-ca="),
    ("mysql://app@db1/shop?ssl-mode=DISABLED", "leave ssl-mode out"),
    ("postgres://app@db1,db2/shop", "is one server"),
])
def test_what_an_address_may_not_say(address, why):
    with pytest.raises(SystemExit) as e:
        addresses.parse(address, "source")
    assert why in str(e.value), str(e.value)
    # and the password never reaches the message
    assert "hunter2" not in str(e.value)


def test_redis_over_tls_is_rediss():
    engine, ep, dbs = addresses.parse("rediss://db1:6380/0")
    assert (engine, ep, dbs) == ("redis", {"host": "db1", "port": 6380,
                                           "tls": True}, ["0"])
    assert addresses.render("redis", ep, dbs, 6379) == "rediss://db1:6380/0"
    # and plain redis:// asks for no TLS of its own
    assert "tls" not in addresses.parse("redis://db1/0")[1]


def test_a_password_by_reference(tmp_path, monkeypatch, keychain):
    monkeypatch.setenv("SRC_PW", "from-env")
    keychain.set_password("migkit", "example-kept", "from-keychain")
    hop = _load(tmp_path, """
        hops:
          h:
            source: postgres://app:env:SRC_PW@db1/shop
            target: {host: db2, user: app, password: "keyring:example-kept"}
    """)["h"]
    assert hop.source.password == "from-env"
    assert hop.target.password == "from-keychain"


def test_a_side_without_a_password_signs_in_with_the_keychains(
        tmp_path, keychain):
    hops = _load(tmp_path, """
        hops:
          h:
            source: postgres://app@db1/shop
            target: postgres://app@db2/shop
    """)
    h = hops["h"]
    # asked only when a connection needs it, and nothing kept is nothing
    assert h.target.password == ""
    keychain.set_password("migkit", "h.source", "s3cret")
    assert h.source.password == "s3cret"
    assert config.keychain_name("h", "source") == "h.source"


def test_a_missing_keyring_reference_is_refused(keychain):
    with pytest.raises(SystemExit) as e:
        config._secret("keyring:nothing-here")
    assert "migkit init" in str(e.value)


def test_the_target_names_as_many_databases_as_the_source(tmp_path):
    with pytest.raises(SystemExit) as e:
        _load(tmp_path, """
            hops:
              h:
                source: mysql://app@db1/a,b
                target: postgres://app@db2/x
        """)
    assert "as many on each" in str(e.value)
    hop = _load(tmp_path, """
        hops:
          h:
            source: mysql://app@db1/a,b
            target: postgres://app@db2/a,y
    """)["h"]
    assert hop.databases == ["a", "b"] and hop.db_map == {"b": "y"}


def test_the_long_form_means_what_it_meant(tmp_path):
    """A hop that says nothing new builds as it always did: the same
    engine default, the same ports, the same keys."""
    hop = _load(tmp_path, """
        hops:
          h:
            source: {host: s, user: u, password: p}
            target: {host: t, port: 6000, user: u, password: p}
            databases: [a]
            workers: 3
    """)["h"]
    assert type(hop.source) is config.Endpoint
    assert (hop.engine, hop.source.port, hop.target.port) == (
        "postgres", 5432, 6000)
    assert (hop.databases, hop.workers, hop.workers_set, hop.goal,
            hop.options) == (["a"], 3, True, "", {})


@pytest.mark.parametrize("engine,endpoint,dbs", [
    ("postgres", {"host": "db1", "port": 6543, "user": "app",
                  "password": "env:X", "sslmode": "verify-full"}, ["shop"]),
    ("mysql", {"host": "db1", "port": 3306, "user": "app",
               "ssl_ca": "/ca.pem", "ssl_verify_identity": False}, ["a"]),
    ("mysql", {"host": "db1", "port": 3306, "user": "app", "ssl": True},
     []),
    ("mongodb", {"hosts": "h1:1,h2:2", "user": "u",
                 "uri_options": "replicaSet=rs0&tls=true"}, ["app"]),
    ("kafka", {"hosts": ["b1:9092", "b2:9092"],
               "security_protocol": "SSL"}, []),
    ("oracle", {"host": "ora", "port": 1521, "user": "scott",
                "service": "FREEPDB1"}, ["SCOTT"]),
    ("snowflake", {"account": "xy123", "user": "u", "database": "DB",
                   "warehouse": "WH"}, ["PUBLIC"]),
    ("bigquery", {"database": "my-project", "location": "EU"}, ["sales"]),
    ("sqlite", {"path": "/tmp/x.db"}, []),
    ("parquet", {"url": "s3://bucket/prefix"}, []),
    ("dynamodb", {"region": "eu-west-1"}, ["shop"]),
    ("pubsub", {"project": "p1"}, []),
    ("redis", {"host": "db1", "port": 6380, "tls": True}, ["0"]),
])
def test_a_side_written_back_as_an_address_reads_the_same(engine, endpoint,
                                                           dbs,
                                                           monkeypatch):
    monkeypatch.setenv("X", "pw")
    default = config.DEFAULT_PORTS.get(engine, 0)
    text = addresses.render(engine, endpoint, dbs, default)
    assert text is not None
    e, ep, names = addresses.parse(text)
    got = config._endpoint(e, ep)
    want = config._endpoint(engine, endpoint)
    assert (e, names) == (engine, list(dbs)), text
    assert (got.host, got.port, got.user, got.password, got.options) == (
        want.host, want.port, want.user, want.password, want.options), text


def test_a_side_an_address_cannot_say_stays_long():
    assert addresses.render("postgres", {"host": "a", "password": "p"}) \
        is None
    assert addresses.render("postgres", {"host": "a", "tunnel": {
        "via": "bastion"}}) is None
    assert addresses.render("mysql", {"host": "a", "port": 0}, [],
                            3306) is None


# ---- who runs a side ------------------------------------------------------

@pytest.mark.parametrize("host,cloud,service", [
    ("shop.example.eu-west-1.rds.amazonaws.com", "aws", "rds"),
    ("shop.cluster-example.eu-west-1.rds.amazonaws.com", "aws", "aurora"),
    ("shop.cluster-ro-example.eu-west-1.rds.amazonaws.com", "aws",
     "aurora-reader"),
    ("px.proxy-example.eu-west-1.rds.amazonaws.com", "aws", "rds-proxy"),
    ("docs.cluster-example.eu-west-1.docdb.amazonaws.com", "aws", "documentdb"),
    ("shop.postgres.database.azure.com", "azure", "flexible"),
    ("shop.database.windows.net", "azure", "sql-database"),
    ("sh-cdb-abcd.sql.tencentcdb.com", "tencent", "tencentdb"),
    ("rm-abc.mysql.rds.aliyuncs.com", "alibaba", "apsaradb"),
    ("pc-abc.mysql.polardb.rds.aliyuncs.com", "alibaba", "polardb"),
    ("1a2b.europe-west1.sql.goog", "gcp", "cloud-sql"),
    ("db.example.com", "", ""),
    ("10.0.0.5", "", ""),
])
def test_the_service_is_read_from_the_host(host, cloud, service):
    got = clouds.from_host(host)
    assert (got.cloud, got.service) == (cloud, service)
    assert got.label


def test_the_server_says_it_where_the_address_does_not():
    class Pg:
        CANON_ENGINE = "postgres"

        def __init__(self, rows):
            self.rows = rows

        def _psql(self, side, db, sql):
            return self.rows
    from migkit.config import Endpoint
    ep = Endpoint(host="10.0.0.5")
    assert clouds.of(Pg("rds_superuser\x1ffalse"), "src", ep).service == "rds"
    assert clouds.of(Pg("rds_superuser\x1ftrue"), "src", ep).service \
        == "aurora"
    assert clouds.of(Pg("cloudsqlsuperuser\x1ffalse"), "src",
                     ep).cloud == "gcp"
    # nothing said by either is a server run by its owner
    assert clouds.of(Pg("\x1ffalse"), "src", ep).cloud == ""
    # the reader endpoint's name is kept where the server says aurora
    reader = Endpoint(host="x.cluster-ro-example.eu-west-1.rds.amazonaws.com")
    got = clouds.of(Pg("rds_superuser\x1ftrue"), "src", reader)
    assert got.service == "aurora-reader" and len(got.evidence) == 2


def test_a_services_own_settings_say_it_before_its_roles():
    """A service's roles come along with every dump of the roles, so a
    server restored from one read as the service; the settings it defines
    for itself do not (audit 2026-10-01, K18)."""
    class Pg:
        CANON_ENGINE = "postgres"

        def __init__(self, rows):
            self.rows, self.asked = rows, ""

        def _psql(self, side, db, sql):
            self.asked = sql
            return self.rows
    from migkit.config import Endpoint
    ep = Endpoint(host="10.0.0.5")
    eng = Pg("rds_superuser\x1ffalse\x1fcloudsql")
    got = clouds.of(eng, "src", ep)
    assert (got.cloud, got.service) == ("gcp", "cloud-sql"), got
    assert got.evidence == ["the server (its cloudsql.* settings)"], got
    assert "pg_settings" in eng.asked
    got = clouds.of(Pg("\x1ffalse\x1fazure"), "src", ep)
    assert (got.cloud, got.service) == ("azure", "flexible"), got
    # no setting of a service's: its role still says it
    got = clouds.of(Pg("rds_superuser\x1ffalse\x1f"), "src", ep)
    assert got.service == "rds" and "role rds_superuser" in got.evidence[0]


def test_a_goal_stands_for_options_and_the_hop_wins(tmp_path):
    hops = _load(tmp_path, """
        hops:
          a:
            source: postgres://app@db1/shop
            target: postgres://app@db2/shop
            goal: cutover
          b:
            source: postgres://app@db1/shop
            target: postgres://app@db2/shop
            goal: cutover
            options: {protect_target: false}
          c:
            source: mongodb://u@h1/app
            target: mongodb://u@h2/app
            goal: keep-in-sync
          d:
            source: postgres://app@db1/shop
            target: postgres://app@db2/shop
            goal: two-way
    """)
    assert hops["a"].options == {"reverse": "at_cutover",
                                 "protect_target": True}
    assert hops["b"].options == {"reverse": "at_cutover",
                                 "protect_target": False}
    # a target migkit cannot keep the application off: not set at all
    assert hops["c"].options == {}
    assert hops["d"].options == {"topology": "two_way", "two_way": True}


def test_a_verify_hop_writes_nothing(tmp_path):
    hop = _load(tmp_path, """
        hops:
          v:
            source: postgres://app@db1/shop
            target: postgres://app@db2/shop
            goal: verify
    """)["v"]
    with pytest.raises(SystemExit) as e:
        hopkeys.refuse_writes(hop, "a move")
    assert "goal verify" in str(e.value)
    hop.goal = "copy"
    assert hopkeys.refuse_writes(hop, "a move") is None


def test_a_verify_hop_refuses_a_move_from_the_command_line(tmp_path):
    import os
    import subprocess
    from pathlib import Path

    from tests.conftest import migkit_cmd
    conf = tmp_path / "hops.yaml"
    conf.write_text(f"hops:\n  v:\n    source: sqlite://{tmp_path}/a.db\n"
                    f"    target: sqlite://{tmp_path}/b.db\n"
                    "    goal: verify\n")
    env = dict(os.environ, MIGKIT_CONF=str(conf),
               MIGKIT_REPORTS=str(tmp_path / "r"),
               PYTHONPATH=str(Path(__file__).resolve().parents[1]))
    r = subprocess.run(migkit_cmd() + ["move", "v", "--go"],
                       capture_output=True, text=True, env=env)
    assert r.returncode != 0
    assert "goal verify" in r.stdout + r.stderr, r.stdout + r.stderr
    assert not (tmp_path / "b.db").exists()


def test_a_hop_from_the_keychain_is_never_this_machines(keychain):
    """The fixture above swaps the keychain for a file: what the tests
    store lands there."""
    keychain.set_password("migkit", "t.source", "x")
    import os
    data = json.loads(open(os.environ["MIGKIT_TEST_KEYCHAIN"]).read())
    assert data == {"migkit/t.source": "x"}
