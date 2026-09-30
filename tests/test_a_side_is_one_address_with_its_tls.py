"""A side is rendered as an address one way, and its TLS decided once and
carried in every form a connection is made in (audit 2026-10-01, SEC2,
DU8, SC17).

Fifteen builders escaped the user and the password four ways - with the
user `app@corp` one parsed the host as `corp` - and the hop's TLS reached
migkit's own connections while the copy, the native replications, the
clone and the bulk programs connected as the server offered. `doctor`
said verified. `Endpoint.url` and `Endpoint.tls_forms` are the one place.
"""
from urllib.parse import parse_qs, unquote, urlsplit

import pytest

from migkit import tls
from migkit.config import Endpoint


@pytest.fixture
def asked(monkeypatch):
    """What asking a server about its certificate finds, set by the test:
    nothing leaves the machine."""
    found = {}
    monkeypatch.setattr(tls, "auto", lambda ep, how, ca=None:
                        found.get((ep.host, how)))
    return found


def test_user_password_and_database_are_escaped_whole(asked):
    ep = Endpoint(host="db.example.com", port=5432, user="app@corp",
                  password="CHANGE_ME/:@#?w")
    got = urlsplit(ep.url("postgresql", "shop/v2"))
    assert got.hostname == "db.example.com" and got.port == 5432, got
    assert unquote(got.username) == "app@corp"
    assert unquote(got.password) == "CHANGE_ME/:@#?w"
    assert unquote(got.path) == "/shop/v2"
    # nothing asked of its TLS: libpq's own default, said out loud
    assert parse_qs(got.query) == {"sslmode": ["prefer"]}


def test_an_ipv6_address_and_a_socket_directory(asked):
    six = urlsplit(Endpoint(host="2001:db8::5", port=3306, user="u")
                   .url("mysql", "app", tls=False))
    assert six.hostname == "2001:db8::5" and six.port == 3306, six
    sock = Endpoint(host="/var/run/postgresql", port=5433, user="u",
                    password="").url("postgres", "app")
    got = urlsplit(sock)
    assert got.hostname is None and got.path == "/app", sock
    q = parse_qs(got.query)
    assert q["host"] == ["/var/run/postgresql"] and q["port"] == ["5433"], q


def test_a_scheme_with_no_tls_query_refuses_rather_than_drop_it(asked):
    ep = Endpoint(host="10.0.0.5", port=1433, user="sa", password="x")
    with pytest.raises(ValueError):
        ep.url("mssql", "app")
    assert ep.url("mssql", "app", tls=False) == "mssql://sa:x@10.0.0.5:1433/app"


def test_postgresql_carries_the_decision_everywhere(asked):
    ep = Endpoint(host="10.0.0.5", port=5432, user="app", password="pw",
                  options={"sslmode": "verify-full",
                           "sslrootcert": "/etc/ca.pem"})
    f = ep.tls_forms("postgres")
    assert f["mode"] == "verify-full"
    assert f["libpq"] == {"sslmode": "verify-full",
                          "sslrootcert": "/etc/ca.pem"}
    assert f["env"] == {"PGSSLMODE": "verify-full",
                        "PGSSLROOTCERT": "/etc/ca.pem"}
    assert parse_qs(f["query"]) == {"sslmode": ["verify-full"],
                                    "sslrootcert": ["/etc/ca.pem"]}
    # a server dialling this side has no copy of the authority's file
    assert f["far"] == {"sslmode": "require"}
    assert f["connector"] == {"database.sslmode": "verify-full",
                              "database.sslrootcert": "/etc/ca.pem"}
    assert "sslmode=verify-full" in ep.url("postgres", "app")
    assert ep.libpq_env() == f["env"]


def test_what_asking_the_server_found_is_carried_too(asked):
    """A hop that says nothing of TLS to a server whose certificate
    verifies: every form verifies it - the bulk programs' flags used to
    read the options only."""
    asked[("10.0.0.5", "postgres")] = "/sys/ca.pem"
    asked[("10.0.0.6", "mysql")] = "/sys/ca.pem"
    pg = Endpoint(host="10.0.0.5", port=5432, user="app").tls_forms(
        "postgres")
    assert pg["mode"] == "verify-full" and pg["far"] == {"sslmode": "require"}
    my = Endpoint(host="10.0.0.6", port=3306, user="app").tls_forms("mysql")
    assert my["mode"] == "VERIFY_IDENTITY"
    assert my["dump"] == ["--ssl-mode", "VERIFY_IDENTITY", "--ca",
                          "/sys/ca.pem"]
    assert my["source"] == "SOURCE_SSL = 1" and my["clone"] == "REQUIRE SSL"
    # and a server that does not verify is connected to as before
    plain = Endpoint(host="10.0.0.7", port=3306, user="app").tls_forms(
        "mysql")
    assert plain["mode"] == "PREFERRED" and plain["dump"] == []
    assert plain["source"] == "GET_SOURCE_PUBLIC_KEY = 1"
    assert plain["clone"] == "" and plain["connector"] == {}
    nothing = Endpoint(host="10.0.0.8", port=5432).tls_forms("postgres")
    assert nothing["far"] == {} and nothing["query"] == "sslmode=prefer"


def test_mysql_carries_the_decision_everywhere(asked):
    ep = Endpoint(host="10.0.0.6", port=3306, user="app",
                  options={"ssl_ca": "/etc/ca.pem", "ssl_cert": "/c.pem",
                           "ssl_key": "/k.pem",
                           "ssl_verify_identity": False})
    f = ep.tls_forms("mysql")
    assert f["mode"] == "VERIFY_CA"
    assert f["client"] == ["--ssl-mode=VERIFY_CA", "--ssl-ca=/etc/ca.pem",
                           "--ssl-cert=/c.pem", "--ssl-key=/k.pem"]
    # the dump and load programs know two modes: a named authority is held
    # to the server's name too
    assert f["dump"] == ["--ssl-mode", "VERIFY_IDENTITY", "--ca",
                         "/etc/ca.pem", "--cert", "/c.pem", "--key", "/k.pem"]
    assert f["uri"] == {"ssl-mode": "VERIFY_CA", "ssl-ca": "/etc/ca.pem",
                        "ssl-cert": "/c.pem", "ssl-key": "/k.pem"}
    assert "ssl-mode=VERIFY_CA" in ep.url("mysql", "app")
    # a replica has no copy of the authority's file: encrypted, and the
    # server's public key not fetched in the clear
    assert f["source"] == "SOURCE_SSL = 1" and f["master"] == "MASTER_SSL = 1"
    assert f["connector"] == {"database.ssl.mode": "required"}
    required = Endpoint(host="10.0.0.6", port=3306, options={"ssl": True})
    got = required.tls_forms("mysql")
    assert got["mode"] == "REQUIRED" and got["dump"] == ["--ssl"]
    assert got["client"] == ["--ssl-mode=REQUIRED"]


def test_mongodb_takes_its_hosts_options_and_tls(asked):
    ep = Endpoint(user="app", password="p w", options={
        "hosts": "h1:27017,h2:27017", "uri_options": "replicaSet=rs0",
        "tls": True, "tls_ca_file": "/etc/ca.pem"})
    got = ep.url("mongodb", "shop")
    assert got.startswith("mongodb://app:p%20w@h1:27017,h2:27017/shop?"), got
    q = parse_qs(urlsplit(got).query)
    assert q == {"replicaSet": ["rs0"], "tls": ["true"],
                 "tlsCAFile": ["/etc/ca.pem"]}, q


def test_the_postgresql_schema_readers_build_the_address_this_way(asked,
                                                                   tmp_path):
    """Built by hand, a password holding `@` moved the host, and the
    structural reader dropped the hop's TLS (SC17)."""
    from migkit.config import Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="x", engine="postgres",
              source=Endpoint(host="10.0.0.5", port=5432, user="app",
                              password="CHANGE_ME@ss:w/rd",
                              options={"sslmode": "require"}),
              target=Endpoint(host="10.0.0.6", port=5432, user="app",
                              password="x"),
              databases=["shop"], db_map={"shop": "shop_v2"})
    hop.report_dir = lambda db=None: tmp_path
    src, dst = PostgresEngine(hop)._schema_urls("shop")
    s, d = urlsplit(src), urlsplit(dst)
    assert (s.hostname, unquote(s.password)) == ("10.0.0.5",
                                                 "CHANGE_ME@ss:w/rd")
    assert parse_qs(s.query) == {"sslmode": ["require"]}
    assert (d.hostname, d.path) == ("10.0.0.6", "/shop_v2")
