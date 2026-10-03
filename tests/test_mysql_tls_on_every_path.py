"""The hop's TLS reaches every way the MySQL engine connects to a side
(audit 2026-10-01, SEC2, SC17).

`doctor` said a side's certificate was verified while the change
stream's reader, the schema's dump, the schema comparison's address, the
second data reading's, the native replica, the row sync and the
pass-through's client all connected without it - the replica fetching
the source's public key unauthenticated besides. Each is now rendered
from `Endpoint.tls_forms`, the one place the TLS decision is made.
"""
import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.mysql import MySQLEngine
from tests import servers
from tests.conftest import needs_docker

one = servers.fixture("mysql-tls")


@pytest.fixture
def ca(tmp_path):
    got = tmp_path / "ca.pem"
    got.write_text("the authority's file")
    return str(got)


def _engine(src=None, dst=None, host="db.example.com"):
    return MySQLEngine(Hop(
        name="t", engine="mysql",
        source=Endpoint(host=host, port=3306, user="app", password="pw",
                        options=src or {}),
        target=Endpoint(host="127.0.0.1", port=3307, user="app",
                        password="pw", options=dst or {}),
        databases=["shop"]))


def test_the_clients_carry_the_authority(ca):
    eng = _engine({"ssl_ca": ca}, {"ssl": True})
    src = eng._client_argv("mysqldump", "src")
    assert f"--ssl-ca={ca}" in src and "--ssl-mode=VERIFY_IDENTITY" in src
    assert "--ssl-mode=REQUIRED" in eng._client_argv("mysql", "dst")
    assert "pw" not in src and eng._client_env("src") == {"MYSQL_PWD": "pw"}


def test_the_schema_comparisons_address_carries_it_as_its_driver_reads_it(
        ca):
    eng = _engine({"ssl_ca": ca}, {"ssl": True})
    src, dst = eng._schema_urls("shop")
    assert src.startswith("mysql://app:pw@db.example.com:3306/shop?")
    # the files its driver reads and checks the server by; never MySQL's
    # ssl-mode, which it would take for a session variable
    assert "ssl-ca=" in src and "ssl-mode" not in src, src
    assert dst.endswith("?tls=skip-verify"), dst
    # nothing asked: TLS where the server offers it
    plain = _engine(host="127.0.0.1")._schema_urls("shop")[0]
    assert plain.endswith("?tls=preferred"), plain


def test_the_second_readings_address_carries_it_or_is_not_made(ca):
    eng = _engine({"ssl_ca": ca}, {"ssl": True})
    src = eng._reladiff_url("src", "shop")
    assert "ssl_ca=" in src and "ssl_verify_cert=1" in src, src
    assert "False" not in src
    # encryption with no authority: its driver cannot be told to require
    # it, so the reading is not run rather than run in the clear
    assert eng._reladiff_url("dst", "shop") is None
    assert eng._reladiff_table("shop", "t", ["id"]) == (None, "", 0, 0)


def test_the_change_streams_reader_signs_in_with_it(ca):
    got = _engine({"ssl_ca": ca})._binlog_settings("src")
    assert got["ssl_ca"] == ca and got["ssl_verify_cert"] is True
    assert got["passwd"] == "pw"


def test_the_native_replica_asks_the_source_for_it(ca, monkeypatch):
    eng = _engine({"ssl_ca": ca})
    monkeypatch.setattr(eng, "_binlog_position", lambda side: ("b.1", 4))
    monkeypatch.setattr(eng, "_gtid_state", lambda brand: (True, "gtid ON"))
    monkeypatch.setattr(eng, "_replica_filter_sql", lambda b: ([], []))
    monkeypatch.setattr(eng, "_parallel_apply_sql", lambda b: ([], None))
    monkeypatch.setattr(eng, "_replica_procedure", lambda side="dst": None)
    for brand, want, never in (("mysql", "SOURCE_SSL = 1",
                                "GET_SOURCE_PUBLIC_KEY"),
                               ("mariadb", "MASTER_SSL = 1",
                                "GET_SOURCE_PUBLIC_KEY")):
        monkeypatch.setattr(eng, "_brands", lambda b=brand: (
            type("B", (), {"name": b})(), type("B", (), {"name": b})()))
        plan = eng.replicate_sql("shop", secret="s3cret")
        said = " ".join(plan["dst"])
        assert want in said and never not in said, said
    # without TLS the key is fetched, as before
    plain = _engine(host="127.0.0.1")
    for name in ("_gtid_state", "_binlog_position", "_replica_filter_sql",
                 "_parallel_apply_sql", "_replica_procedure"):
        monkeypatch.setattr(plain, name, getattr(eng, name))
    monkeypatch.setattr(plain, "_brands", lambda: (
        type("B", (), {"name": "mysql"})(),) * 2)
    said = " ".join(plain.replicate_sql("shop")["dst"])
    assert "GET_SOURCE_PUBLIC_KEY = 1" in said and "SOURCE_SSL" not in said


def test_a_managed_replica_is_told_the_source_is_encrypted(ca, monkeypatch):
    eng = _engine({"ssl_ca": ca})
    for name, value in (("_binlog_position", lambda side: ("b.1", 4)),
                        ("_gtid_state", lambda brand: (True, "")),
                        ("_replica_filter_sql", lambda b: ([], [])),
                        ("_parallel_apply_sql", lambda b: ([], None)),
                        ("_replica_procedure",
                         lambda side="dst": "mysql.rds_set_external_source"),
                        ("_brands", lambda: (
                            type("B", (), {"name": "mysql"})(),) * 2)):
        monkeypatch.setattr(eng, name, value)
    call = eng.replicate_sql("shop", secret="s3cret")["dst"][0]
    assert call.startswith("call mysql.rds_set_external_source"
                           " ('db.example.com', 3306,")
    assert call.endswith(", 1);"), call


def test_the_row_syncs_defaults_file_carries_it(ca):
    from migkit.movers import client_defaults
    ep = Endpoint(host="db.example.com", port=3306, user="app",
                  password="pw", options={"ssl_ca": ca})
    with client_defaults(ep) as (path,):
        text = open(path).read()
    assert "password=pw" in text and f"ssl-ca={ca}" in text
    assert "ssl-mode=VERIFY_IDENTITY" in text


def test_the_pass_throughs_client_signs_in_with_it(ca, monkeypatch):
    from migkit import runner
    from migkit.engines import my_pass
    eng = _engine({"ssl_ca": ca})
    seen = {}

    def start(argv, env=None, **kw):
        seen.update(argv=argv, env=env, kw=kw)
        return "proc"
    monkeypatch.setattr(runner, "start", start)
    copy = my_pass.TableCopy.__new__(my_pass.TableCopy)
    copy.eng, copy.db, copy.table = eng, "shop", "or`ders"
    copy.columns = [("id", "plain")]
    copy.digest_cols = ([("id", "integer")], [("id", "integer")], [])
    assert copy._source("`id` > 0", "id") == "proc"
    assert f"--ssl-ca={ca}" in seen["argv"] and "pw" not in seen["argv"]
    assert seen["env"] == {"MYSQL_PWD": "pw"}
    script = seen["argv"][seen["argv"].index("-e") + 1]
    assert "set session time_zone = '+00:00'" in script
    assert "`shop`.`or``ders`" in script, script


@needs_docker
def test_a_server_that_takes_only_tls_is_read_every_way(one, tmp_path):
    """`require_secure_transport` on: the driver's connection, the change
    stream's reader and the schema's dump all sign in."""
    from migkit.util import which
    one.one.sql("set global require_secure_transport = ON")
    ep = one.one.endpoint(ssl_ca=one.one.ca)
    hop = Hop(name="t", engine="mysql", source=ep, target=ep,
              databases=[one.name])
    hop.report_dir = lambda d=None: tmp_path
    eng = MySQLEngine(hop)
    assert eng.leg_encryption("src", one.name)["encrypted"] is True
    first = eng._q("src", "show binary logs")[0][0]
    assert eng._binlog_started("src", first)
    if which("mysqldump"):
        one.one.sql("create table t (id int primary key)", one.name)
        assert "CREATE TABLE `t`" in eng._dump_schema("src", one.name)
    eng.release_kept()
