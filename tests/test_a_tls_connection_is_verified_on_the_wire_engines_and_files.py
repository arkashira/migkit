"""What the wire engines tell the driver about TLS, and what a files engine
says of it (P0-PARITY, `security.tls`), without a server.

* TiDB (a MySQL client), CockroachDB and Aurora DSQL (a PostgreSQL client)
  pass the endpoint's own TLS options to the driver, so that a hop which
  asks for a checked certificate gets one - the same words the posture
  (`tls.posture`) reads the connection by;
* DuckDB, Parquet and a copybook's files say nothing of TLS on this
  machine's own disks, and call a store reached over HTTPS verified.
"""
import pytest

from migkit import tls
from migkit.config import Endpoint, Hop

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("security.tls", ("tidb", "cockroachdb", "dsql", "duckdb",
                            "parquet", "copybook"))]

FAR = "10.0.0.9"


def _hop(engine, **options):
    ep = Endpoint(host=FAR, port=9999, user="u", password="p",
                  options=dict(options))
    return Hop(name="t", engine=engine, source=ep, target=ep,
               databases=["d"])


def test_tidb_hands_its_certificate_options_to_the_driver(monkeypatch):
    import pymysql

    from migkit.engines.tidb import TiDBEngine
    seen = []

    class Stop(Exception):
        pass

    def connect(**kw):
        seen.append(kw)
        raise Stop
    monkeypatch.setattr(pymysql, "connect", connect)
    eng = TiDBEngine(_hop("tidb", ssl_ca="/etc/ca.pem"))
    with pytest.raises(Stop):
        eng._conn("src", retry=False)
    assert seen[0]["ssl_ca"] == "/etc/ca.pem", seen[0]
    assert seen[0]["ssl_verify_cert"] is True, seen[0]
    assert seen[0]["ssl_verify_identity"] is True, seen[0]
    # and the posture reads the same words as a verified connection
    ep = Endpoint(host=FAR, port=9999, options={"ssl_ca": "/etc/ca.pem"})
    assert tls.posture(ep, "mysql")["state"] == "verified"


@pytest.mark.parametrize("name", ["cockroachdb", "dsql"])
def test_a_postgresql_wire_engine_hands_its_sslmode_to_the_driver(
        monkeypatch, name):
    import psycopg2

    from migkit.engines.cockroachdb import CockroachEngine
    from migkit.engines.dsql import DsqlEngine
    seen = []

    class Stop(Exception):
        pass

    def connect(*a, **kw):
        seen.append(kw)
        raise Stop
    monkeypatch.setattr(psycopg2, "connect", connect)
    cls = {"cockroachdb": CockroachEngine, "dsql": DsqlEngine}[name]
    eng = cls(_hop(name, sslmode="verify-full", sslrootcert="/etc/ca.pem"))
    # DSQL signs its password; the connection is what is asked here
    monkeypatch.setattr(cls, "_password", lambda self, side, ep: "token")
    with pytest.raises(Stop):
        eng._connect("src", "d")
    assert seen[0]["sslmode"] == "verify-full", seen[0]
    assert seen[0]["sslrootcert"] == "/etc/ca.pem", seen[0]
    ep = Endpoint(host=FAR, port=9999, options={
        "sslmode": "verify-full", "sslrootcert": "/etc/ca.pem"})
    assert tls.posture(ep, "postgres")["state"] == "verified"


@pytest.mark.parametrize("name", ["duckdb", "parquet", "copybook"])
def test_files_say_nothing_of_tls_on_this_machine_and_verify_a_store(name):
    here = Endpoint(host="/data/x.file", port=0)
    assert tls.posture(here, name) is None
    store = Endpoint(host="", port=0, options={"url": "s3://bucket/x"})
    got = tls.posture(store, name)
    assert got["state"] == "verified", got
    plain = Endpoint(host="", port=0, options={
        "url": "s3://bucket/x", "endpoint_url": "http://10.0.0.5:9000"})
    assert tls.posture(plain, name)["state"] != "verified"
