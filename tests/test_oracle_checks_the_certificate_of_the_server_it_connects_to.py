"""An Oracle connection is made over TLS when the endpoint asks for it, the
server's certificate checked, and what `assess` says of it is what the
connection is (P0-PARITY, `security.tls`), without a server.

The driver's call is not proved here (`capabilities.UNPROVED`): no TCPS
listener has answered it. What is held is migkit's own - what the options
turn into, that nothing is checked only where `tls_insecure` says so, that
a connection made plain is called plain, and that the pool is not shared
between a plain endpoint and a TLS one.
"""
import ssl
import sys
import types

import pytest

from migkit import tls
from migkit.config import Endpoint, Hop
from migkit.engines import oracle
from migkit.engines.oracle import OracleEngine

FAR = "db.example.com"


def _eng(**options):
    ep = Endpoint(host=FAR, port=2484, user="app", password="CHANGE_ME",
                  options={"service": "FREEPDB1", **options})
    return OracleEngine(Hop(name="t", engine="oracle", source=ep, target=ep,
                            databases=["app"]))


@pytest.fixture
def driver(monkeypatch):
    """A stand-in for the driver that keeps what it is called with."""
    calls = []

    class Defaults:
        fetch_lobs = fetch_decimals = None

    mod = types.SimpleNamespace(
        defaults=Defaults(), POOL_GETMODE_WAIT=1,
        create_pool=lambda **kw: calls.append(("pool", kw)) or _Pool(),
        connect=lambda **kw: calls.append(("connect", kw)) or _Conn())
    monkeypatch.setitem(sys.modules, "oracledb", mod)
    monkeypatch.setattr(oracle, "_session", lambda conn: None)
    monkeypatch.setattr(oracle, "_POOLS", {})
    return calls


class _Pool:
    def acquire(self):
        return _Conn()


class _Conn:
    def close(self):
        pass


def test_an_endpoint_that_asks_for_nothing_connects_as_it_did(driver):
    eng = _eng()
    assert eng._tls(eng._endpoint("src")) == {}
    eng._pool("src")
    kw = driver[0][1]
    assert "protocol" not in kw and "ssl_context" not in kw


def test_tls_is_the_tcps_port_with_the_certificate_and_name_checked(driver):
    eng = _eng(tls=True)
    got = eng._tls(eng._endpoint("src"))
    assert got["protocol"] == "tcps" and got["ssl_server_dn_match"] is True
    ctx = got["ssl_context"]
    assert ctx.verify_mode == ssl.CERT_REQUIRED and ctx.check_hostname
    eng._pool("src")
    eng._dedicated("src")
    assert [(k, kw["protocol"]) for k, kw in driver] == [
        ("pool", "tcps"), ("connect", "tcps")]


def test_a_file_of_authorities_is_what_the_certificate_is_checked_against(
        driver, tmp_path, monkeypatch):
    named = []
    real = ssl.create_default_context
    monkeypatch.setattr(ssl, "create_default_context",
                        lambda cafile=None, **kw: named.append(cafile)
                        or real())
    eng = _eng(tls_ca_file=str(tmp_path / "ca.pem"))
    assert eng._tls(eng._endpoint("src"))["protocol"] == "tcps"
    assert named == [str(tmp_path / "ca.pem")]


def test_nothing_is_checked_only_where_the_endpoint_says_so(driver):
    eng = _eng(tls=True, tls_insecure=True)
    got = eng._tls(eng._endpoint("src"))
    assert got["ssl_server_dn_match"] is False
    assert got["ssl_context"].verify_mode == ssl.CERT_NONE
    assert not got["ssl_context"].check_hostname


def test_a_plain_endpoint_and_a_tls_one_do_not_share_a_pool(driver):
    plain, secure = _eng(), _eng(tls=True)
    plain._pool("src")
    secure._pool("src")
    assert len(driver) == 2 and "protocol" in driver[1][1]


def test_assess_calls_a_connection_what_it_is(monkeypatch):
    monkeypatch.setattr(tls, "on_this_machine", lambda ep: False)
    monkeypatch.setattr(tls, "tunnel_kind", lambda ep: None)

    def state(**options):
        ep = Endpoint(host=FAR, port=2484, options=options)
        return tls.posture(ep, "oracle")["state"]
    assert state() == "plain"
    assert state(tls=True) == "verified"
    assert state(tls_ca_file="/ca.pem") == "verified"
    assert state(tls=True, tls_insecure=True) == "unverified"


def test_the_hops_keys_name_oracle_for_these_options():
    from migkit import hopkeys
    for key in ("tls", "tls_ca_file", "tls_insecure"):
        assert "oracle" in hopkeys.ENDPOINT_OPTIONS[key].engines, key
