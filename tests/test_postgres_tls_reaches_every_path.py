"""The hop's TLS reaches every PostgreSQL path, read off the server (audit
2026-10-01, SEC2): the questions' kept connection, the client program, the
table copier's pass-through and the rows' own connections each ask the
server whether they came encrypted - and a hop that turns TLS off is seen
unencrypted on the same paths, so each answer can say no. Verified is said
only of a connection that checked the certificate.
"""
import pytest

from tests import servers
from tests.conftest import needs_docker

pytestmark = needs_docker

tls = servers.fixture("pg-tls")
ASK = "select ssl from pg_stat_ssl where pid = pg_backend_pid()"


def _eng(tls, tmp_path, **options):
    from migkit.config import Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="tls", engine="postgres",
              source=tls.one.endpoint(**options),
              target=tls.one.endpoint(**options), databases=[tls.name])
    hop.report_dir = lambda d=None: tmp_path
    return PostgresEngine(hop)


@pytest.mark.parametrize("sslmode,encrypted", [("require", "t"),
                                               ("disable", "f")])
def test_every_path_comes_as_the_hop_asks(tls, tmp_path, sslmode,
                                          encrypted):
    from migkit.engines import pg_pass
    from migkit.util import run
    db = tls.name
    eng = _eng(tls, tmp_path, sslmode=sslmode)
    try:
        assert eng._psql("src", db, ASK) == encrypted
        assert eng._psql_program("src", db, ASK) == encrypted
        argv, env = eng._client("dst", db, "psql", "-X", "-At", "-c", ASK)
        assert run(argv, env=env).stdout.strip() == encrypted
        conn = pg_pass.connect(eng, "dst", db, pg_pass.LOAD_OPTIONS)
        try:
            got = conn.execute(ASK).fetchone()[0]
        finally:
            conn.close()
        assert got is (encrypted == "t")
        for purpose in ("read", "write", "probe"):
            conn = eng._connect("src", db, purpose)
            try:
                cur = conn.cursor()
                cur.execute(ASK)
                assert cur.fetchone()[0] is (encrypted == "t"), purpose
            finally:
                conn.close()
        said = eng.leg_encryption("src", db)
        assert said["encrypted"] is (encrypted == "t")
        # `require` encrypts and checks no certificate: not verified
        assert said["verified"] is False
    finally:
        eng.release_kept()


def test_verified_is_said_of_a_connection_that_checked(tls, tmp_path,
                                                      monkeypatch):
    monkeypatch.delenv("MIGKIT_CDC", raising=False)
    eng = _eng(tls, tmp_path, sslmode="verify-full", sslrootcert=tls.one.ca)
    try:
        assert eng._psql("src", tls.name, ASK) == "t"
        said = eng.leg_encryption("src", tls.name)
        assert said["encrypted"] is True and said["verified"] is True
        # the servers' own replication dials the source from the target,
        # which cannot read the authority's file on this machine
        assert said["unverified"] == ["the servers' own replication"]
    finally:
        eng.release_kept()
