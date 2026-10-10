"""MySQL's assess reads both servers' versions and holds them to each
other (P0-PARITY.assess)."""
from tests.conftest import needs_docker
from tests.mysql_pair import engine, my_pair  # noqa: F401 - a fixture

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("assess.versions", "mysql")]


def _row(eng):
    (row,) = [r for r in eng.assess() if r["item"] == "server version match"]
    return row


@needs_docker
def test_the_same_versions_pass_and_different_ones_are_said(my_pair):
    eng = engine("postgres")
    row = _row(eng)
    assert row["level"] == "pass", row
    assert row["detail"].count("8.4") == 2, row
    real = eng._q

    def newer_source(side, sql, args=None, **kw):
        if side == "dst" and sql.strip() == "select version()":
            return [("8.0.36",)]
        return real(side, sql, args, **kw)
    eng._q = newer_source
    row = _row(eng)
    assert row["level"] == "warn" and "8.0.36" in row["detail"], row
