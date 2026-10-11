"""A SQL Server target whose key range runs past the source's is found by
the deep check - writes landing on the target, or applied twice
(P0-PARITY.verify, movement.target-ahead), without a server: the engine's
own `check_deep` over answers given by statement, the target's highest
integer key against the source's."""
import pytest

from migkit import workload
from migkit.config import Endpoint, Hop
from migkit.engines.mssql import MSSQLEngine

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.movement.target-ahead", "mssql")]

EP = Endpoint(host="x", port=1, user="x", password="x")


def _engine(tmp_path, src_max, dst_max, keyed=True):
    hop = Hop(name="ahead", engine="mssql", source=EP, target=EP,
              databases=["d"])
    hop.report_dir = lambda db=None: tmp_path
    eng = MSSQLEngine(hop)
    maxima = {"src": src_max, "dst": dst_max}

    def cmd(side, db, sql, *a, **k):
        if "max(" in sql:
            value = maxima[side]
            if isinstance(value, Exception):
                raise value
            return [(str(value),)]
        if "ty.name in" in sql:
            return [("dbo.t", "id")] if keyed else []
        return []
    eng._cmd = cmd
    # the text check and the workload comparison are not this test's
    eng._mojibake = lambda db: None
    return eng


@pytest.fixture(autouse=True)
def _no_workload(monkeypatch):
    monkeypatch.setattr(workload, "compare", lambda eng, db: None)


def _boundary(eng):
    return [r for r in eng.check_deep("d") if r and r.scope == "d boundary"]


def test_a_target_whose_key_runs_past_the_source_s_is_found(tmp_path):
    got = _boundary(_engine(tmp_path, 3, 100))
    assert [r.status for r in got] == ["diff"], got
    assert "AHEAD of source" in got[0].detail
    assert "dbo.t src_max=3 dst_max=100" in got[0].detail
    assert got[0].category == "movement.target-ahead"


def test_a_target_level_with_or_behind_the_source_is_not_ahead(tmp_path):
    for dst in (3, 1):
        got = _boundary(_engine(tmp_path, 3, dst))
        assert [r.status for r in got] == ["ok"], got
        assert "none ahead of source" in got[0].detail
    assert "1 behind" in _boundary(_engine(tmp_path, 3, 1))[0].detail


def test_a_table_the_server_cannot_answer_for_is_not_said_ahead(tmp_path):
    got = _boundary(_engine(tmp_path, 3, RuntimeError("denied")))
    assert [r.status for r in got] == ["ok"], got
