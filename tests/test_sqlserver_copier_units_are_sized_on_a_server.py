"""SQL Server's statement copier on a real server (backlog
P0-AUTOTUNE-ALL, P0-RESILIENCE-ALL): a batch goes in parts cut at the
table's sizes, each part its own transaction replacing by key and a unit
of the attempt ledger; a part the server refuses for its size is retuned
smaller - the dimension and the new size from the cause - the parts that
committed before it are what the target is found to hold, the rest goes
again from there, and the table ends whole, every row once.
"""
import pytest

from migkit import failpoint, faults, tuning
from migkit.config import Hop
from migkit.engines.mssql import MSSQLEngine
from tests import servers
from tests.conftest import needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("move.table-copy", "mssql")]

ms = servers.fixture("sqlserver")


@needs_docker
def test_a_part_refused_for_its_size_is_retuned_and_the_table_ends_whole(
        ms, tmp_path, monkeypatch):
    db = ms.name
    ms.one.sql("create table dbo.sized (id int primary key,"
               " v nvarchar(400) not null)", db)
    eng = MSSQLEngine(Hop(name="sz", engine="mssql",
                          source=ms.one.endpoint(),
                          target=ms.one.endpoint(), databases=[db]))
    eng.hop.report_dir = lambda s="": tmp_path
    # by statements: the bulk load is a way of its own, proved elsewhere
    monkeypatch.setattr(eng, "_bulk_unusable",
                        lambda: "statements are what this test proves")
    real, kept = tuning.Retune.__init__, {}

    def small(self, work, *a, **kw):
        kw["nbytes"] = 40_000       # parts of a size this batch outgrows
        real(self, work, *a, **kw)
        kept["r"] = self
    monkeypatch.setattr(tuning.Retune, "__init__", small)
    faults.reset()
    failpoint.reset()
    monkeypatch.setenv("MIGKIT_FAILPOINT", "mssql.rows:2:fault=packet")
    cols = [("id", "integer"), ("v", "text")]
    rows = [[i, f"v{i}-" + "x" * 300] for i in range(1, 201)]
    assert eng.write_way("dst", db, "dbo.sized", cols, len(rows))[0] == \
        "statements"
    assert eng.neutral_write("dst", db, "dbo.sized", cols, rows) == 200
    monkeypatch.delenv("MIGKIT_FAILPOINT")
    got = ms.one.sql("select count(*), count(distinct id),"
                     " sum(cast(len(v) as bigint)) from dbo.sized", db)
    assert [int(x) for x in got[0]] == [
        200, 200, sum(len(r[1]) for r in rows)], got
    r = kept["r"]
    assert len(r.history) == 1, r.history
    adj = r.history[0]
    # the size halved for a packet, never the one that failed again
    assert adj.after["bytes"] < adj.before["bytes"]
    names = [n for n in eng.attempts().ledger.records
             if n.startswith("dbo.sized rows ")]
    first = int(names[0].split("..")[1])
    # the first part committed and was kept; the rest went from after it
    assert any(n.startswith(f"dbo.sized rows {first + 1}..")
               for n in names), names
    # written again by key, it converges: every row once
    assert eng.neutral_write("dst", db, "dbo.sized", cols, rows) == 200
    again = ms.one.sql("select count(*) from dbo.sized", db)
    assert int(again[0][0]) == 200
