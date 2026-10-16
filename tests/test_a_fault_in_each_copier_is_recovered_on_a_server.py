"""A fault injected mid-copy into the cross-engine copier, on real
servers, recovered inside the run (backlog P0-RESILIENCE-ALL,
P0-AUTOTUNE-ALL): MySQL into PostgreSQL through the table copier, the
failure raised on purpose (`failpoint`) as the driver would raise it.

* a read whose link dropped is read again from the same key, the read an
  owned operation of the run's ledger with the path chosen and its facts;
* a write lost after it was sent is counted on the target first
  (`faults.landed`) - found whole there, it is not written again;
* a size the server refused is retuned to a smaller unit, the sizes in the
  retune's record strictly decreasing, never the one that failed again;
* every try is bounded by what the range's forecast pays for, kept beside
  the forecast it came from - not a constant;
and in each case the target holds every row once, the digest of both
servers equal.
"""
import os

import pytest

from tests import mysql_postgres_pair as xp

pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not xp.docker(),
                                 reason="docker not available")]


@pytest.fixture(scope="module")
def pair():
    xp.up()
    yield
    xp.down()


@pytest.fixture
def eng(pair, tmp_path, monkeypatch):
    from migkit import crossmove, failpoint, faults, ranges, tuning
    from migkit.engines.hetero import HeteroEngine
    xp.fresh()
    monkeypatch.setattr(ranges, "LEAST", 2)
    ranges.active = ranges.Slots(1)
    os.environ.pop("MIGKIT_TABLE_COPY", None)
    faults.reset()
    failpoint.reset()
    xp.ok(xp.my_sql("create table t (id bigint primary key, v"
                    " varchar(200)); insert into t with recursive s(n) as"
                    " (select 1 union all select n + 1 from s where n < 60)"
                    " select n, repeat(concat('v', n), 20) from s;"
                    " analyze table t"))
    e = HeteroEngine(xp.hop(tmp_path))
    e.create_missing("cx", lambda m: None)
    kept = {}
    real = tuning.Retune.__init__

    def keep(self, *a, **kw):
        real(self, *a, **kw)
        kept["retune"] = self
    monkeypatch.setattr(tuning.Retune, "__init__", keep)
    e.kept = kept
    yield e, monkeypatch, crossmove
    # the kept connections let go, so the next test's fresh database drops
    for inner in (e.src_engine, e.dst_engine):
        getattr(inner, "release_kept", lambda: None)()
    os.environ.pop("MIGKIT_FAILPOINT", None)
    faults.reset()


def _equal(eng):
    my = xp.ok(xp.my_sql("select count(*), count(distinct id),"
                         " sum(length(v)) from t")).split()
    # the PostgreSQL client says a row's columns apart with a bar
    pg = xp.ok(xp.pg_sql("select count(*), count(distinct id),"
                         " sum(length(v)) from t")).replace("|", " ").split()
    assert my == pg and my[0] == "60", (my, pg)


def _ops(eng, operation):
    return {k: v for k, v in eng.attempts().ledger.operations.items()
            if v.get("operation") == operation}


def test_a_read_lost_mid_copy_is_read_again_and_kept(eng):
    e, mp, _ = eng
    mp.setenv("MIGKIT_FAILPOINT", "hetero.read:2:fault=connectivity")
    moved, way, fell = e.copy_range("cx", "t", "public.t", 0, 60, "copier")
    assert moved == 60 and fell is None
    _equal(e)
    retried = [v for v in _ops(e, "read range").values()
               if v.get("decided")]
    assert len(retried) == 1 and retried[0]["state"] == "done"
    d = retried[0]["decided"][0]
    assert d["path"] == "retry" and d["class"] == "connectivity"
    assert retried[0]["tries"] == 2


def test_a_write_lost_after_it_was_sent_is_counted_not_written_again(eng):
    e, mp, _ = eng
    mp.setenv("MIGKIT_FAILPOINT", "batch.written:1:fault=uncertain_commit")
    moved, _, fell = e.copy_range("cx", "t", "public.t", 0, 60, "copier")
    assert moved == 60 and fell is None
    _equal(e)
    w = [v for v in _ops(e, "copy batch").values() if v.get("decided")]
    assert len(w) == 1
    # reconciled by the target's count before anything ran again
    assert w[0]["decided"][0]["path"] == "reconcile"
    assert w[0]["reconciled"] == "committed" and w[0]["tries"] == 1


def test_a_refused_size_is_retuned_smaller_and_the_digest_holds(eng):
    e, mp, _ = eng
    mp.setenv("MIGKIT_FAILPOINT",
              "batch.written:1:fault=packet,batch.written:2:fault=packet")
    moved, _, fell = e.copy_range("cx", "t", "public.t", 0, 60, "copier")
    assert moved == 60 and fell is None
    _equal(e)
    r = e.kept["retune"]
    sizes = [r.history[0].before["bytes"]] + [a.after["bytes"]
                                              for a in r.history]
    assert len(r.history) == 2
    assert all(b < a for a, b in zip(sizes, sizes[1:])), sizes
    # every try bounded by the range's forecast, kept with it
    assert r.bounds and all(b["tries"] >= 2 for b in r.bounds)
    measured = [b for b in r.bounds if b["rate"]]
    assert measured and all(
        b["tries"] == max(2, 1 + -(-b["eta"] // b["took"]))
        for b in measured)
