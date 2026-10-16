"""A sampled check by migkit's own keyed reader is said to be a sample
(DTS.sample): random rows drawn from the source's keys and read by key on
both sides, a difference inside the sample found, one outside it not -
and the verdict never "same" for it, only "no difference in the sample",
the full keyed check still the outcome. On a PostgreSQL pair of the lane
the reader is the server's own; the draw and the verdict on a reader over
lists. The size is the decision engine's, from the reader's measured
rate and the time the quick signal may take (`decide.check_sample`).
"""
import pytest

from migkit import decide, ranges, verdict
from tests import servers
from tests.conftest import needs_docker

COLUMNS = [("id", "number"), ("v", "text")]


class Lists:
    """A reader over {side: {id: v}}, paging by key as the engines do."""

    def __init__(self, src, dst):
        self.sides = {"src": src, "dst": dst}

    def neutral_read(self, side, db, table, columns, after=None, limit=1000):
        keys = sorted(k for k in self.sides[side]
                      if after is None or k > after[0])[:limit]
        rows = [(k,) for k in keys]
        return rows, (rows[-1] if rows else None)

    def neutral_rows_by_key(self, side, db, table, columns, key, keys):
        got = self.sides[side]
        return {str(k[0]): (k[0], got[k[0]]) for k in keys if k[0] in got}


def _pair(n=200, changed=()):
    src = {i: f"v{i}" for i in range(n)}
    dst = dict(src)
    for i in changed:
        dst[i] = "changed"
    return Lists(src, dst)


def test_a_difference_in_the_sample_is_found():
    eng = _pair(changed=range(200))
    got = ranges.sampled(eng, "d", "t", COLUMNS, ["id"], 10, seed=1)
    assert got["status"] == "diff" and got["sampled"]["rows"] == 10
    assert verdict.decide([got]).status == "different"


def test_no_difference_in_the_sample_is_never_same():
    eng = _pair()
    got = ranges.sampled(eng, "d", "t", COLUMNS, ["id"], 10, seed=1)
    said = verdict.decide([got])
    assert got["status"] == "ok" and got["sampled"]["of"] == 200
    assert said.status == "incomplete"
    assert said.why.startswith("sampled: no difference in the sample of 10")


def test_a_row_outside_the_sample_is_not_found_by_it():
    seed = 7
    drawn = ranges.sampled(_pair(), "d", "t", COLUMNS, ["id"], 5, seed=seed)
    assert drawn["status"] == "ok"
    eng = _pair()
    # the draw again, then a row it did not draw changed on the target
    import random
    rnd, keep, seen = random.Random(seed), [], 0
    for k in range(200):
        seen += 1
        if len(keep) < 5:
            keep.append(k)
        elif (j := rnd.randrange(seen)) < 5:
            keep[j] = k
    outside = next(k for k in range(200) if k not in keep)
    eng.sides["dst"][outside] = "changed"
    again = ranges.sampled(eng, "d", "t", COLUMNS, ["id"], 5, seed=seed)
    assert again["status"] == "ok"
    assert verdict.decide([again]).status == "incomplete"


def test_the_size_is_the_measured_readers_and_never_the_table():
    got = decide.check_sample({"asked": True, "rows": 1_000_000,
                               "rate": 5000.0, "seconds": 2})
    assert got.path == "sample" and got.rung.said.startswith("10,000 rows")
    whole = decide.check_sample({"asked": True, "rows": 100,
                                 "rate": 5000.0, "seconds": 2})
    assert whole.path == "full"
    unmeasured = decide.check_sample({"asked": True, "rows": 10 ** 6})
    assert unmeasured.path == "full"
    assert "not measured" in " ".join(w for _, w in unmeasured.passed)
    assert decide.check_sample({"rows": 10 ** 6, "rate": 1.0,
                                "seconds": 9}).path == "full"


pair = servers.fixture("pg")


@needs_docker
def test_on_a_postgres_pair_the_sample_misses_what_the_full_check_finds(
        pair, tmp_path):
    from migkit.config import Hop
    from migkit.engines.postgres import PostgresEngine
    db = pair.name
    for side in (pair.src, pair.dst):
        side.sql("drop table if exists public.t", db=db)
        side.sql("create table public.t (id int primary key, v text)", db=db)
        side.sql("insert into public.t select g, 'v' || g from"
                 " generate_series(1, 500) g", db=db)
    hop = Hop(name="sampled", engine="postgres", source=pair.src.endpoint(),
              target=pair.dst.endpoint(), databases=[db])
    hop.report_dir = lambda d=None: tmp_path
    eng = PostgresEngine(hop)
    cols = [(n, c) for n, c in eng.neutral_columns("src", db, "t")]
    key = eng.neutral_key("src", db, "t")
    same = ranges.sampled(eng, db, "t", cols, key, 20, seed=3)
    assert same["status"] == "ok", same
    assert verdict.decide([same]).status == "incomplete"
    pair.dst.sql("update public.t set v = 'changed'", db=db)
    found = ranges.sampled(eng, db, "t", cols, key, 20, seed=3)
    assert found["status"] == "diff", found
