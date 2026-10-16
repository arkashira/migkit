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


class Tables(Lists):
    """`Lists` as one database of one keyed table, as `check` reads it."""

    def databases(self):
        return ["d"]

    def neutral_tables(self, side, db):
        return ["t"]

    def neutral_columns(self, side, db, table):
        return COLUMNS

    def neutral_key(self, side, db, table):
        return ["id"]

    def table_facts(self, side, db):
        return {"t": {"rows": len(self.sides["src"]), "key": True}}


def _hop(tmp_path, engine="postgres"):
    from migkit.config import Endpoint, Hop
    hop = Hop(name="sampled", engine=engine,
              source=Endpoint(host="10.0.0.1", port=5432, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=5432, user="u",
                              password="CHANGE_ME"), databases=["d"])
    hop.report_dir = lambda d=None: tmp_path
    return hop


def test_check_sample_reaches_the_own_reader_and_stays_incomplete(
        tmp_path, monkeypatch):
    """`check --sample` sizes the sample on the rate timed here and says
    "no difference in the sample" as incomplete; a difference inside it
    is a difference; a table too small for a sample is left to the full
    check, said as not sampled."""
    from migkit import cli
    monkeypatch.setattr(cli, "SAMPLE_PROBE", 50)
    eng = Tables({i: f"v{i}" for i in range(5000)},
                 {i: f"v{i}" for i in range(5000)})
    said, got = cli._sampled_check(_hop(tmp_path), "", "", 0.001, eng=eng)
    assert got[0]["check"] == "sampled rows"
    assert 0 < got[0]["sampled"]["rows"] < 5000, got
    assert said.status == "incomplete" and said.why.startswith("sampled:")
    eng.sides["dst"] = {i: "changed" for i in range(5000)}
    said, got = cli._sampled_check(_hop(tmp_path), "", "", 0.001, eng=eng)
    assert got[0]["status"] == "diff" and said.status == "different"
    small = Tables({1: "a"}, {1: "a"})
    said, got = cli._sampled_check(_hop(tmp_path), "", "", 5, eng=small)
    assert got[0]["status"] == "skip" and "not sampled" in got[0]["detail"]
    assert said.status == "incomplete"


def test_the_command_line_takes_the_sample_and_ends_three(monkeypatch):
    from click.testing import CliRunner

    from migkit import cli
    seen = {}

    def sampled(hop, db, table, seconds, eng=None):
        seen.update(hop=hop, seconds=seconds)
        return verdict.decide([{"check": "sampled rows", "scope": "d.t",
                                "status": "ok", "detail": "x",
                                "sampled": {"rows": 3, "of": 9}}]), []
    monkeypatch.setattr(cli, "get_hop", lambda name: name)
    monkeypatch.setattr(cli, "_require_configured", lambda hop: None)
    monkeypatch.setattr(cli, "_sampled_check", sampled)
    got = CliRunner().invoke(cli.main, ["check", "h", "--sample", "2.5"])
    assert seen == {"hop": "h", "seconds": 2.5}
    assert got.exit_code == 3, got.output


mysql_pair = servers.fixture("mysql")


@needs_docker
@pytest.mark.xfail(strict=True, reason="the MySQL engine's keyed read by key"
                   " renders its declared type ('int') where a class is"
                   " expected (canon.render_value): owed by the MySQL engine")
def test_on_a_mysql_pair_the_sample_misses_what_the_full_check_finds(
        mysql_pair, tmp_path):
    from migkit.config import Hop
    from migkit.engines import get_engine
    db = mysql_pair.name
    for side in (mysql_pair.src, mysql_pair.dst):
        side.sql("drop table if exists t", db=db)
        side.sql("create table t (id int primary key, v text)", db=db)
        side.sql("insert into t select n, concat('v', n) from (select"
                 " a.i * 100 + b.i * 10 + c.i + 1 n from"
                 " (select 0 i union select 1 union select 2 union select 3"
                 " union select 4) a, (select 0 i union select 1 union"
                 " select 2 union select 3 union select 4 union select 5"
                 " union select 6 union select 7 union select 8 union"
                 " select 9) b, (select 0 i union select 1 union select 2"
                 " union select 3 union select 4 union select 5 union"
                 " select 6 union select 7 union select 8 union select 9)"
                 " c) g", db=db)
    hop = Hop(name="sampled", engine="mysql",
              source=mysql_pair.src.endpoint(),
              target=mysql_pair.dst.endpoint(), databases=[db])
    hop.report_dir = lambda d=None: tmp_path
    eng = get_engine(hop)
    cols = [(n, c) for n, c in eng.neutral_columns("src", db, "t")]
    key = eng.neutral_key("src", db, "t")
    same = ranges.sampled(eng, db, "t", cols, key, 20, seed=3)
    assert same["status"] == "ok" and same["sampled"]["of"] == 500, same
    assert verdict.decide([same]).status == "incomplete"
    from migkit import cli
    said, _ = cli._sampled_check(hop, db, "t", 0.05, eng=eng)
    assert said.status == "incomplete", said.why
    mysql_pair.dst.sql("update t set v = 'changed'", db=db)
    found = ranges.sampled(eng, db, "t", cols, key, 20, seed=3)
    assert found["status"] == "diff", found
