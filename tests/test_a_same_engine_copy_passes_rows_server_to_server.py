"""PostgreSQL into PostgreSQL with no row passing through migkit (backlog
R20 items 1, 4 and 5): each range read by the source in COPY's binary
encoding under one snapshot every worker of the table shares, written into
the target's COPY as the bytes arrive, digested by both servers and
recorded only once the two numbers agree - chosen per table against the
pipe by what each measures on the table itself.

Measured before: every row of every range crossed this process twice as
text and was hashed here on the way (`Tally`), and each range was read in
a transaction of its own, so the ranges of a table copied from a source
being written were not of one instant.
"""
import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

ROWS = 60_000

SHAPE = ("(id bigint primary key, payload text, n numeric(12, 2), raw bytea,"
         " at timestamptz, doc jsonb, tags int[], maybe text)")


def _seed(pg_pair, rows=ROWS):
    got = psql(pg_pair["src"], f"""
        create table public.big {SHAPE};
        insert into public.big select g,
               'row ' || g || case when g % 7 = 0 then E'\\n\\ttab' else ''
               end, g / 7.0, decode(md5(g::text), 'hex'),
               timestamptz '2026-01-01 00:00:00+07' + g * interval '1 min',
               jsonb_build_object('g', g, 'odd', g % 2 = 1),
               array[g, g % 5], case when g % 3 = 0 then null
               else 'x' end
          from generate_series(1, {rows}) g;
        analyze public.big""")
    assert got.returncode == 0, got.stderr
    got = psql(pg_pair["dst"], f"create table public.big {SHAPE}")
    assert got.returncode == 0, got.stderr


Q = ("select count(*) || ':' || md5(string_agg(big::text, ',' order by id))"
     " from public.big")


def _same(pg_pair):
    a, b = psql(pg_pair["src"], Q).stdout, psql(pg_pair["dst"], Q).stdout
    assert a.strip() and ":" in a, a
    return a == b


@pytest.fixture
def engine(pg_pair, tmp_path, monkeypatch):
    from migkit import ranges
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="pass", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=4)
    hop.report_dir = lambda db=None: tmp_path
    monkeypatch.setattr(ranges, "LEAST", 10_000)
    monkeypatch.setattr(ranges, "active", ranges.Slots(4))
    return PostgresEngine(hop)


def _only(monkeypatch, *names):
    from migkit.engines.postgres import PostgresEngine
    monkeypatch.setattr(PostgresEngine, "COPY_WAYS", tuple(
        r for r in PostgresEngine.COPY_WAYS if r.name in names))


def _move(engine, tmp_path, said=None):
    from migkit.cli import _Checkpoint
    ck = _Checkpoint(tmp_path / "move.json")
    engine.move_table("postgres", "public", "big", 500_000, ck,
                      (said if said is not None else []).append)
    return _Checkpoint(tmp_path / "move.json")["public.big"]


def test_every_range_passes_with_no_row_through_migkit(
        engine, pg_pair, tmp_path, monkeypatch):
    """Nothing here reads a row: the pipe's tally and a COPY that writes
    row by row are both made to fail, and the copy is whole anyway."""
    from psycopg import Copy

    from migkit.tally import Tally
    _only(monkeypatch, "pass-through")
    _seed(pg_pair)

    def no(*a, **k):
        raise AssertionError("a row passed through migkit")
    monkeypatch.setattr(Tally, "row", no)
    monkeypatch.setattr(Copy, "write_row", no)
    monkeypatch.setattr(Copy, "read_row", no)
    said = []
    st = _move(engine, tmp_path, said)
    assert st["done"] and len(st["ranges"]) >= 4, st
    assert sorted(st["ranges_done"]) == sorted(a for a, _ in st["ranges"])
    assert _same(pg_pair)
    assert any("passed server to server in COPY's binary encoding" in m
               for m in said), said
    # every range read under the one snapshot the table shares
    marks = set(st["ranges_seen"].values())
    assert len(marks) == 1 and None not in marks, marks


def test_the_ranges_are_one_instant_while_the_source_is_written(
        engine, pg_pair, tmp_path, monkeypatch):
    """A row changed on the source after the copy began is copied as it
    was when the table's snapshot was taken, in a range read later - and
    the range's check, reading the source under that same snapshot, does
    not call it different."""
    from migkit.engines import pg_pass
    _only(monkeypatch, "pass-through")
    _seed(pg_pair)
    real, done = pg_pass._Export.__init__, []

    def exported(self, *a, **k):
        real(self, *a, **k)
        if not done:
            # written once the instant is taken, before any range is read
            done.append(self.id)
            got = psql(pg_pair["src"], "update public.big set payload ="
                                       " 'changed later' where id > 55000")
            assert got.returncode == 0, got.stderr
    monkeypatch.setattr(pg_pass._Export, "__init__", exported)
    said = []
    st = _move(engine, tmp_path, said)
    assert st["done"] and len(done) == 1, done
    later = ("select count(*) from public.big where id > 55000 and payload"
             " = 'changed later'")
    assert psql(pg_pair["src"], later).stdout.strip() == "5000"
    # the target holds the instant the copy read, not what came after
    assert psql(pg_pair["dst"], later).stdout.strip() == "0"
    assert not _same(pg_pair)
    assert not any("copying it again" in m for m in said), said


def test_a_table_in_one_range_is_loaded_frozen(engine, pg_pair, tmp_path,
                                               monkeypatch):
    """The table's one range, emptied whole in the transaction that loads
    it, goes in `FREEZE`: its pages are marked all-frozen as they are
    written. A table in several ranges is not - each range deletes its own
    rows and TRUNCATE would take the others'."""
    _only(monkeypatch, "pass-through")
    frozen = ("select all_frozen from pg_visibility_map_summary("
              "'public.big')")
    psql(pg_pair["dst"], "create extension if not exists pg_visibility")
    _seed(pg_pair, rows=8_000)
    psql(pg_pair["dst"], "insert into public.big (id) values (-5)")
    st = _move(engine, tmp_path)
    assert len(st["ranges"]) == 1 and st["done"], st
    assert int(psql(pg_pair["dst"], frozen).stdout) > 0
    assert _same(pg_pair)
    psql(pg_pair["src"], "drop table public.big")
    psql(pg_pair["dst"], "drop table public.big")
    (tmp_path / "move.json").unlink()
    _seed(pg_pair)
    st = _move(engine, tmp_path)
    assert len(st["ranges"]) > 1 and st["done"], st
    assert int(psql(pg_pair["dst"], frozen).stdout) == 0
    assert _same(pg_pair)


def test_each_way_is_timed_on_the_table_and_the_cheaper_kept(
        engine, pg_pair, tmp_path, monkeypatch):
    from migkit import ranges
    from migkit.decide import Trials
    monkeypatch.setattr(ranges, "active", ranges.Slots(1))
    # one worker would halve the table; six ranges of it
    monkeypatch.setattr(ranges, "step", lambda rows, chunk, workers: 10_000)
    _seed(pg_pair)
    picks, trials = [], []
    real = Trials.pick

    def pick(self):
        got = real(self)
        picks.append(got.path)
        if self not in trials:
            trials.append(self)
        return got
    monkeypatch.setattr(Trials, "pick", pick)
    st = _move(engine, tmp_path)
    assert st["done"] and _same(pg_pair)
    # each way on a range of its own, in the list's order, while the one
    # before it is still being timed
    assert picks[:3] == ["pass-through", "pass-through unawaited", "pipe"]
    assert len(trials) == 1 and len(picks) == len(st["ranges"]), picks
    assert set(trials[0].costs) == {"pass-through", "pass-through unawaited",
                                    "pipe"}, trials[0].costs


def test_a_column_of_another_type_on_the_target_goes_through_the_pipe(
        engine, pg_pair, tmp_path):
    """COPY's binary encoding names no type: an int read as the target's
    bigint is refused, or worse. The pass-through is passed over with the
    column named, and the pipe carries the table as before."""
    got = psql(pg_pair["src"], "create table public.big (id bigint primary"
                               " key, n int); insert into public.big select"
                               " g, g * 3 from generate_series(1, 30000) g")
    assert got.returncode == 0, got.stderr
    psql(pg_pair["dst"], "create table public.big (id bigint primary key,"
                         " n bigint)")
    said = []
    st = _move(engine, tmp_path, said)
    assert st["done"]
    assert any("n is integer on the source and bigint on the target" in m
               for m in said), said
    q = "select count(*), sum(n) from public.big"
    assert psql(pg_pair["src"], q).stdout == psql(pg_pair["dst"], q).stdout


def test_the_ways_of_a_whole_table_are_timed_against_each_other(
        engine, pg_pair, tmp_path, monkeypatch):
    """Tables large enough are each copied a way not yet timed, in the
    list's order - the table copier's ranges, then the streaming copy of
    the whole table under the table's snapshot - each proved by both
    servers' digests; then each goes the way cheaper a byte."""
    from migkit.cli import _Checkpoint
    from migkit.engines.postgres import PostgresEngine
    monkeypatch.setattr(PostgresEngine, "STREAM_FROM", 1)
    _seed(pg_pair)
    for t in ("big2", "big3"):
        got = psql(pg_pair["src"], f"create table public.{t} {SHAPE};"
                                   f" insert into public.{t} select * from"
                                   f" public.big; analyze public.{t}")
        assert got.returncode == 0, got.stderr
        psql(pg_pair["dst"], f"create table public.{t} {SHAPE}")
    said = []
    for t in ("big", "big2", "big3"):
        engine.move_table("postgres", "public", t, 500_000,
                          _Checkpoint(tmp_path / "move.json"), said.append)
        q = Q.replace("public.big", f"public.{t}").replace("big::text",
                                                           f"{t}::text")
        a, b = psql(pg_pair["src"], q).stdout, psql(pg_pair["dst"], q).stdout
        assert a.startswith(f"{ROWS}:") and a == b, (t, a, b)
    # the engine's own chooser's costs, which it reports
    costs = engine._way_cost[engine._work_key(engine.TABLE_WORK)]
    assert any(w["work"] == engine.TABLE_WORK
               for w in engine.measured_ways())
    assert "ranges" in costs, costs
    # the streaming copy proved itself and was timed, or said why not
    assert "streaming" in costs or any(
        "did not finish" in m or "other rows" in m for m in said), said
    last = [m for m in said if m.startswith("public.big3: ")][0]
    assert "measured the cheapest" in last or "did not" in last, last
