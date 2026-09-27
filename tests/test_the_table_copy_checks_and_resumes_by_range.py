"""The PostgreSQL table copier: a large table in ranges of equal rows,
side by side, each range read back from the target once it is committed,
and a finished table asked again before it is skipped.

Measured before: the copier went one range at a time with nothing read
back - a move was as long as its largest table, a target that changed
what it was given was found only by a `check` run afterwards, and a table
an earlier run had finished was skipped whatever the source had done
since ("done earlier, skip").
"""
import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

ROWS = 60_000


def _seed(port, rows=ROWS):
    got = psql(port, f"""
        create table public.big (id bigint primary key, payload text,
                                 n numeric(12, 2), raw bytea);
        insert into public.big select g, 'row-' || g, g / 7.0,
               decode(md5(g::text), 'hex') from generate_series(1, {rows}) g;
        analyze public.big""")
    assert got.returncode == 0, got.stderr


def _target_table(port):
    got = psql(port, "create table public.big (id bigint primary key,"
                     " payload text, n numeric(12, 2), raw bytea)")
    assert got.returncode == 0, got.stderr


def _same(pg_pair):
    q = ("select count(*), md5(string_agg(id || payload || n || raw::text,"
         " ',' order by id)) from public.big")
    return psql(pg_pair["src"], q).stdout == psql(pg_pair["dst"], q).stdout


@pytest.fixture
def engine(pg_pair, tmp_path, monkeypatch):
    from migkit import ranges
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="rng", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=4)
    hop.report_dir = lambda db=None: tmp_path
    # ranges small enough that this table is split
    monkeypatch.setattr(ranges, "LEAST", 10_000)
    monkeypatch.setattr(ranges, "active", ranges.Slots(4))
    return PostgresEngine(hop)


def _move(engine, ck, said):
    engine.move_table("postgres", "public", "big", 500_000, ck, said.append)


def test_a_large_table_goes_in_ranges_side_by_side(engine, pg_pair,
                                                   tmp_path):
    from migkit.cli import _Checkpoint
    _seed(pg_pair["src"])
    _target_table(pg_pair["dst"])
    ck, said = _Checkpoint(tmp_path / "move.json"), []
    _move(engine, ck, said)
    st = ck["public.big"]
    assert len(st["ranges"]) >= 4 and st["done"], st
    assert sorted(st["ranges_done"]) == sorted(a for a, _ in st["ranges"])
    # equal rows a range, not equal spans of the key
    assert _same(pg_pair)
    assert st["last"] == ROWS


def test_a_target_that_changes_rows_stops_the_copy_at_the_range(
        engine, pg_pair, tmp_path):
    from migkit.cli import _Checkpoint
    _seed(pg_pair["src"])
    _target_table(pg_pair["dst"])
    got = psql(pg_pair["dst"], """
        create function shout() returns trigger language plpgsql as $$
        begin if new.id = 45000 then new.payload := upper(new.payload);
        end if; return new; end $$;
        create trigger shout before insert on public.big for each row
        execute function shout();
        -- fires even while the copy holds the target's triggers quiet
        alter table public.big enable always trigger shout""")
    assert got.returncode == 0, got.stderr
    ck, said = _Checkpoint(tmp_path / "move.json"), []
    with pytest.raises(SystemExit) as e:
        _move(engine, ck, said)
    msg = str(e.value)
    assert msg.startswith("public.big id "), msg
    assert "copied twice, and the target reads back" in msg, msg
    rng = msg.split(":")[0].split("id ")[1]
    lo, hi = (int(x.replace(",", "")) for x in rng.split(" to "))
    assert lo <= 45000 <= hi, msg
    assert any("copying it again" in m for m in said), said
    assert not ck["public.big"].get("done")


def test_a_crash_resumes_with_the_ranges_not_done(engine, pg_pair,
                                                  tmp_path, monkeypatch):
    from migkit.cli import _Checkpoint
    _seed(pg_pair["src"])
    _target_table(pg_pair["dst"])
    real = engine._copy_checked
    calls = []

    def dies_on_the_third(*a, **k):
        calls.append(a[3])
        if len(calls) == 3:
            raise RuntimeError("the connection went away")
        return real(*a, **k)
    monkeypatch.setattr(engine, "_copy_checked", dies_on_the_third)
    ck = _Checkpoint(tmp_path / "move.json")
    with pytest.raises(RuntimeError):
        _move(engine, ck, [])
    planned = len(ck["public.big"]["ranges"])
    finished = len(ck["public.big"]["ranges_done"])
    assert 0 < finished < planned
    # started again from the checkpoint on disk: only what was not done
    monkeypatch.setattr(engine, "_copy_checked", real)
    again = []
    monkeypatch.setattr(engine, "_copy_checked",
                        lambda *a, **k: again.append(a[3]) or real(*a, **k))
    _move(engine, _Checkpoint(tmp_path / "move.json"), [])
    assert len(again) == planned - finished, (again, planned, finished)
    assert _same(pg_pair)


def test_a_finished_table_is_asked_again_before_it_is_skipped(
        engine, pg_pair, tmp_path, monkeypatch):
    from migkit.cli import _Checkpoint
    _seed(pg_pair["src"])
    _target_table(pg_pair["dst"])
    ck = _Checkpoint(tmp_path / "move.json")
    _move(engine, ck, [])
    said = []
    _move(engine, _Checkpoint(tmp_path / "move.json"), said)
    assert said == ["public.big: done earlier, and both sides still hold"
                    " the same rows - skipped"], said
    # the source changes inside one range after the move
    psql(pg_pair["src"], "update public.big set payload = 'changed'"
                         " where id between 30001 and 30010")
    real, copied = engine._copy_checked, []
    monkeypatch.setattr(engine, "_copy_checked",
                        lambda *a, **k: copied.append(a[3]) or real(*a, **k))
    said = []
    _move(engine, _Checkpoint(tmp_path / "move.json"), said)
    n = len(_Checkpoint(tmp_path / "move.json")["public.big"]["ranges"])
    assert f"public.big: done earlier, and 1 of {n} ranges no longer match" \
        " the source - copying those again" in said, said
    assert len(copied) == 1, copied
    assert _same(pg_pair)


def test_a_table_with_a_column_named_like_the_sample_is_still_sampled(
        engine, pg_pair):
    """`row_to_json(s)` read a column `s` where the table had one."""
    psql(pg_pair["src"], "create table public.words (id int primary key,"
                         " s text); insert into public.words values"
                         " (1, 'cafÃ©')")
    got = [r for r in engine.check_deep("postgres")
           if r.scope == "postgres mojibake"]
    assert [r.status for r in got] == ["diff"], [r.detail for r in got]
    assert "public.words" in got[0].detail, got[0].detail


def _seed_keyless(port, rows=60_000):
    got = psql(port, f"""
        create table public.log (at bigint, payload text);
        insert into public.log select g, 'entry-' || g
        from generate_series(1, {rows}) g; analyze public.log""")
    assert got.returncode == 0, got.stderr


def _same_log(pg_pair):
    q = ("select count(*), md5(string_agg(at || payload, ',' order by at,"
         " payload)) from public.log")
    return psql(pg_pair["src"], q).stdout == psql(pg_pair["dst"], q).stdout


def test_a_table_with_no_key_resumes_by_where_its_rows_are_stored(
        engine, pg_pair, tmp_path, monkeypatch):
    """Nothing identifies a row of a table with no key, so it used to be
    copied in one piece and, stopped, started over. It goes in spans of
    stored position now, a transaction each, checkpointed as each commits:
    a stop costs the span it was in."""
    from migkit import ranges
    from migkit.cli import _Checkpoint
    _seed_keyless(pg_pair["src"])
    psql(pg_pair["dst"], "create table public.log (at bigint, payload text)")
    real, calls = engine._copy_pipe, []

    def dies_on_the_third(*a, **k):
        calls.append(1)
        if len(calls) == 3:
            raise RuntimeError("the connection went away")
        return real(*a, **k)
    monkeypatch.setattr(engine, "_copy_pipe", dies_on_the_third)
    ck = _Checkpoint(tmp_path / "move.json")
    with pytest.raises(RuntimeError):
        engine.move_table("postgres", "public", "log", 500_000, ck, [].append)
    st = _Checkpoint(tmp_path / "move.json")["public.log"]
    assert len(st["spans"]) >= 4 and len(st["spans_done"]) == 2, st

    # the next span commits, and the stop comes before its checkpoint: the
    # target's count says it is there, and it is counted in, not copied twice
    monkeypatch.setattr(engine, "_copy_pipe", real)
    real_done = ranges.span_copied

    def lost_after_commit(*a, **k):
        raise RuntimeError("stopped before the checkpoint")
    monkeypatch.setattr(ranges, "span_copied", lost_after_commit)
    with pytest.raises(RuntimeError):
        engine.move_table("postgres", "public", "log", 500_000,
                          _Checkpoint(tmp_path / "move.json"), [].append)
    monkeypatch.setattr(ranges, "span_copied", real_done)
    again = []
    monkeypatch.setattr(engine, "_copy_pipe",
                        lambda *a, **k: again.append(1) or real(*a, **k))
    engine.move_table("postgres", "public", "log", 500_000,
                      _Checkpoint(tmp_path / "move.json"), [].append)
    st = _Checkpoint(tmp_path / "move.json")["public.log"]
    assert st["done"] and len(again) == len(st["spans"]) - 3, (again, st)
    assert _same_log(pg_pair)


def test_a_count_that_accounts_for_nothing_starts_the_table_over(
        engine, pg_pair, tmp_path):
    from migkit.cli import _Checkpoint
    _seed_keyless(pg_pair["src"])
    psql(pg_pair["dst"], "create table public.log (at bigint, payload text)")
    engine.move_table("postgres", "public", "log", 500_000,
                      _Checkpoint(tmp_path / "move.json"), [].append)
    saved = _Checkpoint(tmp_path / "move.json")
    st = saved["public.log"]
    st["spans_done"] = st["spans_done"][:2]
    st["span_tally"] = {k: v for k, v in st["span_tally"].items()
                        if int(k) in st["spans_done"]}
    del st["done"]
    saved.save()
    # a row nobody accounts for, written by something else
    psql(pg_pair["dst"], "insert into public.log values (-1, 'stray')")
    said = []
    engine.move_table("postgres", "public", "log", 500_000,
                      _Checkpoint(tmp_path / "move.json"), said.append)
    assert any("does not say which are whose - starting it over" in m
               for m in said), said
    assert _same_log(pg_pair)
