"""A changed row is repaired from whichever side changed it last, when the
hop names the column both sides keep for that (`newer_wins`).

`source-wins` overwrote every row that differed, and `keep-target` kept
every one. DTS's `ConditionCover` and pglogical's `last_update_wins` decide
per row from a timestamp both sides maintain; this is that, with the
refusal the backlog asks for when the column is not on both sides.
"""
import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _hop(pg_pair, tmp_path, **options):
    hop = Hop(name="nw", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], options=options)
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _seed(pg_pair, column="updated_at"):
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "drop table if exists public.t;"
                   f" create table public.t (id int primary key, v text,"
                   f" {column} timestamp)")
    psql(pg_pair["src"], "insert into public.t values"
                         " (1, 'source newer', '2026-01-02'),"
                         " (2, 'source older', '2026-01-01')")
    psql(pg_pair["dst"], "insert into public.t values"
                         " (1, 'target older', '2026-01-01'),"
                         " (2, 'target newer', '2026-01-02')")


def _repair(eng):
    eng.check_data("postgres")
    for action in eng.repair_plan("postgres", "rows"):
        eng.apply("postgres", action)


def test_each_row_comes_from_the_side_that_changed_it_last(pg_pair,
                                                           tmp_path):
    from migkit.engines.postgres import PostgresEngine
    _seed(pg_pair)
    eng = PostgresEngine(_hop(pg_pair, tmp_path, newer_wins="updated_at"))
    _repair(eng)
    got = psql(pg_pair["dst"], "select id||':'||v from public.t order by id"
               ).stdout.split("\n")
    assert got[:2] == ["1:source newer", "2:target newer"], got
    kept = next(tmp_path.rglob("data-public.t.kept-newer")).read_text()
    assert kept.strip() == "2", kept


def test_without_the_option_the_source_still_wins(pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    _seed(pg_pair)
    _repair(PostgresEngine(_hop(pg_pair, tmp_path)))
    got = psql(pg_pair["dst"], "select id||':'||v from public.t order by id"
               ).stdout.split("\n")
    assert got[:2] == ["1:source newer", "2:source older"], got


def test_a_column_one_side_lacks_is_refused(pg_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    _seed(pg_pair, column="changed_on")
    eng = PostgresEngine(_hop(pg_pair, tmp_path, newer_wins="updated_at"))
    eng.check_data("postgres")
    with pytest.raises(SystemExit) as e:
        for action in eng.repair_plan("postgres", "rows"):
            eng.apply("postgres", action)
    assert "not on both sides" in str(e.value), e.value
    assert psql(pg_pair["dst"], "select v from public.t where id = 1"
                ).stdout.strip() == "target older"
