"""The second reader on a real PostgreSQL pair: grouped aggregates find
the group a difference is in, migkit's own row compare names the rows
there, and the row reading holds those rows to a second opinion
(backlog 0f).

Against the reader installed in its own environment - nothing stubbed.
The table is 3,000 rows in ten groups of `g`, and the hop's slice is 500,
so a differing table is large enough to narrow.
"""
import os
import sys
from pathlib import Path

import pytest

from migkit import second_reader as sr
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql, verdict
from tests.test_the_report_does_not_name_its_tools import TOOLS

READER = os.environ.get("MIGKIT_SECOND_READER_PYTHON") or str(
    Path(sys.prefix).parent / ".venv-dvt" / "bin" / "python")

pytestmark = [needs_docker,
              pytest.mark.skipif(not Path(READER).exists(),
                                 reason="the second reader is not installed")]

LEAKS = TOOLS + ("dvt", "data-validation", "data_validation")


@pytest.fixture
def reader(monkeypatch):
    monkeypatch.setenv("MIGKIT_SECOND_READER_PYTHON", READER)


def _pg(port, sql):
    got = psql(port, sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _seed(pg_pair, *changes):
    for port in (pg_pair["src"], pg_pair["dst"]):
        _pg(port, "create table public.t (id int primary key, g int,"
                  " n bigint, s text); insert into public.t select i, i % 10,"
                  " i * 7, 'v' || i from generate_series(1, 3000) i")
    # the source's statistics, which the group is chosen from: the test's
    # own doing, never migkit's
    _pg(pg_pair["src"], "analyze public.t")
    for sql in changes:
        _pg(pg_pair["dst"], sql)


def _engine(pg_pair, tmp_path, **options):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="sr0f", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], slice=500, options=options)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def _named(tmp_path, kind):
    f = tmp_path / f"data-public.t.{kind}"
    return sorted(f.read_text().split()) if f.exists() else []


def _contents(pg_pair):
    return {s: _pg(pg_pair[s], "select md5(string_agg(t::text, ',' order by"
                               " id)) from public.t t")
            for s in ("src", "dst")}


def test_the_group_is_found_and_only_its_rows_are_walked(pg_pair, tmp_path,
                                                         reader):
    _seed(pg_pair, "update public.t set n = n + 1 where id = 13")
    before = _contents(pg_pair)
    eng = _engine(pg_pair, tmp_path)
    got = eng.check_data("postgres")
    data = verdict(got, "postgres")
    assert data.status == "diff", data.detail
    assert _named(tmp_path, "changed") == ["13"]
    assert "narrowed by g to 1 of about 10 groups" in data.detail, data.detail
    assert "the rest proved equal" in data.detail, data.detail
    rows = verdict(got, "postgres.public.t rows")
    assert rows.status == "diff" and "the same 1 rows differ" in rows.detail
    # the second reading read the group, not the table: the batch it ran
    kept = (tmp_path / sr.BATCH).read_text()
    assert "coalesce(\"g\" in (3), false)" in kept, kept
    # and nothing was written to either side
    assert _contents(pg_pair) == before
    said = " ".join(f"{r.detail} {r.fix_hint}" for r in got).lower()
    assert not [t for t in LEAKS if t in said], said


def test_a_difference_no_group_measure_sees_is_still_named(pg_pair, tmp_path,
                                                           reader):
    """`v25` to `x25` moves no sum, no count and no length: its group
    passes. migkit's own digest of everything outside the groups that did
    not pass catches it, and the rest is walked."""
    _seed(pg_pair, "update public.t set n = n + 1 where id = 13",
          "update public.t set s = 'x25' where id = 25")
    eng = _engine(pg_pair, tmp_path)
    got = eng.check_data("postgres")
    data = verdict(got, "postgres")
    assert _named(tmp_path, "changed") == ["13", "25"], data.detail
    assert "walked as well" in data.detail, data.detail
    rows = verdict(got, "postgres.public.t rows")
    assert rows.status == "diff" and "the same 2 rows differ" in rows.detail


def test_what_the_second_reading_cannot_see_is_said_not_passed(pg_pair,
                                                               tmp_path,
                                                               reader):
    """A trailing blank: migkit's digest and its row compare see it; the
    reader's row hash drops trailing blanks before hashing. The finding
    stays a difference and says which reading could not see it."""
    _seed(pg_pair, "update public.t set s = s || ' ' where id = 37")
    eng = _engine(pg_pair, tmp_path)
    got = eng.check_data("postgres")
    assert verdict(got, "postgres").status == "diff"
    assert _named(tmp_path, "changed") == ["37"]
    rows = verdict(got, "postgres.public.t rows")
    assert rows.status == "diff", rows.detail
    assert "read the same the second way" in rows.detail, rows.detail
    assert "cannot see" in rows.detail and "37" in rows.detail


def test_a_small_differing_set_is_explained_column_by_column(pg_pair,
                                                             tmp_path,
                                                             reader):
    _seed(pg_pair, "update public.t set n = 0 where id = 13",
          "delete from public.t where id = 14")
    eng = _engine(pg_pair, tmp_path)
    data = verdict(eng.check_data("postgres"), "postgres")
    assert "public.t column by column" in data.detail, data.detail
    said = (tmp_path / "data-t.explained").read_text()
    assert "1 rows only on the source: id=14" in said, said
    assert "n: 1 rows differ - id=13: source 91 target 0" in said, said


def test_two_readings_of_every_table_where_the_hop_asks(pg_pair, tmp_path,
                                                        reader):
    _seed(pg_pair)
    eng = _engine(pg_pair, tmp_path, two_readings=True)
    assert "second" in eng.planned_checks()
    equal = eng.check_second("postgres")
    assert verdict(equal, "postgres.public.t").status == "ok"
    assert "measures agree" in verdict(equal, "postgres.public.t").detail
    rows = verdict(equal, "postgres.public.t rows")
    assert rows.status == "ok" and sr.HASHED in rows.detail
    _pg(pg_pair["dst"], "update public.t set n = n + 1 where id = 13")
    differ = eng.check_second("postgres")
    assert verdict(differ, "postgres.public.t").status == "diff"
    rows = verdict(differ, "postgres.public.t rows")
    assert rows.status == "diff" and "(by key: 13)" in rows.detail
    said = " ".join(r.detail for r in equal + differ).lower()
    assert not [t for t in LEAKS if t in said], said


def test_a_sample_is_an_early_signal_and_never_a_pass(pg_pair, tmp_path,
                                                      reader):
    _seed(pg_pair)
    eng = _engine(pg_pair, tmp_path,
                  two_readings={"sample": 100, "rows": False})
    got = eng.check_second("postgres")
    sample = verdict(got, "postgres.public.t sample")
    assert sample.status == "skip", sample
    assert "not a verdict" in sample.detail and "100 rows" in sample.detail
    assert not [r for r in got if r.scope.endswith(" rows")]
    _pg(pg_pair["dst"], "update public.t set n = n + 1")
    sample = verdict(eng.check_second("postgres"), "postgres.public.t sample")
    assert sample.status == "diff" and "100 of 100" in sample.detail
