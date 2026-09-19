"""The row count a chunked table reports when one of its chunks differs.

Found by running migkit against ten million rows rather than by reading it,
which is the only way it could have been found: the bug needs a table big
enough to be split into ranges, and every table in the suite was small.

    counts   postgres: DIFF public.bench_rows src=2000000 dst=1999992

about a table holding 10,000,000 rows on the source and 9,999,985 on the
target. `migkit check --only counts` on its own said 10000000/9999985; the
number went wrong only when the counts rode along with the checksum pass,
which is the default. Five chunks of two million, and the differing chunk's
count was reported as the table's.

That is a wrong number in the report an operator trusts, in the field they
use to decide how bad it is. The chunked path now asks both sides for the
table's own count at the moment it stops, and the `where` clause continues to
say which range the checksums came from.

The chunk size is forced down here so the same shape reproduces on a table
small enough to run in a second.
"""
import pytest

from migkit import checkpoint as cp_module
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

ROWS = 400


def _engine(pg_pair, tmp_path, monkeypatch):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="chunked", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              workers=2)
    monkeypatch.setattr(hop, "report_dir", lambda db=None: tmp_path,
                        raising=False)
    # small enough that 400 rows become several ranges
    monkeypatch.setattr(PostgresEngine, "CHUNK_MIN_ROWS", 50)
    monkeypatch.setattr(cp_module, "MAX_CHUNK", 100)
    return PostgresEngine(hop)


def _seed(pg_pair):
    for port in (pg_pair["src"], pg_pair["dst"]):
        got = psql(port, "drop table if exists public.wide;"
                         " create table public.wide (id bigint primary key,"
                         " v text);"
                         f" insert into public.wide select g, 'v'||g from"
                         f" generate_series(1,{ROWS}) g;")
        assert got.returncode == 0, got.stderr


def test_a_differing_chunk_does_not_report_its_own_count_as_the_tables(
        pg_pair, tmp_path, monkeypatch):
    _seed(pg_pair)
    # drift inside one range only, and a row count that differs by a known
    # amount so a chunk's count could never be mistaken for the right answer
    psql(pg_pair["dst"], "update public.wide set v='DRIFTED' where id = 7;"
                         " delete from public.wide where id in (11,12,13);")
    eng = _engine(pg_pair, tmp_path, monkeypatch)

    rc, out = eng._data_fast_native("postgres")
    assert rc != 0, out            # it must have found the difference
    line = [l for l in out.splitlines() if l.startswith("public.wide:")][0]
    assert "chunks=" in line or "where" in line, line

    n, rows_src, rows_dst, bad = eng._parse_fast(out)
    assert (rows_src, rows_dst) == (ROWS, ROWS - 3), (line, rows_src,
                                                      rows_dst)
    assert any("public.wide" in b for b in bad), bad


def test_the_same_table_without_drift_still_counts_right(pg_pair, tmp_path,
                                                          monkeypatch):
    """The clean path already summed the ranges; this keeps it that way."""
    _seed(pg_pair)
    eng = _engine(pg_pair, tmp_path, monkeypatch)
    rc, out = eng._data_fast_native("postgres")
    assert rc == 0, out
    n, rows_src, rows_dst, bad = eng._parse_fast(out)
    assert (rows_src, rows_dst) == (ROWS, ROWS), out
    assert not bad, bad


def test_the_counts_check_itself_agrees_with_the_server(pg_pair, tmp_path,
                                                         monkeypatch):
    """What the operator actually reads, against what the database says."""
    _seed(pg_pair)
    psql(pg_pair["dst"], "delete from public.wide where id in (5,6);")
    eng = _engine(pg_pair, tmp_path, monkeypatch)
    got = [r for r in eng.check_counts("postgres") if r.status != "ok"]
    assert got, "no difference reported at all"
    detail = " ".join(r.detail for r in got)
    assert f"src={ROWS}" in detail, detail
    assert f"dst={ROWS - 2}" in detail, detail
