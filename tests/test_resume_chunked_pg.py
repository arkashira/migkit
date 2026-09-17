"""The chunked, restartable checksum must agree with the single-pass one.

Run against real Postgres, because the property being tested is that summing
`sum(md5::bit(64)::bigint::numeric)` over primary-key ranges equals the value
the whole-table aggregate produces - which is a claim about Postgres
arithmetic, not about Python.
"""
import json

from migkit import checkpoint as cp_module
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _seed(port, rows, offset=0):
    psql(port, f"""
        drop table if exists public.big;
        create table public.big (id bigint primary key, payload text,
                                 n numeric, ts timestamptz);
        insert into public.big
        select g + {offset}, 'row-'||g, g * 1.5,
               '2026-01-01'::timestamptz + (g || ' seconds')::interval
        from generate_series(1, {rows}) g;
    """)


def test_chunked_total_equals_single_pass(pg_pair, tmp_path, monkeypatch):
    from migkit.config import Hop, Endpoint
    from migkit.engines.postgres import PostgresEngine

    _seed(pg_pair["src"], 20000)
    _seed(pg_pair["dst"], 20000)

    hop = Hop(name="t", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              workers=2)
    monkeypatch.setattr(hop, "report_dir", lambda db=None: tmp_path,
                        raising=False)
    eng = PostgresEngine(hop)

    # one pass, the way it has always worked
    monkeypatch.setattr(PostgresEngine, "CHUNK_MIN_ROWS", 10**12)
    rc, whole = eng._data_fast_native("postgres")
    assert rc == 0, whole
    one_pass = [l for l in whole.splitlines() if l.startswith("public.big:")][0]

    # and again in ranges small enough to force several of them
    monkeypatch.setattr(PostgresEngine, "CHUNK_MIN_ROWS", 1000)
    monkeypatch.setattr(cp_module, "MAX_CHUNK", 4000)
    rc, chunked = eng._data_fast_native("postgres")
    assert rc == 0, chunked
    line = [l for l in chunked.splitlines() if l.startswith("public.big:")][0]
    assert "chunks=" in line, line

    def nums(s):
        return (s.split("rows=")[1].split()[0],
                s.split("checksum=")[1].split()[0])
    assert nums(line) == nums(one_pass)


def test_a_crash_resumes_instead_of_restarting(pg_pair, tmp_path, monkeypatch):
    from migkit.config import Hop, Endpoint
    from migkit.engines.postgres import PostgresEngine

    _seed(pg_pair["src"], 12000)
    _seed(pg_pair["dst"], 12000)

    hop = Hop(name="t", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              workers=1)
    monkeypatch.setattr(hop, "report_dir", lambda db=None: tmp_path,
                        raising=False)
    monkeypatch.setattr(PostgresEngine, "CHUNK_MIN_ROWS", 1000)
    monkeypatch.setattr(cp_module, "MAX_CHUNK", 3000)
    eng = PostgresEngine(hop)

    # stop the first range from ever being asked for again: do it by hand,
    # record it, and leave the checkpoint behind the way a crash would
    from migkit import checkpoint as cp_mod
    bounds = psql(pg_pair["src"],
                  "select min(id)||'|'||max(id) from public.big").stdout.strip()
    lo, hi = (int(x) for x in bounds.split("|"))
    ranges = cp_mod.plan_ranges(lo, hi, 3000)  # matches MAX_CHUNK
    expr = eng._row_hash_expr("src", "postgres", "public.big")
    cp = cp_mod.Checkpoint(str(tmp_path / "checkpoint.json"))
    cp.begin("public.big", expr, ranges)
    first = eng._psql("src", "postgres",
        f'select count(*)||chr(124)||coalesce(sum((\'x\'||substr({expr},1,16))'
        f'::bit(64)::bigint::numeric),0) from public."big" t where '
        + cp_mod.where('"id"', *ranges[0])).strip()
    cp.record("public.big", *ranges[0], *first.split("|", 1))

    rc, out = eng._data_fast_native("postgres")
    assert rc == 0, out
    line = [l for l in out.splitlines() if l.startswith("public.big:")][0]
    assert "resumed=1/" in line, line
    # and the resumed total still matches every row
    assert f"rows={12000}" in line, line
    # the checkpoint is cleared once the table is fully verified
    left = json.loads((tmp_path / "checkpoint.json").read_text())
    assert "public.big" not in left["tables"]


def test_a_chunk_level_difference_is_localised(pg_pair, tmp_path, monkeypatch):
    from migkit.config import Hop, Endpoint
    from migkit.engines.postgres import PostgresEngine

    _seed(pg_pair["src"], 9000)
    _seed(pg_pair["dst"], 9000)
    psql(pg_pair["dst"],
         "update public.big set payload = 'tampered' where id = 8500")

    hop = Hop(name="t", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              workers=1)
    monkeypatch.setattr(hop, "report_dir", lambda db=None: tmp_path,
                        raising=False)
    monkeypatch.setattr(PostgresEngine, "CHUNK_MIN_ROWS", 1000)
    monkeypatch.setattr(cp_module, "MAX_CHUNK", 2000)
    eng = PostgresEngine(hop)

    rc, out = eng._data_fast_native("postgres")
    assert rc == 1
    line = [l for l in out.splitlines() if l.startswith("public.big:")][0]
    # the report names the key range the difference is in, not just the table
    assert "where" in line and "8" in line, line
