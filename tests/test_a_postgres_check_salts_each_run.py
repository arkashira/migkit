"""The PostgreSQL check sums its row hashes behind a salt each run draws,
as the MySQL and SQL Server checks do.

Without one, two different tables that summed alike once - a chance of
2^-64 a comparison - summed alike on every check after it. With it, both
sides of one run hash alike and the next run draws again; a check resumed
from stored ranges keeps the salt those ranges were summed with, so its
total is the table's under one salt.
"""
from migkit import checkpoint as cp_module
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _seed(port, rows):
    psql(port, f"""
        drop table if exists public.big;
        create table public.big (id bigint primary key, payload text);
        insert into public.big select g, 'row-'||g
        from generate_series(1, {rows}) g;
        analyze public.big;
    """)


def _engine(pg_pair, tmp_path, monkeypatch):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="salt", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              workers=1)
    monkeypatch.setattr(hop, "report_dir", lambda db=None: tmp_path,
                        raising=False)
    return PostgresEngine(hop)


def _line(out):
    got = [x for x in out.splitlines() if x.startswith("public.big:")]
    assert got, out
    return got[0]


def test_each_run_sums_afresh_and_both_sides_alike(pg_pair, tmp_path,
                                                    monkeypatch):
    _seed(pg_pair["src"], 3000)
    _seed(pg_pair["dst"], 3000)
    sums = []
    for _ in range(2):
        rc, out = _engine(pg_pair, tmp_path, monkeypatch)._data_fast_native(
            "postgres", may_skip=False)
        line = _line(out)
        assert rc == 0 and ": OK" in line, line
        sums.append(line.split("checksum=")[1].split()[0])
    assert sums[0] != sums[1], "two runs summed with the same salt"

    psql(pg_pair["dst"], "update public.big set payload = 'x' where id = 7")
    rc, out = _engine(pg_pair, tmp_path, monkeypatch)._data_fast_native(
        "postgres", may_skip=False)
    assert rc != 0 and ": DIFF" in _line(out), out


def test_a_resumed_check_sums_with_the_salt_it_began_with(pg_pair, tmp_path,
                                                          monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    _seed(pg_pair["src"], 12000)
    _seed(pg_pair["dst"], 12000)
    monkeypatch.setattr(PostgresEngine, "CHUNK_MIN_ROWS", 1000)
    monkeypatch.setattr(cp_module, "MAX_CHUNK", 3000)
    eng = _engine(pg_pair, tmp_path, monkeypatch)
    # the first range, summed by an earlier run behind its own salt and
    # left in the checkpoint the way a stop leaves it
    bounds = psql(pg_pair["src"], "select min(id)||'|'||max(id) from"
                                  " public.big").stdout.strip()
    ranges = cp_module.plan_ranges(*(int(x) for x in bounds.split("|")), 3000)
    expr = eng._row_hash_expr("src", "postgres", "public.big")
    earlier = cp_module.new_salt()
    cp = cp_module.Checkpoint(str(tmp_path / "checkpoint.json"))
    cp.begin("public.big", expr, ranges)
    assert cp.salt("public.big", earlier) == earlier
    first = eng._psql("src", "postgres",
                      f"select count(*)||chr(124)||"
                      f"{eng._summed(expr, earlier)} from public.big t"
                      " where " + cp_module.where('"id"', *ranges[0]))
    cp.record("public.big", *ranges[0], *first.strip().split("|", 1))

    rc, out = eng._data_fast_native("postgres")
    line = _line(out)
    assert rc == 0 and "resumed=1/" in line, line
    whole = eng._psql("src", "postgres", f"select {eng._summed(expr, earlier)}"
                                         " from public.big t").strip()
    assert f"checksum={whole} " in line + " ", (line, whole)
