"""The local copy a dump-and-load path makes is private, and does not
outlive the move (backlog 41).

It is the source's data, all of it, in files under the report directory.
It was removed only after a load that succeeded; one that failed or was
stopped left it there, readable by whoever could read the directory,
until the next run happened to remove it first.
"""
import stat

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql


def test_the_copy_is_this_users_only_and_gone_after_a_failure(tmp_path):
    from migkit import movers
    where = tmp_path / "copy"
    where.mkdir()
    (where / "left-by-a-dead-run").write_text("rows")
    with pytest.raises(RuntimeError):
        with movers._LocalCopy(where):
            assert not (where / "left-by-a-dead-run").exists()
            assert stat.S_IMODE(where.stat().st_mode) == 0o700
            (where / "table.dat").write_text("rows")
            raise RuntimeError("the load failed")
    assert not where.exists()


@needs_docker
def test_a_failed_postgres_load_takes_its_copy_with_it(pg_pair, tmp_path,
                                                       monkeypatch):
    from migkit import movers
    for port in pg_pair.values():
        psql(port, "drop table if exists public.lc;"
                   " create table public.lc (id int primary key)")
    psql(pg_pair["src"], "insert into public.lc select generate_series(1,50)")
    hop = Hop(name="lc", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              db_map={"postgres": "postgres"})
    hop.report_dir = lambda db=None: tmp_path
    seen = {}

    def failing(load, env, log):
        copy = [p for p in tmp_path.iterdir() if p.is_dir()]
        seen["modes"] = {p.name: stat.S_IMODE(p.stat().st_mode)
                         for p in copy}
        seen["files"] = sum(1 for p in copy for _ in p.rglob("*"))
        raise RuntimeError("the load failed")
    monkeypatch.setattr(movers, "_pgdump_restore", failing)
    try:
        with pytest.raises(RuntimeError, match="the load failed"):
            movers.pgdump_move(hop, "postgres", 1, True, None)
        # the dump was there, and private, while the load ran
        assert seen["files"] > 0, seen
        assert seen["modes"] and set(seen["modes"].values()) == {0o700}, seen
        # and it is gone
        assert not [p for p in tmp_path.iterdir()
                    if p.is_dir() and p.name in seen["modes"]], \
            list(tmp_path.iterdir())
    finally:
        for port in pg_pair.values():
            psql(port, "drop table if exists public.lc")
