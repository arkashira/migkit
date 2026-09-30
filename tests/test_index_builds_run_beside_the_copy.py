"""A finished table's indexes are built while the next table copies
(backlog R20 item 5), each build under session settings the target's own
size gives - never a server setting.

Measured before: a table's copier built its indexes on its way out and
held its place among the tables copied at once until the last one was
built, so the next table waited on another table's builds.
"""
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql


def _hop(tmp_path=None, ports=(1, 2)):
    hop = Hop(name="ixb", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=ports[0],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=ports[1],
                              user="postgres", password="test"),
              databases=["postgres"], workers=4)
    if tmp_path is not None:
        hop.report_dir = lambda db=None: tmp_path
    return hop


@pytest.mark.parametrize("got, cpus, at_once, said", [
    # a target sized with 16 GB of shared buffers and 16 CPUs, four
    # builds at once: 2 GB each at most, three helpers and the leader each
    ("67108864|17179869184", 16, 4,
     "set maintenance_work_mem = '2097152kB'; set"
     " max_parallel_maintenance_workers = 3; "),
    # the default 128 MB: a share is below what the session has already
    ("67108864|134217728", 2, 2, "set max_parallel_maintenance_workers = 0; "),
    # CPUs not readable: the server's own number of helpers stands
    ("67108864|4294967296", None, 1,
     "set maintenance_work_mem = '2097152kB'; "),
])
def test_each_build_is_sized_from_what_the_target_says(monkeypatch, got,
                                                       cpus, at_once, said):
    from migkit import movers
    from migkit.engines.postgres import PostgresEngine
    monkeypatch.setattr(movers, "_pg_psql", lambda hop, db, sql: got)
    monkeypatch.setattr(PostgresEngine, "capacity",
                        lambda self, side, db: {"cpus": cpus} if cpus else {})
    assert movers._pg_build_settings(_hop(), "postgres", at_once) == said


def test_a_target_that_says_nothing_keeps_its_own_settings(monkeypatch):
    from migkit import movers

    def refused(hop, db, sql):
        raise RuntimeError("permission denied")
    monkeypatch.setattr(movers, "_pg_psql", refused)
    assert movers._pg_build_settings(_hop(), "postgres", 2) == ""


SHAPE = "(id bigint primary key, a text, b int, c numeric)"
INDEXES = ("create index {t}_a on public.{t} (a);"
           " create index {t}_bc on public.{t} (b, c)")


@needs_docker
def test_the_next_table_copies_while_the_last_ones_indexes_build(
        pg_pair, tmp_path, monkeypatch):
    from migkit import cli, movers, ranges, sizing
    from migkit.cli import _Checkpoint
    from migkit.engines.postgres import PostgresEngine
    for t in ("big", "big2"):
        got = psql(pg_pair["src"], f"""
            create table public.{t} {SHAPE};
            insert into public.{t} select g, md5(g::text), g % 97, g / 3.0
              from generate_series(1, 200000) g;
            analyze public.{t}""")
        assert got.returncode == 0, got.stderr
        got = psql(pg_pair["dst"], f"create table public.{t} {SHAPE};"
                                   + INDEXES.format(t=t))
        assert got.returncode == 0, got.stderr
    defs = ("select string_agg(indexdef, ';' order by indexname) from"
            " pg_indexes where schemaname = 'public'")
    before = psql(pg_pair["dst"], defs).stdout
    monkeypatch.setattr(ranges, "LEAST", 50_000)
    monkeypatch.setattr(PostgresEngine, "INDEX_WINDOW_FROM", 50_000)
    monkeypatch.setattr(cli, "_changelog", lambda *a, **k: None)
    # one table at a time, so only the builds can run beside a copy
    monkeypatch.setattr(sizing, "estimate", lambda *a, **k: sizing.Plan(
        1, 1, {}))
    printed = []
    monkeypatch.setattr(cli.console, "print",
                        lambda m="", *a, **k: printed.append(str(m)))
    eng = PostgresEngine(_hop(tmp_path, (pg_pair["src"], pg_pair["dst"])))
    events = []
    real_move, real_build = eng.move_table, movers._IndexWindow._put_back

    def move_table(db, sch, tbl, *a, **k):
        events.append(("copy", tbl, "start", time.monotonic()))
        real_move(db, sch, tbl, *a, **k)
        events.append(("copy", tbl, "end", time.monotonic()))

    def build(self):
        name = self.table[1] if self.table else None
        events.append(("build", name, "start", time.monotonic()))
        real_build(self)
        events.append(("build", name, "end", time.monotonic()))
    monkeypatch.setattr(eng, "move_table", move_table)
    monkeypatch.setattr(movers._IndexWindow, "_put_back", build)
    cli._copy_tables(eng.hop, eng, "postgres",
                     [("public", "big"), ("public", "big2")], 500_000,
                     _Checkpoint(tmp_path / "move.json"))
    at = {(w, t, e): s for w, t, e, s in events}
    first = min(("big", "big2"), key=lambda t: at[("copy", t, "start")])
    second = ({"big", "big2"} - {first}).pop()
    # the second table began copying before the first one's indexes were
    # all built - it did not wait for them
    assert at[("copy", second, "start")] < at[("build", first, "end")], \
        events
    # and the copy of the database ended only once every build had
    assert psql(pg_pair["dst"], defs).stdout == before
    assert sum("2 indexes dropped for the load and rebuilt after" in m
               for m in printed) == 2, printed
    q = "select count(*), sum(b), md5(string_agg(a, ',' order by id)) from"
    for t in ("big", "big2"):
        assert psql(pg_pair["src"], f"{q} public.{t}").stdout == \
            psql(pg_pair["dst"], f"{q} public.{t}").stdout
