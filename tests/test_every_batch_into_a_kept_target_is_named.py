"""Every batch the tail applies into a target nothing keeps from the
application is named (table, kind, unit, rows) as the engine applies it,
each batch its own unit - the table, its ordinal, its rows
(`Engine.neutral_apply` -> `Engine.named_batch` -> `freeze.name_write`),
and so is every batch a repair writes (`Engine._apply_rows`) and every
range a copier loads (its range unit), so `freeze.attributed`
accounts for exactly those writes: a cutover of such a run reports none
unaccounted, and one write by another session is the single finding."""
import pathlib
import re
import types

import pytest

from migkit import canon, freeze
from migkit.config import Hop
from tests import servers
from tests.conftest import needs_docker

PROVES = [("cutover.freeze", ("postgres", "mysql"))]

pg = servers.fixture("pg")
my = servers.fixture("mysql")


def _batch(table, first, n):
    out = [canon.change("insert", table, {"id": i}, {"id": i, "v": f"v{i}"})
           for i in range(first, first + n)]
    canon.ended(out, 0, at="0/1", ts=100 + first)
    return out


def _hop(engine, pair, db, tmp_path):
    hop = Hop(name="named", engine=engine, source=pair.src.endpoint(),
              target=pair.dst.endpoint(), databases=[db],
              options={"protect_target": True})
    where = tmp_path / "reports"

    def report_dir(d=""):
        out = where / d if d else where
        out.mkdir(parents=True, exist_ok=True)
        return out
    hop.report_dir = report_dir
    return hop


def _run(eng, hop, pair, db, table, count):
    """Three batches applied; the target's rows read back, and what the
    hop named."""
    record = freeze._named_by_units(hop, eng,
                                    freeze.hold_for(hop, eng, "dst"), db)
    assert record["way"] == freeze.UNNAMED
    sizes = (3, 2, 4)
    first = 0
    for n in sizes:
        assert eng.neutral_apply("dst", db, _batch(table, first, n)) == n
        first += n
    assert count() == sum(sizes), "the batches did not all land"
    got = freeze.named(hop, db)
    assert [(w["table"], w["kind"], w["unit"], w["rows"]) for w in got] == \
        [(table, "change", f"change {table} #{i} ({n} rows)", n)
         for i, n in enumerate(sizes, 1)]
    me = hop.target.user
    seen = [{"table": table, "kind": "change", "user": me}
            for n in sizes for _ in range(n)]
    ok = freeze.attributed(hop, eng, db, seen)
    assert ok["findings"] == [], ok
    assert ok["named"] == [{"table": table, "kind": "change",
                            "rows": sum(sizes), "units": sorted(
                                f"change {table} #{i} ({n} rows)"
                                for i, n in enumerate(sizes, 1))}]
    # one write by another session: the one finding, naming its table
    pair.dst.sql(f"insert into {table} values (100, 'app')", db=db)
    seen.append({"table": table, "kind": "change", "user": "app"})
    bad = freeze.attributed(hop, eng, db, seen)
    assert bad["findings"] == [f"{table}: a change that is not migkit's"], bad
    # and the cutover's preflight carries it at its gate
    rows = freeze.kept_rows(hop, eng, db, "fail")
    assert rows and rows[0][0] == "ok" and "9 writes" in rows[0][1], rows


@needs_docker
def test_postgres_names_each_batch_and_the_foreign_write_is_the_one_finding(
        pg, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    db = pg.name
    for s in (pg.src, pg.dst):
        s.sql("create table public.t (id int primary key, v text)", db=db)
    hop = _hop("postgres", pg, db, tmp_path)
    eng = PostgresEngine(hop)
    _run(eng, hop, pg, db, "public.t",
         lambda: int(pg.dst.sql("select count(*) from public.t",
                                db=db)[0][0]))


@needs_docker
def test_mysql_names_each_batch_and_the_foreign_write_is_the_one_finding(
        my, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    db = my.name
    for s in (my.src, my.dst):
        s.sql("create table t (id int primary key, v varchar(20))", db=db)
    hop = _hop("mysql", my, db, tmp_path)
    eng = MySQLEngine(hop)
    _run(eng, hop, my, db, "t",
         lambda: int(my.dst.sql("select count(*) from t", db=db)[0][0]))


# --- without a server: the name is the batch's own -----------------------------

def _named_hop(tmp_path):
    hop = types.SimpleNamespace(options={"protect_target": True},
                                databases=["app"])

    def report_dir(d=""):
        out = tmp_path / (d or "_")
        out.mkdir(parents=True, exist_ok=True)
        return out
    hop.report_dir = report_dir
    return hop


def test_a_resumed_run_numbers_its_batches_on_where_the_last_stopped(
        tmp_path):
    from migkit.engines.base import Engine
    hop = _named_hop(tmp_path)
    first = object.__new__(Engine)
    first.hop = hop
    assert first.named_batch("app", "t", "change", 3)
    assert first.named_batch("app", "t", "change", 2)
    assert first.named_batch("app", "t", "repair", 1)
    assert not first.named_batch("app", "t", "change", 0), "no rows, no unit"
    again = object.__new__(Engine)          # the run resumed in a new process
    again.hop = hop
    assert again.named_batch("app", "t", "change", 4)
    got = [(w["kind"], w["unit"], w["rows"]) for w in freeze.named(hop, "app")]
    assert got == [("change", "change t #1 (3 rows)", 3),
                   ("change", "change t #2 (2 rows)", 2),
                   ("repair", "repair t #1 (1 rows)", 1),
                   ("change", "change t #3 (4 rows)", 4)]
    # a hop that names nothing counts nothing
    quiet = object.__new__(Engine)
    quiet.hop = types.SimpleNamespace(options={}, report_dir=hop.report_dir)
    assert not quiet.named_batch("app", "t", "change", 5)
    assert len(freeze.named(hop, "app")) == 4


def test_no_write_of_migkit_is_named_by_one_constant():
    """Every call into the one door names its batch from the batch: no
    unit is a fixed string, so no two batches share a name."""
    root = pathlib.Path(__file__).resolve().parent.parent / "migkit"
    fixed = []
    for f in root.rglob("*.py"):
        text = f.read_text()
        for m in re.finditer(r"named_write\(([^)]*)\)", text):
            args = [a.strip() for a in m.group(1).split(",")]
            if len(args) >= 4 and re.fullmatch(r'f?"[^"{]*"', args[3]):
                fixed.append(f"{f.name}: {m.group(0)}")
    assert fixed == [], fixed
    base = (root / "engines" / "base.py").read_text()
    for where in ("def _apply_rows", "def neutral_apply"):
        body = base[base.index(where):]
        body = body[:body.index("\n    def ", 10)]
        assert "named_batch(" in body, where
