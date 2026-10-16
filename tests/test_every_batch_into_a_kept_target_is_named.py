"""Every batch the tail applies into a target nothing keeps from the
application is named (table, kind, unit, rows) as the engine applies it
(`Engine.neutral_apply` -> `freeze.name_write`), so `freeze.attributed`
accounts for exactly those writes: a cutover of such a run reports none
unaccounted, and one write by another session is the single finding."""
from migkit import canon, freeze
from migkit.config import Hop
from tests import servers
from tests.conftest import needs_docker

PROVES = [("cutover.freeze", ("postgres", "mysql"))]

pytestmark = needs_docker

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
        [(table, "change", "tail batch", n) for n in sizes]
    me = hop.target.user
    seen = [{"table": table, "kind": "change", "user": me}
            for n in sizes for _ in range(n)]
    ok = freeze.attributed(hop, eng, db, seen)
    assert ok["findings"] == [], ok
    assert ok["named"] == [{"table": table, "kind": "change",
                            "rows": sum(sizes), "units": ["tail batch"]}]
    # one write by another session: the one finding, naming its table
    pair.dst.sql(f"insert into {table} values (100, 'app')", db=db)
    seen.append({"table": table, "kind": "change", "user": "app"})
    bad = freeze.attributed(hop, eng, db, seen)
    assert bad["findings"] == [f"{table}: a change that is not migkit's"], bad
    # and the cutover's preflight carries it at its gate
    rows = freeze.kept_rows(hop, eng, db, "fail")
    assert rows and rows[0][0] == "ok" and "9 writes" in rows[0][1], rows


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
