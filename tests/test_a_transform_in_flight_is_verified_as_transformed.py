"""A hop's `transform:` statement runs over each batch as it passes and the
check runs the same statement over the source, so a transformed column is
the same where both took it alike and named where the target holds
another value (backlog W4.a); where it runs is a rung on its measured cost
(W4.b, `decide.transform_place`)."""
import pytest

from migkit import decide, hopkeys, transforms
from tests import servers
from tests.conftest import needs_docker

# the move hands a masked or transformed table to the pair, which writes
# it through the hop's transforms, and the check asks the pair of it: the
# depth matrix's scope.transforms cell, on both servers below
PROVES = [("scope.transforms", ("postgres", "mysql"))]

LOWER = "lower(email)"
DIGITS = "regexp_replace(phone, '[0-9]', '#', 'g')"
ROWS = [(1, "Ann@Example.COM", "081-234-5678", "alice"),
        (2, None, "02 999 0000", "bob"),
        (3, "ÄB@EXAMPLE.COM", None, "carol"),
        (4, "x@example.com", "+66 1", "dave"),
        (5, "Y@Example.com", "5", "eve")]


def _counted(monkeypatch):
    """Each run of the statement, by how many values it took."""
    seen = []
    real = transforms._run_sql

    def run(exprs, columns):
        seen.append((isinstance(exprs, str), len(columns[0])))
        return real(exprs, columns)
    monkeypatch.setattr(transforms, "_run_sql", run)
    return seen


# ---- the statement and its batches -------------------------------------------

def test_a_batch_is_taken_through_one_statement_as_each_value_alone_is():
    at = [(1, f"sql {LOWER}"), (2, f"sql {DIGITS}"), (3, "truncate 1")]
    before = dict(transforms.RUNS)
    got = transforms.apply_rows(at, ROWS)
    assert transforms.RUNS["batch"] == before["batch"] + 1
    assert transforms.RUNS["row"] == before["row"]
    alone = [tuple(transforms.apply(r, v) if i in dict(at) else v
                   for i, v, r in ((i, v, dict(at).get(i))
                                   for i, v in enumerate(row)))
             for row in ROWS]
    assert got == alone
    assert got[0] == (1, "ann@example.com", "###-###-####", "a")
    assert got[1][1] is None and got[2][2] is None


@pytest.mark.parametrize("bad", ["count(email)", "lower(a) || b", "random()",
                                 "now()", "1", "(select 1)",
                                 "read_csv('/etc/hosts')", "*",
                                 "sum(x) over ()"])
def test_a_statement_the_check_cannot_hold_is_refused(bad):
    with pytest.raises(SystemExit):
        transforms.parse(f"sql {bad}")


def test_the_statement_reads_no_file():
    with pytest.raises(SystemExit):
        transforms._run_sql("v || (select 1)", [["a"]])
    con = transforms._engine()
    with pytest.raises(Exception):
        con.execute("select * from read_csv('/etc/hosts')")


def test_the_hop_says_whether_the_target_may_be_rewritten_after_the_copy():
    assert not transforms.authorized({"transform": {"people.email": LOWER}})
    assert transforms.authorized({"transform": {"on_target": True}})
    assert not transforms.authorized({})


def test_the_hop_file_folds_a_transform_into_its_masking_and_checks_it():
    opts = hopkeys.normalised(hopkeys.HOP_OPTIONS, {
        "transform": {"people.email": LOWER, "on_target": False}})
    assert opts["mask_on_write"] == {"people.email": f"sql {LOWER}"}
    said = {"people.email": "lower(email, name)", "email": LOWER,
            "people.phone": "now()", "on_target": "yes"}
    got = hopkeys._transform_problems("hops.h", {"transform": said})
    where = {p.where.rsplit(".", 1)[-1] for p in got}
    assert {"email", "phone", "on_target"} <= where and len(got) == 4
    both = {"transform": {"people.email": LOWER},
            "mask_on_write": {"people.email": "hash"}}
    assert hopkeys._transform_problems("hops.h", both)


# ---- where it runs: a rung on the measured cost ------------------------------

def _place(slow, runs=True, authorized=True, budget=None):
    rules = [(1, f"sql {LOWER}", '"email"')]
    # the first batches, at a copy's batch size
    sample = [list(r) for r in ROWS] * 1000
    real = transforms.measure
    try:
        if slow:
            # a statement a thousand times slower than the copy a row
            transforms.measure = lambda at, rows: 0.5
        return transforms.place(rules, sample, 100_000, 50.0,
                                "postgres" if runs else "nosuch",
                                authorized, budget)
    finally:
        transforms.measure = real


def test_a_statement_that_keeps_up_runs_in_flight():
    got = _place(slow=False)
    assert got.path == "in_flight"
    assert "measured over 5,000 rows" in got.reason, got.reason


def test_a_slow_statement_moves_to_the_target_where_it_can_and_may():
    got = _place(slow=True)
    assert got.path == "on_target", got.reason
    assert "keeping up" in got.reason or "in flight against" in got.reason


def test_a_slow_statement_is_refused_where_the_target_cannot_or_may_not():
    for got in (_place(slow=True, authorized=False),
                _place(slow=True, runs=False)):
        assert got.rung is None
        assert "in flight against" in got.reason
    assert "does not let the target be changed" in \
        _place(slow=True, authorized=False).reason


def test_a_budget_leaves_room_for_a_slow_statement():
    # the target may not be changed after the copy: the budget holds it
    assert _place(slow=True, authorized=False,
                  budget=60_000.0).path == "in_flight"
    # where it may, the cheaper of what holds is taken
    assert _place(slow=True, budget=60_000.0).path == "on_target"


def test_nothing_measured_is_not_placed_in_flight():
    got = decide.transform_place({"measured": False, "target_runs": False})
    assert got.rung is None and "not run over any" in got.reason


# ---- on the servers ----------------------------------------------------------

pg = servers.fixture("pg")
my = servers.fixture("mysql")


def _hop(engine, space, **extra):
    from migkit.config import Hop
    options = hopkeys.normalised(hopkeys.HOP_OPTIONS, {
        "transform": {"people.email": LOWER, "people.phone": DIGITS},
        "mask_on_write": {"people.name": "truncate 1"}, **extra})
    return Hop(name="transformed", engine=engine,
               source=space.src.endpoint(), target=space.dst.endpoint(),
               databases=[space.name], options=options)


def _values(rows):
    return ", ".join("(" + ", ".join(
        "null" if v is None else str(v) if isinstance(v, int)
        else "'" + v + "'" for v in r) + ")" for r in rows)


def _moved_and_checked(eng, space, db, sch, edit, tmp_path, monkeypatch):
    from migkit import cli
    hop = eng.hop
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    seen = _counted(monkeypatch)
    ck = cli._checkpoint(hop, tmp_path / "move.json")
    cli._copy_table_by(eng, db, sch, "people", 2, ck, lambda m: None)
    # every row through the statement once, in batches, none alone
    assert seen and not any(alone for alone, _ in seen), seen
    assert sum(n for _, n in seen) >= len(ROWS)
    res = eng.check_data(db)
    assert res and all(r.status == "ok" for r in res), \
        [(r.scope, r.status, r.detail) for r in res]
    space.dst.sql(edit, db)
    res = eng.check_data(db)
    assert any(r.status == "diff" for r in res), \
        [(r.scope, r.status, r.detail) for r in res]


@needs_docker
def test_postgres_a_transformed_table_is_copied_and_held_as_transformed(
        pg, tmp_path, monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    db = pg.name
    for side in (pg.src, pg.dst):
        side.sql("drop table if exists people; create table people (id int"
                 " primary key, email text, phone text, name text)", db)
    pg.src.sql(f"insert into people values {_values(ROWS)}", db)
    eng = PostgresEngine(_hop("postgres", pg))
    assert not eng.masks_refused(db)
    _moved_and_checked(eng, pg, db, "public",
                       "update people set email = 'Ann@Example.COM'"
                       " where id = 1", tmp_path, monkeypatch)
    got = pg.dst.sql("select email, phone, name from people where id = 3",
                     db)
    assert tuple(got[0]) == ("äb@example.com", None, "c")
    keyed = PostgresEngine(_hop("postgres", pg,
                                transform={"people.id": "abs(id)"}))
    refused = dict(keyed.masks_refused(db))
    assert any(k.endswith("people.id") and "part of a key" in why
               for k, why in refused.items()), refused


@needs_docker
def test_mysql_a_transformed_table_is_copied_and_held_as_transformed(
        my, tmp_path, monkeypatch):
    from migkit.engines.mysql import MySQLEngine
    db = my.name
    for side in (my.src, my.dst):
        side.sql(f"drop table if exists `{db}`.people", db)
        side.sql(f"create table `{db}`.people (id int primary key, email"
                 " varchar(80), phone varchar(40), name varchar(40))"
                 " character set utf8mb4", db)
    my.src.sql(f"insert into `{db}`.people values {_values(ROWS)}", db)
    eng = MySQLEngine(_hop("mysql", my))
    assert not eng.masks_refused(db)
    _moved_and_checked(eng, my, db, None,
                       f"update `{db}`.people set phone = '081-234-5678'"
                       " where id = 1", tmp_path, monkeypatch)
    got = my.dst.sql(f"select email, phone, name from `{db}`.people"
                     " where id = 4", db)
    assert tuple(got[0]) == ("x@example.com", "+## #", "d")
    keyed = MySQLEngine(_hop("mysql", my, transform={"people.id": "abs(id)"}))
    refused = dict(keyed.masks_refused(db))
    assert any(k.endswith("people.id") and "part of a key" in why
               for k, why in refused.items()), refused
