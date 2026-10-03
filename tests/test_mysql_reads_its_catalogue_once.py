"""The MySQL engine reads a side's catalogue in one pass, answers every
reader of a table's shape from it, and drift compares whole definitions
(audit 2026-10-01, SC3, SC14, SC15, SP2, DU26).

Columns, keys, rules and indexes were a statement a table each, each on a
connection of its own; drift compared a column's name and type string, so
a narrowed default, a NOT NULL, a collation, an index or a trigger read as
no change; MariaDB's defaults were read as MySQL writes them, and a
MariaDB source's table was built with `default 'NULL'`; `like 'migkit%'`
left an application's `migkitchen` out of every list.
"""
import pytest

from migkit.engines.mysql import MySQLEngine, default_shown
from tests import servers
from tests.conftest import needs_docker

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.structure.columns", "mysql")]

pair = servers.fixture("mysql")

SHAPES = """
create table shaped (
  b int not null,
  a varchar(20) collate utf8mb4_bin default 'x',
  n decimal(12,2) check (n >= 0),
  v varchar(50),
  g int generated always as (b * 2) stored,
  u datetime default current_timestamp on update current_timestamp,
  i bigint not null auto_increment,
  primary key (b, a), unique key i_u (i), key v_p (v(10)), key b_d (b desc));
create table parent (id int primary key);
create table child (id int primary key, p int,
  constraint child_p foreign key (p) references parent (id)
  on delete cascade);
create trigger child_stamp before insert on child for each row
  set new.id = new.id;
create table migkit_origin (origin varchar(10) primary key);
create table migkitchen (id int primary key);
create view v_child as select id from child;
"""


@pytest.fixture
def db(pair):
    for server in (pair.src, pair.dst):
        server.sql(f"drop database if exists `{pair.name}`;"
                   f" create database `{pair.name}`")
        server.sql(SHAPES, pair.name)
    return pair.name


@pytest.fixture
def eng(pair, db, tmp_path):
    from migkit.config import Hop
    hop = Hop(name="catalogue", engine="mysql",
              source=pair.src.endpoint(), target=pair.dst.endpoint(),
              databases=[db])
    hop.report_dir = lambda d=None: tmp_path
    got = MySQLEngine(hop)
    yield got
    got.release_kept()


# ---- pure ----------------------------------------------------------------

@pytest.mark.parametrize("mariadb,typ,default,extra,shown", [
    # MySQL 8: a literal without its quotes, an expression marked
    (False, "varchar", "new", "", "'new'"),
    (False, "varchar", "it's", "", "'it''s'"),
    (False, "int", "0", "", "0"),
    (False, "datetime", "CURRENT_TIMESTAMP", "DEFAULT_GENERATED",
     "current_timestamp"),
    (False, "datetime", "CURRENT_TIMESTAMP(6)", "DEFAULT_GENERATED",
     "current_timestamp(6)"),
    (False, "varchar", "uuid()", "DEFAULT_GENERATED", "uuid()"),
    (False, "bit", "b'101'", "", "b'101'"),
    # 5.7: the clock with nothing to mark it, read as text before
    (False, "timestamp", "CURRENT_TIMESTAMP", "", "current_timestamp"),
    (False, "varchar", "now", "", "'now'"),
    (False, "varchar", None, "", None),
    # MariaDB: literals quoted by the server, NULL said for no default
    (True, "varchar", "'new'", "", "'new'"),
    (True, "varchar", "'it''s'", "", "'it''s'"),
    (True, "varchar", "'back\\\\slash'", "", "'back\\slash'"),
    (True, "varchar", "NULL", "", None),
    (True, "int", "0", "", "0"),
    (True, "date", "curdate()", "", "curdate()"),
    (True, "timestamp", "current_timestamp()", "", "current_timestamp"),
])
def test_a_default_is_read_as_each_server_writes_it(mariadb, typ, default,
                                                    extra, shown):
    assert default_shown(mariadb, typ, default, extra) == shown


def test_what_a_column_is_besides_its_type_is_one_spelling():
    extra = MySQLEngine._column_extra
    assert extra("DEFAULT_GENERATED on update CURRENT_TIMESTAMP", "") == \
        "on update current_timestamp"
    assert extra("on update current_timestamp()", "") == \
        "on update current_timestamp"
    assert extra("STORED GENERATED", "(`b` * 2)") == \
        "stored generated as ((`b` * 2))"
    assert extra("auto_increment", "") == "auto_increment"
    assert extra("", "") == ""


def test_two_servers_spelling_one_schema_two_ways_compare_alike():
    from migkit.catalog import Catalog, Column
    eng = MySQLEngine.__new__(MySQLEngine)
    old = Catalog(columns={"t": [Column("a", "int(11)", "", False, None,
                                        "utf8_general_ci", ""),
                                 Column("z", "int(5) unsigned zerofill")]})
    new = Catalog(columns={"t": [Column("a", "int", "", False, None,
                                        "utf8mb3_general_ci", ""),
                                 Column("z", "int unsigned zerofill")]})
    a, b = eng._comparable_shape(old), eng._comparable_shape(new)
    # the display width says nothing of the value, but where zero-filled
    assert a.columns["t"][0] == b.columns["t"][0]
    assert a.fingerprint("t") != b.fingerprint("t")
    widened = Catalog(columns={"t": [Column("a", "bigint", "", False, None,
                                            "utf8mb3_general_ci", ""),
                                     Column("z", "int(5) unsigned"
                                                 " zerofill")]})
    assert eng._comparable_shape(widened).columns["t"][0] != \
        b.columns["t"][0]


def test_the_deep_column_check_compares_the_catalogue_in_one_spelling(
        tmp_path, monkeypatch):
    """A 5.7 or MariaDB source and an 8.0 target of one schema read
    alike column by column; a column the target narrows, holds NOT NULL
    or collates otherwise is named, with its narrowing - from the one
    catalogue, never a second reading of the columns in their raw text."""
    from migkit.catalog import Catalog, Column
    from migkit.config import Endpoint, Hop
    hop = Hop(name="c", engine="mysql", source=Endpoint(), target=Endpoint(),
              databases=["app"])
    hop.report_dir = lambda d=None: tmp_path
    eng = MySQLEngine(hop)

    def table(width, null, coll, display=""):
        return [Column("id", f"int{display}", "", False, None, None,
                       "auto_increment"),
                Column("v", f"varchar({width})", "", null, "'x'", coll, ""),
                Column("n", "decimal(12,2)", "", True, None, None, "")]
    shapes = {"src": Catalog(columns={
        "t": table(40, True, "utf8_general_ci", "(11)"),
        "only_src": [Column("id", "int")]}),
        "dst": Catalog(columns={"t": table(40, True, "utf8mb3_general_ci")})}
    monkeypatch.setattr(MySQLEngine, "shape_catalog",
                        lambda self, side, db: shapes[side])
    monkeypatch.setattr(MySQLEngine, "_q", lambda *a, **k: pytest.fail(
        "the columns read again"))
    cols, narrow = eng._deep_column_definitions("app")
    assert cols.scope == "app columns" and narrow.scope == "app narrowing"
    assert cols.status == "ok", cols.detail
    assert "3 columns of 1 tables" in cols.detail, cols.detail
    assert narrow.status == "ok", narrow.detail
    shapes["dst"] = Catalog(columns={"t": table(20, False, "utf8mb4_bin")})
    cols, narrow = eng._deep_column_definitions("app")
    assert cols.status == "diff", cols.detail
    said = (tmp_path / "deep-columns.diff").read_text()
    for part in ("t: column v now NOT NULL",
                 "t: column v collation utf8mb3_general_ci -> utf8mb4_bin"):
        assert part in said, said
    assert narrow.status == "diff" and "t.v" in narrow.detail, narrow.detail
    # the same again: the file of what differed goes with it
    shapes["dst"] = shapes["src"]
    assert eng._deep_column_definitions("app")[0].status == "ok"
    assert not (tmp_path / "deep-columns.diff").exists()


def test_the_texts_difference_stands_where_an_object_differs():
    """The schema-aware reading inspects tables, not a trigger's, a
    view's or a routine's body: the text's difference is made cosmetic
    only where the whole tables and the objects' definitions are the same
    too (audit 2026-10-01, SC16)."""
    from migkit.config import Endpoint, Hop
    from migkit.engines.base import Result
    eng = MySQLEngine(Hop(name="a", engine="mysql", source=Endpoint(),
                          target=Endpoint()))

    def said(tables, objects):
        return eng._atlas_authoritative([
            Result("schema", "db", "diff", "3 changed lines"),
            Result("schema", "db tables", tables, ""),
            Result("schema", "db objects", objects, ""),
            Result("schema", "db (fix DDL)", "ok", "schemas match")])
    assert said("ok", "diff")[0].status == "diff"
    assert said("diff", "ok")[0].status == "diff"
    got = said("ok", "ok")
    assert got[0].status == "ok" and "cosmetic" in got[0].detail
    # the readings that compare objects are never made cosmetic
    assert [r.status for r in said("diff", "diff")[1:3]] == ["diff",
                                                             "diff"]


# ---- on a server -----------------------------------------------------------

@needs_docker
def test_the_catalogue_holds_the_whole_definition(eng, db):
    cat = eng.catalog("src", db)
    cols = {c.name: c for c in cat.columns["shaped"]}
    assert cols["v"].declared == "varchar(50)"
    assert cols["n"].declared == "decimal(12,2)"
    assert cols["b"].nullable is False and cols["v"].nullable is True
    assert cols["a"].default == "'x'" and cols["a"].collation == \
        "utf8mb4_bin"
    assert cols["g"].extra.startswith("stored generated as (")
    assert cols["u"].default == "current_timestamp"
    assert cols["u"].extra == "on update current_timestamp"
    assert cols["i"].extra == "auto_increment"
    assert cat.keys["shaped"] == ("b", "a")
    assert eng.neutral_key("src", db, "shaped") == ["b", "a"]
    ix = {i.name: i for i in cat.indexes["shaped"]}
    assert ix["i_u"].unique and ix["i_u"].columns == ("i",)
    assert ix["v_p"].columns == ("v(10)",)
    assert ix["b_d"].columns == ("b desc",)
    fk = cat.fks["child"][0]
    assert (fk.columns, fk.ref_table, fk.ref_columns, fk.on_delete) == (
        ("p",), "parent", ("id",), "cascade")
    assert any(o.startswith("check ") for o in cat.others["shaped"])
    assert any(o.startswith("trigger child_stamp before insert")
               for o in cat.others["child"])
    # the readers a copy builds and writes a table by, from the same pass
    rules = eng.neutral_column_rules("src", db, "shaped")
    assert rules["a"]["default"] == "'x'" and rules["i"]["identity"]
    assert eng._unwritable_columns("dst", db, "shaped") == {"g"}
    assert ("v_p", False, ["v"], False) in eng.neutral_indexes(
        "src", db, "shaped")
    # a view's columns are there for the readers that compare one
    assert eng.neutral_columns("src", db, "v_child") == [("id", "int")]


@needs_docker
def test_one_pass_answers_every_table(eng, db, pair, monkeypatch):
    for n in range(20):
        pair.src.sql(f"create table t{n} (id int primary key, v text)", db)
    asked = []
    real, named = MySQLEngine._q, MySQLEngine._q_named

    def counted(self, side, sql, *a, **k):
        asked.append(sql)
        return real(self, side, sql, *a, **k)

    def counted_named(self, side, sql, *a, **k):
        asked.append(sql)
        return named(self, side, sql, *a, **k)
    monkeypatch.setattr(MySQLEngine, "_q", counted)
    monkeypatch.setattr(MySQLEngine, "_q_named", counted_named)
    for n in range(20):
        t = f"t{n}"
        assert eng.neutral_columns("src", db, t) == [("id", "int"),
                                                     ("v", "text")]
        assert eng.neutral_key("src", db, t) == ["id"]
        eng.neutral_column_rules("src", db, t)
        eng.neutral_indexes("src", db, t)
    # the catalogue's statements once, and the servers' brands once
    assert len(asked) <= 9, asked


@needs_docker
def test_migkits_own_tables_by_their_exact_names(eng, db):
    got = eng._all_tables("src", db)
    assert "migkit_origin" not in got
    assert "migkitchen" in got, got


@needs_docker
@pytest.mark.parametrize("change,said", [
    ("alter table shaped modify v varchar(80)", "varchar(50) -> varchar(80)"),
    ("alter table shaped modify v varchar(50) not null", "now NOT NULL"),
    ("alter table shaped alter a set default 'y'", "default 'x' -> 'y'"),
    ("alter table shaped modify a varchar(20) collate utf8mb4_0900_ai_ci"
     " default 'x'", "collation utf8mb4_bin -> utf8mb4_0900_ai_ci"),
    ("create index n_i on shaped (n)", "index"),
    ("alter table child drop foreign key child_p", "foreign key"),
    ("alter table shaped add constraint big check (n < 1000000)", "check"),
    ("drop trigger child_stamp; create trigger child_stamp before insert"
     " on child for each row set new.id = new.id + 0", "trigger"),
])
def test_drift_sees_each_kind_of_change(eng, db, pair, change, said):
    """Each change the source can make to a table's definition, read as
    a difference by the one comparer - every one read as none before
    except a type's name."""
    from migkit import drift
    before = drift.shape(eng, "src", db)
    pair.src.sql(change, db)
    after = drift.shape(eng, "src", db)
    got = drift.changes(before, after, "mysql")
    assert got, f"{change}: no change read"
    assert said in " ".join(got), (change, got)


@needs_docker
def test_a_change_migkit_makes_is_read_again_at_once(eng, db):
    assert eng.neutral_columns("dst", db, "parent") == [("id", "int")]
    eng.execute_ddl("dst", db, "alter table parent add column w int")
    assert eng.neutral_columns("dst", db, "parent") == [("id", "int"),
                                                        ("w", "int")]
