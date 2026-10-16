"""MySQL's deep check compares the triggers, the text a client reads for the
types that render differently, and the options each table is stored with
(P0-PARITY.verify).

The logic is held here on answers given in-process, and the catalogue
reads behind the triggers and the tables' options
(`information_schema.triggers`, `information_schema.tables`' engine, row
format and create options) on the sandbox's servers below, with the value
checks' one plan: every column of a table in each read of it, a side
(`_deep_scans`). The rendering's sample read (`cast(... as char)`) has not
run against a server, so it is not named in `PROVES`:
`capabilities.UNPROVED` says its sandbox test is owed.
"""
from migkit import hopkeys
from migkit.config import Endpoint, Hop
from tests import servers
from tests.conftest import needs_docker

PROVES = [("verify.structure.triggers", "mysql"),
          ("verify.structure.collection-options", "mysql"),
          ("verify.value.one-plan", "mysql")]


def _engine(tmp_path, answer, **kw):
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="h", engine="mysql", source=Endpoint(), target=Endpoint(),
              databases=["appdb"], workers=1, **kw)
    hop.report_dir = lambda db=None: tmp_path
    eng = MySQLEngine(hop)
    eng._q = lambda side, sql, args=None, fresh=False: answer(side, sql, args)
    return eng


# ---- triggers --------------------------------------------------------------

def _trigger(table, name, timing="BEFORE", event="INSERT", orient="ROW",
             order=1, body="SET NEW.at = NOW()", mode="STRICT_TRANS_TABLES"):
    return (table, name, timing, event, orient, order, body, mode)


def _triggers(src, dst, tables=("orders", "audit_log", "lines")):
    def answer(side, sql, args):
        if "information_schema.triggers" in sql:
            return src if side == "src" else dst
        if "information_schema.tables" in sql:
            return [(t,) for t in tables]
        return []
    return answer


def test_triggers_that_fire_alike_are_ok(tmp_path):
    held = [_trigger("orders", "trg_stamp"), _trigger("lines", "trg_audit")]
    eng = _engine(tmp_path, _triggers(held, held))
    (got,) = eng._deep_triggers("appdb", "appdb")
    assert got.status == "ok", got.detail
    assert got.category == "structure.triggers"
    assert "2 triggers" in got.detail


def test_a_trigger_that_fires_at_another_time_or_does_another_thing_differs(
        tmp_path):
    src = [_trigger("orders", "trg_stamp"), _trigger("lines", "trg_audit")]
    dst = [_trigger("orders", "trg_stamp", timing="AFTER"),
           _trigger("lines", "trg_audit",
                    body="INSERT INTO audit_log VALUES (NEW.id)")]
    (got,) = _engine(tmp_path, _triggers(src, dst)).\
        _deep_triggers("appdb", "appdb")
    assert got.status == "diff"
    assert "orders.trg_stamp: timing BEFORE -> AFTER" in got.detail
    assert "lines.trg_audit: the body differs" in got.detail
    assert (tmp_path / "deep-triggers.diff").exists()


def test_the_spacing_of_a_body_and_the_database_it_names_are_not_a_difference(
        tmp_path):
    src = [_trigger("orders", "t", body="SET NEW.a =\n  1;")]
    dst = [_trigger("orders", "t", body="SET   NEW.a = 1")]
    (got,) = _engine(tmp_path, _triggers(src, dst)).\
        _deep_triggers("appdb", "appdb")
    assert got.status == "ok", got.detail
    src = [_trigger("orders", "t", body="INSERT INTO `appdb`.`audit_log` "
                                        "VALUES (1)")]
    dst = [_trigger("orders", "t", body="INSERT INTO `other`.`audit_log` "
                                        "VALUES (1)")]
    eng = _engine(tmp_path, _triggers(src, dst))
    eng.hop.db_map = {"appdb": "other"}
    eng.hop.target_db = lambda db: "other"
    (got,) = eng._deep_triggers("appdb", "other")
    assert got.status == "ok", got.detail


def test_a_trigger_one_side_lacks_is_said_here_as_well(tmp_path):
    src = [_trigger("orders", "trg_stamp"), _trigger("lines", "trg_audit")]
    dst = [_trigger("orders", "trg_stamp"), _trigger("lines", "trg_extra")]
    (got,) = _engine(tmp_path, _triggers(src, dst)).\
        _deep_triggers("appdb", "appdb")
    assert got.status == "diff"
    assert "on the source only: lines.trg_audit" in got.detail
    assert "on the target only: lines.trg_extra" in got.detail


def test_a_mode_it_was_made_under_is_a_note_not_a_mismatch(tmp_path):
    src = [_trigger("orders", "t", mode="STRICT_TRANS_TABLES")]
    dst = [_trigger("orders", "t", mode="ANSI_QUOTES")]
    (got,) = _engine(tmp_path, _triggers(src, dst)).\
        _deep_triggers("appdb", "appdb")
    assert got.status == "warn" and "SQL mode" in got.detail


def test_the_triggers_of_a_table_the_hop_leaves_out_are_not_compared(
        tmp_path):
    src = [_trigger("orders", "t"), _trigger("audit_log", "t_log")]
    dst = [_trigger("orders", "t"), _trigger("audit_log", "t_log",
                                             timing="AFTER")]
    eng = _engine(tmp_path, _triggers(src, dst), exclude=["audit_log"])
    (got,) = eng._deep_triggers("appdb", "appdb")
    assert got.status == "ok", got.detail


def test_an_online_schema_changes_trigger_is_not_a_trigger_of_the_application(
        tmp_path):
    src = [_trigger("orders", "t")]
    dst = [_trigger("orders", "t"), _trigger("orders", "pt_osc_appdb_orders_"
                                                       "ins")]
    (got,) = _engine(tmp_path, _triggers(src, dst)).\
        _deep_triggers("appdb", "appdb")
    assert got.status == "ok", got.detail


# ---- rendering -------------------------------------------------------------

def _rendering(src_text, dst_text, moved=None):
    """A decimal column `price` of `items` (key `id`); the source's row 1
    renders `src_text`, the target's `dst_text`. `moved`: what the source
    renders on the second look."""
    looks = []

    def answer(side, sql, args):
        if "information_schema.columns" in sql:
            return [("items", "price", "decimal")]
        if "key_column_usage" in sql:
            return [("id",)]
        if "cast(`price` as char)" in sql:
            if side == "dst":
                return [(1, dst_text)]
            looks.append(1)
            return [(1, src_text if len(looks) == 1 or moved is None
                     else moved)]
        return []
    return answer


def test_a_value_both_sides_render_alike_is_ok(tmp_path):
    got = _engine(tmp_path, _rendering("1.50", "1.50"))._deep_render(
        "appdb", "appdb")
    assert got.status == "ok" and "1 columns" in got.detail, got
    assert got.category == "value.rendering"


def test_a_value_one_side_renders_another_way_differs(tmp_path):
    got = _engine(tmp_path, _rendering("1.50", "1.5"))._deep_render(
        "appdb", "appdb")
    assert got.status == "diff"
    assert "items.price: renders differently" in got.detail
    assert "'1.50'" in got.detail and "'1.5'" in got.detail


def test_a_row_that_moved_on_the_source_meanwhile_is_not_counted(tmp_path):
    got = _engine(tmp_path, _rendering("1.50", "1.5", moved="1.5"))\
        ._deep_render("appdb", "appdb")
    assert got.status == "ok", got.detail


def test_a_table_without_a_key_is_not_sampled(tmp_path):
    def answer(side, sql, args):
        if "information_schema.columns" in sql:
            return [("items", "price", "decimal")]
        return []
    got = _engine(tmp_path, answer)._deep_render("appdb", "appdb")
    assert got.status == "ok" and "no such columns" in got.detail


def test_a_target_that_cannot_be_read_is_said_not_passed(tmp_path):
    def answer(side, sql, args):
        if side == "dst" and "cast(" in sql:
            raise RuntimeError("gone away")
        return _rendering("1.50", "1.50")(side, sql, args)
    got = _engine(tmp_path, answer)._deep_render("appdb", "appdb")
    assert got.status == "diff" and "unreadable on target" in got.detail


# ---- table options ---------------------------------------------------------

def _options(src, dst):
    def answer(side, sql, args):
        if "create_options" in sql:
            return src if side == "src" else dst
        if "information_schema.tables" in sql:
            return [(r[0],) for r in (src if side == "src" else dst)]
        return []
    return answer


def test_tables_stored_alike_are_ok(tmp_path):
    held = [("orders", "InnoDB", "Dynamic", ""),
            ("lines", "InnoDB", "Dynamic", "partitioned")]
    got = _engine(tmp_path, _options(held, held))._deep_table_options(
        "appdb", "appdb")
    assert got.status == "ok" and "2 tables" in got.detail
    assert got.category == "structure.collection-options"


def test_a_table_in_another_engine_or_row_format_differs(tmp_path):
    src = [("orders", "InnoDB", "Dynamic", ""),
           ("lines", "InnoDB", "Dynamic", "")]
    dst = [("orders", "MyISAM", "Fixed", ""),
           ("lines", "InnoDB", "Compact", "")]
    got = _engine(tmp_path, _options(src, dst))._deep_table_options(
        "appdb", "appdb")
    assert got.status == "diff"
    assert "orders: engine innodb -> myisam" in got.detail
    assert "lines: row format dynamic -> compact" in got.detail
    assert got.category == "structure.collection-options"


def test_other_options_are_a_note_and_partitioning_is_the_partition_checks(
        tmp_path):
    src = [("orders", "InnoDB", "Compressed",
            "row_format=COMPRESSED KEY_BLOCK_SIZE=8 partitioned")]
    dst = [("orders", "InnoDB", "Compressed",
            "row_format=COMPRESSED KEY_BLOCK_SIZE=4")]
    got = _engine(tmp_path, _options(src, dst))._deep_table_options(
        "appdb", "appdb")
    assert got.status == "warn"
    assert "KEY_BLOCK_SIZE=8" in got.detail and "KEY_BLOCK_SIZE=4" in \
        got.detail
    assert "partitioned" not in got.detail


def test_a_table_the_hop_leaves_out_is_not_compared(tmp_path):
    src = [("orders", "InnoDB", "Dynamic", ""),
           ("audit_log", "InnoDB", "Dynamic", "")]
    dst = [("orders", "InnoDB", "Dynamic", ""),
           ("audit_log", "MyISAM", "Dynamic", "")]
    got = _engine(tmp_path, _options(src, dst), exclude=["audit_log"])\
        ._deep_table_options("appdb", "appdb")
    assert got.status == "ok", got.detail


def test_the_deep_check_runs_all_three(tmp_path):
    """Wired into `check_deep`'s list, in the one place every deep check
    is read."""
    import inspect
    from migkit.engines.mysql import MySQLEngine
    src = inspect.getsource(MySQLEngine.check_deep)
    for name in ("_deep_triggers", "_deep_render", "_deep_table_options"):
        assert f"self.{name}(db, ddb)" in src, name


# ---- on the sandbox's servers ------------------------------------------------

my = servers.fixture("mysql")


def _served(space, tmp_path):
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="deep", engine="mysql", source=space.src.endpoint(),
              target=space.dst.endpoint(), databases=[space.name],
              options=hopkeys.normalised(hopkeys.HOP_OPTIONS, {}))
    hop.report_dir = lambda db=None, _p=tmp_path: _p
    eng = MySQLEngine(hop)
    return eng, space.name, eng._d("dst", space.name)


def _on_both(space, db, *stmts):
    for side in (space.src, space.dst):
        for s in stmts:
            side.sql(s, db)


@needs_docker
def test_mysql_holds_each_trigger_to_the_targets_on_a_server(my, tmp_path):
    eng, db, ddb = _served(my, tmp_path)
    _on_both(my, db, f"drop table if exists `{db}`.t",
             f"create table `{db}`.t (id int primary key, b varchar(20))",
             f"create trigger `{db}`.trg before insert on `{db}`.t for each"
             " row set new.b = upper(new.b)")
    got = eng._deep_triggers(db, ddb)
    assert [r.status for r in got] == ["ok"], [r.detail for r in got]
    assert "1 triggers" in got[0].detail
    # the target's fires after, and does something else
    my.dst.sql(f"drop trigger `{db}`.trg", db)
    my.dst.sql(f"create trigger `{db}`.trg after insert on `{db}`.t for"
               " each row set @seen = new.id", db)
    got = eng._deep_triggers(db, ddb)
    assert got[0].status == "diff", got[0].detail
    assert "timing BEFORE -> AFTER" in got[0].detail
    assert "the body differs" in got[0].detail
    # one the target lacks is named, never read as clean
    my.dst.sql(f"drop trigger `{db}`.trg", db)
    got = eng._deep_triggers(db, ddb)
    assert got[0].status == "diff" and "on the source only" in got[0].detail


@needs_docker
def test_mysql_holds_each_tables_storage_to_the_targets_on_a_server(
        my, tmp_path):
    eng, db, ddb = _served(my, tmp_path)
    _on_both(my, db, f"drop table if exists `{db}`.t",
             f"create table `{db}`.t (id int primary key) engine=InnoDB"
             " row_format=dynamic")
    got = eng._deep_table_options(db, ddb)
    assert got.status == "ok", got.detail
    assert got.category == "structure.collection-options"
    # an option that changes its size and speed only is a note
    my.dst.sql(f"alter table `{db}`.t stats_persistent=0", db)
    got = eng._deep_table_options(db, ddb)
    assert got.status == "warn", got.detail
    assert "stats_persistent" in got.detail.lower()
    # an engine that takes no transaction is a difference
    my.dst.sql(f"alter table `{db}`.t engine=MyISAM", db)
    got = eng._deep_table_options(db, ddb)
    assert got.status == "diff", got.detail
    assert "engine innodb -> myisam" in got.detail


@needs_docker
def test_mysql_reads_every_column_of_a_table_in_one_read_a_side_on_a_server(
        my, tmp_path):
    eng, db, ddb = _served(my, tmp_path)
    _on_both(my, db, f"drop table if exists `{db}`.t",
             f"create table `{db}`.t (id int primary key, a varchar(20),"
             " b text, f double) character set utf8mb4")
    my.src.sql(f"insert into `{db}`.t values (1, 'x', null, 1.5),"
               " (2, '', 'y', 2.5), (3, null, 'z', null)", db)
    # the target holds one empty text as NULL
    my.dst.sql(f"insert into `{db}`.t values (1, 'x', null, 1.5),"
               " (2, null, 'y', 2.5), (3, null, 'z', null)", db)
    asked, real = [], eng._q

    def q(side, sql, args=None, fresh=False):
        asked.append((side, sql))
        return real(side, sql, args, fresh)
    eng._q = q
    scans = eng._deep_scans(db)
    assert scans is not None
    reads = [(side, sql) for side, sql in asked
             if "is null)" in sql and "`t`" in sql]
    assert {side for side, _ in reads} == {"src", "dst"}
    # each read of the table carries every column's aggregates, never one
    # column a read: as many reads a side as the table has key ranges
    for _, sql in reads:
        assert all(f"`{c}` is null" in sql for c in ("a", "b")), sql
        assert "count(`f`)" in sql, sql
    by_side = {s: sum(1 for side, _ in reads if side == s)
               for s in ("src", "dst")}
    assert by_side["src"] == by_side["dst"] == len(
        {sql for side, sql in reads if side == "src"}), by_side
    def said(got):
        return [(r.status, r.detail) for r in
                (got if isinstance(got, list) else [got])]
    null = said(eng._deep_nullempty(db, ddb, scans))
    assert any(status == "diff" for status, _ in null), null
    # a check given the plan reads the table no more
    before = len(asked)
    flt = said(eng._deep_float(db, ddb, scans))
    assert flt and all(status == "ok" for status, _ in flt), flt
    assert not [s for _, s in asked[before:] if "`t`" in s and "sum(" in s]
