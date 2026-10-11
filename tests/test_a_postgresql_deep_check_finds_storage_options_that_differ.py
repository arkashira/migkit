"""PostgreSQL's deep check holds how the target keeps each table to how the
source does (P0-PARITY.verify; `verdict.options_result`).

A move that makes the target's tables from their columns leaves every
storage option at the server's default: a table made `UNLOGGED` on the
source, a fill factor, the autovacuum settings of a table and of its TOAST
table, how a column's long values are stored and compressed. One of them is
a risk to the data - a target table the server does not log is empty after
a crash and absent from its standbys - and is a difference; the others
change how the table performs and are a warning. Only the tables the hop
takes are read, and a renamed one is paired with its target.
"""
from migkit import verdict
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.structure.collection-options", "postgres")]


# ---- the comparison, with no server ----------------------------------------

def test_options_are_paired_by_name_and_an_unset_one_reads_as_the_default():
    got = verdict.option_differences(
        "public.t", {"persistence": "logged", "parameter fillfactor": "70"},
        {"persistence": "logged", "parameter parallel_workers": "2"})
    assert got == [("public.t", "parameter fillfactor", "70", "default"),
                   ("public.t", "parameter parallel_workers", "default",
                    "2")]
    assert verdict.option_differences("t", {"a": "x"}, {"a": "x"}) == []


def test_only_what_puts_the_data_at_risk_is_a_difference():
    from migkit.engines.postgres import PostgresEngine
    unsafe = PostgresEngine._storage_unsafe
    ok = verdict.options_result("db", [])
    assert (ok.status, ok.category) == ("ok", "structure.collection-options")
    slow = verdict.options_result(
        "db", [("t", "parameter fillfactor", "70", "default")], unsafe)
    assert slow.status == "warn" and "performance, not data" in slow.detail
    # unlogged on the target alone loses rows in a crash; the other way
    # round is only slower
    lost = verdict.options_result(
        "db", [("t", "persistence", "logged", "unlogged"),
               ("t", "parameter fillfactor", "70", "default")], unsafe)
    assert lost.status == "diff" and lost.detail.index("persistence") < \
        lost.detail.index("fillfactor")
    assert "put the target's data at risk" in lost.detail
    safer = verdict.options_result(
        "db", [("t", "persistence", "unlogged", "logged")], unsafe)
    assert safer.status == "warn"


class _Told(object):
    """A PostgreSQL engine whose catalogue answers are said."""

    def __init__(self, tmp_path, tables, columns, **hop):
        from migkit.engines.postgres import PostgresEngine
        ep = Endpoint(host="127.0.0.1", port=1, user="u", password="x")
        h = Hop(name="stor", engine="postgres", source=ep, target=ep,
                databases=["d"], **hop)
        h.report_dir = lambda db=None: tmp_path
        self.eng = PostgresEngine(h)
        self.tables, self.columns = tables, columns
        self.eng._psql = self._psql

    def _psql(self, side, db, sql, *a, **k):
        asked = (self.eng.STORAGE_TABLES, self.eng.STORAGE_COLUMNS)
        assert sql in asked, sql
        return "\n".join(self.tables[side] if sql == asked[0]
                         else self.columns[side])


def _line(*parts):
    return "\x1f".join(parts)


def test_the_catalogue_is_read_into_one_table_a_side_of_options(tmp_path):
    told = _Told(tmp_path, {
        "src": [_line("public.a", "logged", "heap",
                      "fillfactor=70,toast.autovacuum_enabled=false")],
        "dst": []}, {
        "src": [_line("public.a", "body", "e", "l")], "dst": []})
    assert told.eng._storage_of("src", "d") == {"public.a": {
        "persistence": "logged", "access method": "heap",
        "parameter fillfactor": "70",
        "parameter toast.autovacuum_enabled": "false",
        "column body storage": "external",
        "column body compression": "lz4"}}


def test_the_hops_tables_alone_are_read_and_a_renamed_one_is_paired(tmp_path):
    same = _line("public.kept", "logged", "heap", "")
    told = _Told(tmp_path, {
        "src": [_line("public.orders", "logged", "heap", ""),
                _line("public.left_out", "unlogged", "heap", ""), same],
        "dst": [_line("public.orders_v2", "unlogged", "heap", ""),
                _line("public.left_out", "logged", "heap", ""), same]}, {
        "src": [], "dst": []},
        exclude=["public.left_out"],
        mapping={"tables": {"d.public.orders": "orders_v2"}})
    got = told.eng._storage_options("d")
    assert got.status == "diff", got
    assert got.category == "structure.collection-options"
    assert "public.orders: persistence src=logged dst=unlogged" in got.detail
    assert "left_out" not in got.detail
    assert (tmp_path / "deep-storage.diff").read_text().strip() == \
        "public.orders: persistence src=logged dst=unlogged"


def test_matching_tables_are_ok_and_a_stale_report_is_taken_away(tmp_path):
    (tmp_path / "deep-storage.diff").write_text("old\n")
    row = _line("public.a", "logged", "heap", "fillfactor=70")
    told = _Told(tmp_path, {"src": [row], "dst": [row]},
                 {"src": [], "dst": []})
    got = told.eng._storage_options("d")
    assert got.status == "ok", got
    assert not (tmp_path / "deep-storage.diff").exists()


def test_a_server_that_cannot_be_read_is_an_error_not_an_ok(tmp_path):
    told = _Told(tmp_path, {}, {})

    def refuse(*a, **k):
        raise RuntimeError("connection refused\nno route")
    told.eng._psql = refuse
    got = told.eng._storage_options("d")
    assert got.status == "error" and "no route" in got.detail


# ---- on two servers ---------------------------------------------------------

def _engine(pg_pair, tmp_path, **hop):
    from migkit.engines.postgres import PostgresEngine
    ep = lambda side: Endpoint(host="127.0.0.1", port=pg_pair[side],  # noqa
                               user="postgres", password="test")
    h = Hop(name="storage", engine="postgres", source=ep("src"),
            target=ep("dst"), databases=["postgres"], **hop)
    h.report_dir = lambda db=None: tmp_path
    return PostgresEngine(h)


def _both(pg_pair, sql):
    for side in ("src", "dst"):
        got = psql(pg_pair[side], sql)
        assert got.returncode == 0, got.stderr


def _scope(results, name):
    return [r for r in results if r.scope == f"postgres {name}"]


@needs_docker
def test_a_table_the_target_does_not_log_is_a_difference(pg_pair, tmp_path):
    _both(pg_pair, "drop schema if exists stor cascade; create schema stor;"
                   " create table stor.orders (id int primary key, body text)")
    psql(pg_pair["dst"], "alter table stor.orders set unlogged")
    got = _engine(pg_pair, tmp_path)._storage_options("postgres")
    assert got.status == "diff", got
    assert "stor.orders: persistence src=logged dst=unlogged" in got.detail
    assert got.category == "structure.collection-options"
    psql(pg_pair["dst"], "alter table stor.orders set logged")
    got = _engine(pg_pair, tmp_path)._storage_options("postgres")
    assert got.status == "ok", got


@needs_docker
def test_options_that_change_only_speed_are_a_warning(pg_pair, tmp_path):
    _both(pg_pair, "drop schema if exists stor cascade; create schema stor;"
                   " create table stor.events (id int primary key,"
                   " payload text)")
    psql(pg_pair["src"],
         "alter table stor.events set (fillfactor = 70,"
         " toast.autovacuum_enabled = false);"
         " alter table stor.events alter column payload set storage"
         " external")
    got = _engine(pg_pair, tmp_path)._storage_options("postgres")
    assert got.status == "warn", got
    for said in ("parameter fillfactor src=70 dst=default",
                 "parameter toast.autovacuum_enabled src=false",
                 "column payload storage src=external dst=extended"):
        assert said in got.detail, got.detail
    # the deep check carries it among its findings, under its category
    ours = _scope(_engine(pg_pair, tmp_path).check_deep("postgres"),
                  "storage")
    assert [r.category for r in ours] == ["structure.collection-options"]
    psql(pg_pair["dst"],
         "alter table stor.events set (fillfactor = 70,"
         " toast.autovacuum_enabled = false);"
         " alter table stor.events alter column payload set storage"
         " external")
    got = _engine(pg_pair, tmp_path)._storage_options("postgres")
    assert got.status == "ok", got
    _both(pg_pair, "drop schema stor cascade")
