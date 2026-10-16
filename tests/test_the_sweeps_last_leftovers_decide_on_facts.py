"""The AUD-Z sweep's last leftovers (row 132): a counter's head-room and
the servers' own principals decided on facts (`decide.sequence_headroom`,
`decide.system_principals`) where a process-wide figure and a fixed list
stood; the view's roles read one way (`ui.role` through `web.roles`); a
drilldown's file named from its table one way (`base.drill_file`)."""
import types

import pytest

from migkit import decide as d
from migkit import ui, users
from migkit.engines import base, postgres
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("sequence_headroom", "asked", "holds"),
    ("sequence_headroom", "asked", "refuses"),
    ("sequence_headroom", "measured", "holds"),
    ("sequence_headroom", "measured", "refuses"),
    ("sequence_headroom", "none", "holds"),
    ("system_principals", "catalogue", "holds"),
    ("system_principals", "catalogue", "refuses"),
    ("system_principals", "listed", "holds"),
]


# ---- the counter's head-room --------------------------------------------------

def test_the_runs_own_margin_is_the_head_room():
    with watched() as seen:
        n, got = d.sequence_headroom({"margin": 1000, "rate": 50, "gap": 4})
    assert (n, got.path) == (1000, "asked")
    assert branch(seen, "asked") == [HOLDS]


def test_the_measured_rate_over_the_restart_gap_is_the_head_room():
    with watched() as seen:
        n, got = d.sequence_headroom({"margin": None, "rate": 12.5,
                                      "gap": 3.1})
    # 38.75 rows land in the gap: room for 39, never 38
    assert (n, got.path) == (39, "measured")
    assert branch(seen, "asked") == [REFUSES]
    assert "no margin" in why(seen, "asked")
    assert branch(seen, "measured") == [HOLDS]


def test_nothing_measured_reseeds_to_the_highest_id(monkeypatch):
    # the old process-wide figure moves nothing any more
    monkeypatch.setenv("SEQUENCE_BUFFER", "500")
    with watched() as seen:
        n, got = d.sequence_headroom({"margin": None, "rate": 0, "gap": 30})
    assert (n, got.path) == (0, "none")
    assert branch(seen, "measured") == [REFUSES]
    assert branch(seen, "none") == [HOLDS]
    assert postgres._seq_buffer() == 0
    assert postgres._seq_buffer(rate=2, gap=10) == 20
    token = postgres.SEQUENCE_MARGIN.set(7)
    try:
        assert postgres._seq_buffer(rate=2, gap=10) == 7
    finally:
        postgres.SEQUENCE_MARGIN.reset(token)


# ---- the servers' own principals ----------------------------------------------

def test_a_server_with_a_catalogue_tells_its_own_principals_by_it():
    with watched() as seen:
        sql = users.system_principals_sql("postgres", "g.who")
    assert branch(seen, "catalogue") == [HOLDS]
    assert sql.startswith("(g.who in (select rolname::text from pg_roles"
                          " where oid < 16384) or ")
    # the providers' names stay beside it: they are ordinary roles there
    assert "g.who like 'rds%'" in sql and "'postgres'" in sql


def test_a_server_without_one_reads_the_known_names_alone():
    with watched() as seen:
        sql = users.system_principals_sql("mssql", "name")
    assert branch(seen, "catalogue") == [REFUSES]
    assert "keeps no catalogue" in why(seen, "catalogue")
    assert branch(seen, "listed") == [HOLDS]
    assert "pg_roles" not in sql and "name in (" in sql


@pytest.mark.parametrize("column", ["x; drop table t", "a.b.c", "1x",
                                    "who) or (true", ""])
def test_only_a_column_reference_is_written_into_the_condition(column):
    with pytest.raises(ValueError):
        users.system_principals_sql("postgres", column)


def test_the_operators_ignored_roles_are_read_through_the_registry(
        monkeypatch):
    from migkit import envkeys
    monkeypatch.setenv("MONGO_IGNORE_USERS", " app_ro, ,batch ")
    assert envkeys.items("MONGO_IGNORE_USERS") == ["app_ro", "batch"]
    assert users._mongo_sysuser("batch") and not users._mongo_sysuser("app")


# ---- the view's roles ---------------------------------------------------------

def _hop(access):
    return types.SimpleNamespace(options={"access": access} if access
                                 else {}, name="shop")


def test_the_console_view_reads_roles_as_the_web_view_does():
    hop = _hop({"operator": ["Ann@Example.com"], "viewer": ["*@example.com"],
                "admin": ["group:dba"]})
    # any case, as providers differ in how they write a name
    assert ui.role(hop, "ann@example.com") == "operator"
    assert ui.role(hop, "bob@example.com") == "viewer"
    # `group:` is a group, never a name a person may sign in with
    assert ui.role(hop, "group:dba") is None
    assert ui.role(hop, "eve@example.org") is None
    assert ui.role(_hop({"approver": ["cy@example.com"]}),
                   "cy@example.com") == "operator"
    assert ui.role(_hop(None), "anyone@example.org") == "operator"
    assert ui.role(hop, None) == "operator"


# ---- the drilldown's file names -----------------------------------------------

@pytest.mark.parametrize("table", ["orders", "public.orders", "a/b",
                                   "../../etc/passwd", "line\nbreak",
                                   "100%", "a%2Fb", "Ünï cödé"])
def test_a_tables_file_stays_in_the_directory_and_reads_back(table, tmp_path):
    name = base.drill_file(table, "missing")
    path = tmp_path / name
    assert path.parent == tmp_path and "/" not in name and "\n" not in name
    assert base.drill_table(name) == (table, "missing")


def test_two_tables_never_share_a_file_and_plain_names_keep_theirs():
    assert base.drill_file("orders", "extra") == "data-orders.extra"
    assert base.drill_file("a/b", "x") != base.drill_file("a%2Fb", "x")
    assert base.drill_table("report.html") is None
    assert base.drill_table("data-.missing") is None
