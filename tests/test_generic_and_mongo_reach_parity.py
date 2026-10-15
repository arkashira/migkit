"""The generic engine's scope and a refused grant on it and on MongoDB.

The generic engine asks the one scope answer (`Engine.in_scope`) of every
table it lists - the hop's own, or where the hop lists none every base
table the source's standard catalogue holds; reads the hop's row filter in
its copy and its comparison alike; and writes and reads the target under
the hop's name rules. A refusal for want of a grant stops as the grant and
the statement the operator runs, on both engines, never as an empty answer.
"""
import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.enterprise_connectors import MissingGrant

PROVES = [("scope.listings", "generic"), ("scope.deep", "generic"),
          ("scope.copy", "generic"), ("scope.row-filter", "generic"),
          ("scope.names", "generic")]


class Refused(Exception):
    def __init__(self, msg, code=None):
        super().__init__(msg)
        self.code = code


class _Dialect:
    @staticmethod
    def quote(name):
        return f'"{name}"'


class _Conn:
    """A driver connection answering `query` from a table of answers."""

    dialect = _Dialect()

    def __init__(self, answer=(), error=None):
        self.answer, self.error, self.asked = list(answer), error, []

    def query(self, sql, kind):
        self.asked.append(sql)
        if self.error:
            raise self.error
        return list(self.answer)


def _engine(tables=(), exclude=(), mapping=None, src="u", dst="w"):
    from migkit.engines.generic import GenericEngine
    hop = Hop(name="g", engine="generic",
              source=Endpoint(host="", port=0, user="", password="",
                              options={"url": f"postgresql://{src}@10.0.0.1/a"
                                       if src else "postgresql://10.0.0.1/a"}),
              target=Endpoint(host="", port=0, user="", password="",
                              options={"url": f"postgresql://{dst}@10.0.0.2/a"}),
              exclude=list(exclude), mapping=mapping or {},
              options={"tables": list(tables)} if tables else {})
    return GenericEngine(hop)


def _wire(eng, **conns):
    eng._connect = lambda side: conns[side]
    eng._from = lambda conn, table: table
    return eng


def test_without_a_list_the_catalogue_is_probed_and_the_scope_asked():
    eng = _wire(_engine(exclude=["tmp_*"]), src=_Conn([
        ("public", "orders"), ("public", "tmp_load"),
        ("information_schema", "tables"), ("pg_catalog", "pg_class"),
        ("sales", "people")]))
    assert eng._tables() == ["public.orders", "sales.people"]
    assert eng.list_move_tables("-") == [("public", "orders"),
                                         ("sales", "people")]
    # probed once: a second listing asks the server nothing more
    eng._tables()
    assert len(eng._connect("src").asked) == 1


def test_a_listed_table_is_never_widened_by_the_catalogue():
    eng = _wire(_engine(["orders"]), src=_Conn([("public", "audit")]))
    assert eng._tables() == ["orders"]
    assert eng._connect("src").asked == []


def test_a_server_without_a_standard_catalogue_says_why_and_stops():
    eng = _wire(_engine(), src=_Conn(error=Refused(
        "no such table: information_schema.tables")))
    with pytest.raises(SystemExit) as e:
        eng._tables()
    assert "information_schema.tables" in str(e.value)
    assert "options.tables" in str(e.value)


def test_a_refused_catalogue_names_the_grant_to_run():
    eng = _wire(_engine(src="reader"), src=_Conn(error=Refused(
        "permission denied for table tables")))
    with pytest.raises(MissingGrant) as e:
        eng._tables()
    assert ("GRANT SELECT ON information_schema.tables TO reader"
            in str(e.value))


def test_the_row_filter_is_read_by_the_comparison_and_the_row_reader():
    eng = _wire(_engine(["orders"],
                        mapping={"where": {"orders": "region = 'eu'"},
                                 "tables": {"orders": "members"}}),
                src=_Conn([(1,)]), dst=_Conn([(1,)]))
    cmd = eng._reladiff_cmd("orders", [])
    assert cmd[cmd.index("-w") + 1] == "region = 'eu'"
    eng._side_rows("src", "orders", ["id"])
    eng._side_rows("dst", "orders", ["id"])
    assert eng._connect("src").asked == [
        'SELECT "id" FROM orders WHERE region = \'eu\'']
    assert eng._connect("dst").asked == [
        'SELECT "id" FROM members WHERE region = \'eu\'']


def test_without_a_filter_every_row_is_read():
    eng = _wire(_engine(["orders"]), src=_Conn([(1,)]))
    assert "-w" not in eng._reladiff_cmd("orders", [])
    eng._side_rows("src", "orders", ["id"])
    assert eng._connect("src").asked == ['SELECT "id" FROM orders']


def test_the_target_is_named_by_the_hops_rules_and_the_source_as_listed():
    eng = _engine(["orders", "people"],
                  mapping={"tables": {"orders": "members"}})
    assert eng._on("dst", "orders") == "members"
    assert eng._on("src", "orders") == "orders"
    assert eng._on("dst", "people") == "people"


def test_a_refused_read_on_the_target_names_its_grant_on_the_target_name():
    eng = _wire(_engine(["orders"], mapping={"tables": {"orders": "members"}},
                        dst="writer"),
                dst=_Conn(error=Refused("permission denied for table members",
                                        code="42501")))
    with pytest.raises(MissingGrant) as e:
        eng._side_rows("dst", "orders", ["id"])
    assert "GRANT SELECT ON members TO writer" in str(e.value)


def test_a_read_failing_for_another_reason_is_not_called_a_grant():
    eng = _wire(_engine(["orders"]),
                src=_Conn(error=Refused("connection reset by peer")))
    with pytest.raises(SystemExit) as e:
        eng._side_rows("src", "orders", ["id"])
    assert not isinstance(e.value, MissingGrant)
    assert "cannot read orders" in str(e.value)


def test_an_address_without_a_user_names_the_grant_without_a_statement():
    eng = _wire(_engine(src=None), src=_Conn(error=Refused(
        "permission denied for table tables")))
    with pytest.raises(MissingGrant) as e:
        eng._tables()
    assert "SELECT on information_schema.tables" in str(e.value)
    assert "run:" not in str(e.value)


def _mongo(user="reader", options=None):
    from migkit.engines.mongodb import MongoEngine
    hop = Hop(name="m", engine="mongodb",
              source=Endpoint(host="10.0.0.1", port=27017, user=user,
                              password="CHANGE_ME", options=options or {}),
              target=Endpoint(host="10.0.0.2", port=27017, user="writer",
                              password="CHANGE_ME"),
              db_map={"shop": "shop2"})
    return MongoEngine(hop)


def test_mongodb_names_the_role_on_the_database_the_probe_was_refused_on():
    eng = _mongo()
    assert eng.grant_statement("collStats", "shop") == (
        'db.getSiblingDB("admin").grantRolesToUser("reader",'
        ' [{role: "read", db: "shop"}])')
    # the target's database is the mapped one, its user the target's
    assert eng.grant_statement("find", "shop", side="dst") == (
        'db.getSiblingDB("admin").grantRolesToUser("writer",'
        ' [{role: "read", db: "shop2"}])')


def test_mongodb_cluster_actions_are_granted_in_admin_and_the_oplog_in_local():
    eng = _mongo(options={"authSource": "users"})
    assert eng.grant_statement("listDatabases") == (
        'db.getSiblingDB("users").grantRolesToUser("reader",'
        ' [{role: "readAnyDatabase", db: "admin"}])')
    assert '{role: "read", db: "local"}' in eng.grant_statement(
        "reading the oplog")
    assert eng.grant_statement("collStats") is None
    assert eng.grant_statement("an action nobody knows", "shop") is None
    assert _mongo(user="").grant_statement("collStats", "shop") is None


def test_mongodb_refusal_stops_as_the_grant_and_its_statement():
    eng = _mongo()
    with pytest.raises(MissingGrant) as e:
        eng._refused("collStats", Refused("not authorized on shop", 13),
                     db="shop")
    said = str(e.value)
    assert "the collStats action on the collection" in said
    assert 'run: db.getSiblingDB("admin").grantRolesToUser("reader"' in said


def test_mongodb_anything_but_a_refusal_is_left_to_the_caller():
    assert _mongo()._refused("collStats", Refused("network timeout", 89),
                             db="shop") is None
