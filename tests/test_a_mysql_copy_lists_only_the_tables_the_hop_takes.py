"""The tables a MySQL copy lists are the hop's: an excluded one, and an
online schema change's working table, are not among them
(P0-PARITY.scope)."""
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker
from tests.mysql_pair import my, my_pair  # noqa: F401 - a fixture

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("scope.copy", "mysql")]


@needs_docker
def test_the_copy_lists_the_hops_tables_only(my_pair):
    from migkit.engines.mysql import MySQLEngine
    my("src", "drop database if exists listed; create database listed;"
              " create table listed.a (id int primary key);"
              " create table listed.b (id int primary key);"
              " create table listed.c (id int primary key);"
              " create table listed._a_gho (id int primary key)")

    def listed(**kw):
        ep = Endpoint(host="127.0.0.1", port=my_pair["src"], user="root",
                      password="test")
        eng = MySQLEngine(Hop(name="list", engine="mysql", source=ep,
                              target=ep, databases=["listed"], **kw))
        return [t for _, t in eng.list_move_tables("listed")]
    assert listed() == ["a", "b", "c"]
    assert listed(exclude=["listed.b"]) == ["a", "c"]
    assert listed(include=["listed.a", "listed.c"]) == ["a", "c"]
