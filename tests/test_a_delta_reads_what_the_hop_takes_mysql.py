"""A MySQL delta verify reads both sides as the check reads them.

Measured at ee1032d: the keys the binlog named were asked of the raw
table on both sides, so under `mapping.where` a changed row the filter
leaves behind was missing on the target every cycle and the position
never moved; a table the hop renames was read on the target under the
source's name. Now through the row filter - one holding a `%`, which the
keys' arguments must not take for one of theirs - and the target's
table under its own name.
"""
import pytest

from tests import mysql_pair
from tests.mysql_pair import my, my_pair  # noqa: F401 - a fixture

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("scope.names", "mysql"), ("verify.delta", "mysql")]


@pytest.fixture
def reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


def _by_scope(results):
    return {r.scope: r for r in results}


@pytest.mark.docker
def test_a_changed_row_the_filter_leaves_behind_is_not_missing(my_pair,
                                                               reports):
    my("src", "drop database if exists dsc; create database dsc; use dsc;"
              " create table orders (id int primary key, region"
              " varchar(8)); insert into orders values (1, 'TH'),"
              " (2, 'US');")
    my("dst", "drop database if exists dsc; create database dsc; use dsc;"
              " create table orders (id int primary key, region"
              " varchar(8)); insert into orders values (1, 'TH');")
    eng = mysql_pair.engine("dsc")
    eng.hop.mapping = {"where": {"orders": "region like 'T%'"}}
    first = eng.delta_verify("dsc")
    assert first[0].status == "ok", first[0].detail
    my("src", "use dsc; update orders set region = 'CA' where id = 2;"
              " insert into orders values (3, 'US');")
    got = _by_scope(eng.delta_verify("dsc"))
    assert got["dsc"].status == "ok", {k: r.detail for k, r in got.items()}
    # a row the filter takes is still asked, and still found
    my("src", "use dsc; update orders set region = 'TH' where id = 2;")
    got = _by_scope(eng.delta_verify("dsc"))
    assert got["dsc.orders"].status == "diff", got["dsc.orders"].detail
    assert "missing=1" in got["dsc.orders"].detail


@pytest.mark.docker
def test_a_renamed_table_is_read_under_its_name_on_the_target(my_pair,
                                                              reports):
    my("src", "drop database if exists dsr; create database dsr; use dsr;"
              " create table orders (id int primary key, v varchar(8));"
              " insert into orders values (1, 'a'), (2, 'b');")
    my("dst", "drop database if exists dsr; create database dsr; use dsr;"
              " create table orders_v2 (id int primary key, v varchar(8));"
              " insert into orders_v2 values (1, 'a'), (2, 'b');")
    eng = mysql_pair.engine("dsr")
    eng.hop.mapping = {"tables": {"dsr.orders": "orders_v2"}}
    eng.delta_verify("dsr")
    my("src", "use dsr; update orders set v = 'A' where id = 1;")
    my("dst", "use dsr; update orders_v2 set v = 'A' where id = 1;")
    got = _by_scope(eng.delta_verify("dsr"))
    assert got["dsr"].status == "ok", {k: r.detail for k, r in got.items()}
    my("src", "use dsr; update orders set v = 'B' where id = 2;")
    got = _by_scope(eng.delta_verify("dsr"))
    assert "changed=1" in got["dsr.orders"].detail, \
        got["dsr.orders"].detail
