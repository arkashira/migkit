"""A transaction written on the target itself, while it follows the source
as its replica, is found before the cutover (DTS's GTID-set compare).

A write made on the replica - by hand, by a job pointed at the wrong
server - is carried into production by the cutover and replayed nowhere
by a way back. Where the target replicates from this source by GTID, the
two executed sets are subtracted by the target's own server: what the
target executed and the source never did is named, exactly.

Pure: the servers' answers are given in-process.
"""
from types import SimpleNamespace

from migkit.config import Endpoint, Hop
from migkit.engines.mysql import MySQLEngine

SRC = "11111111-1111-1111-1111-111111111111"


def _eng(errant="", behind="", source_uuid=SRC, brand="mysql"):
    eng = MySQLEngine(Hop(name="h", engine="mysql", source=Endpoint(),
                          target=Endpoint()))
    eng._brands = lambda: (SimpleNamespace(name=brand),
                           SimpleNamespace(name=brand))
    eng._gtid_state = lambda b: (True, "gtid ON")
    eng._q_named = lambda side, sql: [{"Source_UUID": source_uuid}]

    def q(side, sql, args=None):
        if "server_uuid" in sql:
            return [(SRC,)]
        if "gtid_subtract" in sql:
            return [(errant, behind)]
        if "gtid_executed" in sql:
            return [(f"{SRC}:1-100",)]
        return []
    eng._q = q
    return eng


def test_a_transaction_only_the_target_executed_is_a_difference():
    got = _eng(errant="22222222-2222-2222-2222-222222222222:1-3") \
        ._errant_transactions("shop")
    assert got.status == "diff" and "22222222" in got.detail


def test_a_replica_holding_only_the_sources_transactions_is_clean():
    got = _eng(behind=f"{SRC}:99-100")._errant_transactions("shop")
    assert got.status == "ok" and "still to apply" in got.detail


def test_a_target_that_is_no_replica_of_this_source_is_not_asked():
    assert _eng(source_uuid="33333333-3333-3333-3333-333333333333") \
        ._errant_transactions("shop") is None
    assert _eng(brand="mariadb")._errant_transactions("shop") is None
