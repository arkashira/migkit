"""A PostgreSQL delta verify reads both sides as the check reads them.

Measured at ee1032d: with `mapping.where: {orders: "region = 'TH'"}`, an
update of a US order read the source's raw table and found the row
missing on the target every cycle - the slot never advanced, the WAL it
held grew, the cutover's last check stopped on it, and `sync --kind rows
--apply` copied the US row onto the target. A table the hop renames was
read on the target under the source's name. And a slot that had stepped
past an excluded table's changes was gone on from after `include` took
the table in. The keys are now read through the row filter, the target's
table under its own name, and a slot taken under another scope is taken
again.
"""
from tests.conftest import needs_docker, psql

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("scope.names", "postgres")]

pytestmark = needs_docker


def _engine(tmp_path, ports, **kw):
    import migkit.config as cfg
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    cfg.REPORTS = tmp_path / "reports"
    hop = Hop(name="dscope", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=ports["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=ports["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], **kw)
    return PostgresEngine(hop)


def _by_scope(results):
    return {r.scope: r for r in results}


def test_a_changed_row_the_filter_leaves_behind_is_not_missing(pg_pair,
                                                               tmp_path):
    src, dst = pg_pair["src"], pg_pair["dst"]
    eng = _engine(tmp_path, pg_pair, mapping={
        "where": {"public.orders": "region = 'TH'"}})
    psql(src, "create table orders (id bigint primary key, region text);"
              " insert into orders values (1, 'TH'), (2, 'US')")
    psql(dst, "create table orders (id bigint primary key, region text);"
              " insert into orders values (1, 'TH')")
    try:
        assert "created" in eng.delta_verify("postgres")[0].detail
        psql(src, "update orders set region = 'CA' where id = 2;"
                  " insert into orders values (3, 'US')")
        got = eng.delta_verify("postgres")
        assert got[0].status == "ok", [r.__dict__ for r in got]
        assert "slot advanced" in got[0].detail, got[0].detail
        # a row the filter takes is still compared, and still found
        psql(src, "update orders set region = 'TH' where id = 2")
        got = _by_scope(eng.delta_verify("postgres"))
        assert got["postgres"].status == "diff", got["postgres"].detail
        assert "missing=1" in got["postgres.public.orders"].detail
        assert "NOT advanced" in got["postgres"].detail
    finally:
        eng.delta_teardown("postgres")


def test_a_renamed_table_is_read_under_its_name_on_the_target(pg_pair,
                                                              tmp_path):
    src, dst = pg_pair["src"], pg_pair["dst"]
    eng = _engine(tmp_path, pg_pair, mapping={
        "tables": {"postgres.public.orders": "orders_v2"}})
    psql(src, "create table orders (id bigint primary key, v text);"
              " insert into orders values (1, 'a'), (2, 'b')")
    psql(dst, "create table orders_v2 (id bigint primary key, v text);"
              " insert into orders_v2 values (1, 'a'), (2, 'b')")
    try:
        eng.delta_verify("postgres")
        psql(src, "update orders set v = 'A' where id = 1")
        psql(dst, "update orders_v2 set v = 'A' where id = 1")
        got = eng.delta_verify("postgres")
        assert got[0].status == "ok", [r.__dict__ for r in got]
        # and a change the target did not take is found there
        psql(src, "update orders set v = 'B' where id = 2")
        got = _by_scope(eng.delta_verify("postgres"))
        assert "changed=1" in got["postgres.public.orders"].detail, \
            got["postgres.public.orders"].detail
    finally:
        eng.delta_teardown("postgres")


def test_a_slot_taken_under_another_scope_is_taken_again(pg_pair, tmp_path):
    """The slot stepped past the changes of the table the hop left out;
    once `include` takes it, going on from that slot would never compare
    them. It is dropped and made again, said, and the cutover compares
    every table until a whole check under the new scope stands."""
    from migkit import cutover
    src, dst = pg_pair["src"], pg_pair["dst"]
    eng = _engine(tmp_path, pg_pair, include=["public.orders"])
    for p in (src, dst):
        psql(p, "create table orders (id bigint primary key, v text);"
                " create table lines (id bigint primary key, v text)")
    try:
        assert "created" in eng.delta_in_scope("postgres")[0].detail
        psql(src, "insert into lines values (1, 'only on the source')")
        got = eng.delta_in_scope("postgres")
        assert got[0].status == "ok", [r.__dict__ for r in got]
        assert "postgres.public.lines" not in _by_scope(got)
        assert cutover._has_delta(eng.hop, eng, "postgres")
        eng.hop.include = ["public.orders", "public.lines"]
        assert not cutover._has_delta(eng.hop, eng, "postgres")
        got = eng.delta_in_scope("postgres")
        assert "created" in got[0].detail, got[0].detail
        assert got[-1].status == "warn" and "scope changed" in \
            got[-1].detail, got[-1].detail
        assert not cutover._has_delta(eng.hop, eng, "postgres")
        # the row the old slot stepped past is found by a whole check
        data = eng.check_data("postgres")
        assert any(r.status == "diff" for r in data), \
            [r.__dict__ for r in data]
    finally:
        eng.delta_teardown("postgres")
