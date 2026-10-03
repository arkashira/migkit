"""On PostgreSQL: a hop that carries no deletes is checked, repaired and
verified as what it is, and a scope that leaves out what its tables need
is said before the move.

Measured before (backlog P0-SCOPE.6f): a target holding the rows the
source deleted, under a hop whose `mapping.ops` carries no deletes,
checked `different` every time; `sync` deleted those rows; `watch
--verify --delta` never moved its slot on. And a hop taking `orders` but
not `customers` moved rows that point at parents the target never got,
with nothing said until the foreign key failed on it.
"""
import pytest

from migkit import verdict
from tests.conftest import needs_docker, psql

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("scope.ops", "postgres"), ("schema.foreign-keys", "postgres")]

pytestmark = needs_docker

NO_DELETES = {"public.d": "insert, update"}


def _engine(tmp_path, ports, ops=None, **kw):
    import migkit.config as cfg
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    cfg.REPORTS = tmp_path / "reports"
    hop = Hop(name="keeps", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=ports["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=ports["dst"],
                              user="postgres", password="test"),
              databases=["postgres"],
              mapping={"ops": ops} if ops else {}, **kw)
    return PostgresEngine(hop)


def _seed(ports, rows=50):
    for p in (ports["src"], ports["dst"]):
        psql(p, "create table d (id bigint primary key, v text);"
                f" insert into d select g, 'v'||g from"
                f" generate_series(1, {rows}) g;")


def _check(eng):
    rs = [r.__dict__ for r in eng.check_data("postgres")]
    return verdict.decide(rs, eng.hop), rs


def test_rows_the_source_deleted_are_kept_not_different(pg_pair, tmp_path):
    _seed(pg_pair)
    psql(pg_pair["src"], "delete from d where id in (7, 8)")
    got, rs = _check(_engine(tmp_path, pg_pair, NO_DELETES))
    assert got.status == "same", (got, rs)
    kept = [r for r in rs if r.get("kept_by_hop")]
    assert kept and "2 rows the source deleted - deletes are not carried" \
        in kept[0]["kept_by_hop"], rs
    assert "rows counted: public.d missing=0 extra=2 changed=0" in \
        kept[0]["detail"]
    # the same target under a hop that carries every change differs
    got, _ = _check(_engine(tmp_path, pg_pair))
    assert got.status == "different"


def test_a_row_missing_beside_them_is_a_difference(pg_pair, tmp_path):
    _seed(pg_pair)
    psql(pg_pair["src"], "delete from d where id = 7;"
                         " insert into d values (900, 'new')")
    got, rs = _check(_engine(tmp_path, pg_pair, NO_DELETES))
    assert got.status == "different", rs
    data = [r for r in rs if r["check"] == "data" and r["scope"] ==
            "postgres"][0]
    assert "public.d: kept by the hop: 1 row the source deleted" in \
        data["detail"]
    assert "1 row missing on the target is a difference" in data["detail"]


def test_the_repair_carries_what_is_missing_and_keeps_what_is_kept(
        pg_pair, tmp_path):
    _seed(pg_pair)
    psql(pg_pair["src"], "delete from d where id = 7;"
                         " insert into d values (900, 'new')")
    eng = _engine(tmp_path, pg_pair, NO_DELETES)
    _check(eng)
    rows = [a for a in eng.repair_plan("postgres", "rows")
            if a.kind == "rows"]
    assert len(rows) == 1 and "left as they are" in rows[0].note, rows
    eng.apply("postgres", rows[0])
    there = psql(pg_pair["dst"], "select string_agg(id::text, ',' order by"
                                 " id) from d where id in (7, 900)").stdout
    assert there.strip() == "7,900", there
    got, _ = _check(eng)
    assert got.status == "same"
    # where every row apart is one the hop keeps, nothing is planned
    assert not [a for a in eng.repair_plan("postgres", "rows")
                if a.kind == "rows"]


def test_a_delta_cycle_moves_on_past_rows_the_hop_keeps(pg_pair, tmp_path):
    _seed(pg_pair)
    eng = _engine(tmp_path, pg_pair, NO_DELETES)
    try:
        assert "created" in eng.delta_verify("postgres")[0].detail
        psql(pg_pair["src"], "delete from d where id = 5")
        r = eng.delta_verify("postgres")
        assert r[0].status == "ok" and "slot advanced" in r[0].detail, \
            [(x.scope, x.status, x.detail) for x in r]
        table = [x for x in r if x.scope.endswith("public.d")][0]
        assert table.status == "warn" and table.kept_by_hop
        assert verdict.decide([x.__dict__ for x in r], eng.hop).status \
            == "same"
        psql(pg_pair["src"], "insert into d values (901, 'new')")
        r = eng.delta_verify("postgres")
        assert r[0].status == "diff" and "NOT advanced" in r[0].detail
    finally:
        eng.delta_teardown("postgres")


SCHEMA = """
create table customers (id int primary key);
create table orders (id int primary key,
                     customer_id int references customers (id));
insert into customers values (1), (2);
insert into orders values (1, 1), (2, 1), (3, 2), (4, null);
create table audit_log (id serial primary key, what text);
create function log_order() returns trigger language plpgsql as $$
begin insert into audit_log (what) values ('order'); return new; end $$;
create trigger orders_audit after insert on orders
  for each row execute function log_order();
create function count_customers() returns bigint language sql as
  $$ select count(*) from customers $$;
create view order_totals as select c.id, count(*) as n
  from customers c join orders o on o.customer_id = c.id group by c.id;
create sequence shared_seq owned by audit_log.id;
create table notes (id int primary key default nextval('shared_seq'));
create table events (id int, at date) partition by range (at);
create table events_2025 partition of events
  for values from ('2025-01-01') to ('2026-01-01');
create table events_2026 partition of events
  for values from ('2026-01-01') to ('2027-01-01');
"""

LEAVES = ["postgres.public.customers", "postgres.public.audit_log",
          "postgres.public.events_2026"]


def _closure(items):
    return [i for i in items if i["item"] == "dependencies"]


def test_what_the_taken_tables_need_is_said_before_the_move(pg_pair,
                                                           tmp_path):
    psql(pg_pair["src"], SCHEMA)
    eng = _engine(tmp_path, pg_pair, exclude=list(LEAVES))
    items = eng._include_items()
    said = {i["detail"].split(":", 2)[1].strip(): i for i in
            _closure(items) if i["level"] == "warn"}
    fk = said["foreign keys across the scope"]["detail"]
    assert "public.orders (customer_id) -> public.customers - 3 of its" \
           " rows hold a key into it" in fk, fk
    assert "take postgres.public.customers out of exclude" in fk
    assert "closure: accept" in fk
    assert "trigger orders_audit on public.orders names public.audit_log" \
        in said["triggers across the scope"]["detail"]
    assert "public.count_customers names public.customers" in \
        said["routines across the scope"]["detail"]
    assert "view public.order_totals reads public.customers" in \
        said["views across the scope"]["detail"]
    assert "public.notes draws from public.shared_seq, which" \
           " public.audit_log owns" in \
        said["sequences across the scope"]["detail"]
    assert "public.events is taken and its partition public.events_2026" \
           " is not" in said["partitions across the scope"]["detail"]
    # and an exclude naming a partition is said to leave it out
    scope = " | ".join(i["detail"] for i in items if i["item"] == "scope")
    assert "exclude 'postgres.public.events_2026' leaves out 1" in scope


def test_a_closure_the_hop_accepts_is_said_as_accepted(pg_pair, tmp_path):
    psql(pg_pair["src"], SCHEMA)
    eng = _engine(tmp_path, pg_pair, exclude=list(LEAVES),
                  options={"closure": "accept"})
    got = _closure(eng._include_items())
    assert got and all(i["level"] == "pass" for i in got), got
    assert any("accepted (closure: accept)" in i["detail"] for i in got)


def test_a_scope_that_takes_what_it_needs_says_nothing_crosses(pg_pair,
                                                              tmp_path):
    psql(pg_pair["src"], SCHEMA)
    eng = _engine(tmp_path, pg_pair,
                  exclude=["postgres.public.events*"])
    got = _closure(eng._include_items())
    assert [i["level"] for i in got] == ["pass"], got
    assert "nothing the hop takes needs a table it leaves out" in \
        got[0]["detail"]


def test_a_misspelt_entry_is_offered_the_real_name(pg_pair, tmp_path):
    psql(pg_pair["src"], SCHEMA)
    eng = _engine(tmp_path, pg_pair,
                  include=["postgres.public.ordres", "postgres.public.*"])
    scope = [i for i in eng._include_items() if i["item"] == "scope"]
    warn = " | ".join(i["detail"] for i in scope if i["level"] == "warn")
    assert "'postgres.public.ordres'" in warn and \
        "did you mean postgres.public.orders?" in warn, warn
    took = " | ".join(i["detail"] for i in scope if i["level"] == "pass")
    assert "tables made later that it names" in took, took


@pytest.fixture(autouse=True)
def _no_slot_left(pg_pair):
    yield
    psql(pg_pair["src"], "select pg_drop_replication_slot(slot_name) from"
                         " pg_replication_slots where not active")
