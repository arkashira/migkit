"""PostgreSQL's views read from the catalogue and carried to a target in the
order they depend on each other, then held to what they answer
(P0-PARITY.all; `PostgresEngine.neutral_views`, `storedcode.convert`,
`storedcode.views`).

A view is carried as the query it was defined with, written in the
target's SQL; the views the hop leaves out and the server's own are not
read, a view made from another comes after the one it reads whatever their
names sort as, and a view carried is proved by the rows it answers on both
sides - not by the statement that made it.
"""
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("schema.views", "postgres")]


def _sql(pg_pair, side, sql):
    got = psql(pg_pair[side], sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _pair(pg_pair, tmp_path, **hop):
    from migkit.engines.hetero import HeteroEngine
    ep = lambda side: Endpoint(host="127.0.0.1", port=pg_pair[side],  # noqa
                               user="postgres", password="test")
    h = Hop(name="views", engine="hetero",
            options={"source_engine": "postgres",
                     "target_engine": "postgres"},
            source=ep("src"), target=ep("dst"), databases=["postgres"],
            **hop)
    h.report_dir = lambda db=None: tmp_path
    return HeteroEngine(h)


def _build(pg_pair):
    """The same rows on both sides, the views on the source only."""
    for side in ("src", "dst"):
        _sql(pg_pair, side, "create table public.orders (id int primary key,"
                            " amount numeric(10,2), cust text);"
                            " insert into public.orders values (1, 5.00,"
                            " 'Ann'), (2, 20.50, 'bob'), (3, 10.00,"
                            " 'Celine'), (4, 99.99, null)")
    _sql(pg_pair, "src",
         "create view public.big_orders as select id, amount from"
         " public.orders where amount > 10;"
         # sorts before the view it reads: the order they are made in has
         # to come from what each reads, not from their names
         " create view public.a_big_ids as select id from public.big_orders;"
         " create view public.hidden as select id from public.orders")


@needs_docker
def test_the_views_the_hop_takes_are_read_and_the_servers_own_are_not(
        pg_pair, tmp_path):
    _build(pg_pair)
    eng = _pair(pg_pair, tmp_path, exclude=["public.hidden"])
    got = dict(eng.src_engine.neutral_views("src", "postgres"))
    assert sorted(got) == ["public.a_big_ids", "public.big_orders"], got
    # the query it was defined with, not a name
    assert "big_orders" in got["public.a_big_ids"], got
    assert "pg_catalog" not in " ".join(got) and \
        "information_schema" not in " ".join(got)


@needs_docker
def test_views_are_carried_in_order_and_proved_by_their_rows(pg_pair,
                                                            tmp_path):
    from migkit.storedcode.views import prove_views
    _build(pg_pair)
    eng = _pair(pg_pair, tmp_path, exclude=["public.hidden"])
    code = eng.converted_code("postgres", propose=False, climb=False)
    made = [n for n, sql, _ in code if sql.startswith("create view")]
    assert made == ["big_orders", "a_big_ids"], code
    # not applied yet: the views are missing on the target, and said so
    before = {v.said: v.status for v in prove_views(eng, "postgres")}
    assert before == {"view big_orders": "missing",
                      "view a_big_ids": "missing"}, before
    for _, sql, _ in code:
        eng.dst_engine.neutral_create_code("dst", "postgres", sql)
    after = {v.said: v.status for v in prove_views(eng, "postgres")}
    assert after == {"view big_orders": "ok", "view a_big_ids": "ok"}, after
    # the view the hop leaves out is not carried
    assert _sql(pg_pair, "dst", "select count(*) from pg_views where"
                                " viewname = 'hidden'") == "0"


@needs_docker
def test_a_carried_view_that_answers_otherwise_is_a_difference(pg_pair,
                                                               tmp_path):
    from migkit.storedcode.views import prove_views
    _build(pg_pair)
    eng = _pair(pg_pair, tmp_path, exclude=["public.hidden"])
    for _, sql, _ in eng.converted_code("postgres", propose=False,
                                        climb=False):
        eng.dst_engine.neutral_create_code("dst", "postgres", sql)
    assert {v.said: v.status for v in prove_views(eng, "postgres")} == {
        "view big_orders": "ok", "view a_big_ids": "ok"}
    _sql(pg_pair, "dst", "create or replace view public.big_orders as select"
                         " id, amount from public.orders where amount > 50")
    got = {v.said: v.status for v in prove_views(eng, "postgres")}
    assert got["view big_orders"] == "diff", got
