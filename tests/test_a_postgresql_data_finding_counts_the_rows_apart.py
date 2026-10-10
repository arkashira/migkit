"""PostgreSQL's data check names the table a finding is about, says how
each table it compared came out and counts the rows apart one by one
(P0-PARITY.verify)."""
from tests.conftest import needs_docker, psql

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("verify.rows-apart", "postgres"), ("verify.tables", "postgres"),
          ("verify.table", "postgres")]


def _engine(pg_pair, tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    ep = lambda side: Endpoint(host="127.0.0.1", port=pg_pair[side],  # noqa
                               user="postgres", password="test")
    hop = Hop(name="apart", engine="postgres", source=ep("src"),
              target=ep("dst"), databases=["postgres"])
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


@needs_docker
def test_the_rows_apart_are_counted_and_the_tables_said(pg_pair, tmp_path):
    ddl = ("drop table if exists apart, same;"
           " create table apart (id int primary key, v int);"
           " create table same (id int primary key, v int)")
    for side in ("src", "dst"):
        psql(pg_pair[side], ddl)
        psql(pg_pair[side], "insert into same values (1, 1)")
    psql(pg_pair["src"], "insert into apart values (1, 1), (2, 2), (3, 3),"
                         " (4, 4)")
    # the target: 1 equal, 2 changed, 3 missing, 4 equal, 9 extra
    psql(pg_pair["dst"], "insert into apart values (1, 1), (2, 99), (4, 4),"
                         " (9, 9)")
    eng = _engine(pg_pair, tmp_path)
    one = eng.check_data("postgres", table="public.apart")
    assert len(one) == 1 and one[0].status == "diff", one
    assert one[0].table == "public.apart"
    assert one[0].rows_apart == {"public.apart": {
        "missing": 1, "extra": 1, "changed": 1, "exact": True}}, one[0]
    whole = [r for r in eng.check_data("postgres")
             if r.status == "diff" and r.tables]
    assert len(whole) == 1, whole
    assert whole[0].tables["public.apart"] == "diff", whole[0].tables
    assert whole[0].tables["public.same"] == "ok", whole[0].tables
    assert whole[0].rows_apart["public.apart"] == {
        "missing": 1, "extra": 1, "changed": 1, "exact": True}
