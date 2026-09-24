"""`assess` names the key-less tables whose updates a replica cannot apply.

Without a key, logical replication finds the row to update or delete on the
target by comparing every column. A column whose type has no `=` - `json`,
`point`, `xml`, an array of them - cannot be compared, and the apply worker
stops. DTS fails such tables outright. Measured here first: a subscription
applying an UPDATE to a key-less table with a `json` column.
"""
import subprocess
import time

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _hop(pg_pair):
    return Hop(name="um", engine="postgres",
               source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                               user="postgres", password="test"),
               target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                               user="postgres", password="test"),
               databases=["postgres"])


def test_the_replica_really_stops_on_such_a_table(pg_pair):
    src_ip = subprocess.run(
        ["docker", "inspect", "-f",
         "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
         "migkit-test-pg-src"], capture_output=True, text=True).stdout.strip()
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table public.doc (body json, n int)")
    psql(pg_pair["src"], "alter table public.doc replica identity full;"
                         " insert into public.doc values ('{\"a\": 1}', 1);"
                         " create publication um_pub for table public.doc")
    try:
        got = psql(pg_pair["dst"],
                   "create subscription um_sub connection"
                   f" 'host={src_ip} port=5432 dbname=postgres user=postgres"
                   " password=test' publication um_pub")
        assert got.returncode == 0, got.stderr
        for _ in range(30):
            if psql(pg_pair["dst"], "select count(*) from public.doc"
                    ).stdout.strip() == "1":
                break
            time.sleep(1)
        psql(pg_pair["src"], "update public.doc set n = 2")
        time.sleep(6)
        logs = subprocess.run(["docker", "logs", "migkit-test-pg-dst"],
                              capture_output=True, text=True)
        text = logs.stdout + logs.stderr
        assert "could not identify an equality operator for type json" \
            in text, text[-800:]
        assert psql(pg_pair["dst"], "select n from public.doc"
                    ).stdout.strip() == "1"
    finally:
        psql(pg_pair["dst"], "drop subscription if exists um_sub")
        psql(pg_pair["src"], "drop publication if exists um_pub")


def test_assess_names_it_with_the_way_out(pg_pair):
    from migkit.engines.postgres import PostgresEngine
    psql(pg_pair["src"], "create table public.doc (body json, n int);"
                         " create table public.keyed (id int primary key,"
                         " body json);"
                         " create table public.plain (n int, t text)")
    got = [i for i in PostgresEngine(_hop(pg_pair))._unmatchable_rows()]
    scopes = {i["scope"]: i for i in got}
    assert "postgres.public.doc" in scopes, got
    assert "body json" in scopes["postgres.public.doc"]["detail"]
    assert "replica identity using index" in \
        scopes["postgres.public.doc"]["detail"]
    # a key makes it matchable; a key-less table of comparable types is fine
    assert "postgres.public.keyed" not in scopes, got
    assert "postgres.public.plain" not in scopes, got
