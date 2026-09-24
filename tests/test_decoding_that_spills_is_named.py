"""A change stream decoding transactions too large for its memory is named,
with the setting that ends it (backlog 6).

What does not fit in `logical_decoding_work_mem` is written to the source's
disk and read back. Measured on PostgreSQL 16: a 3.3 MB transaction decoded
at 64kB spilled `3,460,000` bytes; at 4MB it spilled nothing, at 2MB it
did. `assess` reads the server's own count and names the next power of two
at least the average spilled transaction, never less than twice what is set.
"""
from tests.conftest import needs_docker, psql

pytestmark = needs_docker


def _eng(pg_pair, tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="sp", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def test_a_spill_is_named_with_the_value_to_set(pg_pair, tmp_path):
    src = pg_pair["src"]
    psql(src, "select pg_create_logical_replication_slot('spill_probe',"
              " 'test_decoding')")
    try:
        psql(src, "create table public.t (id int primary key, v text)")
        eng = _eng(pg_pair, tmp_path)
        psql(src, "select pg_stat_reset_replication_slot('spill_probe')")
        assert eng._decoding_spills()[0]["level"] == "pass"
        psql(src, "insert into public.t select g, repeat('x', 40)"
                  " from generate_series(1, 20000) g")
        # decoded under a small memory, the way a stream on a server set
        # low would
        got = psql(src, "set logical_decoding_work_mem = '64kB';"
                        " select count(*) from pg_logical_slot_get_changes("
                        "'spill_probe', null, null)")
        assert got.returncode == 0, got.stderr
        row = eng._decoding_spills()[0]
        assert row["level"] == "warn", row
        assert "for spill_probe spilled" in row["detail"], row
        # the server's own setting is the default 64MB here
        assert "set logical_decoding_work_mem = 128MB" in row["detail"], row
    finally:
        psql(src, "select pg_drop_replication_slot('spill_probe')")
