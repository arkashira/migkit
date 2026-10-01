"""What the DBA must do, written for them (backlog R21).

Each side is asked what the hop's goal needs of it, and what is missing
comes back as the exact statements that grant it and nothing more - in
the words of whoever runs the server: `rds_replication` and a parameter
group on RDS, a cluster parameter group on Aurora, a database flag on
Cloud SQL, a server parameter on Azure, `ALTER SYSTEM` on a server run by
its owner. These drive the requirement code with the servers' answers
given; `test_init_is_the_guide.py` asks real ones.
"""
import pytest

from migkit import dba
from migkit.clouds import Where
from migkit.config import build_hop

SEP = "\x1f"


class Fake:
    """An engine whose server gives the answers it is handed: by (side,
    words in the question)."""

    def __init__(self, canon, answers):
        self.CANON_ENGINE = canon
        self.answers = answers

    def databases(self):
        return ["shop"]

    def _repl_name(self):
        return "migkit_h"

    def _answer(self, side, sql):
        for (s, needle), got in self.answers.items():
            if s == side and needle in sql:
                return got
        raise AssertionError(f"{side}: {sql}")

    def _psql(self, side, db, sql):
        return self._answer(side, sql)

    def _q(self, side, sql, args=None):
        return self._answer(side, sql)


def _pg(super_=False):
    who = f"app{SEP}{int(super_)}{SEP}0{SEP}0{SEP}160004"
    return Fake("postgres", {
        ("src", "rolname = current_user"): who,
        ("dst", "rolname = current_user"): who,
        ("src", "has_schema_privilege(n.oid"):
            "" if super_ else
            f"public{SEP}orders{SEP}r{SEP}1\npublic{SEP}orders_id_seq{SEP}"
            f"S{SEP}1\nbilling{SEP}invoices{SEP}r{SEP}0",
        ("src", "show wal_level"): "logical" if super_ else "replica",
        ("src", "max_replication_slots"): f"10{SEP}10",
        ("src", "pg_publication"): "0",
        # tables the account does not own, which it may not publish
        ("src", "pg_has_role(current_user, c.relowner"): "1",
        ("src", "rds_replication"): "0",
        ("src", "rds_superuser"): "0",
        ("dst", "rds_superuser"): "0",
        ("dst", "has_database_privilege(datname"): f"1{SEP}0",
        ("dst", "has_schema_privilege('public'"): "1" if super_ else "0",
        ("dst", "pg_create_subscription"): "1" if super_ else "0",
        ("src", "from pg_extension"): "plpgsql" + ("" if super_ else
                                                   "\npostgis"),
        ("dst", "pg_available_extensions"): f"plpgsql{SEP}1.0\npostgis{SEP}",
    })


PG = {"source": "postgres://app@db1/shop", "target": "postgres://app@db2/shop"}


def _script(hop, eng, where, side="source", goal="cutover"):
    found = dba.needs(hop, eng, goal, {"source": where, "target": where})
    return found, dba.script(hop, eng, side, found, where)


def test_a_server_run_by_its_owner():
    hop = build_hop("h", PG)
    found, src = _script(hop, _pg(), Where())
    for line in ("grant usage on schema billing to app;",
                 "grant select on public.orders to app;",
                 "grant select on sequence public.orders_id_seq to app;",
                 "grant select on billing.invoices to app;",
                 "alter system set wal_level = 'logical';",
                 "alter role app with replication;",
                 "create publication migkit_h for all tables;"):
        assert line in src, (line, src)
    # a grant for a schema it may already use is not asked for again
    assert "grant usage on schema public" not in src
    _, dst = _script(hop, _pg(), Where(), "target")
    for line in ("grant usage, create on schema public to app;",
                 "grant pg_create_subscription to app;",
                 "create extension if not exists postgis;"):
        assert line in dst, (line, dst)
    assert all(n.follow for n in found if "log" in n.what or "slot" in
               n.what)


def test_rds_and_aurora_take_a_role_and_a_parameter_group():
    hop = build_hop("h", PG)
    _, rds = _script(hop, _pg(), Where("aws", "rds"))
    assert "grant rds_replication to app;" in rds
    assert "alter role app with replication" not in rds
    assert "DB parameter group: rds.logical_replication = 1" in rds
    assert "modify-db-parameter-group" in rds
    assert "alter system" not in rds
    _, aurora = _script(hop, _pg(), Where("aws", "aurora"))
    assert "DB cluster parameter group: rds.logical_replication = 1" in \
        aurora
    assert "modify-db-cluster-parameter-group" in aurora


def test_cloud_sql_azure_tencent_and_alibaba_in_their_own_words():
    hop = build_hop("h", PG)
    _, gcp = _script(hop, _pg(), Where("gcp", "cloud-sql"))
    assert "--database-flags=cloudsql.logical_decoding=on" in gcp
    _, azure = _script(hop, _pg(), Where("azure", "flexible"))
    assert ("az postgres flexible-server parameter set --resource-group"
            " <group> --server-name <server> --name wal_level --value"
            " logical") in azure
    _, azure_dst = _script(hop, _pg(), Where("azure", "flexible"), "target")
    assert "azure.extensions must list postgis" in azure_dst
    _, tencent = _script(hop, _pg(), Where("tencent", "tencentdb"))
    assert "TencentDB console" in tencent and "wal_level = logical" in \
        tencent
    _, ali = _script(hop, _pg(), Where("alibaba", "apsaradb"))
    assert "ModifyParameter" in ali and '"wal_level":"logical"' in ali


def test_nothing_missing_is_no_script():
    """The requirement code must be able to say "all there" too."""
    hop = build_hop("h", PG)
    found, src = _script(hop, _pg(super_=True), Where())
    assert src is None, src
    assert not dba.missing(found)
    assert found and all(n.ok for n in found), [
        (n.what, n.ok) for n in found]


def test_a_copy_asks_nothing_of_the_log():
    hop = build_hop("h", PG)
    found, src = _script(hop, _pg(), Where(), goal="copy")
    assert "wal_level" not in src and "replication" not in src
    assert "grant select on public.orders to app;" in src


def _my(log_bin="ON", fmt="MIXED", grants=("GRANT USAGE ON *.* TO `app`@`%`",)):
    return Fake("mysql", {
        ("src", "substring_index(current_user()"): [("app", "%")],
        ("dst", "substring_index(current_user()"): [("app", "%")],
        ("src", "show grants"): [(g,) for g in grants],
        ("dst", "show grants"): [(g,) for g in grants],
        ("src", "current_role()"): [("NONE",)],
        ("dst", "current_role()"): [("NONE",)],
        ("dst", "information_schema.schemata"): [],
        ("src", "rds_show_configuration"): [
            ("binlog retention hours", None, "")],
        ("src", "variable_name in ('log_bin'"): [
            ("log_bin", log_bin), ("binlog_format", fmt),
            ("binlog_row_image", "FULL"),
            ("binlog_expire_logs_seconds", "3600")],
    })


MY = {"source": "mysql://app@m1/shop", "target": "mysql://app@m2/shop"}


def test_mysql_by_its_owner_and_on_rds():
    hop = build_hop("h", MY)
    _, src = _script(hop, _my(), Where())
    for line in ("grant select, show view on `shop`.* to 'app'@'%';",
                 "set persist binlog_format = 'ROW';",
                 "set persist binlog_expire_logs_seconds = '86400';",
                 "grant replication slave, replication client on *.* to"
                 " 'app'@'%';"):
        assert line in src, (line, src)
    _, dst = _script(hop, _my(), Where(), "target")
    assert "create database if not exists `shop`;" in dst
    assert "grant select, insert, update, delete, create, alter" in dst
    _, rds = _script(hop, _my(), Where("aws", "rds"))
    assert "call mysql.rds_set_configuration('binlog retention hours'," \
        " 24);" in rds
    assert "DB parameter group: binlog_format = ROW" in rds
    assert "set persist" not in rds


def test_mysql_grants_are_read_through_all_privileges():
    hop = build_hop("h", MY)
    found, src = _script(hop, _my(fmt="ROW", grants=(
        "GRANT ALL PRIVILEGES ON *.* TO `app`@`%` WITH GRANT OPTION",)),
        Where())
    lack = [n.what for n in dba.missing(found)]
    assert lack == ["a log kept a day or more"], lack
    assert dba.missing(found)[0].now == "kept 3600s"


def test_a_binary_log_that_is_off():
    hop = build_hop("h", MY)
    _, src = _script(hop, _my(log_bin="OFF"), Where("gcp", "cloud-sql"))
    assert "--enable-bin-log" in src
    _, own = _script(hop, _my(log_bin="OFF"), Where())
    assert "configuration file: log_bin = ON, then restart it" in own


def test_the_script_is_written_and_taken_away(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    hop = build_hop("h", PG)
    eng = _pg()
    found = dba.needs(hop, eng, "cutover")
    got = dba.write(hop, eng, found)
    assert set(got) == {"source", "target"}
    assert got["source"].name == "for-the-dba-source.sql"
    assert "wal_level" in got["source"].read_text()
    done = dba.needs(hop, _pg(super_=True), "cutover")
    assert dba.write(hop, eng, done) == {}
    assert not got["source"].exists() and not got["target"].exists()


@pytest.mark.parametrize("engine", sorted(dba.ASK))
def test_every_engine_asked_has_a_script_form(engine):
    assert engine in dba.SCRIPT
