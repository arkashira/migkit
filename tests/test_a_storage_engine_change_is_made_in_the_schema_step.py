"""A change of storage engine (backlog P0-SCENARIOS): MyISAM, Aria,
MEMORY and MariaDB's own engines into InnoDB, made in the schema step,
with what changes said and checked before the move - rows a foreign key
would now refuse, numbering per group, a MERGE table's borrowed rows - and
the engine proved after it.

The definition rewrite, the hop's words and each engine's traits run
without a server; the rest runs MariaDB 11.4 into MySQL 8.4 (which has no
Aria engine at all).
"""
import subprocess
import time

import pytest
from click.testing import CliRunner

from migkit import tableengine
from tests.conftest import needs_docker

SRC, DST = ("migkit-test-engine-src", 16462), ("migkit-test-engine-dst",
                                              16463)

MYISAM = ("CREATE TABLE `g` (\n"
          "  `grp` int NOT NULL,\n"
          "  `id` int NOT NULL AUTO_INCREMENT,\n"
          "  `v` varchar(10) DEFAULT NULL COMMENT 'a (b)',\n"
          "  PRIMARY KEY (`grp`,`id`)\n"
          ") ENGINE=MyISAM DEFAULT CHARSET=latin1 PACK_KEYS=1 CHECKSUM=1"
          " ROW_FORMAT=FIXED\n"
          "/*!50100 PARTITION BY HASH (`grp`)\nPARTITIONS 2 */")


def test_the_definition_is_made_for_the_new_engine():
    change = tableengine.Change("g", "myisam", "InnoDB", "the hop asks")
    got = tableengine.rewrite(MYISAM, change, "id", [(["grp"], "p",
                                                      ["id"])])
    assert "ENGINE=InnoDB" in got and "MyISAM" not in got
    # what only MyISAM reads is left out, and a row format InnoDB refuses
    for gone in ("PACK_KEYS", "CHECKSUM", "ROW_FORMAT"):
        assert gone not in got, got
    # the key and the foreign key go inside the column list, not after
    # the partition clause's own parentheses
    body = got[:got.index(") ENGINE")]
    assert "KEY `migkit_id` (`id`)" in body
    assert "FOREIGN KEY (`grp`) REFERENCES `p` (`id`)" in body
    assert got.rstrip().endswith("PARTITIONS 2 */")
    # nothing to add: only the engine and its options change
    plain = tableengine.rewrite(MYISAM, change)
    assert "migkit_id" not in plain and "FOREIGN KEY" not in plain
    memory = ("CREATE TABLE `c` (\n  `k` int NOT NULL,\n  PRIMARY KEY (`k`)"
              " USING HASH\n) ENGINE=MEMORY")
    got = tableengine.rewrite(memory, tableengine.Change(
        "c", "memory", "InnoDB", ""))
    assert "USING HASH" not in got and "ENGINE=InnoDB" in got


def test_the_column_list_is_found_past_strings_and_names():
    ddl = "CREATE TABLE `a)b` (\n  `c` varchar(3) DEFAULT ')'\n) X=(1)"
    assert ddl[tableengine._body_end(ddl)] == ")"
    assert ddl[tableengine._body_end(ddl) + 1:].startswith(" X=(1)")


def test_the_hop_says_its_engine_or_is_refused():
    from migkit.config import Endpoint, Hop

    def hop(opts):
        return Hop(name="h", engine="mysql", source=Endpoint(),
                   target=Endpoint(), options=opts)
    assert tableengine.settings(hop({})) is None
    got = tableengine.settings(hop({"table_engine": {
        "to": "InnoDB", "accept": ["group numbering"],
        "foreign_keys": {"orders.customer_id": "customers.id",
                         "lines(order_id, n)": "orders(id, n)"}}}))
    assert got["to"] == "InnoDB" and got["accept"] == ["group numbering"]
    assert got["foreign_keys"] == [
        ("orders", ["customer_id"], "customers", ["id"]),
        ("lines", ["order_id", "n"], "orders", ["id", "n"])]
    for wrong in ({"table_engine": {"accept": ["anything"]}},
                  {"table_engine": {"foreign_keys": {"a.b": "c(d, e)"}}},
                  {"table_engine": {"foreign_keys": {"nodot": "c.d"}}},
                  {"table_engine": {"to": "Inno DB"}}):
        with pytest.raises(SystemExit):
            tableengine.settings(hop(wrong))


def test_what_each_engine_does_is_known_or_said_unknown():
    assert tableengine.traits("MyISAM").group_increment
    assert not tableengine.traits("myisam").transactions
    assert tableengine.traits("InnoDB").foreign_keys
    assert tableengine.traits("MRG_MyISAM").holds == "union"
    assert tableengine.traits("Aria").fulltext == "myisam"
    assert tableengine.traits("SPIDER").holds == "remote"
    assert tableengine.traits("nothing-known").holds == "unknown"


def test_the_way_is_the_definition_or_in_place():
    new = tableengine.Change("t", "myisam", "innodb", "x")
    old = tableengine.Change("t", "myisam", "innodb", "x", there="myisam")
    assert tableengine.way(None, "app", new).path == "definition"
    got = tableengine.way(None, "app", old)
    assert got.path == "in place"
    assert "the target has the table already" in " ".join(
        why for _, why in got.passed)


def test_a_schema_that_differs_by_the_plan_alone_is_the_plan(tmp_path):
    class Hop:
        options = {}

        def report_dir(self, db):
            return tmp_path
    src = ("CREATE TABLE `g` (\n  `grp` int NOT NULL,\n  `id` int NOT NULL"
           " AUTO_INCREMENT,\n  PRIMARY KEY (`grp`,`id`)\n) ENGINE=MyISAM"
           " DEFAULT CHARSET=latin1 PACK_KEYS=1")
    dst = ("CREATE TABLE `g` (\n  `grp` int NOT NULL,\n  `id` int NOT NULL"
           " AUTO_INCREMENT,\n  PRIMARY KEY (`grp`,`id`),\n  KEY"
           " `migkit_id` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin1")
    (tmp_path / "schema-src.sql").write_text(src)
    (tmp_path / "schema-dst.sql").write_text(dst)
    plan = tableengine.Plan("app")
    assert tableengine._schema_as_planned(Hop(), "app", plan) is True
    # a column changed beside the engine is a difference
    (tmp_path / "schema-dst.sql").write_text(dst.replace(
        "`grp` int", "`grp` bigint"))
    assert tableengine._schema_as_planned(Hop(), "app", plan) is False
    # a foreign key the hop did not name is a difference too
    (tmp_path / "schema-dst.sql").write_text(dst.replace(
        "KEY `migkit_id` (`id`)", "CONSTRAINT `x` FOREIGN KEY (`grp`)"
        " REFERENCES `p` (`id`)"))
    assert tableengine._schema_as_planned(Hop(), "app", plan) is False


# --- MariaDB 11.4 into MySQL 8.4 ------------------------------------------------

def _sql(name, sql, client="mysql"):
    got = subprocess.run(["docker", "exec", "-i", name, client, "-uroot",
                          "-ptest", "-N", "-B"], input=sql,
                         capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


SEED = """
create database shop character set latin1 collate latin1_swedish_ci;
use shop;
create table grouped (grp int not null, id int not null auto_increment,
  v varchar(10), primary key (grp, id)) engine=MyISAM;
insert into grouped (grp, v) values (1, 'a'), (1, 'b'), (2, 'c');
create table customers (id int primary key, name varchar(20)) engine=Aria;
insert into customers values (1, 'one'), (2, 'two');
create table orders (id int primary key, customer_id int, note text,
  fulltext key (note)) engine=MyISAM;
insert into orders values (1, 1, 'first order'), (2, 3, 'names nobody');
create table cache (k int primary key, v int) engine=MEMORY;
insert into cache values (1, 1);
create table part_a (id int primary key) engine=MyISAM;
create table part_b (id int primary key) engine=MyISAM;
insert into part_a values (1);
insert into part_b values (2);
create table parts (id int primary key) engine=MRG_MyISAM
  union=(part_a, part_b);
"""


@pytest.fixture(scope="module")
def pair():
    try:
        for (name, port), image, env in (
                (SRC, "mariadb:11.4", "MARIADB_ROOT_PASSWORD=test"),
                (DST, "mysql:8.4", "MYSQL_ROOT_PASSWORD=test")):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
            subprocess.run(["docker", "run", "-d", "--name", name, "-e", env,
                            "-p", f"127.0.0.1:{port}:3306", image],
                           check=True, capture_output=True)
        for (name, _), client in ((SRC, "mariadb"), (DST, "mysql")):
            for _ in range(90):
                if subprocess.run(["docker", "exec", name, client, "-uroot",
                                   "-ptest", "-e", "select 1"],
                                  capture_output=True).returncode == 0:
                    break
                time.sleep(2)
        _sql(SRC[0], SEED, "mariadb")
        yield
    finally:
        for name, _ in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)


@pytest.fixture
def conf(pair, tmp_path, monkeypatch):
    import migkit.config as cfg
    path = tmp_path / "hops.yaml"
    side = "{{host: 127.0.0.1, port: {port}, user: root, password: test}}"
    head = (f"    engine: mysql\n    source: {side.format(port=SRC[1])}\n"
            f"    target: {side.format(port=DST[1])}\n"
            "    databases: [shop]\n")
    path.write_text(
        "hops:\n"
        "  strict:\n" + head + "    exclude: [parts, part_a, part_b]\n"
        "    options:\n"
        "      table_engine: {to: InnoDB, foreign_keys:"
        " {orders.customer_id: customers.id}}\n"
        "  merged:\n" + head + "    db_map: {shop: shop_merge}\n"
        "    exclude: [orders]\n"
        "    options:\n"
        "      table_engine: {to: InnoDB, accept: [group numbering]}\n"
        "  lenient:\n" + head + "    db_map: {shop: shop_ok}\n"
        "    exclude: [orders, parts, part_a, part_b]\n"
        "    options:\n"
        "      table_engine: {to: InnoDB, accept: [group numbering]}\n"
        "  plain:\n" + head + "    db_map: {shop: shop_plain}\n"
        "    exclude: [grouped, orders, parts, part_a, part_b, cache]\n")
    monkeypatch.setattr(cfg, "CONF", str(path))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return path


def _run(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join(got.output.split())


@needs_docker
def test_rows_a_key_would_refuse_and_numbering_per_group_stop_the_move(
        conf):
    got, out = _run("move", "strict", "--mode", "full", "--go")
    assert got.exit_code != 0, out
    assert "nothing was written" in out, out
    assert "orders (customer_id) -> customers (id)" in out, out
    assert "1 rows on the source name no row of customers (e.g. (3,))" in \
        out, out
    assert "AUTO_INCREMENT per group" in out and "grouped.id" in out, out
    assert _sql(DST[0], "select count(*) from information_schema.tables"
                        " where table_schema = 'shop'") == "0"


@needs_docker
def test_a_merge_tables_borrowed_rows_are_refused(conf):
    got, out = _run("move", "merged", "--mode", "full", "--go")
    assert got.exit_code != 0, out
    assert "a MERGE table's rows are the rows of part_a, part_b" in out, out
    assert "create view `parts` as select * from `part_a` union all" \
        " select * from `part_b`" in out, out


@needs_docker
def test_each_table_arrives_in_the_new_engine_and_is_proved(conf):
    got, out = _run("move", "lenient", "--mode", "full")
    assert got.exit_code == 0, out
    assert "a storage engine change:" in out and "MyISAM to InnoDB" in out
    got, out = _run("move", "lenient", "--mode", "full", "--go")
    assert got.exit_code == 0, out
    engines = _sql(DST[0], "select table_name, engine from"
                           " information_schema.tables where table_schema ="
                           " 'shop_ok' order by 1")
    assert engines.split("\n") == ["cache\tInnoDB", "customers\tInnoDB",
                                   "grouped\tInnoDB"], engines
    # per-group numbering kept for the rows carried, a key added so InnoDB
    # takes the column
    assert _sql(DST[0], "select group_concat(concat(grp, ':', id) order by"
                        " grp, id) from shop_ok.grouped") == "1:1,1:2,2:1"
    assert "migkit_id" in _sql(DST[0], "show create table shop_ok.grouped")
    # what a fork change says of the two schemas is its own; the engines
    # are this proof's
    got, out = _run("check", "lenient")
    assert "shop engines: OK 3 tables in their new engine" in out, out
    # a table put back in its old engine behind the proof is found
    _sql(DST[0], "alter table shop_ok.cache engine=MyISAM;")
    got, out = _run("check", "lenient")
    assert got.exit_code != 0, out
    assert "cache is myisam, planned innodb" in out.lower(), out


@needs_docker
def test_a_target_without_the_sources_engine_is_a_change_said(conf):
    got, out = _run("move", "plain", "--mode", "full")
    assert got.exit_code == 0, out
    assert "Aria to InnoDB" in out, out
    got, out = _run("move", "plain", "--mode", "full", "--go")
    assert got.exit_code == 0, out
    assert _sql(DST[0], "select engine from information_schema.tables where"
                        " table_schema = 'shop_plain' and table_name ="
                        " 'customers'") == "InnoDB"
