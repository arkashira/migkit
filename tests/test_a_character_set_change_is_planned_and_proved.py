"""A change of character set or collation (backlog P0-SCENARIOS): planned
from both servers, every value converted exactly and proved in the new
character set, double-encoded text repaired, a value the target has no
code for and two keys the new collation takes for one refused by name.

The codecs, the conversion, the proof's verdicts and the way the check
puts the proof in place of a data check of each side's own bytes run on
stand-ins, each able to say "different"; the servers in the docker tests
below: MySQL 8.4 into MySQL 8.4, latin1 into utf8mb4.
"""
import subprocess
import time

import pytest
from click.testing import CliRunner

from migkit import charsets
from tests.conftest import needs_docker

SRC, DST = ("migkit-test-charset-src", 16460), ("migkit-test-charset-dst",
                                               16461)


# --- what each character set holds ---------------------------------------------

def test_each_character_set_holds_what_its_codec_holds():
    latin1 = charsets.charset("mysql", "latin1")
    assert latin1.codec == "cp1252"
    assert charsets.fits("café €", latin1) is True
    assert charsets.fits("กขค", latin1) is False
    tis = charsets.charset("mysql", "tis620")
    assert charsets.fits("กขค", tis) is True
    mb3 = charsets.charset("mysql", "utf8mb3")
    assert charsets.fits("日本語", mb3) is True
    assert charsets.fits("\U0001f600", mb3) is False
    assert charsets.fits("\U0001f600", charsets.charset("mysql",
                                                         "utf8mb4")) is True
    win874 = charsets.charset("postgres", "WIN874")
    assert win874.codec == "cp874" and charsets.fits("กขค", win874)
    euc = charsets.charset("postgres", "EUC_KR")
    assert charsets.fits("한국어", euc) is True
    assert charsets.fits("日本語ｱ", euc) is False
    assert charsets.charset("mssql", "cp1252").codec == "cp1252"
    assert charsets.charset("mssql", "utf-16").unicode
    assert charsets.charset("oracle", "WE8MSWIN1252").codec == "cp1252"
    assert charsets.charset("db2", "1208").unicode
    # a character set migkit does not know holds nothing it can say
    unknown = charsets.charset("postgres", "MULE_INTERNAL")
    assert not unknown.known and charsets.fits("a", unknown) is None


def test_a_change_is_what_the_columns_hold_not_their_names():
    def col(a, b, ca=None, cb=None):
        return charsets.Column("t", "t", "c",
                               charsets.charset("mysql", a, ca),
                               charsets.charset("mysql", b, cb))
    assert not col("utf8", "utf8mb3").recoded
    assert col("latin1", "utf8mb4").recoded
    assert col("utf8mb3", "utf8mb4").recoded
    assert not col("utf8mb4", "utf8mb4", "utf8mb4_bin",
                   "utf8mb4_bin").recollated
    assert col("utf8mb4", "utf8mb4", "utf8mb4_general_ci",
               "utf8mb4_0900_ai_ci").recollated


def test_each_value_is_converted_as_the_plan_says():
    latin1 = charsets.charset("mysql", "latin1")
    mb4 = charsets.charset("mysql", "utf8mb4")
    # UTF-8 stored twice: repaired only where the plan repairs
    assert charsets.convert("cafÃ©", latin1, mb4, True) == "café"
    assert charsets.convert("cafÃ©", latin1, mb4, False) == "cafÃ©"
    # genuine accented text is never touched
    assert charsets.convert("café", latin1, mb4, True) == "café"
    assert charsets.convert(7, latin1, mb4, True) == 7
    with pytest.raises(charsets.Unfit):
        charsets.convert("ภาษาไทย", mb4, latin1, True)


def test_the_hop_says_its_conversion_or_is_refused():
    from migkit.config import Endpoint, Hop

    def hop(opts):
        return Hop(name="h", engine="mysql", source=Endpoint(),
                   target=Endpoint(), options=opts)
    assert charsets.settings(hop({})) is None
    got = charsets.settings(hop({"charset": "utf8mb4"}))
    assert got == {"to": "utf8mb4", "collation": "", "repair": True}
    got = charsets.settings(hop({"charset": {"collation":
                                             "utf8mb4_0900_ai_ci",
                                             "repair": "none"}}))
    assert got["repair"] is False and got["to"] == ""
    assert charsets._mysql_declared(got).name == "utf8mb4"
    for wrong in ({"charset": {"repair": "sometimes", "to": "x"}},
                  {"charset": {}}, {"charset": "utf8; drop"}):
        with pytest.raises(SystemExit):
            charsets.settings(hop(wrong))


def test_a_value_is_read_only_where_one_can_decide_something():
    wide = charsets.Plan("app", [charsets.Column(
        "t", "t", "c", charsets.charset("mysql", "latin1"),
        charsets.charset("mysql", "utf8mb4", "utf8mb4_bin", True))])
    assert not charsets.needs_scan(wide)
    wide.repair = True
    assert charsets.needs_scan(wide)
    narrow = charsets.Plan("app", [charsets.Column(
        "t", "t", "c", charsets.charset("mysql", "utf8mb4"),
        charsets.charset("mysql", "latin1"))])
    assert charsets.needs_scan(narrow)


def test_each_text_column_is_converted_keeping_its_type():
    ddl = ("CREATE TABLE `people` (\n"
           "  `id` int NOT NULL,\n"
           "  `name` varchar(40) DEFAULT NULL,\n"
           "  `note` text CHARACTER SET latin1 COLLATE latin1_bin COMMENT"
           " 'x, y',\n"
           "  `kind` enum('a','b)c') NOT NULL DEFAULT 'a',\n"
           "  `raw` varbinary(10) DEFAULT NULL,\n"
           "  PRIMARY KEY (`id`)\n"
           ") ENGINE=InnoDB DEFAULT CHARSET=latin1")
    got = charsets._mysql_modify(ddl, "utf8mb4", "utf8mb4_0900_ai_ci")
    assert got == [
        "modify `name` varchar(40) CHARACTER SET utf8mb4 COLLATE"
        " utf8mb4_0900_ai_ci DEFAULT NULL",
        "modify `note` text CHARACTER SET utf8mb4 COLLATE"
        " utf8mb4_0900_ai_ci COMMENT 'x, y'",
        "modify `kind` enum('a','b)c') CHARACTER SET utf8mb4 COLLATE"
        " utf8mb4_0900_ai_ci NOT NULL DEFAULT 'a'",
        "default CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"]


def test_the_schema_differs_only_by_its_character_set_or_it_differs(
        tmp_path):
    class Hop:
        def report_dir(self, db):
            return tmp_path
    src = ("CREATE TABLE `t` (\n  `a` varchar(10) COLLATE"
           " latin1_swedish_ci DEFAULT NULL,\n  `b` text\n)"
           " ENGINE=InnoDB DEFAULT CHARSET=latin1")
    (tmp_path / "schema-src.sql").write_text(src)
    (tmp_path / "schema-dst.sql").write_text(
        src.replace("latin1_swedish_ci", "utf8mb4_0900_ai_ci").replace(
            "CHARSET=latin1", "CHARSET=utf8mb4"))
    assert charsets._schema_as_planned(Hop(), "app") is True
    # a type the conversion widened is a difference, said as one
    (tmp_path / "schema-dst.sql").write_text(
        src.replace("`b` text", "`b` mediumtext"))
    assert charsets._schema_as_planned(Hop(), "app") is False


# --- the proof, and the check it stands in for -----------------------------------

class _Side:
    CANON_ENGINE = "mysql"

    def __init__(self, rows):
        self.rows = rows

    def neutral_key(self, side, db, table):
        return ["id"]

    def _every_row(self, side, db, table, columns, where=None):
        yield [list(r) for r in self.rows]


class _Pair:
    def __init__(self, src, dst):
        self.src_engine, self.dst_engine = _Side(src), _Side(dst)

    def _comparable_columns(self, db, s, t, d, dt):
        return [("id", "integer"), ("name", "text")], \
            [("id", "integer"), ("name", "text")], []

    def _row_text(self, columns, row):
        return tuple(None if v is None else str(v) for v in row)


def _proved(monkeypatch, src, dst, repair=True):
    plan = charsets.Plan("app", [charsets.Column(
        "people", "people", "name", charsets.charset("mysql", "latin1"),
        charsets.charset("mysql", "utf8mb4"))], repair=repair)
    monkeypatch.setattr(charsets, "plan", lambda hop, eng, db: plan)
    monkeypatch.setattr(charsets, "_pair", lambda eng: _Pair(src, dst))

    class Hop:
        name = "h"

        def row_filter(self, *parts):
            return None
    return charsets._proofs(Hop(), None, "app")


def test_the_proof_holds_each_row_to_the_source_as_converted(monkeypatch):
    src = [(1, "café"), (2, "cafÃ©"), (3, "ascii")]
    (r, whole), = _proved(monkeypatch, src,
                          [(1, "café"), (2, "café"), (3, "ascii")])
    assert r.status == "ok" and whole, r
    assert "3 rows, every column" in r.detail
    assert "as repaired" in r.detail
    # a repaired value that did not arrive repaired is different
    (r, whole), = _proved(monkeypatch, src,
                          [(1, "café"), (2, "cafÃ©"), (3, "ascii")])
    assert r.status == "diff" and not whole
    assert "1 rows differ from the source's as converted ('2')" in r.detail
    # carried as it is where the plan does not repair
    (r, _), = _proved(monkeypatch, src,
                      [(1, "café"), (2, "cafÃ©"), (3, "ascii")],
                      repair=False)
    assert r.status == "ok", r
    # a row missing, and one the source has not
    (r, _), = _proved(monkeypatch, src, [(1, "café"), (2, "café"),
                                         (4, "x")])
    assert r.status == "diff"
    assert "1 rows missing on the target ('3')" in r.detail
    assert "1 rows the source does not have ('4')" in r.detail


def test_a_value_the_target_cannot_hold_is_never_proved_equal(monkeypatch):
    plan = charsets.Plan("app", [charsets.Column(
        "people", "people", "name", charsets.charset("mysql", "utf8mb4"),
        charsets.charset("mysql", "latin1"))])
    monkeypatch.setattr(charsets, "plan", lambda hop, eng, db: plan)
    monkeypatch.setattr(charsets, "_pair", lambda eng: _Pair(
        [(1, "ไทย")], [(1, "???")]))

    class Hop:
        name = "h"

        def row_filter(self, *parts):
            return None
    (r, whole), = charsets._proofs(Hop(), None, "app")
    assert r.status == "diff" and not whole
    assert "1 values the target's character set has no code for" in \
        r.detail


def test_the_check_takes_the_proof_only_where_it_covers_the_table(
        monkeypatch):
    from migkit.engines.base import Result
    monkeypatch.setattr(charsets, "applies", lambda hop, eng: True)
    monkeypatch.setattr(charsets, "plan", lambda hop, eng, db: charsets.Plan(
        "app", [object()]))
    monkeypatch.setattr(charsets, "_proofs", lambda hop, eng, db: [
        (Result("charset", "app.a", "ok", "equal"), True),
        (Result("charset", "app.b", "diff", "1 rows differ"), False)])
    data = [{"check": "data", "scope": "app.a", "status": "diff",
             "detail": "bytes"},
            {"check": "data", "scope": "app.b", "status": "diff",
             "detail": "bytes"},
            {"check": "data", "scope": "app.c", "status": "diff",
             "detail": "a real one"},
            {"check": "counts", "scope": "app", "status": "ok"}]
    got = charsets.check(None, None, "app", data)
    kept = {(r["check"], r["scope"]) for r in got}
    # proved whole and equal: the proof stands for it
    assert ("data", "app.a") not in kept
    # the proof says different, or never covered it: the data check stays
    assert ("data", "app.b") in kept and ("data", "app.c") in kept
    assert ("charset", "app.a") in kept and ("charset", "app.b") in kept
    # a database's one result naming its tables: the proved ones leave it
    db_level = [{"check": "data", "scope": "app", "status": "diff",
                 "detail": "tables differ: public.a, public.c (pk-level"
                           " files written)"}]
    monkeypatch.setattr(charsets, "_proofs", lambda hop, eng, db: [
        (Result("charset", "app.public.a", "ok", "equal"), True)])
    got = charsets.check(None, None, "app", db_level)
    left = [r for r in got if r["check"] == "data"]
    assert left and left[0]["detail"].startswith("tables differ: public.c")
    only = [{"check": "data", "scope": "app", "status": "diff",
             "detail": "tables differ: public.a (pk-level files written)"}]
    got = charsets.check(None, None, "app", only)
    assert [r for r in got if r["check"] == "data"] == []


# --- MySQL latin1 into utf8mb4 ---------------------------------------------------

def _my(name, sql):
    got = subprocess.run(["docker", "exec", "-i", name, "mysql", "-uroot",
                          "-ptest", "-N", "-B",
                          "--default-character-set=utf8mb4"],
                         input=sql, capture_output=True, text=True)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


SEED = """
create database shop character set latin1 collate latin1_swedish_ci;
use shop;
create table people (id int primary key, name varchar(40), note text)
  character set latin1;
insert into people values
  (1, convert(unhex('636166E9') using latin1), 'plain'),
  (2, convert(unhex('636166C3A9') using latin1), 'twice'),
  (3, 'ascii', null);
create table tags (id int primary key, tag varchar(20) collate latin1_bin,
  unique key (tag)) character set latin1;
insert into tags values (1, 'Apple'), (2, 'apple');
create database wide character set utf8mb4;
use wide;
create table notes (id int primary key, body varchar(40));
insert into notes values (1, 'plain'), (2, 'ภาษาไทย');
"""


@pytest.fixture(scope="module")
def pair():
    try:
        for name, port in (SRC, DST):
            subprocess.run(["docker", "rm", "-f", "-v", name],
                           capture_output=True)
            subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                            "MYSQL_ROOT_PASSWORD=test", "-p",
                            f"127.0.0.1:{port}:3306", "mysql:8.4"],
                           check=True, capture_output=True)
        for name, _ in (SRC, DST):
            for _ in range(90):
                if subprocess.run(["docker", "exec", name, "mysql",
                                   "-uroot", "-ptest", "-e", "select 1"],
                                  capture_output=True).returncode == 0:
                    break
                time.sleep(2)
        _my(SRC[0], SEED)
        _my(DST[0], "create database shop_tags character set utf8mb4;")
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
            f"    target: {side.format(port=DST[1])}\n")
    path.write_text(
        "hops:\n"
        "  conv:\n" + head + "    databases: [shop]\n    exclude: [tags]\n"
        "    options:\n      target_rows: replace\n"
        "      charset: {to: utf8mb4, collation: utf8mb4_0900_ai_ci}\n"
        "  merged:\n" + head + "    databases: [shop]\n"
        "    db_map: {shop: shop_tags}\n    exclude: [people]\n"
        "    options:\n"
        "      charset: {to: utf8mb4, collation: utf8mb4_0900_ai_ci}\n"
        "  narrow:\n" + head + "    databases: [wide]\n"
        "    options:\n      charset: latin1\n")
    monkeypatch.setattr(cfg, "CONF", str(path))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return path


def _run(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, list(argv))
    return got, " ".join(got.output.split())


@needs_docker
def test_latin1_arrives_as_utf8mb4_repaired_and_proved(conf):
    got, out = _run("move", "conv", "--mode", "full")
    assert got.exit_code == 0, out
    assert "a character set change: latin1 to utf8mb4" in out, out
    got, out = _run("move", "conv", "--mode", "full", "--go")
    assert got.exit_code == 0, out
    assert _my(DST[0], "select character_set_name from"
                       " information_schema.columns where table_schema ="
                       " 'shop' and table_name = 'people' and column_name ="
                       " 'name'") == "utf8mb4"
    # the genuine accent kept, the UTF-8 stored twice repaired, both é
    assert _my(DST[0], "select hex(name) from shop.people where id in"
                       " (1, 2) order by id") == "636166C3A9\n636166C3A9"
    got, out = _run("check", "conv")
    assert got.exit_code == 0, out
    assert "shop.people: OK 3 rows" in out, out
    assert "double-encoded values as repaired" in out, out
    # a value changed behind the proof's back is named by its key
    _my(DST[0], "update shop.people set name = 'cafe' where id = 1;")
    got, out = _run("check", "conv")
    assert got.exit_code != 0, out
    assert "1 rows differ from the source's as converted ('1')" in out, out


@needs_docker
def test_two_keys_the_new_collation_takes_for_one_are_refused(conf):
    got, out = _run("move", "merged", "--mode", "full", "--go")
    assert got.exit_code != 0, out
    assert "nothing was written" in out, out
    assert "groups of its unique values are one under the target's" \
        " collation utf8mb4_0900_ai_ci" in out, out
    assert "'Apple'" in out and "'apple'" in out, out
    assert _my(DST[0], "select count(*) from information_schema.tables"
                       " where table_schema = 'shop_tags'") == "0"


@needs_docker
def test_a_value_the_target_has_no_code_for_is_refused_by_name(conf):
    got, out = _run("move", "narrow", "--mode", "full", "--go")
    assert got.exit_code != 0, out
    assert "wide: notes.body" in out, out
    assert "1 values hold a character latin1 has no code for - by key:" \
        " '2'" in out, out
