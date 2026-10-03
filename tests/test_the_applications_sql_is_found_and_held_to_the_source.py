"""The application's own SQL - from its code and from the source's record
of what it ran - carried to the target and held to the source's answers
(backlog CV.P10, CV.P11, CV.P21, W3).

No server here: placeholders read by the characters around them, the
statements found in a repository of six languages, the corpus kept
without a value, the hop's restructuring applied and refused, and the
prover itself run on two in-memory databases standing for the two
sandboxes - the same rows on both answer alike, one value changed on the
target is found, and a statement nothing meets is never called proved.
The servers' part is `test_conversion_breadth_mysql_to_postgres.py`.
"""
import json
import sqlite3
from types import SimpleNamespace

import pytest

from migkit.config import Endpoint, Hop
from migkit.storedcode import appscan, corpus, harness, statements
from migkit.storedcode.harness import Watched
from migkit.storedcode.translate import Refused, parse

# ---- placeholders -------------------------------------------------------------


@pytest.mark.parametrize("text,dialect,tokens,slots", [
    ("SELECT a FROM t WHERE id = ? AND b LIKE %s", "mysql", ["?", "%s"],
     [0, 1]),
    ("select * from t where s = :st and v > :min and s <> :st", "oracle",
     [":st", ":min", ":st"], [0, 1, 0]),
    ("select a::int from t where x = $1 and y = $2 and z = $1", "postgres",
     ["$1", "$2", "$1"], [0, 1, 0]),
    ("SELECT TOP (@n) a FROM t WHERE b = @b", "tsql", ["@n", "@b"], [0, 1]),
    ("select id from t where id = #{id} and k = ?1 and j = ?1", "mysql",
     ["#{id}", "?1", "?1"], [0, 1, 1]),
])
def test_placeholders_are_read_as_the_driver_writes_them(text, dialect,
                                                         tokens, slots):
    h = statements.Holes(text, dialect)
    assert h.tokens == tokens and h.slots == slots
    # each one a named placeholder the translator reads in every dialect
    tree = parse(h.normalized, dialect)
    assert tree is not None


def test_quotes_comments_casts_and_slices_hold_no_placeholder():
    h = statements.Holes("select '?', \"a?\", x::int, arr[1:2] -- ?\n"
                         " from t /* :x */ where a = 'it''s :n'", "postgres")
    assert h.tokens == [], h.tokens
    # a `%%` a formatting driver reads as one `%`, in quotes too
    h = statements.Holes("update t set a = %(x)s where p like '50%%'",
                         "mysql")
    assert h.executable("mysql") == \
        "update t set a = %s where p like '50%%'"
    assert h.executable("oracle") == "update t set a = %s where p like '50%'"
    # a question mark at the very end is one
    assert statements.Holes("select 1 where a = ?", "mysql").tokens == ["?"]
    # `${x}` is text put in as it runs
    assert statements.Holes("select * from ${t}", "mysql").substituted


def test_the_targets_text_says_where_each_placeholder_went():
    h = statements.Holes("SELECT TOP (@n) a FROM t WHERE b = @b", "tsql")
    from migkit.storedcode.translate import generate
    there = generate(statements.marked(parse(h.normalized, "tsql")), "tsql",
                     "postgres")
    shown, _ = statements.unmarked(there, h)
    assert shown == "SELECT a FROM t WHERE b = @b LIMIT @n", shown
    run, order = statements.unmarked(there, h, "postgres")
    # the LIMIT moved to the end: its value goes second
    assert run == "SELECT a FROM t WHERE b = %s LIMIT %s" and order == [1, 0]


def test_a_placeholder_is_filled_from_the_column_it_is_compared_with():
    tree = parse(statements.Holes(
        "select o.id from orders o join items i on i.order_id = o.id where"
        " o.status = ? and i.qty between ? and ? limit ?", "mysql")
        .normalized, "mysql")
    got = statements.partners(tree)
    assert got == {0: ("orders", "status"), 1: ("items", "qty"),
                   2: ("items", "qty"), 3: "count"}, got
    tree = parse(statements.Holes("insert into t (a, b) values (?, ?)",
                                  "mysql").normalized, "mysql")
    assert statements.partners(tree) == {0: ("t", "a"), 1: ("t", "b")}


def test_a_text_that_only_begins_like_sql_is_not_sql():
    assert statements.tree_of("Update failed", "mysql") is None
    assert statements.tree_of("Select an option", "mysql") is None
    assert statements.tree_of("UPDATE t SET a = 1", "mysql") is not None


# ---- the hop's restructuring -------------------------------------------------


def _hop(**mapping):
    return Hop(name="r", engine="hetero", source=Endpoint(),
               target=Endpoint(), mapping=mapping)


def test_a_renamed_column_is_read_by_its_new_name():
    hop = _hop(columns={"orders": {"rename": {"total": "amount"}}})
    cols = {"orders": ["id", "status", "total"], "items": ["id", "sku"]}
    tree = parse("SELECT o.id, total FROM orders o JOIN items i ON i.id ="
                 " o.id WHERE o.total > 1", "mysql")
    got = statements.restructured(tree, hop, "shop", lambda t: cols[t])
    assert got.sql("mysql") == ("SELECT o.id, amount FROM orders AS o JOIN"
                                " items AS i ON i.id = o.id WHERE o.amount"
                                " > 1"), got.sql("mysql")
    tree = parse("INSERT INTO orders (id, total) VALUES (1, 2)", "mysql")
    got = statements.restructured(tree, hop, "shop", lambda t: cols[t])
    assert "(id, amount)" in got.sql("mysql"), got.sql("mysql")


def test_a_column_left_out_of_the_target_is_refused_by_name():
    hop = _hop(columns={"items": {"drop": ["legacy_code"]}})
    with pytest.raises(Refused, match="items.legacy_code, which the hop"
                                      " leaves out"):
        statements.restructured(parse("select legacy_code from items",
                                      "mysql"), hop, "shop",
                                lambda t: ["id", "legacy_code"])
    with pytest.raises(Refused, match="every column of items"):
        statements.restructured(parse("select * from items", "mysql"), hop,
                                "shop", lambda t: ["id", "legacy_code"])
    # a hop that restructures nothing leaves every statement as it is
    plain = parse("select * from items", "mysql")
    assert statements.restructured(plain, _hop(), "shop",
                                   lambda t: []) is plain


def test_a_column_written_transformed_is_not_held_to_the_source():
    hop = Hop(name="m", engine="hetero", source=Endpoint(),
              target=Endpoint(),
              options={"mask_on_write": {"users.email": "constant x"}})
    tree = parse("select id, email from users where id = 1", "mysql")
    got = statements.transformed(tree, hop, "shop",
                                 lambda t: ["id", "email"])
    assert got == [("users", "email", "constant x")], got
    # one it only writes lands as the application wrote it, on both sides
    tree = parse("update users set email = 'a@example.com' where id = 1",
                 "mysql")
    assert statements.transformed(tree, hop, "shop",
                                  lambda t: ["id", "email"]) == []


# ---- the prover on two in-memory databases ----------------------------------


class Side(harness.Runner):
    """A sandbox server standing in: an in-memory database read and run
    as the runners read and run theirs."""
    engine = "mysql"

    def __init__(self, rows):
        super().__init__(None, "shop", {})
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("create table orders (id integer primary key,"
                          " status text, total real)")
        self.conn.executemany("insert into orders values (?, ?, ?)", rows)
        self.conn.commit()
        self.watched = {"orders": Watched(
            "orders", [("id", "integer"), ("status", "text"),
                       ("total", "float")], ["id"])}
        self.baseline()

    def read(self, w):
        cols = ", ".join(c for c, _ in w.columns)
        raw = [list(r) for r in self.conn.execute(
            f"select {cols} from {w.name}")]
        return [[harness.render(c, v) for (_, c), v in zip(w.columns, r)]
                for r in raw], raw

    def run(self, statement, params, tables, rows=False):
        out = harness.Outcome()
        sql = statement.replace("%s", "?").replace("%%", "%")
        try:
            cur = self.conn.execute(sql, list(params))
            if rows and cur.description:
                out.sets = [[list(r) for r in cur.fetchall()]]
        except sqlite3.Error as e:
            out.error = ("HY000", str(e))
        out.changes = self.changes(tables)
        self.conn.rollback()
        return out


class Proof:
    def __init__(self, a, b, mapping=None, options=None):
        self.a, self.b, self.db = a, b, "shop"
        hop = Hop(name="x", engine="hetero", source=Endpoint(),
                  target=Endpoint(), options=options or {},
                  mapping=mapping or {})
        src = SimpleNamespace(SQL_DIALECT="mysql", CANON_ENGINE="mysql",
                              neutral_views=lambda side, db: [],
                              neutral_tables=lambda side, db: ["orders"])
        dst = SimpleNamespace(SQL_DIALECT="sqlite", CANON_ENGINE="sqlite")
        self.pair = SimpleNamespace(
            src_engine=src, dst_engine=dst, hop=hop,
            _leaf=lambda t: str(t).split(".")[-1],
            _rename=lambda t, db=None: t)

    def _both(self, a, b, on_a, on_b, mode):
        return on_a(), on_b(), False


ROWS = [(1, "open", 10.5), (2, "closed", 20.0), (3, "open", 5.25)]
S = statements.Statement


def _verdicts(proof, *stmts):
    return {v.origin: v for v in statements.prove(proof, list(stmts))}


def test_the_same_rows_answer_alike_and_one_value_apart_is_found():
    stmts = [S("app.py:1", "SELECT id, total FROM orders WHERE status = %s"),
             S("app.py:2", "UPDATE orders SET total = total + 1 WHERE id ="
                           " %s"),
             S("app.py:3", "SELECT id, total FROM orders WHERE id = :id")]
    same = _verdicts(Proof(Side(ROWS), Side(ROWS)), *stmts)
    for k in ("app.py:1", "app.py:2", "app.py:3"):
        assert same[k].status == "ok", same[k]
        # rows that are there, a value no row holds, NULL
        assert same[k].calls >= 3, same[k]
    apart = _verdicts(Proof(Side(ROWS), Side(
        [(1, "open", 10.5), (2, "closed", 20.0), (3, "open", 6.0)])),
        *stmts)
    for k in ("app.py:1", "app.py:2", "app.py:3"):
        assert apart[k].status == "diff", apart[k]
    assert "answered otherwise on the target" in apart["app.py:1"].reason
    assert "rows changed in orders" in apart["app.py:2"].reason


def test_a_statement_nothing_meets_is_never_called_proved():
    got = _verdicts(Proof(Side(ROWS), Side(ROWS)),
                    S("a:1", "SELECT id FROM orders WHERE status ="
                             " 'nothing'"),
                    S("a:2", "SELECT x FROM missing WHERE y = ?"),
                    S("a:3", "SELECT id FROM orders", dynamic="it is put"
                             " together by the application as it runs"),
                    S("a:4", "SELECT id FROM orders WHERE ${where}"))
    assert got["a:1"].status == "warn" and "nothing in the fixture meets" \
        in got["a:1"].reason, got["a:1"]
    assert got["a:2"].status == "not proved" and "missing" in \
        got["a:2"].reason, got["a:2"]
    assert got["a:3"].status == "not proved", got["a:3"]
    assert got["a:4"].status == "not proved" and "as text" in \
        got["a:4"].reason, got["a:4"]


def test_the_values_on_record_are_tried_first_on_both_sides():
    got = _verdicts(Proof(Side(ROWS), Side([(1, "open", 10.5),
                                             (2, "closed", 20.0),
                                             (3, "open", 9.0)])),
                    S("r:1", "SELECT total FROM orders WHERE id = %s",
                      values=[(3,)]))
    assert got["r:1"].status == "diff" and got["r:1"].reason.startswith(
        "run with the values the source's record kept"), got["r:1"]


def test_the_line_weighs_the_statements_by_how_often_each_ran():
    v = [statements.Verdict("a", "a", "ok", runs=90),
         statements.Verdict("b", "b", "diff", "its answer: x", runs=10)]
    line = statements.result("shop", v, "the source's record",
                             "workload replayed")
    assert line.status == "diff" and line.scope == "shop workload replayed"
    assert "of the 100 runs on record, 90.0% are of statements that" \
        " answered alike" in line.detail, line.detail
    assert statements.result("shop", [], "x") is None


def test_the_translated_statements_are_written_for_a_person(tmp_path):
    v = [statements.Verdict("src/a.py:3", "SELECT 1", "ok", target="SELECT"
                            " id, amount FROM orders WHERE id = %s"),
         statements.Verdict("record: SELECT ?", "SELECT ?", "diff", "x")]
    statements.write(tmp_path / "application-sql", v)
    text = (tmp_path / "application-sql.sql").read_text()
    assert "-- src/a.py:3: ok" in text and "amount" in text
    # a statement from the record keeps no translated text: it may hold a
    # value the record kept
    assert "record:" not in text
    got = json.loads((tmp_path / "application-sql.json").read_text())
    assert [g["status"] for g in got] == ["ok", "diff"]


# ---- the application's code ----------------------------------------------


REPO = {
    "src/app/orders.py": '''def f(cur, oid, st):
    cur.execute("SELECT id, total FROM orders WHERE id = %s", (oid,))
    cur.execute(
        "UPDATE orders SET status = %s "
        "WHERE id = %s", (st, oid))
    q = f"SELECT * FROM {table}"
    msg = "Update failed"
    cur.execute("DELETE FROM orders WHERE id = %d" % oid)
''',
    "src/app/Repo.java": '''class Repo {
  @Query("SELECT u FROM User u WHERE u.email = ?1")
  User byEmail(String e);
  @Query(value = "SELECT * FROM users WHERE email = ?1", nativeQuery = true)
  User byEmail2(String e);
  void f() {
    String a = "SELECT id FROM orders WHERE status = ? AND total > ?";
    String b = "SELECT id FROM orders WHERE id = " + id;
    String c = "SELECT id " + "FROM orders " + "WHERE id = ?";
  }
}
''',
    "src/app/q.ts": '''const q = `SELECT id FROM orders WHERE id = ${id}`;
const r = "INSERT INTO orders (id, total) VALUES ($1, $2)";
''',
    "src/app/m.go": '''package m
func f() { q := fmt.Sprintf("SELECT * FROM %v", t); r := `SELECT a FROM t WHERE b = $1` }
''',
    "src/schema.sql": '''-- a comment; with a semicolon
CREATE TABLE x (a int);
SELECT a FROM x WHERE a = 1;
insert into x values (2);
''',
    "src/OrderMapper.xml": '''<mapper namespace="x">
  <select id="one">select id from orders where id = #{id}</select>
  <select id="many">select id from orders <where><if test="s != null">status = #{s}</if></where></select>
</mapper>
''',
    "node_modules/x/i.js": 'const z = "SELECT 1 FROM hidden";\n',
}


@pytest.fixture
def repo(tmp_path):
    for rel, text in REPO.items():
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
    return tmp_path


def test_every_statement_in_the_code_is_found_and_none_invented(repo):
    hop = Hop(name="a", engine="hetero", source=Endpoint(),
              target=Endpoint(), options={"application_sql": str(repo)})
    got, said = appscan.found(hop, "mysql")
    assert said == []
    by = {s.origin: s for s in got}
    text = {s.origin: " ".join(s.text.split()) for s in got
            if not s.dynamic}
    assert text["src/app/orders.py:2"] == \
        "SELECT id, total FROM orders WHERE id = %s"
    # Python's adjacent literals, and Java's joined ones, are one
    assert text["src/app/orders.py:4"] == \
        "UPDATE orders SET status = %s WHERE id = %s"
    assert text["src/app/Repo.java:9"] == "SELECT id FROM orders WHERE id = ?"
    # the mapper's own query language is the mapper's; a native one is SQL
    assert "src/app/Repo.java:2" not in by
    assert text["src/app/Repo.java:4"] == \
        "SELECT * FROM users WHERE email = ?1"
    assert text["src/schema.sql:3"] == "SELECT a FROM x WHERE a = 1"
    assert text["src/OrderMapper.xml:2"] == \
        "select id from orders where id = #{id}"
    # put together as it runs: found, never run in a guessed form
    for k in ("src/app/orders.py:6", "src/app/orders.py:8",
              "src/app/Repo.java:8", "src/app/q.ts:1", "src/app/m.go:2",
              "src/OrderMapper.xml:3"):
        assert any(s.origin == k and s.dynamic for s in got), k
    # the same line's other statement is read as it is
    assert any(s.origin == "src/app/m.go:2" and not s.dynamic and
               s.text == "SELECT a FROM t WHERE b = $1" for s in got)
    # not SQL, not the application's own code
    assert not any("Update failed" in s.text for s in got)
    assert not any("hidden" in s.text for s in got)


def test_a_path_not_on_this_machine_is_said(tmp_path):
    hop = Hop(name="a", engine="hetero", source=Endpoint(),
              target=Endpoint(),
              options={"application_sql": [str(tmp_path / "gone")]})
    got, said = appscan.found(hop, "mysql")
    assert got == [] and "not on this machine" in said[0], said


# ---- the corpus -------------------------------------------------------------


class Record:
    """A MySQL source whose statement digests are given."""
    CANON_ENGINE = "mysql"
    SQL_DIALECT = "mysql"
    MIGKIT_MARKS = ("information_schema", "performance_schema")

    def __init__(self, rows):
        self.rows = rows

    def _d(self, side, db):
        return db

    def _q(self, side, sql, args=None):
        return self.rows


def _pair(rows, **options):
    return SimpleNamespace(
        src_engine=Record(rows),
        hop=Hop(name="c", engine="hetero", source=Endpoint(),
                target=Endpoint(), options=options))


DIGESTS = [
    ("SELECT `id` , `total` FROM `orders` WHERE `status` = ?", 40,
     "SELECT id, total FROM orders WHERE status = 'open'"),
    ("SELECT `id` FROM `orders` WHERE `id` IN (...)", 5,
     "SELECT id FROM orders WHERE id IN (1, 2, 3 ..."),
    ("SELECT `table_name` FROM `information_schema` . `tables`", 9,
     "SELECT table_name FROM information_schema.tables"),
    ("SELECT `migkit` /* migkit proof */ `f` ( ? )", 3,
     "SELECT /* migkit proof */ f(1)"),
    ("UPDATE `orders` SET `note` = ? WHERE `id` = ?", 7,
     "UPDATE orders SET note = 'x' WHERE id = 3"),
    ("CREATE TABLE `t` ( `a` INTEGER )", 1, "CREATE TABLE t (a int)"),
]


def test_the_corpus_is_the_applications_statements_most_run_first():
    got, why = corpus.statements(_pair(DIGESTS), "shop")
    assert why is None
    kinds = [(s.kind, s.runs) for s in got]
    # the catalogue's reads, migkit's own calls and DDL are not in it
    assert kinds == [("read", 40), ("write", 7), ("read", 5)], kinds
    assert got[0].text == "SELECT id, total FROM orders WHERE status = 'open'"
    # a statement the record keeps cut short is said, never guessed at
    assert got[2].dynamic == "the source's record keeps it cut short"


def test_a_masked_hop_runs_no_value_of_the_record(tmp_path):
    got, _ = corpus.statements(_pair(DIGESTS, mask="all"), "shop")
    assert "'open'" not in got[0].text and "?" in got[0].text, got[0].text
    got, _ = corpus.statements(_pair(DIGESTS, mask=["orders.status"]),
                               "shop")
    assert "'open'" not in got[0].text
    # a sample whose values touch no masked column is still run
    assert got[1].text == "UPDATE orders SET note = 'x' WHERE id = 3"


def test_the_corpus_kept_holds_no_value_and_grows(tmp_path):
    hop = Hop(name="c", engine="hetero", source=Endpoint(),
              target=Endpoint())
    hop.report_dir = lambda db=None: tmp_path
    got, _ = corpus.statements(_pair(DIGESTS), "shop")
    corpus.keep(hop, "shop", got)
    text = (tmp_path / "workload-corpus.json").read_text()
    assert "'open'" not in text and "'x'" not in text, text
    # a statement gone from the server's record since stays in the corpus
    corpus.keep(hop, "shop", got[:1])
    kept = json.loads((tmp_path / "workload-corpus.json").read_text())
    assert len(kept["statements"]) == 3
    assert kept["statements"][0]["runs"] == 40


def test_an_oracle_source_is_asked_through_its_shared_pool():
    eng = SimpleNamespace(CANON_ENGINE="oracle", SQL_DIALECT="oracle")
    assert isinstance(corpus.source(eng), corpus.OracleSource)
    # an engine that reads its own statement statistics is asked as it is
    mine = SimpleNamespace(CANON_ENGINE="mysql", workload_reads=lambda *a: [])
    assert corpus.source(mine) is mine
    assert corpus.normalized("select a from t where b = :1 and c = 'q'",
                             "oracle") == "SELECT a FROM t WHERE b = ? AND" \
                                          " c = ?"
