"""The proof's reading parts, held without a server (leapfrog research P4,
P6, P14; backlog R11's leftovers and item 40): the calls the source's
record holds, read into shapes and values and held back where the hop
masks; the clock compared exactly wherever the two sessions can be made
to read one instant; a routine that reads what a probe would change
measured through a copy beside it; a body cut into its statements in
every source's language; the converter's score read as its own figure;
and which declarations leave a domain open to be sized from the data.
What each means on real servers is held by the docker tests beside this
(`test_stored_code_proved_by_history_sequences_and_clock`,
`test_types_sized_from_the_data`, `test_oracle_code_coverage_and_counters`).
"""
import datetime as dt
from decimal import Decimal

import pytest

from migkit.config import Endpoint, Hop
from migkit.storedcode import effort, harness, history, reduce, sizing
from migkit.storedcode.routines import Param, Routine


# ---- the source's record of its calls ----------------------------------------


def test_a_recorded_call_is_read_into_values_nulls_and_placeholders():
    got = history.calls_in("CALL add_order(5.00, 'O''Brien', @x)",
                           {"add_order"}, "mysql")
    assert got == [("add_order", (("value", Decimal("5.00")),
                                  ("value", "O'Brien"),
                                  ("placeholder", None)))]
    # a digest's normalised text: every argument the application fills
    got = history.calls_in("CALL `add_order` (?, ?, @x)", {"add_order"},
                           "mysql")
    assert [k for k, _ in got[0][1]] == ["placeholder"] * 3, got
    # a function anywhere in a statement, qualified or not
    got = history.calls_in("select app.code_of($1), code_of(NULL) from t",
                           {"code_of"}, "postgres")
    assert got == [("code_of", (("placeholder", None),)),
                   ("code_of", (("null", None),))]
    # SQL Server's EXEC, arguments named and without parentheses
    got = history.calls_in("exec dbo.add_order @amt = 5.00, @who = N'ann',"
                           " @made = @m output", {"add_order"}, "mssql")
    assert got == [("add_order", (("value", Decimal("5.00")),
                                  ("value", "ann"), ("placeholder", None)))]
    # a sample cut at the server's text limit is no call
    assert history.calls_in("CALL add_order(5.0, 'abc", {"add_order"},
                            "mysql") == []
    # a routine not asked about is not read
    assert history.calls_in("select other(1)", {"code_of"}, "mysql") == []


def test_recorded_calls_become_inputs_the_most_run_first():
    rec = history.Record()
    rec.add((("value", 5), ("value", "ann"), ("placeholder", None)), 3)
    rec.add((("value", 7), ("null", None), ("placeholder", None)), 10)
    rec.add((("placeholder", None), ("value", "x"), ("placeholder", None)),
            50)
    # the same shape again adds to its runs
    rec.add((("value", 7), ("null", None), ("placeholder", None)), 1)
    params = [Param("a", "int"), Param("w", "varchar(20)"),
              Param("m", "int", "out")]
    assert rec.runs == 64
    # a placeholder for an IN parameter leaves the tuple out; an OUT one
    # is the routine's to write
    assert rec.tuples(params, lambda k: True) == [(7, None, None),
                                                  (5, "ann", None)]
    assert rec.values(1, lambda k: True) == ["x", "ann"]
    # a parameter whose values may not be taken: no tuple needs it but
    # the one that gave it NULL
    assert rec.tuples(params, lambda k: k != 1) == [(7, None, None)]
    assert rec.values(1, lambda k: k != 1) == []


def _hop(mask=None):
    options = {"source_engine": "mysql", "target_engine": "postgres"}
    if mask is not None:
        options["mask"] = mask
    return Hop(name="h", engine="hetero", options=options,
               source=Endpoint(host="10.0.0.1", port=1, user="u",
                               password="CHANGE_ME"),
               target=Endpoint(host="10.0.0.2", port=2, user="u",
                               password="CHANGE_ME"),
               databases=["d"])


def test_recorded_values_are_taken_only_where_the_hop_masks_nothing():
    r = Routine("function", "f", [Param("email", "varchar(40)"),
                                  Param("n", "int")])
    compared = {"email": {"people.email"}, "n": {"orders.qty"}}
    assert history.allowed(_hop(), r, compared)(0) is True
    assert history.allowed(_hop(), r, compared)(1) is True
    everything = history.allowed(_hop(mask="all"), r, compared)
    assert (everything(0), everything(1)) == (False, False)
    # a masked column: the parameter the body compares with it takes no
    # recorded value; the other still does
    some = history.allowed(_hop(mask=["people.email"]), r, compared)
    assert (some(0), some(1)) == (False, True)


def test_the_proofs_own_calls_are_marked_and_left_out_of_the_record():
    # the mark every function call the proof makes carries
    assert harness.MARK.startswith("/*") and harness.MARK.endswith("*/")

    class Eng:
        CANON_ENGINE = "mysql"

    class Pair:
        src_engine = Eng()

    rows = [("SELECT `code_of` (?)", 5,
             f"SELECT {harness.MARK} `code_of`('zz-not-there')"),
            ("SELECT `code_of` (?)", 9, "SELECT code_of('ORDER-7')")]
    history.READERS["mysql"], was = (lambda eng, db: rows), \
        history.READERS["mysql"]
    try:
        got = history.read(Pair(), "d", [Routine("function", "code_of",
                                                 [Param("c", "text")])])
    finally:
        history.READERS["mysql"] = was
    rec = got.of(Routine("function", "code_of"))
    assert rec.values(0, lambda k: True) == ["ORDER-7"]
    # the proof's own call counts by its shape, never by its value
    assert rec.runs == 14


# ---- the clock -----------------------------------------------------------------


class _Runner:
    def __init__(self, engine, sets, reads):
        self.engine, self.SETS_CLOCK, self.READS_CLOCK = engine, sets, reads


MY = _Runner("mysql", True, False)
PG = _Runner("postgres", False, True)
MS = _Runner("mssql", False, False)


def test_the_clock_is_set_to_one_instant_wherever_it_can_be():
    # both can be set: both are
    assert harness.clock_mode(MY, MY, "return now()", "return now()") == \
        ("pinned", None, True)
    # the side that can only be read is called first, the other set to it
    assert harness.clock_mode(MY, PG, "return now()",
                              "return localtimestamp") == \
        ("pinned", "b", True)
    assert harness.clock_mode(PG, MY, "return now()", "return now()") == \
        ("pinned", "a", True)
    # set all the same where the bodies read none: a trigger their writes
    # fire, or a function they call, may
    assert harness.clock_mode(MY, PG, "select 1", "select 1") == \
        ("pinned", "b", False)
    # two sides that can only be read: each value by its distance
    assert harness.clock_mode(PG, PG, "return now()", "return now()") == \
        (harness.RELATIVE, None, True)
    # a routine that commits starts a transaction, and moves its clock
    assert harness.clock_mode(MY, PG, "call x()", "now()", commits=True) \
        == (True, None, True)
    # a clock no session can be set to: within the tolerance, and said
    assert harness.clock_mode(MY, PG, "return sysdate()", "now()") == \
        (True, None, True)
    assert harness.clock_mode(MY, PG, "now()", "clock_timestamp()") == \
        (True, None, True)
    assert harness.clock_mode(MS, PG, "getdate()", "now()") == \
        (True, None, True)
    # nothing to set and nothing read: exact
    assert harness.clock_mode(MS, MS, "select 1", "select 1") == \
        (None, None, False)


def test_an_instant_is_set_to_the_microsecond():
    # a float of today's seconds would lose the last digit
    assert harness.epoch(dt.datetime(2026, 9, 30, 12, 0, 0, 123456)) == \
        "1790769600.123456"
    assert harness.epoch(dt.datetime(2026, 9, 30, 12, 0, 0, 5,
                                     tzinfo=dt.timezone.utc)) == \
        "1790769600.000005"


def test_two_read_clocks_are_compared_by_distance_exactly():
    a = dt.datetime(2026, 9, 30, 12, 0, 0, 900000)
    b = dt.datetime(2026, 9, 30, 12, 0, 0, 100)
    # now() + 1 second on each side, to the microsecond
    assert harness._same_distance("2026-09-30 12:00:01.900000",
                                  "2026-09-30 12:00:01.000100", a, b)
    # cut to the second on both: one distance
    assert harness._same_distance("2026-09-30 12:00:05",
                                  "2026-09-30 12:00:05", a, b)
    # a second more on one side is a difference, not a tolerance
    assert not harness._same_distance("2026-09-30 12:00:02.900000",
                                      "2026-09-30 12:00:01.000100", a, b)
    x, y = harness.Outcome(value="2026-09-30 12:00:02.900000", now=a), \
        harness.Outcome(value="2026-09-30 12:00:01.000100", now=b)
    assert harness.differences(x, y, harness.RELATIVE)
    # which the old five seconds' tolerance took for the same
    assert harness.differences(x, y, True) == []
    same = harness.Outcome(value="2026-09-30 12:00:01.000100", now=b)
    assert harness.differences(
        harness.Outcome(value="2026-09-30 12:00:01.900000", now=a), same,
        harness.RELATIVE) == []


# ---- a routine that reads what a probe would change --------------------------


def test_a_routine_reading_row_count_is_measured_through_a_copy():
    from migkit.storedcode import mysql_pg
    body = ("begin declare n int; update t set a = 1 where id = x;"
            " if row_count() = 0 then insert into t values (x); end if;"
            " select row_count() into n; end")
    r = Routine("procedure", "p", [Param("x", "int")], body=body,
                definition=f"CREATE DEFINER=`root`@`%` PROCEDURE `p`(x int)"
                           f" {body}")
    made, probes, twin = mysql_pg.instrument(r)
    assert twin == "migkit_cover_p"
    assert made.startswith("CREATE DEFINER=`root`@`%` PROCEDURE"
                           " `migkit_cover_p`(x int) begin declare"
                           " _migkit_rc bigint;"), made
    # the copy reads the count kept after each statement, never the live
    # one a probe's insert just changed
    assert "row_count() = 0" not in made and "if _migkit_rc = 0" in made
    assert "select _migkit_rc into n" in made, made
    # as the body begins, and after each of its three statements
    assert made.count("set _migkit_rc = row_count()") == 4, made
    # every statement's count kept before the next probe
    assert made.index("update t set a = 1 where id = x; set _migkit_rc") \
        < made.index("insert into migkit_cover values ('p', 2)"), made
    assert [w for _, _, w in probes] == ["block", "then", "else (none)"]


def test_the_diagnostics_area_is_kept_for_a_handler_that_reads_it():
    from migkit.storedcode import mysql_pg
    body = ("begin declare continue handler for sqlexception begin get"
            " diagnostics condition 1 @s = returned_sqlstate; end;"
            " insert into t values (1); end")
    r = Routine("procedure", "q", [], body=body,
                definition=f"CREATE PROCEDURE `q`() {body}")
    made, _, twin = mysql_pg.instrument(r)
    assert twin == "migkit_cover_q"
    # kept as the handler is entered, before its probe clears the area
    entry = made.index("declare continue handler")
    assert made.index("get diagnostics condition 1"
                      " _migkit_c1_returned_sqlstate", entry) < \
        made.index("insert into migkit_cover values ('q', 2)"), made
    assert "set @s = _migkit_c1_returned_sqlstate" in made, made


def test_what_no_copy_can_keep_is_not_instrumented():
    from migkit.storedcode import mysql_pg
    # a trigger cannot be called beside a copy
    trig = Routine("trigger", "t_bi", body="begin set @n = row_count(); end",
                   definition="CREATE TRIGGER `t_bi` BEFORE INSERT ON `t`"
                              " FOR EACH ROW begin set @n = row_count(); end")
    assert mysql_pg.instrument(trig) is None
    # a condition read by a number held in a variable
    body = "begin declare k int default 1; get diagnostics condition k" \
           " @m = message_text; end"
    r = Routine("procedure", "v", [], body=body,
                definition=f"CREATE PROCEDURE `v`() {body}")
    assert mysql_pg.instrument(r) is None
    # a stacked area is the handler's own copy: nothing to keep
    body = "begin declare exit handler for sqlexception get stacked" \
           " diagnostics condition 1 @m = message_text; select 1; end"
    r = Routine("procedure", "w", [], body=body,
                definition=f"CREATE PROCEDURE `w`() {body}")
    made, _, twin = mysql_pg.instrument(r)
    assert twin is None and "get stacked diagnostics" in made, made


# ---- a body cut into its statements -----------------------------------------------


def test_a_plsql_body_is_cut_by_its_blocks():
    body = ("PROCEDURE bump(p_id IN NUMBER) IS\n  v NUMBER;\nBEGIN\n"
            "  SELECT qty INTO v FROM items WHERE id = p_id;\n"
            "  IF v > 10 THEN\n    UPDATE items SET qty = 0 WHERE id ="
            " p_id;\n  ELSE\n    UPDATE items SET qty = qty + 1 WHERE id ="
            " p_id;\n  END IF;\n  FOR i IN 1..3 LOOP\n    NULL;\n"
            "  END LOOP;\n  INSERT INTO log VALUES (p_id);\n"
            "EXCEPTION WHEN OTHERS THEN NULL;\nEND;")
    cut = reduce._block_spans(body, "oracle")
    got = [body[a:b] for a, b in cut]
    assert got[0] == "SELECT qty INTO v FROM items WHERE id = p_id"
    # a statement that holds others goes or stays whole
    assert got[1].startswith("IF v > 10 THEN") and got[1].endswith("END IF")
    assert got[2].startswith("FOR i IN 1..3 LOOP") and \
        got[2].endswith("END LOOP")
    # the handlers are not statements of the block
    assert got[3] == "INSERT INTO log VALUES (p_id)" and len(got) == 4
    r = Routine("procedure", "BUMP", [Param("p_id", "NUMBER")], body=body,
                definition="create or replace " + body)
    kept = reduce._without(body, cut, {0, 3})
    assert "UPDATE" not in kept and "INSERT INTO log" in kept, kept
    made = reduce.renamed(r, "migkit_reduce_bump", kept,
                          lambda n: '"' + n.upper() + '"')
    assert made.startswith('create or replace PROCEDURE'
                           ' "MIGKIT_REDUCE_BUMP"(p_id IN NUMBER)'), made


def test_every_sources_copy_is_made_under_its_own_name():
    t = Routine("procedure", "up", [Param("i", "int")], body="BEGIN SELECT"
                " 1 END", definition="CREATE PROCEDURE dbo.up @i int AS"
                                     " BEGIN SELECT 1 END")
    assert reduce.renamed(t, "migkit_reduce_up", "BEGIN SELECT 2 END",
                          lambda n: f"[{n}]") == \
        "CREATE PROCEDURE dbo.[migkit_reduce_up] @i int AS BEGIN SELECT 2 END"
    body = "begin update t set a = 1; select 1; end"
    m = Routine("procedure", "p", [], body=body,
                definition=f"CREATE DEFINER=`root`@`%` PROCEDURE `p`() {body}")
    assert reduce.renamed(m, "migkit_reduce_p", "begin select 1; end",
                          lambda n: f"`{n}`") == \
        "CREATE DEFINER=`root`@`%` PROCEDURE `migkit_reduce_p`() begin" \
        " select 1; end"
    p = Routine("function", "f", [], body="\nbegin return 1; end\n",
                definition="CREATE OR REPLACE FUNCTION public.f()\n RETURNS"
                           " integer\n LANGUAGE plpgsql\nAS $function$\n"
                           "begin return 1; end\n$function$\n")
    made = reduce.renamed(p, "migkit_reduce_f", "\nbegin return 2; end\n",
                          lambda n: f'"{n}"')
    assert made.startswith('CREATE OR REPLACE FUNCTION public."migkit_redu'
                           'ce_f"()') and "return 2" in made, made
    # a body its definition does not hold cannot be cut
    assert reduce.renamed(Routine("function", "g", body="x",
                                  definition="y"), "n", "z", str) is None


def test_a_tsql_body_is_cut_by_its_own_parser():
    body = ("BEGIN\n SET NOCOUNT ON\n UPDATE t SET a = 1 WHERE id = @i\n"
            " DELETE FROM t WHERE id = 0\nEND")
    got = [body[a:b] for a, b in reduce._tsql_spans(body)]
    assert got == ["SET NOCOUNT ON", "UPDATE t SET a = 1 WHERE id = @i",
                   "DELETE FROM t WHERE id = 0"], got


# ---- effort -----------------------------------------------------------------------


CONVERTER_SAYS = """-- Function bump estimated cost: 7
\t-- SIZE => 1
\t-- FOR => 2
CREATE FUNCTION bump() ...
-- Total estimated cost: 7 units, 0.5 man-day(s)
"""


def test_the_converters_score_is_read_as_its_own_figure():
    assert effort.units(CONVERTER_SAYS) == (7.0, {"SIZE": 1.0, "FOR": 2.0})
    assert effort.units("CREATE FUNCTION f() ...") is None


def test_what_is_left_for_a_person_is_counted_not_guessed(tmp_path):
    rs = [Routine("procedure", "a", body="begin set @x = 1; end"),
          Routine("procedure", "b", body="begin update t set a = 1;"
                                         " select 1; delete from t; end"),
          Routine("procedure", "c", body="begin select 1; end"),
          Routine("procedure", "d", body="begin select 1; end")]
    records = [
        {"routine": "procedure a", "class": "proved"},
        {"routine": "procedure b", "class": "not converted",
         "reasons": ["migkit's rules: called with (x=1): ...",
                     "migkit's rules: the smallest part of the source that"
                     " still differs: update t set a = 1"]},
        {"routine": "procedure c", "class": "not converted",
         "reasons": ["no candidate proved"]},
        {"routine": "procedure d", "class": "proved with caveats",
         "evidence": {"reason": "on the target, never reached: line 4"
                                " (IF), line 9 (RETURN)"}}]
    got = effort.summary(records, rs, {"b": (12.0, {"SIZE": 2.0})},
                         {"a": 1.5, "b": 3.25}, "mysql")
    m = got["measured"]
    assert (m["routines"], m["proved"], m["proved with caveats"],
            m["not converted"]) == (4, 1, 1, 2)
    # b: only the part that still differs; c: its whole body
    assert got["per routine"]["b"]["statements left"] == 1
    assert got["per routine"]["c"]["statements left"] == 1
    assert m["statements left"] == 2 and m["lines to check"] == 2
    assert m["seconds"] == 4.75
    assert got["converter"]["units"] == 12.0
    assert got["converter"]["minutes"] == 60
    hop = _hop()
    hop.report_dir = lambda db=None: tmp_path
    import json
    (tmp_path / "code-conversion.json").write_text(json.dumps(
        {"effort": got}))
    line = effort.said(hop, "d")
    assert line.startswith("1 of 4 routines proved (1 with a caveat); left"
                           " for a person: 2 statements to write and 2"
                           " lines no call reached"), line
    assert "12 units (1.0 hours at 5 minutes a unit) over 1 routines" in \
        line, line
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    assert not [t for t in TOOLS if t in line.lower()], line


# ---- sizing -----------------------------------------------------------------------


def test_only_a_declaration_that_leaves_its_domain_open_is_sized():
    assert sizing.unbounded_text("mysql", "text")
    assert sizing.unbounded_text("mysql", "longtext")
    assert sizing.unbounded_text("postgres", "character varying")
    assert not sizing.unbounded_text("postgres", "character varying(20)")
    assert not sizing.unbounded_text("mysql", "varchar(20)")
    assert sizing.unbounded_text("mssql", "nvarchar(max)")
    assert sizing.bare_decimal("oracle", "NUMBER")
    assert not sizing.bare_decimal("oracle", "NUMBER(10,2)")
    assert sizing.bare_decimal("postgres", "numeric")
    assert not sizing.bare_decimal("mysql", "decimal")


class _Eng:
    def __init__(self, engine):
        self.CANON_ENGINE = engine


class _Pair:
    def __init__(self, tmp_path, on=True):
        self.hop = _hop()
        self.hop.options["types_from_data"] = on
        self.hop.report_dir = lambda db=None: tmp_path
        self.src_engine = _Eng("mysql")
        self.dst_engine = _Eng("postgres")


@pytest.mark.parametrize("longest,built", [(38, 64), (3, 16), (64, 64)])
def test_a_text_is_built_as_long_as_its_longest_value_rounded_up(
        tmp_path, monkeypatch, longest, built):
    pair = _Pair(tmp_path)
    monkeypatch.setattr(sizing, "_read", lambda *a: {
        "rows": 100, "values": 90, "most_chars": longest,
        "most_bytes": longest, "integral": True, "least": None,
        "most": None})
    got = sizing.sized(pair, "d", "codes", "code", "code", "text", "text",
                       ())
    assert got == ("text", (built,))
    kept = sizing.recorded(pair.hop, "d")["codes.code"]
    assert kept["length"] == built and kept["evidence"]["most_chars"] == \
        longest, kept
    assert kept["refuses"].startswith(f"a value longer than {built}")


def test_a_bare_decimal_holding_whole_numbers_is_built_as_one(
        tmp_path, monkeypatch):
    pair = _Pair(tmp_path)
    pair.src_engine = _Eng("oracle")
    ev = {"rows": 10, "values": 10, "most_chars": 0, "most_bytes": 0,
          "integral": True, "least": -5, "most": 2 ** 40}
    monkeypatch.setattr(sizing, "_read", lambda *a: dict(ev))
    assert sizing.sized(pair, "d", "t", "n", "n", "NUMBER", "decimal",
                        ()) == ("integer", ())
    # one fraction anywhere keeps the declared domain
    ev["integral"] = False
    assert sizing.sized(pair, "d", "t", "m", "m", "NUMBER", "decimal",
                        ()) == ("decimal", ())
    ev.update(integral=True, most=2 ** 63)
    assert sizing.sized(pair, "d", "t", "k", "k", "NUMBER", "decimal",
                        ()) == ("decimal", ())
    assert set(sizing.recorded(pair.hop, "d")) == {"t.n"}


def test_without_the_hop_option_nothing_is_sized_or_read(tmp_path,
                                                         monkeypatch):
    pair = _Pair(tmp_path, on=False)

    def never(*a):
        raise AssertionError("read the data though the hop did not ask")
    monkeypatch.setattr(sizing, "_read", never)
    assert sizing.sized(pair, "d", "codes", "code", "code", "text", "text",
                        ()) == ("text", ())
    assert sizing.recorded(pair.hop, "d") == {}


# ---- the sandbox keeps what a foreign key does ---------------------------------


class _Catalogue:
    """A hop's own server's catalogue, as each engine's reader asks it."""

    def __init__(self, engine, rows):
        self.CANON_ENGINE, self.rows = engine, rows

    def _catalog(self, side, db, sql):
        return self.rows

    def _rows(self, side, db, sql, args=None):
        return self.rows

    def _q(self, name):
        return f"[{name}]"

    def _qualified(self, side, db, table):
        return ".".join(self._q(p) for p in str(table).split(".", 1))

    def _owner(self, side, db):
        return "APP"

    def _said(self, name):
        return str(name).lower()


def test_a_sandboxs_foreign_keys_keep_what_a_delete_does():
    from migkit.storedcode.sandbox import foreign_key_statements
    pg = _Catalogue("postgres", [
        ("public.orders", "public.customers", "public.orders", "orders_fk",
         "FOREIGN KEY (cust) REFERENCES customers(id) ON DELETE CASCADE"),
        ("public.notes", "public.gone", "public.notes", "notes_fk",
         "FOREIGN KEY (g) REFERENCES gone(id)")])
    assert foreign_key_statements(pg, "dst", "d", ["public.orders",
                                                   "public.customers",
                                                   "public.notes"]) == [
        "alter table public.orders add constraint orders_fk FOREIGN KEY"
        " (cust) REFERENCES customers(id) ON DELETE CASCADE"]
    ms = _Catalogue("mssql", [
        ("fk_o", "dbo.orders", "dbo.customers", "cust", "id", "SET_NULL",
         "NO_ACTION")])
    assert foreign_key_statements(ms, "src", "d", ["dbo.orders",
                                                   "dbo.customers"]) == [
        "alter table [dbo].[orders] add constraint [fk_o] foreign key"
        " ([cust]) references [dbo].[customers] ([id]) on delete set null"
        " on update no action"]
    ora = _Catalogue("oracle", [
        ("FK_O", "ORDERS", "CUSTOMERS", "CUST", "ID", "CASCADE",
         "DEFERRABLE", "DEFERRED")])
    assert foreign_key_statements(ora, "src", "d", ["orders",
                                                    "customers"]) == [
        'alter table "ORDERS" add constraint "FK_O" foreign key ("CUST")'
        ' references "CUSTOMERS" ("ID") on delete cascade deferrable'
        " initially deferred"]
    # MySQL's are in its tables' own statements
    assert foreign_key_statements(_Catalogue("mysql", []), "src", "d",
                                  ["orders"]) == []


def test_mysqls_refusals_of_no_class_get_the_class_every_engine_says():
    import pymysql

    from migkit.storedcode.harness import MySQLRunner
    run = MySQLRunner.__new__(MySQLRunner)
    check = pymysql.err.OperationalError(
        3819, "Check constraint 'c' is violated.", sqlstate="HY000")
    assert run._error(check) == ("23514",
                                 "Check constraint 'c' is violated.")
    enum = pymysql.err.DataError(1265, "Data truncated for column 's'",
                                 sqlstate="01000")
    assert run._error(enum)[0] == "22P02"
    # a class of its own is kept as MySQL sent it
    dup = pymysql.err.IntegrityError(1062, "Duplicate entry",
                                     sqlstate="23000")
    assert run._error(dup)[0] == "23000"
    other = pymysql.err.OperationalError(1146, "Table 'x' doesn't exist",
                                         sqlstate="42S02")
    assert run._error(other)[0] == "42S02"


@pytest.mark.parametrize("body,made", [
    ("return now()", "return DATE_TRUNC('SECOND', LOCALTIMESTAMP);"),
    ("return now(3)", "return DATE_TRUNC('MILLISECONDS', LOCALTIMESTAMP);"),
    ("return current_timestamp(6)", "return LOCALTIMESTAMP;"),
    ("return sysdate()",
     "return DATE_TRUNC('SECOND', CAST(clock_timestamp() AS TIMESTAMP));"),
    ("return utc_timestamp()", "return DATE_TRUNC('SECOND', (CURRENT_TIMESTAMP"
                               " AT TIME ZONE 'UTC'));"),
    ("return curtime()",
     "return CAST(DATE_TRUNC('SECOND', LOCALTIMESTAMP) AS TIME);"),
    ("return unix_timestamp()",
     "return CAST(FLOOR(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)) AS BIGINT);")])
def test_mysqls_clock_is_cut_as_mysql_cuts_it(body, made):
    """Compared exactly, a PostgreSQL `now()` with its microseconds, or a
    `localtimestamp(0)` that rounds, is a difference from MySQL's
    `NOW()` - which is the statement's time cut to its digits."""
    from migkit.storedcode import mysql_pg
    got = mysql_pg.convert(Routine("function", "f", [], "datetime",
                                   body=body))
    assert made in got.statements[0], got.statements[0]
    assert ("mysql.clock", 1) in got.constructs, got.constructs


def test_a_clock_read_to_digits_no_rule_cuts_is_refused():
    from migkit.storedcode import mysql_pg
    from migkit.storedcode.translate import Refused
    with pytest.raises(Refused, match="reads the clock to 2 digits"):
        mysql_pg.convert(Routine("function", "f", [], "datetime",
                                 body="return now(2)"))


def test_a_call_in_a_mysql_body_is_read_and_measured():
    """The tokenizer keeps what follows CALL as one piece of text; read
    again, the call is a statement like any other - converted, and
    probed for coverage (a body with one was refused before)."""
    from migkit.storedcode import mysql_pg
    tree = mysql_pg.read("begin call db.p(1, 'a b', x); set @y = 2; end")
    calls = [n for n in tree.walk() if n.kind == "call"]
    assert [(n.name, n.args) for n in calls] == [("p", ["1", "'a b'", "x"])]
    body = "begin call inner(x); select row_count(); end"
    r = Routine("procedure", "q", [Param("x", "int")], body=body,
                definition=f"CREATE PROCEDURE `q`(x int) {body}")
    made, probes, twin = mysql_pg.instrument(r)
    assert twin == "migkit_cover_q" and len(probes) == 1
    # the count a call leaves is kept after it, as any statement's
    assert "call inner(x); set _migkit_rc = row_count();" in made, made
