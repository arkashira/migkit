"""Oracle's PL/SQL by migkit's own rules (backlog W3): standalone
routines, triggers and whole packages - their state, cursors, exceptions
and autonomous transactions - each version held here to PostgreSQL's own
parser and to the meaning rules that make it; what it does is the proof's,
on Oracle itself (`test_oracle_packages_converted_and_proved.py`).
"""
import pytest

from migkit.storedcode import packages, plsql_pg
from migkit.storedcode.plpgsql import unparsed
from migkit.storedcode.routines import Param, Routine
from migkit.storedcode.translate import Refused


class Ctx:
    local = None
    tables = {}
    routines = {}
    emulated = set()
    packages = set()


def _made(r, ctx=None):
    got = plsql_pg.convert(r, ctx or Ctx())
    assert unparsed(got.statements) is None, got.sql
    return got


NET_PRICE = """FUNCTION net_price (p_id IN NUMBER, p_disc IN NUMBER DEFAULT 0)
RETURN NUMBER IS
  v_price NUMBER(10,2);
  v_name VARCHAR2(40) := '';
BEGIN
  SELECT price, name INTO v_price, v_name FROM items WHERE id = p_id;
  UPDATE items SET seen = seen + 1 WHERE id = p_id;
  v_name := v_name || ' (' || SUBSTR(v_name, 0, 3) || ')';
  RETURN ROUND(v_price * (100 - p_disc) / 100, 2);
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN -1;
  WHEN TOO_MANY_ROWS OR VALUE_ERROR THEN
    RAISE_APPLICATION_ERROR(-20001, 'bad item ' || p_id);
END net_price;"""


def test_a_function_keeps_oracles_meaning_not_only_its_syntax():
    got = _made(Routine(kind="function", name="net_price",
                        params=[Param("p_id", "NUMBER"),
                                Param("p_disc", "NUMBER")],
                        returns="NUMBER", body=NET_PRICE))
    sql = got.sql
    # no row and two rows raise, as Oracle's SELECT INTO does
    assert "INTO STRICT v_price, v_name" in sql
    # a handler keeps what the statements before the failing one did:
    # each runs in a block of its own, and the error is raised again where
    # only the handlers catch it
    assert sql.count("exit _migkit_blk1;") >= 3
    assert "raise exception using errcode = _migkit_state" in sql
    assert "when no_data_found then" in sql
    assert ("when too_many_rows or invalid_text_representation or"
            " numeric_value_out_of_range") in sql
    # an empty string is NULL; || is NULL only where every piece is; a
    # start of 0 is 1; / divides exactly
    assert "v_name varchar(40) := nullif(NULL, '')" in sql
    assert "concat(v_name, ' (', NULLIF(substr(v_name, 1, 3), ''), ')')" \
        in sql, sql
    assert "CAST(v_price * (100 - p_disc) AS DECIMAL) / 100" in sql
    # Oracle's error number rides with the message
    assert "hint = 'ORA-20001'" in sql
    assert ("plsql.exception-block", 1) in got.constructs


MOVE = """PROCEDURE move_stock(p_from IN NUMBER, p_qty IN NUMBER,
                       p_done OUT NUMBER) IS
  CURSOR c_items(p_min NUMBER) IS SELECT id, qty FROM items
    WHERE qty >= p_min ORDER BY id;
  v_id items.id%TYPE;
  v_qty NUMBER;
  e_short EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_short, -20002);
BEGIN
  p_done := 0;
  UPDATE items SET qty = qty - p_qty WHERE id = p_from;
  IF SQL%ROWCOUNT = 0 THEN
    RAISE NO_DATA_FOUND;
  END IF;
  OPEN c_items(10);
  LOOP
    FETCH c_items INTO v_id, v_qty;
    EXIT WHEN c_items%NOTFOUND;
    p_done := p_done + 1;
  END LOOP;
  CLOSE c_items;
  FOR r IN (SELECT id FROM items WHERE qty < 0) LOOP
    INSERT INTO alerts (item_id, made) VALUES (r.id, SYSDATE);
  END LOOP;
  FOR i IN REVERSE 1..3 LOOP
    DBMS_OUTPUT.PUT_LINE('step ' || i);
  END LOOP;
EXCEPTION
  WHEN e_short THEN
    p_done := -1;
END;"""


def test_cursors_attributes_and_named_errors_are_kept_apart():
    got = _made(Routine(kind="procedure", name="move_stock",
                        params=[Param("p_from", "NUMBER"),
                                Param("p_qty", "NUMBER"),
                                Param("p_done", "NUMBER", "out")],
                        body=MOVE))
    sql = got.sql
    assert "out p_done numeric" in sql
    assert "c_items cursor(p_min numeric) for SELECT" in sql
    assert "v_id items.id%type;" in sql
    # SQL%ROWCOUNT and a cursor's %NOTFOUND, each in a variable of its own
    assert "get diagnostics _migkit_sql_rowcount = row_count;" in sql
    assert "if _migkit_sql_rowcount = 0 then" in sql
    assert "exit when (NOT c_items_found);" in sql
    # a fetch past the last row leaves the variables as they were
    assert "fetch c_items into _migkit_f" in sql and "if found then" in sql
    # a loop over a query declares its record; REVERSE counts down from
    # the high bound
    assert "r record;" in sql and "for i in reverse 3..1 loop" in sql
    assert "localtimestamp(0)" in sql
    # an application error by its number: another is not this handler's
    assert "when sqlstate 'P0001' then" in sql
    assert "is distinct from 'ORA-20002' then" in sql
    assert ("plsql.cursor", 4) in got.constructs or any(
        c == "plsql.cursor" for c, _ in got.constructs)


def test_a_commit_inside_a_block_that_handles_errors_is_refused():
    with pytest.raises(Refused, match="cannot commit there"):
        _made(Routine(kind="procedure", name="p", body=MOVE.replace(
            "  FOR i IN REVERSE", "  COMMIT;\n  FOR i IN REVERSE"),
            params=[Param("p_from", "NUMBER"), Param("p_qty", "NUMBER"),
                    Param("p_done", "NUMBER", "out")]))


def test_what_has_no_counterpart_is_refused_by_name():
    for body, said in (
            ("PROCEDURE p IS BEGIN EXECUTE IMMEDIATE 'drop table t'; END;",
             "built at run time"),
            ("PROCEDURE p IS BEGIN GOTO done; <<done>> NULL; END;",
             "GOTO"),
            ("PROCEDURE p IS TYPE t_ids IS TABLE OF NUMBER; BEGIN NULL;"
             " END;", "record or a collection"),
            ("FUNCTION f RETURN NUMBER IS BEGIN RETURN SQLCODE; END;",
             "SQLCODE"),
            ("PROCEDURE p IS BEGIN FOR r IN (SELECT id FROM t) LOOP NULL;"
             " END LOOP; SAVEPOINT a; END;", "SAVEPOINT")):
        with pytest.raises(Refused, match=said):
            kind = "function" if body.startswith("FUNCTION") else \
                "procedure"
            plsql_pg.convert(Routine(kind=kind, name="p", body=body,
                                     returns="NUMBER"), Ctx())


def test_a_row_trigger_reads_its_rows_and_says_which_statement_fired():
    got = _made(Routine(kind="trigger", name="orders_bi", table="orders",
                        timing="BEFORE", events=("INSERT", "UPDATE"),
                        level="row", body="""TRIGGER orders_bi
BEFORE INSERT OR UPDATE ON orders
FOR EACH ROW
BEGIN
  IF INSERTING THEN
    :NEW.id := orders_seq.NEXTVAL;
  END IF;
  :NEW.updated := SYSDATE;
END;"""))
    sql = got.sql
    assert "new.id := nextval('orders_seq');" in sql
    assert "if (tg_op = 'INSERT') then" in sql
    assert sql.rstrip(";").endswith(
        "create trigger orders_bi before insert or update on orders for"
        " each row execute function orders_bi_fn()")
    assert "return new;\nend" in sql


def test_an_autonomous_procedure_commits_over_a_loopback_of_its_own():
    got = _made(Routine(kind="procedure", name="log_it",
                        params=[Param("p_msg", "VARCHAR2")],
                        body="""PROCEDURE log_it(p_msg VARCHAR2) IS
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  INSERT INTO app_log (msg) VALUES (p_msg);
  COMMIT;
END;"""))
    assert got.statements[0] == "create extension if not exists dblink"
    assert "procedure log_it__autonomous(p_msg varchar)" in got.sql
    assert "dblink_exec(" in got.statements[2]
    assert "quote_nullable(p_msg) || '::varchar'" in got.statements[2]
    assert ("plsql.autonomous", 1) in got.constructs
    with pytest.raises(Refused, match="autonomous function"):
        plsql_pg.convert(Routine(kind="function", name="f", body="""FUNCTION
f RETURN NUMBER IS PRAGMA AUTONOMOUS_TRANSACTION; BEGIN RETURN 1; END;""",
            returns="NUMBER"), Ctx())


SPEC = """PACKAGE counter AS
  g_count NUMBER := 0;
  c_step CONSTANT NUMBER := 2;
  e_full EXCEPTION;
  -- FUNCTION hidden(x NUMBER) RETURN NUMBER;
  FUNCTION next_value RETURN NUMBER;
  PROCEDURE reset_to(p_value NUMBER);
END counter;"""
BODY = """PACKAGE BODY counter AS
  g_calls PLS_INTEGER := 0;
  FUNCTION helper RETURN NUMBER IS BEGIN RETURN 1; END;
  FUNCTION next_value RETURN NUMBER IS
  BEGIN
    g_calls := g_calls + helper;
    IF g_count >= 100 THEN
      RAISE e_full;
    END IF;
    g_count := g_count + c_step;
    RETURN g_count;
  END next_value;
  PROCEDURE reset_to(p_value NUMBER) IS
  BEGIN
    g_count := p_value;
  END reset_to;
BEGIN
  g_count := 10;
END counter;"""


def _member(name, kind):
    return Routine(kind=kind, name=f"counter.{name}", body="",
                   settings={"package": "counter", "spec": SPEC,
                             "package body": BODY})


def test_a_package_is_a_schema_with_its_state_kept_for_the_session():
    assert packages.public(SPEC) == [("next_value", "function"),
                                     ("reset_to", "procedure")]
    text, lines = packages.member_text(BODY, "next_value")
    assert text.startswith("FUNCTION next_value") and lines == (4, 12)
    got = _made(_member("next_value", "function"))
    sql = got.sql
    assert got.statements[0] == "create schema if not exists counter"
    # set to its first values, and the initialisation run, once a session
    assert "if current_setting('migkit_counter._init', true) = 'y'" in sql
    assert "perform counter._migkit_set_g_count(10);" in sql
    # read and written through the package's own functions
    assert ("perform counter._migkit_set_g_count(counter._migkit_get_g_count"
            "() + counter._migkit_get_c_step());") in sql
    assert "returns integer language plpgsql" in sql
    # a constant is no setting
    assert "_migkit_set_c_step" not in sql
    # a private subprogram is the package's too, and called there
    assert "function counter.helper()" in sql
    assert "counter._migkit_get_g_calls() + counter.helper()" in sql
    # the package's exception keeps one code wherever it is raised
    code = plsql_pg.user_code("counter", "e_full")
    assert f"errcode = '{code}'" in sql and code.startswith("UE")
    assert got.residue and "session settings" in got.residue[0]
    # every member is made from the package whole: the same statements
    ctx = Ctx()
    first = plsql_pg.convert(_member("next_value", "function"), ctx)
    again = plsql_pg.convert(_member("reset_to", "procedure"), ctx)
    assert first is again


def test_a_package_cursor_or_record_is_refused():
    spec = SPEC.replace("e_full EXCEPTION;", "e_full EXCEPTION;\n  CURSOR"
                        " c IS SELECT 1 FROM dual;")
    with pytest.raises(Refused, match="cursor c"):
        plsql_pg.convert(Routine(kind="function", name="counter.next_value",
                                 settings={"package": "counter",
                                           "spec": spec,
                                           "package body": BODY}), Ctx())


def test_the_targets_routines_of_a_package_meet_the_sources_members():
    dst = [Routine(kind="function", name="next_value",
                   settings={"schema": "counter"}),
           Routine(kind="function", name="next_value",
                   settings={"schema": "public"})]
    packages.qualify(dst, {"counter"})
    assert dst[0].name == "counter.next_value" and \
        dst[0].settings["package"] == "counter"
    assert dst[1].name == "next_value"
    assert dst[0].key == ("function", "counter.next_value")


def test_an_oracle_body_that_commits_or_is_autonomous_commits_itself():
    from migkit.storedcode.prove import commits_itself
    assert commits_itself("oracle", Routine(
        kind="procedure", name="p", body="PROCEDURE p IS BEGIN"
        " INSERT INTO t VALUES (1); COMMIT; END;"))
    assert commits_itself("oracle", Routine(
        kind="procedure", name="p", body="PROCEDURE p IS PRAGMA"
        " AUTONOMOUS_TRANSACTION; BEGIN NULL; END;"))
    assert not commits_itself("oracle", Routine(
        kind="function", name="f", body="FUNCTION f RETURN NUMBER IS BEGIN"
        " RETURN 1; END;"))
    assert commits_itself("postgres", Routine(
        kind="procedure", name="p", body="begin perform dblink_exec('x',"
        " 'call y()'); end"))
