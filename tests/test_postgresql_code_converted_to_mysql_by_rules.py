"""PostgreSQL's PL/pgSQL carried to MySQL by migkit's own rules
(`storedcode.pg_mysql`), the mirror of the MySQL-to-PL/pgSQL rules.

The first half holds the rules to the MySQL they write, on routines built
as the PostgreSQL engine reads them from its catalogue: what keeps
PostgreSQL's meaning where MySQL's statement of the same name means
something else (an integer division, a SELECT INTO finding no row or
several, RAISE's message, a FOR loop's bounds, an exception block, a
trigger of two events), what is refused and why, and the offsets and the
statement the reducer cuts a body by. The second half proves the rules'
versions by running them: a PostgreSQL source, a MySQL target and a
sandbox of each, every routine the rules carry proved on the sandbox's
fixture and one they refuse left for a person.
"""
import json
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from migkit.storedcode import pg_mysql
from migkit.storedcode.mysql_pg import Context
from migkit.storedcode.routines import Param, Routine
from migkit.storedcode.translate import Refused

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("schema.routines", "postgres")]

# ---- routines as the PostgreSQL engine reads them --------------------------

_VOLATILE = {"NO SQL": " IMMUTABLE", "READS SQL DATA": " STABLE",
             "MODIFIES SQL DATA": ""}


def _routine(kind, name, params, returns, body, data="MODIFIES SQL DATA",
             options=""):
    """A function or procedure as `code_routines` makes it: parameters
    typed without a modifier, the body as `prosrc` keeps it, the whole
    statement as `pg_get_functiondef` writes it."""
    said = ", ".join(f"{'' if m == 'in' else m.upper() + ' '}{n} {t}"
                     for n, t, m in params)
    head = f"CREATE OR REPLACE {kind.upper()} public.{name}({said})\n"
    if kind == "function":
        head += f" RETURNS {returns}\n"
    head += f" LANGUAGE plpgsql{_VOLATILE[data] if kind == 'function' else ''}"
    head += f"{options}\nAS $function${body}$function$\n"
    return Routine(kind=kind, name=name,
                   params=[Param(n, t, m) for n, t, m in params],
                   returns=returns if kind == "function" else None,
                   body=body, definition=head, language="plpgsql",
                   settings={"schema": "public", "oid": 1, "data": data})


def _trigger(name, table, timing, events, body, level="row"):
    """A trigger as `code_routines` makes it: its function's body, and the
    function's statement then the trigger's for a definition."""
    function = (f"CREATE OR REPLACE FUNCTION public.{name}()\n RETURNS"
                f" trigger\n LANGUAGE plpgsql\nAS $function${body}"
                "$function$\n")
    made = (f"CREATE TRIGGER {name} {timing} {' OR '.join(events)} ON"
            f" public.{table} FOR EACH"
            f" {'ROW' if level == 'row' else 'STATEMENT'} EXECUTE FUNCTION"
            f" {name}()")
    return Routine(kind="trigger", name=name, table=f"public.{table}",
                   timing=timing, events=tuple(events), level=level,
                   body=body, definition=function.rstrip() + ";\n" + made
                   + ";", language="plpgsql",
                   settings={"function": name, "schema": "public",
                             "oid": 2})


RATIO = _routine("function", "ratio", [("a", "integer", "in"),
                                       ("b", "integer", "in")], "integer", """
begin
  return a / b;
end
""", "NO SQL")

OWNER_OF = _routine("function", "owner_of", [("p_id", "integer", "in")],
                    "text", """
declare
  v text;
begin
  select owner into v from accounts where id = p_id;
  return coalesce(v, '?');
end
""", "READS SQL DATA")

STRICT_OWNER = _routine("function", "strict_owner",
                        [("p_min", "numeric", "in")], "text", """
declare
  v text;
begin
  select owner into strict v from accounts where balance > p_min;
  return v;
end
""", "READS SQL DATA")

DEPOSIT = _routine("procedure", "deposit", [("p_id", "integer", "in"),
                                            ("p_amount", "numeric", "in")],
                   None, """
begin
  if p_amount <= 0 then
    raise exception 'amount % must be positive (100%%)', p_amount;
  end if;
  update accounts set balance = balance + p_amount where id = p_id;
end
""")

SUM_TO = _routine("function", "sum_to", [("n", "integer", "in")], "bigint",
                  """
declare
  s bigint := 0;
begin
  if n > 100 then
    n := 100;
  end if;
  for i in 1..n loop
    s := s + i;
  end loop;
  return s;
end
""", "NO SQL")

SAFE_RATIO = _routine("function", "safe_ratio", [("a", "integer", "in"),
                                                 ("b", "integer", "in")],
                      "integer", """
begin
  return a / b;
exception
  when division_by_zero then
    return null;
end
""", "NO SQL")

AUDIT = _trigger("accounts_audit", "accounts", "AFTER", ["INSERT", "UPDATE"],
                 """
begin
  if tg_op = 'INSERT' then
    insert into ledger_log (account_id, note) values (new.id, 'new');
  else
    insert into ledger_log (account_id, note)
      values (new.id, tg_op || ' ' || old.owner);
  end if;
  return null;
end
""")

LOG_OR_FAIL = _routine("procedure", "log_or_fail",
                       [("p_id", "integer", "in")], None, """
begin
  begin
    insert into ledger_log (account_id, note) values (p_id, 'tried');
    perform 1 / (p_id - p_id);
  exception when division_by_zero then
    raise notice 'nothing kept';
  end;
end
""")


def _one(routine, ctx=None):
    made = pg_mysql.convert(routine, ctx)
    assert len(made.statements) == 1, made.statements
    return made.statements[0]


# ---- the rules' MySQL -----------------------------------------------------


def test_an_integer_division_is_div_and_a_zero_divisor_raises():
    # MySQL's `/` answers 3.5000 for 7 / 2 and NULL for a zero divisor;
    # PostgreSQL answers 3 and raises division_by_zero
    assert _one(RATIO) == """\
CREATE FUNCTION `ratio`(a INT, b INT) RETURNS INT
  DETERMINISTIC NO SQL SQL SECURITY INVOKER
BEGIN
  _migkit_body: BEGIN
    IF b = 0 THEN
      SIGNAL SQLSTATE '22012' SET MESSAGE_TEXT = 'division by zero';
    END IF;
    RETURN a DIV b;
  END _migkit_body;
END"""


def test_a_division_whose_operands_are_not_typed_is_refused():
    r = _routine("function", "halves", [("a", "integer", "in")], "integer",
                 "\nbegin\n  return (select max(id) from accounts) / a;"
                 "\nend\n", "READS SQL DATA")
    with pytest.raises(Refused, match="line 3 divides values whose types"
                                      " are not known here"):
        pg_mysql.convert(r)


def test_select_into_sets_its_targets_to_null_first_and_takes_one_row():
    assert _one(OWNER_OF) == """\
CREATE FUNCTION `owner_of`(p_id INT) RETURNS LONGTEXT CHARACTER SET\
 utf8mb4 COLLATE utf8mb4_0900_bin
  NOT DETERMINISTIC READS SQL DATA SQL SECURITY INVOKER
BEGIN
  _migkit_body: BEGIN
    DECLARE v LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
    SET v = NULL;
    SELECT owner FROM accounts WHERE id = p_id LIMIT 1 INTO v;
    RETURN COALESCE(v, '?');
  END _migkit_body;
END"""


def test_select_into_strict_raises_postgresqls_codes_and_messages():
    made = pg_mysql.convert(STRICT_OWNER)
    assert made.statements[0] == """\
CREATE FUNCTION `strict_owner`(p_min DECIMAL(65,30)) RETURNS LONGTEXT\
 CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin
  NOT DETERMINISTIC READS SQL DATA SQL SECURITY INVOKER
BEGIN
  DECLARE _migkit_got1 BOOLEAN DEFAULT FALSE;
  _migkit_body: BEGIN
    DECLARE v LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
    SET _migkit_got1 = FALSE;
    BEGIN
      DECLARE EXIT HANDLER FOR 1172 SIGNAL SQLSTATE 'P0003' SET\
 MESSAGE_TEXT = 'query returned more than one row';
      SELECT TRUE, owner FROM accounts WHERE balance > p_min INTO\
 _migkit_got1, v;
    END;
    IF NOT _migkit_got1 THEN
      SIGNAL SQLSTATE 'P0002' SET MESSAGE_TEXT = 'query returned no rows';
    END IF;
    RETURN v;
  END _migkit_body;
END"""
    # a numeric of no declared precision is said, not held silently
    assert any("DECIMAL(65,30)" in n for n in made.notes), made.notes


def test_raise_writes_each_percent_as_postgresql_writes_its_argument():
    made = pg_mysql.convert(DEPOSIT)
    assert made.statements[0] == """\
CREATE PROCEDURE `deposit`(IN p_id INT, IN p_amount DECIMAL(65,30))
  MODIFIES SQL DATA SQL SECURITY INVOKER
BEGIN
  DECLARE _migkit_msg1 TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
  _migkit_body: BEGIN
    IF p_amount <= 0 THEN
      SET _migkit_msg1 = CONCAT('amount ', COALESCE(TRIM(TRAILING '.' FROM\
 TRIM(TRAILING '0' FROM CAST(p_amount AS CHAR))), '<NULL>'), ' must be\
 positive (100%)');
      SIGNAL SQLSTATE 'P0001' SET MESSAGE_TEXT = _migkit_msg1;
    END IF;
    UPDATE accounts SET balance = balance + p_amount WHERE id = p_id;
  END _migkit_body;
END"""
    assert ("postgres.raise", 4) in made.constructs, made.constructs
    assert any("128 characters" in n for n in made.notes), made.notes
    # one write, the last statement: nothing before it to roll back
    assert "SAVEPOINT" not in made.statements[0]


def test_a_procedure_writing_before_it_can_fail_rolls_back_to_a_savepoint():
    r = _routine("procedure", "move", [("a", "integer", "in"),
                                       ("b", "integer", "in")], None, """
begin
  update accounts set balance = balance - 1 where id = a;
  update accounts set balance = balance + 1 where id = b;
end
""")
    made = _one(r)
    assert """\
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    IF _migkit_sp1 THEN
      ROLLBACK TO SAVEPOINT _migkit_sp_move;
    END IF;
    RESIGNAL;
  END;
  IF @@in_transaction = 1 THEN
    SAVEPOINT _migkit_sp_move;
    SET _migkit_sp1 = TRUE;
  END IF;""" in made, made


def test_an_integer_for_loop_evaluates_its_bounds_once():
    assert _one(SUM_TO) == """\
CREATE FUNCTION `sum_to`(n INT) RETURNS BIGINT
  DETERMINISTIC NO SQL SQL SECURITY INVOKER
BEGIN
  DECLARE _migkit_lo1 INT;
  DECLARE _migkit_hi2 INT;
  DECLARE _migkit_i3 BIGINT;
  _migkit_body: BEGIN
    DECLARE s BIGINT DEFAULT 0;
    IF n > 100 THEN
      SET n = 100;
    END IF;
    SET _migkit_lo1 = 1, _migkit_hi2 = n;
    IF _migkit_hi2 IS NULL THEN
      SIGNAL SQLSTATE '22004' SET MESSAGE_TEXT = 'upper bound of FOR loop\
 cannot be null';
    END IF;
    BEGIN
      DECLARE i INT;
      SET _migkit_i3 = _migkit_lo1 - 1;
      _migkit_l4: LOOP
        SET _migkit_i3 = _migkit_i3 + 1;
        IF _migkit_i3 > _migkit_hi2 THEN
          LEAVE _migkit_l4;
        END IF;
        SET i = _migkit_i3;
        SET s = s + i;
      END LOOP _migkit_l4;
    END;
    RETURN s;
  END _migkit_body;
END"""


def test_an_exception_block_that_writes_nothing_is_an_exit_handler():
    assert _one(SAFE_RATIO) == """\
CREATE FUNCTION `safe_ratio`(a INT, b INT) RETURNS INT
  DETERMINISTIC NO SQL SQL SECURITY INVOKER
BEGIN
  _migkit_body: BEGIN
    DECLARE EXIT HANDLER FOR 1365, SQLSTATE '22012'
    BEGIN
      RETURN NULL;
    END;
    IF b = 0 THEN
      SIGNAL SQLSTATE '22012' SET MESSAGE_TEXT = 'division by zero';
    END IF;
    RETURN a DIV b;
  END _migkit_body;
END"""


def test_a_trigger_of_two_events_is_one_per_event_its_tg_op_a_word():
    made = pg_mysql.convert(AUDIT)
    assert made.statements == ["""\
CREATE TRIGGER `accounts_audit` AFTER INSERT ON accounts FOR EACH ROW
BEGIN
  _migkit_body: BEGIN
    INSERT INTO ledger_log (account_id, note) VALUES (NEW.id, 'new');
  END _migkit_body;
END""", """\
CREATE TRIGGER `accounts_audit_update` AFTER UPDATE ON accounts FOR EACH ROW
BEGIN
  _migkit_body: BEGIN
    INSERT INTO ledger_log (account_id, note) VALUES (NEW.id, CONCAT('UPDATE',\
 ' ', OLD.owner));
  END _migkit_body;
END"""]
    assert "accounts_audit_update for UPDATE" in " ".join(made.notes), \
        made.notes


def test_on_conflict_on_the_tables_one_key_is_on_duplicate_key():
    r = _routine("procedure", "keep_owner", [("p_id", "integer", "in"),
                                             ("p_owner", "text", "in")],
                 None, """
begin
  insert into accounts (id, owner) values (p_id, p_owner)
    on conflict (id) do update set owner = excluded.owner;
end
""")
    made = _one(r, Context(keys={"accounts": [["id"]]}))
    assert "    INSERT INTO accounts (id, owner) VALUES (p_id, p_owner) AS" \
           " _migkit_new ON DUPLICATE KEY UPDATE owner =" \
           " _migkit_new.owner;\n" in made, made


# ---- what is refused, and why ---------------------------------------------


def test_an_exception_block_that_writes_is_refused():
    with pytest.raises(Refused, match="line 4 writes inside a block with an"
                                      " EXCEPTION handler: PostgreSQL rolls"
                                      " back everything"):
        pg_mysql.convert(LOG_OR_FAIL)


def test_return_null_in_a_before_trigger_is_refused():
    r = _trigger("skip_negative", "accounts", "BEFORE", ["INSERT"], """
begin
  if new.balance < 0 then
    return null;
  end if;
  return new;
end
""")
    with pytest.raises(Refused, match="line 4: a BEFORE trigger returning"
                                      " NULL skips the row in PostgreSQL;"
                                      " a MySQL trigger cannot skip a row"):
        pg_mysql.convert(r)


def test_a_function_returning_a_set_is_refused():
    r = _routine("function", "ids", [], "integer",
                 "\nbegin\n  return next 1;\nend\n", "NO SQL")
    r.returns_set = True
    with pytest.raises(Refused, match="it returns a set of rows"):
        pg_mysql.convert(r)


def test_a_statement_level_trigger_is_refused():
    r = _trigger("count_rows", "accounts", "AFTER", ["INSERT"],
                 "\nbegin\n  return null;\nend\n", level="statement")
    with pytest.raises(Refused, match="it is a statement-level trigger"):
        pg_mysql.convert(r)


def test_on_conflict_on_a_table_of_two_unique_keys_is_refused():
    r = _routine("procedure", "keep_tag", [("p_name", "text", "in")], None,
                 """
begin
  insert into tags (name, hits) values (p_name, 1)
    on conflict (name) do update set hits = tags.hits + 1;
end
""")
    ctx = Context(keys={"tags": [["id"], ["name"]]})
    with pytest.raises(Refused, match="line 3: ON CONFLICT on tags, which has"
                                      " 2 unique keys here, not one"):
        pg_mysql.convert(r, ctx)


def test_a_body_postgresql_refuses_is_refused():
    # PL/pgSQL ends an IF's condition at the first THEN, a CASE's included
    r = _routine("function", "odd", [("a", "integer", "in")], "integer", """
begin
  if case when a > 0 then true else false end then
    return 1;
  end if;
  return 0;
end
""", "NO SQL")
    with pytest.raises(Refused, match="PostgreSQL's own parser refuses its"
                                      " body"):
        pg_mysql.convert(r)


# ---- what the reducer cuts a body by ----------------------------------------


def test_spans_cut_the_outermost_block_into_its_statements():
    body = SUM_TO.body
    got = [body[a:b] for a, b in pg_mysql.spans(body)]
    assert got == ["if n > 100 then\n    n := 100;\n  end if",
                   "for i in 1..n loop\n    s := s + i;\n  end loop",
                   "return s"]
    # a comment's or a string's semicolon is not a statement's end
    body = "\nbegin\n  perform 'a;b'; -- c; d\n  return 1;\nend\n"
    assert [body[a:b] for a, b in pg_mysql.spans(body)] == [
        "perform 'a;b'", "return 1"]
    assert pg_mysql.spans(" select 1 ") == []


def test_definition_makes_the_routine_again_under_another_name():
    import pglast
    from pglast.enums import FunctionParameterMode
    cut = "\nbegin\n  return a;\nend\n"
    made = pg_mysql.definition(RATIO, "migkit_reduce_ratio", cut)
    stmt = pglast.parse_sql(made)[0].stmt
    assert [n.sval for n in stmt.funcname] == ["migkit_reduce_ratio"]
    assert [(p.name, p.mode) for p in stmt.parameters] == [
        ("a", FunctionParameterMode.FUNC_PARAM_DEFAULT),
        ("b", FunctionParameterMode.FUNC_PARAM_DEFAULT)]
    options = {o.defname: o.arg for o in stmt.options}
    assert options["language"].sval == "plpgsql"
    assert options["volatility"].sval == "immutable"
    assert options["as"][0].sval == cut
    # and its body is PL/pgSQL PostgreSQL's parser reads
    assert pglast.parse_plpgsql(made)[0]["PLpgSQL_function"]["action"]
    made = pg_mysql.definition(DEPOSIT, "Cut", DEPOSIT.body)
    stmt = pglast.parse_sql(made)[0].stmt
    assert stmt.is_procedure and [n.sval for n in stmt.funcname] == ["Cut"]


# ---- proved by running it --------------------------------------------------

PG, MY = "migkit-test-pgmy-pg", "migkit-test-pgmy-my"
SB_PG, SB_MY = "migkit-test-pgmy-sbpg", "migkit-test-pgmy-sbmy"
PG_PORT, MY_PORT, SB_PG_PORT, SB_MY_PORT = 16290, 16291, 16292, 16293
MY_SMALL = ["--log-bin-trust-function-creators=1",
            "--performance-schema=OFF", "--innodb-buffer-pool-size=16M"]
DB = "pgmy"


def _sh(*a, **kw):
    return subprocess.run(list(a), capture_output=True, text=True, **kw)


def _docker():
    try:
        return _sh("docker", "info", timeout=10).returncode == 0
    except Exception:
        return False


def _pg(sql, db=DB):
    got = _sh("docker", "exec", "-i", PG, "psql", "-U", "postgres", "-d", db,
              "-At", "-v", "ON_ERROR_STOP=1", input=sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _my(*statements, db=DB):
    import pymysql
    conn = pymysql.connect(host="127.0.0.1", port=MY_PORT, user="root",
                           password="test", database=db, autocommit=True)
    try:
        with conn.cursor() as cur:
            for s in statements:
                cur.execute(s)
            return cur.fetchall()
    finally:
        conn.close()


@pytest.fixture(scope="module")
def servers():
    if not _docker():
        pytest.skip("docker not available")
    from migkit.storedcode.sandbox import PG_DOCKERFILE, pg_tag
    tag = pg_tag(16)
    if _sh("docker", "image", "inspect", tag).returncode != 0:
        assert _sh("docker", "build", "-t", tag, "-", timeout=900,
                   input=PG_DOCKERFILE.format(v=16)).returncode == 0
    for n, port, image in ((PG, PG_PORT, "postgres:16"),
                           (SB_PG, SB_PG_PORT, tag)):
        _sh("docker", "rm", "-f", "-v", n)
        _sh("docker", "run", "-d", "--name", n, "-e",
            "POSTGRES_PASSWORD=test", "-p", f"127.0.0.1:{port}:5432", image)
    for n, port in ((MY, MY_PORT), (SB_MY, SB_MY_PORT)):
        _sh("docker", "rm", "-f", "-v", n)
        _sh("docker", "run", "-d", "--name", n, "-e",
            "MYSQL_ROOT_PASSWORD=test", "-p", f"127.0.0.1:{port}:3306",
            "mysql:8.4", *MY_SMALL)
    try:
        for n in (PG, SB_PG):
            for _ in range(60):
                if _sh("docker", "exec", n, "psql", "-U", "postgres", "-c",
                       "select 1").returncode == 0:
                    break
                time.sleep(1)
        for n in (MY, SB_MY):
            for _ in range(90):
                if _sh("docker", "exec", n, "mysql", "-uroot", "-ptest",
                       "-h127.0.0.1", "--protocol=tcp", "-e",
                       "select 1").returncode == 0:
                    break
                time.sleep(2)
        for port in (PG_PORT, MY_PORT, SB_PG_PORT, SB_MY_PORT):
            with socket.socket() as s:
                s.settimeout(5)
                assert s.connect_ex(("127.0.0.1", port)) == 0
        _pg(f"create database {DB}", db="postgres")
        _pg("""
            create table accounts (id integer primary key, owner varchar(20)
              not null, balance numeric(10,2) not null default 0);
            create table ledger_log (id integer generated by default as
              identity primary key, account_id integer not null,
              note varchar(60) not null);
            insert into accounts values (1, 'ann', 100.00), (2, 'Bob', 0),
              (3, 'cy', 250.50);
            create function balance_of(p_id integer) returns numeric
              language sql stable as
              $$ select balance from accounts where id = p_id $$;
            """)
        for r in (RATIO, OWNER_OF, STRICT_OWNER, DEPOSIT, SUM_TO, SAFE_RATIO,
                  AUDIT, LOG_OR_FAIL):
            _pg(r.definition.rstrip().rstrip(";") + ";")
        _my(f"create database {DB}", db=None)
        # the target's tables as a person writes them; its routines are
        # what the conversion makes
        _my("create table accounts (id int primary key, owner varchar(20)"
            " not null, balance decimal(10,2) not null default 0)",
            "create table ledger_log (id int auto_increment primary key,"
            " account_id int not null, note varchar(60) not null)",
            "insert into accounts values (1, 'ann', 100.00), (2, 'Bob', 0),"
            " (3, 'cy', 250.50)")
        yield
    finally:
        for n in (PG, MY, SB_PG, SB_MY):
            _sh("docker", "rm", "-f", "-v", n)


def _engine(tmp_path):
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="pgmy", engine="hetero",
              options={"source_engine": "postgres", "target_engine": "mysql",
                       "proof_sandbox": {
                           "source": {"host": "127.0.0.1",
                                      "port": SB_PG_PORT,
                                      "user": "postgres",
                                      "password": "test"},
                           "target": {"host": "127.0.0.1",
                                      "port": SB_MY_PORT, "user": "root",
                                      "password": "test"}},
                       "code_dir": str(tmp_path / "none")},
              source=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                              password="test"),
              databases=[DB])
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


@pytest.mark.docker
def test_the_rules_versions_are_proved_by_running_them(servers, tmp_path):
    got = {n: s for n, s, _ in _engine(tmp_path).converted_code(DB)}
    conv = json.loads((tmp_path / "code-conversion.json").read_text())
    # the named sandbox was used (else this says why not)
    assert conv["sandbox"] == "used", conv["sandbox"]
    record = {r["routine"]: r for r in conv["routines"]}
    for name in ("function ratio", "function owner_of",
                 "function strict_owner", "function sum_to",
                 "function safe_ratio", "function balance_of",
                 "trigger accounts_audit", "procedure deposit"):
        assert record[name]["rung"] == "rules", record[name]
        assert record[name]["class"] == "proved", record[name]
    # PostgreSQL rolls back what the protected block wrote before its
    # handler runs, MySQL keeps it: a person's to write
    kept = record["procedure log_or_fail"]
    assert kept["class"] == "not converted", kept
    assert any(x.startswith("migkit's rules: ") and "EXCEPTION handler" in x
               for x in kept["reasons"]), kept
    assert got["log_or_fail"].startswith(
        "-- procedure log_or_fail not converted"), got["log_or_fail"]
    assert "DIV" in got["ratio"] and "accounts_audit_update" in \
        got["accounts_audit"], got
    # the hop's own servers were only read
    assert _pg("select count(*) from ledger_log") == "0"
    assert _pg("select string_agg(balance::text, ',' order by id) from"
               " accounts") == "100.00,0.00,250.50"
    assert _my("select count(*) from ledger_log") == ((0,),)
    assert _my("select count(*) from information_schema.routines where"
               f" routine_schema = '{DB}'") == ((0,),)
