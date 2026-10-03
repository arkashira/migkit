"""A source table's partitioning carried to a PostgreSQL target as
advice, and the routing of its rows proved on the sandbox (leapfrog
research P15, backlog CV.P15: partitions).

A MySQL or Oracle table partitioned by range or list whose target is not
partitioned loses what the partitioning gave the application: a read of
one month touching one partition, a month dropped whole. With the hop
option `index_advice`, each such table's scheme is written in
PostgreSQL's declarative partitioning - the key's expression through the
translator, each bound as the source keeps it (MySQL's `VALUES LESS
THAN`, its `VALUES IN`, Oracle's `HIGH_VALUE`), a range's first partition
from MINVALUE - and proved before it is advised: on the sandbox target,
inside a transaction rolled back after it (PostgreSQL's DDL is
transactional, so nothing stays), a copy partitioned so is made, the
fixture's rows put in it, and every row's partition held to the one the
source's sandbox keeps it in (`PARTITION (p)`). A row that lands
elsewhere is named by its key. A hash partitioning is advised with the
same count of partitions and said to route by the target's own hash: no
row is held to the source's partition, since the two hashes differ by
design. What is advised is written to `partitions-for-the-target.sql`
under a name of its own (`<table>_partitioned`), for a person to move
the rows into; migkit changes no table of the target.
"""
import re

from . import harness
from .translate import Refused, translate

#: the file the advice is written to, beside the reports
FILE = "partitions-for-the-target.sql"


def turned_on(hop):
    from .indexadvice import turned_on as on
    return on(hop)


def schemes(eng, side, db):
    """{table: {"m": method, "e": key expression, "p": {partition:
    bound}}} of the side's partitioned tables: MySQL's and Oracle's, {}
    for an engine whose partitions are not read here."""
    engine = getattr(eng, "CANON_ENGINE", "")
    if engine == "mysql":
        return eng.partition_scheme(side, db, eng._d(side, db))
    if engine == "oracle":
        return _oracle(eng, side, db)
    return {}


def _oracle(eng, side, db):
    from ..engines.dbapi import MARK
    owner = eng._owner(side, db)
    out = {}
    kinds = dict(eng._rows(side, db, "select table_name, partitioning_type"
                                     " from all_part_tables where owner ="
                                     f" {MARK}", (owner,)))
    keys = {}
    for t, col, _ in eng._rows(side, db, "select name, column_name,"
                                         " column_position from"
                                         " all_part_key_columns where owner"
                                         f" = {MARK} and object_type ="
                                         " 'TABLE' order by name,"
                                         " column_position", (owner,)):
        keys.setdefault(t, []).append(eng._said(col))
    for t, name, high, _ in eng._rows(side, db, "select table_name,"
                                                " partition_name, high_value,"
                                                " partition_position from"
                                                " all_tab_partitions where"
                                                f" table_owner = {MARK} order"
                                                " by table_name,"
                                                " partition_position",
                                      (owner,)):
        if t not in kinds:
            continue
        said = eng._said(t)
        if eng.hop.excluded(db, said):
            continue
        e = out.setdefault(said, {"m": kinds[t], "e": ", ".join(
            keys.get(t, [])), "p": {}})
        e["p"][eng._said(name)] = str(high.read() if hasattr(high, "read")
                                      else high or "")
    return out


def _q(name):
    return '"' + str(name).replace('"', '""') + '"'


#: Oracle's high value of a date or a timestamp, as its catalogue writes it
_ORACLE_MOMENT = re.compile(r"(?is)^\s*(?:to_date\(\s*'\s*([^']*)'\s*,\s*"
                            r"'S?YYYY-MM-DD HH24:MI:SS'[^)]*\)|timestamp\s*'"
                            r"\s*([^']*)')\s*$")


def _value(text, dialect):
    """A bound as PostgreSQL writes it, or Refused."""
    t = str(text).strip()
    if t.upper() == "MAXVALUE":
        return "MAXVALUE"
    m = _ORACLE_MOMENT.match(t) if dialect == "oracle" else None
    if m:
        return "'" + (m.group(1) or m.group(2)).strip() + "'"
    got = translate(f"SELECT {t}", dialect, "postgres")
    return got[len("SELECT "):]


def _key(parts):
    """A partition key as PostgreSQL takes it: a column as it is, an
    expression in parentheses of its own."""
    return ", ".join(p if re.fullmatch(r'"[^"]+"|[A-Za-z_][\w$]*', p)
                     else f"({p})" for p in parts)


def _values(text, dialect):
    parts, depth, buf, quote = [], 0, [], False
    for ch in str(text):
        if ch == "'":
            quote = not quote
        if not quote:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                parts.append("".join(buf).strip())
                buf = []
                continue
        buf.append(ch)
    if "".join(buf).strip():
        parts.append("".join(buf).strip())
    return [_value(p, dialect) for p in parts]


def advised(table, scheme, target, dialect, name=None):
    """([statements] partitioning a copy of the target's `target` table as
    the source partitions `table`, whether rows are routed as the
    source's are - not by a hash) or Refused naming what is not carried.
    `name` is the copy's (`<table>_partitioned` where none is given)."""
    method = " ".join(str(scheme["m"]).upper().split())
    leaf = str(target).split(".")[-1]
    name = name or f"{leaf}_partitioned"
    if not scheme.get("e"):
        raise Refused(f"{table}'s partition key could not be read")
    try:
        parts_of_key = _values(scheme["e"], dialect)
    except Refused as e:
        raise Refused(f"{table}'s partition key {scheme['e']}: {e}")
    key = _key(parts_of_key)
    columns = len(parts_of_key)
    kind = method.split()[0]
    if method in ("LINEAR HASH", "LINEAR KEY") or kind not in (
            "RANGE", "LIST", "HASH", "KEY"):
        raise Refused(f"{table} is partitioned by {method}, which"
                      " PostgreSQL has no counterpart for")
    # the columns and their defaults: a counter or a generated column is a
    # plain one in the copy, which takes the rows as the table holds them
    head = (f"create table {_q(name)} (like {_q(leaf)} including defaults)"
            f" partition by {'hash' if kind in ('HASH', 'KEY') else kind.lower()}"
            f" ({key})")
    out = [head]
    parts = list(scheme["p"].items())
    if kind in ("HASH", "KEY"):
        for i, (part, _) in enumerate(parts):
            out.append(f"create table {_q(f'{name}_{part}'.lower())}"
                       f" partition of {_q(name)} for values with (modulus"
                       f" {len(parts)}, remainder {i})")
        return out, False
    lower = ", ".join(["MINVALUE"] * columns)
    for part, bound in parts:
        made = _q(f"{name}_{part}".lower())
        if kind == "LIST":
            if str(bound).strip().upper() == "DEFAULT":
                out.append(f"create table {made} partition of {_q(name)}"
                           " default")
            else:
                out.append(f"create table {made} partition of {_q(name)}"
                           f" for values in ({', '.join(_values(bound, dialect))})")
            continue
        upper = ", ".join(_values(bound, dialect))
        out.append(f"create table {made} partition of {_q(name)} for values"
                   f" from ({lower}) to ({upper})")
        lower = upper
    return out, True


def routes(proof, table, statements, name):
    """[(key, the source's partition, the advised copy's)] of the fixture
    rows the advised partitioning puts elsewhere than the source's - [] where
    every row lands as on the source - made and rolled back on the
    sandbox target. Raises where it cannot be asked."""
    a, b = proof.a, proof.b
    w = a.watched.get(table)
    if w is None:
        w = next((x for t, x in a.watched.items()
                  if t.split(".")[-1].lower() == table.lower()), None)
    if w is None or not w.key:
        raise harness.HarnessError(f"{table} is not in the fixture with a"
                                   " key")
    key_at = [[c for c, _ in w.columns].index(k) for k in w.key]
    classes = [w.columns[i][1] for i in key_at]
    tw = b.watched[next(t for t, x in a.watched.items() if x is w)]
    target_key = [tw.columns[i][0] for i in key_at]

    def keyed(row):
        return tuple(harness.render(c, v) for c, v in zip(classes, row))
    scheme = schemes(proof.pair.src_engine, "src", proof.db).get(table) or {}
    source = {}
    for part in scheme.get("p", {}):
        sql = (f"select {', '.join(a.q(k) for k in w.key)} from"
               f" {a.table(w.name)} partition ({a.q(part)})")
        if a.engine == "oracle":
            cur = a.conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
        else:
            with a.conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        for row in rows:
            source[keyed(row)] = f"{name}_{part}".lower()
    a.conn.rollback()
    landed = {}
    with b.conn.cursor() as cur:
        try:
            for s in statements:
                cur.execute(s)
            cur.execute(f"insert into {_q(name)} select * from"
                        f" {b.table(tw.name)}")
            cur.execute(f"select {', '.join(b.q(k) for k in target_key)},"
                        f" tableoid::regclass::text from {_q(name)}")
            for row in cur.fetchall():
                landed[keyed(row[:-1])] = str(row[-1]).strip('"')
        finally:
            b.conn.rollback()
    return apart(source, landed)


def apart(source, landed):
    """[(key, the source's partition, the copy's)] of the rows the copy
    keeps elsewhere than the source - a row it lacks as None - in the
    keys' order."""
    return [(k, source[k], landed.get(k)) for k in sorted(source, key=repr)
            if landed.get(k) != source[k]]


def advise(proof, schema):
    """The `partitions advised` line: each source table partitioned where
    its target is not, its partitioning written for the target and its
    routing proved on the sandbox."""
    from ..engines.base import Result
    pair, db = proof.pair, proof.db
    if pair.dst_engine.CANON_ENGINE != "postgres" or \
            pair.src_engine.SQL_DIALECT not in ("mysql", "oracle"):
        return
    try:
        got = schemes(pair.src_engine, "src", db)
    except Exception as e:  # noqa: BLE001 - said, the proof goes on
        proof.notes.append("the source's partitions could not be read: "
                           + (str(e).strip().splitlines() or [""])[0][:100])
        return
    if not got:
        return
    try:
        held = set(pair.dst_engine._psql(
            "dst", db, "select c.relname from pg_partitioned_table p join"
                       " pg_class c on c.oid = p.partrelid").split())
    except Exception:  # noqa: BLE001 - asked of every table below
        held = set()
    made, said, worst = [], [], "ok"
    for table, scheme in sorted(got.items()):
        target = pair.hop.target_table_in(db, table)
        if target.split(".")[-1] in held:
            continue
        try:
            statements, alike = advised(table, scheme, target,
                                        pair.src_engine.SQL_DIALECT)
        except Refused as e:
            said.append(f"{table}: not advised - {e}")
            worst = "warn"
            continue
        made += statements
        worst = "warn"
        if not alike:
            said.append(f"{table}: {len(scheme['p'])} partitions by the"
                        " target's own hash, which routes rows otherwise"
                        " than the source's")
            continue
        scratch = f"migkit_part_{target.split('.')[-1]}"[:50]
        trial, _ = advised(table, scheme, target,
                           pair.src_engine.SQL_DIALECT, name=scratch)
        try:
            apart = routes(proof, table, trial, scratch)
        except Exception as e:  # noqa: BLE001 - said, not proved
            said.append(f"{table}: its routing could not be proved: "
                        + (str(e).strip().splitlines() or [""])[0][:100])
            continue
        if apart:
            worst = "diff"
            k, there, here = apart[0]
            said.append(f"{table}: the advised partitioning puts the row of"
                        f" key {k!r} in {here} where the source keeps it in"
                        f" {there.rsplit('_', 1)[-1]}"
                        f" ({len(apart)} rows elsewhere)")
        else:
            said.append(f"{table}: partitioned on the source by"
                        f" {scheme['m']} ({scheme['e']}), {len(scheme['p'])}"
                        " partitions; the advised partitioning puts every"
                        " fixture row where the source keeps it")
    if not said:
        return
    path = pair.hop.report_dir(db) / FILE
    if made:
        try:
            path.write_text("-- the source's partitioning as the target"
                            " writes it, proved on the sandbox; written by"
                            " migkit's check, which runs none of it\n"
                            + ";\n".join(made) + ";\n")
        except OSError:
            path = None
    schema.append(Result(
        "schema", f"{db} partitions advised", worst, "; ".join(said[:6]),
        str(path) if made and path else "",
        "make the partitioned tables on the target and move the rows into"
        " them (partitions-for-the-target.sql beside the reports)" if made
        else ""))
