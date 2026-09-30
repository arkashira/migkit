"""An independent second reading of the data, run out of process.

migkit's own checksum reads both sides one way. The second reader reads
them another way, through a different library and different SQL, and
reaches engines migkit does not read natively. It is used where a second
opinion is worth its cost - the planner decides that, not the operator -
and its findings come back in migkit's own verdicts.

It lives in a virtual environment of its own, because it pins an older
numeric stack than migkit runs on (measured on 8.9.3: numpy 1.26.4 against
migkit's 2.5, pandas 2.3.3 against 3.0, sqlglot 19.9 against 30.18 - one
environment cannot hold both), and is driven through
`runners/second_reader.py`: a job as JSON in, findings as JSON out.

What it is asked, and where (backlog 0f):
* column aggregates - count, sum, minimum and maximum of every column, and
  average, deviation and bitwise XOR where the column's type allows -
  where the pair is read a second time (`check_second`)
* the same aggregates per group of one column, only the failing groups
  back, to find *where* in a differing table the difference is; migkit's
  own row compare then walks those groups alone (`narrow`) - a rung of the
  verify passes across a pair (`leaves.PASSES`) and of `WALKS` where an
  engine walks its own rows, chosen per table where measured cheaper
* row by row, by key, only the failing rows back: the independent answer
  on a table migkit's own digest called different (the verify passes'
  `second` part), and on every table where the hop asks for two readings
* random rows by key, where the hop asks for them, as an early signal and
  never as a verdict: a sample that agrees has not read the table
* the views migkit carries, by the same query on both sides
All of one run go into one batch file in the reader's own format and are
executed from it; the file is kept beside the run's other evidence
(`BATCH`).
"""
import json
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

from .engines.base import Result

RUNNER = Path(__file__).parent / "runners" / "second_reader.py"

#: how the reader names each engine migkit hands it
READER_TYPES = {"postgres": "Postgres", "mysql": "MySQL", "mssql": "MSSQL"}

#: the check every verdict of the second reading carries
CHECK = "second reading"

#: the program the reader installs, whose first line names its Python
PROGRAM = "data-validation"

#: the last batch the reader ran, kept in the hop's report directory
BATCH = "data-second-reading.yaml"


def interpreter():
    """The reader's own Python, or None when it is not installed here.

    Named by `MIGKIT_SECOND_READER_PYTHON` - and then only that, so a test
    can say it is absent; otherwise the environment `doctor --install`
    builds, one beside a source checkout's own, or the reader's program
    wherever the path finds it, read for the Python its first line names.
    """
    named = os.environ.get("MIGKIT_SECOND_READER_PYTHON")
    if named:
        return named if Path(named).exists() else None
    from .util import _BASE, which
    for found in (Path.home() / ".migkit" / "second-reader" / "bin" / "python",
                  _BASE / ".venv-dvt" / "bin" / "python"):
        if found.exists():
            return str(found)
    program = which(PROGRAM)
    if program:
        try:
            first = Path(program).read_text(errors="replace").split("\n")[0]
        except OSError:
            return None
        python = first[2:].strip().split(" ")[0] if first.startswith("#!") \
            else ""
        if python and Path(python).exists():
            return python
    return None


def connection(engine, ep, db):
    """One side's connection, in the reader's terms."""
    if engine not in READER_TYPES:
        raise SystemExit(f"no second reading is set up for {engine} hops")
    return {"source_type": READER_TYPES[engine], "host": ep.host,
            "port": int(ep.port), "user": ep.user,
            "password": ep.password, "database": db}


def run(job, python=None, timeout=3600, keep=None):
    """Hand the reader one job; its answer as a dict, never an exception
    for a failure the reader itself reported.

    `keep` is the hop's report directory: the batch the reader ran is
    written there as `data-second-reading.yaml`. It names the connections
    only by `src` and `dst` and holds no password, but its predicates can
    hold the application's values (a group's), so it is one of the files
    the hop's `at_rest` recipients encrypt.
    """
    python = python or interpreter()
    if not python:
        return {"ok": False, "error": "the second reader is not installed"
                                      " on this machine: migkit doctor"
                                      " --install"}
    p = subprocess.run([python, str(RUNNER)], input=json.dumps(job),
                       capture_output=True, text=True, timeout=timeout)
    try:
        answer = json.loads(p.stdout)
    except ValueError:
        return {"ok": False, "error": "the second reader gave no answer"
                                      f" (exit {p.returncode})"}
    if keep is not None and answer.get("config"):
        keep.mkdir(parents=True, exist_ok=True)
        (keep / BATCH).write_text(answer["config"])
    return answer


def asked(hop):
    """What the hop asks of the second reading beyond what migkit does
    with it anyway (every table whose own digest differs is read twice
    where a second reading can be had): `two_readings: true` reads every
    table a second time, row by row where it has a key; a mapping says
    more - `rows: false` for the aggregates alone, `sample: <rows>` for
    the early signal, `case_insensitive: true` to read text without
    regard to case. Nothing is asked of a hop that says nothing."""
    got = (hop.options or {}).get("two_readings")
    if not got:
        return {}
    if not isinstance(got, dict):
        return {"rows": True}
    return {"rows": bool(got.get("rows", True)),
            "sample": int(got["sample"]) if got.get("sample") else None,
            "case_insensitive": bool(got.get("case_insensitive"))}


#: rows a row by row reading may pull. The reader holds every row's key
#: and hash of both sides in its own memory, and a differing table larger
#: than this - after narrowing to its groups - is said, not read: measured
#: on PostgreSQL 16, 200,000 rows by row hash took 3.3 s
ROWS_MOST = 1_000_000


# --- what to ask ------------------------------------------------------------

#: the aggregates beyond count, sum, minimum and maximum, and the neutral
#: classes (`canon`) of the columns each may be asked of. Not of floats:
#: an average or a deviation of doubles is not the same number summed in
#: another order, and the reader's own help says deviations differ between
#: engines. Of integers and exact decimals they held across engines:
#: measured MySQL 8 against PostgreSQL 16 on the same 300 rows (negative
#: bigints, `decimal(12,4)`), all nine read equal - the deviations differed
#: in their last two digits (12.39239364910513 against 12.392393649105134),
#: which the reader reads as equal
MORE = {"avg": ("integer", "decimal"), "std": ("integer", "decimal"),
        "bit_xor": ("integer",)}


def _names(cols, classes):
    return [n for n, c in cols if c in classes]


def aggregates(label, tables, columns=(), same_engine=True, tolerance=None,
               drop=(), grouped=None, where=(None, None),
               only_failures=False):
    """The column validations of one reading: count, sum, minimum and
    maximum of every column, then the typed ones.

    `columns` is [(name, neutral class)] of the columns compared, which
    picks the typed aggregates; `drop` the columns the hop's column rules
    leave out, which the reader is told to leave out of the first too.
    `tolerance` is the percentage the hop names for this table, and never
    anything else: a difference inside it is said, not passed over.
    """
    common = []
    if tolerance is not None:
        common += ["--threshold", str(float(tolerance))]
    if grouped:
        common += ["--grouped-columns", grouped]
    if where[0] or where[1]:
        common += ["--filters", filters(*where)]
    if only_failures:
        common += ["--filter-status", "fail"]
    first = ["--count", "*"]
    if drop:
        listed = ",".join(drop)
        first = ["--count", listed, "--sum", listed, "--min", listed,
                 "--max", listed, "--exclude-columns"]
    else:
        first += ["--sum", "*", "--min", "*", "--max", "*"]
    if grouped:
        # a text value changed to one of the same length moves no sum; its
        # length and a time's seconds are what a group's aggregates can
        # see. Not a length across engines: measured MySQL 8 against
        # PostgreSQL 16, a `char(5)` holding `c1` summed 84 against 150 in
        # every group of equal rows - one side counts the padding
        first += ["--wildcard-include-timestamp"]
        if same_engine:
            first.append("--wildcard-include-string-len")
    out = [{"label": label, "tables": list(tables), "args": first + common}]
    more = []
    for agg, classes in sorted(MORE.items()):
        names = [n for n in _names(columns, classes) if n not in drop]
        if names:
            more += [f"--{agg}", ",".join(names)]
    if more and not grouped:
        out.append({"label": f"{label}+", "tables": list(tables),
                    "args": more + common})
    return out


def padded(declared):
    """Whether a declared type is fixed-width text, which one engine hands
    back padded with blanks and another does not."""
    base = str(declared or "").strip().lower().split("(")[0].strip()
    return base in ("char", "character", "bpchar", "nchar",
                    "national character")


def rows(label, tables, key, columns=(), same_engine=True, where=(None, None),
         sample=None, case_insensitive=False, drop=(), fixed=()):
    """One row validation by key.

    Between two servers of one engine, by a hash of each row, computed in
    the database. Between two engines, by the value of each column that is
    neither a float nor fixed-width text (`fixed`), measured on MySQL 8
    against PostgreSQL 16 holding the same 301 rows: the row hash called
    the row holding the double `-1e-07` different, the two servers
    spelling it differently before hashing; and value by value, a
    `char(5)` holding `c1` read `c1` from MySQL and `c1   ` from
    PostgreSQL, a false difference on every row. Failures only, unless it
    is a sample, whose rows are few and are counted.
    """
    args = ["--primary-keys", ",".join(key)]
    if same_engine:
        args += ["--hash", "*"]
        if drop:
            args = ["--primary-keys", ",".join(key), "--hash",
                    ",".join(drop), "--exclude-columns"]
    else:
        fields = [n for n, c in columns
                  if c not in (None, "float") and n not in key
                  and n not in drop and n not in fixed]
        if not fields:
            return None
        args += ["--comparison-fields", ",".join(fields)]
    if where[0] or where[1]:
        args += ["--filters", filters(*where)]
    if case_insensitive:
        args.append("--case-insensitive-match")
    if sample:
        args += ["--use-random-row", "--random-row-batch-size",
                 str(int(sample))]
    else:
        args += ["--filter-status", "fail"]
    return {"label": label, "kind": "row", "tables": list(tables),
            "args": args}


def custom_query(label, src_sql, dst_sql):
    """The same aggregates over a query on each side - a view migkit
    carries, read through what it answers rather than what it is."""
    return {"label": label, "kind": "custom-query column",
            "args": ["--source-query", src_sql, "--target-query", dst_sql,
                     "--count", "*", "--sum", "*", "--min", "*", "--max",
                     "*"]}


def filters(src, dst):
    """The reader's `--filters`: one predicate for both sides, or the
    source's and the target's apart."""
    src, dst = src or "1=1", dst or "1=1"
    return src if src == dst else f"{src}:{dst}"


# --- what came back ---------------------------------------------------------

#: what the row hash does to every value before comparing, read in the
#: reader's source (8.9.3): cast to text, NULL replaced by a fixed word,
#: trailing blanks dropped. Said wherever it answers, because an equal
#: there is equal after that and not before
HASHED = ("after trailing blanks are dropped and NULL is read as a fixed"
          " word")

#: how rows are read across two engines, said wherever they are
ACROSS = "value by value, floats and fixed-width text left out"

KINDS = ("column", "row", "sample", "view", "group")


def _kind(label):
    base = str(label or "column").split(":")[0].rstrip("+")
    return base if base in KINDS else "column"


def _values(text):
    """A reader column holding JSON (`group_by_columns`, `primary_keys`)
    as a dict, or {} for anything else."""
    if isinstance(text, dict):
        return text
    try:
        got = json.loads(text) if text else {}
    except (TypeError, ValueError):
        return {}
    return got if isinstance(got, dict) else {}


def failing_keys(answer, label, table=None):
    """The keys of the rows one row validation found differing, each as
    the tuple of its key's values in text, in key order."""
    out = []
    for r in answer.get("results") or []:
        if r.get("migkit_label") != label:
            continue
        if table and r.get("source_table_name") not in (table, None):
            continue
        if r.get("validation_status") == "success":
            continue
        keys = _values(r.get("group_by_columns")) or _values(
            r.get("primary_keys"))
        out.append(tuple(str(v) for v in keys.values()))
    return sorted(set(out))


def _ran(answer, label, table):
    return any(d.get("label") == label
               and d.get("table") in (table, None, label)
               for d in answer.get("ran") or [])


def _broken(answer, label, table):
    for d in answer.get("broken") or []:
        if d.get("label") == label and d.get("table") in (table, None):
            return d.get("error") or "no reason given"
    return None


def _keys(keys, n=5):
    """Keys as an operator reads them: a composite key's parts joined."""
    return (", ".join("/".join(k) for k in keys[:n])
            + (" ..." if len(keys) > n else ""))


def _said(rows_, n=4):
    return "; ".join(
        f"{r.get('validation_name') or r.get('aggregation_type')}:"
        f" source {r.get('source_agg_value')}"
        f" target {r.get('target_agg_value')}" for r in rows_[:n])


def _drift(rows_):
    """Measures inside the hop's tolerance that still differ, in words."""
    near = [r for r in rows_ if r.get("validation_status") == "success"
            and r.get("pct_threshold") not in (None, 0, 0.0)
            and r.get("difference") not in (None, 0, 0.0)]
    if not near:
        return ""
    return (f"; {len(near)} of them differ within the hop's tolerance of"
            f" {near[0].get('pct_threshold')}%: {_said(near, 3)}")


def findings(answer, db, job=None, case_insensitive=False,
             same_engine=True):
    """The reader's rows as migkit verdicts, one per table and reading.

    A table is `ok` only when the reader says it read it and every row it
    returned passed. An answer that is not a success, or that returned
    nothing for a table it was asked about, is an error, never a pass: a
    reading that did not happen is not a reading that agreed. A sample is
    never `ok`: it found differing rows, or it read too little to say.
    """
    if not answer.get("ok"):
        return [Result(CHECK, db, "error",
                       f"{answer.get('error', 'no answer')} - nothing was"
                       " read the second way, which is not the same as"
                       " both readings agreeing")]
    wanted = []
    for v in (job or {}).get("validations") or []:
        for t in v.get("tables") or [v["label"].split(":", 1)[-1]]:
            wanted.append((v["label"], t.split("=", 1)[0]))
    by = {}
    for r in answer.get("results") or []:
        label = r.get("migkit_label") or "column"
        # a query has no table: its label names what it reads
        key = (label, r.get("source_table_name")
               or str(label).split(":", 1)[-1])
        by.setdefault(key, []).append(r)
    if not wanted:
        wanted = sorted(by)
    if not wanted:
        return [Result(CHECK, db, "error",
                       "the second reading returned no rows at all")]
    # a table's aggregates come in two validations; one verdict says both
    merged = {}
    for label, table in wanted:
        merged.setdefault((label.rstrip("+"), table), []).append(label)
    out = []
    ci = (" read without regard to case, as the hop asks," if
          case_insensitive else "")
    how = HASHED if same_engine else ACROSS
    for (label, table), labels in sorted(merged.items()):
        kind = _kind(label)
        got = [r for lab in labels for r in by.get((lab, table), [])]
        broke = next((b for b in (_broken(answer, lab, table)
                                  for lab in labels) if b), None)
        ran = all(_ran(answer, lab, table) or by.get((lab, table))
                  for lab in labels)
        if kind == "view":
            scope = f"{db} view {table}"
        elif kind in ("row", "sample"):
            scope = f"{db}.{table} {'rows' if kind == 'row' else 'sample'}"
        else:
            scope = f"{db}.{table}"
        if broke or not ran:
            why = broke or "the reader returned nothing for it"
            out.append(Result(CHECK, scope, "error",
                              f"could not be read the second way: {why} -"
                              " which is not the same as both readings"
                              " agreeing"))
            continue
        bad = [r for r in got if r.get("validation_status") != "success"]
        if kind == "sample":
            asked_ = len(got)
            if bad:
                keys = failing_keys(answer, labels[0], table)
                out.append(Result(CHECK, scope, "diff",
                                  f"{len(bad)} of {asked_} rows picked at"
                                  f" random differ{ci} (by key:"
                                  f" {_keys(keys)}) - an early signal; the"
                                  " whole table is compared by the checks"))
            else:
                out.append(Result(CHECK, scope, "skip",
                                  f"{asked_} rows picked at random read the"
                                  f" same{ci} {how} - a sample that"
                                  " agrees has not read the table, and is"
                                  " not a verdict on it"))
            continue
        if kind == "row":
            if bad:
                keys = failing_keys(answer, labels[0], table)
                out.append(Result(CHECK, scope, "diff",
                                  f"{len(keys)} rows differ when read a"
                                  f" second way{ci} (by key:"
                                  f" {_keys(keys)})"))
            else:
                out.append(Result(CHECK, scope, "ok",
                                  f"every row reads the same by key when"
                                  f" read a second way{ci}, {how}"))
            continue
        if not got:
            out.append(Result(CHECK, scope, "error",
                              "the second reading returned no measures for"
                              " it - which is not the same as both readings"
                              " agreeing"))
            continue
        if not bad:
            out.append(Result(CHECK, scope, "ok",
                              f"{len(got)} measures agree, read a second"
                              f" way{_drift(got)}"))
            continue
        out.append(Result(CHECK, scope, "diff",
                          f"{len(bad)} of {len(got)} measures differ when"
                          f" read a second way: {_said(bad)}"))
    return out


def compared(db, table, mine, theirs, reader_ran, same_engine=True,
             by_value=True):
    """Two readings of one differing table's rows, held to each other.

    `mine` is the set of keys migkit's own row compare named, `theirs` the
    reader's, both as tuples of text. Never `ok`: the table differs by
    migkit's own reading, and the second one is here to confirm it or to
    say which reading is wrong - not to pass it. `by_value` False where the
    two readings spell a key's values differently (a date, a decimal):
    then only how many rows each named is held against the other.
    """
    scope = f"{db}.{table} rows"
    if not reader_ran:
        return Result(CHECK, scope, "error",
                      "these rows could not be read the second way - which"
                      " is not the same as both readings agreeing")
    how = HASHED if same_engine else ACROSS
    if not by_value:
        n, m = len(set(mine)), len(set(theirs))
        if n == m:
            return Result(CHECK, scope, "diff",
                          f"read a second way, {how}: {n} rows differ, as"
                          " many as migkit's own reading named")
        if m < n:
            return Result(CHECK, scope, "diff",
                          f"of the {n} rows that differ, {n - m} read the"
                          f" same the second way, {how}: what differs in"
                          " them is what that reading cannot see")
        return Result(CHECK, scope, "diff",
                      f"read two ways, with two answers: {n} rows differ one"
                      f" way and {m} the other - one of the two readings is"
                      " wrong about this table, so the rows named here"
                      " cannot be relied on as complete", "",
                      "migkit check <hop> --drill --table <name> shows the"
                      " rows side by side; read them before repairing")
    mine, theirs = set(mine), set(theirs)
    only_mine, only_theirs = sorted(mine - theirs), sorted(theirs - mine)
    if not only_mine and not only_theirs:
        return Result(CHECK, scope, "diff",
                      f"read a second way, {how}: the same {len(mine)} rows"
                      " differ")
    if only_theirs:
        return Result(CHECK, scope, "diff",
                      f"read two ways, with two answers: {len(only_theirs)}"
                      f" rows differ only when read the second way"
                      f" ({_keys(only_theirs)}) - one of the two readings is"
                      " wrong about them, so the rows named here cannot be"
                      " relied on as complete", "",
                      "migkit check <hop> --drill --table <name> shows the"
                      " rows side by side; read them before repairing")
    return Result(CHECK, scope, "diff",
                  f"of the {len(mine)} rows that differ, {len(only_mine)}"
                  f" read the same the second way, {how}"
                  f" ({_keys(only_mine)}): what differs in them is what that"
                  " reading cannot see")


# --- narrowing a differing table to the groups that differ ------------------

#: a column splits a differing table into groups; fewer than this rules too
#: little out to be worth a reading
GROUPS_FEWEST = 4
#: and more than this makes the per-group answer as large as the rows
GROUPS_MOST = 10_000
#: rows a group holds, on average, at the fewest
GROUP_ROWS = 10


def group_column(candidates, key=(), same_engine=True, rows=None):
    """(column, class, groups) to narrow a differing table by, or None.

    `candidates` is [(column, class, estimated distinct values)] from the
    source's statistics. The most groups within the bounds wins: the more
    groups, the fewer rows each differing one leaves to walk. An integer
    or a text column, and only an integer across engines - two engines'
    collations group text differently. A single-column key is no group:
    each of its groups is one row - and neither is a column with fewer
    than `GROUP_ROWS` rows a group over the table's `rows`, whose groups
    are nearly the rows themselves.
    """
    best = None
    classes = ("integer", "text") if same_engine else ("integer",)
    most = GROUPS_MOST if not rows else min(GROUPS_MOST,
                                            int(rows) // GROUP_ROWS)
    for col, cls, n in candidates:
        if cls not in classes or n is None or list(key) == [col]:
            continue
        if not GROUPS_FEWEST <= n <= most:
            continue
        if best is None or n > best[2]:
            best = (col, cls, n)
    return best


def _walks():
    from .decide import Need, Rung
    return (
        Rung("groups", "only the groups whose aggregates differ when read a"
                       " second way, the rest proved equal by migkit's own"
                       " digest",
             needs=(Need("the second reader installed for both engines",
                         lambda f: bool(f.get("reader")),
                         "{rung} needs the second reader, and it is not"
                         " installed or does not read both engines"),
                    Need("a column the source's statistics say splits it",
                         lambda f: f.get("group column") is not None,
                         "no column of it is known from the source's"
                         f" statistics to split it into {GROUPS_FEWEST} to"
                         f" {GROUPS_MOST:,} groups")),
             prove=lambda f: f.get("groups failed"),
             because="the second reader's aggregates per group rule out the"
                     " groups that agree, and only the others are walked"),
        Rung("walk", "every row, by the table's own walk",
             because="the table's own walk, which reads every row"),
    )


#: how the rows of a differing table are found where the engine walks
#: them itself (PostgreSQL and MySQL, within one engine; a pair of engines
#: localizes through `leaves.PASSES`, where the same groups are a rung):
#: chosen per table by what each was measured to cost on this hop
WALKS = _walks()


@dataclass
class Narrowing:
    column: str
    cls: str
    #: the group values whose aggregates differ, as text
    values: list
    #: whether the group of NULLs differs
    nulls: bool
    #: how many groups the column was estimated to have
    groups: int

    def _in(self, engine):
        c = engine._quote_ident(self.column)
        parts = []
        if self.values:
            lits = (self.values if self.cls == "integer"
                    else [engine._quote_literal(v) for v in self.values])
            parts.append(f"coalesce({c} in ({', '.join(lits)}), false)")
        if self.nulls:
            parts.append(f"{c} is null")
        return " or ".join(parts) or "false"

    def inside(self, engine, where=None):
        """The rows of the groups that differ, in one side's SQL: `engine`
        is the side's, whose quoting the values are written in."""
        got = f"({self._in(engine)})"
        return f"({where}) and {got}" if where else got

    def outside(self, engine, where=None):
        """Every other row, in one side's SQL."""
        got = f"not ({self._in(engine)})"
        return f"({where}) and {got}" if where else got

    def said(self):
        n = len(self.values) + int(self.nulls)
        return (f"narrowed by {self.column} to {n} of about"
                f" {self.groups:,} groups read a second way")


def _group_values(answer, label, column, cls):
    """([values], nulls) of the failing groups, or None when an integer
    group comes back as something that is not one."""
    values, nulls = set(), False
    for r in answer.get("results") or []:
        if (r.get("migkit_label") != label
                or r.get("validation_status") == "success"):
            continue
        got = _values(r.get("group_by_columns"))
        v = next((x for k, x in got.items()
                  if str(k).lower() == column.lower()), None)
        if v is None:
            nulls = True
            continue
        text = str(v)
        if cls == "integer":
            if not re.fullmatch(r"-?\d+", text):
                # a float-typed group value: the integer it holds
                if not re.fullmatch(r"-?\d+\.0+", text):
                    return None
                text = text.split(".")[0]
        values.add(text)
    return sorted(values, key=lambda x: (len(x), x)), nulls


def narrow(hop, db, names, tables, group, columns=(), where=(None, None),
           python=None, keep=None):
    """The groups of one differing table whose aggregates differ, read the
    second way: a `Narrowing`, or the reason there is none, in words.

    `names` is (source engine, target engine), `tables` the reader's
    `schema.table` on each side. The aggregates see a count, a number's
    sum, a text's length and a time's seconds per group - not a text
    changed to another of the same length - so what they rule out is only
    a place to start: `walk_narrowed` proves the rest equal by migkit's own
    digest before it trusts them.
    """
    col, cls, n = group
    label = f"group:{tables[0]}"
    job = {"source": connection(names[0], hop.source, db),
           "target": connection(names[1], hop.target, hop.target_db(db)),
           "validations": aggregates(
               label, [f"{tables[0]}={tables[1]}"], columns,
               same_engine=names[0] == names[1], grouped=col, where=where,
               only_failures=True)}
    answer = run(job, python, keep=keep)
    if not answer.get("ok") or not _ran(answer, label, tables[0]):
        return ("its groups could not be read the second way ("
                + (_broken(answer, label, tables[0]) or answer.get("error")
                   or "no answer")[:120] + ")")
    got = _group_values(answer, label, col, cls)
    if got is None:
        return f"a group value of {col} did not come back as an integer"
    values, nulls = got
    if not values and not nulls:
        return (f"no group of {col} differs by count, sum, length or"
                " time - what differs is inside values the groups' measures"
                " cannot see")
    return Narrowing(col, cls, values, nulls, int(n))


def walk_narrowed(narrowing, walk, same, engines, where=(None, None)):
    """The rows that differ, walking only the groups that differ - and the
    rest too, unless migkit's own digest proves the rest equal.

    `walk(src_where, dst_where)` is the engine's own row compare over a
    predicate, answering (missing, extra, changed); `same(src_where,
    dst_where)` its own digest of both sides over one, answering whether
    they are equal. The groups' measures are a guess at where to look; the
    digest reads every column of every row outside them, so a difference
    they could not see is walked rather than lost.

    Returns (missing, extra, changed, said, only_inside) - the last True
    where every differing row is inside the groups, proved.
    """
    inside = tuple(narrowing.inside(e, w) for e, w in zip(engines, where))
    outside = tuple(narrowing.outside(e, w) for e, w in zip(engines, where))
    missing, extra, changed = walk(*inside)
    if (missing or extra or changed) and same(*outside):
        return (missing, extra, changed,
                f"{narrowing.said()}, the rest proved equal by migkit's own"
                " digest", True)
    more = walk(*outside)
    # in the order found: a key with NULL in it does not sort
    return (list(dict.fromkeys(list(missing) + list(more[0]))),
            list(dict.fromkeys(list(extra) + list(more[1]))),
            list(dict.fromkeys(list(changed) + list(more[2]))),
            f"{narrowing.said()}; the rest differed too by migkit's own"
            " digest and was walked as well", False)


# --- the row-level explanation of a small differing set ---------------------

#: rows a differing set may hold and still be explained column by column
EXPLAIN_MOST = 50


def explain(src_df, dst_df, keys, hop=None, table=None, limit=EXPLAIN_MOST,
            per_column=3):
    """What differs between two frames of the same rows, column by column,
    in migkit's words: the rows only one side has, how many rows on both
    sides hold a value that differs, and for each column how many and a
    few of them (`per_column`). Values are shown as each side holds them
    (masked where the hop masks the column, keys where it masks any), not
    as the comparison held them - joining the two sides makes a column with a missing row a
    float, and `2` would print as `2.0`. `limit` bounds the lines."""
    import datacompy

    from . import masking
    # compared as held and masked as shown: a masked value is a token of
    # the value, so masking first would compare the same thing and then
    # mask the token a second time on the way out
    cmp = datacompy.PandasCompare(
        src_df, dst_df, join_columns=[k.lower() for k in keys],
        df1_name="source", df2_name="target")
    on = [k.lower() for k in keys]
    hide = bool(hop) and masking.keys(hop, table)

    def key_text(row):
        return " ".join(
            f"{k}={masking.token(hop, row[k]) if hide else row[k]}"
            for k in on)

    def shown(name, v):
        # a frame's number is numpy's, whose repr names the type
        v = v.item() if hasattr(v, "item") else v
        return masking.shown(hop, table, name, v) if hop else v

    out = [f"rows read: source {len(cmp.df1):,}, target {len(cmp.df2):,},"
           f" on both sides {len(cmp.intersect_rows):,}"]
    for side, frame in (("only on the source", cmp.df1_unq_rows),
                        ("only on the target", cmp.df2_unq_rows)):
        if len(frame):
            recs = frame.to_dict(orient="records")
            out.append(f"{len(recs):,} rows {side}: "
                       + ", ".join(key_text(r) for r in recs[:5])
                       + (" ..." if len(recs) > 5 else ""))
    mism = cmp.all_mismatch()
    out.append(f"rows on both sides with a value that differs: {len(mism):,}")
    if not len(mism):
        return out
    src = cmp.df1.set_index(on)
    dst = cmp.df2.set_index(on)
    both = cmp.intersect_rows
    for stat in cmp.column_stats:
        name, n = stat["column"], int(stat["unequal_cnt"])
        if not n or name in on or stat.get("match_column") not in both:
            continue
        rows_ = both[~both[stat["match_column"]]].to_dict(orient="records")
        said = []
        for rec in rows_[:per_column]:
            k = tuple(rec[c] for c in on)
            try:
                a = src.loc[k[0] if len(k) == 1 else k, name]
                b = dst.loc[k[0] if len(k) == 1 else k, name]
            except KeyError:
                continue
            said.append(f"{key_text(rec)}: source {shown(name, a)!r}"
                        f" target {shown(name, b)!r}")
        out.append(f"{name}: {n:,} rows differ"
                   + (" - " + "; ".join(said) if said else "")
                   + (" ..." if n > len(said) and said else ""))
        if len(out) > limit:
            out.append("... and more columns")
            break
    return out
