"""Two open converters, driven as programs of their own (backlog R11).

Each takes the source's own `CREATE` statement in a file and writes the
target's; nothing of either is loaded into migkit, and what they write is
never trusted: it is a candidate like any other, gated and proved in the
sandbox before it is chosen. Both are built by `migkit doctor --install`
(`tools.SOURCE_BUILDS`); a machine without them simply has two candidates
fewer.

* The PL/SQL converter (Perl) reads Oracle, MySQL and SQL Server code
  offline (`-i`), one kind at a time, and writes PostgreSQL's.
* The multi-dialect converter (C++) reads and writes many dialects,
  PostgreSQL and MySQL among them.

What they write around the statements - client settings, a psql
directive, comments, grants - is dropped; `CREATE OR REPLACE` becomes
`CREATE`, as every converted statement is (an object already on the
target is the operator's).
"""
import os
import re
import subprocess
import tempfile

from .plpgsql import Converted
from .translate import Refused

#: the source dialects each program reads, by migkit's name for them
READS = {"ora2pg": {"oracle": [], "mysql": ["-m"], "tsql": ["-M"]},
         "sqlines": {"oracle": "oracle", "mysql": "mysql", "tsql": "sql",
                     "postgres": "postgresql"}}
WRITES = {"ora2pg": {"postgres"},
          "sqlines": {"postgres": "postgresql", "mysql": "mysql",
                      "oracle": "oracle", "tsql": "sql"}}
#: the object type each program is asked for, per kind of routine
ORA2PG_TYPES = {"function": "FUNCTION", "procedure": "PROCEDURE",
                "trigger": "TRIGGER", "view": "VIEW", "event": "PROCEDURE"}
#: seconds a program may take over one routine
TIMEOUT = 120


def available(name):
    from .. import tools
    return tools.built_program(name) is not None


def statements(script):
    """The CREATE statements in a PostgreSQL script: split on semicolons
    outside quotes, dollar quotes and comments."""
    out, buf, i, n = [], [], 0, len(script)
    quote = None
    while i < n:
        ch = script[i]
        if quote:
            if script.startswith(quote, i):
                buf.append(quote)
                i += len(quote)
                quote = None
                continue
            buf.append(ch)
            i += 1
            continue
        if script.startswith("--", i):
            j = script.find("\n", i)
            i = n if j < 0 else j + 1
            buf.append("\n")
            continue
        if script.startswith("/*", i):
            j = script.find("*/", i)
            i = n if j < 0 else j + 2
            continue
        m = re.match(r"\$[A-Za-z_]*\$", script[i:])
        if m:
            quote = m.group(0)
            buf.append(quote)
            i += len(quote)
            continue
        if ch == "'":
            quote = "'"
            buf.append(ch)
            i += 1
            continue
        if ch == "\n" and script[i + 1:i + 2] == "\\":
            j = script.find("\n", i + 1)
            i = n if j < 0 else j
            continue
        if ch == ";":
            out.append("".join(buf))
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    out.append("".join(buf))
    made = []
    for s in (x.strip() for x in out):
        if re.match(r"(?is)^create\b", s):
            made.append(re.sub(r"(?is)^create\s+or\s+replace\b", "create",
                               s))
    return made


def _run(cmd, env, cwd):
    try:
        got = subprocess.run(cmd, env={**os.environ, **env}, cwd=cwd,
                             capture_output=True, text=True,
                             timeout=TIMEOUT)
    except subprocess.TimeoutExpired:
        raise Refused("the converter took longer than"
                      f" {TIMEOUT} seconds over it")
    except OSError as e:
        raise Refused(f"the converter could not be run ({type(e).__name__})")
    return got


def ora2pg(routine, src, dst, pg_version=None):
    """The PL/SQL converter's PostgreSQL for one routine, or Refused."""
    from .. import tools
    if dst not in WRITES["ora2pg"] or src not in READS["ora2pg"]:
        raise Refused(f"the PL/SQL converter does not convert {src} to {dst}")
    got = tools.built_program("ora2pg")
    if got is None:
        raise Refused("the PL/SQL converter is not installed here (migkit"
                      " doctor --install)")
    if not routine.definition:
        raise Refused("its definition could not be read")
    cmd, env = got
    with tempfile.TemporaryDirectory() as work:
        with open(os.path.join(work, "in.sql"), "w") as f:
            f.write(routine.definition.rstrip().rstrip(";") + ";\n")
        with open(os.path.join(work, "run.conf"), "w") as f:
            # no server is asked: the definition is converted from the file
            # NUMBER as numeric, never an integer type or a float the
            # converter would guess from the missing scale: the same values
            f.write("ORACLE_DSN\tdbi:Oracle:host=localhost;sid=none\n"
                    "PLSQL_PGSQL\t1\nEXPORT_SCHEMA\t0\n"
                    "PG_NUMERIC_TYPE\t0\nPG_INTEGER_TYPE\t0\n"
                    "DEFAULT_NUMERIC\tnumeric\n"
                    f"PG_VERSION\t{pg_version or 16}\n")
        run = _run(cmd + ["-c", "run.conf"] + READS["ora2pg"][src] + [
            "-i", "in.sql", "-t", ORA2PG_TYPES.get(routine.kind, "FUNCTION"),
            "-o", "out.sql", "-b", work], env, work)
        path = os.path.join(work, "out.sql")
        script = open(path).read() if os.path.exists(path) else ""
    made = statements(script)
    if run.returncode != 0 or not made:
        why = (run.stderr.strip().splitlines() or ["it wrote nothing"])[-1]
        raise Refused(f"the PL/SQL converter wrote nothing for it: "
                      f"{why[:100]}")
    return Converted(made)


def sqlines(routine, src, dst):
    """The multi-dialect converter's version of one routine, or
    Refused."""
    from .. import tools
    if src not in READS["sqlines"] or dst not in WRITES["sqlines"]:
        raise Refused(f"the multi-dialect converter does not convert {src}"
                      f" to {dst}")
    got = tools.built_program("sqlines")
    if got is None:
        raise Refused("the multi-dialect converter is not installed here"
                      " (migkit doctor --install)")
    if not routine.definition:
        raise Refused("its definition could not be read")
    cmd, env = got
    with tempfile.TemporaryDirectory() as work:
        with open(os.path.join(work, "in.sql"), "w") as f:
            f.write(routine.definition.rstrip().rstrip(";") + ";\n")
        _run(cmd + [f"-s={READS['sqlines'][src]}",
                     f"-t={WRITES['sqlines'][dst]}", "-in=in.sql",
                     "-out=out.sql", "-log=run.log"], env, work)
        path = os.path.join(work, "out.sql")
        script = open(path).read() if os.path.exists(path) else ""
    made = statements(script) if dst == "postgres" else [
        s for s in (x.strip() for x in script.split(";\n")) if s]
    # measured: it exits 146 having written its file whole; what it wrote
    # is the answer, and the proof holds it to the source like any other
    if not made:
        raise Refused("the multi-dialect converter wrote nothing for it")
    return Converted(made)
