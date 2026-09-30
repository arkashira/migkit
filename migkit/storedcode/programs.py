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
READS = {"ora2pg": ("oracle", "mysql", "tsql"),
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


def _converter_args(src, pg_version):
    """(the PL/SQL converter's configuration for one routine, the flags
    that say which dialect it reads). No server is asked: the definition
    is converted from its file; the bodies are converted, the target's
    own names kept, and a NUMBER is numeric - never an integer type or a
    float the converter would guess from a missing scale: the same
    values."""
    conf = (("ORACLE_DSN", "dbi:Oracle:host=localhost;sid=none"),
            ("PLSQL_PGSQL", "1"), ("EXPORT_SCHEMA", "0"),
            ("PG_NUMERIC_TYPE", "0"), ("PG_INTEGER_TYPE", "0"),
            ("DEFAULT_NUMERIC", "numeric"),
            ("PG_VERSION", str(pg_version or 16)))
    return conf, {"oracle": [], "mysql": ["-m"], "tsql": ["-M"]}[src]


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
        conf, flags = _converter_args(src, pg_version)
        with open(os.path.join(work, "run.conf"), "w") as f:
            f.write("".join(f"{k}\t{v}\n" for k, v in conf))
        base = cmd + ["-c", "run.conf"] + flags + [
            "-i", "in.sql", "-t", ORA2PG_TYPES.get(routine.kind, "FUNCTION"),
            "-o", "out.sql", "-b", work]
        path = os.path.join(work, "out.sql")
        # with its cost estimate on, which it writes beside the version
        # as comments the statements are read without (`effort`); a
        # build that refuses the estimate for a kind is run without it
        cost = None
        for extra in (["--estimate_cost"], []):
            if os.path.exists(path):
                os.remove(path)
            run = _run(base + extra, env, work)
            script = open(path).read() if os.path.exists(path) else ""
            made = statements(script)
            if run.returncode == 0 and made:
                if extra:
                    from .effort import units
                    cost = units(script + "\n" + (run.stdout or ""))
                break
    if run.returncode != 0 or not made:
        why = (run.stderr.strip().splitlines() or ["it wrote nothing"])[-1]
        raise Refused(f"the PL/SQL converter wrote nothing for it: "
                      f"{why[:100]}")
    return Converted(made, cost=cost)


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


#: the PL/SQL converter's options migkit's code passes, by the token that
#: turns each on there (`tooluse`: used is read off this file)
_CONVERTER_TOKENS = {o: o for o in (
    "-c", "-i", "-t", "-o", "-b", "-m", "-M", "--estimate_cost",
    "--cost_unit_value", "--human_days_limit", "SHOW_REPORT", "TEST",
    "PLSQL_PGSQL", "PG_NUMERIC_TYPE", "PG_INTEGER_TYPE", "DEFAULT_NUMERIC",
    "PG_VERSION", "EXPORT_SCHEMA", "AUTONOMOUS_TRANSACTION")}


def _register():
    """The PL/SQL converter's account: what migkit asks it for is read off
    this file; what it is not asked for says why - one routine's own
    statement from a file, converted and scored, never a connection to a
    server (migkit reads the catalogue itself)."""
    from .. import tooluse
    if "ora2pg" in tooluse.REGISTRY:
        return
    tooluse.register(
        "ora2pg", licence="GPL-3.0-or-later", release="v25.0",
        declared={"": frozenset(_CONVERTER_TOKENS)},
        reads=(("storedcode/programs.py::ora2pg",
                "storedcode/programs.py::_converter_args"),
               {("", o): t for o, t in _CONVERTER_TOKENS.items()}),
        options={
            ("", "--cost_unit_value"): (
                tooluse.UNUSED, "its own five minutes a unit is kept and"
                                " said with the figure, never changed"),
            ("", "--human_days_limit"): (
                tooluse.UNUSED, "the migration level letter it grades by is"
                                " not shown; migkit's count is per routine"),
            ("", "SHOW_REPORT"): (
                tooluse.UNUSED, "its inventory needs a connection to the"
                                " source; migkit reads the catalogue itself"
                                " and has each routine scored from its"
                                " statement"),
            ("", "TEST"): (
                tooluse.UNUSED, "it counts objects on both sides; migkit"
                                " runs every routine on both and compares"
                                " what each call did"),
            ("", "AUTONOMOUS_TRANSACTION"): (
                tooluse.UNUSED, "its wrapper opens a new connection with a"
                                " password in the body; such a routine is"
                                " left to the other candidates and to the"
                                " proof")})


_register()
