"""An independent second reading of the data, run out of process.

migkit's own checksum reads both sides one way. The second reader reads
them another way, through a different library and different SQL, and
reaches engines migkit does not read natively. It is used where a second
opinion is worth its cost - the planner decides that, not the operator -
and its findings come back in migkit's own verdicts.

It lives in a virtual environment of its own, because it pins an older
numeric stack than migkit runs on, and is driven through
`runners/second_reader.py`: a job as JSON in, findings as JSON out.
"""
import json
import os
import subprocess
from pathlib import Path

from .engines.base import Result

RUNNER = Path(__file__).parent / "runners" / "second_reader.py"

#: how the reader names each engine migkit hands it
READER_TYPES = {"postgres": "Postgres", "mysql": "MySQL", "mssql": "MSSQL"}


def interpreter():
    """The reader's own Python, or None when it is not installed here."""
    found = os.environ.get("MIGKIT_SECOND_READER_PYTHON") or str(
        Path.home() / ".migkit" / "second-reader" / "bin" / "python")
    return found if Path(found).exists() else None


def connection(engine, ep, db):
    """One side's connection, in the reader's terms."""
    if engine not in READER_TYPES:
        raise SystemExit(f"no second reading is set up for {engine} hops")
    return {"source_type": READER_TYPES[engine], "host": ep.host,
            "port": int(ep.port), "user": ep.user,
            "password": ep.password, "database": db}


def run(job, python=None, timeout=3600):
    """Hand the reader one job; its answer as a dict, never an exception
    for a failure the reader itself reported."""
    python = python or interpreter()
    if not python:
        return {"ok": False, "error": "the second reader is not installed"
                                      " on this machine: migkit doctor"
                                      " --install"}
    p = subprocess.run([python, str(RUNNER)], input=json.dumps(job),
                       capture_output=True, text=True, timeout=timeout)
    try:
        return json.loads(p.stdout)
    except ValueError:
        return {"ok": False, "error": "the second reader gave no answer"
                                      f" (exit {p.returncode})"}


def findings(answer, db):
    """The reader's rows as migkit verdicts, one per table.

    A table is `ok` only when every row the reader returned for it passed.
    An answer that is not a success, or that returned nothing for a table
    it was asked about, is an error, never a pass: a reading that did not
    happen is not a reading that agreed.
    """
    if not answer.get("ok"):
        return [Result("second reading", db, "error",
                       f"{answer.get('error', 'no answer')} - nothing was"
                       " read the second way, which is not the same as"
                       " both readings agreeing")]
    by_table = {}
    for row in answer.get("results") or []:
        table = row.get("source_table_name") or "?"
        by_table.setdefault(table, []).append(row)
    if not by_table:
        return [Result("second reading", db, "error",
                       "the second reading returned no rows at all")]
    out = []
    for table, rows in sorted(by_table.items()):
        bad = [r for r in rows if r.get("validation_status") != "success"]
        if not bad:
            out.append(Result("second reading", f"{db}.{table}", "ok",
                              f"{len(rows)} measures agree, read a second"
                              " way"))
            continue
        said = "; ".join(
            f"{r.get('validation_name') or r.get('aggregation_type')}:"
            f" source {r.get('source_agg_value')}"
            f" target {r.get('target_agg_value')}" for r in bad[:4])
        out.append(Result("second reading", f"{db}.{table}", "diff",
                          f"{len(bad)} of {len(rows)} measures differ when"
                          f" read a second way: {said}"))
    return out
