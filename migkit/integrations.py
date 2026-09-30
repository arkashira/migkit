"""migkit inside the team's tools: a task for Airflow and Dagster, a step
for GitHub Actions and Argo Workflows, each a thin wrapper over migkit's
own command line (backlog R23.8).

One way to run migkit from another tool (`run`): the command a person
would type, in a process of its own, and what it answered read back from
the files it writes - the verdict (`verdict.json`) and the run's evidence
(`evidence/<run>/evidence.json`, `export`) - so a pipeline gates on the
status and keeps the evidence as its artefact. There is no second logic
here: every decision is migkit's, taken where it always is, and a task
fails exactly where the command would have exited with an error.

* Airflow: `MigkitOperator`, imported from here where Airflow is
  installed (2.x or 3.x) - its return value, `run`'s, goes to XCom
* Dagster: `migkit_op(name, command, hop, ...)`, an op returning `run`'s
* GitHub Actions: `action.yml` at the repository's root
* Argo Workflows: `deploy/argo/migkit-workflowtemplate.yaml`

Neither Airflow nor Dagster is a dependency of migkit: each is the
scheduler the team already runs, and imports migkit, not the other way.
"""
import os
import subprocess
import sys
import time
from pathlib import Path

#: lines of the command's output kept in the result
TAIL = 40


def _reports(reports):
    if reports:
        return Path(reports).expanduser()
    from . import config
    return config.REPORTS


def run(command, hop, args=(), conf=None, reports=None, env=None,
        timeout=None):
    """`migkit <command> <hop> <args...>` run to its end, as a person
    would run it: {"command", "hop", "exit", "status" (the verdict this
    run wrote, or None), "verdict" and "evidence" (paths, or None),
    "output" (its last lines)}. `conf` and `reports` are the hops file and
    the reports directory where they are not migkit's defaults."""
    import json
    here = str(Path(__file__).resolve().parents[1])
    child = {**os.environ, **(env or {})}
    # the child imports the migkit this is, not whichever is first found
    child["PYTHONPATH"] = os.pathsep.join(
        p for p in (here, child.get("PYTHONPATH", "")) if p)
    if conf:
        child["MIGKIT_CONF"] = str(Path(conf).expanduser())
    if reports:
        child["MIGKIT_REPORTS"] = str(Path(reports).expanduser())
    began = time.time()
    got = subprocess.run([sys.executable, "-m", "migkit.cli", str(command),
                          str(hop), *[str(a) for a in args]],
                         capture_output=True, text=True, env=child,
                         timeout=timeout)
    root = _reports(reports) / str(hop)
    verdict = root / "verdict.json"
    status = None
    try:
        if verdict.stat().st_mtime >= began - 1:
            status = json.loads(verdict.read_text()).get("status")
        else:
            verdict = None
    except (OSError, ValueError):
        verdict = None
    evidence = None
    runs = [p for p in (root / "evidence").glob("*/evidence.json")
            if p.stat().st_mtime >= began - 1] \
        if (root / "evidence").is_dir() else []
    if runs:
        evidence = max(runs, key=lambda p: p.stat().st_mtime)
    lines = (got.stdout + got.stderr).splitlines()
    return {"command": str(command), "hop": str(hop),
            "exit": got.returncode, "status": status,
            "verdict": str(verdict) if verdict else None,
            "evidence": str(evidence) if evidence else None,
            "output": "\n".join(lines[-TAIL:])}


def passed(result, fail_on_difference=True):
    """Whether a task that ran `result` succeeds: the command ended well,
    or it found a difference and the task is told not to fail on one -
    read from the one table the command's exit code comes from
    (`verdict.VERDICTS`), so a task and the command never disagree. A
    verdict that is not `same` fails even where the command ended 0: a
    run that wrote one is held to it."""
    from . import verdict
    status = result.get("status")
    if status is not None and status not in verdict.VERDICTS:
        return False
    if status in ("error", "incomplete"):
        return False
    if status == "different" or \
            result["exit"] == verdict.VERDICTS["different"].exit:
        return not fail_on_difference and status == "different"
    return result["exit"] == verdict.EXIT["done"]


def said(result):
    """The one line a scheduler's log shows for a result."""
    return (f"migkit {result['command']} {result['hop']}: "
            + (result["status"] or f"exit {result['exit']}")
            + (f"; evidence: {result['evidence']}" if result["evidence"]
               else ""))


# ---- a step of a CI job or a workflow ----------------------------------------

def step(environ=None):
    """One step of a GitHub Actions job (`action.yml`) or an Argo workflow
    (`deploy/argo/`): `run` with what the step was given in its
    environment - MIGKIT_COMMAND, MIGKIT_HOP, MIGKIT_ARGS (split as a
    shell would), MIGKIT_CONF, MIGKIT_REPORTS, MIGKIT_FAIL_ON_DIFFERENCE -
    and what it answered written where the runner reads it: GitHub's
    outputs and job summary (the run's evidence, as text) where GitHub
    names them, and a file an output under MIGKIT_OUTPUTS, with the run's
    evidence copied beside them, for a workflow to pick up. Returns the
    exit code the step ends with."""
    import shlex
    import shutil

    from . import schedule
    e = os.environ if environ is None else environ
    got = run(e["MIGKIT_COMMAND"] or "check", e["MIGKIT_HOP"],
              shlex.split(e.get("MIGKIT_ARGS") or ""),
              e.get("MIGKIT_CONF") or None, e.get("MIGKIT_REPORTS") or None)
    print(got["output"])
    print(said(got))
    evidence = Path(got["evidence"]).parent if got["evidence"] else None
    outputs = {"status": got["status"] or "", "exit": str(got["exit"]),
               "evidence": str(evidence or "")}
    if e.get("GITHUB_OUTPUT"):
        with open(e["GITHUB_OUTPUT"], "a") as f:
            f.writelines(f"{k}={v}\n" for k, v in outputs.items())
    if e.get("GITHUB_STEP_SUMMARY"):
        with open(e["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write(_summary(got, evidence) + "\n")
    if e.get("MIGKIT_OUTPUTS"):
        out = Path(e["MIGKIT_OUTPUTS"])
        out.mkdir(parents=True, exist_ok=True)
        for k, v in outputs.items():
            (out / k).write_text(v)
        if evidence is not None:
            shutil.copytree(evidence, out / "evidence", dirs_exist_ok=True)
    return 0 if passed(got, schedule.flag("MIGKIT_FAIL_ON_DIFFERENCE",
                                         True, e)) \
        else (got["exit"] or verdict_exit(got))


def verdict_exit(got):
    """The exit code a step that ended 0 over a verdict that fails it ends
    with: the verdict's own."""
    from . import verdict
    return verdict.VERDICTS.get(got.get("status") or "",
                                verdict.VERDICTS["error"]).exit


def _summary(got, evidence):
    """The job's summary: the run's evidence as text, or the one line
    where there is none to show - or it is encrypted to the hop's
    recipients, and only they read it."""
    from .evidence import AGE
    if evidence is None:
        return said(got)
    try:
        raw = (evidence / "evidence.md").read_bytes()
    except OSError:
        return said(got)
    if raw.startswith(AGE):
        return (said(got) + " (the evidence is encrypted to the hop's"
                " recipients)")
    return raw.decode("utf-8", "replace")


# ---- Airflow ----------------------------------------------------------------

def _airflow_operator():
    try:
        from airflow.sdk import BaseOperator
    except ImportError:
        from airflow.models import BaseOperator
    from airflow.exceptions import AirflowException

    class MigkitOperator(BaseOperator):
        """A migkit command on a hop, as an Airflow task: its result
        (`run`) is the task's return value, so XCom holds the status and
        the evidence's path; a command that fails fails the task, and so
        does a difference unless `fail_on_difference` is False."""
        template_fields = ("command", "hop", "args")

        def __init__(self, *, command, hop, args=(), conf=None,
                     reports=None, fail_on_difference=True, **kwargs):
            super().__init__(**kwargs)
            self.command, self.hop, self.args = command, hop, list(args)
            self.conf, self.reports = conf, reports
            self.fail_on_difference = fail_on_difference

        def execute(self, context):
            got = run(self.command, self.hop, self.args, self.conf,
                      self.reports)
            self.log.info(said(got))
            if not passed(got, self.fail_on_difference):
                raise AirflowException(said(got) + "\n" + got["output"])
            return got

    return MigkitOperator


# ---- Dagster ----------------------------------------------------------------

def migkit_op(name, command, hop, args=(), conf=None, reports=None,
              fail_on_difference=True):
    """A Dagster op running a migkit command on a hop: it returns `run`'s
    result and fails (`dagster.Failure`, with the evidence's path) where
    the command does."""
    from dagster import Failure, MetadataValue, op

    @op(name=name)
    def _migkit(context):
        got = run(command, hop, args, conf, reports)
        context.log.info(said(got))
        if not passed(got, fail_on_difference):
            raise Failure(description=said(got), metadata={
                "output": MetadataValue.text(got["output"]),
                **({"evidence": MetadataValue.path(got["evidence"])}
                   if got["evidence"] else {})})
        return got
    return _migkit


def __getattr__(name):
    if name == "MigkitOperator":
        return _airflow_operator()
    raise AttributeError(name)
