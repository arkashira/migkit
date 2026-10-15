"""A hop's whole move as a scheduler's workflow (backlog R23.8): an
Airflow DAG, a Dagster job or an Argo workflow, each step a migkit command
through `integrations` - the task, the op and the shipped workflow
template (`deploy/argo/migkit-workflowtemplate.yaml`) - so the workflow
holds no logic of its own, only the order.

The steps are the commands a person types, in the order they are typed
(`STEPS`): assess the hop, show the move's plan, move, check, cut over. A
step that fails stops the steps after it, as a person would stop.
"""
import shlex

#: step -> the command and its arguments; one place, read by every target
STEPS = {
    "assess": ("assess", ()),
    "plan": ("move", ("--mode", "full")),
    "copy": ("move", ("--mode", "full", "--go")),
    "verify": ("check", ()),
    "cutover": ("move", ("--mode", "cdc", "--drop", "--go")),
}
DEFAULT_PHASES = tuple(STEPS)


def _steps(phases):
    phases = tuple(phases or DEFAULT_PHASES)
    unknown = [p for p in phases if p not in STEPS]
    if unknown:
        raise ValueError(f"no workflow step {', '.join(map(repr, unknown))};"
                         f" the steps are {', '.join(STEPS)}")
    if len(set(phases)) != len(phases):
        raise ValueError("a workflow step is named twice: "
                         + ", ".join(phases))
    return [(p, *STEPS[p]) for p in phases]


def _name(hop_name, sep):
    return "".join(c if c.isalnum() else sep for c in hop_name).strip(sep)


def _yaml(text):
    return '"' + str(text).replace("\\", "\\\\").replace('"', '\\"') + '"'


def export_airflow_dag(hop_name, phases=None, dag_id=None, schedule=None,
                       fail_on_difference=True):
    dag_id = dag_id or f"migkit_{_name(hop_name, '_')}"
    steps = _steps(phases)
    lines = [
        f'"""migkit: the move of hop {hop_name}, step by step."""',
        "from datetime import datetime",
        "",
        "from airflow import DAG",
        "from migkit.integrations import MigkitOperator",
        "",
        "with DAG(",
        f"    dag_id={dag_id!r},",
        f"    schedule={schedule!r},",
        "    start_date=datetime(2026, 1, 1),",
        "    catchup=False,",
        "    max_active_runs=1,",
        f"    tags=['migkit', {hop_name!r}],",
        ") as dag:",
    ]
    for phase, command, args in steps:
        lines += [
            f"    {phase} = MigkitOperator(",
            f"        task_id={phase!r},",
            f"        command={command!r},",
            f"        hop={hop_name!r},",
            f"        args={list(args)!r},",
            f"        fail_on_difference={bool(fail_on_difference)!r},",
            "    )",
        ]
    lines.append("    " + " >> ".join(p for p, _, _ in steps))
    return "\n".join(lines) + "\n"


def export_dagster_job(hop_name, phases=None, job_name=None,
                       fail_on_difference=True):
    """Each op after the first waits on the one before (`after`), so the
    job runs the steps in order and stops at the first that fails."""
    job_name = job_name or f"migkit_{_name(hop_name, '_')}"
    steps = _steps(phases)
    lines = [
        f'"""migkit: the move of hop {hop_name}, step by step."""',
        "from dagster import job",
        "from migkit.integrations import migkit_op",
        "",
    ]
    for i, (phase, command, args) in enumerate(steps):
        lines.append(
            f"{phase} = migkit_op({phase!r}, {command!r}, {hop_name!r},"
            f" args={list(args)!r},"
            f" fail_on_difference={bool(fail_on_difference)!r},"
            f" after={i > 0!r})")
    lines += ["", "", f"@job(name={job_name!r})", f"def {job_name}():"]
    prev = None
    for phase, _, _ in steps:
        lines.append(f"    done_{phase} = {phase}("
                     + (f"after=done_{prev}" if prev else "") + ")")
        prev = phase
    return "\n".join(lines) + "\n"


def export_argo_workflow(hop_name, phases=None, workflow_name=None,
                         template="migkit", image="migkit:latest",
                         fail_on_difference=True):
    """A workflow of the shipped template's step (`templateRef`): the
    container and its secrets stay in the one template."""
    name = workflow_name or f"migkit-{_name(hop_name, '-').lower()}"
    steps = _steps(phases)
    lines = [
        "apiVersion: argoproj.io/v1alpha1",
        "kind: Workflow",
        "metadata:",
        f"  generateName: {name}-",
        "  labels:",
        "    app.kubernetes.io/managed-by: migkit",
        "spec:",
        "  entrypoint: move",
        "  arguments:",
        "    parameters:",
        "      - name: hop",
        f"        value: {_yaml(hop_name)}",
        "      - name: image",
        f"        value: {_yaml(image)}",
        "      - name: fail-on-difference",
        f"        value: {_yaml(str(bool(fail_on_difference)).lower())}",
        # a step taken from another template runs with this workflow's
        # volumes, not that template's: the same two it mounts
        "  volumes:",
        "    - name: hops",
        "      secret:",
        "        secretName: migkit-hops",
        "    - name: state",
        "      persistentVolumeClaim:",
        "        claimName: migkit-state",
        "  templates:",
        "    - name: move",
        "      dag:",
        "        tasks:",
    ]
    prev = None
    for phase, command, args in steps:
        lines += [
            f"          - name: {phase}",
            "            templateRef:",
            f"              name: {template}",
            "              template: migkit",
        ]
        if prev:
            lines.append(f"            depends: {prev}")
        lines += [
            "            arguments:",
            "              parameters:",
            "                - name: hop",
            f"                  value: {_yaml(hop_name)}",
            "                - name: command",
            f"                  value: {_yaml(command)}",
            "                - name: args",
            f"                  value: {_yaml(shlex.join(args))}",
        ]
        prev = phase
    return "\n".join(lines) + "\n"


TARGETS = {"airflow": export_airflow_dag, "dagster": export_dagster_job,
           "argo": export_argo_workflow}


def export_workflow(target, hop_name, **kwargs):
    got = TARGETS.get(str(target).lower())
    if got is None:
        raise ValueError(f"no workflow target {target!r}; the targets are"
                         f" {', '.join(TARGETS)}")
    return got(hop_name, **kwargs)
