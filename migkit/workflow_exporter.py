"""Workflow exporter for Apache Airflow, Dagster, and Argo Workflows (R23.8)."""
from typing import Any, Dict, List, Optional


DEFAULT_PHASES = ["assess", "plan", "copy", "verify", "cutover"]


def export_airflow_dag(hop_name: str, phases: Optional[List[str]] = None,
                       dag_id: Optional[str] = None, schedule: Optional[str] = None,
                       fail_on_difference: bool = True) -> str:
    actual_dag_id = dag_id or f"migkit_migration_{hop_name.replace('-', '_')}"
    step_phases = phases if phases else DEFAULT_PHASES
    sched_str = repr(schedule) if schedule else "None"

    lines = [
        '"""Auto-generated Airflow DAG for migkit database migration."""',
        "from datetime import datetime, timedelta",
        "from airflow import DAG",
        "from migkit.integrations import MigkitOperator",
        "",
        "default_args = {",
        "    'owner': 'data-platform',",
        "    'depends_on_past': False,",
        "    'retries': 1,",
        "    'retry_delay': timedelta(minutes=2),",
        "}",
        "",
        f"with DAG(",
        f"    dag_id={actual_dag_id!r},",
        f"    default_args=default_args,",
        f"    description='Automated database migration workflow for hop {hop_name}',",
        f"    schedule_interval={sched_str},",
        f"    start_date=datetime(2026, 1, 1),",
        f"    catchup=False,",
        f"    tags=['migkit', 'migration', {hop_name!r}],",
        ") as dag:",
        "",
    ]

    task_names = []
    for p in step_phases:
        t_id = f"task_{p}"
        task_names.append(t_id)
        cmd = p if p != "copy" else "move"
        args_str = "['--mode', 'full']" if p == "copy" else "[]"
        lines.extend([
            f"    {t_id} = MigkitOperator(",
            f"        task_id={t_id!r},",
            f"        command={cmd!r},",
            f"        hop={hop_name!r},",
            f"        args={args_str},",
            f"        fail_on_difference={fail_on_difference},",
            f"    )",
            "",
        ])

    chain = " >> ".join(task_names)
    lines.append(f"    {chain}")
    return "\n".join(lines) + "\n"


def export_dagster_job(hop_name: str, phases: Optional[List[str]] = None,
                       job_name: Optional[str] = None,
                       fail_on_difference: bool = True) -> str:
    actual_job_name = job_name or f"migkit_{hop_name.replace('-', '_')}_job"
    step_phases = phases if phases else DEFAULT_PHASES

    lines = [
        '"""Auto-generated Dagster Job for migkit database migration."""',
        "from dagster import job, Out",
        "from migkit.integrations import migkit_op",
        "",
    ]

    op_names = []
    for p in step_phases:
        op_id = f"op_{p}"
        op_names.append(op_id)
        cmd = p if p != "copy" else "move"
        args_str = "['--mode', 'full']" if p == "copy" else "[]"
        lines.append(
            f"{op_id} = migkit_op(name={op_id!r}, command={cmd!r}, "
            f"hop={hop_name!r}, args={args_str}, fail_on_difference={fail_on_difference})"
        )

    lines.extend([
        "",
        f"@job(name={actual_job_name!r})",
        f"def {actual_job_name}():",
    ])

    prev_res = None
    for idx, op in enumerate(op_names):
        res_var = f"res_{idx}"
        if prev_res:
            lines.append(f"    {res_var} = {op}()")
        else:
            lines.append(f"    {res_var} = {op}()")
        prev_res = res_var

    return "\n".join(lines) + "\n"


def export_argo_workflow(hop_name: str, phases: Optional[List[str]] = None,
                         workflow_name: Optional[str] = None,
                         image: str = "ghcr.io/org/migkit:latest") -> str:
    actual_wf_name = workflow_name or f"migkit-{hop_name.replace('_', '-')}"
    step_phases = phases if phases else DEFAULT_PHASES

    lines = [
        "apiVersion: argoproj.io/v1alpha1",
        "kind: Workflow",
        "metadata:",
        f"  generateName: {actual_wf_name}-",
        "  labels:",
        "    app.kubernetes.io/managed-by: migkit",
        f"    migkit.io/hop: {hop_name}",
        "spec:",
        "  entrypoint: migration-dag",
        "  templates:",
        "  - name: migration-dag",
        "    dag:",
        "      tasks:",
    ]

    prev_task = None
    for p in step_phases:
        task_name = f"step-{p}"
        cmd = p if p != "copy" else "move"
        args_str = "--mode full" if p == "copy" else ""
        lines.append(f"      - name: {task_name}")
        lines.append(f"        template: run-migkit-cmd")
        if prev_task:
            lines.append(f"        dependencies: [{prev_task}]")
        lines.append(f"        arguments:")
        lines.append(f"          parameters:")
        lines.append(f"          - name: command")
        lines.append(f"            value: \"{cmd}\"")
        lines.append(f"          - name: args")
        lines.append(f"            value: \"{args_str}\"")
        prev_task = task_name

    lines.extend([
        "",
        "  - name: run-migkit-cmd",
        "    inputs:",
        "      parameters:",
        "      - name: command",
        "      - name: args",
        "    container:",
        f"      image: {image}",
        "      command: [python3, -m, migkit.integrations]",
        "      env:",
        "      - name: MIGKIT_COMMAND",
        "        value: \"{{inputs.parameters.command}}\"",
        f"      - name: MIGKIT_HOP",
        f"        value: \"{hop_name}\"",
        "      - name: MIGKIT_ARGS",
        "        value: \"{{inputs.parameters.args}}\"",
        "      - name: MIGKIT_FAIL_ON_DIFFERENCE",
        "        value: \"true\"",
    ])

    return "\n".join(lines) + "\n"


def export_workflow(target: str, hop_name: str, **kwargs: Any) -> str:
    target_lower = target.lower()
    if target_lower == "airflow":
        return export_airflow_dag(hop_name, **kwargs)
    elif target_lower == "dagster":
        return export_dagster_job(hop_name, **kwargs)
    elif target_lower == "argo":
        return export_argo_workflow(hop_name, **kwargs)
    else:
        raise ValueError(f"unsupported workflow target '{target}'; expected 'airflow', 'dagster', or 'argo'")
