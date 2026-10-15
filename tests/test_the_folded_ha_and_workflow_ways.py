"""What pipeline_ha and workflow had that their owners lacked, now in
the owners (FIX-1b): a throttle cut the server did not answer is
doubled; a failed health probe is said; a takeover says how long the hop
went with no holder; the workflow export runs migkit's real commands."""
import json
import time

import pytest

from migkit import lease, workflow_exporter
from migkit.throttle import Health, Throttle


class _Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


def test_a_cut_the_server_did_not_answer_is_doubled():
    c = _Clock()
    states = iter([Health(busy_ratio=0.9)] * 3 + [Health(busy_ratio=0.1)] * 50)
    t = Throttle(16, probe=lambda: next(states), clock=c, sleeper=c.sleep)
    orig = t._health

    def health():
        t._last_probe_at = None  # every check probes, for the test
        return orig()
    t._health = health
    t.gate()
    # 16 -1 -2 -4: three stressed checks in a row, none relieved
    assert t.narrowed_to == 9
    assert t.unrelieved == 0 and t.permits == 10


def test_relief_resets_the_doubling():
    c = _Clock()
    t = Throttle(8, probe=None, clock=c, sleeper=c.sleep)
    t.unrelieved = 3
    t.gate()
    assert t.unrelieved == 0 and t.narrowed_to == 8


def test_a_failed_probe_is_counted_and_said():
    c = _Clock()

    def probe():
        raise PermissionError("permission denied for pg_stat_activity")
    t = Throttle(4, probe=probe, clock=c, sleeper=c.sleep)
    with t.unit():
        pass
    assert t.unprobed == {"PermissionError: permission denied for"
                          " pg_stat_activity": 1}
    assert "could not be read" in t.line()
    assert t.summary() == {}


def test_a_replica_rise_that_went_with_slow_reads_is_measured(monkeypatch):
    got = []
    monkeypatch.setattr("migkit.tuning.observe",
                        lambda name, v, hop=None: got.append((name, v)))
    t = Throttle(4)
    t.least_lag = 1.0
    t._measure_rise(Health(lag_seconds=3.5, replica=True))
    t._measure_rise(Health(lag_seconds=4.5, replica=True))
    assert got == [("throttle.REPLICA_RISE", 2.5)]
    t2 = Throttle(4)
    t2.least_lag = 1.0
    t2._measure_rise(Health(lag_seconds=3.5, replica=False))
    t2._measure_rise(Health(lag_seconds=0.5, replica=True))
    assert t2.rise_measured is None


def test_a_takeover_says_how_long_the_hop_had_no_holder(tmp_path):
    path = tmp_path / "lease.json"
    heard = time.time() - 42
    path.write_text(json.dumps({"holder": "gone:1:1", "host": "elsewhere",
                                "pid": 1, "since": heard - 100,
                                "renewed": heard, "expires": heard + 1,
                                "fence": 3}))
    got = lease.Lease(path, fenced=True, term=60).acquire()
    try:
        assert got.took_over["host"] == "elsewhere"
        assert 41 <= got.unheld <= 60
        assert got.fence == 4
        assert json.loads(path.read_text())["renewed"] >= heard
    finally:
        got.release()


def test_a_lease_taken_free_was_not_taken_over(tmp_path):
    got = lease.Lease(tmp_path / "lease.json", term=60).acquire()
    try:
        assert got.took_over is None and got.unheld is None
    finally:
        got.release()


def test_every_exported_step_is_a_real_command():
    from migkit import cli
    names = set(cli.main.commands)
    for command, _ in workflow_exporter.STEPS.values():
        assert command in names, command


def test_the_airflow_dag_runs_the_steps_in_order():
    dag = workflow_exporter.export_airflow_dag("orders", schedule="@daily")
    compile(dag, "dag.py", "exec")
    assert "assess >> plan >> copy >> verify >> cutover" in dag
    assert "command='check'" in dag and "command='verify'" not in dag
    assert "['--mode', 'cdc', '--drop', '--go']" in dag


def test_the_dagster_job_chains_each_op_on_the_one_before():
    job = workflow_exporter.export_dagster_job("orders",
                                               phases=["assess", "verify"])
    compile(job, "job.py", "exec")
    assert "after=False" in job and "after=True" in job
    assert "done_verify = verify(after=done_assess)" in job


def test_the_argo_workflow_uses_the_shipped_template():
    yaml = workflow_exporter.export_argo_workflow("orders", image="m:1")
    assert yaml.count("templateRef:") == 5
    assert "depends: copy" in yaml and 'value: "check"' in yaml
    assert 'value: "m:1"' in yaml and "claimName: migkit-state" in yaml


def test_an_unknown_step_or_target_is_refused():
    with pytest.raises(ValueError, match="no workflow step 'deploy'"):
        workflow_exporter.export_airflow_dag("o", phases=["deploy"])
    with pytest.raises(ValueError, match="named twice"):
        workflow_exporter.export_argo_workflow("o", phases=["copy", "copy"])
    with pytest.raises(ValueError, match="no workflow target"):
        workflow_exporter.export_workflow("cron", "o")
    import migkit.integrations as mi
    assert "kind: Workflow" in mi.export_workflow("argo", "o")
