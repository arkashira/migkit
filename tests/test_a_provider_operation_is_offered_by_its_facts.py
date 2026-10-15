"""The providers' own operations (blue/green, in-place upgrade, RDS into
Aurora, Single to Flexible Server, Cloud SQL's upgrade) are read from the
adapters, held to the instances' facts, and timed only from evidence: a
documented startup and a rate measured on a rehearsal, predicted from
another hop's run, refused with none. An operation no adapter call runs is
not yet, never assumed."""
from dataclasses import replace
from types import SimpleNamespace

from migkit import cloudapi, decide, planner, scenarios
from migkit.cloudapi import Instance


def op(provider, name):
    return next(o for o in cloudapi.OPERATIONS[provider] if o.name == name)


def pair(**dst):
    src = Instance("aws", "rds", "a", engine="postgres", version="15.4",
                   klass="db.r6g.large", status="available", storage_gb=100,
                   maintenance="sun:03:00-sun:04:00", replica_of="",
                   upgrades=frozenset({"16.2"}))
    return src, replace(src, id="b", version="16.2", **dst)


def facts(offer, size=10 ** 9, **dst):
    src, target = pair(**dst)
    return decide.Facts({"source": src, "target": target,
                         "api_reachable": True, "offers": (offer,),
                         "size_bytes": size})


def offer_of(o, runs=(), execute=lambda: "done", truth="measured"):
    return {"name": o.name, "said": o.said, "checks": o.checks,
            "execute": execute, "why_not": o.why_not,
            "startup_seconds": o.startup, "startup_source": o.startup_source,
            "runs": list(runs), "duration_source": "on the rehearsal r",
            "truth": truth}


RUN = [{"bytes": 10 ** 9, "seconds": 1300}]


def test_each_provider_catalogues_its_own_operations():
    names = {o.name for ops in cloudapi.OPERATIONS.values() for o in ops}
    assert {"aws-blue-green", "aws-major-upgrade", "aws-aurora-replica",
            "aws-aurora-snapshot", "azure-single-to-flexible",
            "gcp-major-upgrade"} <= names
    for ops in cloudapi.OPERATIONS.values():
        for o in ops:
            assert o.checks and (o.runs or o.why_not)


def test_the_adapter_offers_its_products_operations_and_no_runner():
    a = cloudapi.Aws({"product": "rds", "id": "a"})
    assert {o.name for o in a.operations()} == {
        "aws-blue-green", "aws-major-upgrade", "aws-aurora-replica",
        "aws-aurora-snapshot"}
    assert all(a.runner(o) is None for o in a.operations())
    got = scenarios.provider_offers(SimpleNamespace(), a)
    assert {o["name"] for o in got} == {o.name for o in a.operations()}
    assert all(o["execute"] is None for o in got)


def test_an_eligible_upgrade_is_timed_by_its_startup_and_measured_rate():
    o = op("aws", "aws-blue-green")
    got = scenarios.managed_case(facts(offer_of(o, RUN)))
    assert got.choice.path == "aws-blue-green", got.words()
    # 300 s documented, then 1 GB in the 1000 s past it
    assert got.choice.cost().mean == 1300
    assert got.choice.cost().truth == "measured"
    big = scenarios.duration(offer_of(o, RUN), 4 * 10 ** 9)
    assert big.mean == 4300 and big.sd > 0


def test_an_unknown_upgrade_list_is_not_yet_never_eligible():
    o = op("aws", "aws-major-upgrade")
    f = facts(offer_of(o, RUN))
    f["source"] = replace(f["source"], upgrades=None)
    got = scenarios.managed_case(f)
    assert got.choice.path is None
    assert "not yet" in got.words()
    assert "a version the source may be upgraded to" in got.words()


def test_a_version_the_provider_does_not_list_is_refused():
    f = facts(offer_of(op("aws", "aws-major-upgrade"), RUN))
    f["target"] = replace(f["target"], version="17.0")
    got = scenarios.managed_case(f)
    assert got.choice.path is None
    assert "no upgrade of the source's" in got.words()


def test_an_unknown_version_and_an_ineligible_engine_are_refused():
    o = op("aws", "aws-aurora-replica")
    f = facts(offer_of(o, RUN), engine="aurora-postgresql")
    assert "same major version" in scenarios.managed_case(f).words()
    f = facts(offer_of(o, RUN), engine="mysql")
    got = scenarios.managed_case(f)
    assert got.choice.path is None
    assert "not the source engine's Aurora" in got.words()
    f = facts(offer_of(o, RUN), engine="aurora-postgresql")
    f["target"] = replace(f["target"], version="")
    assert "not yet" in scenarios.managed_case(f).words()


def test_a_read_replica_source_gets_no_blue_green():
    f = facts(offer_of(op("aws", "aws-blue-green"), RUN))
    f["source"] = replace(f["source"], replica_of="primary-1")
    assert "the source is a read replica" in scenarios.managed_case(f).words()


def test_no_evidence_is_no_duration_and_no_runner_is_not_yet():
    o = op("aws", "aws-blue-green")
    got = scenarios.managed_case(facts(offer_of(o)))
    assert got.choice.path is None
    assert "no rehearsal or other hop has run" in got.words()
    got = scenarios.managed_case(facts(offer_of(o, RUN, execute=None)))
    assert got.choice.path is None
    assert "not yet: migkit's AWS adapter does not yet run" in got.words()


def test_single_to_flexible_is_never_assumed():
    o = op("azure", "azure-single-to-flexible")
    src = Instance("azure", "flexible-postgres", "a", engine="postgres",
                   version="16", klass="GP", status="ready", storage_gb=64,
                   maintenance="Sunday 03:00")
    f = decide.Facts({"source": src, "target": replace(src, id="b"),
                      "api_reachable": True, "size_bytes": 1,
                      "offers": (offer_of(o, RUN, execute=None),)})
    got = scenarios.managed_case(f)
    assert got.choice.path is None
    assert "reads no Single Server" in got.words()


def test_two_sizes_are_fitted_and_one_size_is_only_scaled_up():
    o = offer_of(op("gcp", "gcp-major-upgrade"),
                 [{"bytes": 10 ** 9, "seconds": 700},
                  {"bytes": 3 * 10 ** 9, "seconds": 1700}])
    assert o["startup_seconds"] is None
    assert round(scenarios.duration(o, 2 * 10 ** 9).mean) == 1200
    one = offer_of(op("gcp", "gcp-major-upgrade"), RUN)
    assert scenarios.duration(one, 2 * 10 ** 9).mean == 2600
    assert scenarios.duration(one, 10 ** 8).mean == 1300
    assert scenarios.duration(one, None) is None


def test_runs_are_measured_on_a_rehearsal_and_predicted_elsewhere(monkeypatch):
    monkeypatch.setattr(planner, "runs", lambda hop, key: [])
    monkeypatch.setattr(planner, "_elsewhere",
                        lambda hop, key: (RUN, ["the rehearsal r"]))
    assert scenarios.operation_runs(None, "x")[1:] == (
        "measured on the rehearsal r", "measured")
    monkeypatch.setattr(planner, "_elsewhere", lambda hop, key: (RUN, ["h1"]))
    assert scenarios.operation_runs(None, "x")[2] == "predicted"
    monkeypatch.setattr(planner, "_elsewhere", lambda hop, key: ([], []))
    assert scenarios.operation_runs(None, "x") == ([], "", "")


def test_a_verified_run_is_kept_as_the_next_plans_evidence(monkeypatch):
    kept = []
    monkeypatch.setattr(planner, "_add_run",
                        lambda hop, key, run: kept.append((key, run)))
    monkeypatch.setattr(planner, "_kind", lambda hop: "postgres")
    f = facts(offer_of(op("aws", "aws-blue-green"), RUN))
    scenarios.run_managed_case(f, lambda: True, hop=object())
    assert kept and kept[0][0] == "provider operation aws-blue-green"
    assert kept[0][1]["bytes"] == 10 ** 9


def test_cloud_sql_states_its_upgrade_targets_and_replicas(monkeypatch):
    g = cloudapi.Gcp.__new__(cloudapi.Gcp)
    g.provider, g.product, g.id = "gcp", "cloudsql", "a"
    monkeypatch.setattr(g, "_sql", lambda: {
        "databaseVersion": "POSTGRES_15", "state": "RUNNABLE",
        "settings": {"tier": "db-custom-2-7680", "dataDiskType": "PD_SSD"},
        "replicaNames": ["r1"],
        "upgradableDatabaseVersions": [{"name": "POSTGRES_16"}]},
        raising=False)
    monkeypatch.setattr(g, "_tier", lambda k: (2, 7.5), raising=False)
    monkeypatch.setattr(g, "_address", lambda it: "", raising=False)
    got = g.instance()
    assert got.upgrades == frozenset({"16"})
    assert got.replicas == 1 and got.replica_of == ""
    assert got.storage_type == "PD_SSD"
