"""A provider's own operation ranked against migkit's own move
(`scenarios.managed_case`, `decide.managed_against_own`): its duration
from the catalogue's documented band (`data/managed.yaml` operations) or
measured runs, its eligibility read off the provider's facts. Stand-ins
for the providers: built; not run against a real AWS, Azure or Google
Cloud."""
from dataclasses import replace

from migkit import decide, scenarios
from migkit.cloudapi import OPERATIONS, Instance
from migkit.costmodel import Estimate

TIB = 1024 ** 4
REPLICA = next(o for o in OPERATIONS["aws"] if o.name == "aws-aurora-replica")


def _facts(forecast, engine="aurora-postgresql", version="15", size=TIB):
    src = Instance("aws", "rds", "source", engine="postgres", version="15.4",
                   klass="db.r6g.large", status="available", storage_gb=4000,
                   maintenance="sun:03:00-sun:04:00", replica_of="")
    dst = replace(src, id="target", engine=engine, version=version)
    offer = {"name": REPLICA.name, "said": REPLICA.said,
             "checks": REPLICA.checks, "execute": lambda: "promoted",
             "runs": [], "documented":
                 scenarios.operation_rows()[REPLICA.name]}
    return decide.Facts({"source": src, "target": dst, "api_reachable": True,
                         "offers": (offer,), "size_bytes": size,
                         "forecast": forecast})


def test_every_operation_has_a_catalogue_row():
    rows = scenarios.operation_rows()
    for ops in OPERATIONS.values():
        for op in ops:
            assert rows[op.name]["preconditions"], op.name
            assert rows[op.name]["duration"]["said"], op.name


def test_an_eligible_operation_faster_than_the_forecast_is_chosen():
    got = scenarios.managed_case(_facts(Estimate(30 * 3600, 3600,
                                                 truth="predicted")))
    assert got.choice.path == "aws-aurora-replica"
    words = got.words()
    assert "documented" in words and "several hours per tebibyte" in words
    assert "against migkit's own move" in words
    assert "before it runs, as the provider documents" in words
    assert "no slower than migkit's own move" in words


def test_a_forecast_beating_the_band_keeps_migkits_own_move():
    got = scenarios.managed_case(_facts(Estimate(1800, 60, truth="measured")))
    assert "migkit's own move and its full verify" in got.words()
    assert "is longer than migkit's own" in got.words()


def test_ineligible_facts_keep_migkits_own_move_with_the_reason():
    got = scenarios.managed_case(_facts(Estimate(1800, 60),
                                        engine="postgres"))
    assert got.choice.path is None
    assert "the target is not the source engine's Aurora" in got.words()
    assert "no provider operation is eligible" in got.words()


def test_a_fact_the_api_does_not_give_is_said_not_yet():
    f = _facts(Estimate(1800, 60))
    f["target"] = replace(f["target"], version=None)
    got = scenarios.managed_case(f)
    assert got.choice.path is None and "not yet" in got.words()
    # no documented rate and no run: the duration is not yet, never assumed
    blue = next(o for o in OPERATIONS["aws"] if o.name == "aws-blue-green")
    offer = {"said": blue.said, "runs": [],
             "documented": scenarios.operation_rows()[blue.name]}
    assert scenarios.duration(offer, TIB) is None
    assert "documentation states none" in scenarios.duration_why(offer, TIB)


def test_no_forecast_ranks_no_operation_above_migkit():
    got = scenarios.managed_case(_facts(None))
    assert "has no forecast for the data" in got.words()
