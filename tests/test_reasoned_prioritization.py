"""Reasoned dynamic prioritization of tasks, evidence and probes (backlog P0-DECISION-ALL.priority).

Ranked by mandatory guarantees, consequence, dependencies, uncertainty,
decision value and reversibility; never a fixed list for all workloads.
"""
from migkit import decide as d


def test_mandatory_guarantee_ranks_ahead_of_faster_optional_work():
    optional_fast = d.PriorityItem("cache_check", kind="probe", cost=0.01,
                                   mandatory=False)
    mandatory_slow = d.PriorityItem("checksum_verdict", kind="work", cost=5.0,
                                    mandatory=True, risk="correctness")
    ranked = d.rank_priority([optional_fast, mandatory_slow])
    assert ranked[0].name == "checksum_verdict"
    assert "mandatory correctness guarantee" in ranked[0].why


def test_high_impact_low_cost_probes_run_first_to_unlock_decisions():
    bulky_copy = d.PriorityItem("bulk_table_copy", kind="work", cost=120.0,
                                risk="none")
    quick_probe = d.PriorityItem("probe_gtid_mode", kind="probe", cost=0.05,
                                 uncertainty=0.9, decision_impact=0.95,
                                 critical_path=True, unlocks=("bulk_table_copy",))
    ranked = d.rank_priority([bulky_copy, quick_probe])
    assert ranked[0].name == "probe_gtid_mode"
    assert "on critical path" in ranked[0].why


def test_irreversible_action_held_behind_prerequisites():
    destructive_cutover = d.PriorityItem("cutover_traffic", kind="work",
                                         reversibility="irreversible",
                                         dependencies=("verify_parity",),
                                         risk="unavailability")
    verify_parity = d.PriorityItem("verify_parity", kind="work",
                                   reversibility="read_only",
                                   mandatory=True, risk="correctness")
    ranked = d.rank_priority([destructive_cutover, verify_parity])
    assert ranked[0].name == "verify_parity"
    assert ranked[1].name == "cutover_traffic"
    assert "irreversible action" in ranked[1].why


def test_budget_cutoff_preserves_mandatory_items():
    p1 = d.PriorityItem("mandatory_audit", mandatory=True, cost=10.0)
    p2 = d.PriorityItem("optional_metrics", mandatory=False, cost=5.0)
    p3 = d.PriorityItem("optional_stats", mandatory=False, cost=5.0)
    budgeted = d.rank_priority([p1, p2, p3], max_cost=8.0)
    names = [it.name for it in budgeted]
    assert "mandatory_audit" in names
