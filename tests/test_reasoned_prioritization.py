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


def test_what_the_hop_measured_reorders_the_items():
    """The item's own figure is a prediction: once the hop timed both, the
    measured costs decide, not the figures."""
    a = d.PriorityItem("count_rows", cost=1.0)
    b = d.PriorityItem("sample_rows", cost=5.0)
    assert [i.name for i in d.rank_priority([a, b])] == [
        "count_rows", "sample_rows"]
    costs = d.Costs()
    costs.saw("count_rows", 1, 9.0)
    costs.saw("sample_rows", 1, 2.0)
    ranked = d.rank_priority([a, b], costs=costs)
    assert [i.name for i in ranked] == ["sample_rows", "count_rows"]
    assert "measured" in ranked[0].why


def test_a_worse_loss_goes_first_whatever_it_costs():
    cheap = d.PriorityItem("tune_batch", risk="performance", cost=0.01)
    dear = d.PriorityItem("check_keys", risk="data_loss", cost=500.0)
    assert [i.name for i in d.rank_priority([cheap, dear])] == [
        "check_keys", "tune_batch"]


def test_a_probe_goes_by_its_cost_for_the_choice_it_settles():
    """Two probes of the same cost: the one more likely to change a
    choice first; one of no worth to any choice after both."""
    weak = d.PriorityItem("probe_a", kind="probe", cost=1.0,
                          uncertainty=0.2, decision_impact=0.2)
    strong = d.PriorityItem("probe_b", kind="probe", cost=1.0,
                            uncertainty=0.9, decision_impact=0.9)
    idle = d.PriorityItem("probe_c", kind="probe", cost=0.01)
    assert [i.name for i in d.rank_priority([weak, idle, strong])] == [
        "probe_b", "probe_a", "probe_c"]


def test_an_irreversible_item_waits_for_every_mandatory_one():
    act = d.PriorityItem("drop_source", reversibility="irreversible",
                         risk="data_loss", cost=0.01)
    proof = d.PriorityItem("verdict", mandatory=True, risk="none",
                           cost=50.0)
    other = d.PriorityItem("count", mandatory=True, risk="none", cost=60.0)
    names = [i.name for i in d.rank_priority([act, proof, other])]
    assert names.index("drop_source") == 2


def test_the_budget_is_spent_at_the_measured_cost():
    costs = d.Costs()
    costs.saw("optional_metrics", 1, 20.0)
    p1 = d.PriorityItem("optional_metrics", cost=1.0)
    p2 = d.PriorityItem("optional_stats", cost=1.0)
    names = [i.name for i in d.rank_priority([p1, p2], max_cost=5.0,
                                             costs=costs)]
    assert names == ["optional_stats"]
