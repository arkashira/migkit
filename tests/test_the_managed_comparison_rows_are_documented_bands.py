"""The comparison's managed-service column (backlog C1.managed) is filled
from what the providers' own migration documents for each scenario
(`scenarios.managed_rows`, `data/managed.yaml`'s `comparison`): every cell
says documented, not measured; a band's numbers are the operation's own;
a measured cell is never touched by it."""
from pathlib import Path

from migkit import autonomous, scenarios
from migkit.machine import _took

DOC = Path(__file__).resolve().parents[1] / "docs/research/autonomous-comparison.md"


def test_every_scenario_has_a_documented_cell():
    rows = scenarios.managed_rows()
    assert set(rows) == set(autonomous.SCENARIOS.values())
    for row, cell in rows.items():
        assert cell.startswith(scenarios.MANAGED_SOURCE + ": "), row
        assert "|" not in cell


def test_a_band_is_the_operation_s_own_documented_numbers():
    band = scenarios.operation_rows()["aws-aurora-replica"]["duration"]
    lo, hi = band["per_tib"]
    cell = scenarios.managed_rows()["healthy"]
    assert f"{_took(lo)} to {_took(hi)} a TiB" in cell


def test_the_page_s_managed_column_is_the_catalogue_s():
    table = DOC.read_text()
    # written by the generator: an empty run writes it back the same
    assert autonomous.comparison([], table) == table
    rows = scenarios.managed_rows()
    seen = 0
    for line in table.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if line.startswith("| ") and cells[0] in rows:
            assert cells[-1] == rows[cells[0]]
            seen += 1
    assert seen >= len(rows)


def test_measured_cells_stay_the_runs_and_an_unknown_row_is_left():
    head = ("| Scenario | Engine | " + " | ".join(autonomous.MEASURED)
            + " | Managed service |\n")
    blank = " | ".join(["to be measured"] * (len(autonomous.MEASURED) + 1))
    table = (head + f"| healthy | MySQL | {blank} |\n"
             + f"| something else | MySQL | {blank} |\n")
    line = '{"scenario": "healthy", "engine": "mysql", "seconds": 4.5}'
    out = autonomous.comparison([line], table).splitlines()
    healthy = [c.strip() for c in out[1].strip("|").split("|")]
    assert healthy[2 + autonomous.MEASURED.index("seconds")] == "4.5"
    assert healthy[2] == "to be measured"
    assert healthy[-1] == scenarios.managed_rows()["healthy"]
    assert out[2] == table.splitlines()[2]
