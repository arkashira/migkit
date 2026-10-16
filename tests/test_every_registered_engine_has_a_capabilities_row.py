"""Every engine the package can build is a row of the capability table
(backlog AUD-Z): `engines.NAMES`, the cloud services in `engines.CLOUD`
and every name `_class_for` answers - an engine with no row is one the
commands cannot say anything about, and `doctor` could not tell what it
refuses."""
from migkit import capabilities, engines

PROVES = ()


def test_every_name_the_package_knows_is_a_row():
    rows = set(capabilities.matrix())
    known = set(engines.NAMES) | set(engines.CLOUD)
    assert known - rows == set(), \
        f"engines with no capabilities row: {sorted(known - rows)}"


def test_every_cloud_service_is_a_name_an_address_can_use():
    assert set(engines.CLOUD) - set(engines.NAMES) == set()


def test_every_name_builds_a_class():
    gone = [n for n in engines.NAMES if engines._class_for(n) is None]
    assert gone == [], f"names with no engine class: {gone}"


def test_every_row_is_an_engine_the_package_builds():
    rows = set(capabilities.matrix())
    known = set(engines.NAMES) | set(engines.CLOUD)
    assert rows - known == set(), \
        f"capabilities rows no engine answers: {sorted(rows - known)}"
