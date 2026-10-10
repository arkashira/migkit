"""The playbook is chosen from the service the hop names; an unknown one
falls to migkit's own route and says so, where it used to refuse."""
from types import SimpleNamespace

from migkit import advisors


def test_a_service_with_a_playbook_gets_it():
    service, _why = advisors.pick(SimpleNamespace(name="h", service="aws-dms"))
    assert service == "aws-dms"


def test_a_service_without_one_falls_to_the_native_route_and_says_why():
    service, why = advisors.pick(SimpleNamespace(name="h", service="nope"))
    assert service == "native" and "nope" in why
    assert advisors.playbook(service)


def test_no_service_is_the_native_route():
    service, _why = advisors.pick(SimpleNamespace(name="h", service=None))
    assert service == "native"
