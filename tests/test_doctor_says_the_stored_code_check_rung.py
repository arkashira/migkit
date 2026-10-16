"""`doctor` says the rung the stored-code proof stands on for PostgreSQL
(backlog DEC.deps): installed -> used; not built -> the plan says what it
would add; no docker -> said, never a silent skip."""
import subprocess

from migkit.storedcode import sandbox

PROVES = ()


def _ran(out, code=0):
    def run(cmd, **kw):
        assert cmd[:2] == ["docker", "images"]
        return subprocess.CompletedProcess(cmd, code, out, "")
    return run


def test_an_installed_check_is_named_as_used():
    tag = sandbox.pg_tag(16)
    said = sandbox.static_check_said(
        _ran(tag.split(":")[1] + "\n"), have_docker=True)
    assert "installed" in said and "16" in said and "gates" in said


def test_a_check_not_built_says_what_the_first_proof_adds():
    said = sandbox.static_check_said(_ran(""), have_docker=True)
    assert "not built yet" in said and "adds" in said or "adding" in said


def test_an_old_recipe_image_does_not_count_as_installed():
    said = sandbox.static_check_said(_ran("16-r0\n"), have_docker=True)
    assert "not built yet" in said


def test_no_docker_is_said_with_what_would_be_added():
    said = sandbox.static_check_said(have_docker=False)
    assert "docker is not on this machine" in said and "would add" in said
    assert "not gated" in said


def test_docker_that_cannot_be_asked_is_said_not_assumed():
    def run(cmd, **kw):
        raise OSError("gone")
    said = sandbox.static_check_said(run, have_docker=True)
    assert "could not be asked" in said


def test_the_line_names_no_wrapped_program():
    for said in (sandbox.static_check_said(have_docker=False),
                 sandbox.static_check_said(_ran(""), have_docker=True)):
        assert "plpgsql" not in said.lower()
