"""Every environment variable the code reads is in the one registry (audit
2026-10-01, K13).

`migkit.envkeys` is what a variable is read through and what start-up
checks the environment against, so a variable the code reads and the
registry lacks would be called unknown - and a variable the registry
holds that nothing reads would be offered as a "did you mean" and do
nothing. This reads every string in the code - docstrings too: a
variable the manual's text names is one an operator will set - and fails
on a `MIGKIT_*` name the registry does not declare. Names built in
pieces are listed in `DYNAMIC`, with what they stand for.
"""
import ast
import pathlib
import re

import pytest

from migkit import envkeys

ROOT = pathlib.Path(__file__).resolve().parents[1]
NAME = re.compile(r"MIGKIT_[A-Z0-9_]*")

#: strings that hold `MIGKIT_` and are not a variable: a type the driver
#: registers, a C header's guard, and names of Python objects the
#: docstrings mention
NOT_VARIABLES = {"MIGKIT_FLOAT4", "MIGKIT_FLOAT4_ARRAY",
                 "MIGKIT_SQLINES_LICENSE_H", "MIGKIT_OWN", "MIGKIT_MARKS"}

#: a name built from pieces, where it is built, and a name it stands for
#: that must be declared
DYNAMIC = {
    # `MIGKIT_<HOP>_<SIDE>_PASSWORD`, `MIGKIT_<SIDE>_PASSWORD`
    ("guide.py", "MIGKIT_"): "MIGKIT_APP_SOURCE_PASSWORD",
    # the copy in a process of its own: `MIGKIT_<SIDE>_PASSWORD`
    ("movers.py", "MIGKIT_"): "MIGKIT_SOURCE_PASSWORD",
}


def names_in(src, where="x.py"):
    """[(name, line)] of every `MIGKIT_*` in a module's strings."""
    out = []
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            for m in NAME.finditer(node.value):
                out.append((m.group(0), node.lineno))
            # a setting of migkit's own without the prefix, named whole
            if not NAME.fullmatch(node.value) and node.value in envkeys.VARS:
                out.append((node.value, node.lineno))
    return out


def undeclared(src, where="x.py"):
    """The names in `src` the registry does not declare, with their line."""
    bad = []
    for name, line in names_in(src, where):
        if name in NOT_VARIABLES:
            continue
        if name.endswith("_") or name == "MIGKIT_":
            stands = DYNAMIC.get((where, name))
            if stands is None or envkeys.declared(stands) is None:
                bad.append((name, line))
            continue
        if envkeys.declared(name) is None:
            bad.append((name, line))
    return bad


def _sources(but=("envkeys.py",)):
    """Every module that reads a variable - the registry itself declares
    them, and names the prefix, and reads none by a name of its own."""
    for path in sorted((ROOT / "migkit").rglob("*.py")):
        if path.name not in but:
            yield path.name, path.read_text()
    for path in sorted((ROOT / "tools").glob("*.py")):
        yield path.name, path.read_text()


def test_every_name_the_code_reads_is_declared():
    bad = [(where, name, line) for where, src in _sources()
           for name, line in undeclared(src, where)]
    assert not bad, "\n".join(f"{w}:{ln} {n} is read but not declared in"
                              " migkit/envkeys.py" for w, n, ln in bad)


def test_the_scan_would_notice_one_that_is_not():
    """A scan that finds nothing would pass for the wrong reason."""
    planted = 'x = os.environ.get("MIGKIT_WORKRS", "")\n'
    assert undeclared(planted) == [("MIGKIT_WORKRS", 1)]
    built = 'x = os.environ.get("MIGKIT_" + side + "_TOKEN")\n'
    assert [n for n, _ in undeclared(built, "new.py")] == ["MIGKIT_"]
    assert undeclared('"MIGKIT_REPORTS"') == []
    found = [n for _, src in _sources() for n, _ in names_in(src)]
    assert len(set(found)) > 50, sorted(set(found))


def test_every_declared_name_is_read_somewhere():
    """A declaration nothing reads would be accepted and do nothing."""
    code = {n for _, src in _sources() for n, _ in names_in(src)}
    tests = set()
    for path in (ROOT / "tests").glob("*.py"):
        if path.name == pathlib.Path(__file__).name:
            continue
        tests |= set(NAME.findall(path.read_text()))
    dead = [n for n, v in envkeys.VARS.items()
            if n not in (tests if v.tests else code)]
    assert not dead, dead


def test_on_and_off_are_said_one_way():
    env = {}
    for word in ("1", "true", "YES", "on"):
        env["MIGKIT_EXPLAIN"] = word
        assert envkeys.flag("MIGKIT_EXPLAIN", env) is True, word
    for word in ("0", "false", "No", "off"):
        env["MIGKIT_EXPLAIN"] = word
        assert envkeys.flag("MIGKIT_EXPLAIN", env) is False, word
    # unset or empty is the declared default, which differs by variable
    assert envkeys.flag("MIGKIT_EXPLAIN", {}) is False
    assert envkeys.flag("MIGKIT_FAIL_ON_DIFFERENCE", {}) is True
    assert envkeys.flag("MIGKIT_FAIL_ON_DIFFERENCE",
                        {"MIGKIT_FAIL_ON_DIFFERENCE": ""}) is True
    # anything else is an error that names the variable - `maybe` used to
    # turn some of these on and others off
    with pytest.raises(envkeys.Bad) as e:
        envkeys.flag("MIGKIT_SCHEDULED", {"MIGKIT_SCHEDULED": "maybe"})
    assert "MIGKIT_SCHEDULED=maybe" in str(e.value), e.value


def test_a_number_that_is_not_one_is_an_error_naming_it():
    assert envkeys.number("MIGKIT_STALL_SECONDS", {}) == 300
    assert envkeys.number("MIGKIT_STALL_SECONDS",
                          {"MIGKIT_STALL_SECONDS": "45.5"}) == 45.5
    # below the least it may be, raised to it
    assert envkeys.number("MIGKIT_STALL_SECONDS",
                          {"MIGKIT_STALL_SECONDS": "0"}) == 1
    assert envkeys.number("MIGKIT_READ_TIMEOUT",
                          {"MIGKIT_READ_TIMEOUT": "90"}) == 90
    with pytest.raises(envkeys.Bad) as e:
        envkeys.number("MIGKIT_READ_TIMEOUT", {"MIGKIT_READ_TIMEOUT": "1.5"})
    assert "MIGKIT_READ_TIMEOUT=1.5 is not a whole number" in str(e.value)
    with pytest.raises(envkeys.Bad) as e:
        envkeys.number("MIGKIT_LEASE_SECONDS",
                       {"MIGKIT_LEASE_SECONDS": "a minute"})
    assert "MIGKIT_LEASE_SECONDS=a minute is not a number" in str(e.value)
    # a default the caller works out stands where nothing is set
    assert envkeys.number("MIGKIT_CLOUD_POLL", {}, default=7.0) == 7.0


def test_a_name_that_is_not_declared_cannot_be_read():
    with pytest.raises(KeyError):
        envkeys.flag("MIGKIT_NOT_A_THING", {})


def test_a_typo_is_named_with_the_closest_name():
    env = {"MIGKIT_STAL_SECONDS": "5", "MIGKIT_REPORTS": "/x",
           "MIGKIT_SHOP_SOURCE_PASSWORD": "CHANGE_ME", "PATH": "/bin",
           "MIGKIT_ZZZ": "1"}
    assert envkeys.unknown(env) == [("MIGKIT_STAL_SECONDS",
                                     "MIGKIT_STALL_SECONDS"),
                                    ("MIGKIT_ZZZ", "")]
    said = envkeys.said_unknown(env)
    assert said[0] == ("MIGKIT_STAL_SECONDS is not a variable migkit reads"
                       " - did you mean MIGKIT_STALL_SECONDS?"), said


def test_a_name_that_is_a_programs_is_never_offered_or_printed():
    near = envkeys.unknown({"MIGKIT_PGCOPYDB_NETWRK": "host"})
    assert near == [("MIGKIT_PGCOPYDB_NETWRK", "")], near
    table = envkeys.reference()
    assert "PGCOPYDB" not in table
    assert "| `MIGKIT_STALL_SECONDS` | number | 300 |" in table, table
    assert "| `MIGKIT_EXPLAIN` | flag | off |" in table, table
    # what migkit sets for itself and what only the tests read are not
    # the operator's to set
    assert "MIGKIT_SPEC" not in table and "MIGKIT_FAILPOINT" not in table


def test_secret_values_are_what_the_redactor_is_given():
    env = {"MIGKIT_AI_KEY": "sk-CHANGE_ME", "MIGKIT_AI_MODEL": "m",
           "MIGKIT_APP_TARGET_PASSWORD": "pw-CHANGE_ME", "OTHER": "x"}
    assert sorted(envkeys.secrets(env)) == ["pw-CHANGE_ME", "sk-CHANGE_ME"]


# ---- read through the registry (AUD-Z) ----------------------------------------

#: the ways a module reads a variable raw
READS = ("os.environ.get", "os.getenv", "environ.get", "os.environ.setdefault",
         "os.environ.pop")

#: other programs' variables migkit reads to find them or what they were
#: set to - never a setting of migkit's own, which is declared
FOREIGN = {
    'ALIBABA_CLOUD_CLI_PROFILE_DISABLED', 'ALIBABA_CLOUD_PROFILE',
    'AWS_PROFILE', 'AZURE_CONFIG_DIR', 'AZURE_SUBSCRIPTION_ID',
    'COMSPEC', 'GOOGLE_APPLICATION_CREDENTIALS',
    'GOOGLE_CLOUD_PROJECT', 'HUAWEICLOUD_SDK_CREDENTIALS_FILE',
    'IDENTITY_ENDPOINT', 'IDENTITY_HEADER', 'JAVA_HOME',
    'OCI_CLI_CONFIG_FILE', 'OCI_CLI_PROFILE',
    'OTEL_EXPORTER_OTLP_ENDPOINT',
    'OTEL_EXPORTER_OTLP_TRACES_ENDPOINT', 'OTEL_SDK_DISABLED',
    'OTEL_SERVICE_NAME', 'REMOTE_ADDR', 'TCCLI_PROFILE',
    'TENCENTCLOUD_REGION', 'TENCENTCLOUD_ROLE_ARN',
    'TENCENTCLOUD_ROLE_SESSION_NAME', 'TENCENTCLOUD_SECRET_ID',
    'TENCENTCLOUD_SECRET_KEY', 'TENCENTCLOUD_SESSION_TOKEN',
    'TENCENTCLOUD_TOKEN', 'VAULT_ADDR', 'VAULT_CACERT',
    'VAULT_NAMESPACE', 'VAULT_TOKEN', 'XDG_CACHE_HOME',
    'XDG_CONFIG_HOME', 'XDG_DATA_HOME', 'XDG_STATE_HOME',
}

#: declared variables still read raw beside the registry, by module: row
#: 559 AUD-Z.env-raw moves each onto `envkeys`; one moved leaves this list,
#: and no read may join it
OWED_RAW = {
    ('approvals.py', 'MIGKIT_ACTOR'),
    ('approvals.py', 'MIGKIT_IDENTITY'),
    ('assist.py', 'MIGKIT_AI'),
    ('assist.py', 'MIGKIT_AI_KEY'),
    ('assist.py', 'MIGKIT_AI_MODEL'),
    ('assist.py', 'MIGKIT_AI_SHARE'),
    ('assist.py', 'MIGKIT_AI_URL'),
    ('audit.py', 'MIGKIT_ACTOR'),
    ('bulkways.py', 'MIGKIT_MOVER'),
    ('cli.py', 'MIGKIT_DIAGNOSE'),
    ('cli.py', 'MIGKIT_SOURCE'),
    ('cloudapi.py', 'MIGKIT_CLOUD_POLL'),
    ('cloudapi.py', 'MIGKIT_CLOUD_WAIT'),
    ('config.py', 'MIGKIT_SOURCE'),
    ('config.py', 'MIGKIT_TARGET'),
    ('costmodel.py', 'MIGKIT_KNOWLEDGE'),
    ('crossmove.py', 'MIGKIT_TABLE_COPY'),
    ('cutover.py', 'MIGKIT_CUTOVER'),
    ('decide.py', 'MIGKIT_EXPLAIN'),
    ('engines/base.py', 'MIGKIT_REPAIR_TEXT'),
    ('engines/hetero.py', 'MIGKIT_TAIL_APPLY_ALL'),
    ('engines/mysql.py', 'MIGKIT_READ_TIMEOUT'),
    ('engines/oracle.py', 'MIGKIT_ORACLE_WRITE'),
    ('engines/oracle_redo.py', 'MIGKIT_ORACLE_READER'),
    ('estate.py', 'MIGKIT_SOURCE_PASSWORD'),
    ('evidence.py', 'MIGKIT_IDENTITY'),
    ('export.py', 'MIGKIT_EVIDENCE_KEY'),
    ('failpoint.py', 'MIGKIT_FAILPOINT'),
    ('lease.py', 'MIGKIT_LEASE_SECONDS'),
    ('leaves.py', 'MIGKIT_GENERATIONS'),
    ('machine.py', 'MIGKIT_CATALOG_REFRESH'),
    ('machine.py', 'MIGKIT_INSTANCES'),
    ('model.py', 'MIGKIT_PROBE_FROM'),
    ('model.py', 'MIGKIT_PROBE_SECONDS'),
    ('movers.py', 'MIGKIT_CDC'),
    ('movers.py', 'MIGKIT_FOLLOW_TIMEOUT'),
    ('movers.py', 'MIGKIT_MOVER'),
    ('notify.py', 'MIGKIT_NOTIFY'),
    ('physical.py', 'MIGKIT_SEED_START_WAIT'),
    ('ready.py', 'MIGKIT_PREFLIGHT'),
    ('second_reader.py', 'MIGKIT_SECOND_READER_PYTHON'),
    ('servicecheck.py', 'MIGKIT_COMPARE_WITH'),
    ('session.py', 'MIGKIT_RUN_ID'),
    ('shared.py', 'MIGKIT_STATE_KEY'),
    ('state.py', 'MIGKIT_STATE_KEY'),
    ('tools.py', 'MIGKIT_ACCEPT_TERMS'),
    ('tools.py', 'MIGKIT_DB2_CONNECT_LICENSE'),
    ('tools.py', 'MIGKIT_TOOLS'),
    ('util.py', 'MIGKIT_TOOLS'),
}


def raw_reads(src):
    """[(name, line)] of every variable a module reads by a literal name
    without the registry."""
    out = []
    for node in ast.walk(ast.parse(src)):
        name = None
        if isinstance(node, ast.Call) and ast.unparse(node.func) in READS \
                and node.args and isinstance(node.args[0], ast.Constant):
            name = node.args[0].value
        elif isinstance(node, ast.Subscript) and isinstance(
                node.ctx, ast.Load) and ast.unparse(node.value) in (
                "os.environ", "environ") and isinstance(
                node.slice, ast.Constant):
            name = node.slice.value
        if isinstance(name, str):
            out.append((name, node.lineno))
    return out


def _raw_everywhere():
    for path in sorted((ROOT / "migkit").rglob("*.py")):
        if path.name != "envkeys.py":
            rel = str(path.relative_to(ROOT / "migkit"))
            for name, line in raw_reads(path.read_text()):
                yield rel, name, line


def test_a_setting_of_migkits_own_is_declared_whatever_its_name():
    """A knob without the `MIGKIT_` prefix (`SEQUENCE_BUFFER`,
    `MONGO_IGNORE_ROLES`) slipped past the name scan: every variable read
    is declared or another program's."""
    bad = [f"{w}:{ln} {n}" for w, n, ln in _raw_everywhere()
           if envkeys.declared(n) is None and n not in FOREIGN]
    assert not bad, "declare it in migkit/envkeys.py:\n" + "\n".join(bad)


def test_a_declared_variable_is_read_through_the_registry():
    got = {(w, n) for w, n, _ in _raw_everywhere()
           if envkeys.declared(n) is not None}
    new = sorted(got - OWED_RAW)
    assert not new, "read it through migkit.envkeys: %s" % new
    gone = sorted(OWED_RAW - got)
    assert not gone, "read through envkeys now - take it off OWED_RAW: %s" \
        % gone


def test_the_raw_scan_would_notice_one():
    planted = ('a = os.environ.get("SEQUENCE_BUFFER", "0")\n'
               'b = os.environ["GRANTS_IGNORE_ROLES"]\n'
               'c = envkeys.items("GRANTS_IGNORE_ROLES")\n')
    assert raw_reads(planted) == [("SEQUENCE_BUFFER", 1),
                                  ("GRANTS_IGNORE_ROLES", 2)]
    assert envkeys.declared("SEQUENCE_BUFFER") is None
    assert "SEQUENCE_BUFFER" not in FOREIGN
    assert ("engines/postgres.py", "GRANTS_IGNORE_ROLES") not in OWED_RAW
