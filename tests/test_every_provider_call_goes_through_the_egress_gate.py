"""Every client of a cloud provider's own API is made behind the one egress
gate (`cloudapi.gate`, the `provider` kind; backlog R6.cloud, C1.managed):
the walk reads every module that imports a provider's library and finds,
in each function that makes a client, the gate asked before it; with
`MIGKIT_EGRESS=none` the call is refused with the setting that said no
and no client is made."""
import ast
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from migkit import cloudapi, egress

HOME = Path(cloudapi.__file__).resolve().parent
SDK = ("boto3", "botocore", "google", "googleapiclient", "azure")
#: what makes a client, a session or a credential of a provider's library
MAKER = re.compile(r"^(client|resource|Session|default|build)$|Client$"
                   r"|Credential$|Credentials$|^from_service_account")


def _sdk_names(tree):
    names = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            names |= {(a.asname or a.name).split(".")[0] for a in n.names
                      if a.name.split(".")[0] in SDK}
        elif isinstance(n, ast.ImportFrom) and n.module \
                and n.module.split(".")[0] in SDK:
            names |= {a.asname or a.name for a in n.names}
    return names


def _chain(f):
    parts = []
    while isinstance(f, ast.Attribute):
        parts.append(f.attr)
        f = f.value
    return (parts + [f.id])[::-1] if isinstance(f, ast.Name) else None


def _walk():
    """{module: [(function, line, maker, gated)]}"""
    out = {}
    for path in sorted(HOME.rglob("*.py")):
        tree = ast.parse(path.read_text())
        names = _sdk_names(tree)
        if not names:
            continue
        sites = out.setdefault(str(path.relative_to(HOME)), [])
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            makes, gates = [], []
            for n in ast.walk(fn):
                c = _chain(n.func) if isinstance(n, ast.Call) else None
                if not c:
                    continue
                if c[-1] == "gate":
                    gates.append(n.lineno)
                elif c[0] in names and MAKER.search(c[-1]):
                    makes.append((n.lineno, ".".join(c)))
            if makes:
                first = min(makes)
                sites.append((fn.name, first[0], first[1],
                              bool(gates) and min(gates) < first[0]))
    return out


def test_no_module_makes_a_provider_client_before_the_gate():
    got = _walk()
    # the walk sees the modules the ticket names, and every site in them
    for m in ("cloudapi.py", "cloudseed.py", "engines/dynamodb.py",
              "engines/timestream.py", "engines/spanner.py",
              "engines/cosmosdb.py"):
        assert got.get(m), m
    bypass = [(m, f, line, what) for m, sites in got.items()
              for f, line, what, gated in sites if not gated]
    assert not bypass, bypass
    assert sum(len(s) for s in got.values()) >= 40


def test_a_refused_gate_stops_the_call_before_any_client(monkeypatch):
    import boto3
    made = []
    monkeypatch.setattr(boto3, "client", lambda *a, **k: made.append(a))
    monkeypatch.setenv("MIGKIT_EGRESS", "none")
    egress._SAID.clear()
    from migkit.engines.dynamodb import DynamoDBEngine
    from migkit.engines.timestream import TimestreamEngine
    from migkit import config
    side = SimpleNamespace(options={}, host="10.0.0.1", port=1)
    hop = SimpleNamespace(name="hx", options={}, source=side, target=side)
    for call in (lambda: DynamoDBEngine._client(SimpleNamespace(hop=hop),
                                                "src"),
                 lambda: TimestreamEngine._client(
                     SimpleNamespace(hop=hop, _held={}), "src", "query"),
                 lambda: config._aws_client("s3", "us-east-1", None)):
        with pytest.raises(SystemExit) as e:
            call()
        assert "MIGKIT_EGRESS" in str(e.value)
        assert "let them out" in str(e.value)
    assert made == []
    # the hop's own setting is the gate's too, and lets it through
    monkeypatch.delenv("MIGKIT_EGRESS")
    cloudapi.gate(hop)
    hop.options = {"egress": "notify"}
    with pytest.raises(SystemExit, match="the hop's egress"):
        cloudapi.gate(SimpleNamespace(hop=hop))
