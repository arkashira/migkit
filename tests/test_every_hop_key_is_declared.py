"""Every key the code reads is in the one registry (backlog R21).

`migkit.hopkeys` is what the configuration is checked against before
anything runs, so a key the code reads and the registry lacks would be
refused as unknown - and a key the registry holds that nothing reads
would be accepted and do nothing. This reads the code the way it reads
a hop: every `options.get("x")`, `options["x"]` and `"x" in options` on
a hop's options or a side's, through the local names they are held in,
and fails on any the registry does not declare - or declares for other
engines than the file that reads it.
"""
import ast
import pathlib

import pytest

from migkit import hopkeys

ROOT = pathlib.Path(__file__).resolve().parents[1] / "migkit"
HOPISH = ("hop", "self.hop", "h", "back")

#: the engine each engine file is, for the keys it reads
FILE_ENGINE = {"postgres.py": ("postgres",), "mysql.py": ("mysql",),
               "mongodb.py": ("mongodb",), "mssql.py": ("mssql",),
               "redis.py": ("redis",), "kafka.py": ("kafka",),
               "generic.py": ("generic",), "sqlite.py": ("sqlite",),
               "parquet.py": ("parquet",), "clickhouse.py": ("clickhouse",),
               "dynamodb.py": ("dynamodb",), "oracle.py": ("oracle",),
               "oracle_logminer.py": ("oracle",),
               "oracle_redo.py": ("oracle",), "oracle_pump.py": ("oracle",),
               "db2.py": ("db2",), "opensearch.py": ("opensearch",),
               "cassandra.py": ("cassandra",), "kinesis.py": ("kinesis",),
               "pubsub.py": ("pubsub",), "ase.py": ("ase",),
               "duckdb.py": ("duckdb",), "hetero.py": ("hetero",),
               "copybook.py": ("copybook",), "db2i.py": ("db2i",),
               "db2zos.py": ("db2zos",),
               "warehouse.py": ("redshift", "snowflake", "bigquery")}

#: reads by a key held in a variable, where it comes from, and the keys
#: it stands for - a new one is a read this test cannot follow, and fails
#: until it is added here with its keys declared
DYNAMIC = {
    ("config.py", "libpq_tls"): ("endpoint", ("sslmode", "sslrootcert",
                                              "sslcert", "sslkey",
                                              "sslcrl")),
    ("config.py", "mysql_tls"): ("endpoint", ("ssl_cert", "ssl_key")),
    ("warehouse.py", "_connect"): ("endpoint", ("warehouse", "role")),
    ("config.py", "redis_tls"): ("endpoint", ("tls_cert_file",
                                              "tls_key_file")),
    ("tls.py", "_direct"): ("endpoint", ("tls_ca_file", "tls",
                                         "tls_insecure")),
    ("tls.py", "one"): ("endpoint", ("tls_ca_file", "tls",
                                     "tls_insecure")),
    ("hopkeys.py", "said_by_doctor"): ("hop", ("source_engine",
                                               "target_engine")),
}


def _text(src, node):
    return ast.get_source_segment(src, node) or ""


def _resolve(src, node, alias, depth=0):
    """(kind, path) of an expression that is a hop's or a side's options,
    or a mapping under them; None for anything else."""
    if depth > 8 or node is None:
        return None
    if isinstance(node, ast.Attribute) and node.attr == "options":
        base = _text(src, node.value)
        return ("hop" if base in HOPISH or base.endswith(".hop")
                else "endpoint"), ()
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
            and node.func.id == "getattr" and len(node.args) >= 2 \
            and isinstance(node.args[1], ast.Constant) \
            and node.args[1].value == "options":
        base = _text(src, node.args[0])
        return ("hop" if base in HOPISH or base.endswith(".hop")
                else "endpoint"), ()
    if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
        return _resolve(src, node.values[0], alias, depth + 1)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
            and node.func.id == "dict" and node.args:
        return _resolve(src, node.args[0], alias, depth + 1)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
            and node.func.attr == "get" and node.args \
            and isinstance(node.args[0], ast.Constant) \
            and isinstance(node.args[0].value, str):
        got = _resolve(src, node.func.value, alias, depth + 1)
        if got:
            return got[0], got[1] + (node.args[0].value,)
    if isinstance(node, ast.Subscript) and isinstance(node.slice,
                                                      ast.Constant) \
            and isinstance(node.slice.value, str):
        got = _resolve(src, node.value, alias, depth + 1)
        if got:
            return got[0], got[1] + (node.slice.value,)
    if isinstance(node, ast.Name) and node.id in alias:
        rest = {k: v for k, v in alias.items() if k != node.id}
        return _resolve(src, alias[node.id], rest, depth + 1)
    return None


def reads(src, name="x.py"):
    """([(kind, path, where)], [(function, where)]) of one module's
    reads: by a constant key, and by a key held in a variable."""
    tree = ast.parse(src)
    found, dynamic = [], []
    for fn in [n for n in ast.walk(tree)
               if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]:
        alias = {}
        for n in ast.walk(fn):
            if isinstance(n, ast.Assign) and len(n.targets) == 1 \
                    and isinstance(n.targets[0], ast.Name):
                alias[n.targets[0].id] = n.value
        for n in ast.walk(fn):
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) \
                    and n.func.attr in ("get", "pop", "setdefault") \
                    and n.args:
                recv, key = n.func.value, n.args[0]
            elif isinstance(n, ast.Subscript):
                recv, key = n.value, n.slice
            elif isinstance(n, ast.Compare) and len(n.ops) == 1 \
                    and isinstance(n.ops[0], (ast.In, ast.NotIn)):
                recv, key = n.comparators[0], n.left
            else:
                continue
            got = _resolve(src, recv, alias)
            if not got:
                continue
            where = f"{name}:{n.lineno}"
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                found.append((got[0], got[1] + (key.value,), where))
            elif not got[1]:
                dynamic.append((fn.name, where))
    return found, dynamic


def _all():
    found, dynamic = [], []
    for path in sorted(ROOT.rglob("*.py")):
        f, d = reads(path.read_text(), path.name)
        found += f
        dynamic += [(path.name, fn, w) for fn, w in d]
    return found, dynamic


def test_the_scan_reaches_what_it_claims():
    """A scan that finds nothing would pass for the wrong reason."""
    found, _ = _all()
    keys = {(k, p[0]) for k, p, _ in found}
    assert len(keys) > 80, len(keys)
    for want in (("hop", "verify_batches"), ("hop", "two_way"),
                 ("endpoint", "uri_options"), ("endpoint", "ssl_ca"),
                 ("hop", "mask")):
        assert want in keys, want


def test_a_planted_read_is_caught():
    planted = ("def f(hop, ep):\n"
               "    o = hop.options or {}\n"
               "    a = o.get('undeclared_knob')\n"
               "    b = ep.options['undeclared_side_key']\n"
               "    c = (hop.options or {}).get('state', {}).get('bucket')\n"
               "    d = hop.options.get(name)\n")
    found, dynamic = reads(planted)
    assert ("hop", ("undeclared_knob",), "x.py:3") in found
    assert ("endpoint", ("undeclared_side_key",), "x.py:4") in found
    assert ("hop", ("state", "bucket"), "x.py:5") in found
    assert dynamic == [("f", "x.py:6")]
    assert _undeclared(found) == ["hop options.undeclared_knob (x.py:3)",
                                  "endpoint options.undeclared_side_key"
                                  " (x.py:4)"]


def _undeclared(found):
    out = []
    for kind, path, where in found:
        registry = (hopkeys.HOP_OPTIONS if kind == "hop" else
                    hopkeys.ENDPOINT_OPTIONS)
        key = registry.get(path[0])
        if key is None:
            out.append(f"{kind} options.{path[0]} ({where})")
        elif len(path) > 1 and key.subs is not None \
                and path[1] not in key.subs:
            out.append(f"{kind} options.{'.'.join(path)} ({where})")
    return out


def test_every_key_the_code_reads_is_declared():
    found, _ = _all()
    missing = _undeclared(found)
    assert not missing, "declare in migkit/hopkeys.py:\n" + "\n".join(
        missing)


def test_an_engine_file_reads_only_keys_declared_for_its_engine():
    found, _ = _all()
    wrong = []
    for kind, path, where in found:
        name = where.split(":")[0]
        mine = FILE_ENGINE.get(name)
        registry = (hopkeys.HOP_OPTIONS if kind == "hop" else
                    hopkeys.ENDPOINT_OPTIONS)
        key = registry.get(path[0])
        if mine is None or key is None or not key.engines:
            continue
        if name == "hetero.py" and kind == "hop":
            continue
        if not set(mine) & set(key.engines):
            wrong.append(f"{path[0]} read in {where}, declared for"
                         f" {key.engines}")
    assert not wrong, "\n".join(wrong)


def test_every_read_by_a_held_key_is_known():
    _, dynamic = _all()
    sites = {(f, fn) for f, fn, _ in dynamic}
    assert sites == set(DYNAMIC), sites ^ set(DYNAMIC)
    for kind, keys in DYNAMIC.values():
        registry = (hopkeys.HOP_OPTIONS if kind == "hop" else
                    hopkeys.ENDPOINT_OPTIONS)
        for k in keys:
            assert k in registry, k


def test_the_keys_a_hop_is_built_from_are_declared():
    """`config.build_hop` reads the hop's own keys from its mapping, and
    `_endpoint` a side's: every one of them in the registry."""
    src = (ROOT / "config.py").read_text()
    tree = ast.parse(src)
    fns = {n.name: n for n in ast.walk(tree)
           if isinstance(n, ast.FunctionDef)}
    for fn, registry in (("build_hop", hopkeys.HOP_KEYS),
                         ("_endpoint", {**hopkeys.ENDPOINT_KEYS,
                                        **hopkeys.ENDPOINT_OPTIONS})):
        got = {n.args[0].value for n in ast.walk(fns[fn])
               if isinstance(n, ast.Call)
               and isinstance(n.func, ast.Attribute)
               and n.func.attr == "get" and n.args
               and isinstance(n.args[0], ast.Constant)
               and isinstance(n.args[0].value, str)
               and _text(src, n.func.value) in ("raw", "extra")}
        assert got, fn
        assert got <= set(registry), (fn, got - set(registry))


def test_nothing_declared_is_read_by_nothing():
    """The other way: a key the registry declares must be read somewhere,
    or it is accepted and does nothing - `big_rows` is the one that is
    declared for exactly that, so `doctor` can say so."""
    found, _ = _all()
    read = {(k, p[0]) for k, p, _ in found}
    for kind, keys in DYNAMIC.values():
        read |= {(kind, k) for k in keys}
    unread = [k for k in hopkeys.HOP_OPTIONS if ("hop", k) not in read]
    unread += [k for k in hopkeys.ENDPOINT_OPTIONS
               if ("endpoint", k) not in read and k != "auth"]
    assert not unread, unread


# ---- the checking itself -------------------------------------------------

def _one(raw):
    return [p for p in hopkeys.check({"hops": {"h": raw}})]


PG = {"source": "postgres://app@db1/shop", "target": "postgres://app@db2/shop"}


def test_an_unknown_key_is_refused_with_the_closest_one():
    got = _one({**PG, "workrs": 2, "options": {"verfy_batches": False}})
    said = {p.where: p.text for p in got if p.level == "error"}
    assert "did you mean workers?" in said["hops.h.workrs"], said
    assert "did you mean verify_batches?" in said[
        "hops.h.options.verfy_batches"], said


def test_a_key_named_for_a_program_is_not_offered_back():
    got = _one({**PG, "options": {"atlass": False}})
    (p,) = [p for p in got if p.level == "error"]
    assert "not a key migkit knows" in p.text
    assert "atlas" not in p.text.replace("atlass", ""), p.text


def test_a_known_key_raises_nothing():
    """The check must be able to say "fine" as well as "wrong"."""
    got = _one({**PG, "workers": 3, "options": {"verify_batches": False,
                                                "mask": ["users.email"]}})
    assert not [p for p in got if p.level == "error"], got


def test_wrong_types_are_refused():
    got = _one({**PG, "workers": "four", "databases": "shop",
                "options": {"verify_batches": "yes please",
                            "repair_window": 30}})
    bad = {p.where for p in got if p.level == "error"}
    assert bad == {"hops.h.workers", "hops.h.databases",
                   "hops.h.options.verify_batches"}, got


def test_a_filter_on_an_excluded_table_is_a_contradiction():
    got = _one({**PG, "exclude": ["public.audit*"],
                "mapping": {"where": {"public.audit_log": "id > 5",
                                      "public.orders": "id > 5"}}})
    bad = [p for p in got if p.level == "error"]
    assert [p.where for p in bad] == [
        "hops.h.mapping.where.public.audit_log"], bad
    assert "also excluded" in bad[0].text


def test_a_goal_that_writes_nothing_beside_an_option_that_writes():
    got = _one({**PG, "goal": "verify",
                "options": {"reverse": "at_cutover"}})
    assert [p.where for p in got if p.level == "error"] == [
        "hops.h.options.reverse"], got
    got = _one({**PG, "goal": "copy", "options": {"topology": "two_way"}})
    assert [p.where for p in got if p.level == "error"] == [
        "hops.h.options.topology"], got
    assert not [p for p in _one({**PG, "goal": "cutover",
                                 "options": {"reverse": "at_cutover"}})
                if p.level == "error"]


def test_an_unknown_goal_and_engine():
    got = _one({**PG, "goal": "cutovr"})
    assert "did you mean cutover?" in got[0].text, got
    got = _one({"engine": "postgress", "source": {"host": "a"},
                "target": {"host": "b"}})
    assert "did you mean postgres?" in got[0].text, got


def test_a_plain_password_is_said_and_a_reference_is_not():
    got = _one({"source": {"host": "a", "user": "u", "password": "hunter2"},
                "target": {"host": "b", "user": "u",
                           "password": "env:DST_PW"}})
    warn = [p for p in got if p.level == "warn"]
    assert [p.where for p in warn] == ["hops.h.source.password"], got
    assert "keychain" in warn[0].text


def test_an_engine_and_an_address_that_disagree():
    got = _one({"engine": "mysql", **PG})
    assert got and got[0].level == "error"
    assert "leave engine out" in got[0].text


def test_load_refuses_before_anything_runs(tmp_path):
    from migkit.config import load_hops
    conf = tmp_path / "hops.yaml"
    conf.write_text("hops:\n  h:\n    source: postgres://a@b/c\n"
                    "    target: postgres://a@d/c\n    workrs: 2\n"
                    "    options: {mask: [x], protect_targt: true}\n")
    with pytest.raises(SystemExit) as e:
        load_hops(conf)
    said = str(e.value)
    assert "2 problems" in said and "workrs" in said \
        and "protect_targt" in said and "nothing was run" in said, said


def test_every_existing_long_form_still_loads(tmp_path):
    """The long form, as the example and the tests write it, loads as it
    did - the check refuses what nothing reads, not what hops say."""
    from migkit.config import load_hops
    conf = tmp_path / "hops.yaml"
    conf.write_text("""
hops:
  old:
    engine: postgres
    service: native
    source: {host: s, port: 5432, user: u, password: env:HOME}
    target: {host: t, port: 5432, user: u, password: p,
             options: {sslmode: verify-full, sslrootcert: /ca.pem}}
    databases: []
    db_map: {}
    workers: 4
    big_rows: 5000000
    slice: 1000000
    options: {two_way: {on_conflict: error}, state: {backend: local},
              schedule: {cron: "0 * * * *", run: check}}
""")
    hop = load_hops(conf)["old"]
    assert hop.workers == 4 and hop.workers_set
    assert hop.target.options["sslmode"] == "verify-full"


# ---- what doctor says ----------------------------------------------------

def test_doctor_says_ceilings_and_keys_that_do_nothing():
    from migkit.config import build_hop
    raw = {**PG, "workers": 6, "big_rows": 10, "slice": 5000,
           "options": {"server_id": 7, "atlas": False}}
    said = dict(hopkeys.said_by_doctor(build_hop("h", raw), raw))
    assert "ceiling" in said["workers"] and "past 6" in said["workers"]
    assert "read by nothing" in said["big_rows"]
    assert "otherwise decides" in said["slice"]
    assert "does nothing on this postgres hop" in said["options.server_id"]
    assert "turns off one of the ways" in said["options.atlas"]
    # and a hop that sets none of them hears nothing
    assert hopkeys.said_by_doctor(build_hop("h", PG), PG) == []


def test_doctor_says_a_side_key_its_engine_does_not_read():
    from migkit.config import build_hop
    raw = {"source": {"host": "a", "ssl_ca": "/ca.pem"},
           "target": {"host": "b", "sslmode": "require"}}
    said = dict(hopkeys.said_by_doctor(build_hop("h", raw), raw))
    assert "does nothing on this postgres one" in said["source.ssl_ca"]
    assert "target.sslmode" not in said
