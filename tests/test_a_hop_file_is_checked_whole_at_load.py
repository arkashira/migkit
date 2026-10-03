"""A hop file is held to the registry whole before anything runs (audit
2026-10-01, K1, K2, K13, K15, K20, ST7, SEC10, DU6, DU16, DU26, K17).

Measured before: a hop with `approvals: {step: [cutover]}`, `mapping:
{colums: {public.users: {drop: [ssn]}}}`, `notify: {too: ...}`, `ddl:
parkk` and `cutover: {freeze: servr}` loaded with zero errors - approvals
off, the SSN column copied, nobody told - and two `options:` blocks
loaded as the second. Each check here is shown to say "wrong" and to say
"fine".
"""
import os
import stat

import pytest
from click.testing import CliRunner

from migkit import addresses, config, hopkeys
from migkit.config import Endpoint, Hop, build_hop

PG = {"source": "postgres://app@10.0.0.1/shop",
      "target": "postgres://app@10.0.0.2/shop"}


def _errors(raw):
    return {p.where: p.text for p in hopkeys.check({"hops": {"h": raw}})
            if p.level == "error"}


def _write(tmp_path, text, mode=0o600):
    conf = tmp_path / "hops.yaml"
    conf.write_text(text)
    os.chmod(conf, mode)
    return conf


# ---- K1: every key under an option, every value, every owner's reader ------

def test_the_measured_typos_are_each_an_error_with_the_nearest_key():
    got = _errors({**PG,
                   "mapping": {"colums": {"public.users": {"drop": ["ssn"]}},
                               "columns": {"public.orders": {
                                   "dorp": ["x"], "renmae": {"a": "b"}}}},
                   "options": {"approvals": {"step": ["cutover"]},
                               "notify": {"too": "env:HOOK"},
                               "schedule": {"crn": "0 * * * *"}}})
    for where, near in (("hops.h.mapping.colums", "columns"),
                        ("hops.h.mapping.columns.public.orders.dorp",
                         "drop"),
                        ("hops.h.mapping.columns.public.orders.renmae",
                         "rename"),
                        ("hops.h.options.approvals.step", "steps"),
                        ("hops.h.options.notify.too", "to"),
                        ("hops.h.options.schedule.crn", "cron")):
        assert f"did you mean {near}?" in got.get(where, ""), (where, got)


def test_the_same_hop_spelled_right_loads_clean():
    got = _errors({**PG,
                   "mapping": {"columns": {"public.users": {
                       "drop": ["ssn"], "rename": {"a": "b"}}},
                       "where": {"public.orders": {"expr": "id > 5",
                                                   "tz": "+07:00"}},
                       "tolerance": {"public.orders": 0.5}},
                   "options": {"approvals": {"steps": ["cutover"],
                                             "count": 2, "expires": 600},
                               "notify": {"to": "env:HOOK",
                                          "events": ["verdict"], "lag": 30},
                               "schedule": {"cron": "0 * * * *",
                                            "timezone": "Asia/Bangkok"},
                               "state": {"backend": "s3", "bucket": "b"},
                               "cutover": {"budget": 20, "freeze": "server"},
                               "follow_until": "caught-up",
                               "limits": {"source_load": "30%"},
                               "deadline": "6h"}})
    assert got == {}, got


def test_a_value_is_held_to_its_words_and_its_owners_reader():
    got = _errors({**PG, "options": {
        "approvals": {"steps": ["Cutover"], "count": 0},
        "state": {"backend": "S3"},
        "schedule": {"cron": "every hour", "timezone": "Mars/Base"},
        "cutover": {"budget": "30s", "freeze": "servr"},
        "follow_until": "forever", "deadline": "soon",
        "limits": {"source_load": "lots"}, "checksum": "md5"}})
    assert "did you mean cutover?" in got["hops.h.options.approvals.steps"]
    assert "at least 1" in got["hops.h.options.approvals.count"]
    assert "one of local, s3" in got["hops.h.options.state.backend"]
    assert "five fields" in got["hops.h.options.schedule.cron"]
    assert "not a time zone" in got["hops.h.options.schedule.timezone"]
    assert "a number" in got["hops.h.options.cutover.budget"]
    assert "one of text, jsonb" in got["hops.h.options.checksum"]
    # read by the owning module's own reader, at load
    assert "forever" in got["hops.h.options.follow_until"]
    assert "soon" in got["hops.h.options.deadline"]
    # each wrong option said once, under its own name: the limits' reader
    # reads the deadline too, and is not given it
    assert "source_load" in got["hops.h.options.limits"]
    assert "deadline" not in got["hops.h.options.limits"]
    got = _errors({**PG, "options": {"cutover": {"freeze": "servr"}}})
    assert "freeze servr" in got["hops.h.options.cutover"], got


def test_a_secret_written_out_is_said_wherever_the_registry_has_one():
    raw = {"source": {"host": "a", "user": "u", "password": "env:PW",
                      "options": {"mine_password": "hunter2"}},
           "target": {"host": "b", "user": "u", "password": "env:PW"},
           "engine": "oracle",
           "options": {"cutover": {"pooler": {"kind": "pgbouncer",
                                              "password": "plain"}}}}
    warned = {p.where for p in hopkeys.check({"hops": {"h": raw}})
              if p.level == "warn" and p.text == hopkeys.PLAIN}
    assert warned == {"hops.h.source.mine_password",
                      "hops.h.options.cutover.pooler.password"}, warned


# ---- K2: a key written twice --------------------------------------------------

def test_a_key_written_twice_is_refused_and_a_merge_is_not(tmp_path):
    conf = _write(tmp_path, "hops:\n  h:\n"
                  "    source: postgres://app@10.0.0.1/shop\n"
                  "    target: postgres://app@10.0.0.2/shop\n"
                  "    options: {protect_target: true}\n"
                  "    options: {mask: [x]}\n")
    with pytest.raises(SystemExit) as e:
        config.load_hops(conf)
    said = str(e.value)
    assert "'options' is written twice" in said and "line 5" in said \
        and "line 6" in said and "nothing was run" in said, said
    conf = _write(tmp_path, "base: &b\n  protect_target: true\n"
                  "hops:\n  h:\n"
                  "    source: postgres://app@10.0.0.1/shop\n"
                  "    target: postgres://app@10.0.0.2/shop\n"
                  "    options:\n      <<: *b\n      protect_target: false\n")
    assert config.load_hops(conf)["h"].options["protect_target"] is False


# ---- K20: one spelling for every reader ---------------------------------------

def test_a_word_is_built_in_its_own_spelling():
    hop = build_hop("h", {**PG, "goal": "cutover", "options": {
        "topology": "two-way", "on_conflict": "keep_target",
        "reverse": "at-cutover"}})
    assert hop.options["topology"] == "two_way"
    assert hop.options["on_conflict"] == "keep-target"
    assert hop.options["reverse"] == "at_cutover"
    assert hopkeys.topology({"topology": "bidirectional"}) == "two_way"
    assert hopkeys.topology(hop) == "two_way"
    assert hopkeys.topology({}) == "one_way"
    assert hopkeys.reverse({}) == ""
    with pytest.raises(SystemExit, match="one_way or two_way"):
        hopkeys.topology({"topology": "sideways"})
    assert "sideways" in _errors({**PG, "options": {
        "topology": "sideways"}})["hops.h.options.topology"]
    side = build_hop("h", {"source": {"host": "a", "sslmode": "VERIFY-FULL"},
                           "target": {"host": "b"}})
    assert side.source.options["sslmode"] == "verify-full"


# ---- K15: one reader of a default ---------------------------------------------

def test_a_default_is_read_from_the_registry():
    hop = build_hop("h", PG)
    assert (hop.workers, hop.slice, hop.workers_set) == (4, 1_000_000, False)
    assert hopkeys.opt(hop, "fence_timeout") == 300
    assert hopkeys.opt({"fence_timeout": "45"}, "fence_timeout") == 45
    assert hopkeys.opt({}, "on_conflict") == "source-wins"
    assert hopkeys.opt({"settle": 1.5}, "settle") == 1.5
    # a fraction of a second was refused as not a whole number
    assert not _errors({**PG, "options": {"settle": 1.5}})
    assert "at least 1" in _errors({**PG, "workers": 0})["hops.h.workers"]


# ---- the file's own permissions -----------------------------------------------

def test_a_file_anyone_may_change_is_refused(tmp_path):
    text = ("hops:\n  h:\n    source: postgres://app@10.0.0.1/shop\n"
            "    target: postgres://app@10.0.0.2/shop\n")
    conf = _write(tmp_path, text, 0o666)
    with pytest.raises(SystemExit, match="may be changed by every user of"
                                         " this machine"):
        config.load_hops(conf)
    os.chmod(conf, 0o644)
    assert "h" in config.load_hops(conf)


def test_a_password_written_out_where_others_read_it_is_said(tmp_path):
    text = ("hops:\n  h:\n    engine: postgres\n"
            "    source: {host: a, user: u, password: hunter2}\n"
            "    target: {host: b, user: u, password: env:PW}\n")
    conf = _write(tmp_path, text, 0o644)
    data = config.read_yaml(conf.read_text())
    said = [p for p in hopkeys.check(data, conf) if p.where == str(conf)]
    assert [p.level for p in said] == ["warn"], said
    assert "readable by other users" in said[0].text
    os.chmod(conf, 0o600)
    assert not [p for p in hopkeys.check(data, conf)
                if p.where == str(conf)]


# ---- K13: the environment at start --------------------------------------------

def test_the_environment_is_read_once_at_start():
    from migkit import cli
    got = CliRunner().invoke(cli.main, ["doctor", "--help"],
                             env={"MIGKIT_EXPLAN": "1"})
    assert got.exit_code == 0, got.output
    assert "MIGKIT_EXPLAN is not a variable migkit reads - did you mean" \
           " MIGKIT_EXPLAIN?" in got.stderr, got.stderr
    got = CliRunner().invoke(cli.main, ["doctor", "--help"],
                             env={"MIGKIT_EXPLAIN": "maybe"})
    assert got.exit_code == 2, got.output
    assert "MIGKIT_EXPLAIN=maybe is neither on" in got.output, got.output
    got = CliRunner().invoke(cli.main, ["doctor", "--help"],
                             env={"MIGKIT_EXPLAIN": "off"})
    assert got.exit_code == 0, got.output
    assert "MIGKIT_EXPLAIN" not in got.stderr, got.stderr


# ---- ST7, SEC10: the state root and its permissions ---------------------------

def test_an_installed_copy_keeps_its_state_beside_its_config(tmp_path,
                                                              monkeypatch):
    monkeypatch.delenv("MIGKIT_REPORTS", raising=False)
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    monkeypatch.setattr(config, "BASE",
                        tmp_path / "lib" / "site-packages")
    monkeypatch.chdir(tmp_path)
    project = tmp_path / "proj" / "conf" / "hops.yaml"
    assert config._reports_root(project) == tmp_path / "proj" / "reports"
    user = tmp_path / ".config" / "migkit" / "hops.yaml"
    got = config._reports_root(user)
    assert got == tmp_path / "state" / "migkit" / "reports"
    # never the working directory it was started from
    assert config._reports_root(None) != tmp_path / "reports"
    monkeypatch.setattr(config, "BASE", tmp_path / "checkout")
    assert config._reports_root(user) == tmp_path / "checkout" / "reports"


def test_a_report_directory_is_only_its_users(tmp_path, monkeypatch):
    root = tmp_path / "reports"
    root.mkdir(mode=0o755)
    os.chmod(root, 0o755)
    monkeypatch.setattr(config, "REPORTS", root)
    hop = Hop(name="h", engine="postgres", source=Endpoint(),
              target=Endpoint())
    got = hop.report_dir("shop")
    for d in (root, root / "h", root / "h" / "shop"):
        assert stat.S_IMODE(d.stat().st_mode) == 0o700, d
    assert got.name == "shop"


def test_restore_points_are_mirrored_beside_the_state(tmp_path,
                                                       monkeypatch):
    from migkit import state
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "xdg"))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.delenv("MIGKIT_STATE_KEY", raising=False)
    monkeypatch.setattr(config, "REPORTS", tmp_path / "reports")
    hop = Hop(name="h", engine="postgres", source=Endpoint(),
              target=Endpoint())
    store = state.LocalStore(hop, {})
    assert store.mirror == (tmp_path / "xdg" / "migkit" / "restore-points"
                            / "h")
    undo = tmp_path / "undo"
    undo.mkdir()
    (undo / "undo.sql").write_text("update t set v = 'CHANGE_ME';\n")
    store._commit("shop", "20261001-000000", undo)
    tar = store.mirror / "shop-20261001-000000.tar.gz"
    # whole rows: only this user's, from the first byte
    assert stat.S_IMODE(tar.stat().st_mode) == 0o600
    assert stat.S_IMODE(store.mirror.stat().st_mode) == 0o700
    # one an earlier version mirrored to the home directory is still found
    old = tmp_path / "home" / ".migkit-state" / "h"
    old.mkdir(parents=True)
    (old / "shop-20250101-000000.tar.gz").write_bytes(tar.read_bytes())
    got = store.fetch("shop", "20250101-000000")
    assert (got / "undo.sql").read_text().startswith("update t")


# ---- DU6: one name for an engine ----------------------------------------------

def test_an_engine_is_built_with_its_own_name_and_port():
    from migkit.engines import PORTS, SCHEMES, canonical
    hop = build_hop("h", {"engine": "mariadb",
                          "source": {"host": "a"}, "target": {"host": "b"}})
    assert (hop.engine, hop.brand) == ("mysql", {"source": "mariadb",
                                                 "target": "mariadb"})
    assert hop.source.port == 3306, hop.source.port
    # a name an address took and `engine:` refused
    hop = build_hop("h", {"engine": "postgresql",
                          "source": {"host": "a"}, "target": {"host": "b"}})
    assert hop.engine == "postgres" and hop.source.port == 5432
    assert not _errors({"engine": "sqlserver", "source": {"host": "a"},
                        "target": {"host": "b"}})
    # a cross-engine hop's long-form sides took port 0
    hop = build_hop("h", {"engine": "hetero", "source": {"host": "a"},
                          "target": {"host": "b"},
                          "options": {"source_engine": "percona",
                                      "target_engine": "aurora-postgres"}})
    assert (hop.source.port, hop.target.port) == (3306, 5432)
    assert hop.options["source_engine"] == "mysql"
    assert hop.brand == {"source": "percona", "target": "aurora-postgres"}
    assert canonical("Aurora-Postgres") == "postgres"
    assert canonical("nonsense") is None
    assert SCHEMES["alloydb"] == "postgres" and PORTS["oracle"] == 1521
    assert addresses.parse("tdsql://app@10.0.0.5/shop")[0] == "mysql"


# ---- DU16, DU26: one rule for what is not the application's -------------------

def test_a_noise_prefix_leaves_its_tables_out_on_every_engine():
    hop = Hop(name="h", engine="mysql", source=Endpoint(), target=Endpoint(),
              options={"noise_prefix": "_pgc_, awsdms_"})
    assert hop.excluded("shop", "awsdms_status")
    assert hop.excluded("shop", "_pgc_state")
    assert not hop.excluded("shop", "orders")
    # a database is not a table
    assert not hop.excluded("awsdms_db")
    plain = Hop(name="h", engine="mysql", source=Endpoint(),
                target=Endpoint())
    assert not plain.excluded("shop", "awsdms_status")


def test_migkits_own_tables_by_one_exact_rule():
    hop = Hop(name="h", engine="mysql", source=Endpoint(), target=Endpoint())
    assert hop.excluded("shop", "migkit_origin")
    assert hop.excluded("shop", "migkit_claims")
    # `migkit%` took an application's own
    assert not hop.excluded("shop", "migkitchen")
    keys = Hop(name="h", engine="redis", source=Endpoint(), target=Endpoint())
    # a key the application named
    assert not keys.excluded("0", "migkit_session")
    assert config.own("migkit_x") and not config.own("migkitchen")
    assert config.own("migkit_origin", keyed=True)


def test_a_name_handed_over_whole_is_excluded_as_its_parts():
    hop = Hop(name="h", engine="postgres", source=Endpoint(),
              target=Endpoint(), exclude=["orders"])
    assert hop.excluded("shop", "public", "orders")
    assert hop.excluded("shop", "public.orders")
    assert not hop.excluded("shop", "public.customers")


# ---- K24: a cutover's counter margin is its own thread's ----------------------

def test_a_cutovers_margin_reaches_its_own_plan_only(monkeypatch):
    """`keys: apart` was handed to the counters' plan through the
    process's environment, which every other thread read too."""
    import threading

    from migkit.engines.postgres import SEQUENCE_MARGIN, _seq_buffer
    monkeypatch.delenv("SEQUENCE_BUFFER", raising=False)
    seen = []
    token = SEQUENCE_MARGIN.set(1000)
    try:
        other = threading.Thread(target=lambda: seen.append(_seq_buffer()))
        other.start()
        other.join()
        assert _seq_buffer() == 1000
    finally:
        SEQUENCE_MARGIN.reset(token)
    assert seen == [0]
    assert _seq_buffer() == 0 and "SEQUENCE_BUFFER" not in os.environ


# ---- K17: a reference holding `/` in an address -------------------------------

def test_an_address_whose_reference_holds_a_slash():
    engine, ep, dbs = addresses.parse(
        "postgres://app:file:/run/secrets/pw@db1/shop")
    assert (ep["host"], ep["user"], ep["password"], dbs) == (
        "db1", "app", "file:/run/secrets/pw", ["shop"]), ep
    engine, ep, dbs = addresses.parse(
        "mysql://app:vault:secret/db#pw@10.0.0.5:3307/shop")
    assert (ep["host"], ep["port"], ep["password"], dbs) == (
        "10.0.0.5", 3307, "vault:secret/db#pw", ["shop"]), ep
    # a password in the address is still refused
    with pytest.raises(SystemExit, match="holds a password"):
        addresses.parse("postgres://app:s3cret@db1/shop")
