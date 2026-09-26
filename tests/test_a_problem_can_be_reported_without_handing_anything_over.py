"""A diagnostics bundle carries nothing to hand over (backlog 42).

`MIGKIT_DIAGNOSE=<file>.zip migkit doctor` writes what a maintainer needs
to see a problem: versions, what each engine can do, the configuration,
the last verdicts and the change log. Passwords, tokens, notification
addresses, credentials inside an address, and every finding's detail -
where keys and values are - are taken out before anything is written.
"""
import json
import zipfile

from click.testing import CliRunner

SECRET = "CHANGE_ME-secret-42"
HOOK = "https://hooks.slack.com/services/T0/B0/CHANGE_ME_hook"


def test_the_bundle_holds_no_secret_and_no_value(tmp_path, monkeypatch):
    import migkit.config as cfg
    from migkit import cli
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {tmp_path / 'a.db'}, user: x,"
        f" password: {SECRET}}}\n"
        f"    target: {{host: {tmp_path / 'b.db'}, user: x,"
        f" password: {SECRET}}}\n"
        "    databases: [main]\n"
        f"    options: {{notify: ['{HOOK}'], vault_token: {SECRET},"
        f" source_uri: 'postgresql://app:{SECRET}@10.0.0.5:5432/app'}}\n")
    reports = tmp_path / "reports"
    (reports / "lite").mkdir(parents=True)
    (reports / "lite" / "verdict.json").write_text(json.dumps({
        "status": "different", "findings": [{
            "check": "data", "scope": "main.people", "status": "diff",
            "detail": "missing key ann@example.com",
            "fix_hint": "insert ann@example.com"}]}))
    (reports / "lite" / "changelog.jsonl").write_text(json.dumps(
        {"op": "repair", "db": "main", "token": SECRET}) + "\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", reports)
    bundle = tmp_path / "diag.zip"
    monkeypatch.setenv("MIGKIT_DIAGNOSE", str(bundle))
    got = CliRunner().invoke(cli.main, ["doctor"])
    assert got.exit_code == 0, got.output
    assert "diagnostics:" in got.output, got.output
    with zipfile.ZipFile(bundle) as z:
        names = z.namelist()
        text = "".join(z.read(n).decode() for n in names)
    assert {"about.json", "hops.yaml", "lite/verdict.json",
            "lite/changelog.jsonl"} <= set(names), names
    for secret in (SECRET, HOOK, "ann@example.com"):
        assert secret not in text, secret
    # and what it is for is still there
    assert "main.people" in text and '"different"' in text, text
    assert "10.0.0.5:5432/app" in text and "lite" in text, text
    assert '"migkit"' in text and "capabilities" in text, text


def test_scrub_reaches_every_depth():
    from migkit import diagnostics
    got = diagnostics.scrub({"a": {"b": [{"password": "x"}],
                                   "uri": "mongodb://u:p@h:1/db"},
                             "api_key": ["k1", "k2"], "port": 5432})
    assert got == {"a": {"b": [{"password": "<removed>"}],
                         "uri": "mongodb://<removed>@h:1/db"},
                   "api_key": ["<removed>", "<removed>"], "port": 5432}
