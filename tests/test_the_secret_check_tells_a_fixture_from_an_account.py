import importlib.util
import pathlib

_spec = importlib.util.spec_from_file_location(
    "check_no_secrets",
    pathlib.Path(__file__).resolve().parent.parent / "tools" / "check_no_secrets.py")
check = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check)

ACCOUNT = "1234567" + "89012"


def test_an_account_id_is_still_caught_wherever_an_account_id_sits():
    for line in (f'"{ACCOUNT}"', f"arn:aws:iam::{ACCOUNT}:role/app",
                 f"account = {ACCOUNT}", f"({ACCOUNT}, 'x')"):
        assert check.flagged(line) == "AWS account id", line


def test_twelve_digits_inside_a_fraction_or_a_uuid_are_not_an_account():
    assert check.flagged('"2024-01-01 00:00:00.123456789000"') is None
    assert check.flagged("'00000000-0000-0000-0000-" + "0" * 12 + "'") is None
    assert check.flagged(f"{ACCOUNT}9") is None


def test_a_documentation_address_passes_and_a_private_one_does_not():
    assert check.flagged("host = '192.0.2.1'") is None
    assert check.flagged("host = '10." + "255.255.1'") == "private IP address"
    assert check.flagged("host = '172." + "31.4.9'") == "private IP address"


def test_an_endpoint_passes_only_when_it_says_it_is_an_example():
    real = "app.cluster-abc.eu-west-1.rds." + "amazonaws.com"
    fake = "app.cluster-example.eu-west-1.rds." + "amazonaws.com"
    assert check.flagged(real) == "real RDS/DocDB endpoint"
    assert check.flagged(fake) is None


def test_ones_own_names_are_caught_from_a_list_kept_off_the_repo(tmp_path,
                                                                 monkeypatch):
    """An organisation's own project, database and table names are not a
    pattern anyone can write down in a public file - the list would be the
    leak. They are read from a file on this machine, and match as whole
    words, whatever the case, even on a line that says it is an example."""
    words = tmp_path / "deny-words.txt"
    words.write_text("# ours\nshopco\nshop_staging\n")
    monkeypatch.setattr(check, "DENY_FILE", words)
    deny = check.deny_pattern(check.deny_words())
    said = "a name of one's own organisation"
    assert check.flagged("exclude: [shop_staging.public.t]  # example",
                         deny) == said
    assert check.flagged("ShopCo moved it", deny) == said
    assert check.flagged("shopcoffee", deny) is None
    assert check.deny_pattern([]) is None
    monkeypatch.setattr(check, "DENY_FILE", tmp_path / "none.txt")
    assert check.deny_words() == []
