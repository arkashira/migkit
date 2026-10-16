"""Where the cloud's own sign-in is minted, how a side is reached, how
sure migkit is of the brand answering, where a hop's change-stream runtime
answers and how an account a grant names reaches the target - each rung
held and refused on what this machine and the server said (backlog
AUD-Z.rungs-owed): a signer only with its library, a forwarder only where
the endpoint names it and its client is installed, a brand only where
nothing read contradicts it, a recorded port only while it is the hop's or
free, an account made only where the hop carries accounts and its password
is known. Fakes only."""
from migkit import decide
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("cloud_sign_in", "signer", "holds"), ("cloud_sign_in", "signer", "refuses"),
    ("cloud_sign_in", "chain", "holds"), ("cloud_sign_in", "chain", "refuses"),
    ("cloud_sign_in", "identity endpoint", "holds"),
    ("cloud_sign_in", "identity endpoint", "refuses"),
    ("cloud_sign_in", "metadata", "holds"),
    ("cloud_sign_in", "metadata", "refuses"),
    ("cloud_forwarder", "direct", "holds"),
    ("cloud_forwarder", "direct", "refuses"),
    ("cloud_forwarder", "<name>", "holds"),
    ("cloud_forwarder", "<name>", "refuses"),
    ("brand_confidence", "api", "holds"), ("brand_confidence", "api", "refuses"),
    ("brand_confidence", "named", "holds"),
    ("brand_confidence", "named", "refuses"),
    ("brand_confidence", "agreed", "holds"),
    ("brand_confidence", "agreed", "refuses"),
    ("brand_confidence", "server", "holds"),
    ("brand_confidence", "server", "refuses"),
    ("brand_confidence", "address", "holds"),
    ("brand_confidence", "address", "refuses"),
    ("connector_port", "recorded", "holds"),
    ("connector_port", "recorded", "refuses"),
    ("connector_port", "found", "holds"), ("connector_port", "found", "refuses"),
    ("account_carried", "made", "holds"), ("account_carried", "made", "refuses"),
    ("account_carried", "locked", "holds"),
    ("account_carried", "locked", "refuses"),
    ("account_carried", "left", "holds"),
]


def test_an_iam_token_is_signed_here_only_with_the_providers_library():
    with watched() as seen:
        got = decide.cloud_sign_in({"kind": "aws_iam", "sdk": True})
    assert got.path == "signer"
    assert branch(seen, "signer") == [HOLDS]
    assert why(seen, "chain") == "the side's sign-in is aws_iam"
    with watched() as seen:
        got = decide.cloud_sign_in({"kind": "aws_iam", "sdk": False})
    assert got.path is None
    assert why(seen, "signer") == "the provider's library is not installed"


def test_a_managed_identity_is_minted_by_chain_endpoint_or_metadata():
    every = {"kind": "azure_managed_identity", "sdk": True,
             "credentials": True, "identity endpoint": "http://10.0.0.1/id",
             "metadata": True}
    with watched() as seen:
        got = decide.cloud_sign_in(every)
    assert got.path == "chain"
    for way in ("chain", "identity endpoint", "metadata"):
        assert branch(seen, way) == [HOLDS], way
    assert branch(seen, "signer") == [REFUSES]
    with watched() as seen:
        got = decide.cloud_sign_in({**every, "credentials": False})
    assert got.path in ("identity endpoint", "metadata")
    assert why(seen, "chain") == "its chain finds no credentials here"
    with watched() as seen:
        got = decide.cloud_sign_in({"kind": "azure_managed_identity",
                                    "sdk": False})
    assert got.path is None
    assert why(seen, "identity endpoint") == ("the environment names no"
                                              " identity endpoint")
    assert "is not one of the cloud's" in why(seen, "metadata")


def test_a_side_is_reached_directly_else_by_a_forwarder_named_and_installed():
    with watched() as seen:
        got = decide.cloud_forwarder({"answers": True})
    assert got.path == "direct"
    assert branch(seen, "direct") == [HOLDS]
    with watched() as seen:
        got = decide.cloud_forwarder({"named": ["ssm"],
                                      "installed": {"ssm": True},
                                      "answers": False, "private": True})
    assert got.path == "ssm"
    assert why(seen, "direct") == "it does not answer from here"
    assert branch(seen, "ssm") == [HOLDS]
    assert "does not name the database service's own connector" in why(
        seen, "cloudsql")
    with watched() as seen:
        got = decide.cloud_forwarder({"named": ["cloudsql"],
                                      "installed": {"cloudsql": False}})
    assert got.path is None
    assert "is not installed here" in why(seen, "cloudsql")


def test_the_brand_is_as_sure_as_the_strongest_sign_nothing_contradicts():
    with watched() as seen:
        got = decide.brand_confidence({"api": "aurora", "server": "aurora",
                                       "asked": True})
    assert got.path == "api"
    assert branch(seen, "api") == [HOLDS]
    assert branch(seen, "agreed") == [HOLDS]
    assert branch(seen, "server") == [HOLDS]
    assert "the hop names no service" in why(seen, "named")
    assert why(seen, "address") == ("the address names no brand of this"
                                    " family")
    with watched() as seen:
        got = decide.brand_confidence({"api": "aurora", "server": "rds"})
    assert got.path is None
    assert "does not agree" in why(seen, "api")
    assert why(seen, "server") == "the server gave no sign of its own"
    with watched() as seen:
        got = decide.brand_confidence({"named": "rds"})
    assert got.path == "named"
    assert branch(seen, "named") == [HOLDS]


def test_a_server_sign_is_agreed_only_where_its_address_says_the_same():
    with watched() as seen:
        got = decide.brand_confidence({"server": "rds", "address": "other"})
    assert got.path == "server"
    assert branch(seen, "agreed") == [REFUSES]
    assert why(seen, "agreed") == "its address does not say the same"
    with watched() as seen:
        got = decide.brand_confidence({"server": "rds"})
    assert why(seen, "agreed") == ("a question that tells its brands apart"
                                   " went unanswered")
    with watched() as seen:
        got = decide.brand_confidence({"endpoint": "aurora",
                                       "address": "aurora"})
    assert got.path == "address"
    assert branch(seen, "address") == [HOLDS]


def test_a_recorded_port_is_taken_again_only_while_it_is_the_hops():
    with watched() as seen:
        got = decide.connector_port({"recorded": True, "mine": True})
    assert got.path == "recorded"
    assert branch(seen, "found") == [REFUSES]
    assert why(seen, "found") == ("no free port could be found on this"
                                  " machine")
    with watched() as seen:
        got = decide.connector_port({"recorded": True, "mine": False,
                                     "free": False, "found": True})
    assert got.path == "found"
    assert why(seen, "recorded") == "another program holds the recorded port"
    assert branch(seen, "found") == [HOLDS]
    with watched() as seen:
        got = decide.connector_port({"recorded": None, "found": True})
    assert why(seen, "recorded") == "the hop's stack names no port yet"


def test_an_account_a_grant_names_is_made_locked_or_left():
    with watched() as seen:
        got = decide.account_carried({"carried": True, "planned": True,
                                      "login": True})
    assert got.path == "made"
    assert branch(seen, "locked") == [HOLDS]
    assert branch(seen, "left") == [HOLDS]
    with watched() as seen:
        got = decide.account_carried({"carried": True, "planned": False,
                                      "login": True})
    assert got.path == "locked"
    assert "no password for it" in why(seen, "made")
    with watched() as seen:
        got = decide.account_carried({"carried": True, "login": False})
    assert got.path == "left"
    assert why(seen, "locked") == "not a login"
    with watched() as seen:
        got = decide.account_carried({"carried": False, "planned": True,
                                      "login": True})
    assert got.path == "left"
    assert why(seen, "made") == ("the hop does not carry users, roles or"
                                 " grants")
    assert branch(seen, "locked") == [REFUSES]
