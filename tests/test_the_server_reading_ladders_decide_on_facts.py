"""How deep two vector servers' settings are read, how a loaded graph's
statistics are refreshed and what the view asks of an identity provider,
each rung held and refused on what the servers answered and published
(backlog AUD-Z.rungs-owed): a deeper reading only where there is more to
see and the time a call may take holds it, a resample only of what the
server's own sampling would miss, and a signed, logged-out or encrypted
exchange only where the provider asks for it and the view holds a key.
Fakes only."""
from migkit import decide
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("settings_depth", "cluster", "holds"),
    ("settings_depth", "cluster", "refuses"),
    ("settings_depth", "shards", "holds"),
    ("settings_depth", "shards", "refuses"),
    ("settings_depth", "collections", "holds"),
    ("settle_way", "resample", "holds"),
    ("settle_way", "resample", "refuses"),
    ("settle_way", "outdated", "holds"),
    ("settle_way", "outdated", "refuses"),
    ("settle_way", "wait", "holds"),
    ("saml_asks", "signed", "holds"),
    ("saml_asks", "signed", "refuses"),
    ("saml_asks", "plain", "holds"),
    ("saml_asks", "plain", "refuses"),
    ("saml_asks", "redirect", "holds"),
    ("saml_asks", "redirect", "refuses"),
    ("saml_asks", "local", "holds"),
    ("saml_asks", "encrypted", "holds"),
    ("saml_asks", "encrypted", "refuses"),
    ("saml_asks", "either", "holds"),
    ("saml_asks", "either", "refuses"),
]


def _depth(**over):
    facts = {"collections": 4, "shards": 24, "peers": 3, "rtt": 0.01,
             "timeout": 30, "shards_answer": None, "cluster_answer": None}
    facts.update(over)
    return facts


def test_a_cluster_of_split_collections_is_read_to_its_cluster():
    with watched() as seen:
        got = decide.settings_depth(_depth())
    for name in ("cluster", "shards", "collections"):
        assert branch(seen, name) == [HOLDS], name
    assert got.path in ("cluster", "shards", "collections")


def test_a_reading_with_nothing_more_to_see_or_too_slow_stays_shallow():
    with watched() as seen:
        got = decide.settings_depth(_depth(peers=1, shards=8))
    assert got.path == "collections"
    assert why(seen, "cluster") == ("the servers run alone: there is no"
                                    " cluster to read")
    assert "every collection is one shard" in why(seen, "shards")
    with watched() as seen:
        got = decide.settings_depth(_depth(cluster_answer="forbidden"))
    assert why(seen, "cluster") == "the cluster is not read: forbidden"
    assert branch(seen, "shards") == [HOLDS]
    with watched() as seen:
        got = decide.settings_depth(_depth(rtt=2.0, timeout=30))
    # 16 requests for the shards at 2s each pass the 30s a call may take
    assert got.path == "collections"
    assert why(seen, "shards") == ("16 requests at about 2.000s would pass"
                                   " the 30s a call may take")
    assert branch(seen, "cluster") == [REFUSES]


def _graph(**over):
    facts = {"indexes": 5, "rows": 1000, "loaded": 800, "sampling": 75,
             "procedures": ("db.resampleIndex",
                            "db.resampleOutdatedIndexes")}
    facts.update(over)
    return facts


def test_a_load_past_the_servers_sampling_is_resampled_now():
    with watched() as seen:
        got = decide.settle_way(_graph())
    assert branch(seen, "resample") == [HOLDS]
    assert branch(seen, "outdated") == [HOLDS]
    assert branch(seen, "wait") == [HOLDS]
    assert got.path in ("resample", "outdated", "wait")


def test_a_small_load_refreshes_the_outdated_and_an_empty_one_waits():
    with watched() as seen:
        got = decide.settle_way(_graph(loaded=100))
    assert why(seen, "resample") == ("the load changed 10% of the graph,"
                                     " under the 75% the server resamples"
                                     " by itself")
    assert branch(seen, "outdated") == [HOLDS]
    with watched() as seen:
        got = decide.settle_way(_graph(loaded=0, procedures=()))
    assert got.path == "wait"
    assert why(seen, "resample") == ("the server lists no refresh of every"
                                     " index")
    assert "no refresh of the outdated" in why(seen, "outdated")
    with watched() as seen:
        decide.settle_way(_graph(loaded=0))
    assert why(seen, "outdated") == "nothing was loaded: nothing is outdated"
    with watched() as seen:
        got = decide.settle_way(_graph(indexes=0))
    assert got.path == "wait"
    assert "holds no index" in why(seen, "resample")
    assert "holds no index" in why(seen, "outdated")


def test_a_view_with_a_key_signs_logs_out_and_takes_only_encrypted():
    with watched() as seen:
        got = decide.saml_asks({"key": True, "wants_signed": True,
                                "logout_to": "https://idp.example.com/slo",
                                "demanded": True})
    assert {k: c.path for k, c in got.items()} == {
        "request": "signed", "logout": "redirect", "assertion": "encrypted"}
    for name in ("signed", "redirect", "local", "encrypted"):
        assert branch(seen, name) == [HOLDS], name
    assert "demands signed requests" in why(seen, "plain")
    assert "an encryption is demanded" in why(seen, "either")


def test_a_view_without_a_key_asks_plainly_or_is_blocked_by_name():
    with watched() as seen:
        got = decide.saml_asks({"key": False, "wants_signed": False,
                                "logout_to": "", "demanded": False})
    assert {k: c.path for k, c in got.items()} == {
        "request": "plain", "logout": "local", "assertion": "either"}
    assert why(seen, "signed") == ("the provider's metadata does not ask for"
                                   " signed requests")
    assert "offers no single logout" in why(seen, "redirect")
    assert why(seen, "encrypted") == "no encryption is demanded"
    assert branch(seen, "plain") == [HOLDS]
    assert branch(seen, "either") == [HOLDS]
    with watched() as seen:
        got = decide.saml_asks({"key": False, "wants_signed": True,
                                "logout_to": "https://idp.example.com/slo",
                                "demanded": True})
    # a provider that demands signing or encryption the view cannot do is
    # a blocker, never a quiet weaker way
    assert got["request"].path is None and got["assertion"].path is None
    assert got["logout"].path == "local"
    assert why(seen, "signed") == "the view holds no key to sign with"
    assert why(seen, "redirect") == ("the view holds no key to sign a logout"
                                     " with")
    assert "no key to open an assertion" in why(seen, "encrypted")
