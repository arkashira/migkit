"""The ways the rows go from server to server are rungs of the one ladder
a whole database climbs (backlog R22 item 4): taken only where the hop
allows their footprint and both servers can give them, each passed over
with its reason otherwise - and a hop that does not turn them on moves
exactly as before. Where migkit should run is said from the round trips
it measured. No docker here: the servers' answers are given as facts."""
import pytest

from migkit import decide, legs, movers
from migkit.config import Endpoint, Hop


def _hop(**options):
    return Hop(name="h", engine="postgres",
               source=Endpoint(host="10.0.0.5", port=5432, user="app",
                               password="p"),
               target=Endpoint(host="10.0.0.6", port=5432, user="app",
                               password="p"),
               databases=["app"], options=options)


GOOD = {"target": {"available": True, "installed": False, "may_pull": True,
                   "version": 160004, "may_subscribe": True,
                   "logs_ddl": False},
        "source": {"large_objects": False, "version": 160002,
                   "wal_level": "logical", "replication": True,
                   "free_slots": 3, "free_senders": 3, "may_publish": True}}


#: this machine across a wide network from both sides, as measured
FAR = {"src": 45.2, "dst": 38.0}


def _climb(hop, known=GOOD, go=False, rtt=FAR):
    from migkit import sourcechange
    facts = decide.Facts({"engine": "postgres", "hop": hop, "go": go,
                          "db": "app", "legs": known, "rtt": rtt,
                          "published": ['"public"."t"']})
    # the hop's answers to a change on the source, as `movers.choice` puts
    # them
    sourcechange.put_answers(hop, facts, movers._ladder())
    return decide.climb("app", movers._ladder(), facts)


def _with(side, **changes):
    out = {k: dict(v) for k, v in GOOD.items()}
    out[side].update(changes)
    return out


def test_a_hop_that_does_not_allow_it_moves_as_before():
    got = _climb(_hop())
    assert got.path not in ("pgpull", "pgsubscribe")
    assert got.path == movers.pick("postgres")
    passed = dict((r.name, why) for r, why in got.passed)
    assert "the hop does not allow them" in passed["pgpull"]
    # and asks the servers nothing: the leave is looked at first
    facts = decide.Facts({"engine": "postgres", "hop": _hop(), "go": False,
                          "db": "app"},
                         legs=lambda: pytest.fail("asked the servers"))
    decide.climb("app", movers._ladder(), facts)


def test_the_pull_is_taken_where_the_hop_and_the_servers_allow_it():
    got = _climb(_hop(server_to_server=True))
    assert got.path == "pgpull", got.reason
    assert "once, from server to server" in got.reason
    # beside the source, the writes would cross the link: the servers pull
    got = _climb(_hop(server_to_server=True), rtt={"src": 0.3, "dst": 45.2})
    assert got.path == "pgpull", got.reason


@pytest.mark.parametrize("rtt,why", [
    ({"src": 45.2, "dst": 0.4}, "0.4 ms from the target"),
    ({"src": 0.3, "dst": 3.1}, "3.1 ms from the target"),
    ({"src": 45.2, "dst": None}, "could not be measured"),
])
def test_a_machine_beside_the_target_copies_itself(rtt, why):
    """Measured (`legs`' table): beside the target, migkit's own copy is
    the faster, so neither server-to-server way is taken there - and the
    servers are not asked."""
    got = _climb(_hop(server_to_server=True), rtt=rtt)
    assert got.path not in ("pgpull", "pgsubscribe")
    passed = dict((r.name, w) for r, w in got.passed)
    assert why in passed["pgpull"], passed["pgpull"]
    assert why in passed["pgsubscribe"]
    facts = decide.Facts({"engine": "postgres", "go": False, "db": "app",
                          "hop": _hop(server_to_server=True), "rtt": rtt},
                         legs=lambda: pytest.fail("asked the servers"))
    decide.climb("app", movers._ladder(), facts)


@pytest.mark.parametrize("known,why", [
    (_with("target", available=False), "does not offer the foreign-data"),
    (_with("target", may_pull=False), "may neither make the foreign-data"),
    (_with("source", large_objects=True), "holds large objects"),
    ({"target": {"error": "password authentication failed"},
      "source": GOOD["source"]}, "the target could not be asked"),
])
def test_the_pull_passed_over_says_why_and_the_subscription_is_next(known,
                                                                     why):
    got = _climb(_hop(server_to_server=True), known)
    passed = dict((r.name, w) for r, w in got.passed)
    assert why in passed["pgpull"], passed["pgpull"]
    if "large objects" in why or "could not be asked" in why:
        # the subscription needs the same: neither is taken
        assert got.path not in ("pgpull", "pgsubscribe")
    else:
        assert got.path == "pgsubscribe", got.reason
        assert why in got.reason


@pytest.mark.parametrize("known,why", [
    (_with("source", version=150004), "only the same version"),
    (_with("source", wal_level="replica"), "wal_level is replica"),
    (_with("source", free_slots=0), "no replication slot or sender free"),
    (_with("source", replication=False), "may not replicate"),
    (_with("target", may_subscribe=False), "may not make a subscription"),
])
def test_the_subscription_passed_over_says_why(known, why):
    known = {k: dict(v) for k, v in known.items()}
    known["target"]["available"] = False
    got = _climb(_hop(server_to_server=True), known)
    passed = dict((r.name, w) for r, w in got.passed)
    assert why in passed["pgsubscribe"], passed["pgsubscribe"]
    assert got.path not in ("pgpull", "pgsubscribe")


def test_a_source_only_this_machine_can_reach_is_said():
    hop = _hop(server_to_server=True)
    hop.source.host = "127.0.0.1"
    got = _climb(hop)
    passed = dict((r.name, w) for r, w in got.passed)
    assert "the target cannot dial" in passed["pgpull"]
    hop.options = {"server_to_server": {"source": "10.0.0.5:6432"}}
    assert legs.source_for_target(hop) == ("10.0.0.5", 6432)
    assert _climb(hop).path == "pgpull"
    # a tunnelled source: the address the tunnel reaches it at
    hop.options = {"server_to_server": True}
    hop.source.options = {"tunnel_to": "10.0.0.7:5432"}
    assert legs.source_for_target(hop) == ("10.0.0.7", 5432)


def test_a_failed_proof_falls_a_rung_saying_why(monkeypatch):
    monkeypatch.setattr(legs, "prove_pull",
                        lambda hop, db: "could not connect to server")
    monkeypatch.setattr(legs, "prove_subscribe", lambda hop, db: None)
    got = _climb(_hop(server_to_server=True), go=True)
    assert got.path == "pgsubscribe"
    assert "did not prove itself: could not connect to server" in got.reason
    # a dry run proves nothing: it acts on no server
    monkeypatch.setattr(legs, "prove_pull",
                        lambda hop, db: pytest.fail("proved in a dry run"))
    assert _climb(_hop(server_to_server=True), go=False).path == "pgpull"


def test_the_pull_takes_filtered_and_mapped_tables_to_the_copier():
    from migkit import planner
    hop = _hop(server_to_server=True)
    hop.mapping = {"where": {"public.orders": "id > 10"},
                   "columns": {"public.users": {"drop": ["secret"]}}}
    got = {d.table: d.path for d in planner.plan(
        hop, "app", "pgpull", ["public.orders", "public.users",
                               "public.items"])}
    assert got == {"public.items": planner.BULK,
                   "public.orders": planner.COPIER,
                   "public.users": planner.COPIER}
    movers.refuse_unpushable_filters(hop, "app", "pgpull", "postgres")


def test_a_table_without_a_key_is_never_published():
    """In a publication, a table without a key makes the source refuse its
    application's updates and deletes: the subscription copies only the
    keyed ones, and every other way still takes all."""
    from migkit import planner
    hop = _hop(server_to_server=True)
    facts = {"public.k": {"key": False, "rows": 5},
             "public.t": {"key": True, "rows": 5}}
    for via, keyless in (("pgsubscribe", planner.COPIER),
                         ("pgpull", planner.BULK),
                         ("pgcopydb", planner.BULK)):
        got = {d.table: d.path for d in planner.plan(
            hop, "app", via, ["public.k", "public.t"], "public", facts)}
        assert got == {"public.k": keyless, "public.t": planner.BULK}, via
    assert movers.routed_to_copier(hop, "app", "pgsubscribe",
                                   ["public.k", "public.t"], "public",
                                   facts=facts) == ["public.k"]


def test_ranges_cover_every_key_once():
    spans = legs._ranges(-5, 1_000_003, 1_000_000)
    assert spans[0][0] == -5 and spans[-1][1] == 1_000_004
    for (a, b), (c, _) in zip(spans, spans[1:]):
        assert b == c and a < b
    assert len(spans) == 1_000_000 // legs.RANGE_ROWS + 1
    assert legs._ranges(None, None, 0) == [(None, None)]


def test_a_fetch_carries_the_round_trip_within_the_memory_allowed():
    # 20 ms away, rows of 200 bytes: 100 MB/s x 0.02 s = 10,485 rows
    assert legs._fetch_rows(200, 0.020) == 10485
    # a near source still fetches in thousands, never a hundred
    assert legs._fetch_rows(200, 0.0002) == legs.FETCH_LEAST
    # wide rows are held to 64 MB a fetch
    assert legs._fetch_rows(1_000_000, 1.0) == 67


def test_a_pull_that_could_not_run_a_range_is_never_complete(monkeypatch):
    hop = _hop(server_to_server=True)
    work = [("public.big", "insert 1"), ("public.big", "insert 2")]

    def refused(ep, db, autocommit=True):
        raise RuntimeError("could not connect to server")
    monkeypatch.setattr(legs, "_connect", refused)
    with pytest.raises(RuntimeError, match="could not connect"):
        legs._run_ranges(hop, "app", work, 4, "none", None)

    class Cur:
        rowcount = 10

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql):
            if sql == "insert 2":
                raise RuntimeError("duplicate key value")

    class Conn:
        def cursor(self):
            return Cur()

        def commit(self):
            pass

        def close(self):
            pass
    monkeypatch.setattr(legs, "_connect", lambda *a, **k: Conn())
    with pytest.raises(RuntimeError, match="the target's pull stopped:"
                                           " duplicate key value"):
        legs._run_ranges(hop, "app", work, 1, "none", None)
    said = []
    legs._run_ranges(hop, "app", work[:1], 2, "none", said.append)
    assert said == ["public.big: 10 rows pulled"]


def test_where_migkit_should_run():
    far_both = legs.placement(45.2, 38.0)
    assert "every row crosses a wide network twice" in far_both
    assert "Run migkit next to the target" in far_both
    beside_source = legs.placement(0.4, 45.2)
    assert "beside the source and 45.2 ms from the target" in beside_source
    assert "next to the target" in beside_source
    # beside the target is where it should be: nothing to say, unless the
    # reads are made beside the source too
    assert legs.placement(45.2, 0.4) == ""
    assert "compressed" in legs.placement(45.2, 0.4, relay=True)
    assert legs.placement(0.3, 0.4) == ""
    # far from the target, the servers pulling is offered where the hop
    # does not allow it yet, with their own round trip where one was seen
    said = legs.placement(45.2, 38.0, servers_ms=1.2)
    assert "the servers are 1.2 ms apart" in said
    assert "server_to_server" in said
    assert "server_to_server" in legs.placement(0.4, 45.2)
    assert "server_to_server" not in legs.placement(45.2, 38.0, 1.2,
                                                    may_pull=True)
    assert "server_to_server" not in legs.placement(45.2, 0.4, 1.2)
    direct = legs.placement(45.2, 38.0, 1.2, direct_way=True)
    assert "does not change the speed" in direct
    assert legs.placement(None, 3.0) == ""


def test_the_doctor_line_goes_through_the_same_placement():
    from migkit.cli import _placement_said
    assert _placement_said({"src": 0.4, "dst": 45.2}).startswith(
        "[yellow]placement[/yellow]: this machine is beside the source")


# --- the subscription's change on the source ---------------------------------

def _only_the_subscription(**changes):
    known = _with("source", **changes)
    known["target"]["available"] = False
    return known


def test_the_subscriptions_publication_is_a_change_on_the_source(tmp_path,
                                                                 monkeypatch):
    """The temporary subscription makes a publication and a slot on the
    source: a change `sourcechange` offers, never assumes. The hop's
    `server_to_server` is its answer given ahead - the leave for what the
    servers' own copy makes on them - so a hop that set it moves as it
    did; `source_changes` says otherwise, and a source whose user may not
    make it is the DBA's, with the statements."""
    from migkit import config
    monkeypatch.setattr(config, "REPORTS", tmp_path)
    rung = [r for r in movers._ladder() if r.name == "pgsubscribe"][0]
    assert [c.name for c in rung.changes] == ["temporary_publication"]
    known = _only_the_subscription()
    got = _climb(_hop(server_to_server=True), known)
    assert got.path == "pgsubscribe", got.reason
    no = _climb(_hop(server_to_server=True,
                     source_changes={"temporary_publication": "no"}), known)
    assert no.path != "pgsubscribe"
    why = dict((r.name, w) for r, w in no.passed)["pgsubscribe"]
    assert "the answer is no" in why, why
    dba = _climb(_hop(server_to_server=True),
                 _only_the_subscription(may_publish=False))
    assert dba.path != "pgsubscribe"
    why = dict((r.name, w) for r, w in dba.passed)["pgsubscribe"]
    assert "only a DBA can make here" in why, why
    assert "create publication" in why and '"public"."t"' in why, why


def test_the_publication_is_made_and_taken_off_as_a_change(tmp_path,
                                                          monkeypatch):
    """Made through `sourcechange.making`: recorded before it is made,
    its statements the definition's, taken off again after."""
    from migkit import config, sourcechange
    monkeypatch.setattr(config, "REPORTS", tmp_path)
    hop = _hop(server_to_server=True)
    ran = []
    facts = {"hop": hop, "db": "app", "published": ['"public"."t"']}
    with sourcechange.making(hop, legs.PUBLICATION, facts, ran.append,
                             "app", lambda m: None):
        assert any(s.startswith("create publication") and '"public"."t"'
                   in s for s in ran), ran
        made = list(ran)
    taken = ran[len(made):]
    assert any(s.startswith("drop publication") for s in taken), taken
    assert any("pg_drop_replication_slot" in s for s in taken), taken
    assert sourcechange.left_on_source(hop) == []
