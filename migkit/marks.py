"""How a two-way tail tells its own writes apart on a side: the rungs
(backlog R3, decided 2026-09-27).

Two things a rung can give. *apart*: the tail reading that side leaves
out every transaction migkit applied there. *exact*: the side itself
says which batch it last committed, so a batch is never applied twice -
what counters need (`twoway.exact`). A hop with counters leaves out the
rungs that are apart only.

    PostgreSQL
      origin    a replication origin of migkit's own, set up on the
                applying session; the batch's number handed to
                `pg_replication_origin_xact_setup`, its progress the last
                batch committed. The reader asks the server to leave out
                what carries an origin (`only-local`); `test_decoding`
                never prints the origin itself (measured on 14 and 16:
                `include-origin` is not an option it knows), so it is
                the server that filters, before anything is decoded.
                The binary protocol does send it (an Origin message after
                BEGIN, on 14 and 16), and from 16 leaves it out itself
                (`origin = none`; 14 does not know the option) - the
                decoding is in `pgslot` for a reader that speaks it.
                Apart and exact; the functions are the superuser's until
                granted, and GRANT EXECUTE opens them on 14 and 16 alike
                (measured). Only where no other origin applies into the
                side: the filter would leave its changes out too.
      message   `pg_logical_emit_message(true, 'migkit', ...)` first in
                the transaction; printed after its BEGIN on 14 and 16.
                Apart only; any user.
      table     a row of `migkit_origin` first in the transaction.
                Apart and exact; needs CREATE once.
    MySQL
      gtid_tag  a tagged GTID of migkit's own (8.3+, `gtid_mode` ON,
                TRANSACTION_GTID_TAG and one of SESSION_VARIABLES_ADMIN,
                SYSTEM_VARIABLES_ADMIN or REPLICATION_APPLIER, measured
                on 8.4): the transaction's GTID is migkit's UUID, the tag
                `migkit` and the batch's number, so `gtid_executed` names
                the batches committed. The reader parses the tagged GTID
                event itself (`binlog_marks`). Apart and exact. Measured
                on 8.4: a transaction under a GTID the server executed
                already is skipped, its statements answering as though
                they ran - so a number is never handed out twice.
      comment   every statement applied starts with a comment the
                reader finds in the transaction's rows-query event (kept
                as sent, measured on 8.4) - only where the server already
                logs those (`binlog_rows_query_log_events`); migkit
                changes no setting. Apart only.
      table     as on PostgreSQL.
    MariaDB
      skip_flag the session's `skip_replication`: every event of the
                transaction carries the flag (0x8000) in its header,
                measured on 11.8, where a user with no global privilege
                may set it. Apart only. The target's own replicas that
                filter such events do not receive migkit's writes, and a
                transaction the application itself marks so is left out
                as well - `doctor` says both.
      table     as on PostgreSQL.

    MongoDB
      txn_doc   a document of `migkit_origin` first in a multi-document
                transaction: the change stream names the transaction
                (`lsid`, `txnNumber`) of every event, so what follows an
                insert into `migkit_origin` in the same transaction is
                migkit's own. Apart and exact; needs a replica set.
      field     the field `_migkit_marker` on every document written (and,
                before a delete, set on the document so the pre-image of
                the delete carries it): needs change stream pre-images on
                the collections already, as migkit changes no setting.
                Apart only; no transaction needed.
    Cassandra
      batch     a logged batch whose first statement writes `migkit_origin`
                and whose every statement carries the batch's write time:
                the reader leaves out the cells written at a time the origin
                table names. Apart and exact.
    Redis
      multi     MULTI / EXEC with `HSET migkit:origin` first: a monitor
                or a replication stream shows the marker command opening
                the transaction. Apart and exact; not on a cluster that
                spreads the unit over slots.
    DynamoDB
      item      TransactWriteItems whose first item puts the unit into
                `migkit_origin` and whose items carry `_migkit_marker`; a
                delete is told by the keys the origin item names. Apart
                and exact.
    Kafka
      header    the header `migkit-marker` on every record produced.
                Apart only.

No sandbox, so not proved (the rung each would take, written): Cosmos DB,
a transactional batch whose first operation upserts a `migkit_origin`
item in the partition; Couchbase, a transaction whose first mutation
writes a `migkit_origin` document; Keyspaces, a logged batch as on
Cassandra; Spanner, a read-write transaction whose first mutation writes
`migkit_origin` (a change stream names the transaction).

The reader knows every rung's mark at once, whichever the other hop
chose: the hop that applies into a side and the one that reads it are
two tails, and neither has to tell the other anything.

Which rung, per side: the ones the side's version, settings and grants
allow (`needs`, from `Engine.mark_facts`), ranked by what they were
measured to cost - the owner's rule: what is measured faster wins, and
what a rung leaves on the side only breaks a tie (`TIE_US`). Then the
proof, before anything is applied: a probe transaction written through
the rung and read back from that side's log with migkit's own reader,
which has to see it as migkit's own (`Engine.mark_prove`); a rung that
does not prove itself is said and the next one down is tried. The rung
chosen is kept in the tail's token, so a restart stands on the same one
and asks it which batch the side committed; both directions may stand on
different rungs.

Measured (`bench/marks_cost.py`, 300 one-row transactions a round applied
through migkit's own applier - a connection a batch, as it applies -
under each rung and under none, the rungs in turn; each rung's time less
none's in the same round, the median of ten rounds over two runs, five
for MariaDB; and the tail's reader over what each wrote), microseconds a
transaction, what the mark adds / what the reader spends:

    PostgreSQL 16   origin 676 / 600    message <= 0 / 625   table 204 / 612
                    none's rounds spread by 614 and 476
    MySQL 8.4       gtid_tag 277 / 155  comment 197 / 136    table 325 / 199
                    none's rounds spread by 1840 and 1274
    MariaDB 11.8    skip_flag 237 / 152                      table 575 / 189
                    none's rounds spread by 1144

The machine was shared while this ran, and it shows: every MySQL rung is
inside the spread of none's own rounds, so there the footprint decides
and the tagged GTID, which leaves nothing, goes first. On PostgreSQL the
origin was the dearest in eight rounds of ten: a connection taking it
costs about a millisecond (the same without migkit: a session set up a
transaction, 2158, against one kept, 1111), and migkit's applier opens
a connection a batch. So there the table goes before it for a hop that
counts, and the message first for one that does not - the owner's rule,
measured, not assumed. On PostgreSQL 14 (`bench/marks_probe_pg.py`, one
connection kept) the three cost alike: table 1157, origin 1083, message
1069 against none's 900 and 856.
"""
import json

from . import decide

APART, EXACT = "apart", "exact"

#: measured, microseconds a transaction: (what the mark adds to one the
#: applier commits, what the reader spends a transaction on it), by the
#: brand of server it was measured on (`bench/marks_cost.py`)
COST = {
    ("postgres", "origin"): (676.0, 600.0),
    ("postgres", "message"): (0.0, 625.0),
    ("postgres", "table"): (204.0, 612.0),
    ("mysql", "gtid_tag"): (277.0, 155.0),
    ("mysql", "comment"): (197.0, 136.0),
    ("mysql", "table"): (325.0, 199.0),
    ("mariadb", "gtid_domain"): (180.0, 140.0),
    ("mariadb", "skip_flag"): (237.0, 152.0),
    ("mariadb", "table"): (575.0, 189.0),
}
#: two costs closer than this are the same cost: how far none's own rounds
#: spread, not a difference between rungs
TIE_US = {"postgres": 545.0, "mysql": 1557.0, "mariadb": 1144.0}


def brand(facts):
    return "mariadb" if facts.get("mariadb") else facts.get("family", "")


#: what a probe on a side measured (`measure`), by the brand, the side
#: ("src" / "dst") and the rung: the same pair as `COST`, and over it - the
#: owner's rule, what is measured here wins over what was measured once
#: elsewhere. Keyed by the side too: the two sides of a two-way hop of one
#: brand are two servers, and one's probe says nothing of the other's
MEASURED = {}
#: how far a side's bare rounds spread in that probe, microseconds, by
#: (brand, side): two rungs closer than it cannot be told apart there
SPREAD = {}


def cost_us(brand_, name, side=None):
    """Microseconds a transaction rung `name` costs: what a probe on
    `side` measured (`measure`), else what was measured once on that
    brand (`COST`), 0.0 where neither says."""
    key = (brand_, side, name)
    return sum(MEASURED[key] if key in MEASURED else
               COST.get((brand_, name), (0.0, 0.0)))


def cost(r, facts, side="dst"):
    """What a rung costs on the side's kind of server: what a probe on
    this very side measured (`measure`), else what was measured once."""
    return cost_us(brand(facts), r.name, side)


def prices(rungs, facts, side="dst"):
    """{rung name: `costmodel.Estimate` in seconds, or None} - the facts
    the decision engine ranks `rungs` by (`decide.rank`, `decide.climb`):
    measured on this side where a probe ran (`measure`), else what was
    measured once on that brand, handed in as a prediction (`COST`); None
    where nothing was. Each carries as its noise how far a side's bare
    rounds spread - measured here, else `TIE_US` - so two rungs within it
    tie, and the tie goes to the one that leaves nothing on the side."""
    from .costmodel import MEASURED_TRUTH, PREDICTED, Estimate
    brand_ = brand(facts)
    out = {}
    for r in rungs:
        here = (brand_, side, r.name) in MEASURED
        us = cost_us(brand_, r.name, side)
        if not here and not us:
            out[r.name] = None
            continue
        tie = SPREAD.get((brand_, side)) if here else None
        tie = TIE_US.get(brand_, 0.0) if tie is None else tie
        out[r.name] = Estimate(
            us / 1e6, truth=MEASURED_TRUTH if here else PREDICTED,
            source=("a probe of this side" if here else
                    "measured once on this kind of server"),
            noise=(tie / us) if us else 0.0)
    return out


#: one way of marking migkit's own writes on one family of servers: a
#: `decide.ServerRung` - its name, what it is in migkit's words (`said`),
#: what it gives, the one need that the side allows it (`allowed`), the
#: family (`engine`), and what it leaves on the side (`leaves`, its
#: `footprint` 1 where something stays)
Rung = decide.ServerRung


def allowed(why_not):
    """The need that the side allows a mark: `why_not(facts)` is None
    where it does, else why not."""
    return decide.Need("the side to allow it",
                       lambda f: why_not(f) is None, why_not)


def _pg_origin(f):
    if not f.get("origin_grants"):
        return ("the replication origin functions are not granted to this"
                " user (GRANT EXECUTE on pg_replication_origin_create,"
                " _session_setup, _xact_setup and _progress)")
    if f.get("foreign_origins"):
        return ("another replication origin applies into this server"
                f" ({', '.join(f['foreign_origins'])}), and leaving out what"
                " carries an origin would leave its changes out too")
    return None


def _pg_message(f):
    return None if f.get("version", 0) >= 90600 else \
        "logical messages are written from PostgreSQL 9.6"


def _table(f):
    return None if f.get("can_create") else \
        "this user cannot make the table in the database"


def _gtid_tag(f):
    if f.get("mariadb"):
        return "MariaDB has no tagged GTIDs"
    if tuple(f.get("version") or ()) < (8, 3):
        return "tagged GTIDs are MySQL 8.3 and later"
    if str(f.get("gtid_mode", "")).upper() != "ON":
        return f"gtid_mode is {f.get('gtid_mode') or 'OFF'} here"
    grants = f.get("grants")
    if grants is not None and "TRANSACTION_GTID_TAG" not in grants:
        return "this user is not granted TRANSACTION_GTID_TAG"
    if grants is not None and not grants & {
            "SYSTEM_VARIABLES_ADMIN", "SESSION_VARIABLES_ADMIN",
            "REPLICATION_APPLIER", "SUPER"}:
        return ("this user may not set a session's GTID"
                " (SESSION_VARIABLES_ADMIN or REPLICATION_APPLIER)")
    return None


def _comment(f):
    if f.get("mariadb"):
        return ("MariaDB sends its annotate events only to a reader that"
                " follows by GTID, and migkit's follows by position")
    if not f.get("rows_query"):
        return ("the server does not log each statement's text with its"
                " rows (binlog_rows_query_log_events), and migkit changes"
                " no setting")
    return None


def _gtid_domain(f):
    if not f.get("mariadb"):
        return "GTID domain is MariaDB's"
    if not f.get("gtid_domain"):
        return "MariaDB GTID domain is not configured"
    return None


def _skip_flag(f):
    if not f.get("mariadb"):
        return "skip_replication is MariaDB's"
    return _table(f)


RUNGS = (
    Rung("origin", "a replication origin of migkit's own",
         frozenset({APART, EXACT}), (allowed(_pg_origin),), engine="postgres",
         footprint=1, leaves="a replication origin per applying"
         " connection (migkit_twoway_<hop>), each an entry of"
         " max_replication_slots"),
    Rung("message", "a logical message first in each transaction",
         frozenset({APART}), (allowed(_pg_message),), engine="postgres",
         leaves="nothing"),
    Rung("table", "a row of the table migkit_origin first in each transaction",
         frozenset({APART, EXACT}), (allowed(_table),), engine="postgres",
         footprint=1, leaves="the table public.migkit_origin"),
    Rung("gtid_tag", "a GTID of migkit's own, tagged migkit",
         frozenset({APART, EXACT}), (allowed(_gtid_tag),), engine="mysql",
         leaves="nothing"),
    Rung("comment", "a comment on every statement applied",
         frozenset({APART}), (allowed(_comment),), engine="mysql",
         footprint=1, leaves="the table migkit_origin, for its proof"),
    Rung("gtid_domain", "a GTID domain of migkit's own (gtid_domain_id)",
         frozenset({APART, EXACT}), (allowed(_gtid_domain),), engine="mysql",
         leaves="nothing"),
    Rung("skip_flag", "the transaction marked for skip (skip_replication)",
         frozenset({APART}), (allowed(_skip_flag),), engine="mysql",
         footprint=1, leaves="the table migkit_origin, for its proof"),
    Rung("table", "a row of the table migkit_origin first in each transaction",
         frozenset({APART, EXACT}), (allowed(_table),), engine="mysql",
         footprint=1, leaves="the table migkit_origin"),
)


def _mongo_txn(f):
    if not f.get("transactions"):
        return ("a transaction needs a replica set or a sharded cluster,"
                " and this deployment is neither")
    return _table(f)


def _mongo_field(f):
    if not f.get("pre_images"):
        return ("a delete leaves the field on nothing the change stream"
                " shows unless pre-images are on for every collection"
                " (changeStreamPreAndPostImages), and migkit changes no"
                " setting")
    return None


def _batch(f):
    return _table(f)


def _redis_multi(f):
    if f.get("cluster"):
        return ("a cluster's transaction keeps to one slot, and the marker"
                " key is in another")
    return None if tuple(f.get("version") or ()) >= (2, 6) else \
        "MULTI with a first command is Redis 2.6 and later"


def _ddb_item(f):
    if not f.get("transactions"):
        return "this endpoint takes no TransactWriteItems"
    return _table(f)


def _kafka_header(f):
    return None if f.get("headers") else \
        "this broker takes no record headers (0.11 and later do)"


RUNGS = RUNGS + (
    Rung("txn_doc", "a document of migkit_origin first in the transaction",
         frozenset({APART, EXACT}), (allowed(_mongo_txn),), engine="mongodb",
         footprint=1, leaves="the collection migkit_origin"),
    Rung("field", "the field _migkit_marker on every document written",
         frozenset({APART}), (allowed(_mongo_field),), engine="mongodb",
         leaves="nothing"),
    Rung("batch", "a logged batch whose first statement writes migkit_origin",
         frozenset({APART, EXACT}), (allowed(_batch),), engine="cassandra",
         footprint=1, leaves="the table migkit_origin"),
    Rung("multi", "MULTI / EXEC with the marker key migkit:origin first",
         frozenset({APART, EXACT}), (allowed(_redis_multi),), engine="redis",
         footprint=1, leaves="the hash migkit:origin"),
    Rung("item",
         "a transaction whose first item puts the unit into migkit_origin",
         frozenset({APART, EXACT}), (allowed(_ddb_item),), engine="dynamodb",
         footprint=1, leaves="the table migkit_origin"),
    Rung("header", "the header migkit-marker on every record",
         frozenset({APART}), (allowed(_kafka_header),), engine="kafka",
         leaves="nothing"),
)


def rung(family, name):
    for r in RUNGS:
        if r.engine == family and r.name == name:
            return r
    return None


def _why_not(r, facts):
    """Why the side does not allow rung `r`, None where it does."""
    if facts.get("exact") and EXACT not in r.gives:
        return ("it tells migkit's writes apart but cannot say which batch"
                " the side last committed, and this hop counts")
    return r.refused(facts)


def _decided(rungs, side_engine, side, db, facts):
    """`rungs` as the decision engine's (`decide.Rung`): allowed by
    `_why_not`, proved by `Engine.mark_prove`, what each leaves on the
    side its footprint. Handed over cheapest first on this side: the
    engine breaks a tie by the footprint and then by the order it was
    given, so of two within the spread that both leave something, the
    cheaper goes first."""
    from . import decide
    rungs = sorted(rungs, key=lambda r: cost(r, facts, side))
    return [decide.Rung(
        r.name, r.said, footprint=r.footprint, leaves=r.leaves,
        needs=(decide.Need(
            "the side to allow it", lambda f, r=r: _why_not(r, f) is None,
            lambda f, r=r: _why_not(r, f)),),
        prove=lambda f, r=r: side_engine.mark_prove(side, db, r.name))
        for r in rungs]


def choose_rung(side_engine, facts, side="dst"):
    """([rungs to try, best first], [(rung, why not)]) for one side.

    `facts` are the side's (`Engine.mark_facts`) with `exact` added: a
    hop with counters takes only rungs that give it. The rest are put in
    order by the decision engine (`decide.rank`) on what each costs on
    that side (`prices`): within its spread of each other, the one that
    leaves nothing on the side goes first. What `doctor` says it would
    try; the tail's own choice is `choice`."""
    from . import decide
    family = facts.get("family") or getattr(side_engine, "CANON_ENGINE", "")
    facts = dict(facts, family=family)
    kept, dropped = [], []
    for r in RUNGS:
        if r.engine != family:
            continue
        why = _why_not(r, facts)
        if why:
            dropped.append((r, why))
            continue
        kept.append(r)
    by = {r.name: r for r in kept}
    order = decide.rank(_decided(kept, side_engine, side, None, facts),
                        prices(kept, facts, side))
    return [by[r.name] for r in order], dropped


def saved(token_path):
    """The tail's envelope (`tailctl.position`), {} where none is saved. A
    file that cannot be read whole stops, as it stops the tail: read as
    empty, a rung the batches were numbered on was chosen afresh."""
    from . import tailctl
    return tailctl.position(token_path) or {}


def _keep_rung(token_path, name):
    """`name` kept in the tail's envelope as the rung it stands on."""
    from . import tailctl
    tailctl.keep(token_path, {**(saved(token_path) or {"token": None}),
                              "rung": name})


def kept_rung(token_path):
    """The rung a tail's token keeps, or None: a token that numbers its
    batches and names no rung is from before the rungs, when the table
    was the only one."""
    got = saved(token_path)
    if got.get("rung"):
        return got["rung"]
    if got.get("batch") is not None:
        return "table"
    return None


def settle(pair, db, token_path, exact, log):
    """The rung this tail marks its writes on the target with: the one
    its token keeps, or the highest that proves itself now, written into
    the token before anything is applied. A kept rung the side no longer
    allows is climbed from again where no batch depends on it; where
    batches are numbered on it, the tail stops and says so."""
    return settle_engine(pair.dst_engine, db, token_path, exact, log,
                         hop=getattr(pair, "hop", None))


def settle_engine(dst, db, token_path, exact, log, hop=None):
    """`settle` for an engine that is its own pair - one engine object
    reading the source and writing the target, the tails of the document
    stores, the key-value stores and the brokers. The facts the rung was
    taken on are kept on the engine (`boundary`)."""
    name = kept_rung(token_path)
    if name is not None:
        r = rung(getattr(dst, "CANON_ENGINE", ""), name)
        why = r.refused(dict(dst.mark_facts("dst", db), exact=bool(exact))) \
            if r is not None else f"{name} is not a rung migkit knows"
        if why and exact:
            raise SystemExit(
                f"two_way: this tail numbers its batches on the target by"
                f" {r.said if r else name}, and the target no longer"
                f" allows it: {why}. Which batch it last committed is kept"
                " there, so put that right and start the tail again")
        if why:
            log(f"two_way: the target no longer allows {r.said if r else name}"
                f" ({why}); choosing again")
            name = None
        elif exact and saved(token_path).get("batch") is None:
            # batches about to be numbered afresh on it: proved again,
            # which sets back what an earlier run left counted there
            why = dst.mark_prove("dst", db, name)
            if why:
                log(f"two_way: {r.said} did not prove itself on the target"
                    f" ({why}); choosing again")
                name = None
    if name is None:
        name = choice(dst, "dst", db, exact, log, hop=hop).rung.name
        _keep_rung(token_path, name)
    dst._mark_rung = name
    dst._mark_taken = _taken(dst, db)
    return name


def _taken(dst, db):
    """The side's facts as a rung is taken on them, to be compared at the
    next boundary: what `Engine.mark_facts` says of the target."""
    return {k: v for k, v in dst.mark_facts("dst", db).items()
            if k != "can_create"}


def boundary(dst, db, token_path, exact, log):
    """A safe boundary of a tail (between two rounds, nothing in flight):
    where the target's facts are no longer those its rung was taken on
    (`Engine.mark_facts` - a server upgraded, a collection's images
    turned off, a cluster joined), the rung is chosen again, through the
    same probe and the same decision engine as at the start, and kept in
    the token. A tail that numbers its batches on the rung keeps it where
    the side still allows it (`settle_engine` stops it where it does
    not). Returns the rung the tail stands on."""
    now = _taken(dst, db)
    if now == dst.__dict__.get("_mark_taken"):
        return dst.__dict__.get("_mark_rung")
    log("two_way: the target's facts changed; choosing the mark again")
    if not (exact and saved(token_path).get("batch") is not None):
        _keep_rung(token_path, None)
    return settle_engine(dst, db, token_path, exact, log)


def tail_start(dst, db, go, token_path, log):
    """The mark an engine that is its own pair writes its tail's changes
    with, taken before the first one is read: the rung `settle_engine`
    stands on where the hop is two-way and the tail applies, else None -
    and the engine writes bare, as a one-way tail does."""
    from . import twoway
    dst.__dict__.pop("_mark_rung", None)
    if not go or twoway.settings(getattr(dst, "hop", None)) is None:
        return None
    return settle_engine(dst, db, token_path, False, log)


def tail_round(dst, db, token_path, log):
    """What a tail does between two rounds for its mark: `boundary`, where
    the tail marks at all."""
    if dst.__dict__.get("_mark_rung") is None:
        return None
    return boundary(dst, db, token_path, False, log)


def tail_batch(dst):
    """The number the next unit of this tail's writes carries in its mark,
    counted from 1 for the life of the engine object."""
    n = dst.__dict__.get("_mark_batch", 0) + 1
    dst._mark_batch = n
    return n


def exact_wanted(hop):
    """Whether a one-way tail numbers its batches on the target (F7, R19
    lever 11): by default, unless the hop's `exact_batches` is false."""
    got = (hop.options or {}).get("exact_batches")
    return got is None or bool(got)


def settle_exact(pair, db, token_path, log):
    """The rung a one-way tail numbers its batches on the target by, or
    None where the target keeps none - the tail then reads again after a
    stop what it cannot tell was applied, as it always did.

    Only `table`: the rung that says which batch a side committed on
    PostgreSQL, MySQL and MariaDB alike with no grant and no version
    beyond making the table, and measured the cheapest of those that do on
    PostgreSQL (204 us a transaction against the origin's 676, `COST`).
    A tagged GTID or an origin proves itself through a read of the side's
    own log (`mark_prove`), which a one-way target need not keep; the
    table is proved by what it keeps: a batch's mark written in a
    transaction of its own and read back (`origin_seen`). The number the
    probe leaves is below every batch's (`hetero._saved_batch`), so it is
    never taken for one."""
    import secrets

    dst = pair.dst_engine
    if not exact_wanted(pair.hop) or not callable(
            getattr(dst, "mark_facts", None)) or not callable(
            getattr(dst, "origin_seen", None)):
        return None
    family = getattr(dst, "CANON_ENGINE", "")
    if kept_rung(token_path) == "table":
        dst._mark_rung, dst._numbered = "table", True
        return "table"
    try:
        facts = dst.mark_facts("dst", db)
    except Exception as e:  # noqa: BLE001 - not numbered, then
        log("batches are not numbered on the target: its facts could not"
            f" be read ({type(e).__name__})")
        return None
    r = rung(family, "table")
    why = r.refused(facts) if r is not None else \
        f"{family} keeps no batch of migkit's"
    probe = -1 - secrets.randbits(30)
    if not why:
        dst._mark_rung, dst._numbered = "table", True
        dst._batch_seen = json.dumps({"token": None, "batch": probe})
        try:
            with dst._apply_session("dst", db):
                pass
            got = dst.origin_seen("dst", db)
            if not got or int(got.get("batch", 0)) != probe:
                why = "the probe's mark did not read back"
        except Exception as e:  # noqa: BLE001 - said, not numbered
            why = (str(e).strip().splitlines() or [type(e).__name__])[0]
        finally:
            dst.__dict__.pop("_batch_seen", None)
    if why:
        dst.__dict__.pop("_numbered", None)
        dst.__dict__.pop("_mark_rung", None)
        log(f"batches are not numbered on the target ({why}): after a"
            " stop the tail reads again from its saved position, as it"
            " always did")
        return None
    _keep_rung(token_path, "table")
    log("batches are numbered on the target (a row of migkit_origin): a"
        " stop goes on after the last batch the target committed, not"
        " from before it")
    return "table"


def said(side_engine, token_path, db):
    """A line for `doctor`: the rung a side stands on and what it leaves
    there, or the ones it would try - in order - where no tail has
    chosen yet."""
    family = getattr(side_engine, "CANON_ENGINE", "")
    name = kept_rung(token_path)
    if name:
        r = rung(family, name)
        if r is None:
            return f"two-way: marks migkit's writes by {name}"
        out = (f"two-way: migkit's own writes here carry {r.said}"
               + (f"; it leaves {r.leaves}" if r.footprint else
                  "; it leaves nothing on the server"))
        if r.name == "skip_flag":
            out += (". [yellow]This server's own replicas that filter"
                    " events marked for skip (replicate_events_marked_for_"
                    "skip other than REPLICATE) do not receive migkit's"
                    " writes, and a transaction the application itself"
                    " marks for skip is left out of the way back as"
                    " migkit's own[/yellow]")
        return out
    try:
        facts = side_engine.mark_facts("dst", db)
    except Exception:  # noqa: BLE001 - a probe never fails doctor
        return ""
    ranked, _ = choose_rung(side_engine, facts)
    if not ranked:
        return ("two-way: [red]no way of marking migkit's own writes is"
                " allowed here[/red]")
    return ("two-way: no tail has chosen how to mark migkit's writes here"
            " yet; it tries " + ", then ".join(r.said for r in ranked)
            + " - and proves the one it stands on first")


def lsn(n):
    """A batch's number as a position PostgreSQL keeps for an origin."""
    n = int(n)
    return f"{n >> 32:X}/{n & 0xFFFFFFFF:X}"


def number(position):
    hi, _, lo = str(position).partition("/")
    return (int(hi, 16) << 32) | int(lo, 16)


def batch_of(engine):
    """The number of the batch being applied, where the hop numbers them
    (`twoway.batch_seen`), else None."""
    from . import twoway
    got = twoway.seen_of(twoway.batch_seen(engine))
    return int(got["batch"]) if got else None


def measure(side_engine, side, db, facts, rungs):
    """Probe what each of `rungs` adds to a unit on this side, in
    microseconds: the side writes a scratch unit through each rung and
    bare, the rungs in turn within every round (`Engine.mark_timed`), and
    a rung's cost is its median less the bare median of the same rounds.
    The rounds go on until the median of the last three agrees with the
    one before to within the bare rounds' own spread, or the side has
    been probed as many rounds as it has rungs times three - a side that
    will not settle is priced by what it did show. Kept in `MEASURED`."""
    import statistics
    brand_ = brand(facts)
    names = [None] + [r.name for r in rungs]
    seen = {n: [] for n in names}
    last = None
    for rnd in range(1, 3 * len(names) + 1):
        for n in names:
            seen[n].extend(side_engine.mark_timed(side, db, n, 1))
        if rnd >= 3:
            now = {n: statistics.median(seen[n][-rnd:]) for n in names}
            spread = (max(seen[None]) - min(seen[None])) or 1e-9
            if last is not None and all(
                    abs(now[n] - last[n]) <= spread for n in names):
                break
            last = now
    bare = statistics.median(seen[None])
    SPREAD[(brand_, side)] = (max(seen[None]) - min(seen[None])) * 1e6
    for r in rungs:
        MEASURED[(brand_, side, r.name)] = (
            max(0.0, (statistics.median(seen[r.name]) - bare) * 1e6), 0.0)
    return {r.name: cost(r, facts, side) for r in rungs}


def choice(side_engine, side, db, exact=False, log=None, hop=None):
    """The decision engine's `Choice` of the rung this side's marks go by
    (`decide.climb`): the rungs the side allows, each priced by a probe
    on the side itself where it can be probed (`measure`,
    `Engine.mark_timed`) and by what was measured once on that kind of
    server where it cannot (`prices`), and proved (`Engine.mark_prove`)
    before it is taken - a rung that does not prove itself falls to the
    next. The rung stands on `side_engine._mark_rung`; the Choice, with
    the cost of each rung and every one passed over, goes into the hop's
    decision record (`decide.record`)."""
    from . import decide
    log = log or (lambda _m: None)
    where = "the target" if side == "dst" else "the source"
    facts = dict(side_engine.mark_facts(side, db), exact=bool(exact))
    facts["family"] = facts.get("family") or getattr(
        side_engine, "CANON_ENGINE", "")
    allowed, _ = choose_rung(side_engine, facts, side)
    if allowed and callable(getattr(side_engine, "mark_timed", None)):
        measure(side_engine, side, db, facts, allowed)
    family = [r for r in RUNGS if r.engine == facts["family"]]
    got = decide.climb(f"the mark of migkit's writes on {where}",
                       _decided(family, side_engine, side, db, facts),
                       facts,
                       costs=prices(allowed, facts, side))
    for r, why in got.passed:
        if "did not prove itself" in why:
            log(f"two_way: {why} on {where}; trying the next")
    if got.rung is None:
        raise SystemExit(
            f"two_way: no way of marking migkit's own writes proved itself"
            f" on {where}, so what it applies would come back to where it"
            " began. " + ("; ".join(why for _, why in got.passed)
                          or "None of them is allowed there."))
    log(f"two_way: migkit's own writes on {where} carry {got.rung.said}"
        " - a probe written so came back from its log as migkit's")
    side_engine._mark_rung = got.rung.name
    hop = hop if hop is not None else getattr(side_engine, "hop", None)
    if hop is not None:
        decide.record(hop, got)
    return got


def teardown(side_engine, side, db, go=True):
    """Take away what the marks left on the side (`Engine.mark_teardown`,
    one signature on every engine: side, db, the rung, go): the origin
    table, the marker key. The rung stands no more where `go`. [what it
    takes away, done where `go`]"""
    steps = side_engine.mark_teardown(
        side, db, side_engine.__dict__.get("_mark_rung"), go)
    if go:
        side_engine.__dict__.pop("_mark_rung", None)
    return steps
