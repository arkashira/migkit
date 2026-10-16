"""Every rung of the decision engine is reached by a test on each branch
it has (backlog AUD-Z.rungs): a rung no test holds and refuses can carry a
need that never fails or one that never holds, and nothing says so.

The way chosen is a declaration over a recorder, not a tracer over the
suite. A tracer (`sys.settrace`, coverage of the need lambdas) proves a
need was evaluated only by running every decide-touching test inside this
one - minutes, docker lanes and a guard whose answer depends on what ran.
A declaration is read from the source in milliseconds and answers alike
everywhere; it is kept honest by what may declare: a test module names a
rung in `PROVES_RUNGS` as `(ladder, rung, branch)` only where it imports
`tests.rung_branches` and opens `watched()`, which records each rung
`decide.prune` was handed as held, refused by one of its own needs, or
lacking a capability before any need was asked - so the module's asserts
stand on needs seen evaluated, and a refusal by capability is not a
refusal by need.

A rung with needs (or changes on the source) is reached only where both
`holds` and `refuses` are declared: one proved only on facts that make
every need hold is unreached on its other branch. A rung with no need has
only `holds`. Rungs are named `(ladder, rung)`: the top-level function or
constant in `migkit/decide.py` that builds them, and the rung's name (an
expression in angle brackets where the name is computed). A `climb` over
rungs its caller passes is counted where those rungs are built; the rungs
other modules build are owed by backlog row AUD-Z.rungs-outside.

What no test reaches yet is `OWED`, each with the test module that should
reach it (one that already names the ladder, or `new:` where none does).
The list only shrinks: an entry reached fails until it is taken out, and a
rung added unreached fails until a test reaches it."""
import ast
import functools
import pathlib

HERE = pathlib.Path(__file__).resolve().parent
DECIDE = HERE.parent / "migkit" / "decide.py"
HOLDS, REFUSES = "holds", "refuses"
HELPER = "tests.rung_branches"

#: (ladder, rung) -> the test module that should reach it on every branch
#: it has; owned by backlog row AUD-Z.rungs-owed
OWED = {
    # never pruned: `rank_priority` prices it (`costed`, `_ranked`)
    # and no `prune` sees it, so `watched()` cannot record it (W20A)
    ('_priority_cost', '<item.name>'):
        'new: tests/test_priority_cost_rungs.py',
    ('db2_statistics', 'current'):
        'tests/test_db2_takes_sequences_params_users_and_statistics_on_fakes.py',
    ('db2_statistics', 'distribution'):
        'tests/test_db2_takes_sequences_params_users_and_statistics_on_fakes.py',
    ('db2_statistics', 'table'):
        'tests/test_db2_takes_sequences_params_users_and_statistics_on_fakes.py',
    ('ddl_follow', 'events'):
        'tests/test_documentdb_preflight_reads_streams_retention_and_ddl_snapshots.py',
    ('ddl_follow', 'snapshots'):
        'tests/test_documentdb_preflight_reads_streams_retention_and_ddl_snapshots.py',
    ('dialect_bulk', 'client'):
        'tests/test_the_other_sources_are_dialects_of_the_generic_engine.py',
    ('dialect_bulk', 'copier'):
        'tests/test_the_other_sources_are_dialects_of_the_generic_engine.py',
    ('dialect_bulk', 'landing'):
        'tests/test_the_other_sources_are_dialects_of_the_generic_engine.py',
    ('dialect_bulk', 'tool'):
        'tests/test_the_other_sources_are_dialects_of_the_generic_engine.py',
    ('estate_eta', 'engine'):
        'tests/test_the_estate_is_discovered_sized_and_priced.py',
    ('estate_eta', 'none'):
        'tests/test_the_estate_is_discovered_sized_and_priced.py',
    ('estate_eta', 'pair'):
        'tests/test_the_estate_is_discovered_sized_and_priced.py',
    ('estate_price', 'catalog'):
        'new: tests/test_estate_price_rungs.py',
    ('estate_price', 'none'):
        'new: tests/test_estate_price_rungs.py',
    ('free_space_source', 'directory'):
        'tests/test_free_space_and_release_are_read_from_who_says.py',
    ('free_space_source', 'none'):
        'tests/test_free_space_and_release_are_read_from_who_says.py',
    ('free_space_source', 'provider'):
        'tests/test_free_space_and_release_are_read_from_who_says.py',
    ('free_space_source', 'server'):
        'tests/test_free_space_and_release_are_read_from_who_says.py',
    ('gauss_partition_catalogue', 'declarative'):
        'tests/test_the_wire_compatible_services_take_bulk_move_large_values_and_partitions_as_documented.py',
    ('gauss_partition_catalogue', 'own'):
        'tests/test_the_wire_compatible_services_take_bulk_move_large_values_and_partitions_as_documented.py',
    ('hide_column', 'fpe'):
        'tests/test_values_are_encrypted_in_their_own_shape_and_tokenised.py',
    ('hide_column', 'hash'):
        'tests/test_values_are_encrypted_in_their_own_shape_and_tokenised.py',
    ('hide_column', 'token'):
        'tests/test_values_are_encrypted_in_their_own_shape_and_tokenised.py',
    ('kafka_sign_in', 'AWS_MSK_IAM'):
        'tests/test_the_msk_sign_in_is_minted_and_checked_by_the_broker.py',
    ('kafka_sign_in', 'PLAIN'):
        'tests/test_the_msk_sign_in_is_minted_and_checked_by_the_broker.py',
    ('kafka_sign_in', 'SCRAM-SHA-256'):
        'tests/test_the_msk_sign_in_is_minted_and_checked_by_the_broker.py',
    ('kafka_sign_in', 'SCRAM-SHA-512'):
        'tests/test_the_msk_sign_in_is_minted_and_checked_by_the_broker.py',
    ('landing_load', 'copier'):
        'new: tests/test_landing_load_rungs.py',
    ('landing_load', 'landing'):
        'new: tests/test_landing_load_rungs.py',
    ('leftover_dump', 'keep'):
        'tests/test_values_are_encrypted_in_their_own_shape_and_tokenised.py',
    ('leftover_dump', 'remove'):
        'tests/test_values_are_encrypted_in_their_own_shape_and_tokenised.py',
    ('lift_hold', 'lift'):
        'tests/test_leftovers_are_lifted_from_the_command.py',
    ('lift_hold', 'wait'):
        'tests/test_leftovers_are_lifted_from_the_command.py',
    ('lost_link_wait', 'as long again'):
        'tests/test_a_copier_reconciles_and_retunes.py',
    ('lost_link_wait', 'at once'):
        'tests/test_a_copier_reconciles_and_retunes.py',
    ('lost_link_wait', 'measured'):
        'tests/test_a_copier_reconciles_and_retunes.py',
    ('managed_against_own', '<MANAGED_OWN_MOVE>'):
        'tests/test_a_managed_services_own_operation_is_ranked_by_its_eligibility.py',
    ('managed_against_own', "<choice.path or 'provider-operation'>"):
        'tests/test_a_managed_services_own_operation_is_ranked_by_its_eligibility.py',
    ('mongo_change_images', 'images'):
        'tests/test_mongodb_oplog_is_read_raw_and_expanded_where_the_server_has_it.py',
    ('mongo_change_images', 'looked up'):
        'tests/test_mongodb_oplog_is_read_raw_and_expanded_where_the_server_has_it.py',
    ('mpp_spread', 'even'):
        'new: tests/test_mpp_spread_rungs.py',
    ('mpp_spread', 'hash'):
        'new: tests/test_mpp_spread_rungs.py',
    ('mpp_spread', 'replicate'):
        'new: tests/test_mpp_spread_rungs.py',
    ('mssql_follow_way', 'capture'):
        'tests/test_sql_server_follows_through_change_data_capture_or_change_tracking_by_what_runs.py',
    ('mssql_follow_way', 'log'):
        'tests/test_sql_server_follows_through_change_data_capture_or_change_tracking_by_what_runs.py',
    ('mssql_follow_way', 'tracking'):
        'tests/test_sql_server_follows_through_change_data_capture_or_change_tracking_by_what_runs.py',
    ('mssql_log_way', 'live log'):
        'tests/test_sql_server_changes_are_read_from_its_log_and_its_backups.py',
    ('mssql_log_way', 'log backups'):
        'tests/test_sql_server_changes_are_read_from_its_log_and_its_backups.py',
    ('oceanbase_value_bound', "<f'v{since}'>"):
        'tests/test_the_wire_compatible_services_take_bulk_move_large_values_and_partitions_as_documented.py',
    ('oceanbase_value_bound', 'unread'):
        'tests/test_the_wire_compatible_services_take_bulk_move_large_values_and_partitions_as_documented.py',
    ('oracle_digest_rungs', 'here'):
        'new: tests/test_oracle_digest_rungs_rungs.py',
    ('oracle_digest_rungs', 'server'):
        'new: tests/test_oracle_digest_rungs_rungs.py',
    ('oracle_reader_rungs', 'logminer'):
        'new: tests/test_oracle_reader_rungs_rungs.py',
    ('oracle_reader_rungs', 'redo-files'):
        'new: tests/test_oracle_reader_rungs_rungs.py',
    ('packet_way', 'kept'):
        'tests/test_a_value_wider_than_the_packet_is_carried_or_refused_by_a_rung.py',
    ('packet_way', 'raised'):
        'tests/test_a_value_wider_than_the_packet_is_carried_or_refused_by_a_rung.py',
    ('packet_way', 'refused'):
        'tests/test_a_value_wider_than_the_packet_is_carried_or_refused_by_a_rung.py',
    ('page_rows', 'by width'):
        'tests/test_a_keyed_read_is_paged_by_what_it_measured.py',
    ('page_rows', 'measured'):
        'tests/test_a_keyed_read_is_paged_by_what_it_measured.py',
    ('page_rows', 'unmeasured'):
        'tests/test_a_keyed_read_is_paged_by_what_it_measured.py',
    ('phase_order', 'constraints-after'):
        'tests/test_every_phase_declares_its_contract.py',
    ('phase_order', 'schema-whole'):
        'tests/test_every_phase_declares_its_contract.py',
    ('phase_step', 'blocked'):
        'tests/test_every_phase_declares_its_contract.py',
    ('phase_step', 'enter'):
        'tests/test_every_phase_declares_its_contract.py',
    ('phase_step', 'wait'):
        'tests/test_every_phase_declares_its_contract.py',
    ('provider_asked', 'ask'):
        'new: tests/test_provider_asked_rungs.py',
    ('quiet_way', 'flag'):
        'tests/test_the_export_side_keeps_its_triggers.py',
    ('quiet_way', 'session'):
        'tests/test_the_export_side_keeps_its_triggers.py',
    ('quiet_way', 'untouched'):
        'tests/test_the_export_side_keeps_its_triggers.py',
    ('range_stall', 'close'):
        'tests/test_a_lost_answer_closes_only_its_session.py',
    ('range_stall', 'replay'):
        'tests/test_a_lost_answer_closes_only_its_session.py',
    ('range_stall', 'wait'):
        'tests/test_a_lost_answer_closes_only_its_session.py',
}


def _ladder(stmt):
    if isinstance(stmt, (ast.FunctionDef, ast.ClassDef)):
        return stmt.name
    if isinstance(stmt, ast.Assign):
        return ast.unparse(stmt.targets[0])
    if isinstance(stmt, ast.AnnAssign):
        return ast.unparse(stmt.target)
    return "<module>"


def _some(node):
    """A needs/changes argument that may hold something."""
    return not (isinstance(node, (ast.Tuple, ast.List)) and not node.elts)


def rungs(source=None):
    """{(ladder, rung): the branches it has} of every `Rung(` built."""
    tree = ast.parse(DECIDE.read_text() if source is None else source)
    got = {}
    for stmt in tree.body:
        ladder = _ladder(stmt)
        for node in ast.walk(stmt):
            if not (isinstance(node, ast.Call) and getattr(
                    node.func, "id", getattr(node.func, "attr", None))
                    == "Rung"):
                continue
            first = node.args[0] if node.args else next(
                (k.value for k in node.keywords if k.arg == "name"), None)
            name = first.value if isinstance(first, ast.Constant) \
                else f"<{ast.unparse(first)}>"
            asks = len(node.args) > 3 and _some(node.args[3]) or any(
                k.arg in ("needs", "changes") and _some(k.value)
                for k in node.keywords)
            had = got.get((ladder, name), (HOLDS,))
            got[(ladder, name)] = (HOLDS, REFUSES) if asks \
                or REFUSES in had else (HOLDS,)
    return got


def _declaration(tree):
    for stmt in tree.body:
        if isinstance(stmt, ast.Assign) and any(
                getattr(t, "id", None) == "PROVES_RUNGS"
                for t in stmt.targets):
            return ast.literal_eval(stmt.value)
    return None


def _watches(tree):
    imports = any(isinstance(n, ast.ImportFrom) and n.module == HELPER
                  or isinstance(n, ast.Import)
                  and any(a.name == HELPER for a in n.names)
                  for n in ast.walk(tree))
    opens = any(isinstance(n, ast.Call) and getattr(
        n.func, "id", getattr(n.func, "attr", None)) == "watched"
        for n in ast.walk(tree))
    return imports and opens


def declared(where=HERE):
    """{test module: ([(ladder, rung, branch)], it watches the needs)}."""
    got = {}
    for path in sorted(where.glob("test_*.py")):
        text = path.read_text()
        if "PROVES_RUNGS" not in text:
            continue
        tree = ast.parse(text, str(path))
        entries = _declaration(tree)
        if entries is not None:
            got[f"tests/{path.name}"] = ([tuple(e) for e in entries],
                                         _watches(tree))
    return got


def reached(built, declarations):
    """The rungs every branch of which some watching module declares."""
    seen = {}
    for entries, watches in declarations.values():
        for ladder, rung, branch in entries if watches else ():
            seen.setdefault((ladder, rung), set()).add(branch)
    return {k for k, branches in built.items()
            if set(branches) <= seen.get(k, set())}


@functools.lru_cache(maxsize=None)
def _built():
    return rungs()


@functools.lru_cache(maxsize=None)
def _declared():
    return declared()


def test_every_rung_is_reached_on_each_branch_or_owed():
    built = _built()
    left = set(built) - reached(built, _declared()) - set(OWED)
    assert not left, ("rungs no test holds and refuses: reach them in a"
                      f" watching test, or owe them: {sorted(left)}")


def test_an_owed_rung_once_reached_leaves_the_list():
    built = _built()
    done = reached(built, _declared()) & set(OWED)
    assert not done, f"reached now, take them out of OWED: {sorted(done)}"


def test_an_owed_rung_is_one_the_engine_still_builds():
    gone = set(OWED) - set(_built())
    assert not gone, f"owed rungs decide.py no longer builds: {sorted(gone)}"


def test_owed_names_the_module_that_should_reach_each():
    for key, module in OWED.items():
        assert module.startswith("new: tests/test_") or (
            HERE.parent / module).is_file(), (key, module)


def test_a_declaration_names_a_rung_and_a_branch_it_has():
    built = _built()
    for module, (entries, _) in _declared().items():
        for ladder, rung, branch in entries:
            assert (ladder, rung) in built, (module, ladder, rung)
            assert branch in built[(ladder, rung)], (module, rung, branch)


def test_a_declaring_module_watches_the_needs_it_names():
    blind = [m for m, (_, watches) in _declared().items() if not watches]
    assert not blind, ("PROVES_RUNGS without tests.rung_branches.watched():"
                       f" {blind}")


SOURCE = '''
LADDER = (Rung("asks", "a", needs=(Need("x", bool),)),
          Rung("plain", "p"), Rung("none", "n", needs=()))
def ladder(f):
    return climb("w", (Rung(f.name, "s", changes=(c,)),), f)
'''


def test_a_rung_proved_only_where_it_holds_is_still_unreached():
    built = rungs(SOURCE)
    assert built == {("LADDER", "asks"): (HOLDS, REFUSES),
                     ("LADDER", "plain"): (HOLDS,),
                     ("LADDER", "none"): (HOLDS,),
                     ("ladder", "<f.name>"): (HOLDS, REFUSES)}
    half = {"m": ([("LADDER", "asks", HOLDS), ("LADDER", "plain", HOLDS)],
                  True)}
    assert reached(built, half) == {("LADDER", "plain")}
    whole = {"m": ([("LADDER", "asks", HOLDS), ("LADDER", "asks", REFUSES)],
                   True)}
    assert reached(built, whole) == {("LADDER", "asks")}
    # a declaration with no recorder behind it reaches nothing
    assert reached(built, {"m": (whole["m"][0], False)}) == set()


def test_the_recorder_tells_a_refused_need_from_a_lacking_capability():
    from migkit import decide
    from tests.rung_branches import LACKS, REFUSES as NO, watched
    ladder = (decide.Rung("a", "a", gives=frozenset({"x"}), needs=(
                  decide.Need("y", lambda f: f.get("y")),)),
              decide.Rung("b", "b"))
    with watched() as seen:
        decide.climb("w", ladder, decide.Facts({"y": False}), prove=False)
        decide.climb("w", ladder, decide.Facts({"y": True}), need=("z",),
                     prove=False)
    assert ("a", NO, "a needs y") in seen
    assert [b for n, b, _ in seen if n == "a"][-1] == LACKS
    assert decide.prune.__module__ == "migkit.decide"
