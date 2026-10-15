import json
import threading
import time
from types import SimpleNamespace

import pytest

from migkit import sizing
from migkit.engines.mysql import MySQLEngine


def reader(tmp_path, monkeypatch, fail=False):
    engine = object.__new__(MySQLEngine)
    engine.hop = SimpleNamespace(slice=1000000,
                                report_dir=lambda db: tmp_path)
    engine._pk_cols = lambda db, table: ['id']
    engine._checksum_ranges = lambda db, table, pks: ([(None, 2), (2, None)], 'id')
    engine._scope = lambda side, db, table: f'`{db}`.`{table}`'
    monkeypatch.setattr(sizing, 'estimate', lambda *a, **k: sizing.Plan(2, 4, {}))
    monkeypatch.setattr(sizing, 'resources', lambda *a, **k: {})
    state = {'busy': 0, 'maximum': 0, 'calls': []}
    lock = threading.Lock()
    data = {'src': [(1, 'a', None), (2, '\ufffd', 'b')],
            'dst': [(1, 'a', None), (2, '\ufffd', 'b'),
                    (99, '\ufffd\ufffd', '\ufffd\ufffd\ufffd')]}

    def query(side, sql):
        assert 'convert(`a`' in sql and 'convert(`b`' in sql
        assert ' where ' in sql
        with lock:
            state['busy'] += 1
            state['maximum'] = max(state['maximum'], state['busy'])
            state['calls'].append((side, sql))
        try:
            time.sleep(0.02)
            if fail and side == 'dst':
                raise OSError('interrupted read')
            low = '< 2' in sql
            rows = [r for r in data[side] if (r[0] < 2) == low]
            return [(len(rows), sum((r[1] or '').count('\ufffd') for r in rows),
                     sum((r[2] or '').count('\ufffd') for r in rows))]
        finally:
            with lock:
                state['busy'] -= 1

    engine._q = query
    return engine, state


def test_charset_fuses_columns_and_compares_complete_disjoint_ranges(tmp_path, monkeypatch):
    engine, state = reader(tmp_path, monkeypatch)
    got = engine._charset_counts('app', [('t', 'a'), ('t', 'b')])
    assert got == {('src', 't', 'a'): 1, ('src', 't', 'b'): 0,
                   ('dst', 't', 'a'): 3, ('dst', 't', 'b'): 3}
    assert len(state['calls']) == 4
    assert state['maximum'] >= 2
    progress = json.loads((tmp_path / 'deep-charset-progress.json').read_text())
    assert progress['status'] == 'complete'
    assert progress['completed'] == progress['planned'] == 4


def test_charset_failed_range_stays_incomplete(tmp_path, monkeypatch):
    engine, state = reader(tmp_path, monkeypatch, fail=True)
    with pytest.raises(OSError, match='interrupted read'):
        engine._charset_counts('app', [('t', 'a'), ('t', 'b')])
    progress = json.loads((tmp_path / 'deep-charset-progress.json').read_text())
    assert progress['status'] == 'incomplete'
    assert progress['completed'] < progress['planned']


def test_shared_read_pool_uses_adaptive_budget_and_releases_failed_slots(monkeypatch):
    from migkit import ranges
    monkeypatch.setattr(sizing, 'estimate', lambda *a, **k: sizing.Plan(2, 4, {}))
    monkeypatch.setattr(sizing, 'resources', lambda *a, **k: {})
    with ranges.ReadPool(object(), object(), 'app', 3) as pool:
        assert pool.pace.most == 3
        def fail():
            raise OSError('read failed')
        with pytest.raises(OSError, match='read failed'):
            pool.submit(fail).result()
        assert pool.submit(lambda: (7, 'digest')).result() == (7, 'digest')
        assert pool.pace.total == 7


def test_lob_merges_parallel_ranges_and_both_sides(tmp_path, monkeypatch):
    engine, state = reader(tmp_path, monkeypatch)
    engine.LOB_TYPES = ['text']
    engine._d = lambda side, db: db
    engine._scoped_rows = lambda db, rows: rows
    seen = []
    def query(side, sql, args=None):
        if 'information_schema.columns' in sql:
            return [('t', 'a'), ('t', 'b')]
        if '@@max_allowed_packet' in sql:
            return [(10000,)]
        seen.append((side, sql))
        low = '< 2' in sql
        return [(1, 5 if low else 20, 12 if low else 3)]
    engine._q = query
    engine._lob_result = lambda db, findings, *args: findings
    got = engine._lob_check('app')
    assert got == [('t', 'a', 20, 20, 10000), ('t', 'b', 12, 12, 10000)]
    assert len(seen) == 4
    assert {side for side, sql in seen} == {'src', 'dst'}


def test_nested_read_pool_respects_global_budget_and_prevents_deadlock(monkeypatch):
    from migkit import ranges
    monkeypatch.setattr(sizing, 'estimate', lambda *a, **k: sizing.Plan(2, 3, {}))
    monkeypatch.setattr(sizing, 'resources', lambda *a, **k: {})

    budget = 3
    lock = threading.Lock()
    active = 0
    peak = 0

    def work():
        nonlocal active, peak
        with lock:
            active += 1
            if active > peak:
                peak = active
        try:
            time.sleep(0.02)
            return (1, 'done')
        finally:
            with lock:
                active -= 1

    engine = SimpleNamespace(hop=SimpleNamespace(slice=1000000))
    from migkit.engines.base import Engine
    engine._read_pool = Engine._read_pool.__get__(engine)

    def outer_task(idx):
        with engine._read_pool(f'db_{idx}', workers=4) as child_pool:
            futs = [child_pool.submit(work) for _ in range(4)]
            return [f.result() for f in futs]

    with ranges.ReadPool(engine.hop, engine, 'app', budget) as outer_pool:
        futs = [outer_pool.submit(outer_task, i) for i in range(3)]
        results = [f.result() for f in futs]

    assert len(results) == 3
    for res in results:
        assert len(res) == 4
    assert peak <= budget
    assert peak >= 2


def test_nested_read_pool_multi_level_and_failure_recording(monkeypatch):
    from migkit import ranges
    monkeypatch.setattr(sizing, 'estimate', lambda *a, **k: sizing.Plan(2, 2, {}))
    monkeypatch.setattr(sizing, 'resources', lambda *a, **k: {})

    budget = 2
    lock = threading.Lock()
    active = 0
    peak = 0

    def leaf_work(fail=False):
        nonlocal active, peak
        with lock:
            active += 1
            if active > peak:
                peak = active
        try:
            time.sleep(0.01)
            if fail:
                raise ValueError("leaf failed")
            return (5, 'ok')
        finally:
            with lock:
                active -= 1

    engine = SimpleNamespace(hop=SimpleNamespace(slice=1000000))
    from migkit.engines.base import Engine
    engine._read_pool = Engine._read_pool.__get__(engine)

    def mid_task(fail=False):
        with engine._read_pool('db_mid', workers=2) as child_pool:
            def inner_task():
                with engine._read_pool('db_leaf', workers=2) as grandchild_pool:
                    f = grandchild_pool.submit(leaf_work, fail=fail)
                    return f.result()
            futs = [child_pool.submit(inner_task)]
            return futs[0].result()

    with ranges.ReadPool(engine.hop, engine, 'app', budget) as outer_pool:
        res = outer_pool.submit(mid_task, fail=False).result()
        assert res == (5, 'ok')
        assert outer_pool.pace.total == 5

        with pytest.raises(ValueError, match="leaf failed"):
            outer_pool.submit(mid_task, fail=True).result()
        assert outer_pool.pace.total == 5
        assert outer_pool.pace._n >= 2

        res2 = outer_pool.submit(mid_task, fail=False).result()
        assert res2 == (5, 'ok')
        assert outer_pool.pace.total == 10

    assert peak <= budget


def test_engine_read_pool_links_ambient_and_coordinator(monkeypatch):
    from migkit import ranges
    from migkit.engines.base import Engine
    monkeypatch.setattr(sizing, 'estimate', lambda *a, **k: sizing.Plan(2, 4, {}))
    monkeypatch.setattr(sizing, 'resources', lambda *a, **k: {})

    hop = SimpleNamespace(slice=1000000)
    engine = SimpleNamespace(hop=hop)
    engine._read_pool = Engine._read_pool.__get__(engine)

    with ranges.ReadPool(hop, engine, 'app', 2) as ambient:
        child = engine._read_pool('app', 6)
        try:
            assert child.parent is ambient
            assert child.slots.workers <= 2
            assert child.slots.root is ambient.slots
        finally:
            child.shutdown()


def test_nested_slots_deadlock_freedom_under_saturation():
    from migkit import ranges

    root = ranges.Slots(2)
    child1 = ranges.Slots(2, parent=root)
    child2 = ranges.Slots(2, parent=root)

    order = []
    lock = threading.Lock()

    def run_sub(name):
        with lock:
            order.append(f"{name}_start")
        time.sleep(0.02)
        with lock:
            order.append(f"{name}_end")
        return (1, name)

    def outer(child, name):
        lease = root._take()
        tok = ranges._active_lease_var.set(lease)
        try:
            child.each([f"{name}_1", f"{name}_2"], run_sub)
        finally:
            ranges._active_lease_var.reset(tok)
            root._give(lease)

    o1 = threading.Thread(target=outer, args=(child1, "o1"))
    o2 = threading.Thread(target=outer, args=(child2, "o2"))
    o1.start()
    o2.start()
    o1.join(timeout=2.0)
    o2.join(timeout=2.0)

    assert not o1.is_alive()
    assert not o2.is_alive()
    assert len(order) == 8
    assert root._busy == 0
    assert child1._busy == 0
    assert child2._busy == 0


