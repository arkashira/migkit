"""A Redis 7.4 or later source into a Valkey target is said before the
move, not found as a replica's first sync empties the target (R13.redis)."""
from migkit.engines.redis import RedisEngine
from migkit.variants import Brand


def _brand(name, version):
    return Brand(name=name, family="redis", version=version)


def test_redis_74_and_later_into_valkey_is_refused_before_the_move():
    refuse = RedisEngine._snapshot_valkey_refuses
    assert "Valkey does not read" in refuse(
        _brand("redis", "7.4.1"), _brand("valkey", "8.1.10"))
    assert refuse(_brand("redis", "8.0.2"), _brand("valkey", "8.1.10"))


def test_what_valkey_reads_or_other_pairs_are_not_refused():
    refuse = RedisEngine._snapshot_valkey_refuses
    assert refuse(_brand("redis", "7.2.5"), _brand("valkey", "8.1.10")) is None
    assert refuse(_brand("valkey", "8.1.10"), _brand("valkey", "9.0.0")) is None
    assert refuse(_brand("redis", "7.4.1"), _brand("redis", "8.0.0")) is None
    assert refuse(_brand("redis", "7.4.1"), _brand("dragonfly", "1.2")) is None
