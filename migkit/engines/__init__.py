ALIASES = {"mariadb": "mysql", "percona": "mysql", "aurora-mysql": "mysql",
           "aurora-postgres": "postgres", "alloydb": "postgres",
           "documentdb": "mongodb", "cosmosdb-mongo": "mongodb",
           "azure-sql": "mssql", "tdsql": "mysql"}


def _class_for(name):
    """The engine class for a canonical name, or None."""
    if name == "postgres":
        from .postgres import PostgresEngine
        return PostgresEngine
    if name == "mysql":
        from .mysql import MySQLEngine
        return MySQLEngine
    if name == "mongodb":
        from .mongodb import MongoEngine
        return MongoEngine
    if name == "mssql":
        from .mssql import MSSQLEngine
        return MSSQLEngine
    if name == "redis":
        from .redis import RedisEngine
        return RedisEngine
    if name == "kafka":
        from .kafka import KafkaEngine
        return KafkaEngine
    if name == "generic":
        from .generic import GenericEngine
        return GenericEngine
    if name == "sqlite":
        from .sqlite import SQLiteEngine
        return SQLiteEngine
    if name == "hetero":
        from .hetero import HeteroEngine
        return HeteroEngine
    return None


def engine_named(name, hop):
    """Build the engine for `name` on this hop, whatever the hop's own engine.

    A cross-engine hop needs one driver per side, and both hang off the same
    hop - so the lookup cannot go through `hop.engine`. One table for both
    entry points: a second copy would be one rename away from a hop that
    resolves differently depending on which of the two was asked.
    """
    canonical = ALIASES.get(name, name)
    cls = _class_for(canonical)
    if cls is None:
        raise SystemExit(f"unsupported engine {name}")
    return cls(hop)


def get_engine(hop):
    return engine_named(hop.engine, hop)
