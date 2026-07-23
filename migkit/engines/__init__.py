ALIASES = {"mariadb": "mysql", "percona": "mysql", "aurora-mysql": "mysql",
           "aurora-postgres": "postgres", "alloydb": "postgres",
           "documentdb": "mongodb", "cosmosdb-mongo": "mongodb",
           "azure-sql": "mssql", "tdsql": "mysql"}


def get_engine(hop):
    name = ALIASES.get(hop.engine, hop.engine)
    if name == "postgres":
        from .postgres import PostgresEngine
        return PostgresEngine(hop)
    if name == "mysql":
        from .mysql import MySQLEngine
        return MySQLEngine(hop)
    if name == "mongodb":
        from .mongodb import MongoEngine
        return MongoEngine(hop)
    if name == "mssql":
        from .mssql import MSSQLEngine
        return MSSQLEngine(hop)
    if name == "redis":
        from .redis import RedisEngine
        return RedisEngine(hop)
    if name == "kafka":
        from .kafka import KafkaEngine
        return KafkaEngine(hop)
    if name == "generic":
        from .generic import GenericEngine
        return GenericEngine(hop)
    if name == "sqlite":
        from .sqlite import SQLiteEngine
        return SQLiteEngine(hop)
    if name == "hetero":
        from .hetero import HeteroEngine
        return HeteroEngine(hop)
    raise SystemExit(f"unsupported engine {name}")
