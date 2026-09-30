"""An edition change: the same engine, sold or built as another edition
(backlog P0-SCENARIOS - "SQL Server Web edition to Datacenter
(Enterprise)", Express, Standard, Developer; Oracle SE2 and EE; MySQL
Community and Enterprise; MongoDB Community and Enterprise).

Each side's edition is read from its own server (`read`) - SQL Server's
`SERVERPROPERTY('Edition')` and `('EngineEdition')`, Oracle's `v$version`
banner, MySQL's `@@license` and `@@version_comment`, MongoDB's `buildInfo`
modules - never from an address. Then, before anything is written:

* **what the source's databases use that the target's edition lacks**
  (`used`, `FEATURES`): the features SQL Server itself records a database
  as holding (`sys.dm_db_persisted_sku_features`), Oracle's own catalogue
  of partitioned, compressed, encrypted and in-memory tables, the
  enterprise plugins and components a MySQL server runs and what uses
  them, a MongoDB server's own configuration and users. Each has a fate
  on the target: *stripped* - the logical copy makes the tables without
  it and the rows arrive the same (data compression, partitioning:
  migkit makes a table from its columns and key, never its storage);
  *behaviour* - something around the data works otherwise there (a
  thread pool); *lost* - a protection or a feature the application
  relies on is not there (encryption at rest, auditing, change data
  capture), refused unless the hop accepts it (`options: {edition:
  {accept: [...]}}`); *refused* - what uses it cannot be made there;
* **the target edition's caps against the source** (`CAPS`): a database
  larger than Express takes, a source using more memory or cores than
  the target's edition will;
* **the way** (`physical`, `backupseed`): a copy of the pages - SQL
  Server's own backup restored, MySQL's clone - is a rung only where the
  target's edition takes what the pages hold, and the logical copy
  otherwise; `goal: upgrade` takes an edition up as an upgrade
  (`upgrade`);
* **what it is said as**: an edition down is a licence-driven move, and
  the plan says so, with what the target's edition leaves behind.

The catalogue is from each vendor's own pages, as of 2026-10:

* SQL Server - Microsoft Learn: "Editions and supported features of SQL
  Server 2016/2017/2019/2022/2025", "Compute capacity limits by edition of
  SQL Server", "SQL Server 2016 Service Pack 1 release notes" (the
  programmability surface - compression, partitioning, columnstore,
  in-memory OLTP, multiple FILESTREAM containers - in every edition from
  13.0.4001), "sys.dm_db_persisted_sku_features";
* Oracle - "Database Licensing Information User Manual" 19c, 21c and 23ai
  (the options only Enterprise Edition carries; Standard Edition 2 on at
  most 16 CPU threads), "Oracle Database Express Edition Licensing
  Information" 11g, 18c and 21c, "Oracle Database 23ai Free";
* MySQL - "MySQL 8.4 Reference Manual": Enterprise Audit, Thread Pool,
  Enterprise Firewall, Data Masking, Enterprise Encryption, the keyring
  plugins and components, the pluggable authentication of LDAP, Kerberos,
  PAM, Windows and WebAuthn, and the `license` system variable; Percona
  Server's own audit, masking and thread pool;
* MongoDB - "MongoDB Enterprise features" in the manual: the encrypted
  storage engine, auditing, LDAP and Kerberos authentication, log
  redaction, the in-memory storage engine; Percona Server for MongoDB's
  own versions of each.
"""
import re
from dataclasses import dataclass, field

SCOPE = "edition"

#: what a feature the target's edition lacks means for the move
STRIPPED, BEHAVIOUR, LOST, REFUSED = ("stripped", "behaviour", "lost",
                                      "refused")

#: how each engine's edition is said
ENGINE_WORDS = {"mssql": "SQL Server", "oracle": "Oracle Database",
                "mysql": "MySQL", "mongodb": "MongoDB", "redis": "Redis"}
#: an edition's name as its vendor writes it, where it is not the name
#: with capitals
TITLES = {"ee": "Enterprise Edition", "se2": "Standard Edition 2",
          "se": "Standard Edition", "xe": "Express Edition",
          "free": "Free", "personal": "Personal Edition",
          "azure sql database": "(Azure SQL Database)",
          "managed instance": "(Azure SQL Managed Instance)",
          "edge": "Edge"}


def _row(level, item, detail, fix=""):
    return {"level": level, "scope": SCOPE, "item": item,
            "detail": detail + (f"  [fix] {fix}" if fix and level != "pass"
                                else "")}


def _last(e):
    return (str(e).strip().splitlines() or [type(e).__name__])[-1][:160]


def _side(eng, side):
    return getattr(eng, "src_engine" if side == "src" else "dst_engine",
                   None) or eng


def _version(text):
    from .lacks import numbers
    return numbers(text, 3)


@dataclass(frozen=True)
class Edition:
    """One side's edition as its server says it. `rank` orders an engine's
    editions by what they carry - None where it is not known; `licence`
    is what the edition may be used for, where that is narrower than
    production."""
    engine: str
    name: str = "unknown"
    rank: int = None
    said: str = ""
    version: tuple = None
    evidence: str = ""
    licence: str = ""
    extra: dict = field(default_factory=dict)

    @property
    def known(self):
        return self.name != "unknown"

    def label(self):
        # a fork's editions are said under its own name: MariaDB
        # Community, not MySQL's
        engine = {"mariadb": "MariaDB"}.get(
            (self.extra or {}).get("brand"),
            ENGINE_WORDS.get(self.engine, self.engine))
        if not self.known:
            return f"{engine} of an edition not read"
        if self.name == "percona server":
            return ("Percona Server for MongoDB" if self.engine == "mongodb"
                    else "Percona Server")
        name = TITLES.get(self.name) or (self.name.title() if
                                         self.engine != "redis"
                                         else self.name)
        return f"{engine} {name}"


# --- each side's edition, from its own server --------------------------------------

#: SQL Server's editions, by what they carry; Developer and Evaluation
#: carry Enterprise's features and are not licensed for production
MS_RANKS = {"express": 0, "web": 1, "standard": 2,
            "business intelligence": 2, "enterprise": 3, "developer": 3,
            "evaluation": 3, "azure sql database": 3,
            "managed instance": 3}
MS_LICENCE = {"developer": "development and test only, never production",
              "evaluation": "an evaluation of 180 days",
              "web": "public web sites and services only, through a"
                     " hosting provider",
              "express": "free, within its caps"}
#: the editions that carry every feature Enterprise does
ENTERPRISE_CLASS = ("enterprise", "developer", "evaluation",
                    "azure sql database", "managed instance")


def ms_name(text, engine_edition):
    """SQL Server's edition in migkit's words, from `SERVERPROPERTY
    ('Edition')` and `('EngineEdition')`: 4 is Express, 2 Standard or Web,
    3 Enterprise, Developer or Evaluation, 5 Azure SQL Database, 8 a
    managed instance, 9 SQL Edge."""
    t = str(text or "").lower()
    try:
        n = int(engine_edition)
    except (TypeError, ValueError):
        n = None
    if n == 4 or "express" in t:
        return "express"
    if n == 9 or "edge" in t:
        return "edge"
    if n == 5 or "sql azure" in t:
        return "azure sql database"
    if n == 8:
        return "managed instance"
    for word in ("web", "developer", "evaluation", "business intelligence",
                 "enterprise", "standard"):
        if word in t:
            return word
    return "unknown"


def _ms_read(eng, side):
    e = _side(eng, side)
    text, n, ver = e._rows(side, "master",
                           "select cast(serverproperty('Edition') as"
                           " nvarchar(128)), cast(serverproperty("
                           "'EngineEdition') as int), cast(serverproperty("
                           "'ProductVersion') as nvarchar(128))")[0]
    name = ms_name(text, n)
    return Edition("mssql", name, MS_RANKS.get(name), str(text or ""),
                   _version(ver), f"SERVERPROPERTY('Edition') = {text}",
                   MS_LICENCE.get(name, ""), {"engine_edition": n})


#: Oracle's editions: Express and Free carry most of Enterprise's options
#: from 18c, within their caps
ORA_RANKS = {"xe": 0, "free": 0, "se": 1, "se2": 1, "ee": 2,
             "personal": 2}
ORA_LICENCE = {"xe": "free, within its caps", "free": "free, within its"
                                                      " caps",
               "personal": "one user only"}


def ora_name(banner):
    """Oracle's edition from its `v$version` banner."""
    t = str(banner or "").lower()
    if "enterprise edition" in t:
        return "ee"
    if "standard edition 2" in t or "standard edition two" in t:
        return "se2"
    if "standard edition" in t:
        return "se"
    if "express edition" in t:
        return "xe"
    if "personal" in t:
        return "personal"
    if re.search(r"\bfree\b", t):
        return "free"
    return "unknown"


def _ora_read(eng, side, db):
    e = _side(eng, side)
    try:
        rows = e._rows(side, db, "select banner from v$version")
    except Exception:  # noqa: BLE001 - its public counterpart
        rows = e._rows(side, db, "select product || ' ' || version from"
                                 " product_component_version")
    banner = " ".join(str(r[0]) for r in rows if r and r[0])
    name = ora_name(banner)
    got = re.search(r"Release (\d+(?:\.\d+)*)", banner) or re.search(
        r"(\d+(?:\.\d+)+)", banner)
    return Edition("oracle", name, ORA_RANKS.get(name), banner[:120],
                   _version(got.group(1) if got else ""),
                   f"v$version: {banner[:80]}", ORA_LICENCE.get(name, ""))


def _my_read(eng, side):
    from .upgrade import _my
    version, comment = _my(eng, side, "select version(),"
                                      " @@version_comment")[0]
    try:
        licence = str(_my(eng, side, "select @@license")[0][0] or "")
    except Exception:  # noqa: BLE001 - a server without the variable
        licence = ""
    text = f"{version} {comment}".lower()
    brand = _side(eng, side)._brands()[0 if side == "src" else 1].name
    if brand == "mariadb":
        name = "enterprise" if "enterprise" in text else "community"
    elif brand == "percona":
        name = "percona server"
    elif licence.lower() == "commercial" or "enterprise" in text \
            or "-commercial" in text:
        name = "enterprise"
    elif licence or "community" in text or "gpl" in text:
        name = "community"
    else:
        name = "unknown"
    plugins, components = _my_plugins(eng, side)
    return Edition("mysql", name, {"community": 0, "percona server": 0,
                                   "enterprise": 1}.get(name),
                   str(comment or ""), _version(version),
                   f"@@license = {licence or '?'}, @@version_comment ="
                   f" {comment}", "", {"brand": brand,
                                       "plugins": plugins,
                                       "components": components})


def _my_plugins(eng, side):
    """(active plugins, components), lower-cased."""
    from .upgrade import _my
    plugins = {str(n).lower() for (n,) in _my(
        eng, side, "select plugin_name from information_schema.plugins"
                   " where plugin_status = 'ACTIVE'")}
    try:
        comps = {str(u).lower().rsplit("/", 1)[-1] for (u,) in _my(
            eng, side, "select component_urn from mysql.component")}
    except Exception:  # noqa: BLE001 - before 8.0, or not allowed to read
        comps = set()
    return sorted(plugins), sorted(comps)


def _mongo_read(eng, side):
    e = _side(eng, side)
    info = dict(e._client(side).admin.command("buildInfo"))
    modules = [str(m).lower() for m in info.get("modules") or []]
    if "psmdbVersion" in info:
        name = "percona server"
    elif "enterprise" in modules:
        name = "enterprise"
    elif info.get("gitVersion"):
        name = "community"
    else:
        name = "unknown"
    return Edition("mongodb", name, {"community": 0, "percona server": 0,
                                     "enterprise": 1}.get(name),
                   ", ".join(modules) or "no modules",
                   _version(info.get("version")),
                   f"buildInfo modules = {modules}", "",
                   {"modules": modules})


def _redis_read(eng, side, db):
    from .lacks import _modules
    e = _side(eng, side)
    client = e._client(side, db or 0)
    info = client.info("server")
    try:
        mods = _modules(client)
    except Exception:  # noqa: BLE001 - a server without modules
        mods = {}
    name = ("with modules " + ", ".join(sorted(mods))) if mods \
        else "open source"
    return Edition("redis", name, None, name,
                   _version(info.get("redis_version")),
                   f"MODULE LIST = {sorted(mods)}", "", {"modules": mods})


def read(eng, side, db=None):
    """The `Edition` of one side, as its server says it; an Edition named
    "unknown" where it would not say, with why in its evidence."""
    engine = getattr(_side(eng, side), "CANON_ENGINE", "") or ""
    try:
        if engine == "mssql":
            return _ms_read(eng, side)
        if engine == "oracle":
            return _ora_read(eng, side, db)
        if engine == "mysql":
            return _my_read(eng, side)
        if engine == "mongodb":
            return _mongo_read(eng, side)
        if engine == "redis":
            return _redis_read(eng, side, db)
    except Exception as e:  # noqa: BLE001 - said as not read
        return Edition(engine, evidence=f"could not be read: {_last(e)}")
    return None


def direction(src, dst):
    """How the edition changes, in one word: up, down, same, another (the
    same rank, another name), or None where either is not known."""
    if src is None or dst is None or not src.known or not dst.known:
        return None
    if src.name == dst.name:
        return "same"
    if src.rank is None or dst.rank is None:
        return "another"
    if dst.rank > src.rank:
        return "up"
    if dst.rank < src.rank:
        return "down"
    return "another"


# --- the features, and which editions have each ---------------------------------------

@dataclass(frozen=True)
class Feature:
    """One feature an edition may lack: in words, its fate on a target
    without it, which editions have it (`has(edition)` -> True, False or
    None where it is not known) and what to do."""
    key: str
    said: str
    fate: str
    has: object
    fix: str = ""


#: SQL Server 2016 SP1, from which the programmability surface is in
#: every edition
SP1 = (13, 0, 4001)


def _ms_all_from_sp1(ed):
    if ed.name in ENTERPRISE_CLASS:
        return True
    if ed.name in ("edge", "unknown") or ed.version is None:
        return None
    return ed.version >= SP1


def _ms_cdc(ed):
    if ed.name in ENTERPRISE_CLASS:
        return True
    if ed.name == "standard":
        return None if ed.version is None else ed.version >= SP1
    if ed.name in ("web", "express", "business intelligence"):
        return False
    return None


def _ms_tde(ed):
    if ed.name in ENTERPRISE_CLASS:
        return True
    if ed.name == "standard":
        return None if ed.version is None else ed.version >= (15, 0, 0)
    if ed.name in ("web", "express", "business intelligence"):
        return False
    return None


_STRIP_FIX = ("nothing to do for a logical copy, which makes each table"
              " from its columns and key; turn it on again on the target"
              " where its edition has it")

#: SQL Server: `sys.dm_db_persisted_sku_features`' names
MS_FEATURES = {f.key: f for f in (
    Feature("Compression", "data compression", STRIPPED, _ms_all_from_sp1,
            _STRIP_FIX),
    Feature("Partitioning", "table partitioning", STRIPPED,
            _ms_all_from_sp1, _STRIP_FIX),
    Feature("ColumnStoreIndex", "columnstore indexes", STRIPPED,
            _ms_all_from_sp1, _STRIP_FIX),
    Feature("InMemoryOLTP", "memory-optimized tables", STRIPPED,
            _ms_all_from_sp1, _STRIP_FIX),
    Feature("MultipleFSContainers", "more than one FILESTREAM container",
            STRIPPED, _ms_all_from_sp1, _STRIP_FIX),
    Feature("ChangeCapture", "change data capture", LOST, _ms_cdc,
            "a target edition with change data capture (Standard from 2016"
            " SP1, or Enterprise) where something reads its change tables,"
            " or the loss accepted"),
    Feature("TransparentDataEncryption", "transparent data encryption",
            LOST, _ms_tde,
            "a target edition with it (Standard from 2019, or Enterprise),"
            " or the target's volume encrypted and the loss accepted"),
)}


def _ora_ee(ed):
    """An Enterprise Edition feature or option: Express from 18c and Free
    carry them within their caps; Standard Edition 2 does not."""
    if ed.name in ("ee", "personal"):
        return True
    if ed.name in ("se", "se2"):
        return False
    if ed.name in ("xe", "free"):
        return None if ed.version is None else ed.version >= (18, 0, 0)
    return None


ORA_FEATURES = {f.key: f for f in (
    Feature("partitioning", "partitioned tables", STRIPPED, _ora_ee,
            _STRIP_FIX),
    Feature("compression", "compressed tables", STRIPPED, _ora_ee,
            _STRIP_FIX),
    Feature("securefile", "LOBs compressed or deduplicated", STRIPPED,
            _ora_ee, _STRIP_FIX),
    Feature("in-memory", "tables kept in the in-memory column store",
            STRIPPED, _ora_ee, _STRIP_FIX),
    Feature("bitmap", "bitmap indexes", STRIPPED, _ora_ee,
            "nothing to do: the target's indexes are ordinary ones"),
    Feature("tde", "tables in encrypted tablespaces, or encrypted columns",
            LOST, _ora_ee,
            "a target edition with Advanced Security, or its storage"
            " encrypted and the loss accepted"),
    Feature("label security", "tables under Label Security policies",
            REFUSED, _ora_ee,
            "a target edition with Label Security: the rows each user sees"
            " are its policies', and nothing else carries them"),
    Feature("vault", "objects inside Database Vault realms", LOST, _ora_ee,
            "a target edition with Database Vault, or the loss accepted"),
)}


def _brand_is(ed, *names):
    return (ed.extra or {}).get("brand") in names


def _my_enterprise(percona=False, mariadb=False):
    """A MySQL Enterprise feature: Enterprise has it; Percona Server and
    MariaDB where their own version of it is named."""
    def has(ed):
        if ed.name == "enterprise":
            return True
        if ed.name == "percona server" or _brand_is(ed, "percona"):
            return percona
        if _brand_is(ed, "mariadb"):
            return mariadb
        if ed.name == "community":
            return False
        return None
    return has


#: MySQL: {key: (the plugins and components that are it, the Feature)}
MY_FEATURES = {
    "audit": (("audit_log", "component_audit_log"), Feature(
        "audit", "auditing of the server's activity (audit_log)", LOST,
        _my_enterprise(percona=True),
        "an audit plugin on the target (Percona Server's audit_log;"
        " MariaDB's server_audit audits otherwise), or the loss accepted")),
    "thread pool": (("thread_pool",), Feature(
        "thread pool", "the thread pool", BEHAVIOUR,
        _my_enterprise(percona=True, mariadb=True),
        "a pooler in front of the target, or its own connection handling"
        " measured under the application's load")),
    "firewall": (("mysql_firewall", "component_mysql_firewall"), Feature(
        "firewall", "the statement firewall", LOST, _my_enterprise(),
        "a firewall in front of the target, or the loss accepted")),
    "masking": (("data_masking", "component_masking"), Feature(
        "masking", "data masking functions", BEHAVIOUR,
        _my_enterprise(percona=True),
        "the application's statements that call them changed")),
    "encryption functions": (("openssl_udf",
                              "component_enterprise_encryption"), Feature(
        "encryption functions", "the enterprise encryption functions",
        BEHAVIOUR, _my_enterprise(),
        "the application's statements that call them changed")),
    "telemetry": (("component_telemetry",), Feature(
        "telemetry", "the server's telemetry", BEHAVIOUR, _my_enterprise(),
        "the target's own monitoring")),
}

#: sign-in plugins only an Enterprise server has; Percona Server has its
#: own LDAP ones
MY_AUTH = {"authentication_ldap_sasl": True, "authentication_ldap_simple":
           True, "authentication_kerberos": False,
           "authentication_windows": False, "authentication_pam": False,
           "authentication_fido": False, "authentication_webauthn": False,
           "authentication_oci": False}

#: the functions each plugin gives, as code calls them
MY_CALLS = {
    "masking": r"\b(mask_(inner|outer|pan|pan_relaxed|ssn|uk_nin|uuid|iban"
               r"|canada_sin)|gen_(range|rnd_email|rnd_pan|rnd_ssn|rnd_us_"
               r"phone|dictionary|blocklist|blacklist))\s*\(",
    "encryption functions": r"\b(asymmetric_(encrypt|decrypt|sign|verify|"
                            r"derive)|create_asymmetric_(priv|pub)_key|"
                            r"create_dh_parameters|create_digest)\s*\(",
}


def _mongo_enterprise(ed):
    if ed.name in ("enterprise", "percona server"):
        return True
    if ed.name == "community":
        return False
    return None


MONGO_FEATURES = {f.key: f for f in (
    Feature("encryption at rest", "the encrypted storage engine", LOST,
            _mongo_enterprise,
            "the target's volume encrypted and the loss accepted, or an"
            " edition with it"),
    Feature("auditing", "auditing", LOST, _mongo_enterprise,
            "an edition with it, or the loss accepted"),
    Feature("ldap", "users signing in through LDAP", LOST,
            _mongo_enterprise,
            "each user made with a password on the target, or an edition"
            " with it"),
    Feature("kerberos", "users signing in through Kerberos", LOST,
            _mongo_enterprise,
            "each user made with a password on the target, or an edition"
            " with it"),
    Feature("log redaction", "log redaction", LOST, _mongo_enterprise,
            "an edition with it, or the loss accepted"),
    Feature("in-memory", "the in-memory storage engine", STRIPPED,
            _mongo_enterprise,
            "nothing to do: the target keeps the same documents on disk"),
)}


# --- what the source uses ---------------------------------------------------------

@dataclass(frozen=True)
class Use:
    """A feature the source uses, and where."""
    key: str
    where: tuple = ()


def _ms_used(eng, db):
    e = _side(eng, "src")
    return [Use(str(n), (db,)) for (n,) in e._rows(
        "src", db, "select feature_name from"
                   " sys.dm_db_persisted_sku_features")]


def _ora_q(eng, db, sql, missing_ok=False):
    """Names the source's catalogue gives for the schema `db`; a view a
    server without the option does not have (ORA-00942) is nothing used,
    where `missing_ok`."""
    from .engines.dbapi import MARK
    e = _side(eng, "src")
    owner = e._owner("src", db)
    try:
        return [str(r[0]) for r in e._rows(
            "src", db, sql.replace(":owner", MARK),
            (owner,) * sql.count(":owner")) if r and r[0]]
    except Exception as ex:  # noqa: BLE001 - absent view, or no right
        if missing_ok and "ORA-00942" in str(ex):
            return []
        raise


ORA_USED = {
    "partitioning": ("select table_name from all_part_tables where owner ="
                     " :owner", False),
    "compression": ("select table_name from all_tables where owner = :owner"
                    " and compression = 'ENABLED' union select table_name"
                    " || ' (' || partition_name || ')' from"
                    " all_tab_partitions where table_owner = :owner and"
                    " compression = 'ENABLED'", False),
    "securefile": ("select table_name || '.' || column_name from all_lobs"
                   " where owner = :owner and (compression not in ('NO',"
                   " 'NONE') or deduplication not in ('NO', 'NONE'))",
                   False),
    "in-memory": ("select table_name from all_tables where owner = :owner"
                  " and inmemory = 'ENABLED'", True),
    "bitmap": ("select index_name from all_indexes where owner = :owner and"
               " index_type like '%BITMAP%'", False),
    "tde": ("select t.table_name from all_tables t join dba_tablespaces s"
            " on s.tablespace_name = t.tablespace_name where t.owner ="
            " :owner and s.encrypted = 'YES' union select table_name || '.'"
            " || column_name from all_encrypted_columns where owner ="
            " :owner", False),
    "label security": ("select table_name from all_sa_table_policies where"
                       " schema_name = :owner", True),
    "vault": ("select object_name from dvsys.dba_dv_realm_object where"
              " owner = :owner", True),
}


def _ora_used(eng, db):
    out = []
    for key, (sql, missing_ok) in ORA_USED.items():
        try:
            got = _ora_q(eng, db, sql, missing_ok)
        except Exception as e:  # noqa: BLE001 - not known: said
            out.append(Use(key, (f"?{_last(e)}",)))
            continue
        if got:
            out.append(Use(key, tuple(got)))
    return out


def _my_used(eng, db, src_ed):
    """The enterprise plugins and components the source runs, with what
    uses them; sign-in plugins with their users."""
    from .lacks import _my_code
    from .upgrade import _my
    extra = src_ed.extra or {}
    on = set(extra.get("plugins") or ()) | set(extra.get("components") or ())
    out = []
    for key, (names, _) in MY_FEATURES.items():
        if not on & set(names):
            continue
        where = ("(the server)",)
        if key in MY_CALLS:
            try:
                calls = _my_code(MY_CALLS[key])(eng, db)
            except Exception:  # noqa: BLE001 - the server's own is enough
                calls = []
            if calls:
                where = tuple(calls)
        out.append(Use(key, where))
    auth = [str(p).lower() for p in MY_AUTH]
    marks = ", ".join(["%s"] * len(auth))
    try:
        rows = _my(eng, "src", f"select concat(user, '@', host), plugin from"
                               f" mysql.user where plugin in ({marks})",
                   tuple(auth))
    except Exception:  # noqa: BLE001 - not allowed to read the users
        rows = []
    by = {}
    for user, plugin in rows:
        by.setdefault(str(plugin).lower(), []).append(str(user))
    for plugin, users in sorted(by.items()):
        out.append(Use(f"sign-in {plugin}", tuple(users)))
    return out


def _mongo_used(eng):
    e = _side(eng, "src")
    client = e._client("src")
    out = []
    try:
        parsed = dict(client.admin.command("getCmdLineOpts").get(
            "parsed") or {})
    except Exception as ex:  # noqa: BLE001 - not allowed to ask
        return [Use("?", (f"?{_last(ex)}",))]
    security = dict(parsed.get("security") or {})
    if security.get("enableEncryption"):
        out.append(Use("encryption at rest", ("(the server)",)))
    if parsed.get("auditLog"):
        out.append(Use("auditing", ("(the server)",)))
    if security.get("redactClientLogData"):
        out.append(Use("log redaction", ("(the server)",)))
    engine = str((parsed.get("storage") or {}).get("engine") or "")
    if engine == "inMemory":
        out.append(Use("in-memory", ("(the server)",)))
    try:
        users = client["$external"].command("usersInfo").get("users") or []
    except Exception:  # noqa: BLE001 - none, or not allowed to read them
        users = []
    for mech, key in (("PLAIN", "ldap"), ("GSSAPI", "kerberos")):
        named = [str(u.get("user")) for u in users
                 if mech in (u.get("mechanisms") or [])]
        if named:
            out.append(Use(key, tuple(named)))
    return out


def feature(engine, key):
    """The `Feature` of `engine` named `key`, or None."""
    if engine == "mssql":
        return MS_FEATURES.get(key)
    if engine == "oracle":
        return ORA_FEATURES.get(key)
    if engine == "mongodb":
        return MONGO_FEATURES.get(key)
    if engine == "mysql":
        if key.startswith("sign-in "):
            plugin = key[len("sign-in "):]
            return Feature(key, f"users signing in with {plugin}", LOST,
                           _my_auth(plugin),
                           f"{plugin} put on the target, or each user given"
                           " another way to sign in there")
        got = MY_FEATURES.get(key)
        return got[1] if got else None
    return None


def _my_auth(plugin):
    percona = MY_AUTH.get(plugin, False)
    return _my_enterprise(percona=percona)


def used(eng, db, src_ed):
    """[Use] of the features `db` on the source uses that an edition may
    lack."""
    engine = src_ed.engine
    if engine == "mssql":
        return _ms_used(eng, db)
    if engine == "oracle":
        return _ora_used(eng, db)
    if engine == "mysql":
        return _my_used(eng, db, src_ed)
    if engine == "mongodb":
        return _mongo_used(eng)
    return []


def _target_runs(eng, key, dst_ed):
    """Whether the target itself runs what `key` is, read on it: the same
    plugin or component on a MySQL target, the same setting on a MongoDB
    one - then its edition does not matter."""
    extra = dst_ed.extra or {}
    if dst_ed.engine == "mysql":
        on = set(extra.get("plugins") or ()) | set(extra.get("components")
                                                   or ())
        if key.startswith("sign-in "):
            return key[len("sign-in "):] in on
        names = (MY_FEATURES.get(key) or ((), None))[0]
        return bool(on & set(names))
    return False


# --- the caps ------------------------------------------------------------------------

GB = 1024 ** 3
MB = 1024 ** 2

#: SQL Server's caps by edition, the first whose major version is at or
#: below the target's: a database's data files (Express), the buffer pool,
#: the cores one instance uses
MS_CAPS = {
    "express": [((17,), {"database": 50 * GB, "memory": 1410 * MB,
                         "cores": 4}),
                ((10, 50), {"database": 10 * GB, "memory": 1410 * MB,
                            "cores": 4})],
    "web": [((13,), {"memory": 64 * GB, "cores": 16}),
            ((11,), {"memory": 64 * GB, "cores": 16})],
    "standard": [((17,), {"memory": 256 * GB, "cores": 32}),
                 ((13,), {"memory": 128 * GB, "cores": 24}),
                 ((12,), {"memory": 128 * GB, "cores": 16}),
                 ((11,), {"memory": 64 * GB, "cores": 16})],
}

#: Oracle's: the user data Express and Free hold, the memory and the CPU
#: threads they use; Standard Edition 2's threads
ORA_CAPS = {
    "xe": [((18,), {"database": 12 * GB, "memory": 2 * GB, "cores": 2}),
           ((11,), {"database": 11 * GB, "memory": 1 * GB, "cores": 1})],
    "free": [((23,), {"database": 12 * GB, "memory": 2 * GB, "cores": 2})],
    "se2": [((12, 1), {"cores": 16})],
}


def caps(ed):
    """{"database", "memory", "cores"} the edition `ed` holds at most, as
    far as the catalogue says; {} where it caps nothing it knows."""
    table = {"mssql": MS_CAPS, "oracle": ORA_CAPS}.get(ed.engine, {})
    rows = table.get(ed.name) or []
    if not rows:
        return {}
    from .lacks import _pad
    v = ed.version
    for at, got in rows:
        if v is None or _pad(v) >= _pad(at):
            return dict(got)
    return dict(rows[-1][1])


def _ms_load(eng, db):
    e = _side(eng, "src")
    used_b, alloc = e._rows("src", db, "select sum(cast(fileproperty(name,"
                                       " 'SpaceUsed') as bigint)) * 8192,"
                                       " sum(cast(size as bigint)) * 8192"
                                       " from sys.database_files where type"
                                       " = 0")[0]
    out = {"database": int(used_b or 0), "allocated": int(alloc or 0)}
    try:
        out["memory"] = int(e._rows(
            "src", db, "select cast(cntr_value as bigint) * 8192 from"
                       " sys.dm_os_performance_counters where counter_name ="
                       " 'Database pages' and object_name like '%Buffer"
                       " Manager%'")[0][0])
        out["cores"] = int(e._rows(
            "src", db, "select count(*) from sys.dm_os_schedulers where"
                       " status = 'VISIBLE ONLINE'")[0][0])
    except Exception:  # noqa: BLE001 - VIEW SERVER STATE is not given
        pass
    return out


def _ora_load(eng, db):
    out = {}
    try:
        got = _ora_q(eng, db, "select to_char(sum(bytes)) from dba_segments"
                              " where owner = :owner")
        out["database"] = int(got[0]) if got else 0
    except Exception:  # noqa: BLE001 - not allowed to read it
        pass
    e = _side(eng, "src")
    try:
        rows = dict((str(n), str(v)) for n, v in e._rows(
            "src", db, "select name, value from v$parameter where name in"
                       " ('cpu_count', 'sga_target', 'pga_aggregate_target')"))
        out["cores"] = int(rows.get("cpu_count") or 0) or None
        out["memory"] = (int(rows.get("sga_target") or 0)
                         + int(rows.get("pga_aggregate_target") or 0)) or None
    except Exception:  # noqa: BLE001 - not allowed to read it
        pass
    return {k: v for k, v in out.items() if v is not None}


def load(eng, db, engine):
    """What the source holds and uses that an edition caps."""
    if engine == "mssql":
        return _ms_load(eng, db)
    if engine == "oracle":
        return _ora_load(eng, db)
    return {}


# --- the checks ------------------------------------------------------------------------

def accepted(hop):
    """The features whose loss the hop accepts, as it names them."""
    got = (hop.options or {}).get("edition")
    names = got.get("accept") if isinstance(got, dict) else None
    if isinstance(names, str):
        names = [names]
    return {str(n).lower() for n in names or ()}


def pair(eng, db=None):
    """(the source's Edition, the target's), read once for this engine."""
    cache = eng.__dict__.setdefault("_editions", {})
    if "pair" not in cache:
        cache["pair"] = (read(eng, "src", db), read(eng, "dst", db))
    return cache["pair"]


def words(src, dst):
    """The edition change in words, or "" where there is none to say."""
    way = direction(src, dst)
    if way in (None, "same"):
        return ""
    if way == "another":
        return f"another edition: {src.label()} to {dst.label()}"
    head = f"an edition {way}: {src.label()} to {dst.label()}"
    if way == "down":
        head += (" - a licence-driven move: what the target's edition"
                 " lacks is said below")
    return head


def lacking(eng, db, src_ed, dst_ed, hop=None):
    """[(Feature, Use, verdict)] of what `db` uses that `dst_ed` may lack:
    verdict False where it lacks it, None where that is not known."""
    out = []
    for u in used(eng, db, src_ed):
        f = feature(src_ed.engine, u.key)
        if f is None:
            if u.key == "?":
                out.append((None, u, None))
            continue
        if _target_runs(eng, u.key, dst_ed):
            continue
        have = f.has(dst_ed)
        if have is True:
            continue
        out.append((f, u, have))
    return out


def _level(f, have, hop):
    if have is None:
        return "warn"
    if f.fate in (STRIPPED, BEHAVIOUR):
        return "warn"
    if f.fate == LOST and hop is not None and (
            f.key.lower() in accepted(hop) or "*" in accepted(hop)):
        return "warn"
    return "fail"


def _feature_rows(hop, eng, db, src_ed, dst_ed):
    out = []
    for f, u, have in lacking(eng, db, src_ed, dst_ed, hop):
        where = ", ".join(u.where[:6]) + (
            f" and {len(u.where) - 6} more" if len(u.where) > 6 else "")
        if f is None:
            out.append(_row("warn", "what the source uses",
                            f"could not be read: {where.lstrip('?')} -"
                            " unknown, not clean"))
            continue
        if any(str(w).startswith("?") for w in u.where):
            out.append(_row("warn", f.said,
                            f"{db}: could not be read on the source:"
                            f" {where.lstrip('?')} - unknown, not clean"))
            continue
        level = _level(f, have, hop)
        if have is None:
            detail = (f"{db} uses it ({where}); whether {dst_ed.label()}"
                      " has it is not known - unknown, not clean")
        elif f.fate == STRIPPED:
            detail = (f"{db} uses it ({where}) and {dst_ed.label()} has"
                      " none: the logical copy makes the tables without it,"
                      " the rows the same; a copy of the pages is refused")
        elif f.fate == BEHAVIOUR:
            detail = (f"{db} uses it ({where}) and {dst_ed.label()} has"
                      " none: it works otherwise there")
        elif level == "warn":
            detail = (f"{db} uses it ({where}) and {dst_ed.label()} has"
                      " none - its loss is accepted by the hop")
        elif f.fate == LOST:
            detail = (f"{db} uses it ({where}) and {dst_ed.label()} has"
                      " none: it is lost on the target")
        else:
            detail = (f"{db} uses it ({where}) and {dst_ed.label()} has"
                      " none: what uses it cannot be made there")
        fix = f.fix
        if f.fate == LOST and level == "fail":
            fix += (f" (options: {{edition: {{accept: [{f.key}]}}}} says the"
                    " loss is accepted)")
        out.append(_row(level, f.said, detail, fix))
    return out


def _human(n):
    from .wording import human_bytes
    return human_bytes(n)


def _cap_rows(eng, db, src_ed, dst_ed):
    cap = caps(dst_ed)
    if not cap:
        return []
    have = load(eng, db, src_ed.engine)
    out = []
    if "database" in cap and have.get("database") is not None:
        if have["database"] > cap["database"]:
            out.append(_row(
                "fail", "the edition's size",
                f"{db} holds {_human(have['database'])} of data and"
                f" {dst_ed.label()} holds {_human(cap['database'])} a"
                " database at most: the move would stop when it is full",
                "a target edition without the cap, or fewer tables in the"
                " hop"))
        else:
            out.append(_row("pass", "the edition's size",
                            f"{db} holds {_human(have['database'])}, within"
                            f" the {_human(cap['database'])}"
                            f" {dst_ed.label()} holds"))
    if "memory" in cap and have.get("memory"):
        if have["memory"] > cap["memory"]:
            out.append(_row(
                "warn", "the edition's memory",
                f"the source keeps {_human(have['memory'])} of pages in"
                f" memory and {dst_ed.label()} uses {_human(cap['memory'])}"
                " at most: reads it answers from memory go to disk there",
                "measure the application's statements on the target before"
                " the cutover (migkit check compares them)"))
    if "cores" in cap and have.get("cores"):
        if have["cores"] > cap["cores"]:
            out.append(_row(
                "warn", "the edition's cores",
                f"the source runs on {have['cores']} cores and"
                f" {dst_ed.label()} uses {cap['cores']} at most",
                "measure the application's load on the target before the"
                " cutover"))
    return out


def _licence_rows(src_ed, dst_ed):
    out = []
    if dst_ed.licence and dst_ed.name != src_ed.name:
        out.append(_row("warn", "the target's licence",
                        f"{dst_ed.label()} is licensed for {dst_ed.licence}",
                        "a target edition licensed for what it will"
                        " serve"))
    return out


def checks(hop, eng, dbs):
    """assess rows of an edition change for the hop's databases: each
    side's edition, the change in words, what the source uses that the
    target's edition lacks, and the target edition's caps against the
    source. [] for an engine without editions."""
    got = pair(eng, dbs[0] if dbs else None)
    src_ed, dst_ed = got
    if src_ed is None:
        return []
    if not src_ed.known or not dst_ed.known:
        which = [f"the {s}'s: {e.evidence}" for s, e in (("source", src_ed),
                                                           ("target", dst_ed))
                 if not e.known]
        return [_row("warn", "editions", "not read - " + "; ".join(which)
                     + " - unknown, not clean")]
    way = direction(src_ed, dst_ed)
    if way == "same":
        said = (f"{src_ed.label()} on both sides"
                if src_ed.label() == dst_ed.label() else
                f"{src_ed.label()} and {dst_ed.label()}, the same edition")
        return [_row("pass", "editions", said)]
    out = [_row("warn" if way == "down" else "pass", "editions",
                words(src_ed, dst_ed) or f"{src_ed.label()} to"
                                         f" {dst_ed.label()}",
                "" if way != "down" else
                "nothing to do if the licence is the reason: each thing the"
                " target's edition lacks is below, with its own fix")]
    out += _licence_rows(src_ed, dst_ed)
    for d in dbs:
        try:
            out += _feature_rows(hop, eng, d, src_ed, dst_ed)
        except Exception as e:  # noqa: BLE001 - unknown, said so
            out.append(_row("warn", "what the source uses",
                            f"{d}: could not be read: {_last(e)} - unknown,"
                            " not clean"))
        try:
            out += _cap_rows(eng, d, src_ed, dst_ed)
        except Exception as e:  # noqa: BLE001 - unknown, said so
            out.append(_row("warn", "the edition's caps",
                            f"{d}: could not be read: {_last(e)} - unknown,"
                            " not clean"))
    return out


def refusals(eng, db, src_ed, dst_ed, hop=None):
    """What stops a copy of the pages into `dst_ed`: every feature the
    database uses that the target's edition does not have (a restored
    database with one does not start there), in words - [] where none."""
    out = []
    for f, u, have in lacking(eng, db, src_ed, dst_ed, hop):
        if f is None:
            continue
        out.append(f"{f.said} ({f.key})" + (" - whether the target's"
                                             " edition has it is not known"
                                             if have is None else ""))
    return out


def stripped(eng, db, src_ed, dst_ed):
    """The features a logical copy leaves behind on `dst_ed`, in words."""
    return [f.said for f, u, have in lacking(eng, db, src_ed, dst_ed)
            if f is not None and have is False and f.fate == STRIPPED]
