import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

BASE = Path(__file__).resolve().parent.parent
CONF = Path(os.environ.get("MIGKIT_CONF", BASE / "conf" / "hops.yaml"))
REPORTS = Path(os.environ.get("MIGKIT_REPORTS", BASE / "reports"))


@dataclass
class Endpoint:
    host: str = ""
    port: int = 0
    user: str = ""
    password: str = ""
    options: dict = field(default_factory=dict)

    def configured(self):
        return bool(self.host or self.options.get("hosts")
                    or self.options.get("path") or self.options.get("url"))


@dataclass
class Hop:
    name: str
    engine: str
    source: Endpoint
    target: Endpoint
    databases: list = field(default_factory=list)
    exclude: list = field(default_factory=list)
    service: str = ""
    big_rows: int = 5_000_000
    slice: int = 1_000_000
    workers: int = 4
    options: dict = field(default_factory=dict)

    def report_dir(self, db=""):
        d = REPORTS / self.name / db if db else REPORTS / self.name
        d.mkdir(parents=True, exist_ok=True)
        return d


DEFAULT_PORTS = {
    "postgres": 5432, "mysql": 3306, "mssql": 1433,
    "mongodb": 27017, "redis": 6379, "kafka": 9092,
}


def _endpoint(engine, raw):
    raw = raw or {}
    extra = {k: v for k, v in raw.items()
             if k not in ("host", "port", "user", "password")}
    nested = extra.pop("options", None)
    if isinstance(nested, dict):
        extra.update(nested)
    return Endpoint(
        host=raw.get("host", ""),
        port=int(raw.get("port") or DEFAULT_PORTS.get(engine, 0)),
        user=raw.get("user", ""),
        password=str(raw.get("password", "")),
        options=extra,
    )


def load_hops(path=None):
    path = Path(path or CONF)
    if not path.exists():
        raise SystemExit(f"missing config {path}, copy conf/hops.example.yaml")
    data = yaml.safe_load(path.read_text()) or {}
    hops = {}
    for name, raw in (data.get("hops") or {}).items():
        engine = raw.get("engine", "postgres")
        hops[name] = Hop(
            name=name,
            engine=engine,
            source=_endpoint(engine, raw.get("source")),
            target=_endpoint(engine, raw.get("target")),
            databases=raw.get("databases") or [],
            exclude=raw.get("exclude") or [],
            service=raw.get("service", ""),
            big_rows=int(raw.get("big_rows", 5_000_000)),
            slice=int(raw.get("slice", 1_000_000)),
            workers=int(raw.get("workers", 4)),
            options=raw.get("options") or {},
        )
    return hops


def get_hop(name):
    hops = load_hops()
    if name not in hops:
        raise SystemExit(f"unknown hop {name}, have: {', '.join(hops) or 'none'}")
    return hops[name]
