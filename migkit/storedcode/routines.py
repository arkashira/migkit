"""What one piece of stored code is, whichever engine wrote it.

Every engine reads its own catalogue into these (`code_routines`); the
converters, the sandbox and the proof read nothing else. A routine carries
its source text twice: the body, which a converter reads, and the whole
`CREATE` statement the engine itself gives back, which is what the sandbox
runs to make the same routine there.
"""
from dataclasses import dataclass, field

#: what each kind of routine is called on the other side of a pair: a MySQL
#: event is carried as a procedure the target's scheduler (or a person)
#: runs; the rest keep their kind
TARGET_KIND = {"event": "procedure"}


@dataclass
class Param:
    name: str
    type: str
    mode: str = "in"          #: in, out or inout


@dataclass
class Routine:
    kind: str                 #: function, procedure, trigger or event
    name: str
    params: list = field(default_factory=list)
    #: the declared type a function returns; None for anything else
    returns: str = None
    #: a function that answers rows rather than one value
    returns_set: bool = False
    #: the body as its engine keeps it
    body: str = None
    #: the whole statement that makes it, as its engine gives it back
    definition: str = None
    #: a trigger's table (as `neutral_tables` names it), when (BEFORE,
    #: AFTER, INSTEAD OF), on what (INSERT, UPDATE, DELETE) and for what
    #: (ROW, STATEMENT)
    table: str = None
    timing: str = None
    events: tuple = ()
    level: str = "row"
    #: an event's schedule, as data: {"every": "1 HOUR", "starts": ...}
    schedule: dict = None
    #: the language the body is in, where the engine has several
    language: str = None
    #: what the engine says about it: sql_mode, whether it reads or writes
    #: data, the function a trigger runs
    settings: dict = field(default_factory=dict)

    @property
    def key(self):
        """The name the two sides share: the kind the target keeps it as,
        and the name without case (MySQL names routines without it)."""
        return (TARGET_KIND.get(self.kind, self.kind), self.name.lower())

    @property
    def said(self):
        return f"{self.kind} {self.name}"


def by_key(routines):
    return {r.key: r for r in routines}
