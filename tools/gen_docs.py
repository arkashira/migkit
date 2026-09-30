#!/usr/bin/env python3
"""The manual and the site, written from the code (backlog R24, AUD-DOC).

The site described a version of migkit months old, the README badge named
12 engines of 34, and `docs/goals-and-modes.md` said it came "from one
table in the code" while four of its cells had drifted from that table
(audit LG15, LG16). So nothing here is typed twice: each page is read off
the registries the code itself runs on, and a test fails when a page on
disk is not what this writes, or names a program migkit drives.

  gen_docs.py            write every page that is behind the code
  gen_docs.py --check    write nothing; name each page behind it, exit 3
                         (1 is a failure of its own, which a hook tells
                         apart from a page behind)

What is written, and what each is read from:

* `docs/index.html` and `docs/site/index.html` - the front page: the
  engines (`engines.NAMES`, `TITLES`) and the goals (`hopkeys.GOALS`).
* `docs/site/quickstart.html` - two addresses to a cutover: `init`'s own
  questions (`guide.ASK_*`), the modes (`hopkeys.MODES`), the commands'
  help (the CLI).
* `docs/site/engines.html` - every engine, its aliases and address
  schemes (`engines.ALIASES`, `SCHEMES`), what it can do
  (`capabilities.matrix()` with each gap's reason and what to do
  meanwhile), each shape of table it handles (`decide.cell`, the cells
  of `decide.coverage()`), the
  pairs whose stored code is converted and proved (`storedcode.rungs`),
  the engines two ways run on (`marks.RUNGS`) and the managed services
  recognised (`managed` where it is, `clouds.LABELS` otherwise).
* `docs/site/goals.html` - `hopkeys.GOALS`, `MODES`, `CUTOVER`, `LIMITS`.
* `docs/site/recipes.html` - source -> target pairs, each hop checked by
  `hopkeys.check`, each capability it leans on held to the matrix, each
  said in the words `migkit init` says it (`hopkeys.explain_goal`).
* `docs/site/options.html` - every key a hop and a side may say
  (`hopkeys.HOP_KEYS`, `HOP_OPTIONS`, `ENDPOINT_KEYS`, `ENDPOINT_OPTIONS`).
* `docs/site/environment.html` - every `MIGKIT_*` variable
  (`envkeys.VARS` where it is, the names the code reads otherwise).
* `docs/site/commands.html` - every command and option (the CLI itself)
  and every metric family `/metrics` serves (`ui`).
* `docs/site/troubleshooting.html` and `docs/troubleshooting.md` - every
  refusal (`troubleshooting.by_area`).
* `docs/site/evidence.html` - the evidence export's files, tables and keys
  (`export.FILES`, `gather`, `_sections`, `table_rows`).
* `docs/site/releases.html` - each tagged release's commits, grouped as
  `tools/gen_changelog.py` groups them, a program's name in a subject
  taken out by the words filter (`wording.without_programs`, over the
  evidence export's list of names); left out where the copy has no tags
  to read. The version shown on every page is `migkit.__version__`.
* The pages that will read registries still on their way: the
  environment page reads `envkeys.VARS` where this tree has it, the names
  the code reads otherwise; the managed services come from `managed` where
  this tree has it, from `clouds.LABELS` otherwise (`optional`).
* `README.md` and `docs/goals-and-modes.md` - only between their
  `<!-- gen_docs: NAME -->` markers; the prose around them is the owner's.

Every page says migkit's own words: a program migkit drives is named on
none of them (the owner's rule), which is why a hop option named for one,
or whose words name one, is left out of the options page, and why the
program sources are shown without their schemes.
"""
import ast
import html
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "tools") not in sys.path:
    sys.path.insert(0, str(ROOT / "tools"))

SITE = Path("docs") / "site"
#: what `--check` exits with when a page is behind the code
BEHIND = 3
SOURCE = "https://github.com/arkashira/migkit"
#: what migkit is, in the words every page leads with
POSITION = ("migkit's own decision engine finds the fastest, exact,"
            " consistent way for each source -> target pair")


# ---- the blocks a page is made of -------------------------------------------

class Said(str):
    """Words taken from the code as they are: backticks become code, and
    nothing else in them is read as markup."""


class H:
    def __init__(self, text, anchor, level=2):
        self.text, self.anchor, self.level = text, anchor, level


class P:
    def __init__(self, text, cls=""):
        self.text, self.cls = text, cls


class Pre:
    def __init__(self, text):
        self.text = text


class UL:
    def __init__(self, items, ordered=False):
        self.items, self.ordered = list(items), ordered


class Cell:
    def __init__(self, text, cls="", title=""):
        self.text, self.cls, self.title = text, cls, title


class Pills:
    def __init__(self, items):
        self.items = list(items)


class Table:
    """`ids`, where given, names each row (`<tr id=...>`), so a test can
    find an engine's or an option's row by its name."""

    def __init__(self, head, rows, ids=None, wide=False):
        self.head, self.rows = list(head), [list(r) for r in rows]
        self.ids, self.wide = ids, wide


class Page:
    def __init__(self, slug, title, nav, lede, blocks, description=""):
        self.slug, self.title, self.nav = slug, title, nav
        self.lede, self.blocks = lede, blocks
        self.description = description or lede


# ---- inline words -----------------------------------------------------------

_MARKUP = re.compile(r"`([^`]+)`|\*\*(.+?)\*\*|\[([^\]]+)\]\(([^)\s]+)\)")
_CODE = re.compile(r"`([^`]+)`")


def _esc(text):
    return html.escape(str(text), quote=False)


def _href(target, prefix):
    """A link to another page of the site from where this page is."""
    if re.match(r"^[a-z]+\.html(#.*)?$", target):
        return prefix + target
    return target


def inline(text, prefix=""):
    """`text` as HTML: `code`, **bold** and [links](page.html) in what is
    written here; only `code` in what the code says (`Said`)."""
    text = "" if text is None else text
    pattern = _CODE if isinstance(text, Said) else _MARKUP
    out, at = [], 0
    for m in pattern.finditer(text):
        out.append(_esc(text[at:m.start()]))
        if m.group(1) is not None:
            out.append(f"<code>{_esc(m.group(1))}</code>")
        elif m.group(2) is not None:
            out.append(f"<b>{inline(m.group(2), prefix)}</b>")
        else:
            out.append(f'<a href="{html.escape(_href(m.group(4), prefix))}">'
                       f"{inline(m.group(3), prefix)}</a>")
        at = m.end()
    out.append(_esc(text[at:]))
    return "".join(out)


def _md_inline(text, base):
    """`text` as Markdown: a link to a page of the site made whole."""
    if isinstance(text, Said):
        return str(text)
    return re.sub(r"\]\(([a-z]+\.html(?:#[^)]*)?)\)",
                  lambda m: f"]({base}{m.group(1)})", str(text))


# ---- rendering --------------------------------------------------------------

CSS = """
:root{--bg:#fbfbfa;--panel:#f2f3f1;--ink:#1d232b;--mut:#5b6573;
--line:#dfe2e6;--acc:#0b63c4;--ok:#1f7a3d;--warn:#9a6700;--no:#b42318;
--code:#f3f4f6;--codeink:#1d232b}
@media (prefers-color-scheme:dark){:root{--bg:#0d1117;--panel:#111721;
--ink:#d6dee8;--mut:#8b98a8;--line:#222c39;--acc:#4aa3ff;--ok:#39b96a;
--warn:#e0a93b;--no:#e5534b;--code:#0a0e14;--codeink:#cbd6e4}}
*{box-sizing:border-box}
html{scroll-behavior:smooth}
body{margin:0;background:var(--bg);color:var(--ink);
font:15px/1.65 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,
Helvetica,Arial,sans-serif}
a{color:var(--acc);text-decoration:none}
a:hover{text-decoration:underline}
code,pre{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
code{background:var(--code);color:var(--codeink);padding:.1em .35em;
border-radius:4px;font-size:.9em}
pre{background:var(--code);border:1px solid var(--line);border-radius:8px;
padding:14px 16px;overflow:auto;font-size:13px;line-height:1.55}
pre code{background:none;padding:0}
.layout{display:flex;max-width:1240px;margin:0 auto}
nav{position:sticky;top:0;align-self:flex-start;width:250px;height:100vh;
overflow:auto;padding:26px 16px 40px 22px;border-right:1px solid var(--line)}
nav .brand{font-weight:700;font-size:19px}
nav .tag{color:var(--mut);font-size:12px;margin:4px 0 18px}
nav a{display:block;color:var(--mut);padding:3px 0;font-size:13.5px}
nav a.here{color:var(--ink);font-weight:600}
nav a.sub{padding-left:12px;font-size:12.5px}
main{flex:1;min-width:0;padding:34px 40px 120px;max-width:980px}
h1{font-size:30px;margin:.2em 0 .1em}
h2{font-size:22px;margin:2em 0 .5em;padding-top:.4em;
border-top:1px solid var(--line)}
h3{font-size:16px;margin:1.6em 0 .4em}
.lede{color:var(--mut);font-size:16px;margin:.2em 0 1.4em}
.rule{background:var(--panel);border-left:3px solid var(--acc);
border-radius:0 8px 8px 0;padding:12px 16px;margin:18px 0}
.mut{color:var(--mut);font-size:13px}
.wide{overflow-x:auto}
table{border-collapse:collapse;width:100%;margin:14px 0;font-size:13.5px}
th,td{border:1px solid var(--line);padding:6px 9px;text-align:left;
vertical-align:top}
th{background:var(--panel);font-weight:600}
.wide table{font-size:12.5px}
.wide td,.wide th{padding:4px 6px}
td.y{color:var(--ok);font-weight:600}
td.p{color:var(--warn);font-weight:600}
td.na{color:var(--mut)}
.pill{display:inline-block;background:var(--panel);
border:1px solid var(--line);
color:var(--mut);border-radius:20px;padding:2px 10px;font-size:12px;
margin:2px 4px 2px 0}
footer{color:var(--mut);font-size:12.5px;border-top:1px solid var(--line);
margin-top:40px;padding-top:16px}
@media(max-width:820px){nav{display:none}main{padding:24px 18px 80px}}
""".strip()


def _block_html(b, prefix):
    if isinstance(b, H):
        return (f'<h{b.level} id="{b.anchor}">{inline(b.text, prefix)}'
                f"</h{b.level}>")
    if isinstance(b, P):
        cls = f' class="{b.cls}"' if b.cls else ""
        return f"<p{cls}>{inline(b.text, prefix)}</p>"
    if isinstance(b, Pre):
        return f"<pre><code>{_esc(b.text)}</code></pre>"
    if isinstance(b, UL):
        tag = "ol" if b.ordered else "ul"
        items = "".join(f"<li>{inline(i, prefix)}</li>" for i in b.items)
        return f"<{tag}>{items}</{tag}>"
    if isinstance(b, Pills):
        return "<p>" + "".join(f'<span class="pill">{inline(i, prefix)}'
                               "</span>" for i in b.items) + "</p>"
    if isinstance(b, Table):
        head = "".join(f"<th{_title(h)}>{inline(_text(h), prefix)}</th>"
                       for h in b.head)
        rows = []
        for i, row in enumerate(b.rows):
            rid = f' id="{b.ids[i]}"' if b.ids else ""
            cells = "".join(
                f"<td{_cls(c)}{_title(c)}>{inline(_text(c), prefix)}</td>"
                for c in row)
            rows.append(f"<tr{rid}>{cells}</tr>")
        table = f"<table><tr>{head}</tr>{''.join(rows)}</table>"
        return f'<div class="wide">{table}</div>' if b.wide else table
    raise TypeError(b)


def _text(c):
    return c.text if isinstance(c, Cell) else c


def _cls(c):
    return f' class="{c.cls}"' if isinstance(c, Cell) and c.cls else ""


def _title(c):
    return (f' title="{html.escape(str(c.title))}"'
            if isinstance(c, Cell) and c.title else "")


def _block_md(b, base):
    if isinstance(b, H):
        return "#" * b.level + " " + _md_inline(b.text, base)
    if isinstance(b, P):
        return _md_inline(b.text, base)
    if isinstance(b, Pre):
        return "```\n" + b.text.rstrip("\n") + "\n```"
    if isinstance(b, UL):
        return "\n".join((f"{n}. " if b.ordered else "- ")
                         + _md_inline(i, base)
                         for n, i in enumerate(b.items, 1))
    if isinstance(b, Pills):
        return ", ".join(_md_inline(i, base) for i in b.items)
    if isinstance(b, Table):
        def cell(c):
            return _md_inline(_text(c), base).replace("|", "\\|") \
                .replace("\n", " ")
        out = ["| " + " | ".join(cell(h) for h in b.head) + " |",
               "|" + "|".join("---" for _ in b.head) + "|"]
        out += ["| " + " | ".join(cell(c) for c in row) + " |"
                for row in b.rows]
        return "\n".join(out)
    raise TypeError(b)


def markdown(blocks, base=""):
    return "\n\n".join(_block_md(b, base) for b in blocks)


def page_html(page, pages, prefix=""):
    """One page of the site; `prefix` is where the other pages are from
    it (`site/` for the copy of the front page at the root)."""
    from migkit import __version__
    nav = []
    for p in pages:
        here = p.slug == page.slug
        mark = ' class="here"' if here else ""
        nav.append(f'<a href="{prefix}{p.slug}.html"{mark}>'
                   f"{_esc(p.nav)}</a>")
        if here:
            nav += [f'<a class="sub" href="#{b.anchor}">'
                    f"{inline(b.text, prefix)}</a>"
                    for b in page.blocks if isinstance(b, H) and b.level == 2]
    body = "\n".join(_block_html(b, prefix) for b in page.blocks)
    named = "The manual" if page.slug == "index" else page.title
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{_esc(named)} - migkit {__version__}</title>
<meta name="description" content="{html.escape(str(page.description))}">
<style>
{CSS}
</style>
</head>
<body>
<div class="layout">
<nav>
<div class="brand">migkit</div>
<div class="tag">version {__version__}</div>
{chr(10).join(nav)}
</nav>
<main>
<h1>{_esc(page.title)}</h1>
<p class="lede">{inline(page.lede, prefix)}</p>
{body}
<footer>migkit {__version__} - this page is written from the code by
<code>tools/gen_docs.py</code>, so it says what this version does.
Source: <a href="{SOURCE}">{SOURCE[len("https://"):]}</a>.
AGPL-3.0-or-later.</footer>
</main>
</div>
</body>
</html>
"""


# ---- what the code says -----------------------------------------------------

#: what one writing of every page reads more than once, kept for that
#: writing only (`outputs`); None outside it, so a page built on its own
#: reads the code as it is then
_MEMO = None


def _once(key, read):
    if _MEMO is None:
        return read()
    if key not in _MEMO:
        _MEMO[key] = read()
    return _MEMO[key]


def refusals_by_area():
    from migkit import troubleshooting
    return _once("refusals", troubleshooting.by_area)


def _engines():
    from migkit.engines import ALIASES, NAMES, SCHEMES, TITLES
    return NAMES, ALIASES, SCHEMES, TITLES


def optional(name):
    """`migkit.<name>` where this tree has it, else None. Asked by its
    file, not by importing: an editable install of another checkout
    answers an import of a module this tree lacks with that checkout's,
    and the pages would say what another tree does."""
    if not (ROOT / "migkit" / f"{name}.py").is_file():
        return None
    import importlib
    return importlib.import_module(f"migkit.{name}")


def title(name):
    return _engines()[3].get(name, name)


def _aliases(name):
    _, aliases, _, _ = _engines()
    return sorted(a for a, n in aliases.items() if n == name)


def _schemes(name):
    """The address schemes that name `name` - none for the program
    sources, whose scheme is the kind of connector program they run."""
    if name == "saas":
        return []
    _, _, schemes, _ = _engines()
    return [s for s, n in schemes.items() if n == name]


def _unsaid():
    """The programs and libraries migkit drives, which no page names: the
    words filter's own list (`export._names`), less what is also an
    engine's, an alias's or a scheme's name."""
    from migkit import export
    names, aliases, schemes, _ = _engines()
    engines = set(names) | set(aliases) | set(schemes)
    return _once("unsaid", lambda: sorted(
        {n.lower() for n in export._names()} - engines))


def names_a_program(text):
    low = str(text).lower()
    flat = low.replace("_", "-")
    return [n for n in _unsaid() if n in low or n.replace("_", "-") in flat]


def role(name, row):
    """What the engine can be in a pair, read off its row of the matrix."""
    from migkit.capabilities import NOT_APPLICABLE
    if name == "hetero":
        return "the pair: a source of one engine, a target of another"
    if name == "generic":
        return "either side, compared by the tables the hop lists"
    if all(v == NOT_APPLICABLE for v in row.values()):
        return "a stream a pair delivers its changes into"
    if row["table-copy"] == NOT_APPLICABLE and row["data"] == "yes":
        return "a source, read and never written"
    return "either side"


def _cap_cell(name, cap, state):
    from migkit import capabilities
    if state == "yes":
        return Cell("yes", "y", capabilities.CAPABILITIES[cap])
    gap = capabilities.GAPS.get(name, {}).get(cap)
    if state == capabilities.NOT_APPLICABLE:
        return Cell("n/a", "na", gap[1] if gap else "")
    if state == capabilities.NOT_YET:
        return Cell("not yet", "p",
                    "meanwhile, " + capabilities.INSTEAD[cap])
    # a gap nobody declared, which its own test fails on: said as it is
    return Cell(state, "u")


def _shape_cell(state, detail):
    from migkit.capabilities import NOT_APPLICABLE, NOT_YET
    if state == "yes":
        return Cell("yes", "y")
    if state == NOT_APPLICABLE:
        return Cell("n/a", "na", detail)
    if state == NOT_YET:
        return Cell("not yet", "p")
    # undeclared, or a way gone from the code: its own test fails on it
    return Cell(state, "u")


def _code_candidates():
    """(the SQL engines, {(source, target): {candidate: (its words,
    {kinds})}}) - what each way of converting routines gives each pair
    of engines that speak SQL, read off the candidates themselves."""
    from types import SimpleNamespace

    from migkit.config import Endpoint, Hop
    from migkit.engines import _class_for
    from migkit.storedcode import rungs
    names, _, _, _ = _engines()
    sql = [n for n in names if n not in ("hetero", "generic")
           and getattr(_class_for(n), "SQL_DIALECT", None)]
    out = {}
    for a in sql:
        for b in sql:
            ca, cb = _class_for(a), _class_for(b)
            if ca.SQL_DIALECT == cb.SQL_DIALECT:
                continue
            hop = Hop(name="x", engine="hetero", source=Endpoint(host="h"),
                      target=Endpoint(host="h"),
                      options={"source_engine": a, "target_engine": b})
            pair = SimpleNamespace(src_engine=ca, dst_engine=cb, hop=hop)
            pre = f"{ca.SQL_DIALECT}->{cb.SQL_DIALECT}:"
            out[(a, b)] = {c.name: (c.said, {g[len(pre):] for g in c.gives
                                             if g.startswith(pre)})
                           for c in rungs.candidates(pair, None, {})}
    return sql, out


def stored_code():
    """(the SQL engines, [(a way every pair has, [kinds])], [(source,
    target, [kinds])]): what every pair of SQL engines has its routines
    carried by, and the pairs migkit's own converters carry beyond it.
    Each version, whoever wrote it, is proved the same way."""
    from migkit.storedcode import rungs

    def ordered(kinds):
        return [k for k in rungs.KINDS if k in kinds]
    sql, pairs = _once("code", _code_candidates)
    every = {}
    for got in pairs.values():
        for name, (said, kinds) in got.items():
            if kinds:
                every.setdefault(name, [said, 0])[1] += 1
    generic = [n for n, (_, count) in every.items() if count == len(pairs)]
    shared = []
    for name in generic:
        kinds = set.intersection(*(got[name][1] for got in pairs.values()))
        shared.append((every[name][0], ordered(kinds)))
    special = []
    for (a, b), got in pairs.items():
        kinds = set().union(*(k for n, (_, k) in got.items()
                              if n not in generic))
        if kinds:
            special.append((a, b, ordered(kinds)))
    return sql, shared, special


def two_way_engines():
    from migkit import marks
    seen = []
    for r in marks.RUNGS:
        if r.family not in seen:
            seen.append(r.family)
    return seen


#: a cloud's own name, for the recognised services where no catalogue of
#: managed services is in the code yet
_CLOUDS = {"aws": "Amazon Web Services", "gcp": "Google Cloud",
           "azure": "Microsoft Azure", "tencent": "Tencent Cloud",
           "alibaba": "Alibaba Cloud"}


def managed_services():
    """[(provider, service, the engine migkit moves it as or "")], from
    the catalogue of managed services where the code has one, else from
    the services `clouds` recognises by their addresses."""
    managed = optional("managed")
    if managed is not None:
        return [(s.provider_label, s.label, s.kind)
                for s in managed.services().values() if s.managed]
    from migkit import clouds
    return [(_CLOUDS.get(cloud, cloud), label, "")
            for (cloud, _), label in clouds.LABELS.items()]


# ---- the pages --------------------------------------------------------------

def _engine_rows():
    from migkit import capabilities
    names, _, _, _ = _engines()
    return names, _once("matrix", capabilities.matrix)


def page_engines():
    from migkit import capabilities, decide
    names, matrix = _engine_rows()
    caps = list(capabilities.CAPABILITIES)
    blocks = [
        P(f"Every engine this version has, and what it can do - read off the"
          f" code, so this page claims no more and no less than it does."
          f" A hop names its two sides by their addresses; {POSITION}, and"
          f" says which it chose and why."),
        H("The engines", "engines"),
        Table(["Engine", "Name", "Also written", "Address schemes", "Role"],
              [[f"[{title(n)}](#engine-{n})", f"`{n}`",
                ", ".join(f"`{a}`" for a in _aliases(n)) or "-",
                ", ".join(f"`{s}://`" for s in _schemes(n)) or "-",
                role(n, matrix[n])] for n in names],
              ids=[f"row-{n}" for n in names]),
        H("What each can do", "capabilities"),
        P("**yes**: the code does it for this engine. **n/a**: it does not"
          " apply, and the reason is said. **not yet**: not done yet, and"
          " what to do meanwhile is said. Hover a cell for its reason."),
        UL([f"`{c}`: {w}" for c, w in capabilities.CAPABILITIES.items()]),
        Table(["Engine"] + [Cell(c, title=capabilities.CAPABILITIES[c])
                            for c in caps],
              [[title(n)] + [_cap_cell(n, c, matrix[n][c]) for c in caps]
               for n in names], ids=[f"cap-{n}" for n in names], wide=True),
        H("Each shape of table", "shapes"),
        P("A table without a key, a key of many columns, a table too large"
          " for one range: each shape has a way of its own on each engine,"
          " chosen by the decision engine."),
        UL([f"`{s}`: {w}" for s, w in decide.SHAPES.items()]),
        Table(["Engine"] + list(decide.SHAPES),
              [[title(n)] + [_shape_cell(*decide.cell(n, s))
                             for s in decide.SHAPES] for n in names],
              ids=[f"shape-{n}" for n in names], wide=True),
    ]
    sql, shared, special = stored_code()
    blocks += [
        H("Stored code carried and proved", "stored-code"),
        P("Views, functions, procedures and triggers are converted one at a"
          " time by whichever way proves itself on each, and each version,"
          " whoever wrote it, is run on both sides in a sandbox and"
          " compared. Between any two of "
          + ", ".join(title(n) for n in sql) + ", a routine is carried by:"),
        UL(Said(f"{said}: " + ", ".join(kinds)) for said, kinds in shared),
        P("and these pairs further, by migkit's own converters:"),
        Table(["Source", "Target", "What is converted"],
              [[title(a), title(b), ", ".join(k)] for a, b, k in special]),
        H("Both ways at once", "two-way"),
        P("A pair of hops, each side's changes carried to the other and"
          " migkit's own writes told apart, runs on: "
          + ", ".join(title(n) for n in two_way_engines()) + "."),
    ]
    services = managed_services()
    blocks += [
        H("Managed services", "managed"),
        P("A managed service is the engine it runs, reached at its address;"
          " migkit recognises these, and says the provider's own way of"
          " changing a setting on each."),
        Table(["Provider", "Service"] + (["Engine"] if any(
            s[2] for s in services) else []),
              [[p, s] + ([k] if any(x[2] for x in services) else [])
               for p, s, k in services]),
        H("Each engine", "each"),
    ]
    for n in names:
        row = matrix[n]
        can = [capabilities.CAPABILITIES[c] for c in caps if row[c] == "yes"]
        na = [f"{capabilities.CAPABILITIES[c]}: "
              + capabilities.GAPS.get(n, {}).get(c, ("", ""))[1]
              for c in caps if row[c] == capabilities.NOT_APPLICABLE]
        yet = [f"{capabilities.CAPABILITIES[c]} - meanwhile,"
               f" {capabilities.INSTEAD[c]}"
               for c in caps if row[c] == capabilities.NOT_YET]
        blocks += [H(title(n), f"engine-{n}", 3),
                   P(f"`{n}`" + (", also written "
                                 + ", ".join(f"`{a}`" for a in _aliases(n))
                                 if _aliases(n) else "")
                     + f" - {role(n, row)}.")]
        if can:
            blocks.append(P("It does: " + "; ".join(can) + "."))
        if na:
            blocks += [P("Does not apply:"), UL(Said(x) for x in na)]
        if yet:
            blocks += [P("Not yet:"), UL(Said(x) for x in yet)]
    return Page("engines", "Engines", "Engines",
                f"{len(names) - 2} engines, any two of them as a pair, and"
                " what each can do.", blocks)


def _goal_rows():
    from migkit import hopkeys
    return [[f"`{g}`" + (" (the default)" if g == hopkeys.DEFAULT_GOAL
                         else ""),
             Said(v["what"]), Said(v["source"]), Said(v["target"]),
             Said(v["guarantees"])] for g, v in hopkeys.GOALS.items()]


def goals_table():
    from migkit import hopkeys
    return Table(["Goal", "What it is for", "On the source",
                  "On the target", "Guaranteed"], _goal_rows(),
                 ids=[f"goal-{g}" for g in hopkeys.GOALS])


def modes_table():
    from migkit import hopkeys
    return Table(["Command", "What it does"],
                 [[f"`{c} --mode {m}`", Said(w)]
                  for (c, m), w in hopkeys.MODES.items()],
                 ids=[f"mode-{c}-{m.replace(' ', '')}"
                      for c, m in hopkeys.MODES])


def cutover_blocks():
    from migkit import hopkeys
    c = hopkeys.CUTOVER
    steps = [Said(s.format(hop="HOP", back=" (and starts the stream back,"
                                          " with `reverse: at_cutover`)"))
             for s in c["by hand"]]
    return [P("**Off** (no `cutover:` in the hop's options, whatever the"
              " goal): on the source, " + c["off source"] + ". In order:"),
            UL(steps, ordered=True),
            P("**On** (`cutover: true`, or its settings): migkit does all of"
              " that itself, and adds:"),
            UL(Said(a) for a in c["adds"]),
            P(Said("With it on, the source is "
                   + c["on source"].format(hop="HOP") + "."))]


def limits_blocks():
    from migkit import hopkeys
    return [UL(f"**{k}**: " + Said(v) for k, v in hopkeys.LIMITS.items())]


def page_goals():
    from migkit import hopkeys
    blocks = [
        H("Goals", "goals"),
        P("A hop says what it is for in one word, its `goal`. What each does"
          " on the source and on the target, and what it guarantees, is"
          " what `migkit init`, `migkit assess` and every dry run say for"
          " the hop at hand."),
        goals_table(),
        P("The commands each goal runs, in order:"),
        Table(["Goal", "Commands, in order"],
              [[f"`{g}`", "; ".join(f"`{s.format(hop='HOP')}`"
                                    for s in v["next"])]
               for g, v in hopkeys.GOALS.items()]),
        P("**`goal: cutover` does not turn `cutover:` on.** The goal sets up"
          " the stream, the stream back and the protection of the target;"
          " the cutover migkit drives itself runs only where the hop's"
          " options say `cutover:`."),
        H("Modes", "modes"),
        modes_table(),
        P("Without `--go` (or `--apply`) every one of them only shows its"
          " plan, starting with what each side will see."),
        H("The cutover, on or off", "cutover"),
    ] + cutover_blocks() + [
        H("Limits the move keeps to", "limits"),
        P("Each is a hard limit: the plan keeps to it wherever some way can,"
          " and where none can it says what binds and what would help. A hop"
          " that sets none is planned for the soonest end."),
    ] + limits_blocks()
    return Page("goals", "Goals and modes", "Goals and modes",
                "What a hop is for, what each command's modes do on each"
                " side, and what the cutover adds when a hop turns it on.",
                blocks)


_KNOBS = {"ceiling": "a ceiling: migkit works the number out itself and"
                     " never goes past this",
          "size": "a number migkit otherwise decides; leave it out unless"
                  " its own choice failed you",
          "way": "false turns off one of the ways migkit may choose; left"
                 " out, migkit chooses",
          "nothing": "read by nothing"}


def options_listed(registry):
    """(listed, left out) of one registry: a key named for a program, or
    whose words name one, is left out."""
    listed, out = {}, []
    for k, key in registry.items():
        if key.named_for_a_program or names_a_program(f"{k} {key.what}"):
            out.append(k)
        else:
            listed[k] = key
    return listed, out


def _option_table(slug, registry):
    from migkit import hopkeys
    listed, out = options_listed(registry)

    def engines(key):
        return ("every engine" if not key.engines else
                ", ".join("a pair across two engines" if e == "hetero"
                          else title(e) for e in key.engines))
    rows = []
    for k, key in listed.items():
        what = Said(key.what)
        more = []
        if key.subs:
            more.append("keys under it: "
                        + ", ".join(f"`{s}`" for s in key.subs))
        if key.knob and _KNOBS[key.knob] not in key.what:
            more.append(_KNOBS[key.knob])
        rows.append([f"`{k}`", "any" if key.kind == "any"
                     else hopkeys._kind_words(key.kind), engines(key),
                     Said(what + (" - " + "; ".join(more) if more else ""))])
    return Table(["Key", "Written as", "Read on", "What it is"], rows,
                 ids=[f"opt-{slug}-{k}" for k in listed]), out


def page_options():
    from migkit import hopkeys
    blocks = [P("Every key a hop may say, from the one registry the"
                " configuration is checked against before anything runs: an"
                " unknown key is refused with the closest known one, a"
                " wrong type or a contradiction with where it is.")]
    left = []
    for slug, heading, registry, words in (
            ("hop", "A hop's keys", hopkeys.HOP_KEYS,
             "The keys of a hop itself, beside its two sides."),
            ("options", "A hop's options", hopkeys.HOP_OPTIONS,
             "The keys under a hop's `options:`."),
            ("side", "A side's keys", hopkeys.ENDPOINT_KEYS,
             "A side written in the long form rather than as an address."),
            ("side-options", "A side's options", hopkeys.ENDPOINT_OPTIONS,
             "The keys of a side's own options, under it or under its"
             " `options:` - most may also be written in its address.")):
        table, out = _option_table(slug, registry)
        left += out
        blocks += [H(heading, slug), P(words), table]
    if left:
        blocks.append(P(f"{len(left)} more "
                        + ("key turns" if len(left) == 1 else "keys turn")
                        + " one named way of migkit's on or off; `migkit"
                        " doctor` says what each does on a hop that sets"
                        " one.", "mut"))
    return Page("options", "Hop options", "Hop options",
                "Every key of a hop and of its sides: its type, the engines"
                " that read it, and what it does.", blocks)


def _env_scan():
    """{name: default or None} of every `MIGKIT_*` the code names, with
    the default a `.get(name, "default")` gives - the names a Python
    constant is called (`config.MIGKIT_OWN`) left out."""
    tok = re.compile(r"(?<![A-Za-z0-9_.])MIGKIT_[A-Z][A-Z0-9_]*"
                     r"(?![A-Za-z0-9_{])")
    default = re.compile(r"""\.get(?:env)?\(\s*["'](MIGKIT_[A-Z0-9_]+)["']"""
                         r"""\s*,\s*["']([^"']*)["']\s*\)""")
    found, constants = {}, set()
    for path in sorted((ROOT / "migkit").rglob("*.py")):
        src = path.read_text()
        constants |= set(re.findall(r"^(MIGKIT_[A-Z0-9_]+)\s*=", src, re.M))
        for m in tok.finditer(src):
            found.setdefault(m.group(0), None)
        for name, value in default.findall(src):
            found[name] = value or None
    return {k: v for k, v in found.items() if k not in constants}


def environment():
    """[(name, kind, default, what)] of every variable an operator may
    set: the environment registry's where the code has one, else every
    name the code reads with the default it gives."""
    envkeys = optional("envkeys")
    if envkeys is not None:
        out = []
        for name in sorted(envkeys.VARS):
            var = envkeys.VARS[name]
            # the registry says which are the operator's to set
            if var.internal or var.tests or var.named_for_a_program:
                continue
            default = "" if var.default is None else (
                ("on" if var.default else "off") if var.kind == "flag"
                else str(var.default))
            out.append((name, var.kind, default, var.what))
        for pattern, var in getattr(envkeys, "FAMILIES", ()):
            out.append((pattern.pattern.strip("^$").replace(
                "[A-Z0-9_]+", "<HOP>_<SIDE>"), var.kind, "", var.what))
        return out
    return [(name, "", "" if d is None else str(d), "")
            for name, d in sorted(_env_scan().items())
            if not names_a_program(name)]


def page_environment():
    rows = environment()
    return Page("environment", "Environment", "Environment",
                "Every `MIGKIT_*` variable an operator may set: nothing needs"
                " one, and each only changes what its words say.",
                [H("Variables", "variables"),
                 Table(["Variable", "Kind", "Default", "What it does"],
                       [[f"`{n}`", k or "-", f"`{d}`" if d else "-",
                         Said(w) if w else "-"] for n, k, d, w in rows],
                       ids=[f"env-{n}" for n, _, _, _ in rows])])


def _help_paragraphs(text):
    """A command's help as click shows it: `\\b` keeps a paragraph's
    lines as they are."""
    import inspect
    text = inspect.cleandoc(text or "")
    out = []
    for para in re.split(r"\n\s*\n", text):
        if para.startswith("\b"):
            out.append(Pre(para.lstrip("\b").strip("\n")))
        elif para.strip():
            out.append(P(Said(" ".join(para.split()))))
    return out


def commands():
    """[(name, command)] of every command `migkit --help` lists."""
    from migkit import cli
    return [(n, c) for n, c in cli.main.commands.items() if not c.hidden]


def _usage(name, cmd):
    import click

    from migkit import cli
    with click.Context(cli.main, info_name="migkit") as parent:
        ctx = click.Context(cmd, info_name=name, parent=parent)
        return " ".join(cmd.collect_usage_pieces(ctx)).join(
            (f"migkit {name} ", ""))


def metrics():
    """[(family, type, help)] of every metric family `/metrics` serves,
    read off the dashboard's own help lines."""
    from migkit import ui
    tree = ast.parse(Path(ui.__file__).read_text())
    helps, types = {}, {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            for m in re.finditer(r"# HELP (\S+) ([^\n]+)", node.value):
                helps.setdefault(m.group(1), m.group(2).strip())
            for m in re.finditer(r"# TYPE (\S+) (\S+)", node.value):
                types.setdefault(m.group(1), m.group(2))
    return [(f, types.get(f, ""), h) for f, h in helps.items()]


def page_commands():
    from migkit import cli
    blocks = [H("Every command", "commands"),
              Table(["Command", "What it does"],
                    [[f"[`{n}`](#cmd-{n})",
                      Said(" ".join((c.help or "").split()).split(". ")[0]
                           .rstrip(".") + ".")] for n, c in commands()],
                    ids=[f"row-cmd-{n}" for n, _ in commands()])]
    for n, c in commands():
        blocks += [H(f"`{n}`", f"cmd-{n}", 3), Pre(_usage(n, c))]
        blocks += _help_paragraphs(c.help)
        opts = [p for p in c.params if getattr(p, "opts", None)
                and p.param_type_name == "option"]
        if opts:
            blocks.append(Table(["Option", "What it does"],
                                [[", ".join(f"`{o}`" for o in p.opts),
                                  Said(" ".join((p.help or "").split()))
                                  or "-"] for p in opts],
                                ids=[f"opt-{n}-{p.name}" for p in opts]))
    hidden = [(n, c) for n, c in cli.main.commands.items() if c.hidden]
    blocks += [H("Older names", "aliases"),
               P("Names earlier versions used, kept so scripts written for"
                 " them still run:"),
               Table(["Name", "Now"],
                     [[f"`{n}`", Said(" ".join((c.help or "").split())
                                      .replace("(alias) ", ""))]
                      for n, c in hidden])]
    blocks += [H("Metrics", "metrics"),
               P("`migkit report --serve` serves these at `/metrics`, for a"
                 " scraper to alert on:"),
               Table(["Family", "Type", "What it is"],
                     [[f"`{f}`", t or "-", Said(h)] for f, t, h in metrics()],
                     ids=[f"metric-{f}" for f, _, _ in metrics()])]
    return Page("commands", "Commands and metrics", "Commands",
                f"The {len(commands())} commands, every option they take,"
                " and the metrics the dashboard serves.", blocks)


def page_troubleshooting():
    blocks = [P("Every refusal migkit can stop with, read from where the code"
                " raises it: search this page for the words you saw. The"
                " parts filled in at run time are shown as `<...>`. Each"
                " message says what to do.")]
    groups = refusals_by_area()
    for i, (heading, texts) in enumerate(groups):
        blocks += [H(heading, f"area-{i}"), UL(Said(t) for t in texts)]
    total = sum(len(t) for _, t in groups)
    return Page("troubleshooting", "Troubleshooting", "Troubleshooting",
                f"{total} refusals, each in the words migkit prints it.",
                blocks)


def evidence_parts():
    """(the files, what the export is, [(table, columns)], the JSON's keys,
    the CSV's columns), read off an export of a hop with nothing done."""
    from migkit import export
    from migkit.config import Endpoint, Hop
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        hop = Hop(name="shop", engine="postgres",
                  source=Endpoint(host="db1.example.com", port=5432),
                  target=Endpoint(host="db2.example.com", port=5432),
                  databases=["shop"])
        hop.report_dir = lambda db=None: root / db if db else root
        doc = export.gather(hop)
        sections = [(t, h) for t, h, _ in export._sections(doc)]
        csv_head = export.table_rows(doc).splitlines()[0].split(",")
    return export.FILES, export.WHAT, sections, list(doc), csv_head


def page_evidence():
    files, what, sections, keys, csv_head = evidence_parts()
    kinds = {".json": "the whole record, for tools",
             ".html": "the page, for people",
             ".md": "the same as text",
             ".csv": "every table's numbers, a row each"}
    return Page("evidence", "The evidence of a migration", "Evidence",
                "What `migkit report` exports for a hop, and every run for"
                " its own run: what was done and what was measured.",
                [P(Said(what)),
                 H("The files", "files"),
                 Table(["File", "What it is"],
                       [[f"`{f}`", kinds.get(Path(f).suffix, "")]
                        for f in files]),
                 H("What the page and the text show", "sections"),
                 Table(["Part", "Columns"],
                       [[t, ", ".join(h)] for t, h in sections]),
                 H("The record's keys", "keys"),
                 P("`evidence.json` holds, at its top level: "
                   + ", ".join(f"`{k}`" for k in keys) + "."),
                 H("The numbers of every table", "csv"),
                 P("`tables.csv`, a row for each table: "
                   + ", ".join(f"`{c}`" for c in csv_head) + ".")])


# ---- recipes ----------------------------------------------------------------

class Recipe:
    """One source -> target pair: the hop, the capabilities of each side
    it leans on (held to the matrix - a recipe the code cannot do is not
    written), and what it adds to the words `init` says."""

    def __init__(self, anchor, heading, intro, hops, needs=(), code=None,
                 then=()):
        self.anchor, self.heading, self.intro = anchor, heading, intro
        self.hops, self.needs, self.code, self.then = hops, needs, code, then


RECIPES = (
    Recipe("managed-postgres-across-clouds",
           "A managed PostgreSQL on one cloud to a managed PostgreSQL on"
           " another",
           "The same engine on both sides and a different cloud under each."
           " The application keeps writing to the source while the copy runs"
           " and its changes follow; the switch waits until the two are"
           " proved equal, and a stream back is ready from the moment of it.",
           {"shop": {"goal": "cutover",
                     "source": "postgres://app@db1.example.com:5432/shop"
                               "?sslmode=verify-full",
                     "target": "postgres://app@db2.example.com:5432/shop"
                               "?sslmode=verify-full"}},
           needs=(("source", "stream"), ("source", "fence"),
                  ("target", "table-copy"), ("target", "sequences"),
                  ("target", "users")),
           then=("migkit move shop --mode cdc --drop --go",
                 "migkit check shop")),
    Recipe("mysql-to-managed-mysql-cut-over-by-migkit",
           "MySQL on servers of your own to a managed MySQL, the cutover"
           " run by migkit",
           "The cutover as one timed path: migkit holds the source's writes"
           " itself, waits for the stream to pass where the source stands,"
           " proves the two equal inside the outage, raises the target's"
           " counters, and undoes all of it if the budget passes.",
           {"orders": {"goal": "cutover",
                       "source": "mysql://app@10.0.0.5:3306/orders",
                       "target": "mysql://app@db2.example.com:3306/orders",
                       "options": {"cutover": {"budget": 30},
                                   "limits": {"outage": 30}}}},
           needs=(("source", "stream"), ("source", "fence"),
                  ("source", "confirm"), ("target", "table-copy"),
                  ("target", "sequences")),
           then=("migkit move orders --mode cdc --drop --go",)),
    Recipe("oracle-to-postgres-with-stored-code",
           "Oracle to PostgreSQL, with its stored code proved",
           "Two engines: the tables are built on the target in its own"
           " types, the rows copied and compared value by value in each"
           " engine's rendering, and every view, function, procedure and"
           " trigger converted by whichever way proves itself on it - run on"
           " both sides in a sandbox and compared, never trusted unrun.",
           {"erp": {"goal": "copy",
                    # the service, then the schema that is the database
                    "source": "oracle://app@db1.example.com:1521/"
                              "ORCLPDB1/ERP",
                    "target": "postgres://app@db2.example.com:5432/erp",
                    "options": {"proof_sandbox": "docker"}}},
           needs=(("source", "data"), ("source", "sequences"),
                  ("target", "table-copy")),
           code=("oracle", "postgres")),
    Recipe("sql-server-to-postgres",
           "SQL Server to PostgreSQL, followed until the switch",
           "A change of engine with the least downtime: the copy, then the"
           " source's changes carried as they happen, and the T-SQL"
           " procedures converted and proved beside them.",
           {"crm": {"goal": "cutover",
                    "source": "mssql://app@db1.example.com:1433/crm",
                    "target": "postgres://app@db2.example.com:5432/crm",
                    "options": {"proof_sandbox": "docker"}}},
           needs=(("source", "stream"), ("source", "fence"),
                  ("target", "table-copy")),
           code=("mssql", "postgres"),
           then=("migkit move crm --mode cdc --drop --go",)),
    Recipe("postgres-major-upgrade",
           "PostgreSQL to a newer major version of PostgreSQL",
           "What the new version removes or changes is checked before"
           " anything is written; then the new server is filled and"
           " followed - or, where the hop allows it, a copy of the source's"
           " pages is upgraded there - and switched to.",
           {"billing": {"goal": "upgrade",
                        "source": "postgres://app@db1.example.com:5432/"
                                  "billing",
                        "target": "postgres://app@db2.example.com:5432/"
                                  "billing"}},
           needs=(("source", "stream"), ("target", "table-copy"),
                  ("target", "params"))),
    Recipe("mongodb-with-a-way-back",
           "MongoDB to MongoDB, with a way back ready at the switch",
           "Collections copied and followed through the source's change"
           " stream; at the switch, the stream back to the old source"
           " starts, so going back loses nothing written since.",
           {"catalog": {"goal": "cutover",
                        "source": "mongodb://app@db1.example.com:27017/"
                                  "catalog",
                        "target": "mongodb://app@db2.example.com:27017/"
                                  "catalog"}},
           needs=(("source", "stream"), ("source", "fence"),
                  ("target", "table-copy"), ("target", "users")),
           then=("migkit move catalog --mode cdc --drop --go",)),
    Recipe("verify-two-databases",
           "Two databases proved to hold the same, nothing written to"
           " either",
           "After a move someone else made, or between a primary and a"
           " copy of it: every table and row that differs named, and"
           " nothing written anywhere.",
           {"proof": {"goal": "verify",
                      "source": "mysql://app@db1.example.com:3306/shop",
                      "target": "postgres://app@db2.example.com:5432/shop"}},
           needs=(("source", "data"), ("target", "data"))),
    Recipe("masked-test-copy",
           "Production to a staging database, as a masked subset",
           "The rows chosen and every row they refer to, masked where the"
           " hop says, so every reference in the copy resolves inside it.",
           {"staging": {"goal": "subset",
                        "source": "postgres://app@db1.example.com:5432/shop",
                        "target": "postgres://app@staging.example.com:5432/"
                                  "shop",
                        "options": {"subset": {"start": {"orders": "10%"},
                                               "children": True},
                                    "mask": ["customers.email"]}}},
           needs=(("source", "data"), ("target", "table-copy"))),
    Recipe("rehearse-then-run",
           "A rehearsal on a copy, and the real run forecast from it",
           "The whole path on a copy of the source into a target of its"
           " own, every phase timed, so the real hop's plan is forecast"
           " from what was measured rather than guessed.",
           {"shop-rehearsal": {"goal": "rehearse",
                               "source": "postgres://app@copy.example.com:"
                                         "5432/shop",
                               "target": "postgres://app@db3.example.com:"
                                         "5432/shop",
                               "options": {"rehearses": "shop"}},
            "shop": {"goal": "cutover",
                     "source": "postgres://app@db1.example.com:5432/shop",
                     "target": "postgres://app@db2.example.com:5432/shop"}},
           needs=(("source", "stream"), ("target", "table-copy"))),
)


def _recipe_engines(raw):
    from migkit import hopkeys
    src, dst, _ = hopkeys.sides(raw)
    names, aliases, _, _ = _engines()
    return aliases.get(src, src), aliases.get(dst, dst)


def recipe_blocks(recipe):
    """The blocks of one recipe, or SystemExit naming what the code does
    not do that the recipe says it does."""
    import yaml

    from migkit import capabilities, hopkeys
    from migkit.config import build_hop
    problems = [p for p in hopkeys.check({"hops": recipe.hops})
                if p.level == "error"]
    if problems:
        raise SystemExit(f"recipe {recipe.anchor}: "
                         + "; ".join(map(str, problems)))
    # the first hop is the recipe's; a second is the one it stands beside
    name, raw = next(iter(recipe.hops.items()))
    src, dst = _recipe_engines(raw)
    for side, cap in recipe.needs:
        engine = src if side == "source" else dst
        if not capabilities.implemented(engine, cap):
            raise SystemExit(f"recipe {recipe.anchor} leans on"
                             f" {capabilities.CAPABILITIES[cap]} on its"
                             f" {side} ({engine}), which the code does not"
                             " do")
    hop = build_hop(name, raw)
    said = hopkeys.explain_goal(hop)
    goal = hop.goal or hopkeys.DEFAULT_GOAL
    steps = [s.format(hop=name) for s in hopkeys.GOALS[goal]["next"]]
    steps += [s for s in recipe.then if s not in steps]
    blocks = [H(recipe.heading, recipe.anchor),
              P(recipe.intro),
              P(f"{title(src)} -> {title(dst)}. The hop, in"
                " `conf/hops.yaml` - two addresses and what it is for; the"
                " passwords are kept by `migkit init`, never in the file:"),
              Pre(yaml.safe_dump({"hops": recipe.hops}, sort_keys=False,
                                 default_flow_style=False).rstrip()),
              P("What `migkit init` and every dry run say of it:"),
              # the goal's four lines, and the cutover's first: its steps
              # are the same for every hop, and said once, on their page
              UL([Said(line.strip()) for line in said[:4]]
                 + [line.strip().rstrip(":")
                    + " ([the steps](goals.html#cutover))"
                    for line in said[4:5]]),
              P("The commands, in order - one that writes shows its plan"
                " until it is given `--go`:"),
              Pre("\n".join(steps))]
    opts = raw.get("options") or {}
    if opts:
        blocks.append(UL(f"`{k}`: " + Said(hopkeys.HOP_OPTIONS[k].what)
                         for k in opts))
    used = sorted({cap for _, cap in recipe.needs},
                  key=list(capabilities.CAPABILITIES).index)
    blocks.append(P("What it leans on, each done by the code for this pair: "
                    + "; ".join(capabilities.CAPABILITIES[c] for c in used)
                    + "."))
    if recipe.code:
        a, b = recipe.code
        got = next((k for s, t, k in stored_code()[2] if (s, t) == (a, b)),
                   None)
        if not got:
            raise SystemExit(f"recipe {recipe.anchor}: no converter of"
                             f" migkit's carries {a} to {b}")
        blocks.append(P("Stored code carried for this pair: "
                        + ", ".join(got) + " - each version, whoever wrote"
                        " it, run on both sides in the sandbox and"
                        " compared."))
    return blocks


def page_recipes():
    blocks = [P("Each recipe is one source -> target pair, from two"
                " addresses; for each, migkit's own decision engine finds"
                " the fastest, exact, consistent way. The"
                " hops below are checked against the configuration's own"
                " registry, and each capability a recipe leans on against"
                " what the code does - a recipe this version cannot do is"
                " not on this page."),
              UL(f"[{r.heading}](#{r.anchor})" for r in RECIPES)]
    for r in RECIPES:
        blocks += recipe_blocks(r)
    return Page("recipes", "Recipes", "Recipes",
                "Use cases as source -> target pairs, each from two"
                " addresses.", blocks)


# ---- the quickstart and the front page --------------------------------------

def _first_sentence(text):
    return " ".join((text or "").split()).split(". ")[0].rstrip(".") + "."


def quickstart_steps(hop="shop"):
    """[(what, commands, words)]: two addresses to a cutover, the words
    each step's own registry says."""
    from migkit import cli, hopkeys
    return [
        ("Install, and let migkit fill what this machine lacks",
         ["uv tool install migkit", "migkit doctor --install"],
         _first_sentence(cli.main.commands["doctor"].help)),
        ("Two addresses",
         ["migkit init",
          f"MIGKIT_SOURCE=postgres://app@db1.example.com/{hop}"
          f" MIGKIT_TARGET=postgres://app@db2.example.com/{hop}"
          f" MIGKIT_GOAL=cutover MIGKIT_HOP={hop} migkit init"],
         _first_sentence(cli.main.commands["init"].help)),
        ("What each side needs, before anything moves",
         [f"migkit assess {hop}"],
         _first_sentence(cli.main.commands["assess"].help)),
        ("Move: the copy, then the changes",
         [f"migkit move {hop} --mode full+cdc",
          f"migkit move {hop} --mode full+cdc --go"],
         hopkeys.MODES[("move", "full+cdc")]),
        ("Check: every row, both sides",
         [f"migkit check {hop}"],
         hopkeys.GOALS["copy"]["guarantees"]),
        ("Cut over",
         [f"migkit move {hop} --mode cdc --drop --go"],
         hopkeys.MODES[("move", "cdc --drop")]),
    ]


def page_quickstart():
    from migkit import guide
    blocks = [P("From two addresses to a cutover. Every command that writes"
                " shows its plan first and does nothing until `--go`.")]
    for i, (what, cmds, words) in enumerate(quickstart_steps(), 1):
        blocks += [H(f"{i}. {what}", f"step-{i}"), Pre("\n".join(cmds)),
                   P(Said(words))]
        if i == 2:
            blocks += [P("In a terminal, `init` asks, and nothing else:"),
                       UL(Said(q) for q in (guide.ASK_SOURCE,
                                            guide.ASK_TARGET,
                                            guide.ASK_KEEP)),
                       P("Without one it reads the addresses from"
                         " `MIGKIT_SOURCE` and `MIGKIT_TARGET`, and the"
                         " passwords from `MIGKIT_SOURCE_PASSWORD` and"
                         " `MIGKIT_TARGET_PASSWORD`. It connects, says what"
                         " it found, proposes what the hop is for, and"
                         " writes the hop.")]
        if i == 6:
            blocks.append(P("Without `cutover:` in the hop's options, the"
                            " steps around it are the operator's, in the"
                            " order [the cutover section](goals.html#cutover)"
                            " gives; with it, migkit runs them as one timed"
                            " path that undoes itself."))
    return Page("quickstart", "Five minutes, two addresses",
                "Quickstart",
                "Install, two addresses, assess, move, check, cut over.",
                blocks)


PAGES = (("index", "Overview"), ("quickstart", "Quickstart"),
         ("engines", "Engines"), ("goals", "Goals and modes"),
         ("recipes", "Recipes"), ("options", "Hop options"),
         ("environment", "Environment"), ("commands", "Commands"),
         ("troubleshooting", "Troubleshooting"), ("evidence", "Evidence"),
         ("releases", "Releases"))


def page_index():
    from migkit import hopkeys
    names, matrix = _engine_rows()
    shown = [n for n in names if n not in ("hetero", "generic")]
    blocks = [
        Pills([f"[{title(n)}](engines.html#engine-{n})" for n in shown]
              + ["any two of them as a pair",
                 "any other SQL engine by its URL"]),
        P(f"**{POSITION}**, and says which it"
          " chose and why. Around the move it does what a mover leaves"
          " out: it prepares the target, follows the source's changes,"
          " proves every row and object arrived, repairs what did not with"
          " the undo kept, and cuts over with the least downtime.", "rule"),
        H("Five minutes", "start"),
        Pre("\n".join(c for _, cmds, _ in quickstart_steps() for c in cmds
                      if not c.startswith("MIGKIT_"))),
        P("Each step, and what it does on each side:"
          " [the quickstart](quickstart.html)."),
        H("What a hop is for", "goals"),
        Table(["Goal", "What it is for"],
              [[f"[`{g}`](goals.html#goal-{g})", Said(v["what"])]
               for g, v in hopkeys.GOALS.items()]),
        H("The manual", "manual"),
        UL(["[Quickstart](quickstart.html): install, two addresses, assess,"
            " move, check, cut over",
            f"[Engines](engines.html): {len(shown)} engines, what each can"
            " do, the shapes of table, stored code, the managed services",
            "[Goals and modes](goals.html): what each does on the source and"
            " the target, and what it guarantees",
            "[Recipes](recipes.html): use cases as source -> target pairs",
            "[Hop options](options.html): every key a hop may say",
            "[Environment](environment.html): every `MIGKIT_*` variable",
            "[Commands](commands.html): every command and option, and the"
            " metrics",
            "[Troubleshooting](troubleshooting.html): every refusal, in"
            " migkit's words",
            "[Evidence](evidence.html): what the evidence export holds",
            "[Releases](releases.html): each release's changes"]),
        H("Safety", "safety"),
        UL(["Every command that writes shows its plan first, starting with"
            " what each side will see, and writes nothing until `--go` or"
            " `--apply`.",
            "What each goal does on the source is said before it runs:"
            " [goals](goals.html). No goal turns the cutover migkit drives"
            " on; a hop turns it on itself.",
            "Every repair keeps its undo, and `migkit rollback` puts it"
            " back.",
            "`migkit check` never writes to either side."]),
    ]
    return Page("index", "migkit", "Overview",
                "Move, follow, prove and cut over databases between any two"
                " engines.", blocks,
                description="migkit moves, follows, proves and cuts over"
                            " databases between any two engines.")


# ---- releases ---------------------------------------------------------------

def _git(*args):
    try:
        p = subprocess.run(["git", *args], cwd=ROOT, capture_output=True,
                           text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return p.stdout if p.returncode == 0 else None


def releases():
    """[(tag, date, {heading: [(words, sha)]})] newest first, or None
    where the history cannot be read (a copy without git, or without its
    tags)."""
    import gen_changelog

    from migkit.wording import without_programs
    got = _git("tag", "--list", "v*", "--sort=-creatordate")
    tags = [t for t in (got or "").split() if t]
    if not tags:
        return None
    unsaid = _unsaid()
    out = []
    for i, tag in enumerate(tags):
        prev = tags[i + 1] if i + 1 < len(tags) else ""
        rng = f"{prev}..{tag}" if prev else tag
        log = _git("log", "--no-merges", "--format=%s|%h", rng) or ""
        entries = [ln.split("|", 1) for ln in log.splitlines() if "|" in ln]
        buckets = gen_changelog.group(
            [(without_programs(s, unsaid, "a program migkit drives"), sha)
             for s, sha in entries])
        date = (_git("log", "-1", "--format=%cs", tag) or "").strip()
        out.append((tag, date, {h: buckets[h]
                                for h in gen_changelog.headings(buckets)}))
    return out


def page_releases(got):
    from migkit import __version__
    tags = [t for t, _, _ in got]
    this = f"v{__version__}"
    blocks = [P(f"This manual describes migkit {__version__}"
                + ("." if this in tags else
                   f", which is not tagged yet; its changes are those made"
                   f" since {tags[0]}, and are written here when it is."))]
    for tag, date, buckets in got:
        blocks.append(H(f"{tag}" + (f" - {date}" if date else ""),
                        f"release-{tag}"))
        for head, entries in buckets.items():
            blocks += [P(f"**{head}**"),
                       UL(Said(f"{w} ({sha})") for w, sha in entries)]
    return Page("releases", "Releases", "Releases",
                "Each tagged release's changes, from its commits.", blocks)


# ---- the outputs ------------------------------------------------------------

BUILDERS = {"index": page_index, "quickstart": page_quickstart,
            "engines": page_engines, "goals": page_goals,
            "recipes": page_recipes, "options": page_options,
            "environment": page_environment, "commands": page_commands,
            "troubleshooting": page_troubleshooting,
            "evidence": page_evidence}


def _site_base():
    """Where the site is published, for the links the README carries."""
    cname = ROOT / "docs" / "CNAME"
    try:
        host = cname.read_text().strip()
    except OSError:
        host = ""
    return f"https://{host}/site/" if host else "docs/site/"


def _splice(text, name, body, where):
    start, end = f"<!-- gen_docs: {name} -->", f"<!-- /gen_docs: {name} -->"
    a, b = text.find(start), text.find(end)
    if a < 0 or b < a:
        raise SystemExit(f"{where} has no {start} ... {end} markers")
    return text[:a + len(start)] + "\n" + body.strip("\n") + "\n" \
        + text[b:]


def readme_sections():
    """{marker: Markdown} of the README's generated parts."""
    names, matrix = _engine_rows()
    engines = [n for n in names if n not in ("hetero", "generic")]
    base = _site_base()
    wants = re.search(r'requires-python\s*=\s*">=\s*([0-9.]+)"',
                      (ROOT / "pyproject.toml").read_text())
    ci = f"{SOURCE}/actions/workflows/ci.yml"
    badge = "\n".join([
        f"[![ci]({ci}/badge.svg)]({ci})",
        f"[![engines](https://img.shields.io/badge/engines-{len(engines)}"
        "-2a78d6)](#supported-engines)",
        "[![pairs](https://img.shields.io/badge/cross--engine-any_pair"
        "-0ca30c)](#cross-engine-hetero)",
        "[![python](https://img.shields.io/badge/python-"
        f"{wants.group(1) if wants else '3'}+-3776ab)](pyproject.toml)",
        "[![license](https://img.shields.io/badge/license-"
        "AGPL--3.0--or--later-0ca30c)](LICENSE)"])
    steps = quickstart_steps()
    quick = [Pre("\n".join(c for _, cmds, _ in steps for c in cmds
                           if not c.startswith("MIGKIT_")))]
    by_role = {}
    for n in names:
        by_role.setdefault(role(n, matrix[n]), []).append(n)
    table = Table(["Role", "Engines"],
                  [[r, ", ".join(f"{title(n)} (`{n}`)" for n in ns)]
                   for r, ns in by_role.items()])
    aliases = ", ".join(f"`{a}`" for a in sorted(_engines()[1]))
    services = managed_services()
    engines_md = [
        P(f"{len(engines)} engines, any two of them as a pair; {POSITION}."
          f" What each can do, cell by cell, is on [the engines"
          f" page](engines.html)."),
        table,
        P(f"Also written: {aliases}."),
        P("Managed services recognised: "
          + "; ".join(sorted({f"{s} ({p})" for p, s, _ in services}))
          + "."),
    ]
    cmds = [Table(["Command", "What it does"],
                  [[f"`{n}`", Said(_first_sentence(c.help))]
                   for n, c in commands()])]
    return {"badge": badge,
            "quickstart": markdown(quick, base),
            "engines": markdown(engines_md, base),
            "commands": markdown(cmds, base),
            "goals": markdown([Table(["Goal", "What it is for"],
                                     [[f"`{g}`", Said(v["what"])]
                                      for g, v in _goals().items()])],
                              base)}


def _goals():
    from migkit import hopkeys
    return hopkeys.GOALS


def goals_md_sections():
    return {"goals": markdown([goals_table()]),
            "modes": markdown([modes_table()]),
            "cutover": markdown(cutover_blocks()),
            "limits": markdown(limits_blocks())}


def site_pages():
    pages = [BUILDERS[slug]() for slug, _ in PAGES if slug in BUILDERS]
    got = releases()
    if got is not None:
        pages.append(page_releases(got))
    return pages


def outputs():
    """{path under the repository: its text} of everything written here.
    The releases page is left out where the history cannot be read."""
    global _MEMO
    _MEMO = {}
    try:
        return _outputs()
    finally:
        _MEMO = None


def _outputs():
    from migkit import troubleshooting
    pages = site_pages()
    nav = [p for p in pages]
    if not any(p.slug == "releases" for p in nav):
        nav.append(Page("releases", "Releases", "Releases", "", []))
    out = {}
    for p in pages:
        out[str(SITE / f"{p.slug}.html")] = page_html(p, nav)
    index = next(p for p in pages if p.slug == "index")
    out["docs/index.html"] = page_html(index, nav, prefix="site/")
    readme = (ROOT / "README.md").read_text()
    for name, body in readme_sections().items():
        readme = _splice(readme, name, body, "README.md")
    out["README.md"] = readme
    goals = (ROOT / "docs" / "goals-and-modes.md").read_text()
    for name, body in goals_md_sections().items():
        goals = _splice(goals, name, body, "docs/goals-and-modes.md")
    out["docs/goals-and-modes.md"] = goals
    out["docs/troubleshooting.md"] = troubleshooting.render(
        refusals_by_area())
    return out


def stale(got=None):
    """The paths whose text on disk is not what the code writes."""
    got = outputs() if got is None else got
    out = []
    for path, text in got.items():
        f = ROOT / path
        if not f.exists() or f.read_text() != text:
            out.append(path)
    return out


def main(argv):
    got = outputs()
    behind = stale(got)
    if "--check" in argv:
        for path in behind:
            print(f"{path} is behind the code")
        return BEHIND if behind else 0
    for path in behind:
        f = ROOT / path
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(got[path])
        print(f"wrote {path}")
    print(f"{len(got)} pages, {len(behind)} written again")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
