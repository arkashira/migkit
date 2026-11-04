# Threat model

What migkit touches, what it keeps, and what stands between those and
someone who should not have them. This page describes the code as it is,
and each claim below names the test that holds it.

## What migkit is given

* **Two database endpoints per hop**, with an account on each. These come
  from `hops.yaml` or from Vault. The file is the operator's, and
  `conf/hops.yaml` is ignored by git.
* **Programs it runs** for the bulk paths. They are found on the path,
  and their options are checked against the installed build before a move
  writes anything (`test_the_installed_build_takes_the_options.py`).

## What it writes, and where

* **The source is read, not written.** Emptying a table and applying
  changes refuse the source side outright (`_target_only`). A business
  rule, which is the operator's own SQL, runs in a read-only transaction
  (`test_business_rules.py`). A check does not write to the target
  either (`test_a_check_does_not_write_to_the_target.py`).

  The exception is the setup a change stream needs. On PostgreSQL that
  is a publication. On MySQL it is a replication user with a fresh
  random password (`test_replication_user_password.py`). These are
  printed as a plan, and they run only with `--go`.
* **The target is written** by moves, repairs, the change tail and the
  replication migkit sets up. Each repair keeps its undo. Each operation
  is appended to `reports/<hop>/changelog.jsonl`, which stays on the
  machine and is never written into the target.
* **The machine running migkit** gets reports, checkpoints and, while a
  dump-and-load move runs, a local copy of the source's data. That copy
  is readable by this user only, and it is removed when the move ends,
  whichever way it ends (`test_the_local_copy_is_private_and_goes.py`).
  `at_rest` cannot seal a program's own dump; `at_rest.scratch` puts the
  copy on a volume the operator keeps encrypted or in memory
  (`test_a_programs_files_go_where_at_rest_says.py`). Every location and
  its protection is in [At rest](#at-rest).

## At rest

What stays on the machine that runs migkit after a run, where it lives,
and what protects it. The paths are the defaults: `MIGKIT_REPORTS` moves
the report directories, `XDG_STATE_HOME` the state directory, and
`MIGKIT_TELEMETRY_SPOOL` the telemetry spool. The directories migkit
makes are `0700`; the state it writes through `durable` is `0600`.

| what | where | protection |
|---|---|---|
| summaries and run records: `summary.json`, `verdict.json`, `report.html`, `runs/<run>.jsonl`, `commands.log` | `reports/<hop>/` | not encrypted. The run log and `commands.log` go through the redactor, so no secret the hop holds is written (`test_a_run_keeps_its_own_log.py`) |
| the change record | `reports/<hop>/changelog.jsonl` | not encrypted. Each entry carries the hash of the one before, so an entry edited, removed or moved shows (`migkit/audit.py`) |
| values: drilldowns, undo statements, two-way conflicts and parked rows, the evidence export (`data-*`, `undo*`, `conflicts*`, `evidence.*`, `tables.csv`) | the report directory | age-encrypted to the hop's `at_rest` recipients where it names them, plain otherwise (`migkit/evidence.py`, `HOLDS_VALUES`) |
| a tail's spool of changes set aside for a table | `tail-spool/` beside the tail's state | each frame is pickled and compressed, and sealed to the `at_rest` recipients where named, plain otherwise (`tailtables._pack`) |
| positions, checkpoints, cutover and move state, the lease | the report directory | written whole and synced, `0600`, by `durable`; not sealed. A position names a point in a stream, not a row |
| the attempts ledger (`attempts*.json`) | the report directory | written by `durable`; holds each unit's timings and which run closed it; not sealed |
| restore points, whole rows for an undo | `state_home()/restore-points/<hop>` (a mirror), or the bucket of `state: {backend: s3}` | sealed on the way out, by the first that applies: the `at_rest` recipients, then `MIGKIT_STATE_KEY`, else plain (`state.sealed`) |
| the local copy of a dump-and-load source | the report directory, or `at_rest.scratch` | `0700` while it exists and removed when the move ends; `at_rest.scratch` names a volume the operator chose and keeps. Not sealed by migkit (`evidence.program_path`) |
| the masking key and salt | `mask.key` in the state directory (`0600`); `mask.salt` in the hop's report directory | a shown value is a keyed HMAC-SHA256; the key never sits in a report (`migkit/masking.py`) |
| telemetry events | `state_home()/telemetry`, `0700` | typed fields only, no free text, a bounded local spool, nothing sent (`migkit/telemetry.py`) |

Three things are done to a value, and each only where its option asks:
it is **encrypted** in a file that holds values, where the hop names
recipients; it is **masked** in a report, where the hop's `mask` option
asks; and it is **redacted** from every text migkit writes or sends,
always (`migkit/redact.py`).

## Secrets

* **Passwords never go on a command line.** The wrapped programs are
  given them through their environment, as `PGPASSWORD` and `MYSQL_PWD`
  (`test_no_program_is_handed_a_password.py`). The run's `commands.log`
  records each command line with every secret removed.
* **Notification addresses are secrets.** A webhook's address is its
  password, so it is never printed or logged
  (`test_alerts_and_notifications.py`).
* **The diagnostics bundle** (`MIGKIT_DIAGNOSE`) takes out passwords,
  tokens, keys, notification addresses and credentials inside addresses
  before anything is written, from the configuration and from the run
  logs, the choices and each database's `commands.log` it carries
  (`test_a_problem_can_be_reported_without_handing_anything_over.py`).
* **The run log** (`reports/<hop>/runs/<run>.jsonl`) is written through
  the same cleaner as everything that leaves: no secret the hop holds in
  any form, none by its shape, no program named
  (`test_a_run_keeps_its_own_log.py`).

## The data in reports

A report shows rows, because a difference that names the row is the one
someone can fix. Keys and values appear in drilldowns and in repair
plans. The hop option `mask` shows them as keyed HMACs instead. Equal
values show as equal tokens, so a difference is still visible. The key
has two parts: the hop's salt in its report directory, and a secret that
stays in the user's state directory and never in a report, so a report
shared with its salt cannot be tested against guessed values
(`test_the_drilldown_can_be_masked.py`, `test_a_shown_hash_is_keyed.py`).

Where the hop names recipients for the files holding values, those files
are sealed before they are written: drilldowns, a repair's undo, a
two-way tail's conflicts and the evidence export become age files to the
hop's keys and are read back with the operator's own (`MIGKIT_IDENTITY`)
(`evidence.HOLDS_VALUES`, `evidence.report_path`; `test_what_holds_values
_is_kept_encrypted.py`). The report's own summaries are not among them.

## What leaves the machine

migkit sends four things off the machine of its own accord, and only
where the operator configured each:

| what | where to | configured by |
|---|---|---|
| notifications | the hop's receivers | `notify`, `MIGKIT_NOTIFY` |
| the run's spans | an OpenTelemetry collector | `OTEL_EXPORTER_OTLP_*` |
| findings, and code migkit could not convert | a language model | `MIGKIT_AI`, `code_model` |
| the evidence's digests | the run state's bucket | `state: {backend: s3}` |

Each goes through one gate (`migkit/egress.py`), which:
* asks the environment (`MIGKIT_EGRESS`, `MIGKIT_EGRESS_TO`) and the
  hop (`egress: {allow, to}`) whether it may go, and to which hosts -
  before a receiver's address is read, a name looked up or a connection
  opened. With `MIGKIT_EGRESS=none` nothing is attempted, proved by a test
  in which every connection and every name lookup fails the test
  (`test_nothing_leaves_where_nothing_may.py`)
* cleans it of every secret the hop holds, in every form it reaches
  text in, of any secret by its shape, and of the programs migkit drives

What each carries:
* a notification names each finding's check, scope and status, never its
  detail; an error that stopped something is named by its kind - its
  words can quote the row it was writing - unless the hop's `notify`
  says `detail: true`, and never where the hop masks values
* a span carries names and numbers: a check's status, a table's rows, a
  way's words, a failure's kind
* the model is sent each finding's check, scope, status and category, and
  code only with `MIGKIT_AI_SHARE=code`; a finding's detail only with
  `MIGKIT_AI_SHARE=detail` on a hop that masks nothing. The key goes only
  over TLS: an address that is not `https` is refused, this machine's own
  aside (a local model)
* the bucket is given the evidence's digests, not the evidence

What is written for someone to hand over:
* the evidence export: every table's counts and digests, with no secret
  and no program named; the values of a two-way conflict masked where the
  hop masks them
* the diagnostics bundle, which drops every finding's detail

## The web view and the API

`migkit report --serve` serves the web view and its API (`migkit.web`,
`docs/web.md`). It listens on 127.0.0.1 unless `MIGKIT_UI_BIND` says
otherwise, and then only with TLS - its own certificate, or a reverse
proxy named in `MIGKIT_WEB_PROXIES`, the only connection answered and
the only forwarded headers read. The one exception is `MIGKIT_WEB_PLAIN=on`, which the shipped container sets and which is said again at every start (`migkit/web/server.py`). It answers only to the name in
`MIGKIT_WEB_URL` and those `MIGKIT_UI_HOSTS` adds.
* Who: signed in by the organisation's provider (OpenID Connect with
  PKCE, the ID token checked against the provider's keys; or SAML, only a
  signed answer to a request the view made, once), or the one local
  operator of an install without one (Argon2id, a doubling wait after
  five wrong passwords, set through a one-time printed address).
  Provisioned by SCIM, a deactivated person is signed out at once.
* What: a role per hop from the hops file's `access` only; a hop not
  the person's answers as one that is not there; every refusal and every
  change written to the hop's record or the view's own.
* Sessions: a random cookie, HttpOnly, SameSite=Lax, `__Host-` and
  Secure over https, kept on the server as its digest, new at each
  sign-in, ended idle, at its term and by deprovisioning; every change
  carries the page's CSRF token and comes from the view's own origin.
  API tokens are kept as digests and end with their person.
* What runs: only the command line's own commands, from a fixed list,
  every value checked before it is an argument; never a shell.
* What it shows: the hop's secrets hidden, no program named, its own
  script and style only (no inline code), no framing, nothing cached.
* `/metrics`: a scraper with `MIGKIT_METRICS_TOKEN`, or a person signed
  in, their own hops.
* Not covered: a signed SAML authentication request, single logout
  (encrypted SAML assertions are supported and enforced when
  `MIGKIT_WEB_SAML_ENCRYPTED=on`).

(`test_web_*.py`)

## The dashboard

`migkit ui` (the earlier view) listens on 127.0.0.1 unless
`MIGKIT_UI_BIND` says otherwise (the shipped container, published on the
host's 127.0.0.1 only). It answers only to this machine's names and those
`MIGKIT_UI_HOSTS` adds. The printed address opens it once; a proxy that
signs people in is believed only with its secret
(`MIGKIT_UI_PROXY_SECRET`), and does not start without one. Each person
sees and operates the hops their `access` gives them, `/metrics`
included, which a scraper reads with the bearer `MIGKIT_METRICS_TOKEN`.
A tail is held only for a database of the hop named. The page runs only
its own script, named by its digest in its content security policy, and
puts every name in as text
(`test_the_dashboard_holds_a_tail_only_for_its_page.py`,
`test_the_dashboard_says_the_verdict.py`).

## Connections

`assess` and `doctor` say of every engine migkit reaches over a network
whether the connection is verified, and fail one that is not. A
connection counts as local only on this machine or through the hop's
own SSH tunnel; one carried by a tunnel's command (a cloud's port
forward, `kubectl port-forward`) is "unknown", a warning, unless the
server's certificate is checked end to end
(`test_tls_is_verified_only_where_it_is.py`).

## Processes

A move that is stopped stops the programs it started. A move killed so
that it cannot answer leaves a list of the programs still running. The
next move finds the list, and waits instead of writing beside them
(`test_a_stopped_move_leaves_nothing_writing.py`).

## Restore points

A restore point holds whole rows: the undo of a repair. It is kept in a
mirror directory, or in a bucket, which may have more readers than this
machine. It is sealed before it leaves the working directory
(`state.sealed`), by the first of these that applies:
* the hop's `at_rest` recipients, where it names them - the same age
  encryption as the other files that hold values
* `MIGKIT_STATE_KEY`:
  * Fernet, with a key derived from the passphrase and the point's own
    salt
  * without the passphrase, or with another one, it is not read back
* neither: the point is written as it is

(`test_state.py`)

## Releases

A release carries a CycloneDX bill of materials of the wheel as
installed, and build provenance signed through GitHub's Sigstore
attestation (`.github/workflows/release.yml`).

## Not covered yet

* The summaries and run records (`report.html`, `summary.json`,
  `verdict.json`, `runs/*.jsonl`, `commands.log`), the change record, the
  positions, checkpoints and the attempts ledger are not encrypted at rest.
  For these, use the machine's disk encryption; `mask` covers the values
  a summary shows.
* Without `at_rest` recipients, a drilldown, an undo, a tail's spool and the
  evidence export are plain files in the private report directory. A
  dump's own files are plain while the move runs, whatever the recipients
  (see `at_rest.scratch`).
