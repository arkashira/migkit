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
plans. The hop option `mask` shows them as salted hashes instead. Equal
values show as equal tokens, so a difference is still visible. The salt
is readable by this user only (`test_the_drilldown_can_be_masked.py`).

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
the only forwarded headers read. It answers only to the name in
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
* Not covered: an encrypted SAML assertion, a signed authentication
  request, single logout.

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
machine. With `MIGKIT_STATE_KEY` set, a restore point is encrypted
before it leaves the working directory:
* Fernet, with a key derived from the passphrase and the point's own
  salt
* without the passphrase, or with another one, it is not read back

(`test_state.py`)

## Releases

A release carries a CycloneDX bill of materials of the wheel as
installed, and build provenance signed through GitHub's Sigstore
attestation (`.github/workflows/release.yml`).

## Not covered yet

* The report directory's own files (drilldowns and summaries) are not
  encrypted at rest. Use the machine's disk encryption for them, and
  `mask` for the values in them.
