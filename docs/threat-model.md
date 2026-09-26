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
  before anything is written
  (`test_a_problem_can_be_reported_without_handing_anything_over.py`).

## The data in reports

A report shows rows, because a difference that names the row is the one
someone can fix. Keys and values appear in drilldowns and in repair
plans. The hop option `mask` shows them as salted hashes instead. Equal
values show as equal tokens, so a difference is still visible. The salt
is readable by this user only (`test_the_drilldown_can_be_masked.py`).

What leaves the machine never carries values:
* a notification names each finding's check, scope and status, not its
  detail
* the diagnostics bundle drops every finding's detail

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
