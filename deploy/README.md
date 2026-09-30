# Running migkit as a migration service on a VM

migkit can stand in for a managed migration service (DMS/DTS) on a trusted VM:
full load once, then continuously verify (and repair) the incremental stream.

`sync --mode`:
- `verify` - read-only, consistent-snapshot check (exit 1 on diff)
- `seed` - align schema -> bulk load (best mover) -> reconcile rows/sequences -> verify
- `stream` - start/continue CDC, then delta-verify each cycle (O(changes))
- `migrate` - full, then incremental (the DMS "full load + CDC")

Add `--go` to execute (dry-run otherwise), `--serve` to loop forever.

## docker
    cp conf/hops.example.yaml deploy/conf/hops.yaml   # fill endpoints, chmod 600
    echo "MIGKIT_METRICS_TOKEN=<a long random value>" > deploy/.env
    docker compose -f deploy/docker-compose.yml up -d --build
    docker compose -f deploy/docker-compose.yml logs migkit-ui   # the address

The dashboard is published on the VM's own 127.0.0.1:8899, never on its
network: open the address it printed through an SSH tunnel to the VM's
port 8899, which opens the view once. To share it, put a proxy that signs
people in before it - it names each person in a header of its own
(`MIGKIT_UI_USER_HEADER`, e.g. `X-Auth-Request-Email`) and proves it is
the proxy with a secret it sends in `X-Migkit-Proxy`
(`MIGKIT_UI_PROXY_SECRET`); without the secret the header is not
believed, and the view does not start with a header and no secret. Add
the name the proxy reaches it by to `MIGKIT_UI_HOSTS`. A hop's `access`
option says who may see it and who may hold its tail.

## systemd
    sudo cp -r . /opt/migkit && /opt/migkit/bootstrap.sh
    sudo install -Dm600 conf/hops.yaml /etc/migkit/hops.yaml
    sudo cp deploy/migkit-sync@.service /etc/systemd/system/
    sudo systemctl enable --now migkit-sync@my-hop

State (`/state` or `/var/lib/migkit`) holds resume checkpoints and reports, so
a restart continues where it stopped. The source is only ever read; the target
gets the migrated data and nothing else.

For continuous CDC, run `migkit move <hop> --mode cdc --go` and let migkit
verify the stream. Where an engine has no native change feed, migkit stands
up and supervises its own streaming pipeline behind the same command.

## Alerts and notifications

`/metrics` on the dashboard (`migkit report --serve`) carries each hop's last
verdict (`migkit_verdict`, one sample a verdict: same, different, error,
incomplete, none) and, for a running change tail, how far behind it is,
whether it is still going round its loop, and whether it stopped on an
error - and what works inside migkit, under migkit's names: the copy's rows
per table and rows a second, each check's seconds, the verify's progress,
every table's rows on each side and whether they agree as one run took
them, the last cutover's outage and phases, the hop's own change stream's
parts and its reader's readings. It is read from a snapshot taken every 15
seconds, whose age and failed readings it carries too
(`migkit_collect_age_seconds`, `migkit_collect_errors_total`). Scrape it
with the bearer token:

    scrape_configs:
      - job_name: migkit
        authorization: {credentials_file: /etc/prometheus/migkit-token}
        static_configs: [{targets: ["migkit-ui:8899"]}]

A person signed in sees the metrics of the hops their `access` gives them.
`prometheus-alerts.yml` here has rules over those metrics; load it with
`rule_files:` and change the thresholds to fit. `grafana/migkit.json` is a
dashboard over the same metrics (import it and pick the Prometheus source).
The families renamed in 0.x (`migkit_hop_status`, `migkit_check_pass`,
`migkit_tail_changes_applied`, ...) are still served beside their new
names until 1.0; their help says what replaces them.

With `OTEL_EXPORTER_OTLP_ENDPOINT` set (the standard OpenTelemetry
variables), every command is a trace: a span per command, per check of each
database, per table copied and per cutover phase, with the ways the engine
chose as events.

Every run of a command on a hop also writes its own log,
`reports/<hop>/runs/<run>.jsonl`: one JSON line an event, in UTC, in
migkit's words, with no secret in it; the warnings of the libraries under
the engines go there instead of the terminal. The diagnostics bundle
(`MIGKIT_DIAGNOSE=<file>.zip migkit doctor`) carries the last ones.

To be told without Prometheus, give the hop receivers, kept where its
passwords are kept:

    options:
      notify:
        to:
          - env:SLACK_HOOK          # Slack, Teams, Google Chat, Discord, LINE,
          - keyring:migkit-lark     # Lark/Feishu, Telegram, email, SNS, ...
          - pagerduty:env:PD_KEY    # one incident per hop, resolved when it clears
        events: [verdict, drift, lag, cutover, approval, failure, fallback, done]
        lag: 300                    # seconds a change tail may be behind

or set `MIGKIT_NOTIFY` (addresses separated by commas) for every hop. What
is sent names each finding's check and table, never the rows' values; an
error is named by its kind, its words kept in the run's log unless the
hop's `notify` says `detail: true`.

## What leaves the machine

Four things, each only where it is configured: notifications, traces, the
model asked for help (`MIGKIT_AI`), and the evidence's digests put in the
run state's bucket. Each goes through one gate, which cleans it of every
secret and asks two settings first, the stricter winning:

    MIGKIT_EGRESS=none                   # nothing leaves, for every hop
    MIGKIT_EGRESS=notify                 # only notifications
    MIGKIT_EGRESS_TO=*.corp.example.com,10.0.0.0/8   # and only there

    options:
      egress:
        allow: [notify]
        to: [mattermost.corp.example.com]

In a network with no way out, `MIGKIT_EGRESS=none` makes sure nothing
tries: no address is read, no name looked up, no connection opened.

## In the team's pipelines

- GitHub Actions: `uses: arkashira/migkit@v1` with `hop:` (and `command:`,
  `args:`, `config:`); the job is gated on the verdict, and the run's
  evidence is its artefact and summary (`action.yml`).
- Argo Workflows: `argo/migkit-workflowtemplate.yaml`, the image above, the
  verdict an output parameter and the evidence an output artefact.
- Airflow: `from migkit.integrations import MigkitOperator`; Dagster:
  `migkit.integrations.migkit_op(name, command, hop)`.

Each runs the command a person would type, and nothing else.
