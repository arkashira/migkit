# Running migkit as a migration service on a VM

migkit can stand in for a managed migration service (DMS/DTS) on a trusted VM:
full load once, then continuously verify (and repair) the incremental stream.

`sync --mode`:
- `verify` — read-only, consistent-snapshot check (exit 1 on diff)
- `seed` — align schema -> bulk load (best mover) -> reconcile rows/sequences -> verify
- `stream` — start/continue CDC, then delta-verify each cycle (O(changes))
- `migrate` — full, then incremental (the DMS "full load + CDC")

Add `--go` to execute (dry-run otherwise), `--serve` to loop forever.

## docker
    cp conf/hops.example.yaml deploy/conf/hops.yaml   # fill endpoints, chmod 600
    docker compose -f deploy/docker-compose.yml up -d --build
    # dashboard: http://<vm>:8899

## systemd
    sudo cp -r . /opt/migkit && /opt/migkit/bootstrap.sh
    sudo install -Dm600 conf/hops.yaml /etc/migkit/hops.yaml
    sudo cp deploy/migkit-sync@.service /etc/systemd/system/
    sudo systemctl enable --now migkit-sync@my-hop

State (`/state` or `/var/lib/migkit`) holds resume checkpoints and reports, so
a restart continues where it stopped. The source is only ever read; the target
gets the migrated data and nothing else.

For platform-grade CDC, generate Debezium configs with
`migkit move <hop> --mode cdc --via debezium` and let migkit verify the stream.
