# The web view and the API: setting it up

`migkit report --serve` serves the web view, its REST API and `/metrics`.
What it shows and what each role may do is on the generated page
([docs/site/web.html](site/web.html)); this page is how to run it.

## On one machine, for one operator

```sh
migkit report --serve --port 8899
# migkit: set the operator's password at http://127.0.0.1:8899/setup#<code>
```

With no identity provider set, the view has one local account - the
operator, admin of every hop. The address it prints sets its password
once (12 characters at the least) within 30 minutes; the code sits after
`#`, which no browser sends anywhere. Lost the password: start it again
with `MIGKIT_WEB_RESET=1` and open the new address. Five wrong passwords
in a row and each next one doubles the wait.

It listens on 127.0.0.1 only (`MIGKIT_UI_BIND`).

In the shipped container (`deploy/docker-compose.yml`) it listens on every
address inside the container, published on the host's 127.0.0.1 only, and
says so with `MIGKIT_WEB_PLAIN=on` - the one case plain HTTP beyond a
loopback is taken, said again at every start. The set-up address is in the
container's log; `MIGKIT_WEB_STATE=/state/web` keeps the view's state on
the volume.

## For a team: an identity provider, TLS, a reverse proxy

Anything but this machine's loopback needs TLS, and the view refuses to
start without it. Two ways:

**Its own certificate:**

```sh
MIGKIT_UI_BIND=0.0.0.0 \
MIGKIT_WEB_URL=https://migkit.example.com:8899 \
MIGKIT_WEB_TLS_CERT=/etc/migkit/tls/fullchain.pem \
MIGKIT_WEB_TLS_KEY=/etc/migkit/tls/key.pem \
migkit report --serve --port 8899
```

TLS 1.2 at the least; HSTS is sent.

**Behind a reverse proxy that does TLS** - the view on loopback (or a
private address), the proxy named so its connections are the only ones
answered and its `X-Forwarded-*` headers the only ones read:

```sh
MIGKIT_WEB_URL=https://migkit.example.com \
MIGKIT_WEB_PROXIES=127.0.0.1 \
migkit report --serve --port 8899
```

nginx:

```nginx
server {
    listen 443 ssl http2;
    server_name migkit.example.com;
    ssl_certificate     /etc/nginx/tls/fullchain.pem;
    ssl_certificate_key /etc/nginx/tls/key.pem;
    client_max_body_size 8m;          # a provider provisioning a large group
    location / {
        proxy_pass http://127.0.0.1:8899;
        proxy_set_header Host              $host;
        proxy_set_header X-Forwarded-For   $remote_addr;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header X-Forwarded-Host  $host;
        proxy_read_timeout 60s;           # holding a tail waits up to 20 s
    }
}
```

Caddy:

```
migkit.example.com {
    reverse_proxy 127.0.0.1:8899
}
```

The Host the proxy passes must be the one in `MIGKIT_WEB_URL` (or listed
in `MIGKIT_UI_HOSTS`): a request for any other name is refused, so a page
elsewhere cannot reach the view through a name it controls.

## OpenID Connect

Register a web client with the provider (Entra ID, Okta, Google, Keycloak,
Auth0, ...):

* redirect URI: `https://migkit.example.com/auth/oidc/callback`
* grant: authorization code, with PKCE (S256); client authentication by
  its secret (`client_secret_basic`)
* scopes: `openid email profile` (and whatever puts groups in the token)

```sh
MIGKIT_WEB_OIDC_ISSUER=https://login.example.com/realms/ops
MIGKIT_WEB_OIDC_CLIENT_ID=migkit
MIGKIT_WEB_OIDC_CLIENT_SECRET=env:OIDC_SECRET     # or file:, vault:, keyring:
MIGKIT_WEB_OIDC_GROUPS=groups                     # the claim groups are in
```

The issuer's discovery document must name the same issuer. A person is
known by their e-mail address (any case), or their username where the
provider gives no e-mail; an e-mail the provider says is unverified is
refused. Groups come from the claim named, or from the provider's user
information where the ID token does not carry them.

## SAML 2.0

Give the provider the view's metadata,
`https://migkit.example.com/auth/saml/metadata` (entity id is the same
address; the assertion consumer service is `/auth/saml/acs`, HTTP-POST),
and give the view the provider's:

```sh
MIGKIT_WEB_SAML_METADATA=/etc/migkit/idp-metadata.xml
MIGKIT_WEB_SAML_GROUPS=groups        # the attribute groups are in
MIGKIT_WEB_SAML_NAME=email           # where the name identifier is not the e-mail
```

The provider signs the response or the assertion (SHA-256 or stronger)
and leaves the assertion unencrypted - TLS carries it; an encrypted one is
refused, said. Only an answer to a request the view made is taken
(sign-ins the provider starts on its own are not), once.

## SCIM provisioning

```sh
MIGKIT_WEB_SCIM_TOKEN=env:SCIM_TOKEN   # 24 characters at the least
```

Point the provider's provisioning at `https://migkit.example.com/scim/v2`
with that bearer token. From then on only people it provisioned, and has
not deactivated, sign in; their groups are the provider's; deactivating
or deleting someone there ends every session and API token of theirs at
once.

## Roles

In the hops file, beside `hops` - the view writes none of it:

```yaml
access:
  admin: ["group:migkit-admins"]
  groups:
    payments:
      hops: ["pay-*", orders]
      viewer: ["*@example.com"]
      operator: ["group:dba"]
      approver: ["group:change-board"]
hops:
  orders:
    options:
      access:
        operator: [ann@example.com]
      approvals:
        steps: [cutover, point-of-no-return]
        count: 2
```

A hop whose access names an approver takes approvals given in the view:
an approver signed in approves the standing request on the hop's page; the
approval is signed with the view's own key and counts only for that
request, only while they still hold the role (and, with provisioning, are
still active), never for a step they started themselves. `signers` (SSH
keys) may be kept beside it or left out.

## API tokens

Made on the account page, shown once, kept as their digest, for at most
`MIGKIT_WEB_TOKEN_DAYS` (90) days, with the roles of the person who made
them:

```sh
curl -H "Authorization: Bearer mkt_..." https://migkit.example.com/api/v1/hops
curl -H "Authorization: Bearer mkt_..." -H "Content-Type: application/json" \
     -d '{"action": "check"}' https://migkit.example.com/api/v1/hops/orders/runs
```

The description: `/api/v1/openapi.json`, and
[docs/site/openapi.json](site/openapi.json).

## `/metrics`

`/metrics` is served from a snapshot taken every 15 seconds, the
dashboard's own: to a scraper with the bearer token (every hop), or to
someone signed in (the hops theirs to see); to anyone else, nothing.

```sh
MIGKIT_METRICS_TOKEN=env:METRICS_TOKEN
```

```yaml
scrape_configs:
  - job_name: migkit
    scheme: https
    authorization: {credentials_file: /etc/prometheus/migkit-token}
    static_configs: [{targets: [migkit.example.com]}]
```

## What it keeps

`MIGKIT_WEB_STATE` (`~/.local/share/migkit/web`, 0700): `web.db` - the
sessions, API tokens and the provisioned directory, each session and token
as its digest; `approvals.key` - the key approvals given in the view are
signed with (0600; `approvals.pub` beside it is what a run checks them
with); `record.jsonl` - the view's own record of sign-ins, provisioning,
tokens and refusals, chained as a hop's is. A run started from the view
keeps its output under the hop's reports, `view-runs/`. Back the directory
up with the reports; a run that checks an approval given in the view reads
`approvals.pub` there, so it runs on the view's machine.

## Not done

* An encrypted SAML assertion, a signed authentication request, SAML
  single logout and OpenID Connect back-channel logout: signing out ends
  the view's session, not the provider's.
