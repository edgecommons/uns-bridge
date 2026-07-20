# Site-broker deploy recipes (P3-5, D-B13)

The site broker and the `uns-bridge` **deploy as a pair** — a bridge with no site broker to point
at does nothing useful, and a site broker with no ACL is a hole, not a boundary. This directory
holds the broker-side half; the bridge's own packaging (`recipe.yaml`, `gdk-config.json`,
`build.sh`) lives at the repo root. EMQX is the broker everywhere (HOST, GREENGRASS, KUBERNETES) —
the ecosystem's established choice (`../../CLAUDE.md`'s "Local EMQX broker" note; `edgecommons`'s
`test-infra/compose.yaml` is the house-style precedent every file here mirrors).

## Which recipe for which platform

| Platform | Recipe | What it deploys |
|---|---|---|
| HOST | [`docker-compose.yml`](docker-compose.yml) | The site broker as a Docker container on a gateway box (§4.1). Doubles as the local dual-EMQX dev/test rig (D-B14) — see below. |
| GREENGRASS | [`greengrass/recipe.yaml`](greengrass/recipe.yaml) + [`greengrass/README.md`](greengrass/README.md) | The **same** `docker-compose.yml`, run as a GG-managed container via `aws.greengrass.DockerApplicationManager` (§4.2). |
| KUBERNETES | [`k8s/emqx.yaml`](k8s/emqx.yaml) + [`k8s/README.md`](k8s/README.md) | The in-cluster broker itself — the aggregation point, natively (§4.3). **No bridge runs inside a cluster**, except the one documented exception, [`k8s/boundary-bridge.example.yaml`](k8s/boundary-bridge.example.yaml) (`replicas: 1` + `Recreate`, D-B15). |

## The ACL is the real boundary — not the bridge's code

[`acl.conf`](acl.conf) is the load-bearing file in this directory (§4.4/§5.4/§7.5 pt 3 of the
design doc, D-B4/D-B13). The bridge relays other components' `state`/`cfg`/`evt`/`metric`/`data`/
`log` **verbatim**, at the raw `MessagingProvider` level, with **no in-process reserved-class
guard** in that path (a deliberate design choice — see `../../README.md` "How it connects"). So the
only thing stopping a bridge (misconfigured, compromised, or just buggy) from publishing outside
its own device's subtree is this file. It confines every device bridge's client (identified by its
mTLS certificate's Common Name, mapped to the MQTT username) to exactly:

- **publish** `ecv1/<device>/#` and `edgecommons/+` (its own device subtree + the reply back-haul,
  §2.4)
- **subscribe** `ecv1/<device>/+/+/cmd/#` and `edgecommons/+` (only commands addressed to its own
  device, including the `_bcast` broadcast — `+` in the component position — plus reply topics)
- everything else: **denied**

via one generic rule pair using EMQX's `${username}` topic placeholder — not a hand-maintained
per-device list. `acl.conf`'s comments walk through a worked example for one device (`gw-01`) and
the generic fleet template side by side; see that file for the full explanation, and
[`TLS.md`](TLS.md) for how a client cert's CN becomes that username in the first place.

**Deploy the bridge only against an ACL-enforcing site broker.** The HOST and GREENGRASS recipes
ship the ACL mounted and on by default; don't strip it out to "simplify" a deployment. KUBERNETES
is the one deliberate exception — the *pure in-cluster* broker (`k8s/emqx.yaml`'s default) runs
**without** the ACL, because every in-cluster component connects anonymously and EMQX's
authorization chain isn't listener-scoped (mounting it there would deny ordinary in-cluster traffic
too, not just external connections) — see `k8s/README.md` for the full reasoning and how the ACL
comes back for the one case it applies to in a cluster (a broker instance dedicated to external
boundary traffic).

## Running the local dual-EMQX rig (D-B14)

The most common case on this dev machine: the edgecommons monorepo's own device broker
(`edgecommons-emqx`, `edgecommons/test-infra/compose.yaml`) is already running on `:1883`/`:8883`. This
directory's `docker-compose.yml` then only needs to add the **site** broker:

```bash
cd deploy/site-broker
bash gen-tls-certs.sh        # once — CA + server cert + worked-example client certs (TLS.md)
cp .env.example .env         # defaults already match this layout; edit only for non-default ports
docker compose up -d         # starts just the site broker: :1884 (plaintext) / :8884 (mTLS) / :18084 (dashboard)
```

If you don't already have `edgecommons-emqx` running (a fresh checkout, or you'd rather not clone the
monorepo), this same file gives you a **complete two-broker rig** on its own:

```bash
docker compose --profile dual up -d   # adds a throwaway device broker on :1883 too
```

Then run the bridge against it (from the repo root):

```bash
cargo run -- --platform HOST --transport MQTT ./test-configs/config.json \
  -c FILE ./test-configs/config.json --thing gw-01
```

### Two ways to run this locally

`test-configs/config.json`'s `siteBroker` points at the **plaintext** `:1884` listener with no
credentials — deliberately, for a friction-free "does the relay logic work" smoke run. But
`acl.conf` is mounted and on by default, and a plaintext connection carries no certificate — so no
`${username}`, so no ACL match, so **every publish/subscribe onto the site broker is denied**. This
is not a bug in the compose file: it's the point.

**Verified against a real broker while building this recipe** (not just read from EMQX's docs): a
denied *subscribe* comes back as a normal MQTT SUBACK failure code (0x80) — visible, the client
knows immediately. A denied *publish*, under EMQX's default `deny_action = ignore`, comes back as an
**ordinary successful PUBACK** — the publishing client (the bridge) has no way to tell the message
was silently dropped rather than delivered. Concretely: the bridge's `relay_publish_failed` metric
(`../../README.md` §2.8) counts *transport*-level failures (the local MQTT client call itself
erroring, e.g. the connection being down) — it does **not** increment on a broker-side ACL denial,
because from the transport's point of view the publish succeeded. So running the plaintext dev
config against this ACL'd broker looks completely quiet: no errors, no metric, just messages that
never arrive on the site side. That silence is worth knowing about before you see it in production —
if you need the broker to fail loudly instead (e.g. to catch a misconfigured ACL/cert during ops),
set `EMQX_AUTHORIZATION__DENY_ACTION=disconnect` in `docker-compose.yml`, which drops the
connection on the first denied publish and *does* surface as a transport error / `site_connected`
flip.

To see a fully working relay end-to-end locally — and to see the denial happen where you CAN observe
it (a subscribe failure, or a second client watching for a message that never arrives) — point the
config at the mTLS listener instead (`:8884`) with the generated `client-gw-01` cert. `TLS.md` has
the exact config snippet and a `mosquitto_pub`/`mosquitto_sub` walkthrough, verified end-to-end
(own-subtree publish delivered, own-cmd-topic subscribe delivered, cross-device publish silently
dropped and never observed by a third-party subscriber, anonymous plaintext subscribe rejected at
SUBACK).

## Files in this directory

| Path | What |
|---|---|
| `docker-compose.yml` | HOST recipe + the dual-EMQX dev/test rig (D-B14) |
| `.env.example` | Port/image-tag overrides (copy to `.env`) |
| `acl.conf` | **The security boundary.** Worked example + fleet template + site-consumer rules |
| `gen-tls-certs.sh` | Throwaway dev CA + server cert + two worked client certs (device, consumer) |
| `TLS.md` | Cert wiring: which config field is which, dev vs. prod CA posture, rotation notes |
| `greengrass/` | GG `DockerApplicationManager` recipe sketch + artifact/lifecycle notes |
| `k8s/` | In-cluster broker manifests + the one documented boundary-bridge exception |

## Validation performed for this slice

No live broker/cluster/GG deployment was exercised (out of scope — see each subdirectory's "left
as a live-deploy step" notes). What *was* checked:

- `docker compose config` against `docker-compose.yml` (default profile, and `--profile dual`) —
  parses cleanly, images/ports/volumes resolve as expected.
- Every YAML/JSON file in this tree parses (compose, both k8s manifests' multi-doc YAML, both
  recipe.yaml sketches, the JSON embedded in the k8s ConfigMaps).
- `acl.conf`'s rule syntax (the tuple form, `${username}`/`${clientid}` topic placeholders,
  `{username, {re, "..."}}` who-specs) was checked against EMQX 5's documented file-ACL grammar.
- `test-configs/config.json` was re-read against these recipes: its `siteBroker.port` (`1884`,
  plaintext) matches this directory's dev-default `SITE_MQTT_PORT`, and its lack of a
  `credentials`/mTLS block is consistent with "plaintext dev smoke, not the ACL-enforcing path"
  above. The site LWT is derived by the bridge, so it is not part of this config.
