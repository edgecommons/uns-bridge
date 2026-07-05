# How-to Guides

Recipes for specific tasks. Each assumes the bridge builds and runs (see the [tutorial](tutorial.md)). For
concepts see [explanation.md](explanation.md); for exhaustive options see
[reference/configuration.md](reference/configuration.md); for complete worked configs see
[sample-configurations.md](sample-configurations.md).

---

## Declare the site broker

The site broker is the bridge's **external system**, declared in the bridge's own `component.instances[]`
exactly the way an adapter declares its OPC UA endpoints — reusing the library's `mqttBroker` shape (no
schema change). Put it in the entry with `id: "site"`:

```jsonc
"component": {
  "instances": [
    { "id": "site",
      "siteBroker": { "host": "site-broker.dallas.example", "port": 8883, "clientId": "uns-bridge-gw-01" }
    }
  ]
}
```

The `messaging` section (top level) is the **device** bus; the `siteBroker` here is the **site** bus. If
exactly one entry declares a `siteBroker` you may name it anything — the bridge falls back to the sole broker
entry. But if two entries carry a `siteBroker`, one **must** be named `"site"` or startup fails as ambiguous.

---

## Secure the site connection with TLS + per-device ACL

The site link is where device traffic leaves the device — secure it, and secure it at the **broker**, because
the relay carries no in-process guard (see [explanation → security](explanation.md#a-note-on-security)). Point
the `siteBroker.credentials` at your client cert/key/CA and use the TLS port:

```jsonc
"siteBroker": {
  "host": "site-broker.dallas.example", "port": 8883, "clientId": "uns-bridge-gw-01",
  "credentials": { "certPath": "/certs/client.pem", "keyPath": "/certs/client.key", "caPath": "/certs/ca.pem" }
}
```

Then give the broker a per-device ACL so each bridge may publish only under its own `ecv1/{device}/#` subtree
and read only its own `cmd`. The `deploy/site-broker/` recipe set ships a ready `acl.conf`, a
`gen-tls-certs.sh`, and matching server/client certs — start from there; the bridge and the site broker
deploy **as a pair**.

---

## Register a Last-Will for whole-device reachability

An abrupt device/bridge death is invisible unless the broker announces it. Configure a Last-Will on the site
connection that publishes `UNREACHABLE` on the bridge's **own state topic**, so a site console watching
`ecv1/+/+/+/state` sees the device go dark immediately:

```jsonc
"lwt": { "topic": "ecv1/gw-01/uns-bridge/main/state", "payload": { "status": "UNREACHABLE" }, "qos": 1 }
```

The topic **must** equal the bridge's real state topic (`ecv1/{device}/uns-bridge/main/state`) — otherwise the
broker publishes UNREACHABLE where no one listens. The bridge template-resolves `{ThingName}` in this topic
and cross-checks it at startup; a mismatch (or a missing LWT) logs a **WARN** but never fails the bridge (the
check is advisory — config stays authoritative). Watch the startup logs for `site LWT topic ... does NOT match`
to catch a typo.

---

## Turn a class on or off

Every uplinkable class can be switched off; a disabled class's messages are dropped and counted. Set
`enabled` under `uplink.classes.<class>`:

```jsonc
"uplink": { "classes": {
  "log": { "enabled": false },     // don't ship log tailing across the WAN
  "app": { "enabled": true }       // DO relay the free-form app class (off by default)
} }
```

- **`app` is opt-in** — off by default, and off means its filter is never even subscribed.
- **`log` is on by the code default**, but the shipped sample config (and the §2.5 recommendation) ship it
  **off** — set `"log": { "enabled": false }` unless you really want log tailing to cross the site link.
- The six consumer classes (`state`, `cfg`, `evt`, `metric`, `data`) are on by default.
- `cmd` is not on this list — it is never uplinked and has no policy knob.

---

## Cap a class's rate

High-volume classes (`data`, `metric`) can flood the site link. Cap each with a token bucket —
`maxRatePerSec` is the sustained rate, `burst` the bucket capacity (default `2×rate`; the bucket starts
full):

```jsonc
"uplink": { "classes": {
  "data":   { "maxRatePerSec": 200, "burst": 400 },
  "metric": { "maxRatePerSec": 50 }
} }
```

| You want… | Set |
|-----------|-----|
| A steady ceiling on a class | `maxRatePerSec` |
| A larger momentary burst allowance | `burst` (defaults to `2×maxRatePerSec`) |
| No cap | omit both (the default — unlimited) |
| Only an initial burst, then nothing | `maxRatePerSec: 0` + a `burst` (prefer `enabled: false` to switch a class off) |

Over-cap traffic **drops** — it never queues. The live UNS path is deliberately not durable; if you need
every sample, that is the streaming subsystem's job, not the bridge's.

---

## Keep alarms across a WAN outage

Events are the one class you don't want to lose during a blip. The `evt` disconnect replay buffer is **on by
default (1000 messages, drop-oldest)** — you only touch it to resize or disable it:

```jsonc
"uplink": { "classes": {
  "evt": { "bufferWhileDisconnected": { "maxMessages": 5000 } }   // bigger buffer for a chatty site
} }
```

- `bufferWhileDisconnected: { "enabled": false }` (or `"maxMessages": 0`) turns it off — then `evt` drops on
  disconnect like every other class.
- `bufferWhileDisconnected` on any class **other than `evt`** is ignored with a warning (the scope is
  evt-only).
- On reconnect the buffer replays **in order**, then clears; overflow while down drops the oldest.

The buffer is **memory-only** — it survives a WAN blip, not a bridge restart.

---

## Proxy site→device request/reply (and the paired knob)

Request/reply across the bridge works automatically — a site-side `request()` gets its `reply_to` rewritten
down and the reply carried back up (see [explanation](explanation.md#requestreply-across-the-bridge--the-correlation-map)).
The one thing you must keep aligned is the **TTL paired knob**:

```jsonc
"messaging": { "requestTimeoutSeconds": 30 },   // top level: the requester's deadline
"component": { "instances": [ { "id": "site",
  "reply": { "ttlSecs": 60, "maxPending": 1024 }   // MUST be >= 2x requestTimeoutSeconds
} ] }
```

- `reply.ttlSecs` defaults to **60 s = 2× the framework's 30 s** request-deadline default. If you raise
  `messaging.requestTimeoutSeconds`, raise `reply.ttlSecs` in step, or the bridge may tear down a reply path
  before the requester's own deadline settles it.
- `reply.maxPending` (default 1024) bounds in-flight requests; overflow evicts the **oldest** (a stuck
  responder must not starve fresh commands).

---

## Size the per-class subscription queues

The relay pumps are serial per class; each class's provider subscription has a bounded queue, and overflow
drops at the provider. `data` gets a deep queue, everything else a shallow one:

```jsonc
"queue": { "data": 512, "default": 64 }
```

Raise `queue.data` if a bursty `data` producer outpaces the uplink momentarily; raise `queue.default` if a
downlink `cmd` burst (or another class) needs more slack.

---

## Guard against relay loops in a complex topology

Loop protection is automatic (the hop tag), but two knobs and one rule matter:

- **`maxHops`** (default **4**) caps how many distinct bridges a message may traverse before it's dropped.
  Lower it in a shallow topology to fail fast; raise it only if you genuinely chain more than four bridges.
- **Exactly one bridge per device bus.** Two bridges on the same bus pair **double-deliver** everything — the
  hop tag prevents *loops*, not *duplication*. On Kubernetes that means `replicas: 1` + `strategy: Recreate`.
- Inside a Kubernetes cluster there is **no** bridge — the in-cluster broker is the aggregation point; a
  bridge only appears at a boundary.

---

## Deploy to a platform

**HOST:** run the binary against a config file naming the device broker (`messaging`) and the site broker
(`component.instances[site]`):

```bash
uns-bridge --config ./config.json --thing gw-01     # -t falls back to $GGCOMMONS_THING_NAME
```

**Kubernetes (boundary bridge):** deploy the *same* binary as a `replicas: 1` / `strategy: Recreate`
Deployment between the on-prem device bus and the in-cluster broker. See
`deploy/site-broker/k8s/boundary-bridge.example.yaml` for a worked manifest, and `deploy/site-broker/k8s/`
for the in-cluster aggregation broker it bridges onto.

**Greengrass:** the **site broker's** Greengrass recipe is in `deploy/site-broker/greengrass/`. The *bridge's*
own Greengrass packaging (`recipe.yaml`, `gdk-config.json`) is a **stub** — and the intended
PRIMARY=Nucleus-IPC variant is a documented follow-up, so a production Greengrass IPC-primary bridge is not
yet buildable from this repo. Until then, a Greengrass core can run the bridge in its HOST shape against a
device-local MQTT broker.

---

## Run the dual-broker end-to-end test

The bridge-level proof against two **real** brokers — one command, needs only Docker + cargo:

```bash
bash tests/e2e/run.sh
```

It boots a throwaway two-EMQX rig on dedicated ports, runs the real bridge binary against the shipped sample
config, and asserts (with per-assertion PASS/FAIL): uplink of a `state` envelope, an `evt` envelope, and a
**raw** `data` payload arrive topic-verbatim (envelopes hop-tagged, raw byte-verbatim); downlink of an
own-device `cmd`; the drop of a foreign-device `cmd`; a reply round-trip; the loop-drop of an own-echo; and
the bridge's own heartbeat `state` + relay-counter `metric`s appearing and riding the relay. The test is
`#[ignore]`d and gated on `UNS_BRIDGE_E2E=1`, so a plain `cargo test` never touches it.

---

## Observe the bridge's health and throughput

- **Metrics** — every 30 s the bridge publishes relay counters as `metric`s on
  `ecv1/{device}/uns-bridge/main/metric/<name>` (with the shipped `metricEmission.target: messaging`).
  Watch `relay_uplinked` / `relay_downlinked` for throughput, `relay_dropped_*` for policy drops,
  `relay_loop_dropped` for loop protection firing, `relay_reply_relayed` / `relay_reply_expired` for the
  reply proxy, `relay_evt_buffered` / `relay_evt_replayed` for disconnect handling, and the gauges
  `relay_pending_replies` and `site_connected` for live state. Full table:
  [reference/messaging-interface.md](reference/messaging-interface.md#metrics).
- **State keepalive** — the bridge's own `state` on `ecv1/{device}/uns-bridge/main/state` every ~5 s; the
  site LWT flips it to `UNREACHABLE` on an abrupt death.
- **Logs** — startup logs the resolved identity, hop id, filter counts, and the active uplink policy
  (disabled classes, rate-capped classes, evt buffer size); shutdown logs a one-line tally of every counter.
