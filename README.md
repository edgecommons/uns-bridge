# uns-bridge

**One `uns-bridge` per device bus**: an envelope-aware relay between the device-local bus and the
**site UNS broker**, making the logical [Unified Namespace](https://github.com/edgecommons/ggcommons)
(`ecv1/{device}/{component}/{instance}/{class}[/channel]`) a real **site-wide** bus. Each device has
its own bus (a local MQTT broker on HOST; the Nucleus IPC bus on GREENGRASS) with no cross-device
visibility — the bridge subscribes the device's UNS traffic, republishes it **topic-verbatim** onto
the site broker under the device's namespace, and relays commands back down. Any site-scoped
consumer (a historian, an MES bridge, the edge console) then connects to **one** bus.

Unlike a dumb broker bridge it is envelope-aware: it stamps a **hop tag** for loop protection,
rewrites `reply_to` so site→device request/reply crosses the bridge (the TTL'd correlation map,
§2.4), rate-caps the data plane (P3-4), and registers a Last-Will `UNREACHABLE` on the site
connection for fast whole-device reachability detection.

Design source of truth: `docs/platform/DESIGN-uns-bridge.md` (and `DESIGN-uns.md` §9) in the
ggcommons monorepo. Section references below (§…) point there.

## How it connects (§1, §2.1)

| Connection | What | Built from |
|---|---|---|
| **PRIMARY** | the device-local bus | the standard `messaging` config section (HOST: local MQTT broker; GREENGRASS IPC variant is a follow-up) |
| **SITE** | the site UNS broker — the bridge's **external system** | the bridge's own `component.instances[]` `"site"` entry, by **reusing the ggcommons core's public MQTT objects**: `MqttProvider::connect(&site_cfg)` — zero library change |

The relay runs at the **raw `MessagingProvider` level** on both connections (byte relay). The
reserved-class publish guard is a `MessagingService` concern and is deliberately not in this path
(§1.3) — the site broker's **per-device ACL** is the durable boundary: a bridge may publish only
under its own `ecv1/{device}/#` subtree.

The site uplink is intermittent by design (edge-first): the bridge comes up and serves the device
bus while the WAN is down, retrying the site connect in its own loop (§1.4); the provider
re-subscribes every filter on each CONNACK, so reconnection is transparent.

## The relay matrix (§2.2)

| Direction | Classes | Filter | Republished |
|---|---|---|---|
| **Uplink** device → site | `state` `cfg` `evt` `metric` `data` `log` (six consumer wildcards; `app` opt-in, default **off**) | `ecv1/+/+/+/state` · `ecv1/+/+/+/cfg` · `ecv1/+/+/+/evt/#` · `ecv1/+/+/+/metric/#` · `ecv1/+/+/+/data/#` · `ecv1/+/+/+/log/#` | same topic string, on the site broker |
| **Downlink** site → device | `cmd` only (broadcast rides the `+` component position → `_bcast`) | `ecv1/{device}/+/+/cmd/#` — **pinned to this bridge's own device** | same topic string, on the device bus |

Explicit non-flows (v1): `cmd` is never uplinked (no cross-device request/reply, D-B7); reply
topics (`ggcommons/reply-…`, non-`ecv1`) never match a UNS filter and only cross via the §2.4
correlation map (below). The uplink∩downlink class **disjointness** is also the structural loop
guard for raw (non-enveloped) messages.

### Hop-tag loop protection (§2.3)

Every relayed **envelope** gets the reserved tag `tags._relay` — a JSON array of hop ids
(`{device}/uns-bridge`) — appended. Before relaying, the bridge:

1. drops silently if the array already contains its **own** id (own echo);
2. drops if the array already carries `maxHops` ids (default **4** — defense against a cycle among
   *distinct* bridges);
3. otherwise appends its id and relays, envelope otherwise untouched (topic-verbatim, structural
   identity per D-U22).

Raw messages carry no tags and are protected by the class disjointness. Consumers ignore `_relay`
(it doubles as the "which path did this message take" breadcrumb). Tag keys starting with `_` are
library/system-reserved.

### `reply_to` rewrite — the TTL'd correlation map (§2.4 / D-B9)

Request/reply crossing the bridge breaks without rewriting: a site-side requester sets
`header.reply_to = ggcommons/reply-<uuid>` — an ephemeral topic **on the site broker** — so a
device-side responder would `reply()` onto the device bus where nobody listens. The bridge proxies
the reply path:

1. **Down**: a relayed `cmd` carrying `header.reply_to` gets a **bridge-minted** reply topic
   (`ggcommons/reply-<uuid>`, the core's standard prefix) written into the header; the bridge
   subscribes it on the device bus (**before** relaying the cmd; `max_messages = 1`,
   first-reply-wins) and records `bridge topic → original site reply topic` in the correlation
   map. A `cmd` **without** `reply_to` is a fire-and-forget notification and relays untouched.
2. **Up**: the first message on a bridge reply topic relays to the **original site `reply_to`**
   verbatim — `correlation_id` and body untouched, hop tag appended, `header.reply_to` dropped —
   then the entry is removed and the bridge topic unsubscribed (one-shot).
3. **TTL sweep**: entries expire after `reply.ttlSecs` (default **60 s = 2×** the framework's 30 s
   request-deadline default — raise them **in step**, a paired knob); expiry unsubscribes the
   bridge topic and counts `reply_expired`. The map is bounded by `reply.maxPending` (default
   **1024**): overflow **evicts the oldest** entry (expired early + counted) rather than refusing
   fresh commands. Stray/late replies with no live entry are dropped + counted.

## Configuration (§2.7)

The site broker lives in the bridge's **own** `component.instances[]` — the existing per-instance
surface every component has (exactly how the opcua-adapter declares its OPC UA endpoints), reusing
the library's `MessagingConfig`/`mqttBroker` shape (including `lwt`). No schema change, no core
change. See [`test-configs/config.json`](test-configs/config.json) for a complete sample
(device broker `:1883`, site broker `:1884` — the dual-EMQX dev layout):

```jsonc
{
  "messaging": {                       // PRIMARY: the device-local bus
    "local": { "host": "localhost", "port": 1883, "clientId": "uns-bridge-local" }
  },
  "component": {
    "name": "com.mbreissi.uns-bridge",
    "instances": [
      { "id": "site",                  // the SITE broker — the bridge's external system
        "siteBroker": { "host": "site-broker.dallas.example", "port": 8883,
                        "clientId": "uns-bridge-gw-01",
                        "credentials": { "certPath": "…", "keyPath": "…", "caPath": "…" } },
        "lwt": { "topic": "ecv1/gw-01/uns-bridge/main/state",     // §2.6: whole-device UNREACHABLE
                 "payload": { "status": "UNREACHABLE" }, "qos": 1 },
        "uplink": { "classes": { "app": { "enabled": false } } }, // P3-2 honors the app opt-in
        "reply":  { "ttlSecs": 60, "maxPending": 1024 },          // §2.4 correlation map (paired knob: 2× requestTimeoutSeconds)
        "maxHops": 4,
        "queue":  { "data": 512, "default": 64 } }                // per-class max_messages
    ]
  }
}
```

Notes for the current slice: values are concrete (template substitution like `{ThingName}` arrives
with the full facade integration); the `lwt` entry is already applied at CONNECT by the reused
provider (the startup topic cross-check lands in P3-4); `uplink` per-class enables/rate-caps/buffers
are parsed and carried but enforced in P3-4 (only `classes.app.enabled` is honored today); the
`reply` knobs are fully enforced (P3-3). The relay/reply counters (`reply_relayed`,
`reply_expired`, `reply_stray`, the `pending_replies` gauge, …) are held in-process and logged at
shutdown; publishing them as `metric`s lands in P3-4 (§2.5 table).

## Run locally (HOST)

```bash
# device broker on :1883 (the standard ggcommons test-infra EMQX) and a site broker on :1884
cargo run -- --config ./test-configs/config.json --thing gw-01
```

Device identity: `-t/--thing` or `GGCOMMONS_THING_NAME`. Logging: `RUST_LOG` (default `info`).
Graceful shutdown (Ctrl-C / SIGTERM) aborts the pumps and **unsubscribes every filter at both
brokers** before exit.

## Building

```bash
cargo build            # standalone (default) — the HOST MQTT<->MQTT pair; builds on any OS
cargo test
cargo clippy --all-targets
```

**Local development against the sibling library**: this repo pins `ggcommons` by git rev in
`Cargo.toml` (what CI resolves). For local dev, a **gitignored** `.cargo/config.toml` patches the
dep to the sibling checkout — create it as:

```toml
[patch."https://github.com/edgecommons/ggcommons.git"]
ggcommons = { path = "../ggcommons/libs/rust" }
```

(The telemetry-processor pattern; delete the file for a pure git-rev build.)

## Repo layout

| Path | What |
|---|---|
| `src/relay.rs` | The **pure** relay decision engine: §2.2 class routing + own-device pinning, §2.3 hop tag — no IO, fully unit-tested |
| `src/reply.rs` | The **pure** §2.4 reply proxy logic: the TTL'd correlation map (`rewrite_downlink`/`take`/`sweep`, evict-oldest) + the reply back-haul transform — no IO, injected clock |
| `src/io.rs` | The pumps: raw-provider subscriptions → `RelayEngine::decide` (+ the reply rewrite on the downlink) → topic-verbatim republish; per-reply one-shot pumps + the TTL sweep task; counters; unsubscribe-on-shutdown (incl. pending reply topics) |
| `src/config.rs` | The §2.7 config shape; maps the `"site"` instance entry onto the core `MessagingConfig`; typed `reply` knobs |
| `src/main.rs` | Two connections (primary fatal, site retried), signal handling, graceful stop |
| `test-configs/` | Sample dual-broker config |
| `recipe.yaml`, `gdk-config.json`, `build.sh` | GREENGRASS packaging stubs (finalized in P3-5/P3-6) |

## Roadmap (the Phase-3 slices)

| Slice | Contents | Status |
|---|---|---|
| **P3-2** | repo scaffold; relay engine (six uplink filters + pinned downlink, topic-verbatim, hop tag/maxHops); unit tests over trait fakes | **done** |
| **P3-3** | `reply_to` rewrite: TTL'd correlation map, maxPending eviction, reply back-haul | **done** |
| P3-4 | per-class uplink policy (enable/rate caps/evt buffer), drop-counter **metrics**, reconnect `republish-*` broadcast rehydration, LWT startup cross-check | pending |
| P3-5 | `deploy/site-broker/` recipes (HOST compose, GG DockerApplicationManager, k8s boundary notes, the per-device **ACL** file) | pending |
| P3-6 | registry entry, docs-site sync, dual-EMQX e2e + 3-platform validation | pending |

Also follow-ups: the GREENGRASS variant (PRIMARY = Nucleus IPC) and the full ggcommons facade
integration (standard CLI contract, heartbeat/state + `cfg` announce + metric counters riding the
bridge's own relay, §2.8).

## Operational rules

- **Exactly one bridge per device bus** — two bridges on one bus pair double-deliver everything
  (hop tags prevent loops, not duplication). On Kubernetes a boundary bridge is `replicas: 1` +
  `strategy: Recreate`; inside a cluster there is **no** bridge (the in-cluster broker is the
  aggregation point).
- **The site broker's per-device ACL is the security boundary** (§5.4 in the design) — deploy the
  bridge only against an ACL-enforcing site broker.
- **Live-path loss during WAN outages is by design** — durability belongs to the streaming
  subsystem, not the bus.
