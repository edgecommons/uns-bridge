# uns-bridge

**One `uns-bridge` per device bus**: an envelope-aware relay between the device-local bus and the
**site UNS broker**, making the logical [Unified Namespace](https://github.com/edgecommons/edgecommons)
(`ecv1/{device}/{component}/{instance}/{class}[/channel]`) a real **site-wide** bus. Each device has
its own bus (a local MQTT broker on HOST or GREENGRASS) with no cross-device visibility — the bridge
subscribes the device's UNS traffic, republishes it **topic-verbatim** onto the site broker under the
device's namespace, and relays commands back down. Any site-scoped
consumer (a historian, an MES bridge, the edge console) then connects to **one** bus.

Unlike a dumb broker bridge it is envelope-aware: it stamps a **hop tag** for loop protection,
rewrites `reply_to` so site→device request/reply crosses the bridge (the TTL'd correlation map,
§2.4), applies a **per-class uplink policy** — enables, token-bucket rate caps, and a bounded
`evt` replay buffer for WAN blips (§2.5 / D-B10) — and derives a private site Last-Will
`UNREACHABLE` for fast whole-device reachability detection.

Design source of truth: `docs/platform/DESIGN-uns-bridge.md` (and `DESIGN-uns.md` §9) in the
edgecommons monorepo. Section references below (§…) point there. This repo's own `DESIGN.md` carries
the local decision register (packaging, coverage, and config-schema decisions specific to this repo)
plus its build history and current validation gaps.

## How it connects (§1, §2.1, §2.8)

The bridge is a **proper edgecommons component**: a real `EdgeCommons` runtime (built from the same
config) owns the bridge's own observability. The bridge holds **two** connections:

| Connection | What | Built from |
|---|---|---|
| **DEVICE BUS** (shared) | the `EdgeCommons` runtime's observability — identity, the automatic heartbeat `state` keepalive, the effective-(redacted-)config `cfg` publisher, `gg.metrics()` (`metricEmission.target = "messaging"`), logging init, SIGTERM handling — **and** the relay itself: the provider-level protobuf relay (§1.3), the reply proxy, and the rehydration broadcast | the runtime's resolved transport (**MQTT on HOST**, **Nucleus IPC on GREENGRASS**); the relay shares it via `gg.raw_device_provider()` (`EdgeCommons::raw_device_provider`) |
| **SITE** | the site UNS broker — the bridge's **external system**; carries the private bridge-console D-B11 LWT; always MQTT, independent of the device-bus transport | the bridge's own `component.instances[]` `"site"` entry for the broker endpoint; the bridge derives the LWT from its canonical state topic and passes it to `MqttProvider::connect_with_last_will` |

The relay shares the runtime's **one** device-bus connection: `gg.raw_device_provider()` hands back the
runtime's raw `MessagingProvider` — the same connection it already uses — so the relay operates at the
raw-provider level **below** the reserved-class publish guard (§1.3). Working below the guard is what
lets the bridge forward reserved classes (`state`/`metric`/`cfg`/`log`) verbatim; it also means one
device-bus client instead of two, which matters under the Greengrass shared-connection quota. The
payload contract is the standard edgecommons wire contract: normal UNS messages are protobuf
`EdgeCommonsMessage` bytes. The bridge may decode that envelope to append `_relay` or rewrite
`reply_to`, then re-encodes protobuf; opaque application bytes survive inside the protobuf body.
Foreign/non-protobuf bytes on the relay paths are dropped as malformed rather than forwarded.

The reserved-class publish guard is a `MessagingService` concern and is deliberately not in the relay
path (§1.3) — the site broker's **per-device ACL** is the durable boundary: a bridge may publish only
under its own `ecv1/{device}/#` subtree.

**HOST and GREENGRASS.** On HOST the device bus is a local MQTT broker (default `standalone` feature);
on a Greengrass core it is the Nucleus IPC pubsub (`greengrass` feature — a Linux-only C-FFI IPC
provider). Either way the relay's PRIMARY is whatever transport the runtime resolved, so a single code
path serves both. The site half is always MQTT.
- HOST: `uns-bridge --platform HOST --transport MQTT ./config.json -c FILE ./config.json -t gw-01`
- GREENGRASS: deployed by `recipe.yaml` with `--platform GREENGRASS --transport IPC -c GG_CONFIG -t {iot:thingName}` (built with `--features greengrass`).

The site uplink is intermittent by design (edge-first): the bridge comes up and serves the device
bus while the WAN is down, retrying the site connect in its own loop (§1.4); the provider
re-subscribes every filter on each CONNACK, so reconnection is transparent.

## The relay matrix (§2.2)

| Direction | Classes | Filter | Republished |
|---|---|---|---|
| **Uplink** device → site | `state` `cfg` `evt` `metric` `data` `log` (six consumer wildcards; `app` opt-in, default **off**) | `ecv1/+/+/+/state` · `ecv1/+/+/+/cfg` · `ecv1/+/+/+/evt/#` · `ecv1/+/+/+/metric/#` · `ecv1/+/+/+/data/#` · `ecv1/+/+/+/log/#` | same topic string, on the site broker |
| **Downlink** site → device | `cmd` only (broadcast rides the `+` component position → `_bcast`) | `ecv1/{device}/+/+/cmd/#` — **pinned to this bridge's own device** | same topic string, on the device bus |

Explicit non-flows (v1): `cmd` is never uplinked (no cross-device request/reply, D-B7); reply
topics (`edgecommons/reply-…`, non-`ecv1`) never match a UNS filter and only cross via the §2.4
correlation map (below). The uplink∩downlink class **disjointness** is also a structural guard
against a single bridge matching its own downlink as uplink.

### Hop-tag loop protection (§2.3)

Every relayed protobuf `EdgeCommonsMessage` gets the reserved envelope tag `tags._relay` appended.
The tag is encoded as normal protobuf metadata; JSON renders it as an array of hop ids
(`{device}/uns-bridge`) only in diagnostic projections after decode. Before relaying, the bridge:

1. drops silently if the array already contains its **own** id (own echo);
2. drops if the array already carries `maxHops` ids (default **4** — defense against a cycle among
   *distinct* bridges);
3. otherwise appends its id and relays, message otherwise semantically untouched (topic-verbatim,
   protobuf re-encoded, opaque body bytes preserved).

Non-protobuf payloads carry no edgecommons envelope and are dropped as malformed on normal relay
paths. Consumers ignore `_relay` (it doubles as the "which path did this message take" breadcrumb).
Tag keys starting with `_` are library/system-reserved and remain orthogonal metadata, not message
body content.

### `reply_to` rewrite — the TTL'd correlation map (§2.4 / D-B9)

Request/reply crossing the bridge breaks without rewriting: a site-side requester sets
`header.reply_to = edgecommons/reply-<uuid>` — an ephemeral topic **on the site broker** — so a
device-side responder would `reply()` onto the device bus where nobody listens. The bridge proxies
the reply path:

1. **Down**: a relayed `cmd` carrying `header.reply_to` gets a **bridge-minted** reply topic
   (`edgecommons/reply-<uuid>`, the core's standard prefix) written into the header; the bridge
   subscribes it on the device bus (**before** relaying the cmd; `max_messages = 1`,
   first-reply-wins) and records `bridge topic → original site reply topic` in the correlation
   map. A `cmd` **without** `reply_to` is a fire-and-forget notification and relays untouched.
2. **Up**: the first protobuf message on a bridge reply topic relays to the **original site `reply_to`**
   after decode/mutate/re-encode — `correlation_id` and body untouched, hop tag appended,
   `header.reply_to` dropped — then the entry is removed and the bridge topic unsubscribed (one-shot).
3. **TTL sweep**: entries expire after `reply.ttlSecs` (default **60 s = 2×** the framework's 30 s
   request-deadline default — raise them **in step**, a paired knob); expiry unsubscribes the
   bridge topic and counts `reply_expired`. The map is bounded by `reply.maxPending` (default
   **1024**): overflow **evicts the oldest** entry (expired early + counted) rather than refusing
   fresh commands. Stray/late replies with no live entry are dropped + counted.

### Per-class uplink policy (§2.5 / D-B10)

Every uplink-forwardable message passes a **pure policy engine** (`src/policy.rs`) after the relay
decision:

- **Enable/disable** — any of the seven uplinkable classes can be switched off
  (`uplink.classes.<class>.enabled`); a disabled class's messages **drop + count**
  (`dropped_disabled`, per class). Defaults: `app` **off** (opt-in — off also means the seventh
  filter is never subscribed), every other class **on**. (§2.5 recommends shipping `log` off — the
  sample config does — but the code default keeps it on, matching the P3-2/P3-3 behavior.)
- **Rate caps** — a token bucket per rate-capped class: `maxRatePerSec` refill, `burst` capacity
  (default `2×rate`; the bucket starts full). Over-cap traffic **drops** — never queues; the live
  UNS path is deliberately not durable (durability = the streaming subsystem's job) — counted per
  class (`dropped_rate`). The bucket's clock is injected, so the math is fully deterministic under
  test.
- **Disconnect behavior** (D-B10) — while the site link is down (`connected()` false **or** a site
  publish fails), every class **drops + counts** (`dropped_disconnected`, per class) **except
  `evt`**: events/alarms ride a bounded, memory-only, **drop-oldest** replay buffer
  (`bufferWhileDisconnected`, default **on**, **1000**) — a WAN blip must not lose an alarm
  raise/clear. On reconnect a watcher task replays the buffered `evt` **strictly in order**
  (topic-verbatim, hop tag already stamped in the protobuf envelope) and the buffer clears
  (`evt_replayed`); overflow while
  down evicts the oldest (`evt_buffer_dropped`). A live `evt` arriving while older ones are still
  queued joins the queue rather than overtaking them.
- **Reconnect rehydration** (DESIGN-uns §9.3 layer 2) — on the site-reconnect **rising edge** the
  bridge publishes `ecv1/{device}/_bcast/main/cmd/republish-state` and `…/republish-cfg` on the
  **device bus** (best-effort, notification-style `cmd` envelopes, **before** the `evt` replay) so
  every component's re-announce can ride the uplink and the site view rehydrates `state`/`cfg`
  without retain. Startup is not an edge — the relay only starts after the site link is first
  established. The device-side listener that answers the broadcast — components re-publishing
  their `state` keepalive and effective `cfg` on demand — **shipped in the edgecommons library**
  (`RepublishListener` in Java/Python/TS, `uns.rs` in Rust; on by default, jittered + coalesced),
  so a rev-bumped component fleet rehydrates the site view on every bridge reconnect. The
  broadcast is no longer inert.

## The bridge's own observability (§2.8, P3-4b)

Nothing bespoke: the heartbeat publishes the bridge's `state` keepalive on the device bus, the
`cfg` publisher announces its (redacted) effective config, and the §2.5 counters emit as
`metric`s — all of it matches the uplink filters and is **relayed by the bridge itself**, so the
site broker sees the bridge exactly as it sees any component (plus the private LWT only it sets).

Every **30 s** a task snapshots the relay counters and emits them through `gg.metrics()`
(`ecv1/{device}/uns-bridge/main/metric/<name>` with the shipped `messaging` target). Counters emit
**interval deltas**, gauges the current value:

| Metric | Measures | Kind |
|---|---|---|
| `relay_uplinked`, `relay_dropped_disabled`, `relay_dropped_rate`, `relay_dropped_disconnected` | per class: `state` `cfg` `evt` `metric` `data` `log` `app` | counters |
| `relay_downlinked`, `relay_loop_dropped`, `relay_routed_dropped`, `relay_malformed_dropped`, `relay_publish_failed`, `relay_reply_relayed`, `relay_reply_expired`, `relay_reply_stray`, `relay_evt_buffered`, `relay_evt_buffer_dropped`, `relay_evt_replayed` | `count` | counters |
| `relay_pending_replies` | `count` | gauge |
| `site_connected` | `connected` (0/1) | gauge |

**Site LWT** (§2.6/§3.7, D-B11): at startup the bridge derives the site Last-Will topic from its
**real** state topic (`gg.uns().topic(State)` — what the keepalive actually publishes on). Payload is
a protobuf EdgeCommons `state` envelope from the bridge identity with `status:"UNREACHABLE"`, QoS is
fixed to 1, and retain remains disabled in the provider. This is a private bridge-console contract,
not user configuration.

## Configuration (§2.7)

The site broker lives in the bridge's **own** `component.instances[]` — the existing per-instance
surface every component has (exactly how the opcua-adapter declares its OPC UA endpoints), reusing
the library's `mqttBroker` shape for the endpoint. The site LWT is derived by the bridge and must not
be configured. See [`test-configs/config.json`](test-configs/config.json) for a complete sample
(device broker `:1883`, site broker `:1884` — the dual-EMQX dev layout):

```jsonc
{
  "messaging": {                       // the device-local bus (runtime + relay connections)
    "local": { "host": "localhost", "port": 1883, "clientId": "uns-bridge-local" }
  },
  "heartbeat": { "enabled": true, "intervalSecs": 5 },   // the automatic state keepalive
  "metricEmission": { "target": "messaging" },           // §2.8 counters ride the UNS metric class
  "component": {
    // NOTE: no `component.name` — the canonical schema allows only global/instances
    // there; the component's full name is supplied by the runtime builder.
    "instances": [
      { "id": "site",                  // the SITE broker — the bridge's external system
        "siteBroker": { "host": "site-broker.dallas.example", "port": 8883,
                        "clientId": "uns-bridge-gw-01",
                        "credentials": { "certPath": "…", "keyPath": "…", "caPath": "…" } },
        "uplink": { "classes": {                                  // §2.5 per-class policy (all knobs optional)
            "log":  { "enabled": false },
            "metric": { "maxRatePerSec": 50 },                    // burst defaults to 2× rate
            "data": { "maxRatePerSec": 200, "burst": 400 },
            "evt":  { "bufferWhileDisconnected": { "maxMessages": 1000 } },  // D-B10 (default on, 1000)
            "app":  { "enabled": false } } },                     // opt-in seventh class
        "reply":  { "ttlSecs": 60, "maxPending": 1024 },          // §2.4 correlation map (paired knob: 2× requestTimeoutSeconds)
        "maxHops": 4,
        "queue":  { "data": 512, "default": 64 } }                // per-class max_messages
    ]
  }
}
```

Notes for the current slice: the config file is validated against the **canonical edgecommons
schema** at startup (the runtime loads it via `-c FILE`) — a shipped-config test pins that it
passes. The bridge rejects `component.instances[site].lwt`; the private site LWT is derived from the
runtime's state topic at startup. The `uplink` policy (§2.5 / D-B10) and the `reply` knobs (§2.4)
are fully enforced; the counters publish as `metric`s every 30 s (§2.8, above) and are still logged
once at shutdown.

## Run locally (HOST)

```bash
# device broker on :1883 (the standard edgecommons test-infra EMQX) and a site broker on :1884
cargo run -- --platform HOST --transport MQTT ./test-configs/config.json \
  -c FILE ./test-configs/config.json -t gw-01
```

The bridge uses the standard edgecommons CLI: `--platform`, `--transport`, `-c/--config`, and
`-t/--thing`. For `CONFIG_COMPONENT`, pass a bootstrap MQTT config to `--transport MQTT` and let
the ConfigComponent serve the effective bridge config through `-c CONFIG_COMPONENT`. Logging is
owned by the edgecommons runtime (the config's `logging` section; default console `info`).
Graceful shutdown (Ctrl-C / SIGTERM, via the library's signal watcher) aborts the pumps and
**unsubscribes every filter at both brokers** before exit. The bridge's own `state` keepalive /
`cfg` announce / `metric` emission appear on the device bus immediately and on the site broker
once the relay runs.

## Building

```bash
cargo build            # standalone (default) — the HOST MQTT<->MQTT pair; builds on any OS
cargo test
cargo clippy --all-targets
```

### The dual-EMQX end-to-end test (P3-6)

The bridge-level relay proof against two **real** brokers — one command, needs only Docker + cargo
(runs on Windows Git Bash, Linux, macOS):

```bash
bash tests/e2e/run.sh
```

It boots a throwaway two-EMQX rig (`tests/e2e/docker-compose.e2e.yml`: a device broker and a site
broker, plaintext/anonymous, **dedicated ports `:21883`/`:21884`** so it never collides with the
standing `edgecommons-emqx` on `:1883` or the P3-5 site broker on `:1884` — override with
`E2E_DEVICE_PORT`/`E2E_SITE_PORT`), then runs `tests/e2e_dual_broker.rs`, which spawns the **real
bridge binary** against the shipped sample config (ports swapped in) and asserts, over live MQTT,
per assertion with a printed PASS/FAIL:

- **A1–A3 uplink** — a `state` message, an `evt` message (with channel), and a `data` message with an
  **opaque protobuf body** published on the device bus arrive **topic-verbatim** on the site broker;
  each envelope carries the appended hop tag, and the opaque body bytes are preserved;
- **B downlink** — an own-device `cmd` published on the site broker arrives (hop-tagged) on the
  device bus;
- **C pinning** — a `cmd` addressed to another device is **not** relayed;
- **D reply round-trip** — a site-side request's `reply_to` is rewritten to a bridge-minted topic
  on the way down, and the device-side reply returns to the **original** site reply topic
  (`correlation_id`/body intact, `reply_to` dropped) — the §2.4 proxy, live;
- **E loop-drop** — an envelope already stamped with the bridge's own hop id is dropped, never
  re-relayed;
- **F observability** — the bridge's own heartbeat `state` keepalive **and** the §2.8
  relay-counter `metric`s appear on the device bus (and ride the bridge's own relay to the site).

The Rust test is `#[ignore]`d and additionally gated on `UNS_BRIDGE_E2E=1`, so a plain
`cargo test` (or even `--include-ignored` without the rig) never touches it. The brokers are torn
down on exit either way; runtime ≈ 40 s (dominated by the 30 s first metric-emission tick).
The security-boundary counterpart — the ACL'd/mTLS site broker denying cross-device publishes —
is deliberately **not** this test: see `deploy/site-broker/` (P3-5).

**Local development against the sibling library**: this repo pins `edgecommons` by git rev in
`Cargo.toml` (what CI resolves). For local dev, a **gitignored** `.cargo/config.toml` patches the
dep to the sibling checkout — create it as:

```toml
[patch."https://github.com/edgecommons/edgecommons.git"]
edgecommons = { path = "../core/libs/rust" }
```

(The telemetry-processor pattern; delete the file for a pure git-rev build.)

## Deploying the site broker (P3-5, D-B13)

The bridge and the site broker deploy **as a pair** — see
[`deploy/site-broker/README.md`](deploy/site-broker/README.md) for the full recipe set: a
`docker-compose.yml` for HOST (and the local dual-EMQX dev/test rig it doubles as), a
`greengrass/recipe.yaml` sketch running the same compose via
`aws.greengrass.DockerApplicationManager`, `k8s/` manifests for the in-cluster aggregation broker
(no bridge runs inside a cluster, with one documented boundary-pod exception), and the per-device
**ACL** (`acl.conf`) that is the actual security boundary — the bridge's own provider-level relay
(§1.3 above) carries no in-process guard, so an ACL-less site broker has no boundary at all.

## Repo layout

| Path | What |
|---|---|
| `src/relay.rs` | The **pure** relay decision engine: §2.2 class routing + own-device pinning, §2.3 hop tag, the §9.3 rehydration-topic derivation — no IO, fully unit-tested |
| `src/reply.rs` | The **pure** §2.4 reply proxy logic: the TTL'd correlation map (`rewrite_downlink`/`take`/`sweep`, evict-oldest) + the reply back-haul transform — no IO, injected clock |
| `src/policy.rs` | The **pure** §2.5/D-B10 uplink policy: per-class enables, token buckets (injected clock), and the bounded drop-oldest `evt` replay buffer — no IO |
| `src/observability.rs` | The **pure** §2.8 pieces: the counter→metric mapping (snapshot deltas → named measure groups) — no IO |
| `src/io.rs` | The pumps: provider-level subscriptions → `RelayEngine::decide` → the §2.5 policy governor (uplink) / the reply rewrite (downlink) → topic-verbatim republish; per-reply one-shot pumps + the TTL sweep + the connectivity watcher (rising-edge rehydration broadcast + evt replay) + the 30 s metric emission; counters; unsubscribe-on-shutdown (incl. pending reply topics) |
| `src/config.rs` | The §2.7 config shape; maps the `"site"` instance entry onto the core `MessagingConfig`; typed `reply` + `uplink` knobs (the device bus is the runtime's shared provider, not parsed here) |
| `src/main.rs` | The EdgeCommons runtime (observability) + the relay's PRIMARY from `gg.raw_device_provider()` (fatal if no transport), the retried site connection, the private derived site LWT, graceful stop |
| `test-configs/` | Sample dual-broker config |
| `tests/e2e_dual_broker.rs`, `tests/e2e/` | The P3-6 **dual-EMQX end-to-end test** (real binary between two real brokers, assertions A–F above) + its rig (`run.sh`, `docker-compose.e2e.yml`) |
| `recipe.yaml`, `gdk-config.json`, `build.sh` | GREENGRASS packaging for the **bridge itself**: the device-IPC ↔ site-MQTT deploy (`--platform GREENGRASS --transport IPC -c GG_CONFIG`, IPC pubsub `accessControl`, the `greengrass` feature build) |
| `deploy/site-broker/` | The **site broker's** deploy recipes (P3-5, D-B13): HOST compose, GREENGRASS `DockerApplicationManager` recipe, KUBERNETES manifests, and the per-device ACL — see [`deploy/site-broker/README.md`](deploy/site-broker/README.md) |
| `config.schema.json` | The `component.instances[]` config contract (`edgecommons component validate` checks against it) |
| `AGENTS.md`, `CLAUDE.md`, `DESIGN.md` | Governance: agent notes, the Claude Code entry point, and the local decision register (build history, coverage/lockfile decisions, current validation gaps) |

## Operational rules

- **Exactly one bridge per device bus** — two bridges on one bus pair double-deliver everything
  (hop tags prevent loops, not duplication). On Kubernetes a boundary bridge is `replicas: 1` +
  `strategy: Recreate`; inside a cluster there is **no** bridge (the in-cluster broker is the
  aggregation point).
- **The site broker's per-device ACL is the security boundary** (§5.4 in the design) — deploy the
  bridge only against an ACL-enforcing site broker.
- **Live-path loss during WAN outages is by design** — durability belongs to the streaming
  subsystem, not the bus.
