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
§2.4), applies a **per-class uplink policy** — enables, token-bucket rate caps, and a bounded
`evt` replay buffer for WAN blips (§2.5 / D-B10) — and registers a Last-Will `UNREACHABLE` on the
site connection for fast whole-device reachability detection.

Design source of truth: `docs/platform/DESIGN-uns-bridge.md` (and `DESIGN-uns.md` §9) in the
ggcommons monorepo. Section references below (§…) point there.

## How it connects (§1, §2.1, §2.8)

Since P3-4b the bridge is a **proper ggcommons component**: a real `GgCommons` runtime (built from
the same config file) owns the bridge's own observability. The bridge therefore holds **three**
connections:

| Connection | What | Built from |
|---|---|---|
| **OBSERVABILITY** (device bus) | the `GgCommons` runtime: identity, the automatic heartbeat `state` keepalive on `ecv1/{device}/uns-bridge/main/state`, the effective-(redacted-)config `cfg` publisher, `gg.metrics()` (`metricEmission.target = "messaging"`), logging init, SIGTERM handling | the standard `messaging` config section — the config file doubles as the `--transport MQTT` payload |
| **RELAY PRIMARY** (device bus) | the raw byte relay (§1.3) + reply proxy + rehydration broadcast | the same `messaging` section with the client id suffixed **`-relay`** (two MQTT clients must not share an id) and any `lwt` stripped (the will belongs to the runtime connection) |
| **SITE** | the site UNS broker — the bridge's **external system**; carries the D-B11 LWT | the bridge's own `component.instances[]` `"site"` entry, by **reusing the ggcommons core's public MQTT objects**: `MqttProvider::connect(&site_cfg)` — zero library change |

Why two device-bus connections: `GgCommons` deliberately does **not** expose its raw
`MessagingProvider` (`DefaultMessagingService` keeps it private), and the relay must stay at the
raw provider level — byte-verbatim, no reserved-class guard (§1.3) — so sharing the runtime's
connection would require a ggcommons change this slice deliberately does not make. The cost is one
extra local TCP client on the device broker (trivial on HOST/EMQX). *Follow-up (Rust-only library
affordance)*: expose the runtime's raw provider (e.g. `DefaultMessagingService::provider()`) so
the relay can share it — one client less, which matters under the GREENGRASS shared-connection
quota once the IPC-primary variant lands.

The relay runs at the **raw `MessagingProvider` level** on both of its connections (byte relay).
The reserved-class publish guard is a `MessagingService` concern and is deliberately not in this
path (§1.3) — the site broker's **per-device ACL** is the durable boundary: a bridge may publish
only under its own `ecv1/{device}/#` subtree.

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
  (topic-verbatim, hop tag already stamped) and the buffer clears (`evt_replayed`); overflow while
  down evicts the oldest (`evt_buffer_dropped`). A live `evt` arriving while older ones are still
  queued joins the queue rather than overtaking them.
- **Reconnect rehydration** (DESIGN-uns §9.3 layer 2) — on the site-reconnect **rising edge** the
  bridge publishes `ecv1/{device}/_bcast/main/cmd/republish-state` and `…/republish-cfg` on the
  **device bus** (best-effort, notification-style `cmd` envelopes, **before** the `evt` replay) so
  every component's re-announce can ride the uplink and the site view rehydrates `state`/`cfg`
  without retain. Startup is not an edge — the relay only starts after the site link is first
  established. The device-side listener that answers the broadcast — components re-publishing
  their `state` keepalive and effective `cfg` on demand — **shipped in the ggcommons library**
  (`RepublishListener` in Java/Python/TS, `uns.rs` in Rust; on by default, jittered + coalesced),
  so a rev-bumped component fleet rehydrates the site view on every bridge reconnect. The
  broadcast is no longer inert.

## The bridge's own observability (§2.8, P3-4b)

Nothing bespoke: the heartbeat publishes the bridge's `state` keepalive on the device bus, the
`cfg` publisher announces its (redacted) effective config, and the §2.5 counters emit as
`metric`s — all of it matches the uplink filters and is **relayed by the bridge itself**, so the
site broker sees the bridge exactly as it sees any component (plus the LWT only it sets).

Every **30 s** a task snapshots the relay counters and emits them through `gg.metrics()`
(`ecv1/{device}/uns-bridge/main/metric/<name>` with the shipped `messaging` target). Counters emit
**interval deltas**, gauges the current value:

| Metric | Measures | Kind |
|---|---|---|
| `relay_uplinked`, `relay_dropped_disabled`, `relay_dropped_rate`, `relay_dropped_disconnected` | per class: `state` `cfg` `evt` `metric` `data` `log` `app` | counters |
| `relay_downlinked`, `relay_loop_dropped`, `relay_routed_dropped`, `relay_malformed_dropped`, `relay_publish_failed`, `relay_reply_relayed`, `relay_reply_expired`, `relay_reply_stray`, `relay_evt_buffered`, `relay_evt_buffer_dropped`, `relay_evt_replayed` | `count` | counters |
| `relay_pending_replies` | `count` | gauge |
| `site_connected` | `connected` (0/1) | gauge |

**LWT startup cross-check** (§2.6/§3.7, D-B11): at startup the bridge template-resolves the
configured site `lwt.topic` and compares it against its **real** state topic
(`gg.uns().topic(State)` — what the keepalive actually publishes on). A mismatch (the classic
misconfig: a typo'd or unsanitized device token) logs a **WARN**; a missing site LWT also WARNs
(whole-device UNREACHABLE detection would not work). The check is advisory — config stays
authoritative and the configured value is still what gets registered.

## Configuration (§2.7)

The site broker lives in the bridge's **own** `component.instances[]` — the existing per-instance
surface every component has (exactly how the opcua-adapter declares its OPC UA endpoints), reusing
the library's `MessagingConfig`/`mqttBroker` shape (including `lwt`). No schema change, no core
change. See [`test-configs/config.json`](test-configs/config.json) for a complete sample
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
        "lwt": { "topic": "ecv1/gw-01/uns-bridge/main/state",     // §2.6: whole-device UNREACHABLE
                 "payload": { "status": "UNREACHABLE" }, "qos": 1 },
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

Notes for the current slice: the config file is validated against the **canonical ggcommons
schema** at startup (the runtime loads it via `-c FILE`) — a shipped-config test pins that it
passes. Template substitution (`{ThingName}` → the sanitized device token) is resolved for the
**site `lwt.topic` only** (the load-bearing case, §1.2); templates elsewhere in the `instances[]`
entry stay literal until the full facade integration. The `lwt` entry is applied at CONNECT by
the reused provider and cross-checked at startup (D-B11, above); the `uplink` policy (§2.5 /
D-B10) and the `reply` knobs (§2.4) are fully enforced; the counters publish as `metric`s every
30 s (§2.8, above) and are still logged once at shutdown.

## Run locally (HOST)

```bash
# device broker on :1883 (the standard ggcommons test-infra EMQX) and a site broker on :1884
cargo run -- --config ./test-configs/config.json --thing gw-01
```

Device identity: `-t/--thing` or `GGCOMMONS_THING_NAME`. Logging is owned by the ggcommons
runtime (the config's `logging` section; default console `info`). Graceful shutdown (Ctrl-C /
SIGTERM, via the library's signal watcher) aborts the pumps and **unsubscribes every filter at
both brokers** before exit. The bridge's own `state` keepalive / `cfg` announce / `metric`
emission appear on the device bus immediately and on the site broker once the relay runs.

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
standing `ggcommons-emqx` on `:1883` or the P3-5 site broker on `:1884` — override with
`E2E_DEVICE_PORT`/`E2E_SITE_PORT`), then runs `tests/e2e_dual_broker.rs`, which spawns the **real
bridge binary** against the shipped sample config (ports swapped in) and asserts, over live MQTT,
per assertion with a printed PASS/FAIL:

- **A1–A3 uplink** — a `state` envelope, an `evt` envelope (with channel), and a **raw** `data`
  payload published on the device bus arrive **topic-verbatim** on the site broker; envelopes carry
  the appended hop tag, the raw payload is **byte-verbatim**;
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

**Local development against the sibling library**: this repo pins `ggcommons` by git rev in
`Cargo.toml` (what CI resolves). For local dev, a **gitignored** `.cargo/config.toml` patches the
dep to the sibling checkout — create it as:

```toml
[patch."https://github.com/edgecommons/ggcommons.git"]
ggcommons = { path = "../ggcommons/libs/rust" }
```

(The telemetry-processor pattern; delete the file for a pure git-rev build.)

## Deploying the site broker (P3-5, D-B13)

The bridge and the site broker deploy **as a pair** — see
[`deploy/site-broker/README.md`](deploy/site-broker/README.md) for the full recipe set: a
`docker-compose.yml` for HOST (and the local dual-EMQX dev/test rig it doubles as), a
`greengrass/recipe.yaml` sketch running the same compose via
`aws.greengrass.DockerApplicationManager`, `k8s/` manifests for the in-cluster aggregation broker
(no bridge runs inside a cluster, with one documented boundary-pod exception), and the per-device
**ACL** (`acl.conf`) that is the actual security boundary — the bridge's own raw-provider relay
(§1.3 above) carries no in-process guard, so an ACL-less site broker has no boundary at all.

## Repo layout

| Path | What |
|---|---|
| `src/relay.rs` | The **pure** relay decision engine: §2.2 class routing + own-device pinning, §2.3 hop tag, the §9.3 rehydration-topic derivation — no IO, fully unit-tested |
| `src/reply.rs` | The **pure** §2.4 reply proxy logic: the TTL'd correlation map (`rewrite_downlink`/`take`/`sweep`, evict-oldest) + the reply back-haul transform — no IO, injected clock |
| `src/policy.rs` | The **pure** §2.5/D-B10 uplink policy: per-class enables, token buckets (injected clock), and the bounded drop-oldest `evt` replay buffer — no IO |
| `src/observability.rs` | The **pure** §2.8 pieces: the counter→metric mapping (snapshot deltas → named measure groups) + the D-B11 LWT cross-check — no IO |
| `src/io.rs` | The pumps: raw-provider subscriptions → `RelayEngine::decide` → the §2.5 policy governor (uplink) / the reply rewrite (downlink) → topic-verbatim republish; per-reply one-shot pumps + the TTL sweep + the connectivity watcher (rising-edge rehydration broadcast + evt replay) + the 30 s metric emission; counters; unsubscribe-on-shutdown (incl. pending reply topics) |
| `src/config.rs` | The §2.7 config shape; maps the `"site"` instance entry onto the core `MessagingConfig`; the relay's `-relay`-suffixed device connection; typed `reply` + `uplink` knobs |
| `src/main.rs` | The GgCommons runtime (observability) + the relay's raw connections (device fatal, site retried), the D-B11 LWT cross-check, graceful stop |
| `test-configs/` | Sample dual-broker config |
| `tests/e2e_dual_broker.rs`, `tests/e2e/` | The P3-6 **dual-EMQX end-to-end test** (real binary between two real brokers, assertions A–F above) + its rig (`run.sh`, `docker-compose.e2e.yml`) |
| `recipe.yaml`, `gdk-config.json`, `build.sh` | GREENGRASS packaging stubs for the **bridge itself** (finalized with the GREENGRASS/IPC variant follow-up) |
| `deploy/site-broker/` | The **site broker's** deploy recipes (P3-5, D-B13): HOST compose, GREENGRASS `DockerApplicationManager` recipe, KUBERNETES manifests, and the per-device ACL — see [`deploy/site-broker/README.md`](deploy/site-broker/README.md) |

## Roadmap (the Phase-3 slices)

| Slice | Contents | Status |
|---|---|---|
| **P3-2** | repo scaffold; relay engine (six uplink filters + pinned downlink, topic-verbatim, hop tag/maxHops); unit tests over trait fakes | **done** |
| **P3-3** | `reply_to` rewrite: TTL'd correlation map, maxPending eviction, reply back-haul | **done** |
| **P3-4** | per-class uplink policy: enables, token-bucket rate caps, D-B10 disconnect behavior + the bounded drop-oldest `evt` replay buffer with in-order reconnect replay; per-class drop counters | **done** |
| **P3-4b** | the bridge's own GgCommons observability (§2.8): heartbeat `state` keepalive + `cfg` announce + counters published as `metric`s (30 s, riding the bridge's own relay); the D-B11 LWT startup cross-check; the bridge-side reconnect `republish-*` `_bcast` rehydration | **done** |
| **P3-5** | `deploy/site-broker/` recipes (HOST compose + dual-EMQX dev rig, GG DockerApplicationManager, k8s in-cluster broker + boundary-bridge example, the per-device **ACL** file, TLS notes) | **done** |
| **P3-6** | the repeatable **dual-EMQX bridge-level e2e** (`tests/e2e/run.sh` — real binary between two real brokers, 9/9 assertions A–F green) + the `edgecommons/registry` catalog entry (`category: bridge`) | **done** |

### Release state & remaining follow-ups

Shipped at the v0.2.0 UNS release:

- **GitHub remote + git-rev pin bump — done.** `edgecommons/uns-bridge` is published, and
  `Cargo.toml` pins `ggcommons` at rev `b1d8d85` — the v0.2.0 UNS release on `main` — so a pure
  git-rev build compiles against the shipped UNS core (the gitignored sibling `[patch]` override
  is local-dev only).
- **The 4-language `republish-state`/`republish-cfg` broadcast listener — done.** It shipped in
  the ggcommons library (`RepublishListener` in Java/Python/TS, `uns.rs` in Rust), so the bridge's
  reconnect rehydration broadcast is now answered by every rev-bumped component — no longer inert.
- **The edge-console as the first site-side client — done.** The full-system test (console ↔ site
  broker ↔ bridge ↔ device components) has been run and passed (HOST → kind); the P3-6 e2e above
  remains the *bridge-level* proof.

Still deferred (genuinely unbuilt):

- **GREENGRASS/IPC-primary variant** (PRIMARY = Nucleus IPC, SITE = MQTT): the `greengrass`
  feature today only compiles the library's IPC provider; the IPC-primary relay wiring is the
  follow-up, and **GREENGRASS** deployment validation rides it (HOST is proven by the e2e and
  KUBERNETES by the boundary-bridge deploy of `deploy/site-broker/k8s/`).
- The standard `-c`/`--platform`/`--transport` CLI contract (today the minimal `--config`/`--thing`
  CLI synthesizes the standard argv internally) and template substitution across the whole
  `instances[]` entry.
- A Rust-only library affordance exposing the runtime's raw `MessagingProvider` so the relay can
  share the runtime's device-bus connection (see "How it connects").
- Docs-site sync of this component's docs into the ggcommons website.

## Operational rules

- **Exactly one bridge per device bus** — two bridges on one bus pair double-deliver everything
  (hop tags prevent loops, not duplication). On Kubernetes a boundary bridge is `replicas: 1` +
  `strategy: Recreate`; inside a cluster there is **no** bridge (the in-cluster broker is the
  aggregation point).
- **The site broker's per-device ACL is the security boundary** (§5.4 in the design) — deploy the
  bridge only against an ACL-enforcing site broker.
- **Live-path loss during WAN outages is by design** — durability belongs to the streaming
  subsystem, not the bus.
