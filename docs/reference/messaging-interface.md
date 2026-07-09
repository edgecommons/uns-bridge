# Reference — Messaging Interface & CLI

Every topic and filter the bridge subscribes or publishes, exactly which of the eight UNS classes cross it and
how, the metrics it emits, and its CLI flags. For the model behind the two directions, see
[explanation.md](../explanation.md); for the envelope/tag structures, see [data-types.md](data-types.md); for
client recipes, the [how-to guides](../how-to-guides.md).

## The UNS topic grammar

All addressing follows the **Unified Namespace**. A concrete topic is:

```
ecv1[/{site}]/{device}/{component}/{instance}/{class}[/{channel…}]
```

- `ecv1` — the fixed UNS root literal.
- `{site}` — present **only** under the rooted grammar (`topic.includeRoot: true` **and** a multi-level
  hierarchy). The bridge relays the **rootless** grammar (`topic.includeRoot: false`, the default).
- `{device}` — the resolved Thing name (the last `hierarchy` level).
- `{component}` — the component short name (the bridge's own is `uns-bridge`; the reserved broadcast
  pseudo-component is `_bcast`).
- `{instance}` — a component instance id, or `main`.
- `{class}` — one of the eight closed classes (below).
- `{channel…}` — 1–3 further tokens for channeled classes; **absent** for leaf classes (`state`, `cfg`).

Tokens forbid `/ + # \` and control characters and the `..` sequence; topics cap at 7 `/` separators (AWS IoT
Core's 8-level limit) and 256 UTF-8 bytes. The bridge builds every filter/topic through the library
(`Uns::filter` / `Uns::topic_for`), so a bad device token fails at **startup**, not at subscribe time.

## The eight UNS classes

| Class | Token | Leaf/Channeled | Reserved (library-owned publish) |
|-------|-------|----------------|----------------------------------|
| State | `state` | leaf | ✅ |
| Config | `cfg` | leaf | ✅ |
| Metric | `metric` | channeled | ✅ |
| Log | `log` | channeled | ✅ |
| Data | `data` | channeled | — |
| Event | `evt` | channeled | — |
| Command | `cmd` | channeled | — |
| App | `app` | channeled | — |

"Reserved" governs who may *publish* (components can't raw-publish to reserved classes); it does **not** limit
the relay, which forwards below the guard.

## The relay matrix

This is the whole routing contract. **Uplink** subscribes six wildcards on the device bus and republishes each
valid edgecommons protobuf message, topic-verbatim, on the site broker; **downlink** subscribes one pinned
filter on the site broker and republishes valid protobuf commands on the device bus.

| Direction | Classes relayed | Subscription filter(s) | Republished to |
|-----------|-----------------|------------------------|----------------|
| **Uplink** (device → site) | `state`, `cfg`, `evt`, `metric`, `data`, `log` (six consumer classes); `app` opt-in | `ecv1/+/+/+/state` · `ecv1/+/+/+/cfg` · `ecv1/+/+/+/evt/#` · `ecv1/+/+/+/metric/#` · `ecv1/+/+/+/data/#` · `ecv1/+/+/+/log/#` (+ `ecv1/+/+/+/app/#` when `app` enabled) | the **identical topic** on the site broker, protobuf envelope decoded, hop tag appended, then re-encoded |
| **Downlink** (site → device) | `cmd` only, **pinned to this bridge's own device** | `ecv1/{device}/+/+/cmd/#` | the **identical topic** on the device bus, hop tag appended |

Notes:

- **Leaf filters have no `/#`** (`state`, `cfg` end at the class token); channeled filters do. This is why the
  `state`/`cfg` filters look different from the rest.
- The downlink filter's `+` in the component position also matches the reserved **`_bcast`** pseudo-component,
  so `ecv1/{device}/_bcast/main/cmd/republish-*` is relayed like any other own-device `cmd`.
- **`cmd` is never uplinked** (no cross-device request/reply). The uplink set ∩ downlink set = ∅, which
  prevents a single bridge from matching its own downlink as uplink. Non-protobuf payloads are not a fallback
  relay path; they are dropped as malformed.
- Even though the filters already constrain arrivals, the engine **re-checks** class + device on every message
  (defense against a misconfigured broker ACL); a message that fails re-check is dropped and counted
  (`ClassNotRelayed` / `NotOwnDevice` / `NotUnsTopic` → `relay_routed_dropped`).

A **site-side fleet consumer** subscribes the same six wildcards on the site broker and sees every bridged
device with zero per-device knowledge:

```text
ecv1/+/+/+/state     ecv1/+/+/+/cfg      ecv1/+/+/+/evt/#
ecv1/+/+/+/metric/#  ecv1/+/+/+/data/#   ecv1/+/+/+/log/#
```

## What the bridge itself publishes

Because the bridge is a edgecommons component, it also **originates** traffic (on the device bus, via its
OBSERVABILITY connection), which then rides its own uplink to the site:

| Topic | Class | Cadence | What |
|-------|-------|---------|------|
| `ecv1/{device}/uns-bridge/main/state` | `state` | ~5 s (heartbeat) | The bridge's liveness keepalive. The private derived **site LWT** publishes `UNREACHABLE` here on abrupt death. |
| `ecv1/{device}/uns-bridge/main/cfg` | `cfg` | on start / change | The bridge's effective (redacted) config. |
| `ecv1/{device}/uns-bridge/main/metric/<name>` | `metric` | 30 s | The relay counters/gauges (below). |
| `ecv1/{device}/_bcast/main/cmd/republish-state` · `…/republish-cfg` | `cmd` | site-reconnect rising edge | The rehydration broadcasts, on the **device bus** only (best-effort; device components answer via the library's `RepublishListener`). |
| `edgecommons/reply-<uuid>` | (non-UNS) | per proxied request | A bridge-minted reply topic on the **device bus**, subscribed for one reply (see below). |

## Request/reply proxying

A downlink `cmd` carrying `header.reply_to` is proxied through the correlation map:

1. The bridge mints a device-bus reply topic (`edgecommons/reply-<uuid>`), **subscribes it first**, rewrites the
   command's `header.reply_to` to it, records `bridge topic → original site reply_to`, then relays the command
   to the device bus (hop-tagged).
2. The **first** protobuf message on that bridge topic is decoded, relayed to the **original** site `reply_to`
   with the hop tag appended and `header.reply_to` dropped, then re-encoded; the entry is removed and the
   bridge topic unsubscribed (one-shot).
3. Entries expire after `reply.ttlSecs` (default 60); the map is bounded by `reply.maxPending` (default 1024,
   evict-oldest). A reply with no live entry is a **stray** (dropped, counted `relay_reply_stray`).

A `cmd` **without** `reply_to` is a fire-and-forget notification and relays untouched. `correlation_id` is
never touched — correlation survives inside the relayed envelope.

## Metrics

Emitted every 30 s through `gg.metrics()`; with `metricEmission.target: messaging` they publish on the UNS
`metric` class (`ecv1/{device}/uns-bridge/main/metric/<name>`) and ride the bridge's own relay to the site.
Counters are **interval deltas**; gauges are **current** values. For every metric's measures, units,
and diagnostic purpose, see
[Reference - Metrics](metrics.md).

## Reserved classes and the guard

`state`/`metric`/`cfg`/`log` are library-owned **reserved** classes — a *component's* raw publish to them is
rejected by the messaging service's guard. The bridge is exempt: its relay runs at the raw provider level (no
guard in the path), which is exactly why it can forward other components' reserved-class protobuf traffic.
The durable boundary is instead the **site broker's per-device ACL** — deploy the bridge only against an
ACL-enforcing site broker.

## Startup, shutdown, and reconnection behavior

- **Startup order:** edgecommons runtime (device bus, fatal if down) → relay's provider-level device-bus connection (fatal
  if down) → derive the private site LWT topic from the bridge state topic → site connect (retried forever,
  ~5 s between tries; abandonable by a shutdown signal) → subscribe all filters → `relay running`.
- **Intermittent uplink:** the site connect retries in the bridge's own loop; the provider re-subscribes every
  filter on each reconnect, so recovery is transparent. A dead **device** bus is fatal (the bridge is useless
  without it); a dead **site** bus is not.
- **Shutdown (Ctrl-C / SIGTERM):** aborts every pump (incl. the TTL sweep and per-reply pumps), then
  **unsubscribes every filter at both brokers** — the six/seven uplink filters, the downlink filter, and every
  still-pending bridge reply topic — before exit, and logs a one-line counter tally. Unreplayed buffered `evt`
  is discarded (memory-only by design).

## CLI

The bridge's minimal CLI:

| Flag | Values | Notes |
|------|--------|-------|
| `-c`, `--config` | `<file>` | Bridge config file (default `test-configs/config.json`). Feeds both the runtime (`-c FILE <file>`) and the bridge's own site parse. |
| `-t`, `--thing` | `<name>` | Device (thing) token — the `{device}` of every UNS topic. Falls back to `$EDGECOMMONS_THING_NAME`; required (via one or the other). Takes the **full** string (guards the historical one-char truncation bug). |
| `-h`, `--help` | — | Usage. |

Internally the runtime is built with the synthesized standard argv
`uns-bridge --platform HOST --transport MQTT <file> -c FILE <file> -t <thing>`.
