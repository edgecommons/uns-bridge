# Reference — Configuration

Every configuration option. For *why* these exist, see [explanation.md](../explanation.md); for tasks, the
[how-to guides](../how-to-guides.md); for the envelope/topic surface, see
[messaging-interface.md](messaging-interface.md); for complete worked configs, see
[sample-configurations.md](../sample-configurations.md).

## Config source

The bridge reads **one JSON document**. That same file feeds two things: the ggcommons runtime loads it as
the standard `-c FILE` config (and its top-level `messaging` section doubles as the `--transport MQTT`
payload — the **device** bus), and the bridge reads its own `component.instances[]` from it (the **site**
broker). The file is validated against the canonical ggcommons config schema at startup.

The bridge's CLI is minimal — `--config <file>` and `--thing <name>` — and synthesizes the standard ggcommons
argv (`--platform HOST --transport MQTT <file> -c FILE <file> -t <thing>`) internally.

## Top-level sections

| Section | Required | Purpose |
|---------|----------|---------|
| `messaging` | **yes** | The **device-local** bus (the runtime's OBSERVABILITY connection *and*, with `-relay` appended to the client id, the relay's PRIMARY connection). Also the request-deadline knob. |
| `component` | **yes** | Carries `instances[]`; the entry with a `siteBroker` declares the **site** broker and all relay knobs. |
| `hierarchy` | optional | UNS enterprise-hierarchy level names; the last level is the device. Absent ⇒ `["device"]`. |
| `identity` | optional | Values for every hierarchy level except the last (the resolved thing name). Together with `hierarchy` these set the bridge's own `identity` and its real `state` topic (which the LWT cross-check compares against). |
| `heartbeat` | optional | The bridge's own `state` keepalive (`{enabled, intervalSecs}`; on by default, 5 s). |
| `metricEmission` | optional | Routes the relay counters (`target: messaging` publishes them on the UNS `metric` class — the sample setting). |
| `logging` | optional | Standard ggcommons logging (console `info` by default). |
| `topic` | optional | `includeRoot` (default `false`); insert the site level after `ecv1` on a multi-site broker (effective only for a multi-level hierarchy). |

The top level tolerates other standard ggcommons sections (`tags`, etc.); unknown sections in the bridge's
own parse are ignored (forward compatibility).

> **There is deliberately no `component.name` in config.** The canonical schema allows only `global`/`instances`
> under `component`; the component's full name (`com.mbreissi.uns-bridge`) is supplied by the runtime builder,
> never by config. (The Greengrass `recipe.yaml` default config does set `component.name`, but that value is
> not what names the component.)

## `messaging` (the device bus)

The standard ggcommons `messaging` section. Only the fields the bridge relies on are called out here.

| Key | Type | Default | Definition |
|-----|------|---------|-----------|
| `local` | object | **required** | The device broker: `host`, `port`, `clientId` (+ `credentials`/TLS as any ggcommons broker). The runtime connects with the configured `clientId`; the relay connects with `clientId + "-relay"`. |
| `requestTimeoutSeconds` | number | `30` | The framework request-deadline. **Paired** with `reply.ttlSecs` — see below. |
| `lwt` | object | — | A device-bus Last-Will, if any, belongs to the **runtime** connection; the relay connection always strips it (the will must never be registered twice). The bridge's *load-bearing* LWT is the **site** LWT (`instances[site].lwt`), not this one. |

## `component.instances[]` — the site entry

The bridge scans `component.instances[]` for its site entry: **the entry with `id: "site"`**, or — when none
carries that id — the **single** entry that declares a `siteBroker`. Two entries with a `siteBroker` and none
named `"site"` is an error (ambiguous); no site entry at all is an error.

| Key | Type | Default | Definition |
|-----|------|---------|-----------|
| `id` | string | **required** | Instance id; `"site"` selects this entry explicitly. |
| `siteBroker` | object | **required** | The site broker endpoint — the library `mqttBroker` shape (below). Maps onto the reused provider's `local` slot; there is deliberately no `iotCore` on the site link. |
| `lwt` | object | — | The **site-connection** Last-Will (below). Applied verbatim by the reused provider at CONNECT; template-resolved + cross-checked at startup. |
| `uplink` | object | see §uplink | Per-class uplink policy: enables, rate caps, and the `evt` replay buffer. |
| `reply` | object | see §reply | The reply correlation-map knobs. |
| `maxHops` | number | `4` | Hop-tag cap (loop protection). |
| `queue` | object | see §queue | Per-class subscription queue depths. |

### `siteBroker`

The library `mqttBroker` shape (identical to any ggcommons broker config).

| Key | Type | Definition |
|-----|------|-----------|
| `host` | string | Site broker host. |
| `port` | number | Site broker port (e.g. `1883` plaintext, `8883` TLS). |
| `clientId` | string | MQTT client id on the site broker — unique per bridge. |
| `credentials` | object | mTLS: `{ certPath, keyPath, caPath }`. Omit for a plaintext/anonymous broker (dev only). |

### `lwt` (site Last-Will)

| Key | Type | Default | Definition |
|-----|------|---------|-----------|
| `topic` | string | — | **Must** equal the bridge's real state topic `ecv1/{device}/uns-bridge/main/state`. `{ThingName}` is template-resolved. A mismatch WARNs at startup (advisory). |
| `payload` | object/string | — | The will payload — conventionally `{ "status": "UNREACHABLE" }`. |
| `qos` | number | `0`* | Publish QoS for the will (the sample uses `1`). |

*The library applies its own QoS default when omitted; the bundled sample sets `qos: 1`.

### `uplink` — per-class policy

`uplink.classes` is a map keyed by class token. Every knob is optional; the defaults below are applied by the
policy engine, not by config. Unknown members inside a class are tolerated (forward compatibility).

| Key (per class) | Type | Default | Definition |
|-----|------|---------|-----------|
| `enabled` | bool | `true` for the six consumer classes, `false` for `app` | Whether the class is relayed. A disabled class's messages drop + count (`dropped_disabled`); a disabled `app` also means its filter is never subscribed. |
| `maxRatePerSec` | number | — (unlimited) | Token-bucket refill rate (messages/second). Over-cap traffic drops + counts (`dropped_rate`). `0` forwards only the initial `burst`, then drops forever (prefer `enabled: false`). |
| `burst` | number | `2 × maxRatePerSec` | Token-bucket capacity; the bucket starts full, so an initial burst of up to `burst` passes immediately. |
| `bufferWhileDisconnected` | object | see below | The `evt` disconnect replay buffer. **Honored for `evt` only** — on any other class it is ignored with a warning. |

`bufferWhileDisconnected`:

| Key | Type | Default | Definition |
|-----|------|---------|-----------|
| `enabled` | bool | `true` | Whether `evt` buffers (rather than drops) while the site link is down. |
| `maxMessages` | number | `1000` | Buffer bound; overflow drops the **oldest** (`evt_buffer_dropped`). `0` disables buffering. |

Class tokens accepted by `uplink.classes`: `state`, `cfg`, `evt`, `metric`, `data`, `log`, `app`. (`cmd` is
never uplinked and has no policy slot.)

> **`log` default nuance:** the design *recommends* shipping `log` off, and the sample config sets
> `"log": { "enabled": false }` — but the code default for `log` is **on** (matching the pre-policy relay
> behavior). Set it explicitly if you care.

### `reply` — the correlation map

| Key | Type | Default | Definition |
|-----|------|---------|-----------|
| `ttlSecs` | number | `60` | Correlation-entry TTL. **Paired knob:** `60 = 2 × messaging.requestTimeoutSeconds` (30). If you raise `requestTimeoutSeconds`, raise this in step. |
| `maxPending` | number | `1024` | In-flight entry bound; overflow evicts the **oldest** (counted as expired). `0` is treated as `1`. |

The TTL sweep runs every `min(ttlSecs/4, 5 s)`, floored at 100 ms.

### `queue` — per-class subscription depths

| Key | Type | Default | Definition |
|-----|------|---------|-----------|
| `data` | number | `512` | Queue depth for the `data` class subscription (deep — bursty telemetry). |
| `default` | number | `64` | Queue depth for every other subscription (shallow), including the downlink `cmd`. |

Overflow drops at the provider (with a warning). Per-reply device-bus subscriptions use a fixed depth of 1
(first-reply-wins).

## Identity, the state topic, and the LWT cross-check

`hierarchy.levels` names the UNS enterprise tree, deepest (the device) last; `identity` supplies every level's
value **except** the last (which is the resolved thing name from `-t/--thing`). These determine the bridge's
own `identity` element and its real state topic:

```jsonc
"hierarchy": { "levels": ["site", "device"] },
"identity":  { "site": "dallas" }
// with -t gw-01  →  state topic ecv1/gw-01/uns-bridge/main/state
```

At startup the bridge derives that exact topic (`gg.uns().topic(State)`) and compares it to the configured
`instances[site].lwt.topic`. Match ⇒ INFO; mismatch or missing LWT ⇒ WARN (advisory — the configured value is
still what gets registered). Set your `lwt.topic` to match.

## Precedence & defaults summary

- Site entry selection: **`id == "site"` ▸ the sole `siteBroker` entry ▸ error**.
- Class enable: **`uplink.classes.<class>.enabled` ▸ built-in (`true` for the six, `false` for `app`)**.
- Rate cap: **absent ⇒ unlimited**; `burst` **absent ⇒ 2×rate**.
- `evt` buffer: **absent ⇒ on/1000**; on a non-`evt` class ⇒ ignored + warn.
- `maxHops` **absent ⇒ 4**; `reply.ttlSecs` **absent ⇒ 60**, `reply.maxPending` **absent ⇒ 1024**;
  `queue.data` **absent ⇒ 512**, `queue.default` **absent ⇒ 64**.

## Complete example

The bundled [`test-configs/config.json`](../../test-configs/config.json) — device broker `:1883`, site broker
`:1884` (the dual-EMQX dev layout):

```jsonc
{
  "hierarchy": { "levels": ["site", "device"] },
  "identity": { "site": "dallas" },

  "messaging": {
    "local": { "host": "localhost", "port": 1883, "clientId": "uns-bridge-local" },
    "requestTimeoutSeconds": 30
  },

  "heartbeat": { "enabled": true, "intervalSecs": 5 },
  "metricEmission": { "target": "messaging" },

  "component": {
    "instances": [
      {
        "id": "site",
        "siteBroker": { "host": "localhost", "port": 1884, "clientId": "uns-bridge-site" },
        "lwt": { "topic": "ecv1/gw-01/uns-bridge/main/state", "payload": { "status": "UNREACHABLE" }, "qos": 1 },
        "uplink": { "classes": {
          "state":  { "enabled": true },
          "cfg":    { "enabled": true },
          "evt":    { "enabled": true, "bufferWhileDisconnected": { "maxMessages": 1000 } },
          "metric": { "enabled": true, "maxRatePerSec": 50 },
          "data":   { "enabled": true, "maxRatePerSec": 200, "burst": 400 },
          "log":    { "enabled": false },
          "app":    { "enabled": false }
        } },
        "reply": { "ttlSecs": 60, "maxPending": 1024 },
        "maxHops": 4,
        "queue": { "data": 512, "default": 64 }
      }
    ]
  }
}
```

## Current limits

- **Greengrass PRIMARY = Nucleus IPC is not supported.** The binary requires the default `standalone`
  feature; on a Greengrass core it runs in its HOST shape against a device-local MQTT broker.
- **The CLI is `--config`/`--thing` only** — it synthesizes the standard HOST/MQTT ggcommons argv internally.
- **Template substitution reaches only the site `lwt.topic`** (`{ThingName}`); other `instances[]` values are
  taken literally.
