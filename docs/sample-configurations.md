# Sample Configurations

Complete, copy-paste-ready configurations for the UNS bridge (`com.mbreissi.edgecommons.UnsBridge`), built up from a
trivial two-broker dev loop to a realistic, TLS-secured site deployment with per-class policy — with an
explanation of **what every option does and how it changes runtime behavior**.

These are worked examples. For the exhaustive option list see [reference/configuration.md](reference/configuration.md);
for the topics/filters/metrics see [reference/messaging-interface.md](reference/messaging-interface.md); for the
envelope/hop-tag structures see [reference/data-types.md](reference/data-types.md); for task recipes see
[how-to-guides.md](how-to-guides.md); for the model behind it all see [explanation.md](explanation.md).

The bridge loads **one JSON document**. The top level carries the standard edgecommons `messaging` (the
**device** bus), `hierarchy`/`identity`, `heartbeat`, `metricEmission`, `logging`, and the required
`component` — whose `instances[]` entry with a `siteBroker` declares the **site** broker and every relay knob.
The single most important thing to keep straight in every config below: **`messaging.local` is the device bus;
`component.instances[site].siteBroker` is the site bus.** One process, two buses.

---

## Read this first — the site entry, and the state topic

The bridge finds its site broker in `component.instances[]`: the entry with `id: "site"`, or (if none is named
that) the sole entry that declares a `siteBroker`. Two `siteBroker` entries and none named `"site"` is a
startup error.

The bridge's **own** UNS identity comes from `hierarchy`/`identity` + `-t/--thing`, and it determines the real
`state` topic the LWT must match:

```jsonc
"hierarchy": { "levels": ["site", "device"] },
"identity":  { "site": "dallas" }
// with  --thing gw-01   →   state topic = ecv1/gw-01/uns-bridge/main/state
```

Set `instances[site].lwt.topic` to exactly that. A mismatch only WARNs (advisory), but a WARN means your
UNREACHABLE will land where nobody listens.

---

## 1. Minimal local / dev (two MQTT brokers)

The smallest config that relays one device bus onto a second broker standing in for the site bus. This is the
bundled [`test-configs/config.json`](../test-configs/config.json) shape — device broker `:1883`, site broker
`:1884`.

Bring up two brokers and run it:

```bash
docker run -d --name uns-device-broker -p 1883:1883 emqx/emqx
docker run -d --name uns-site-broker   -p 1884:1883 emqx/emqx
uns-bridge --config ./config.json --thing gw-01
```

`config.json`:

```jsonc
{
  "hierarchy": { "levels": ["site", "device"] },
  "identity": { "site": "dallas" },
  "logging": { "level": "INFO" },

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
        "lwt": { "topic": "ecv1/gw-01/uns-bridge/main/state", "payload": { "status": "UNREACHABLE" }, "qos": 1 }
      }
    ]
  }
}
```

**What each option does at runtime**

| Option | Effect |
|--------|--------|
| `hierarchy` / `identity` | Place the bridge in the UNS tree. With `--thing gw-01` the device token is `gw-01`; the bridge's own topics are `ecv1/gw-01/uns-bridge/main/...`. |
| `messaging.local.host/port` | The **device** broker. The runtime connects here for the bridge's own state/cfg/metric; the relay opens a **second** client here (id `uns-bridge-local-relay`) for the raw byte relay. |
| `messaging.local.clientId` | The runtime's device-bus client id. The relay derives `<clientId>-relay` so the two never collide (a shared id makes the broker bounce them in a session-takeover loop). |
| `messaging.requestTimeoutSeconds` | The framework request deadline. Paired with `reply.ttlSecs` (defaulted here to 60 = 2×30). |
| `heartbeat` | The bridge's own `state` keepalive on `ecv1/gw-01/uns-bridge/main/state` every 5 s — which matches the uplink `state` filter and rides the relay to the site. |
| `metricEmission.target: messaging` | Publishes the 30 s relay counters on the UNS `metric` class, so they too ride the relay. |
| `component.instances[site].siteBroker` | The **site** broker — the bridge's external system. Every uplinked message is republished here topic-verbatim. |
| `lwt` | Registers a Last-Will on the site connection: if the bridge dies abruptly, the site broker publishes `{status:UNREACHABLE}` on the bridge's state topic. |

With no `uplink`/`reply`/`maxHops`/`queue` given, the defaults apply: all six consumer classes relayed (`app`
off, `log` on by code default), no rate caps, the `evt` buffer on at 1000, `reply.ttlSecs 60` /
`maxPending 1024`, `maxHops 4`, `queue { data:512, default:64 }`.

---

## 2. A realistic site deployment (TLS + per-class policy)

This is the centerpiece: one gateway device (`gw-01`) at a Dallas plant bridging onto a **TLS + per-device-ACL**
site broker, with a per-class uplink policy sized for a busy line — `data` and `metric` rate-capped, `log`
kept off the WAN, the `evt` buffer enlarged, and the reply TTL raised in step with a longer request deadline.

```jsonc
{
  "tags": { "appId": "line5" },
  "hierarchy": { "levels": ["site", "area", "line", "device"] },
  "identity": { "site": "dallas", "area": "assembly", "line": "5" },
  "logging": { "level": "INFO" },

  "messaging": {
    "local": { "host": "localhost", "port": 1883, "clientId": "uns-bridge-gw-01" },
    "requestTimeoutSeconds": 45
  },

  "heartbeat": { "enabled": true, "intervalSecs": 5 },
  "metricEmission": { "target": "messaging" },

  "component": {
    "instances": [
      {
        "id": "site",
        "siteBroker": {
          "host": "site-broker.dallas.example", "port": 8883, "clientId": "uns-bridge-gw-01",
          "credentials": { "certPath": "/certs/client.pem", "keyPath": "/certs/client.key", "caPath": "/certs/ca.pem" }
        },
        "lwt": { "topic": "ecv1/gw-01/uns-bridge/main/state", "payload": { "status": "UNREACHABLE" }, "qos": 1 },

        "uplink": { "classes": {
          "state":  { "enabled": true },
          "cfg":    { "enabled": true },
          "evt":    { "enabled": true, "bufferWhileDisconnected": { "maxMessages": 5000 } },
          "metric": { "enabled": true, "maxRatePerSec": 50 },
          "data":   { "enabled": true, "maxRatePerSec": 200, "burst": 400 },
          "log":    { "enabled": false },
          "app":    { "enabled": false }
        } },

        "reply":  { "ttlSecs": 90, "maxPending": 2048 },
        "maxHops": 4,
        "queue":  { "data": 1024, "default": 64 }
      }
    ]
  }
}
```

Note the device token here is `gw-01` even though `hierarchy` has four levels — the **last** level is always
the resolved thing name (`--thing gw-01`); `site`/`area`/`line` come from `identity`. So the LWT topic is still
`ecv1/gw-01/uns-bridge/main/state` (rootless grammar — the enterprise path rides the envelope `identity`, not
the topic).

### How this config behaves

**TLS + ACL boundary.** The site connection is mTLS (`8883` + `credentials`). Because the relay carries no
in-process guard, the site broker's per-device ACL — which must permit `uns-bridge-gw-01` to publish only under
`ecv1/gw-01/#` and read only `ecv1/gw-01/+/+/cmd/#` — is the actual security boundary. Pair this bridge with a
site broker deployed from `deploy/site-broker/` (its `acl.conf` + certs).

**`data` rate cap (200/s, burst 400).** A token bucket starts full at 400 tokens; a momentary burst of up to
400 `data` messages passes immediately, then the sustained ceiling is 200/s. Anything over is **dropped** and
counted `relay_dropped_rate[data]` — never queued (the live path is not durable). If a line genuinely produces
>200 `data`/s sustained and you need all of it, that's the streaming subsystem's job, not the bridge's.

**`metric` rate cap (50/s, burst 100).** `burst` defaults to `2×rate`, so 100 here. Protects the site link
from a metric storm.

**`evt` buffer (5000).** Events/alarms are the one class kept across a WAN blip. If the site link drops, up to
5000 `evt` are held in a memory-only drop-oldest buffer; on reconnect they replay **in order** and the buffer
clears. `data`/`metric`/`state`/`cfg` published during the same outage are dropped.

**`log` off.** The code default is on, but shipping log tailing across the WAN is rarely wanted — disabling it
means the `ecv1/+/+/+/log/#` filter is still subscribed but every match drops + counts `dropped_disabled[log]`.
(To not even subscribe a class, that mechanism exists only for `app`.)

**`app` off.** The seventh uplink filter (`ecv1/+/+/+/app/#`) is **not subscribed** at all.

**Reply paired knob (90 s / deadline 45 s).** `requestTimeoutSeconds` was raised to 45, so `reply.ttlSecs` is
raised to 90 (≥ 2×) — the bridge won't tear down a reply path before a 45 s request settles.

**Queues.** `data` gets a 1024-deep subscription queue (a bursty line); everything else (incl. the downlink
`cmd`) gets 64.

### The resolved uplink filters (subscribed on the device bus)

With `app` off, six filters (the leaf classes `state`/`cfg` have no `/#`):

```text
ecv1/+/+/+/state     ecv1/+/+/+/cfg        ecv1/+/+/+/evt/#
ecv1/+/+/+/metric/#  ecv1/+/+/+/data/#     ecv1/+/+/+/log/#
```

…and one downlink filter, pinned to this device, on the site broker:

```text
ecv1/gw-01/+/+/cmd/#
```

### Worked example — one telemetry message crossing up

A device component publishes on the device bus:

```
topic:  ecv1/gw-01/opcua-adapter/kep1/data/Temperature
body:   { "header": {...}, "identity": {...}, "body": { "value": 21.4, "quality": "GOOD" } }
```

The bridge matches `ecv1/+/+/+/data/#`, re-checks class=`data` (relayed) — parses the envelope, appends its hop
id, and (assuming the `data` bucket has a token) republishes **byte-structurally identical except the hop tag**
to the *same topic* on the site broker:

```
site topic:  ecv1/gw-01/opcua-adapter/kep1/data/Temperature   (identical)
tags._relay: [ "gw-01/uns-bridge" ]                            (appended)
```

`relay_uplinked[data]` increments. A site consumer on `ecv1/+/+/+/data/#` receives it with zero per-device
configuration.

---

## 3. Tuning the uplink policy — three worked scenarios

The `uplink.classes` block is the main behavioral lever. Three common shapes:

**a) "Alarms only over a thin link."** Ship state/cfg/evt, drop the high-volume classes entirely:

```jsonc
"uplink": { "classes": {
  "state": { "enabled": true }, "cfg": { "enabled": true }, "evt": { "enabled": true },
  "metric": { "enabled": false }, "data": { "enabled": false }, "log": { "enabled": false }
} }
```

Every `data`/`metric`/`log` message now drops + counts `dropped_disabled`. `evt` still buffers on disconnect.

**b) "Protect the link, keep everything."** Cap the firehose classes but relay all:

```jsonc
"uplink": { "classes": {
  "data":   { "maxRatePerSec": 100, "burst": 200 },
  "metric": { "maxRatePerSec": 20 },
  "log":    { "maxRatePerSec": 10 }
} }
```

Under sustained overload the *excess* drops (counted `dropped_rate` per class) while the classes stay logically
enabled.

**c) "Never lose an alarm, tolerate a long outage."** Enlarge the `evt` buffer:

```jsonc
"uplink": { "classes": { "evt": { "bufferWhileDisconnected": { "maxMessages": 20000 } } } }
```

Up to 20000 events survive a WAN outage in memory (drop-oldest beyond that), replayed in order on reconnect.
Remember: memory-only — a bridge *restart* during the outage still loses them.

**Option → runtime effect (uplink policy)**

| Option | Effect on runtime behavior |
|--------|---------------------------|
| `enabled: false` | The class's messages drop + count `dropped_disabled`. For `app` only, off also means the filter isn't subscribed. |
| `maxRatePerSec` | Token-bucket sustained ceiling. Over-cap → drop + `dropped_rate`. `0` = only the initial burst passes, then nothing. |
| `burst` | Bucket capacity (default `2×rate`); the bucket starts full, allowing an initial burst. |
| `bufferWhileDisconnected` (evt only) | On the site link being down / a publish failing, `evt` buffers (drop-oldest) instead of dropping; replayed in order on reconnect. `enabled:false` or `maxMessages:0` disables it. On any non-`evt` class it's ignored + warned. |

---

## 4. Request/reply across the bridge (and the paired knob)

Nothing extra is needed to make site→device request/reply work — the bridge proxies it. The only config that
matters is keeping the TTL aligned with the request deadline:

```jsonc
"messaging": { "requestTimeoutSeconds": 30 },
"component": { "instances": [ { "id": "site",
  "reply": { "ttlSecs": 60, "maxPending": 1024 }
} ] }
```

At runtime: a site console calls `request()` on `ecv1/gw-01/opcua-adapter/main/cmd/<verb>` with
`header.reply_to = edgecommons/reply-<uuid>` (a topic on the *site* broker). The bridge:

1. Mints a device-bus reply topic, subscribes it, rewrites the command's `reply_to` to it, records the
   mapping, and relays the command down (hop-tagged) — all before a fast responder could reply.
2. When the device responds on the bridge topic, relays that reply up to the **original** site `reply_to`
   (`correlation_id`/body intact, `reply_to` dropped, hop tag appended), then unsubscribes — one-shot.
3. If nothing answers within `ttlSecs`, expires the entry (unsubscribe + `reply_expired`).

**Option → runtime effect (reply)**

| Option | Effect |
|--------|--------|
| `reply.ttlSecs` | How long a proxied reply path lives. Must be ≥ `2×requestTimeoutSeconds` or the bridge may drop a still-valid reply path. Also sets the sweep cadence (`min(ttl/4, 5 s)`). |
| `reply.maxPending` | Max concurrent in-flight requests. Overflow evicts the **oldest** (counted `reply_expired`) so a stuck responder can't starve fresh commands. `0` → treated as 1. |

---

## 5. Multi-bridge topology and `maxHops`

`maxHops` is loop defense across *distinct* bridges (own-echo handles a single bridge). Consider a device whose
bus is bridged to a **line** aggregation broker, which is itself bridged to a **plant** broker:

```
device bus ──uns-bridge(gw-01)──▶ line broker ──uns-bridge(line5)──▶ plant broker
```

Each hop appends a hop id, so a message arriving at the plant broker carries
`_relay: ["gw-01/uns-bridge", "line5/uns-bridge"]`. With the default `maxHops: 4` you can chain up to four
bridges before a message is dropped (`MaxHopsExceeded` → `relay_loop_dropped`). In a shallow topology, lower it
to fail fast on an accidental loop:

```jsonc
"maxHops": 2
```

**The non-negotiable rule:** exactly **one bridge per device bus**. Two bridges on the same bus pair
double-deliver everything — the hop tag prevents *loops*, not *duplication*. (On Kubernetes: `replicas: 1`,
`strategy: Recreate`.)

---

## 6. Kubernetes boundary bridge

The *same binary* deployed as a boundary between an on-prem device bus and an in-cluster aggregation broker. The
config is a standard config mounted from a ConfigMap; the only Kubernetes-specific requirements are the
Deployment shape and the "no bridge inside the cluster" rule.

```yaml
# boundary-bridge (see deploy/site-broker/k8s/boundary-bridge.example.yaml)
apiVersion: apps/v1
kind: Deployment
metadata: { name: uns-bridge-gw-01 }
spec:
  replicas: 1                 # EXACTLY one — two would double-deliver
  strategy: { type: Recreate } # never two overlapping bridges during a rollout
  template:
    spec:
      containers:
        - name: uns-bridge
          image: uns-bridge:latest
          args: ["--config", "/config/config.json", "--thing", "gw-01"]
          volumeMounts: [{ name: cfg, mountPath: /config }]
      volumes: [{ name: cfg, configMap: { name: uns-bridge-gw-01 } }]
```

The config's `messaging.local` points at the **on-prem device broker** (reachable from the pod); the
`siteBroker` points at the **in-cluster** aggregation broker (`emqx.yaml` in the same deploy set). There is
**no** bridge *inside* the cluster — the in-cluster broker *is* the aggregation point.

---

## 7. Greengrass

On a Greengrass core the bridge runs in its **HOST/MQTT** shape: the device bus is a device-local MQTT broker
and the site half is MQTT. The bridge requires the default `standalone` feature; a Nucleus-IPC-primary device
bus is not supported. The `recipe.yaml`/`gdk-config.json` package it for a Greengrass core against that
device-local broker (`GG_CONFIG`).

The recipe's default config uses `{ThingName}`/`{ComponentFullName}` templates in the site entry, resolved
Greengrass-side at deployment:

```yaml
component:
  instances:
    - id: "site"
      siteBroker:
        host: "site-broker.example"
        port: 8883
        clientId: "uns-bridge-{ThingName}"
        credentials:
          certPath: "/greengrass/v2/work/{ComponentFullName}/certs/client.pem"
          keyPath:  "/greengrass/v2/work/{ComponentFullName}/certs/client.key"
          caPath:   "/greengrass/v2/work/{ComponentFullName}/certs/ca.pem"
      lwt: { topic: "ecv1/{ThingName}/uns-bridge/main/state", payload: { status: "UNREACHABLE" }, qos: 1 }
```

> The bridge's own config layer template-resolves only the site **`lwt.topic`**; the `siteBroker`/`credentials`
> templates above are resolved by the recipe's Greengrass-side substitution.

The **site broker** also has its own Greengrass deployment (running the broker via
`aws.greengrass.DockerApplicationManager`) — see `deploy/site-broker/greengrass/`. That runs the broker the
bridge connects to, not a bridge inside a core.

---

## Deployment quick-reference

| Platform | Device bus | Site bus | Notes |
|----------|-----------|----------|-------|
| **HOST** | local MQTT | MQTT (TLS in prod) | the default; `uns-bridge --config … --thing …` |
| **KUBERNETES** (boundary) | on-prem MQTT | in-cluster MQTT | `replicas:1` + `Recreate`; no bridge *inside* the cluster |
| **GREENGRASS** | device-local MQTT | MQTT | runs in the HOST/MQTT shape on a core; a Nucleus-IPC-primary device bus is not supported |

Pair every bridge with an **ACL-enforcing site broker** (`deploy/site-broker/`) — that ACL, not any code in the
bridge, is the security boundary.
