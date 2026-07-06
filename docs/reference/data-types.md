# Reference — Data Types

The bridge is a relay, not a codec — it does **not** decode payloads into typed values. What it *does* read
and write are a small set of **envelope structures**: the edgecommons message envelope (to append a hop tag and,
for a reply, to rewrite a header), the reserved `_relay` hop tag, the reply `reply_to`, and the UNS class
taxonomy that decides routing. This page is the reference for those structures. For the topics and messages
they ride on, see [messaging-interface.md](messaging-interface.md).

## Envelope vs raw — the fundamental split

Every message the bridge touches is one of two kinds, and it treats them differently:

| Kind | What it is | How the bridge relays it |
|------|-----------|--------------------------|
| **Envelope** | A edgecommons message: a JSON object with a `header` (and optionally `identity`, `tags`, `body`). | Parsed; the **hop tag** is appended (and for a reply, `header.reply_to` is stripped); re-serialized and forwarded. Structurally identical to the input except those touches. |
| **Raw** | Anything else — a bare JSON value with no envelope shape, or non-JSON bytes. | Forwarded **byte-for-byte**. No tag is added (there is nowhere to put one); never re-wrapped as `{"raw": …}`. |

A message that *claims* to be an envelope but is malformed (e.g. `header` is not an object) is **dropped**
(`MalformedEnvelope`), not forwarded — a structurally broken envelope is a bug, not a payload to pass on.

The consequence for loop protection: raw messages cannot carry the hop tag, so they rely entirely on the
**class-disjointness** structural guard (uplink and downlink relay disjoint class sets — see
[messaging-interface.md](messaging-interface.md#the-relay-matrix)).

## The EdgeCommons envelope

The standard edgecommons envelope: `{ header, identity, tags, body }`. The bridge only ever
reads/writes `header` (for replies) and `tags` (for the hop tag); `identity` and `body` travel untouched.

```jsonc
{
  "header": {
    "name": "reload-config",                 // the message/verb name
    "version": "1.0",
    "uuid": "…", "timestamp": "…",
    "correlation_id": "corr-1",               // preserved verbatim across the bridge
    "reply_to": "edgecommons/reply-<uuid>"      // REWRITTEN on downlink; DROPPED on the reply back-haul
  },
  "identity": { "hier": [ … ], "path": "dallas/gw-01", "component": "opcua-adapter", "instance": "main" },
  "tags": {
    "site": "dallas",                          // arbitrary business metadata — untouched
    "_relay": [ "gw-01/uns-bridge" ]           // the RESERVED hop tag the bridge appends
  },
  "body": { … }                                // untouched
}
```

The bridge never invents an `identity` or `body`, never re-orders members meaningfully (serde member order is
deterministic; structural equality is what's guaranteed), and touches exactly the two things below.

## The `_relay` hop tag

The reserved envelope tag `tags._relay` is the bridge's loop-protection ledger.

| Aspect | Value |
|--------|-------|
| Key | `_relay` (the `_` prefix marks a library/system-reserved tag key, alongside `_bcast`). |
| Type | A JSON **array of strings**. |
| Element | A **hop id**: `{device}/uns-bridge` (this bridge's device token + the component token). |
| Order | Insertion order — each bridge appends its own id to the end; foreign hops are preserved in order. |

The three rules the bridge applies before appending (see [explanation](../explanation.md#loop-protection--the-hop-tag)):

1. **Own echo** — if the array already contains this bridge's own hop id → **drop** (`OwnEcho`).
2. **Max hops** — if the array already holds `maxHops` ids (default 4) → **drop** (`MaxHopsExceeded`).
3. Otherwise **append** this bridge's id and forward.

Edge behaviors, exactly as implemented:

- An envelope with **no `tags` member** grows one containing just `_relay` on first hop.
- A **non-array** `_relay` (a spec violation by some non-conforming relay) is **normalized** to a fresh array
  (with a warning); the `maxHops` cap still bounds any residual cycle.
- A **raw** message is a no-op for hop-stamping — it never grows a `tags` member.
- Consumers should **ignore** `_relay` for business logic; it doubles as a "which bridges did this traverse"
  breadcrumb.

## `reply_to` — the two rewrites

`header.reply_to` is the only header field the bridge modifies, and it does so in exactly two places, only for
**envelope** commands/replies (raw ones pass byte-for-byte):

| Where | What the bridge does |
|-------|----------------------|
| **Downlink `cmd` with `reply_to`** | Replaces the site-side `reply_to` (e.g. `edgecommons/reply-<uuid>` on the *site* broker) with a **freshly minted** `edgecommons/reply-<uuid>` topic on the *device* bus, subscribes that topic, and records `bridge topic → original site reply_to`. A `cmd` **without** `reply_to` is a fire-and-forget notification — passed through untouched. |
| **The reply back-haul** | `header.reply_to` is **dropped** entirely (a reply carries none, and a device-bus topic is meaningless at the site). |

`correlation_id`, `body`, `identity`, and every other tag are preserved verbatim across both. The minted topic
uses the core's standard `edgecommons/reply-` prefix, so it is a non-UNS topic (never matches a UNS filter) and
is structurally exempt from the reserved-class guard.

## The UNS class taxonomy (what routes where)

Routing is by the **class** token — the 5th topic level (`ecv1/{device}/{component}/{instance}/{class}`). The
eight closed UNS classes, and how the bridge treats each:

| Class | Leaf/Channeled | Reserved? (library-owned publish) | Uplink (device→site) | Downlink (site→device) |
|-------|----------------|-----------------------------------|----------------------|------------------------|
| `state` | leaf | reserved | ✅ always | — |
| `cfg` | leaf | reserved | ✅ always | — |
| `evt` | channeled | open | ✅ always (+ disconnect replay buffer) | — |
| `metric` | channeled | reserved | ✅ always | — |
| `data` | channeled | open | ✅ always | — |
| `log` | channeled | reserved | ✅ always (default; sample disables) | — |
| `app` | channeled | open | ⚙️ opt-in (default off) | — |
| `cmd` | channeled | open | ❌ never | ✅ own-device only |

- **Leaf** classes (`state`, `cfg`) end at the class token — no channel; their subscription filters have no
  trailing `/#`. **Channeled** classes require ≥ 1 channel token; their filters end in `/#`.
- **Reserved** classes are library-owned on the *publishing* side (a component may not raw-publish to them),
  but the bridge relays them freely — it operates below the reserved-class guard by design.
- The **uplink set** (`state cfg evt metric data log`, + `app` when enabled) and the **downlink set** (`cmd`)
  are disjoint — the structural loop guard.

## The rehydration broadcast envelope

On a site-reconnect rising edge the bridge publishes two notification-style `cmd` envelopes on the device bus
(`ecv1/{device}/_bcast/main/cmd/republish-state` and `…/republish-cfg`):

```jsonc
{ "header": { "name": "republish-state", "version": "1.0" }, "body": {} }
```

They carry **no** `identity`, **no** `tags`, and **no** `reply_to` — fire-and-forget. Each device component
answers by re-announcing its state keepalive and effective cfg. Answering is built into the edgecommons library
(the four-language device-side `RepublishListener`), on by default — components need no wiring. See
[explanation → reconnect rehydration](../explanation.md#reconnect-rehydration).

## The LWT payload

The site Last-Will is a normal MQTT will registered by the reused provider on the site connection:

| Field | Value |
|-------|-------|
| topic | `ecv1/{device}/uns-bridge/main/state` (must equal the bridge's real state topic) |
| payload | conventionally `{ "status": "UNREACHABLE" }` |
| qos | as configured (sample: `1`) |

Because the will lands on the bridge's own `state` topic, a site console tracking `ecv1/+/+/+/state` sees the
whole device flip to UNREACHABLE on an abrupt bridge/device death — no bespoke plumbing.

## Metric value shapes

The relay counters are emitted as edgecommons metrics (see [messaging-interface.md](messaging-interface.md#metrics)).
Two value kinds:

| Kind | Emitted value | Examples |
|------|---------------|----------|
| **Counter** | the **interval delta** since the previous 30 s snapshot (`curr − prev`, saturating so a restart never yields a negative), so deltas sum correctly in CloudWatch/EMF | `relay_uplinked`, `relay_downlinked`, `relay_dropped_*`, `relay_reply_*`, `relay_evt_*` |
| **Gauge** | the **current** value | `relay_pending_replies` (in-flight replies), `site_connected` (`1`/`0`) |

Per-class counter metrics (`relay_uplinked`, `relay_dropped_disabled`, `relay_dropped_rate`,
`relay_dropped_disconnected`) carry one measure per class in the fixed order
`state, cfg, evt, metric, data, log, app`; scalar counters carry a single `count` measure; the
`site_connected` gauge carries a single `connected` measure (unit `None`; everything else is `Count`).

## Drop reasons (for reading logs & counters)

Every non-forward decision has a reason, which maps to a counter:

| Reason | Meaning | Counter |
|--------|---------|---------|
| `OwnEcho` | hop tag already holds our own id | `relay_loop_dropped` |
| `MaxHopsExceeded` | hop tag already holds `maxHops` ids | `relay_loop_dropped` |
| `NotUnsTopic` | topic isn't a valid `ecv1/…/{class}` UNS topic | `relay_routed_dropped` |
| `ClassNotRelayed` | class doesn't flow in this direction (e.g. `cmd` on uplink) | `relay_routed_dropped` |
| `NotOwnDevice` | a downlink `cmd` for a different device | `relay_routed_dropped` |
| `MalformedEnvelope` | claims to be an envelope but isn't structurally valid | `relay_malformed_dropped` |
| (disabled / rate / disconnected) | uplink policy verdicts, per class | `relay_dropped_disabled` / `_rate` / `_disconnected` |
