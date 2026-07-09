# Reference - Metrics

The UNS bridge emits relay metrics through the EdgeCommons metric service. With
`metricEmission.target: messaging`, metrics are published on the reserved UNS `metric` class:

```text
ecv1/{device}/uns-bridge/main/metric/{metricName}
```

The bridge originates its own metrics on the device bus and then relays them to the site broker like
other device traffic. It also forwards other components' `metric` messages. This page describes only
the bridge's own custom non-system metrics.

## Dimension model

The bridge's metric measures encode UNS class or scalar counter names instead of adding per-class
CloudWatch dimensions. Runtime-injected component dimensions identify the bridge component and device.

No topic, reply topic, endpoint, or error text is used as a metric dimension. Use logs and events for
those high-cardinality diagnostics.

## Emission model

Counters emit interval deltas. Gauges emit the current value. Deltas are saturating, so a process
restart or counter reset never produces a negative value.

Per-class metrics use one measure per relayed class: `state`, `cfg`, `evt`, `metric`, `data`, `log`,
and `app`.

## Per-class relay metrics

Dimensions: runtime-injected component dimensions only.

| Metric | Measures | Unit | Purpose |
|---|---|---:|---|
| `relay_uplinked` | `state`, `cfg`, `evt`, `metric`, `data`, `log`, `app` | Count | Messages successfully relayed from device bus to site broker by class. Helps verify which traffic classes are crossing the bridge. |
| `relay_dropped_disabled` | `state`, `cfg`, `evt`, `metric`, `data`, `log`, `app` | Count | Uplink messages dropped because the class is disabled by policy. Helps diagnose missing traffic caused by configuration. |
| `relay_dropped_rate` | `state`, `cfg`, `evt`, `metric`, `data`, `log`, `app` | Count | Uplink messages dropped by token-bucket rate caps. Helps tune class-level rate limits. |
| `relay_dropped_disconnected` | `state`, `cfg`, `evt`, `metric`, `data`, `log`, `app` | Count | Uplink messages dropped while the site link was down or after site publish failure when the class is not buffered. Helps quantify WAN outage loss. |

## Scalar counter metrics

Dimensions: runtime-injected component dimensions only.

Each metric uses one measure, `count`.

| Metric | Unit | Purpose |
|---|---:|---|
| `relay_downlinked` | Count | Commands relayed from site to device. Helps measure remote control traffic. |
| `relay_loop_dropped` | Count | Messages dropped by hop-tag loop protection or max-hop checks. Helps detect relay loops or miswired bridges. |
| `relay_routed_dropped` | Count | Messages dropped by class-routing, device-pinning, or non-UNS-topic checks. Helps diagnose broker ACL or topic-shape problems. |
| `relay_malformed_dropped` | Count | Payloads that could not decode as valid EdgeCommons protobuf envelopes. Helps detect non-EdgeCommons publishers on relayed filters. |
| `relay_publish_failed` | Count | Forwarding decisions whose republish failed at the transport. Helps identify broker or network publish failures. |
| `relay_reply_relayed` | Count | Replies relayed from device back to site through the correlation map. Helps confirm request/reply proxying works. |
| `relay_reply_expired` | Count | Correlation entries removed without a reply because of TTL expiry or eviction. Helps detect slow or lost device replies. |
| `relay_reply_stray` | Count | Replies that arrived without a live correlation entry and were dropped. Helps identify late replies or duplicate reply traffic. |
| `relay_evt_buffered` | Count | Events buffered while the site link was down. Helps verify disconnect buffering is active. |
| `relay_evt_buffer_dropped` | Count | Events evicted from the full disconnect buffer. Helps size the event replay buffer. |
| `relay_evt_replayed` | Count | Buffered events replayed after site reconnect. Helps confirm recovery behavior. |

## Gauge metrics

Dimensions: runtime-injected component dimensions only.

| Metric | Measure | Unit | Purpose |
|---|---|---:|---|
| `relay_pending_replies` | `count` | Count | Current in-flight request/reply correlations. Helps detect buildup in proxied commands. |
| `site_connected` | `connected` | None | `1` when the site broker connection is up, `0` when down. Helps drive bridge reachability alarms. |
