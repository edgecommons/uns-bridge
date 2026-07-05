# uns-bridge — Documentation

`com.mbreissi.uns-bridge` is an **envelope-aware relay** that joins a device-local message bus to the
**site UNS broker** — one bridge per device bus. Every device carries its own bus (a local MQTT broker
on HOST, the Nucleus IPC bus on Greengrass) with no cross-device visibility; the bridge subscribes the
device's Unified-Namespace traffic, republishes it **topic-verbatim** onto the site broker under the
device's namespace, and relays commands back down. Any site-scoped consumer — a historian, an MES
bridge, the edge console — then connects to **one** bus instead of every device's. Built on the
`ggcommons` (`greengrass-commons`) Rust library, the bridge is itself a first-class ggcommons component:
it has its own identity, heartbeat, config announce, and metrics, all of which ride its own relay.

Unlike a dumb broker-to-broker bridge it understands the message envelope: it stamps a **hop tag** for
loop protection, **rewrites `reply_to`** so site→device request/reply survives the crossing, applies a
**per-class uplink policy** (enable/disable, token-bucket rate caps, and a bounded event replay buffer
for WAN blips), and registers a Last-Will `UNREACHABLE` on the site connection for fast whole-device
reachability detection.

| Doc | Start here when you want to… |
|-----|------------------------------|
| **[Tutorial](tutorial.md)** | learn by doing — bring a bridge up between two brokers and watch traffic cross, end to end |
| **[How-to guides](how-to-guides.md)** | accomplish a task — declare the site broker, tune per-class policy, proxy request/reply, add TLS, deploy |
| **[Reference](reference/)** | look up an exact option, topic, filter, envelope field, or metric |
| **[Explanation](explanation.md)** | understand how it works and why — the three connections, the relay matrix, loop protection, the two disconnect stories |

## Quick routing

- **"I'm new here."** → [Tutorial](tutorial.md).
- **"What config option does X?"** → [Reference — Configuration](reference/configuration.md).
- **"Which classes cross the bridge, and on which topics?"** → [Reference — Messaging Interface](reference/messaging-interface.md).
- **"What exactly is a hop tag / the `_relay` array?"** → [Reference — Data Types](reference/data-types.md).
- **"Why three connections? Why isn't `cmd` uplinked?"** → [Explanation](explanation.md).
- **"Show me a real, complete config."** → [Sample configurations](sample-configurations.md).

## The one-sentence model

The bridge holds **three broker connections** — two to the device bus (one for its own observability,
one raw one for the relay) and one to the site broker — and pumps messages across a fixed **relay
matrix**: six UNS classes go **up** (device → site) topic-verbatim, `cmd` comes **down** (site → device)
pinned to this bridge's own device, and request/reply is proxied through a TTL'd correlation map.
Everything else is policy on top of that: loop protection, rate limiting, disconnect buffering, and a
Last-Will for reachability.

## Audience

These docs are for **integrators and operators** — people who deploy the bridge, run the site broker it
pairs with, and write the site-side clients that consume device traffic or command devices across it.
They do not cover modifying the bridge's own source (for that, the module-level rustdoc in `src/*.rs` and
`docs/platform/DESIGN-uns-bridge.md` in the ggcommons monorepo are the reference).

## Platforms at a glance

- The binary runs on **HOST** as an MQTT↔MQTT pair (a device broker and a site broker) and on
  **KUBERNETES** as a boundary-bridge deployment of that same binary. Both are the same code.
- On **Greengrass**, the bridge runs in its HOST/MQTT shape against a device-local MQTT broker; a
  Nucleus-IPC-primary device bus (with the site half over MQTT) is not supported. See
  [Explanation → Platforms](explanation.md#platforms-where-a-bridge-runs).
