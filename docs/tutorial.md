# Tutorial — From zero to a live site bus

By the end you'll have a bridge relaying a device's Unified-Namespace traffic onto a **second** broker
that stands in for the site UNS bus, and you'll have watched telemetry go **up**, a command come **down**,
and a request/reply round-trip cross the bridge intact. No cloud, no hardware — two local MQTT brokers and
`cargo`.

The whole point of the bridge only becomes visible when there are **two** brokers: a *device* bus that a
component publishes to, and a *site* bus that a consumer subscribes to. So the first thing we do is stand
up both.

## 1. Prerequisites

- Rust (stable) and `cargo`.
- Docker, for two throwaway EMQX brokers.
- An MQTT CLI such as `mosquitto_sub` or MQTTX is useful for watching topics. Normal edgecommons payloads are
  protobuf bytes, so a plain MQTT CLI cannot handcraft or inspect them as JSON.
- This repo checked out, buildable against the sibling `edgecommons` library (the gitignored
  `.cargo/config.toml` `[patch]` override — see the repo `README.md`).

## 2. Start two brokers

The device bus on `:1883` and the site bus on `:1884` — the layout the bundled
[`test-configs/config.json`](../test-configs/config.json) expects:

```bash
docker run -d --name uns-device-broker -p 1883:1883 emqx/emqx
docker run -d --name uns-site-broker   -p 1884:1883 emqx/emqx
```

Think of `:1883` as "the broker on this one device" and `:1884` as "the plant-wide bus every device
bridges onto."

## 3. Run the bridge

```bash
cargo run -- --platform HOST --transport MQTT ./test-configs/config.json \
  -c FILE ./test-configs/config.json --thing gw-01
```

You should see it (in order): initialize the edgecommons runtime against the **device** bus, share the
runtime's device-bus provider for the **relay**, derive the private site Last-Will from the bridge's state
topic, connect to the **site** broker, subscribe its uplink filters, and log `relay running`. The device
identity is `gw-01` (from `--thing`); the bundled config places it at `dallas/gw-01` via `hierarchy`/`identity`.

Leave it running. It is now doing three things at once: mirroring its own health onto both buses, pumping
device traffic up to the site broker, and listening for commands to bring down.

## 4. Watch the bridge announce itself (uplink, the easy case)

The bridge is a edgecommons component, so it emits its own heartbeat `state` keepalive — which *matches its
own uplink filter* and therefore rides its own relay to the site bus. Subscribe the whole UNS `state`
class on the **site** broker (`:1884`):

```bash
mosquitto_sub -p 1884 -t 'ecv1/+/+/+/state' -v
```

Within ~5 s you'll see `ecv1/gw-01/uns-bridge/main/state` arrive **on the site broker** even though the
bridge published it on the *device* broker. The payload is a protobuf `EdgeCommonsMessage`, so the CLI may show
binary output rather than readable JSON. After decode, the diagnostic projection includes
`tags._relay: ["gw-01/uns-bridge"]` — the hop tag the bridge stamped as it forwarded. That tag is the bridge's
loop protection and its "which path did this take" breadcrumb.

## 5. Send telemetry up with a real EdgeCommons producer

Normal UNS messages are protobuf `EdgeCommonsMessage` bytes on the wire. Do **not** use
`mosquitto_pub -m '{"header":...}'` as a shortcut: that sends JSON text, not protobuf, and the bridge correctly
drops it as `MalformedEnvelope`.

Use any EdgeCommons component/client to publish a `data` message on the **device** bus (`:1883`) to a topic
such as `ecv1/gw-01/opcua-adapter/kep1/data/Temperature`. If the application payload is opaque bytes, put them
in the message's opaque body with a content type; the bridge will preserve those body bytes while it appends
the hop tag to envelope metadata.

Your `state` subscriber won't see it (wrong class), so open a second subscriber on the **site** broker for
the `data` class:

```bash
mosquitto_sub -p 1884 -t 'ecv1/+/+/+/data/#' -v
```

Publish the reading from the EdgeCommons producer. It appears on the site broker on the **identical topic** —
that's what "topic-verbatim" means — with the hop tag appended after protobuf decode/re-encode. Foreign
payloads that are not protobuf EdgeCommons messages do not relay on these normal UNS paths.

## 6. Bring a command down

Commands flow the other way — from the site bus down to the device — and only for **this** device.
Subscribe the device bus for commands, then send one with an EdgeCommons site-side client:

```bash
# terminal A — watch the device bus
mosquitto_sub -p 1883 -t 'ecv1/gw-01/+/+/cmd/#' -v
```

It arrives on the device bus, hop-tagged, as protobuf bytes. The **device pinning** rule is the same: a command
for a different device (`ecv1/gw-99/...`) never reaches `gw-01`'s device bus because the downlink filter is
pinned to `ecv1/gw-01/+/+/cmd/#`. A bridge only pulls down commands addressed to its own device (which is also
exactly what the site broker's per-device ACL allows it to read).

## 7. Prove request/reply survives the crossing

This is the subtle one. A site-side requester sets `header.reply_to` to a topic **on the site broker**; a
device-side responder would naively reply onto the *device* bus, where the requester isn't listening. The
bridge proxies the whole path. With an EdgeCommons client this is one `request()` call across the bridge. The
command and reply are both decoded as protobuf, mutated, and re-encoded by the bridge:

```bash
# site side: subscribe your own reply topic on the SITE bus if you want to watch the returned bytes
mosquitto_sub -p 1884 -t 'edgecommons/reply-demo' -v
```

Watch your device-bus `cmd` subscriber (terminal A from step 6): after protobuf decode, the relayed command's
`reply_to` is **not** `edgecommons/reply-demo` — the bridge rewrote it to a fresh `edgecommons/reply-...` topic
it minted and subscribed **on the device bus**. When the device responds on that bridge topic, your
`edgecommons/reply-demo` subscriber on the **site** bus receives the reply — `correlation_id` and body intact,
`reply_to` stripped, hop tag appended. That is the correlation map at work.

For a fully repeatable local proof of telemetry uplink, command downlink, request/reply, loop-drop, and opaque
body preservation, use the bundled e2e harness:

```bash
bash tests/e2e/run.sh
```

## 8. See the disconnect story

Stop the **site** broker (`docker stop uns-site-broker`) and publish a couple of protobuf `evt` messages and a
couple of protobuf `data` messages on the device bus. The bridge logs that the site link is down: the `data`
messages are
**dropped** (the live UNS path is deliberately not durable), but the `evt` messages are **buffered**
(events/alarms must survive a WAN blip). Start the site broker again (`docker start uns-site-broker`); on the
reconnect rising edge the bridge publishes its two rehydration broadcasts on the device bus and then
**replays the buffered `evt`, in order**, to the site broker. Watch your site-side `evt` subscriber
(`ecv1/+/+/+/evt/#`) to see them arrive after the reconnect.

## 9. Shut down cleanly

Ctrl-C the bridge. It aborts its pumps and **unsubscribes every filter on both brokers** before exiting
(the unsubscribe-before-exit rule), then logs a one-line tally: uplinked, downlinked, loop-dropped,
replies relayed/expired, per-reason drops, `evt` replayed. Tear the brokers down with
`docker rm -f uns-device-broker uns-site-broker`.

Next: the [how-to guides](how-to-guides.md) for the real tasks (site TLS, per-class policy, the reply
paired-knob, multi-bridge topologies, deployment); the [reference](reference/) for every option, topic, and
metric; the [explanation](explanation.md) for the model behind all of it.
