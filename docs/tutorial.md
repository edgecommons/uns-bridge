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
- An MQTT CLI for poking the brokers by hand — `mosquitto_pub`/`mosquitto_sub`, or MQTTX.
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
cargo run -- --config ./test-configs/config.json --thing gw-01
```

You should see it (in order): initialize the edgecommons runtime against the **device** bus, establish the
**relay's** own device-bus connection, run the **LWT cross-check**, connect to the **site** broker, subscribe
its uplink filters, and log `relay running`. The device identity is `gw-01` (from `--thing`); the bundled
config places it at `dallas/gw-01` via `hierarchy`/`identity`.

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
bridge published it on the *device* broker. Look at the payload: it carries `tags._relay: ["gw-01/uns-bridge"]`
— the hop tag the bridge stamped as it forwarded. That single tag is the bridge's loop protection and its
"which path did this take" breadcrumb.

## 5. Send telemetry up

Publish a fake sensor reading on the **device** bus (`:1883`), on the UNS `data` class, exactly as a
component would:

```bash
mosquitto_pub -p 1883 -t 'ecv1/gw-01/opcua-adapter/kep1/data/Temperature' \
  -m '{"header":{"name":"data","version":"1.0"},"body":{"value":21.4}}'
```

Your `state` subscriber won't see it (wrong class), so open a second subscriber on the **site** broker for
the `data` class:

```bash
mosquitto_sub -p 1884 -t 'ecv1/+/+/+/data/#' -v
```

Re-publish the reading. It appears on the site broker on the **identical topic** — that's what
"topic-verbatim" means — with the hop tag appended. Publish a **raw** (non-envelope) payload too
(`-m '{"value":21.4}'`): it relays **byte-for-byte**, no tag added, because a raw message has no envelope
to stamp.

## 6. Bring a command down

Commands flow the other way — from the site bus down to the device — and only for **this** device.
Subscribe the device bus for commands, then publish one on the **site** bus:

```bash
# terminal A — watch the device bus
mosquitto_sub -p 1883 -t 'ecv1/gw-01/+/+/cmd/#' -v
# terminal B — issue a command from the site side
mosquitto_pub -p 1884 -t 'ecv1/gw-01/opcua-adapter/main/cmd/reload-config' \
  -m '{"header":{"name":"reload-config","version":"1.0"},"body":{}}'
```

It arrives on the device bus, hop-tagged. Now prove the **device pinning**: publish the same command for a
*different* device (`ecv1/gw-99/...`) on the site bus — it never reaches `gw-01`'s device bus, because the
downlink filter is pinned to `ecv1/gw-01/+/+/cmd/#`. A bridge only pulls down commands addressed to its own
device (which is also exactly what the site broker's per-device ACL allows it to read).

## 7. Prove request/reply survives the crossing

This is the subtle one. A site-side requester sets `header.reply_to` to a topic **on the site broker**; a
device-side responder would naively reply onto the *device* bus, where the requester isn't listening. The
bridge proxies the whole path. With a edgecommons client this is one `request()` call across the bridge; by
hand:

```bash
# 1. site side: subscribe your own reply topic on the SITE bus
mosquitto_sub -p 1884 -t 'edgecommons/reply-demo' -v
# 2. site side: send a request naming that reply topic
mosquitto_pub -p 1884 -t 'ecv1/gw-01/opcua-adapter/main/cmd/ping' \
  -m '{"header":{"name":"ping","version":"1.0","reply_to":"edgecommons/reply-demo","correlation_id":"c1"},"body":{}}'
```

Watch your device-bus `cmd` subscriber (terminal A from step 6): the relayed command's `reply_to` is **not**
`edgecommons/reply-demo` — the bridge rewrote it to a fresh `edgecommons/reply-...` topic it minted and subscribed
**on the device bus**. Now play the responder: publish a reply on *that* bridge-minted topic, on the device
bus, with the same `correlation_id`:

```bash
mosquitto_pub -p 1883 -t '<the-bridge-minted-reply-topic>' \
  -m '{"header":{"name":"ping-reply","version":"1.0","correlation_id":"c1"},"body":{"ok":true}}'
```

Your `edgecommons/reply-demo` subscriber on the **site** bus receives it — `correlation_id` and body intact,
`reply_to` stripped, hop tag appended. The bridge carried the reply back to the original site topic. That is
the correlation map at work.

## 8. See the disconnect story

Stop the **site** broker (`docker stop uns-site-broker`) and publish a couple of `evt` envelopes and a couple
of `data` envelopes on the device bus. The bridge logs that the site link is down: the `data` messages are
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
