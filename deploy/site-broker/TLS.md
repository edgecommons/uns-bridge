# TLS / mTLS wiring for the site broker

The site broker is the one place in this whole system where the security boundary lives (§4.4/§5.4
of `docs/platform/DESIGN-uns-bridge.md`, mirrored in `acl.conf` here): the ACL confines each device
bridge to `ecv1/<device>/#`, but it can only do that if EMQX knows *which device* is on the other
end of a connection. That identity comes from the client's **mTLS certificate Common Name (CN)**,
mapped to the MQTT username by `mqtt.peer_cert_as_username = cn` (set in `docker-compose.yml`'s
`site` service). No CN, no username, no `${username}`-scoped ACL match — see `acl.conf`'s header
comment for exactly what that means for unauthenticated connections.

## The three things the bridge's config needs

The bridge's `component.instances[].siteBroker` block (§2.7 of the design doc;
`test-configs/config.json` for the shipped shape) carries the client side of this:

```jsonc
"siteBroker": {
  "host": "site-broker.dallas.example",
  "port": 8883,
  "clientId": "uns-bridge-gw-01",
  "credentials": {
    "certPath": "/path/to/client-gw-01.crt",   // this device's client certificate
    "keyPath":  "/path/to/client-gw-01.key",   // its private key
    "caPath":   "/path/to/ca.crt"              // the CA that signed the BROKER's server cert
  }
}
```

These three fields map directly onto the canonical schema's `mqttCredentials` definition
(`schema/edgecommons-config-schema.json`, shared with `messaging.local`/`messaging.northbound` —
there is no bridge-specific credentials shape). **The certificate's CN is the device token** — it
must equal what the ACL and the bridge's own topics use (e.g. a bridge that publishes on
`ecv1/gw-01/...` needs a client cert with `CN=gw-01`, not `CN=uns-bridge-gw-01` or the client id).

## Dev: self-signed, generated locally

`gen-tls-certs.sh` (this directory) generates a throwaway CA plus:

| File | CN | Use |
|---|---|---|
| `server.{crt,key}` | `localhost` | the broker's own TLS identity (SAN covers `localhost`/`127.0.0.1`) |
| `client-gw-01.{crt,key}` | `gw-01` | a worked-example device bridge — matches `acl.conf`'s worked example |
| `client-consumer-console.{crt,key}` | `consumer-console` | a worked-example site consumer (historian/console) |
| `ca.crt` | `uns-bridge-site-test-ca` | trust root for both the broker's server cert and every client cert above |

Run it once (`bash gen-tls-certs.sh`), then `docker compose up -d` — the compose file mounts the
whole `tls-certs/` directory into the broker at `/opt/emqx/etc/certs`. `tls-certs/` is gitignored
(never commit generated key material, even dev-only).

**Exercise the mTLS + ACL path** with any MQTT client that supports client certs (e.g.
`mosquitto_pub`/`mosquitto_sub`, MQTTX, or the bridge itself). The walkthrough below is the exact
sequence used to verify `acl.conf` against a real EMQX 5.8.2 while building this recipe (`-h
localhost` needs no `--insecure`: the dev server cert's SAN covers `localhost`/`127.0.0.1`):

```bash
# Terminal 1 — watch the WHOLE site tree as a site consumer (console/historian role):
mosquitto_sub -h localhost -p 8884 \
  --cafile tls-certs/ca.crt --cert tls-certs/client-consumer-console.crt --key tls-certs/client-consumer-console.key \
  -t 'ecv1/#' -v

# Terminal 2 — gw-01 publishing into its OWN subtree: arrives in Terminal 1.
mosquitto_pub -h localhost -p 8884 \
  --cafile tls-certs/ca.crt --cert tls-certs/client-gw-01.crt --key tls-certs/client-gw-01.key \
  -t 'ecv1/gw-01/uns-bridge/main/state' -m '{"status":"test"}'

# gw-01 attempting a CROSS-DEVICE publish — the boundary. mosquitto_pub reports success (EMQX's
# default deny_action=ignore returns a normal PUBACK even when it silently drops the message —
# verified; see acl.conf's header comment), but it NEVER shows up in Terminal 1. That silent
# absence, not a client-side error, is what "the ACL holds" looks like.
mosquitto_pub -h localhost -p 8884 \
  --cafile tls-certs/ca.crt --cert tls-certs/client-gw-01.crt --key tls-certs/client-gw-01.key \
  -t 'ecv1/gw-02/uns-bridge/main/state' -m '{"status":"should never arrive"}'
```

Two more worth knowing before you build against this ACL:

```bash
# gw-01 subscribing to its own DOWNLINK cmd topic — allowed (this is the one topic shape a device
# bridge itself subscribes to on the site side, per the relay matrix, ../../README.md §2.2):
mosquitto_sub -h localhost -p 8884 \
  --cafile tls-certs/ca.crt --cert tls-certs/client-gw-01.crt --key tls-certs/client-gw-01.key \
  -t 'ecv1/gw-01/+/+/cmd/#' -v

# gw-01 subscribing to its own FULL subtree (not just cmd) — DENIED, and unlike a denied publish
# this IS visible: mosquitto_sub -d shows "All subscription requests were denied" / SUBACK 0x80. A
# device bridge may PUBLISH its own subtree (the uplink) but does not read it back over the site
# connection — only the pinned cmd downlink is a legitimate subscribe for a device identity.
mosquitto_sub -h localhost -p 8884 -d \
  --cafile tls-certs/ca.crt --cert tls-certs/client-gw-01.crt --key tls-certs/client-gw-01.key \
  -t 'ecv1/gw-01/#' -v
```

To run the **bridge itself** against the mTLS listener instead of the plaintext dev port, copy
`test-configs/config.json` and point `siteBroker` at `localhost:8884` with the
`deploy/site-broker/tls-certs/client-gw-01.{crt,key}` + `ca.crt` paths (and set the device token to
`gw-01` to match, e.g. `cargo run -- --platform HOST --transport MQTT <copy>.json -c FILE <copy>.json --thing gw-01`).

## Prod: a real CA

Self-signed dev certs are fine on a laptop; a real site deployment should NOT reuse them. Two
workable models, in increasing order of operational weight:

1. **A small site-private CA** (e.g. `step-ca`, or the same `openssl` commands as
   `gen-tls-certs.sh` run somewhere durable, not regenerated per dev-machine) issuing one client
   cert per device (CN = the device token) and one server cert for the broker. This is the natural
   next step up from dev — same shape, just a CA you don't throw away.
2. **AWS IoT Core-style per-device certs / your existing PKI**, if the site already has one (e.g.
   the fleet already has device certs from Greengrass provisioning) — reuse them for the site-broker
   connection too rather than minting a second cert per device, as long as the CN (or a
   consistently-derived value) still matches the device token the ACL expects. If the CN can't be
   made to match cleanly, fall back to `{allow, {username, "<literal>"}, ...}` per-device ACL blocks
   (see `acl.conf`'s commented alternative) instead of the `${username}` fleet template.

Either way, the **operational discipline that matters** is: every device gets its *own* client
cert/key pair (never a shared fleet-wide client cert — that collapses the whole per-device boundary
back to "any device can impersonate any other"), and cert issuance is the actual device-onboarding
step (provision the cert, and the ACL boundary for that device exists automatically via the fleet
template — no per-device ACL edit needed).

## Rotation / revocation (not built here)

This slice ships the wiring, not a rotation/revocation pipeline. Options for later: short-lived
certs re-issued by a step-ca-style CA (sidesteps revocation entirely), or EMQX's CRL/OCSP support
on the mTLS listener if a longer-lived-cert model is preferred. Track this alongside the design
doc's D-B6 "creation checklist" as an org follow-up, not a P3-5 deliverable.
