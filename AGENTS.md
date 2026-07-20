# uns-bridge — component notes

EdgeCommons **bridge** component (Rust). Full name `com.mbreissi.edgecommons.UnsBridge`, crate/binary
`uns-bridge`. Depends on the `edgecommons` Rust library. If this repo lives inside the EdgeCommons org
umbrella workspace, read its root `AGENTS.md` first (org repo map, design-fidelity contract,
validation matrix, platform/transport model); everything below is this component's own detail.

## What it is

**One `uns-bridge` per device bus**: an envelope-aware relay between the device-local bus and the
site UNS broker. It uplinks the six edgecommons UNS classes (`state`/`cfg`/`evt`/`metric`/`data`/
`log`, plus opt-in `app`) topic-verbatim, downlinks commands addressed to its own device, stamps a
hop tag for loop protection, and proxies site<->device request/reply across the bridge. It is a
**bridge**, not a southbound adapter — it has no device connection, no `sb/*` command family, no
`southbound_health` metric, and no console panel trio; it is judged against its own shape. See
`README.md` for the wire-level behavior and `DESIGN.md` for the local decision register (the
canonical behavioral spec lives in `DESIGN-uns.md` §9 / `DESIGN-uns-bridge.md` in the `edgecommons`
core monorepo).

## The two connections

The bridge holds two live connections: the **device bus**, which the `EdgeCommons` runtime owns for its
own observability (heartbeat, `cfg` announce, `gg.metrics()`) and which the relay SHARES at the raw
provider level via `gg.raw_device_provider()` — one connection, MQTT on HOST or Nucleus IPC on
GREENGRASS — and the **SITE** connection to the site broker (always MQTT; the bridge's external system,
declared in its own `component.instances[]` entry). See README "How it connects" for the shared-provider
architecture (the relay runs below the reserved-class guard so it can relay reserved classes verbatim).

## Config location

The site broker and relay knobs live in `component.instances[]` (`config.schema.json` is the
contract; the bridge declares no `component.global` knobs of its own). The sibling sections (`tags`,
`hierarchy`, `identity`, `messaging`, `metricEmission`, `logging`, `heartbeat`) are the standard
`edgecommons` envelope — `messaging` in particular is the bridge's own **device-local** bus, not the
site broker. `test-configs/config.json` carries a runnable dual-broker example.

## Validation expectations

- `cargo test` covers the pure `config`/`io`/`observability`/`policy`/`relay`/`reply` modules against
  an in-memory fake `MessagingProvider` — no broker required. 112 tests as of this baseline.
- `cargo llvm-cov --ignore-filename-regex 'main\.rs' --fail-under-lines 90` is the coverage gate
  (`.github/workflows/ci.yml`'s `coverage` job) — the org rule is 90% line coverage per language.
  Only `src/main.rs` (the live-MQTT-driver bootstrap/retry-loop seam, validated instead by the gated
  e2e) is excluded; see `DESIGN.md` D-UB-2. Do not lower the gate or exclude testable code to pass —
  add tests.
- `bash tests/e2e/run.sh` (`UNS_BRIDGE_E2E=1`, gated `#[ignore]`) is the live dual-EMQX relay proof
  against two real brokers. See `DESIGN.md` "Known validation gap" for its current status.
- `edgecommons component validate` checks this repo's config against `config.schema.json` and warns
  if `Cargo.lock` is not committed (it is — see `DESIGN.md` D-UB-1 for the regeneration discipline).

## Org conventions this component follows

- Builders/facades are the construction path — the relay itself works at the raw `MessagingProvider`
  level by design (below the reserved-class publish guard, §1.3), but every other surface (metrics,
  heartbeat, config) goes through the standard facades.
- Four-way parity does not apply here in the usual sense — `uns-bridge` is Rust-only; there is no
  Java/Python/TypeScript sibling bridge.
- Runtime artifacts (generated dev/test TLS certs, logs, build output, local broker state) stay out
  of Git — see `.gitignore`'s `/deploy/site-broker/tls-certs/` and `/deploy/site-broker/.env` rules.
