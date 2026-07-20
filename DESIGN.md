# DESIGN — uns-bridge

> Treat this document as the **design-fidelity contract** for this component: before changing
> behavior, update the relevant section here in the same change, and review new work against what
> is written here — not against a summary of it.

## What it is

`uns-bridge` (Greengrass component `com.mbreissi.edgecommons.UnsBridge`) is an EdgeCommons
**bridge** category component: an envelope-aware relay between a device-local bus and the site UNS
broker, making the logical Unified Namespace a real site-wide bus. It is not a southbound adapter —
it has no device connection, no `sb/*` command family, no `southbound_health` metric, and no console
panels — it is judged against its own shape: uplink of the six UNS classes, hop-tag loop protection,
command downlink pinned to its own device, and a request/reply proxy. The full behavioral design —
the relay matrix, hop-tag algorithm, `reply_to` proxy, per-class uplink policy, and the site
Last-Will contract — is specified in `DESIGN-uns.md` §9 / `DESIGN-uns-bridge.md` in the
`edgecommons` core monorepo; the section references (§1.1, §2.2, §2.3, …) throughout this repo's
`README.md` and `src/` module docs point there. This file is the *local* decision register — the
baseline-adoption and packaging decisions specific to this repo — not a restatement of that spec.

## Decisions

- **D-UB-1 (lockfile-commit policy, org SD-B).** `Cargo.lock` is committed. It is regenerated with
  the local `.cargo/config.toml` `[patch]` override **inactive** so it records the pinned git `rev`
  in `Cargo.toml`, not a path source into the sibling `core/libs/rust` checkout — the recorded
  resolution is valid on a fresh clone and in CI. A build or test run made with the `[patch]`
  override active rewrites the in-memory resolution (and can add a `[[patch.unused]]` marker to
  `Cargo.lock`) without changing the committed pin; that local churn is never committed — regenerate
  from a clean checkout (no `.cargo/config.toml` in scope) before committing the lock.
- **D-UB-2 (coverage exclusion, org WS-4).** The 90%-line coverage gate
  (`.github/workflows/ci.yml`'s `coverage` job) excludes exactly one file from the denominator:
  `src/main.rs`. It is the thin live-MQTT-driver wiring seam — runtime bootstrap, the real
  `MqttProvider::connect`/`connect_with_last_will` calls, and the site retry loop — with no unit
  tests of its own, validated instead by the gated dual-EMQX e2e (`tests/e2e_dual_broker.rs`,
  `UNS_BRIDGE_E2E=1`). Every other module (`config.rs`, `io.rs`, `observability.rs`, `policy.rs`,
  `relay.rs`, `reply.rs`) is pure logic against the in-memory fake `MessagingProvider` and stays in
  the denominator (each individually 92-100% line coverage as of this change; aggregate ~95%). Do
  not widen this exclusion or lower the threshold to pass the gate — add tests instead.
- **D-UB-3 (license reconciliation, issue #2).** `Cargo.toml`'s `license` field is `BUSL-1.1`,
  matching the shipped `LICENSE` file (Business Source License 1.1) — it previously read
  `Apache-2.0`, a stale leftover from before the org's BUSL-1.1 policy.
- **D-UB-4 (`config.schema.json` scope).** The schema models only what `src/config.rs` actually
  parses: `component.instances[]` (the bridge declares no `component.global` knobs). The site entry
  requires `id` + `siteBroker`; `uplink`/`reply`/`maxHops`/`queue` are optional, matching the code's
  defaults. The schema explicitly rejects a configured `siteBroker.lwt` (`"not": {}`), mirroring
  `BridgeConfig::reject_configured_lwt` in `src/config.rs` — the site Last-Will is derived, never
  configured. `component.token` (a canonical-schema-owned key, sibling to `global`/`instances`) is
  intentionally not modeled here.
- **D-UB-5 (docs relocation).** The README's former "Roadmap (the Phase-3 slices)", "Release state &
  remaining follow-ups", and "Still deferred" sections moved here (below) per the org's public-docs
  rule: user-facing docs describe current behavior only, in present tense; status/history/roadmap
  belongs in internal docs.
- **D-UB-6 (GREENGRASS/IPC-primary relay — the shared-provider architecture).** The relay's device-bus
  PRIMARY is the `EdgeCommons` runtime's OWN raw provider, obtained via the core affordance
  `gg.raw_device_provider()` (`EdgeCommons::raw_device_provider` → `DefaultMessagingService::provider`,
  core PR #58, pinned rev `6d836fe917cf21c1930daaaab087c06f2a71adfb`). Consequences, all binding:
  - **One device-bus connection, shared below the guard.** The relay no longer opens a second
    device-bus client (the former `-relay`-suffixed `MqttProvider::connect`). It shares the runtime's
    single connection at the raw-provider level — below the reserved-class publish guard (§1.3), which
    is what lets it forward reserved classes (`state`/`metric`/`cfg`/`log`) verbatim — sparing a client
    under the Greengrass shared-connection quota. The bridge now holds **two** connections total
    (device bus + site), not three.
  - **HOST and GREENGRASS unified.** The PRIMARY is whatever transport the runtime resolved: **MQTT on
    HOST** (`standalone`), **Nucleus IPC on GREENGRASS** (`greengrass`). A single code path serves both;
    the `#[cfg(not(feature="standalone"))] compile_error!` is removed. `raw_device_provider()` returning
    `None` (no transport wired) is a fatal startup error.
  - **The SITE half is always MQTT.** It stays `MqttProvider::connect_with_last_will`, independent of the
    device-bus transport. The edgecommons `mqtt` provider module is gated behind `edgecommons/standalone`,
    so the `greengrass` feature **builds on** `standalone` (`greengrass = ["standalone",
    "edgecommons/greengrass"]`) — both the IPC provider (device bus) and the MQTT provider (site) are
    compiled in.
  - **The vestigial `messaging` field is removed from `BridgeConfig`.** It was only ever consumed by the
    deleted `relay_primary_messaging()`; the runtime reads its own transport config from `--transport
    MQTT <path>` (HOST) or needs none (IPC). Dropping it lets `GG_CONFIG` (which carries no `messaging`
    section) parse. A `messaging` section in a HOST config is tolerated and ignored.
  - **The `recipe.yaml` arg-grammar mismatch is resolved to the standard CLI** (closing the former
    "Known validation gap"): the GREENGRASS `Run.Script` now invokes `--platform GREENGRASS --transport
    IPC -c GG_CONFIG -t {iot:thingName}` — the real edgecommons CLI contract (`-c/--config` takes a
    SOURCE keyword first). `src/main.rs` forwards argv verbatim (no synthesis), so the docs' former
    "minimal `--config <file>` with internal synthesis" claim — which never matched `main.rs` and would
    fail to parse — is corrected across the recipe and `docs/` to the standard-args form the e2e harness
    already uses. IPC pubsub `accessControl` grants the bridge SUBSCRIBE + PUBLISH across the local UNS
    surface (uplink class wildcards, own-device cmd downlink, the reply-proxy `edgecommons/reply-*`
    topics, and the `_bcast` rehydration), modeled on the reference adapter recipes (`resources: ["*"]`);
    no mqttproxy/TES, since the site link is a plain external MQTT socket, not Greengrass-brokered IoT
    Core.
- **D-UB-7 (GREENGRASS IPC stream ceiling — raised to 64 in the committed build).** The
  `aws-greengrass-component-sdk` C-FFI compiles `GG_IPC_MAX_STREAMS` with a default of **16** (an
  `#ifndef` guard). The relay opens **more** concurrent IPC subscription streams than that (~18: the
  six/seven uplink class wildcards, both D-U28 subscription scopes, the own-device `cmd` downlink scopes,
  and the per-request reply-proxy reply topics), so on a Nucleus the extra streams are rejected (NOMEM)
  and the component crash-loops on start. The **committed greengrass build raises the ceiling to 64**:
  `build.sh` (the gdk `custom_build_command` that produces the shipped artifact) exports
  `CFLAGS`/`TARGET_CFLAGS` `-DGG_IPC_MAX_STREAMS=64` before `cargo build --features greengrass` whenever
  the feature set includes `greengrass`, so the SDK's `cc` build picks it up automatically and the shipped
  artifact is deployable with **no deploy-time env**. 64 is comfortable headroom over the ~18 the relay
  uses; the shared-stream-budget tradeoff is accepted (maintainer decision). Validated on `lab-5950x`: the
  as-shipped default-16 build crash-looped (NOMEM → exit 1); with the ceiling at 64 the Scenario-B
  bridge-over-IPC round-trip completed. **Any local greengrass dev build must go through `build.sh`** (or
  export the same define) to get the raise — a plain `cargo build --features greengrass` does not set it.

## Phase history (moved from README)

The Phase-3 slices that built this component, for provenance (`git log --oneline` has the per-phase
commits):

| Slice | Contents |
|---|---|
| P3-2 | repo scaffold; relay engine (six uplink filters + pinned downlink, topic-verbatim, hop tag/maxHops); unit tests over trait fakes |
| P3-3 | `reply_to` rewrite: TTL'd correlation map, maxPending eviction, reply back-haul |
| P3-4 | per-class uplink policy: enables, token-bucket rate caps, D-B10 disconnect behavior + the bounded drop-oldest `evt` replay buffer with in-order reconnect replay; per-class drop counters |
| P3-4b | the bridge's own EdgeCommons observability (§2.8): heartbeat `state` keepalive + `cfg` announce + counters published as `metric`s (30 s, riding the bridge's own relay); private derived D-B11 site LWT; the bridge-side reconnect `republish-*` `_bcast` rehydration |
| P3-5 | `deploy/site-broker/` recipes (HOST compose + dual-EMQX dev rig, GG DockerApplicationManager, k8s in-cluster broker + boundary-bridge example, the per-device ACL file, TLS notes) |
| P3-6 | the repeatable dual-EMQX bridge-level e2e (`tests/e2e/run.sh`) + the `edgecommons/registry` catalog entry (`category: bridge`) |

All P3-2 through P3-6 slices are implemented and shipped on `main`.

Shipped at the v0.2.0 UNS release: the GitHub remote + git-rev pin bump (`edgecommons/uns-bridge` is
published; `Cargo.toml` pins the UNS-core `edgecommons` rev); the 4-language
`republish-state`/`republish-cfg` broadcast listener (`RepublishListener` in Java/Python/TypeScript,
`uns.rs` in Rust — the bridge's reconnect rehydration broadcast is answered by every rev-bumped
component, no longer inert); and the edge-console as the first site-side client (the full-system
test — console ↔ site broker ↔ bridge ↔ device components — has been run and passed, HOST → kind;
the P3-6 e2e is the bridge-level proof specifically).

## Still deferred (genuinely unbuilt)

- Template substitution across the whole `component.instances[]` entry (the facade integration
  `src/config.rs` module docs mention).
- Docs-site sync of this component's docs into the edgecommons website (this change adds
  `.github/workflows/deploy-docs.yml`, which triggers the sync on doc-only pushes once the repo's
  `CLOUDFLARE_DEPLOY_HOOK` secret is set — the trigger mechanism, not the first sync itself).

## Validation run for the IPC-primary change

- **HOST/standalone**: `cargo build`, `cargo test` (112 pass), `cargo clippy --all-targets` (clean),
  and the coverage gate `cargo llvm-cov --ignore-filename-regex 'main\.rs' --fail-under-lines 90`
  (95.5% lines) all green on Windows against the affordance worktree via the `.cargo` `[patch]`.
- **GREENGRASS/IPC**: the greengrass build **links on WSL** (Linux ELF binary produced) — the proof
  that the IPC-shared-primary path compiles, including the Linux-only `aws-greengrass-component-sdk`
  C-FFI. Building through the committed `build.sh` (`EDGECOMMONS_FEATURES=greengrass`) with no manual
  env confirmed the D-UB-7 `GG_IPC_MAX_STREAMS=64` raise is applied automatically (the define appears in
  the SDK's `cc` build). On-device: the default-16 build crash-looped and the ceiling-64 build completed
  the Scenario-B bridge-over-IPC round-trip on `lab-5950x`.
- **Lockfile**: `Cargo.lock` records `edgecommons` git-sourced at rev `6d836fe…` (not a path source),
  re-resolved with the `[patch]` inactive per D-UB-1.

## Known validation gap

- **`tests/e2e_dual_broker.rs` (the gated live dual-EMQX HOST proof) was NOT run for this change**,
  and it has a pre-existing readiness-gate defect independent of it. The test's readiness gate
  hardcodes the pre-D-U28 `ecv1/{device}/uns-bridge/main/state` topic, but the D-U28 "optional-
  instance" topic behavior makes `EdgeCommons::uns().topic(UnsClass::State)` (the component-scope
  builder the bridge's own runtime/heartbeat uses) omit the instance segment
  (`ecv1/{device}/uns-bridge/state`), so the gate never observes the bridge's own relayed heartbeat
  and times out. This predates the IPC-primary work and is unrelated to the shared-provider wiring
  (which the unit suite exercises against the in-memory fake). A corrected topic expectation is a
  follow-up. The GREENGRASS/IPC on-device regression is likewise the orchestrator's lab step, not run
  here.

## Config

`config.schema.json` is the source of truth for `component.instances[]`'s shape; `docs/reference/
configuration.md` narrates *why* each key exists. Update both together when the shape changes.

## Validation

- `cargo test` — 112 unit tests over the pure `config`/`io`/`observability`/`policy`/`relay`/`reply`
  modules against in-memory fakes; no broker required.
- `cargo llvm-cov --ignore-filename-regex 'main\.rs' --fail-under-lines 90` — the coverage gate
  (D-UB-2); ~95% as of this change.
- `cargo clippy --all-targets` — clean.
- `bash tests/e2e/run.sh` (`UNS_BRIDGE_E2E=1`, gated, `#[ignore]`d) — the live dual-EMQX relay proof;
  see "Known validation gap" above for its current status against the pinned core rev.
- Fresh-clone build proof: a clone with no `.cargo/config.toml` in any ancestor directory resolves
  `edgecommons` from the pinned git `rev` via the committed `Cargo.lock` and builds/tests/lints
  clean — this is what D-UB-1 makes possible.
