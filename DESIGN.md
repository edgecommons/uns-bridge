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

- **GREENGRASS/IPC-primary variant** (PRIMARY = Nucleus IPC, SITE = MQTT): the `greengrass` cargo
  feature today only compiles the library's IPC provider; the IPC-primary relay wiring is the
  follow-up, and GREENGRASS deployment validation rides it (HOST is proven by the e2e and
  KUBERNETES by the boundary-bridge deploy of `deploy/site-broker/k8s/`).
- Template substitution across the whole `component.instances[]` entry (the facade integration
  `src/config.rs` module docs mention).
- A Rust-only library affordance exposing the runtime's raw `MessagingProvider` so the relay can
  share the runtime's device-bus connection instead of holding a second client (see README "How it
  connects" for why the relay cannot reuse the runtime's connection today without a — deliberately
  unmade — edgecommons core change).
- Docs-site sync of this component's docs into the edgecommons website (this change adds
  `.github/workflows/deploy-docs.yml`, which triggers the sync on doc-only pushes once the repo's
  `CLOUDFLARE_DEPLOY_HOOK` secret is set — the trigger mechanism, not the first sync itself).

## Known validation gap

- **`tests/e2e_dual_broker.rs` fails against the currently pinned core rev
  (`36a70c48b65b35f77bfab70d3a73869debdfc407`), independent of this change.** The pinned rev's D-U28
  "optional-instance" topic behavior makes `EdgeCommons::uns().topic(UnsClass::State)` (the
  component-scope builder the bridge's own runtime/heartbeat uses) omit the instance segment —
  `ecv1/{device}/uns-bridge/state`, not `ecv1/{device}/uns-bridge/main/state`. The e2e test's
  readiness gate still hardcodes the pre-D-U28 `.../main/state` topic string, so it never observes
  the bridge's own relayed heartbeat and times out after 60 s waiting for it. This predates this PR
  (verified against a clean worktree with no source changes beyond the lockfile) and is unrelated to
  the baseline-adoption items (WS-1/2/3/4/5/7) implemented here. It blocks running the gated e2e as
  live validation for this change; a corrected topic expectation (and, depending on intent, whether
  the D-U28 dual-scope subscriptions `src/io.rs` already carries should also update the bridge's own
  emitted own-identity topics to the instance-scoped form) is tracked as a follow-up, not fixed in
  this PR.
- **`recipe.yaml`'s GREENGRASS `Run.Script` invokes the binary with `--config <path> --thing
  <name>`, which does not match the standard edgecommons CLI grammar the docs describe.** `-c/
  --config` is one flag (both spellings resolve to the same clap arg) and requires its first value
  to be a source keyword — `FILE`, `CONFIGMAP`, `ENV`, `GG_CONFIG`, `SHADOW`, or `CONFIG_COMPONENT`
  — followed by that source's own args, e.g. `--config FILE /path/to/config.json`; a bare path
  after `--config` fails to parse as an unknown config source. `src/main.rs` forwards
  `std::env::args_os()` to `EdgeCommonsBuilder` verbatim — it does not pre-process or synthesize
  argv — so nothing in this binary rewrites a minimal `--config <path> --thing <name>` invocation
  into the full form. `docs/reference/configuration.md`, `docs/explanation.md`, and
  `docs/reference/messaging-interface.md` all describe a minimal-CLI-with-internal-synthesis
  design that is not implemented in `src/main.rs` as shipped. Since GREENGRASS deployment of this
  bridge is itself a still-deferred variant (see above) this mismatch has apparently never been
  exercised live. Resolving it (implement the described synthesis in `main.rs`, or correct the docs
  and `recipe.yaml` to the full standard-args form) is a design decision outside this baseline-
  adoption change's scope (WS-1/2/3/4/5/7) — flagged here rather than guessed at.

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
