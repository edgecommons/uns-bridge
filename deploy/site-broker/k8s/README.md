# KUBERNETES site-broker recipe (§4.3)

**No bridge runs inside a cluster, by default.** Every in-cluster ggcommons component already
shares one broker (`messaging.local` pointing at its Service DNS) — aggregation across components
is native to the cluster, so there is nothing for a `uns-bridge` to relay (DESIGN-uns-bridge.md
§4.3 / DESIGN-uns.md §9.2). `emqx.yaml` is that broker: apply it, point every in-cluster
component's `messaging.local.host` at `uns-bridge-site-broker.<namespace>.svc.cluster.local:1883`,
done.

## `emqx.yaml` — the in-cluster broker

A Deployment + Service, structurally the same shape as `ggcommons/test-infra/k8s/emqx.yaml` (the
existing kind/k3s smoke precedent) — plaintext-only, anonymous, **no ACL**. That last part is a
deliberate departure from the HOST/GREENGRASS recipes, and worth explaining rather than papering
over: every in-cluster component connects anonymously (no client cert), same as the existing
`ggcommons-emqx` precedent — the cluster's own NetworkPolicy/RBAC is the trust boundary for
in-cluster traffic, not per-pod MQTT identity. `acl.conf`'s rules are keyed on `${username}`
(populated from a client cert's CN); EMQX's open-source edition evaluates **one global
authorization chain across every listener** (no per-listener ACL scoping), so mounting `acl.conf`
here would deny every anonymous in-cluster client too — verified while building this recipe (it's
exactly what happens on `docker-compose.yml`'s plaintext dev port, which is intentional there and
would NOT be intentional here). Don't "fix" this by mounting the ACL onto this Deployment.

The commented block at the bottom of `emqx.yaml` covers the case where this cluster ALSO needs to
aggregate external gateways bridging in (§4.3's cross-cluster case): a **second, dedicated** broker
Deployment with the mTLS listener + `acl.conf` + an mTLS-exposed Service — kept separate from the
Deployment above specifically because of the single-global-ACL-chain constraint just described.
In-cluster components keep talking to the plain `uns-bridge-site-broker`; only external gateways
(and, if deployed, this cluster's own `boundary-bridge.example.yaml`) talk to the second instance.

## `boundary-bridge.example.yaml` — the one case a bridge DOES run in a cluster

When the cluster itself is one line of a *bigger* site (§4.3), a single `uns-bridge` pod bridges
this cluster's own broker (PRIMARY) out to a higher-tier external broker (SITE) — same relationship
a HOST or GREENGRASS gateway's bridge has to *its* site broker, just running as a pod instead of a
process or a GG component. The manifest is marked **EXAMPLE ONLY** in its header because:

- This repo does not yet ship a Dockerfile/image for the bridge (`image: REPLACE_ME`) — building
  and publishing one is a follow-up (naturally P3-6 org-integration territory, alongside the
  registry entry), not part of the site-broker recipes this slice delivers.
- The bridge's CLI today is the minimal `--config <path> [--thing <name>]` form, not yet the
  standard `-c/--platform/--transport` contract every other ggcommons component has (see
  `../../README.md` "Also follow-ups"). A mounted ConfigMap file path works fine as a `--config`
  argument, but the CONFIGMAP *source's* hot-reload-on-`..data`-swap behavior other components get
  is not there yet — a known, documented gap, not something this slice fixes.

**D-B15's duplication guard is what actually matters here and IS fully specified**:
`replicas: 1` + `strategy: Recreate`. Two boundary bridges relaying the same broker pair
double-deliver every message (the hop-tag guard, §2.3, stops *loops*, not *duplication* — a second
bridge is not "the same bridge relaying twice", it is a second, independent relay of everything).
`Recreate` additionally avoids the rolling-update window where the old and new pod would briefly
both be running.

## What's a live-deploy step vs. validated here

Validated (no live cluster): both manifests are well-formed YAML (`kubectl apply --dry-run=client`
needs a live API server for full schema validation, which wasn't run — but the shapes mirror the
already-validated `ggcommons/test-infra/k8s/emqx.yaml` and
`ggcommons/templates/rust/k8s/deployment.yaml` precedents closely enough that structural mistakes
would be surprising).

Left as live-deploy steps: actually applying `emqx.yaml` to kind/k3s and confirming a component
reaches it (the `ggcommons/test-infra/k8s/smoke.sh` pattern would be the natural harness to extend);
building a uns-bridge container image before `boundary-bridge.example.yaml` can run at all; and, if
the mTLS-exposure block is used, provisioning the actual server cert/key Secret (`../TLS.md` "Prod").
