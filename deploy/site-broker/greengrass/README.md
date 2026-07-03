# GREENGRASS site-broker recipe (§4.2)

Deploys the site broker as a Greengrass-managed Docker container on the gateway core, via the
stock `aws.greengrass.DockerApplicationManager` component — **not** a bespoke Java/Python/Rust/TS
component. `recipe.yaml` here is a **packaging stub** (P3-5); the bucket/version placeholders and
the actual zip-build step are release-time items (see the root README's "Remaining release-time
items") alongside CI/image publishing — the `registry/components.json` entry itself landed in
P3-6.

## Why DockerApplicationManager, not a custom component

EMQX is a third-party binary with its own container image and its own lifecycle (compose up/down);
wrapping it in a GG component whose only job is "run this container" is exactly what
`aws.greengrass.DockerApplicationManager` is for (AWS-maintained, no code of ours to build/publish
for the broker itself). The bridge (`../../recipe.yaml`) is a *real* ggcommons component because it
has actual business logic (the relay); the broker doesn't.

## Artifact / image

Two artifacts, per `recipe.yaml`:

1. **`docker:emqx/emqx:5.8.2`** — the pinned EMQX image, pulled straight from Docker Hub by
   DockerApplicationManager (public image; no ECR mirror or registry credentials needed). Bump the
   tag as a deliberate, reviewed change — never track `latest` (a broker version bump is exactly
   the kind of change you want in a diff, not a surprise on next deployment).
2. **`s3://BUCKET_NAME/COMPONENT_NAME/COMPONENT_VERSION/site-broker-config.zip`** — the
   configuration bundle, built and staged the same way the bridge's own `build.sh`/`gdk-config.json`
   stage its artifact (GDK `custom_build_command`). It must contain:
   - `docker-compose.yml` (a copy of `../docker-compose.yml` — same file, same file name, so the
     HOST and GREENGRASS recipes never drift apart)
   - `acl.conf` (`../acl.conf`)
   - `tls-certs/` — **NOT** the dev certs from `gen-tls-certs.sh`. A real gateway deployment ships
     the production server cert/key + CA here (see `../TLS.md` "Prod"), provisioned per-gateway,
     never checked into the repo.
   - `.env` — a **production** env file (unlike the HOST dev default), setting
     `SITE_MQTT_PORT=1883`/`SITE_MQTTS_PORT=8883`/`SITE_DASHBOARD_PORT=18083`: the gateway core's
     Nucleus IPC bus is not itself a port-bound MQTT broker, so there's no local-broker port clash
     to dodge the way there is on a HOST dev box (`../README.md`) — the site broker can own the
     standard ports outright.

## Lifecycle

`Run` / `Shutdown` just delegate to `docker compose up` / `docker compose down` against the
unpacked bundle — identical commands to running the HOST recipe by hand. GDK unpacks a `ZIP`
artifact under `{artifacts:decompressedPath}/<zip-stem>/`, hence the
`site-broker-config/docker-compose.yml` path in the recipe.

**Deployment ordering is deliberately loose**: the bridge's site connection retries in its own
background loop and is non-fatal while down (DESIGN-uns-bridge.md §1.4), so it tolerates the broker
component starting after it (or restarting independently) with no `DependsOn` needed. The gateway
core typically runs **both** this component and its own `uns-bridge`
(`../../recipe.yaml`) in the same deployment — the core's local Nucleus IPC bus is a device bus
like any other, so it bridges to the very broker it's colocated with.

## What's left as a live-deploy step

This stub is validated for YAML well-formedness only (no live GG deployment was run for this
slice). Actually deploying it needs, in order:

1. A real S3 bucket + the config-bundle build step (P3-6; today `../docker-compose.yml`/`acl.conf`
   are staged by hand, not by CI).
2. Production TLS material provisioned per gateway (`../TLS.md`).
3. `greengrass-cli deployment create --recipeDir … --artifactDir … --merge
   "com.mbreissi.site-broker=1.0.0"` on a real core (the lab-5950x pattern documented in
   `../../../CLAUDE.md`'s validation matrix), alongside the bridge's own component.
