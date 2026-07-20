//! # uns-bridge — entry point
//!
//! The **UNS bridge** (DESIGN-uns §9 / DESIGN-uns-bridge in the edgecommons
//! monorepo): one per device bus, an envelope-aware relay between the device-local
//! bus and the site UNS broker.
//!
//! Since P3-4b the bridge is a **proper edgecommons component** (§2.8): a real
//! `EdgeCommons` runtime — built from the same config file — owns the bridge's own
//! observability: the resolved identity, the automatic heartbeat `state` keepalive
//! on `ecv1/{device}/uns-bridge/state`, the effective-(redacted-)config `cfg`
//! publisher, and `gg.metrics()` (the relay counters emit through it periodically,
//! `metricEmission.target = "messaging"` in the shipped config). All of that
//! traffic matches the bridge's own uplink filters, so it **rides its own relay**
//! to the site broker.
//!
//! ## The two connections (the connection-architecture decision)
//!
//! | Connection | Owner | Purpose |
//! |---|---|---|
//! | device bus | the `EdgeCommons` runtime, **shared** with the relay | the runtime's heartbeat `state`, `cfg` announce, and `metric` emission (through the reserved-class guard), plus — via the runtime's raw provider — the relay's provider-level protobuf relay (§1.3), the reply proxy, and the rehydration broadcast (below the guard) |
//! | site broker | the bridge | the uplink/downlink relay target; carries the D-B11 LWT |
//!
//! The relay's PRIMARY is the runtime's own device-bus provider, obtained via the
//! core affordance `gg.raw_device_provider()` (`EdgeCommons::raw_device_provider`,
//! backed by `DefaultMessagingService::provider`). This is the SAME transport the
//! runtime already resolved — **IPC on GREENGRASS** (the `greengrass` feature),
//! **MQTT on HOST** (`standalone`) — so the bridge holds ONE device-bus client, not
//! two. The relay publishes/subscribes raw protobuf `EdgeCommonsMessage` bytes
//! across every class **below the reserved-class publish guard** (§1.3): that guard
//! is a `MessagingService` concern and the raw provider is beneath it, which is
//! exactly what lets the bridge relay reserved classes (`state`/`metric`/`cfg`/
//! `log`) verbatim. Sharing the one connection unifies HOST and GREENGRASS and
//! spares a client under the Greengrass shared-connection quota.
//!
//! - **SITE** connection = the bridge's external system, declared in its own
//!   `component.instances[]` "site" entry and built by **reusing the edgecommons
//!   core's MQTT provider** (`MqttProvider::connect_with_last_will`) — always MQTT,
//!   independent of the device-bus transport. The site Last-Will is a private
//!   bridge-console contract derived from the bridge's canonical UNS state topic,
//!   not user config. The site connect is retried in the bridge's own loop
//!   (non-fatal uplink, §1.4); the provider re-subscribes every filter on each
//!   CONNACK, so reconnection is transparent.
//!
//! ## Run locally (HOST, device broker :1883 + site broker :1884)
//! ```bash
//! cargo run -- --platform HOST --transport MQTT ./test-configs/config.json \
//!   -c FILE ./test-configs/config.json -t gw-01
//! ```
//!
//! ## Run on Greengrass (device IPC ↔ site MQTT)
//! Deployed by `recipe.yaml` with `--platform GREENGRASS --transport IPC -c
//! GG_CONFIG -t {iot:thingName}`; the device bus is the Nucleus IPC pubsub (no
//! `messaging` section needed), the site broker comes from the deployment's
//! `component.instances[]` `siteBroker`. Requires the `greengrass` feature
//! (Linux-only C-FFI IPC provider).

mod config;
mod io;
mod observability;
mod policy;
mod relay;
mod reply;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use edgecommons::messaging::config::MessagingConfig;
use edgecommons::messaging::provider::mqtt::{MqttLastWill, MqttProvider};
use edgecommons::messaging::{MessageBuilder, MessagingProvider, Qos};
use edgecommons::uns::UnsClass;
use edgecommons::EdgeCommonsBuilder;
use serde_json::json;

use crate::config::BridgeConfig;
use crate::relay::RelayEngine;

// The crate builds for both device-bus transports: the default `standalone` feature
// (HOST, MQTT) and the Linux-only `greengrass` feature (GREENGRASS, Nucleus IPC). The
// relay's PRIMARY is whichever transport the runtime resolves (`gg.raw_device_provider()`),
// so a single code path serves both.

/// The component's full name (matches `recipe.yaml` / `gdk-config.json`; its
/// sanitized UNS token is exactly `uns-bridge`, D-U18).
const COMPONENT_NAME: &str = "com.mbreissi.edgecommons.UnsBridge";

/// Delay between site-broker connect attempts (§1.4 — bridge-owned retry loop).
const SITE_RETRY_DELAY: Duration = Duration::from_secs(5);

/// §1.4: the uplink is intermittent by design — the bridge must come up and serve
/// the device bus while the WAN is down, so the site connect retries forever in
/// the bridge's own loop (`MqttProvider::connect_with_last_will` itself blocks ≤ 10 s per try).
async fn connect_site_with_retry(
    site_cfg: &MessagingConfig,
    site_last_will: &MqttLastWill,
) -> Arc<MqttProvider> {
    loop {
        match MqttProvider::connect_with_last_will(site_cfg, Some(site_last_will)).await {
            Ok(provider) => {
                tracing::info!("site-broker connection established");
                return Arc::new(provider);
            }
            Err(e) => {
                tracing::warn!(error = %e, retry_in = ?SITE_RETRY_DELAY, "site-broker connect failed");
                tokio::time::sleep(SITE_RETRY_DELAY).await;
            }
        }
    }
}

fn site_unreachable_last_will(topic: String, payload: Vec<u8>) -> MqttLastWill {
    MqttLastWill {
        topic,
        payload,
        qos: Qos::AtLeastOnce,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // §2.8: the edgecommons runtime — the bridge's device-bus connection (which the
    // relay then SHARES, below) plus identity, logging init, the heartbeat `state`
    // keepalive, the effective-config `cfg` publisher, gg.metrics(), and the
    // library-owned SIGTERM/Ctrl-C shutdown signal. A dead device bus is fatal — the
    // bridge is useless without it (unlike the site uplink, which retries below).
    let gg = EdgeCommonsBuilder::new(COMPONENT_NAME)
        .args(std::env::args_os())
        .build()
        .await
        .context("initializing the edgecommons runtime — is the device-bus broker up?")?;
    let runtime_config = gg.config();
    let device = runtime_config.identity().device().to_string();
    let cfg = BridgeConfig::from_value(runtime_config.raw.clone())
        .context("parsing bridge settings from the effective EdgeCommons config")?;
    let site_entry = cfg.site_instance()?;

    let engine = Arc::new(RelayEngine::new(
        &device,
        site_entry.effective_max_hops(),
        site_entry.uplink.app_enabled(),
    )?);
    tracing::info!(
        component = COMPONENT_NAME,
        device = %engine.device(),
        hop_id = %engine.hop_id(),
        max_hops = site_entry.effective_max_hops(),
        uplink_filters = engine.uplink_subscriptions().len(),
        downlink_filters = engine.downlink_filters().len(),
        reply_ttl_secs = site_entry.reply.ttl_secs,
        reply_max_pending = site_entry.reply.max_pending,
        "uns-bridge starting"
    );

    // RELAY PRIMARY = the runtime's OWN raw device-bus provider (core #58's
    // affordance): the SAME connection the runtime already resolved for its
    // observability — IPC on GREENGRASS, MQTT on HOST — handed back below the
    // reserved-class publish guard (§1.3) so the relay forwards raw protobuf across
    // ALL classes verbatim, WITHOUT opening a second device-bus client. `None` means
    // the runtime wired no transport, which for a relay is fatal: there is no device
    // bus to relay.
    let primary: Arc<dyn MessagingProvider> = gg.raw_device_provider().context(
        "the runtime resolved no messaging transport — a bridge has no device bus to relay; \
         supply --transport IPC (GREENGRASS) or --transport MQTT <path> (HOST)",
    )?;
    tracing::info!(
        "relay shares the runtime's device-bus provider (raw, below the reserved-class guard)"
    );

    let site_cfg = site_entry.site_messaging()?;
    let site_lwt_topic = gg
        .uns()
        .topic(UnsClass::State)
        .context("deriving the private site LWT topic from the bridge state topic")?;
    let site_lwt_payload = MessageBuilder::new("state", "1.0")
        .from_config(runtime_config.as_ref())
        .state_update(json!({ "status": "UNREACHABLE" }))
        .build()
        .to_vec()
        .context("building protobuf site Last-Will state payload")?;
    let site_last_will = site_unreachable_last_will(site_lwt_topic.clone(), site_lwt_payload);
    tracing::info!(
        topic = %site_lwt_topic,
        "site Last-Will derived from bridge state topic for console reachability"
    );

    // SITE: the reused core MqttProvider over the bridge's own instances[] entry
    // (§1.1) — retried, and abandonable by a shutdown signal while still trying.
    let site: Arc<dyn MessagingProvider> = tokio::select! {
        provider = connect_site_with_retry(&site_cfg, &site_last_will) => provider,
        _ = gg.shutdown_signal() => {
            tracing::info!("shutdown before the site broker became reachable; exiting");
            return Ok(());
        }
    };

    // The relay: six (+1) uplink pumps through the §2.5 policy + the pinned
    // downlink pump + the §2.4 reply proxy (correlation map + TTL sweep) + the
    // §2.5/D-B10 connectivity watcher (rehydration broadcast + evt replay) + the
    // §2.8 metric emission over gg.metrics().
    let relay = io::RelayIo::start(
        engine,
        primary,
        site,
        &site_entry.queue,
        &site_entry.reply,
        &site_entry.uplink,
        Some(io::ObservabilityHook {
            metrics: gg.metrics(),
            config: gg.config(),
        }),
    )
    .await?;
    tracing::info!("relay running");

    gg.shutdown_signal().await;
    tracing::info!("shutdown signal received; stopping relay");

    let (
        uplinked,
        downlinked,
        loop_dropped,
        reply_relayed,
        reply_expired,
        dropped_disabled,
        dropped_rate,
        dropped_disconnected,
        evt_replayed,
    ) = {
        use std::sync::atomic::Ordering;
        let c = relay.counters();
        (
            c.uplinked.total(),
            c.downlinked.load(Ordering::Relaxed),
            c.loop_dropped.load(Ordering::Relaxed),
            c.reply_relayed.load(Ordering::Relaxed),
            c.reply_expired.load(Ordering::Relaxed),
            c.dropped_disabled.total(),
            c.dropped_rate.total(),
            c.dropped_disconnected.total(),
            c.evt_replayed.load(Ordering::Relaxed),
        )
    };
    let pending_replies = relay.pending_replies();
    let buffered_evt = relay.buffered_evt();
    relay.shutdown().await; // aborts pumps + unsubscribes everything at both brokers
    tracing::info!(
        uplinked,
        downlinked,
        loop_dropped,
        reply_relayed,
        reply_expired,
        dropped_disabled,
        dropped_rate,
        dropped_disconnected,
        evt_replayed,
        buffered_evt,
        pending_replies,
        "uns-bridge stopped"
    );
    Ok(())
}
