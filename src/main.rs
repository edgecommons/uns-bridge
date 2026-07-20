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
//! ## The three connections (the P3-4b connection-architecture decision)
//!
//! | Connection | Owner | Purpose |
//! |---|---|---|
//! | device bus (observability) | the `EdgeCommons` runtime | heartbeat `state`, `cfg` announce, `metric` emission |
//! | device bus (relay, client id `…-relay`) | the bridge | the provider-level protobuf relay (§1.3) + the reply proxy + the rehydration broadcast |
//! | site broker | the bridge | the uplink/downlink relay target; carries the D-B11 LWT |
//!
//! `EdgeCommons` deliberately does **not** expose its raw `MessagingProvider`
//! (`DefaultMessagingService` keeps it private), and the relay must stay at the
//! raw provider level — below the reserved-class guard (§1.3) — so the relay
//! cannot reuse the runtime's connection **without a edgecommons change, which
//! this slice deliberately does not make**. Normal relay payloads remain
//! protobuf `EdgeCommonsMessage` bytes. Follow-up (Rust-only library affordance):
//! expose the runtime's raw provider (e.g.
//! `DefaultMessagingService::provider()`), letting the relay share the runtime's
//! device-bus connection — one client less, which matters under the GREENGRASS
//! shared-connection quota once the IPC-primary variant lands.
//!
//! - **SITE** connection = the bridge's external system, declared in its own
//!   `component.instances[]` "site" entry and built by **reusing the edgecommons
//!   core's MQTT provider** (`MqttProvider::connect_with_last_will`). The site
//!   Last-Will is a private bridge-console contract derived from the bridge's
//!   canonical UNS state topic, not user config. The site connect is retried in
//!   the bridge's own loop (non-fatal uplink, §1.4); the provider re-subscribes
//!   every filter on each CONNACK, so reconnection is transparent.
//!
//! ## Run locally (HOST, device broker :1883 + site broker :1884)
//! ```bash
//! cargo run -- --platform HOST --transport MQTT ./test-configs/config.json \
//!   -c FILE ./test-configs/config.json -t gw-01
//! ```
//!
//! ## Follow-ups (see DESIGN.md "Still deferred")
//! - GREENGRASS variant (PRIMARY = Nucleus IPC) — this binary targets the HOST
//!   MQTT↔MQTT pair; the IPC-primary relay wiring is not yet built.

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

#[cfg(not(feature = "standalone"))]
compile_error!(
    "uns-bridge targets the HOST MQTT<->MQTT pair; build with the default `standalone` \
     feature (the GREENGRASS primary=IPC variant is a documented follow-up)"
);

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
    // §2.8: the edgecommons runtime — the bridge's OBSERVABILITY device-bus
    // connection plus identity, logging init, the heartbeat `state` keepalive,
    // the effective-config `cfg` publisher, gg.metrics(), and the library-owned
    // SIGTERM/Ctrl-C shutdown signal. A dead device bus is fatal — the bridge is
    // useless without it (unlike the site uplink, which retries below).
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

    // RELAY PRIMARY: the bridge's second, provider-level device-bus connection
    // (client id suffixed `-relay` so it never collides with the runtime's). The
    // relay runs below the MessagingService guard by design (§1.3); see the
    // module docs for why it cannot share the runtime's connection without a
    // (deliberately unmade) edgecommons change.
    let primary: Arc<dyn MessagingProvider> = Arc::new(
        MqttProvider::connect(&cfg.relay_primary_messaging())
            .await
            .context(
                "connecting the relay's device-bus (PRIMARY) connection — is the local broker up?",
            )?,
    );
    tracing::info!("relay device-bus connection established");

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
