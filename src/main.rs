//! # uns-bridge — entry point
//!
//! The **UNS bridge** (DESIGN-uns §9 / DESIGN-uns-bridge in the ggcommons
//! monorepo): one per device bus, an envelope-aware relay between the device-local
//! bus and the site UNS broker.
//!
//! - **PRIMARY** connection = the device bus, from the standard `messaging`
//!   config section (P3-2 targets HOST: a local MQTT broker).
//! - **SITE** connection = the bridge's external system, declared in its own
//!   `component.instances[]` "site" entry and built by **reusing the ggcommons
//!   core's already-pub MQTT objects** (`MqttProvider::connect(&site_cfg)`) —
//!   ZERO core change (§1.1). The site connect is retried in the bridge's own
//!   loop (non-fatal uplink, §1.4); the provider re-subscribes every filter on
//!   each CONNACK, so reconnection is transparent.
//! - The relay itself runs at the raw `MessagingProvider` level on **both**
//!   connections (byte relay — no reserved-class guard in the path, §1.3).
//!
//! ## Run locally (HOST, device broker :1883 + site broker :1884)
//! ```bash
//! cargo run -- --config ./test-configs/config.json --thing gw-01
//! ```
//!
//! ## Follow-ups (see README "Roadmap")
//! - GREENGRASS variant (PRIMARY = Nucleus IPC) — P3-2 targets the HOST
//!   MQTT↔MQTT pair.
//! - Full ggcommons facade integration (standard `-c`/`--platform`/`--transport`
//!   CLI contract, template substitution, heartbeat/state, `cfg` announce,
//!   metric-surfaced counters) — the facade's own state/metric traffic then rides
//!   this very relay (§2.8).

mod config;
mod io;
mod relay;
mod reply;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use ggcommons::messaging::config::MessagingConfig;
use ggcommons::messaging::provider::mqtt::MqttProvider;
use ggcommons::messaging::MessagingProvider;

use crate::config::BridgeConfig;
use crate::relay::RelayEngine;

#[cfg(not(feature = "standalone"))]
compile_error!(
    "uns-bridge P3-2 targets the HOST MQTT<->MQTT pair; build with the default `standalone` \
     feature (the GREENGRASS primary=IPC variant is a documented follow-up)"
);

/// The component's full name (matches `recipe.yaml` / `gdk-config.json`; its
/// sanitized UNS token is exactly `uns-bridge`, D-U18).
const COMPONENT_NAME: &str = "com.mbreissi.uns-bridge";

/// Delay between site-broker connect attempts (§1.4 — bridge-owned retry loop).
const SITE_RETRY_DELAY: Duration = Duration::from_secs(5);

/// Minimal P3-2 arguments. Full ggcommons CLI-contract parsing arrives with the
/// facade integration.
struct Args {
    config_path: String,
    thing: String,
}

fn parse_args() -> anyhow::Result<Args> {
    let mut config_path = "test-configs/config.json".to_string();
    let mut thing: Option<String> = None;
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "-c" | "--config" => {
                config_path = it.next().context("--config requires a path")?;
            }
            "-t" | "--thing" => {
                // The full string value (the historical one-char truncation bug is
                // exactly what the standard contract guards against).
                thing = Some(it.next().context("--thing requires a name")?);
            }
            "-h" | "--help" => {
                println!(
                    "uns-bridge — UNS relay between the device bus and the site broker\n\n\
                     USAGE: uns-bridge [--config <file>] [--thing <device>]\n\n\
                     OPTIONS:\n  \
                     -c, --config <file>   bridge config (default: test-configs/config.json)\n  \
                     -t, --thing <name>    device (thing) token; falls back to $GGCOMMONS_THING_NAME"
                );
                std::process::exit(0);
            }
            other => anyhow::bail!("unknown argument '{other}' (see --help)"),
        }
    }
    // Standard identity chain, minimally: -t ▸ platform env. (Full chain with the
    // facade integration.)
    let thing = thing
        .or_else(|| std::env::var("GGCOMMONS_THING_NAME").ok().filter(|v| !v.is_empty()))
        .context("device identity required: pass -t/--thing or set GGCOMMONS_THING_NAME")?;
    Ok(Args { config_path, thing })
}

/// Resolves on Ctrl-C (all platforms) or SIGTERM (unix) — the graceful-shutdown
/// trigger (unsubscribe-before-exit).
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "SIGTERM handler unavailable; Ctrl-C only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// §1.4: the uplink is intermittent by design — the bridge must come up and serve
/// the device bus while the WAN is down, so the site connect retries forever in
/// the bridge's own loop (`MqttProvider::connect` itself blocks ≤ 10 s per try).
async fn connect_site_with_retry(site_cfg: &MessagingConfig) -> Arc<MqttProvider> {
    loop {
        match MqttProvider::connect(site_cfg).await {
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = parse_args()?;
    let cfg = BridgeConfig::load(&args.config_path).await?;
    let site_entry = cfg.site_instance()?;

    let engine = Arc::new(RelayEngine::new(
        &args.thing,
        site_entry.effective_max_hops(),
        site_entry.uplink.app_enabled(),
    )?);
    tracing::info!(
        component = COMPONENT_NAME,
        device = %engine.device(),
        hop_id = %engine.hop_id(),
        max_hops = site_entry.effective_max_hops(),
        uplink_filters = engine.uplink_subscriptions().len(),
        downlink_filter = %engine.downlink_filter(),
        reply_ttl_secs = site_entry.reply.ttl_secs,
        reply_max_pending = site_entry.reply.max_pending,
        "uns-bridge starting"
    );

    // PRIMARY: the device bus. A dead device bus is fatal — the bridge is useless
    // without it (unlike the site uplink, which retries below).
    let primary: Arc<dyn MessagingProvider> =
        Arc::new(MqttProvider::connect(&cfg.primary_messaging()).await.context(
            "connecting the PRIMARY (device-bus) broker — is the local broker up?",
        )?);
    tracing::info!("device-bus connection established");

    // SITE: the reused core MqttProvider over the bridge's own instances[] entry
    // (§1.1) — retried, and abandonable by a shutdown signal while still trying.
    let site_cfg = site_entry.site_messaging()?;
    let site: Arc<dyn MessagingProvider> = tokio::select! {
        provider = connect_site_with_retry(&site_cfg) => provider,
        _ = shutdown_signal() => {
            tracing::info!("shutdown before the site broker became reachable; exiting");
            return Ok(());
        }
    };

    // The relay: six (+1) uplink pumps + the pinned downlink pump + the §2.4
    // reply proxy (correlation map + TTL sweep).
    let relay =
        io::RelayIo::start(engine, primary, site, &site_entry.queue, &site_entry.reply).await?;
    tracing::info!("relay running");

    shutdown_signal().await;
    tracing::info!("shutdown signal received; stopping relay");

    let (uplinked, downlinked, loop_dropped, reply_relayed, reply_expired) = {
        use std::sync::atomic::Ordering;
        let c = relay.counters();
        (
            c.uplinked.load(Ordering::Relaxed),
            c.downlinked.load(Ordering::Relaxed),
            c.loop_dropped.load(Ordering::Relaxed),
            c.reply_relayed.load(Ordering::Relaxed),
            c.reply_expired.load(Ordering::Relaxed),
        )
    };
    let pending_replies = relay.pending_replies();
    relay.shutdown().await; // aborts pumps + unsubscribes everything at both brokers
    tracing::info!(
        uplinked,
        downlinked,
        loop_dropped,
        reply_relayed,
        reply_expired,
        pending_replies,
        "uns-bridge stopped"
    );
    Ok(())
}
