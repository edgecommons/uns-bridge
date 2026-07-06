//! # config — the bridge's own component config (§2.7 shape)
//!
//! **One-liner purpose**: Parse the bridge's config file: the standard `messaging`
//! section (the PRIMARY, device-bus connection) plus the `component.instances[]`
//! entry declaring the SITE broker — the bridge's **external system**, exactly how
//! an adapter declares its OPC UA endpoints.
//!
//! The site entry reuses the library's existing `MessagingConfig`/`mqttBroker`
//! shape for the broker endpoint ([`BrokerConfig`]). The site Last-Will is not
//! configurable: the runtime derives it from the bridge's canonical UNS state
//! topic and passes it explicitly to the core MQTT provider.
//!
//! The `uplink` block ([`UplinkConfig`]) is fully typed and enforced since P3-4
//! (per-class enable/rate-caps + the D-B10 `evt` replay buffer —
//! [`crate::policy`]); the `reply` knobs ([`ReplyConfig`]) are enforced since
//! P3-3 (the `reply_to` rewrite, [`crate::reply`]).

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use edgecommons::messaging::config::{BrokerConfig, Messaging, MessagingConfig, QosConfig};
use serde::Deserialize;
use serde_json::Value;

use crate::policy::DEFAULT_EVT_BUFFER_MAX;
use crate::relay::DEFAULT_MAX_HOPS;
use crate::reply::{DEFAULT_MAX_PENDING, DEFAULT_REPLY_TTL_SECS};

/// The default site-instance id (the documented convention, §2.1).
pub const SITE_INSTANCE_ID: &str = "site";

/// Default bounded client-side queue for the `data` class subscription (§2.2).
pub const DEFAULT_DATA_QUEUE: usize = 512;
/// Default bounded client-side queue for every other class subscription (§2.2).
pub const DEFAULT_QUEUE: usize = 64;

/// The bridge's top-level config file (unknown sections — `hierarchy`, `identity`,
/// `heartbeat`, … — are tolerated; the full facade integration is a follow-up).
#[derive(Debug, Clone, Deserialize)]
pub struct BridgeConfig {
    /// PRIMARY connection: the device-local bus. The standard edgecommons
    /// `messaging` shape (doubles as the `--transport MQTT <file>` payload).
    pub messaging: Messaging,
    /// The component section carrying the site-broker instance entry.
    pub component: ComponentSection,
}

/// The `component` config section. There is deliberately no `name` member: the
/// canonical schema's `component` section allows only `global`/`instances`
/// (`additionalProperties:false`) — the component's full name is supplied by the
/// runtime builder (`EdgeCommonsBuilder::new`), never by config.
#[derive(Debug, Clone, Deserialize)]
pub struct ComponentSection {
    /// Per-instance entries; the bridge's site broker lives in the entry with
    /// id [`SITE_INSTANCE_ID`].
    #[serde(default)]
    pub instances: Vec<InstanceEntry>,
}

/// One `component.instances[]` entry (§2.7). For the bridge, the `"site"` entry
/// declares the site broker plus the relay knobs.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceEntry {
    /// The instance id (`"site"` selects the site-broker entry).
    pub id: String,
    /// The site broker endpoint — the existing library `mqttBroker` shape
    /// (host/port/clientId/credentials, TLS via cert paths).
    #[serde(default)]
    pub site_broker: Option<BrokerConfig>,
    /// Per-class uplink policy (§2.5): enables, rate caps, and the D-B10 `evt`
    /// replay buffer — enforced by [`crate::policy::UplinkPolicy`] since P3-4.
    #[serde(default)]
    pub uplink: UplinkConfig,
    /// Hop-tag cap (§2.3), default [`DEFAULT_MAX_HOPS`].
    #[serde(default)]
    pub max_hops: Option<usize>,
    /// Per-class subscription queue depths (§2.2).
    #[serde(default)]
    pub queue: QueueConfig,
    /// Reply correlation-map knobs (§2.4 / D-B9): the TTL'd map behind the
    /// `reply_to` rewrite ([`crate::reply`]).
    #[serde(default)]
    pub reply: ReplyConfig,
}

impl InstanceEntry {
    /// The effective hop cap.
    pub fn effective_max_hops(&self) -> usize {
        self.max_hops.unwrap_or(DEFAULT_MAX_HOPS)
    }

    /// Build the site connection's [`MessagingConfig`] from this entry — the exact
    /// input the reused core `MqttProvider::connect` takes (§1.1). The site broker
    /// maps onto the config's `local` slot (the provider's primary connection);
    /// there is deliberately no `northbound` broker on the site link.
    ///
    /// # Errors
    /// When the entry has no `siteBroker`.
    pub fn site_messaging(&self) -> anyhow::Result<MessagingConfig> {
        let broker = self
            .site_broker
            .clone()
            .with_context(|| format!("component.instances[{}] has no siteBroker", self.id))?;
        Ok(MessagingConfig {
            messaging: Messaging {
                local: broker,
                northbound: None,
                qos: QosConfig::default(),
            },
        })
    }
}

/// The §2.5 per-class uplink policy block, fully typed (P3-4). Enforced at
/// runtime by [`crate::policy::UplinkPolicy::from_config`].
#[derive(Debug, Clone, Default, Deserialize)]
pub struct UplinkConfig {
    /// Per-class policy keyed by class token (`state`, …, `app`).
    #[serde(default)]
    pub classes: BTreeMap<String, ClassPolicy>,
}

impl UplinkConfig {
    /// Whether the optional seventh uplink class `app` is relayed (default off).
    /// Consulted at [`crate::relay::RelayEngine`] construction — `app` off means
    /// the seventh filter is never even subscribed.
    pub fn app_enabled(&self) -> bool {
        self.classes
            .get("app")
            .and_then(|c| c.enabled)
            .unwrap_or(false)
    }
}

/// One class's uplink policy knobs (§2.5). Every knob is optional; the §2.5
/// defaults (every class enabled except `app`; unlimited rate; the D-B10 `evt`
/// buffer on) are applied by [`crate::policy::UplinkPolicy::from_config`], not
/// here. Unknown members are tolerated (forward compatibility — e.g. a future
/// `onDisconnect`).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClassPolicy {
    /// Whether the class is relayed (default: `true` for the six consumer
    /// classes, `false` for `app`). A disabled class's messages drop + count.
    #[serde(default)]
    pub enabled: Option<bool>,
    /// Token-bucket refill rate in messages/second. Absent = unlimited. `0`
    /// forwards only the initial `burst` then drops forever (prefer
    /// `enabled: false` to switch a class off).
    #[serde(default)]
    pub max_rate_per_sec: Option<u32>,
    /// Token-bucket capacity. Default: `2 × maxRatePerSec` (§2.5).
    #[serde(default)]
    pub burst: Option<u32>,
    /// The D-B10 disconnect replay buffer. Honored for **`evt` only** (the
    /// evt-only scope call); ignored — with a warning — on any other class.
    #[serde(default)]
    pub buffer_while_disconnected: Option<DisconnectBufferConfig>,
}

/// The `evt` replay-buffer knobs (D-B10: default **on**, **1000**, drop-oldest,
/// memory-only — a WAN blip must not lose an alarm raise/clear).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DisconnectBufferConfig {
    /// Whether `evt` buffers (rather than drops) while the site link is down.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Buffer bound; overflow drops the OLDEST buffered message. `0` disables
    /// buffering entirely.
    #[serde(default = "default_evt_buffer_max")]
    pub max_messages: usize,
}

impl Default for DisconnectBufferConfig {
    fn default() -> Self {
        DisconnectBufferConfig {
            enabled: true,
            max_messages: DEFAULT_EVT_BUFFER_MAX,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_evt_buffer_max() -> usize {
    DEFAULT_EVT_BUFFER_MAX
}

/// The `reply` knobs (§2.4 / D-B9): the TTL'd correlation map behind the
/// `reply_to` rewrite.
///
/// `ttlSecs` defaults to 60 — **2×** the framework's 30 s request-deadline
/// default — so the bridge never tears down a reply path before the requester's
/// own deadline settles it. Deployments raising
/// `messaging.requestTimeoutSeconds` must raise this in step (a documented
/// **paired knob**). `maxPending` bounds the in-flight entries; overflow evicts
/// the oldest.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplyConfig {
    /// Correlation-entry TTL in seconds (default
    /// [`DEFAULT_REPLY_TTL_SECS`]).
    #[serde(default = "default_reply_ttl_secs")]
    pub ttl_secs: u64,
    /// In-flight entry bound (default [`DEFAULT_MAX_PENDING`]); overflow evicts
    /// the oldest entry (expired early + counted).
    #[serde(default = "default_max_pending")]
    pub max_pending: usize,
}

impl ReplyConfig {
    /// The TTL as a [`Duration`].
    pub fn ttl(&self) -> Duration {
        Duration::from_secs(self.ttl_secs)
    }
}

fn default_reply_ttl_secs() -> u64 {
    DEFAULT_REPLY_TTL_SECS
}

fn default_max_pending() -> usize {
    DEFAULT_MAX_PENDING
}

impl Default for ReplyConfig {
    fn default() -> Self {
        ReplyConfig {
            ttl_secs: DEFAULT_REPLY_TTL_SECS,
            max_pending: DEFAULT_MAX_PENDING,
        }
    }
}

/// Per-class bounded subscription queue depths (`max_messages`, §2.2): `data`
/// deep, everything else shallow.
#[derive(Debug, Clone, Deserialize)]
pub struct QueueConfig {
    /// Queue depth for the `data` class subscription.
    #[serde(default = "default_data_queue")]
    pub data: usize,
    /// Queue depth for every other subscription.
    #[serde(default = "default_queue", rename = "default")]
    pub default_depth: usize,
}

fn default_data_queue() -> usize {
    DEFAULT_DATA_QUEUE
}

fn default_queue() -> usize {
    DEFAULT_QUEUE
}

impl Default for QueueConfig {
    fn default() -> Self {
        QueueConfig {
            data: DEFAULT_DATA_QUEUE,
            default_depth: DEFAULT_QUEUE,
        }
    }
}

impl BridgeConfig {
    /// Load and parse the bridge config from a JSON file.
    pub async fn load(path: impl AsRef<Path>) -> anyhow::Result<BridgeConfig> {
        let path = path.as_ref();
        let bytes = tokio::fs::read(path)
            .await
            .with_context(|| format!("reading bridge config {}", path.display()))?;
        let value: Value = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing bridge config {}", path.display()))?;
        Self::reject_configured_lwt(&value)?;
        serde_json::from_value(value)
            .with_context(|| format!("parsing bridge config {}", path.display()))
    }

    fn reject_configured_lwt(value: &Value) -> anyhow::Result<()> {
        let Some(instances) = value
            .get("component")
            .and_then(|component| component.get("instances"))
            .and_then(Value::as_array)
        else {
            return Ok(());
        };

        for instance in instances {
            if instance.get("lwt").is_some() {
                let id = instance
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or("<missing id>");
                anyhow::bail!(
                    "component.instances[{id}].lwt is not configurable; uns-bridge derives the site Last-Will from its canonical UNS state topic for the console contract"
                );
            }
        }
        Ok(())
    }

    /// The relay's own raw device-bus connection config: the standard `messaging`
    /// section with `-relay` appended to every client id.
    ///
    /// Since P3-4b the bridge holds **two** device-bus connections (README
    /// "Connections"): the EdgeCommons runtime builds the observability connection
    /// from the config **file** (this same `messaging` section — it doubles as the
    /// `--transport MQTT` payload), and the relay holds this second raw one. The
    /// suffix keeps the two MQTT client ids distinct (a shared id makes the broker
    /// bounce the clients in a session-takeover loop).
    pub fn relay_primary_messaging(&self) -> MessagingConfig {
        let mut messaging = self.messaging.clone();
        messaging.local.client_id = format!("{}-relay", messaging.local.client_id);
        if let Some(northbound) = messaging.northbound.as_mut() {
            northbound.client_id = format!("{}-relay", northbound.client_id);
        }
        MessagingConfig { messaging }
    }

    /// The site-broker instance entry: the entry with id [`SITE_INSTANCE_ID`], or —
    /// when none carries that id — the single entry declaring a `siteBroker`.
    ///
    /// # Errors
    /// When no entry qualifies, or several entries carry a `siteBroker` and none
    /// is named `"site"` (ambiguous).
    pub fn site_instance(&self) -> anyhow::Result<&InstanceEntry> {
        if let Some(entry) = self
            .component
            .instances
            .iter()
            .find(|i| i.id == SITE_INSTANCE_ID)
        {
            return Ok(entry);
        }
        let mut with_broker = self
            .component
            .instances
            .iter()
            .filter(|i| i.site_broker.is_some());
        match (with_broker.next(), with_broker.next()) {
            (Some(entry), None) => Ok(entry),
            (Some(_), Some(_)) => anyhow::bail!(
                "several component.instances[] declare a siteBroker and none is named \
                 '{SITE_INSTANCE_ID}' — name the site entry '{SITE_INSTANCE_ID}'"
            ),
            _ => anyhow::bail!(
                "no site-broker entry found: add a component.instances[] entry with \
                 id '{SITE_INSTANCE_ID}' and a siteBroker section"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The §2.7 sample shape (concrete values — template substitution arrives with
    /// the facade integration).
    const FULL: &str = r#"{
        "hierarchy": { "levels": ["site", "device"] },
        "identity":  { "site": "dallas" },
        "messaging": {
            "local": { "host": "localhost", "port": 1883, "clientId": "uns-bridge-local" },
            "requestTimeoutSeconds": 30
        },
        "component": {
            "instances": [
                { "id": "site",
                  "siteBroker": { "host": "site-broker.dallas.example", "port": 8883,
                                  "clientId": "uns-bridge-site",
                                  "credentials": { "certPath": "c.pem", "keyPath": "k.pem", "caPath": "ca.pem" } },
                  "uplink": { "classes": { "app": { "enabled": true },
                                            "data": { "enabled": true, "maxRatePerSec": 200, "burst": 400 } } },
                  "reply": { "ttlSecs": 60, "maxPending": 1024 },
                  "maxHops": 6,
                  "queue": { "data": 256, "default": 32 } }
            ]
        }
    }"#;

    #[test]
    fn parses_the_full_section_2_7_shape() {
        let cfg: BridgeConfig = serde_json::from_str(FULL).unwrap();
        let site = cfg.site_instance().unwrap();
        assert_eq!(site.id, "site");
        assert_eq!(site.effective_max_hops(), 6);
        assert!(site.uplink.app_enabled());
        assert_eq!(site.queue.data, 256);
        assert_eq!(site.queue.default_depth, 32);
        assert_eq!(site.reply.ttl_secs, 60);
        assert_eq!(site.reply.ttl(), Duration::from_secs(60));
        assert_eq!(site.reply.max_pending, 1024);
        // The P3-4 knobs parse typed.
        let data = &site.uplink.classes["data"];
        assert_eq!(data.enabled, Some(true));
        assert_eq!(data.max_rate_per_sec, Some(200));
        assert_eq!(data.burst, Some(400));
        assert!(data.buffer_while_disconnected.is_none());
    }

    #[test]
    fn the_shipped_sample_config_parses_with_typed_uplink_knobs() {
        // Backward compatibility with the committed sample (§2.7 shape).
        let cfg: BridgeConfig =
            serde_json::from_str(include_str!("../test-configs/config.json")).unwrap();
        let uplink = &cfg.site_instance().unwrap().uplink;
        assert_eq!(uplink.classes["log"].enabled, Some(false));
        assert_eq!(uplink.classes["metric"].max_rate_per_sec, Some(50));
        assert_eq!(
            uplink.classes["metric"].burst, None,
            "burst defaults to 2x rate downstream"
        );
        assert_eq!(uplink.classes["data"].max_rate_per_sec, Some(200));
        assert_eq!(uplink.classes["data"].burst, Some(400));
        let buf = uplink.classes["evt"]
            .buffer_while_disconnected
            .as_ref()
            .unwrap();
        assert!(
            buf.enabled,
            "enabled defaults true when only maxMessages is given"
        );
        assert_eq!(buf.max_messages, 1000);
        assert!(!uplink.app_enabled());
    }

    #[test]
    fn buffer_knobs_default_and_unknown_class_members_are_tolerated() {
        // `bufferWhileDisconnected: {}` -> the D-B10 defaults (on, 1000);
        // unknown members (e.g. a future `onDisconnect`) must not break parsing.
        let uplink: UplinkConfig = serde_json::from_str(
            r#"{ "classes": {
                "evt":  { "bufferWhileDisconnected": {} },
                "data": { "enabled": true, "onDisconnect": "drop" }
            } }"#,
        )
        .unwrap();
        let buf = uplink.classes["evt"]
            .buffer_while_disconnected
            .as_ref()
            .unwrap();
        assert!(buf.enabled);
        assert_eq!(buf.max_messages, DEFAULT_EVT_BUFFER_MAX);
        assert_eq!(uplink.classes["data"].enabled, Some(true));
        // And the plain Default carries the same knobs.
        let d = DisconnectBufferConfig::default();
        assert!(d.enabled);
        assert_eq!(d.max_messages, DEFAULT_EVT_BUFFER_MAX);
    }

    #[test]
    fn site_messaging_uses_core_broker_shape_without_configurable_lwt() {
        let cfg: BridgeConfig = serde_json::from_str(FULL).unwrap();
        let site = cfg.site_instance().unwrap();
        let mc = site.site_messaging().unwrap();
        assert_eq!(
            mc.messaging.local.resolved_host().unwrap(),
            "site-broker.dallas.example"
        );
        assert_eq!(mc.messaging.local.port, 8883);
        assert!(
            mc.messaging.northbound.is_none(),
            "no northbound broker on the site link"
        );
    }

    #[test]
    fn relay_primary_messaging_wraps_the_section_with_a_distinct_client_id() {
        let cfg: BridgeConfig = serde_json::from_str(FULL).unwrap();
        let mc = cfg.relay_primary_messaging();
        assert_eq!(mc.messaging.local.resolved_host().unwrap(), "localhost");
        assert_eq!(mc.messaging.local.port, 1883);
        // The EdgeCommons runtime connects with the CONFIGURED id (from the file);
        // the relay's raw connection must never collide with it.
        assert_eq!(mc.messaging.local.client_id, "uns-bridge-local-relay");
        assert!(mc.messaging.northbound.is_none());
    }

    #[test]
    fn defaults_apply_when_knobs_are_absent() {
        let cfg: BridgeConfig = serde_json::from_str(
            r#"{
                "messaging": { "local": { "host": "h", "port": 1883, "clientId": "c" } },
                "component": { "instances": [
                    { "id": "site", "siteBroker": { "host": "s", "port": 1884, "clientId": "cs" } }
                ] }
            }"#,
        )
        .unwrap();
        let site = cfg.site_instance().unwrap();
        assert_eq!(site.effective_max_hops(), DEFAULT_MAX_HOPS);
        assert!(!site.uplink.app_enabled(), "app is default OFF");
        assert_eq!(site.queue.data, DEFAULT_DATA_QUEUE);
        assert_eq!(site.queue.default_depth, DEFAULT_QUEUE);
        assert_eq!(site.reply.ttl_secs, DEFAULT_REPLY_TTL_SECS);
        assert_eq!(site.reply.max_pending, DEFAULT_MAX_PENDING);
    }

    #[test]
    fn partial_reply_section_fills_the_other_knob_with_its_default() {
        let cfg: BridgeConfig = serde_json::from_str(
            r#"{
                "messaging": { "local": { "host": "h", "port": 1883, "clientId": "c" } },
                "component": { "instances": [
                    { "id": "site",
                      "siteBroker": { "host": "s", "port": 1884, "clientId": "cs" },
                      "reply": { "ttlSecs": 120 } }
                ] }
            }"#,
        )
        .unwrap();
        let site = cfg.site_instance().unwrap();
        assert_eq!(site.reply.ttl_secs, 120, "the paired-knob override");
        assert_eq!(site.reply.max_pending, DEFAULT_MAX_PENDING);
    }

    #[test]
    fn site_selection_falls_back_to_the_sole_broker_entry() {
        let cfg: BridgeConfig = serde_json::from_str(
            r#"{
                "messaging": { "local": { "host": "h", "port": 1883, "clientId": "c" } },
                "component": { "instances": [
                    { "id": "uplink-1", "siteBroker": { "host": "s", "port": 1884, "clientId": "cs" } }
                ] }
            }"#,
        )
        .unwrap();
        assert_eq!(cfg.site_instance().unwrap().id, "uplink-1");
    }

    #[test]
    fn missing_or_ambiguous_site_entry_is_an_error() {
        let none: BridgeConfig = serde_json::from_str(
            r#"{
                "messaging": { "local": { "host": "h", "port": 1883, "clientId": "c" } },
                "component": { "instances": [] }
            }"#,
        )
        .unwrap();
        assert!(none.site_instance().is_err());

        let ambiguous: BridgeConfig = serde_json::from_str(
            r#"{
                "messaging": { "local": { "host": "h", "port": 1883, "clientId": "c" } },
                "component": { "instances": [
                    { "id": "a", "siteBroker": { "host": "s1", "port": 1884, "clientId": "c1" } },
                    { "id": "b", "siteBroker": { "host": "s2", "port": 1885, "clientId": "c2" } }
                ] }
            }"#,
        )
        .unwrap();
        assert!(ambiguous.site_instance().is_err());

        let entry_without_broker: BridgeConfig = serde_json::from_str(
            r#"{
                "messaging": { "local": { "host": "h", "port": 1883, "clientId": "c" } },
                "component": { "instances": [ { "id": "site" } ] }
            }"#,
        )
        .unwrap();
        assert!(entry_without_broker
            .site_instance()
            .unwrap()
            .site_messaging()
            .is_err());
    }

    #[tokio::test]
    async fn load_reads_and_parses_a_file() {
        let dir = std::env::temp_dir().join(format!("uns-bridge-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.json");
        std::fs::write(&path, FULL).unwrap();
        let cfg = BridgeConfig::load(&path).await.unwrap();
        assert_eq!(cfg.site_instance().unwrap().id, "site");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn load_rejects_configurable_site_lwt() {
        let dir = std::env::temp_dir().join(format!("uns-bridge-cfg-lwt-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.json");
        std::fs::write(
            &path,
            r#"{ "messaging": { "local": { "host": "h", "port": 1883, "clientId": "c" } },
                 "component": { "instances": [
                   { "id": "site",
                     "siteBroker": { "host": "s", "port": 1884, "clientId": "cs" },
                     "lwt": { "topic": "ecv1/gw-01/uns-bridge/main/state" } }
                 ] } }"#,
        )
        .unwrap();

        let err = BridgeConfig::load(&path).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("component.instances[site].lwt is not configurable"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn load_missing_file_is_an_error() {
        assert!(BridgeConfig::load("/no/such/uns-bridge-config.json")
            .await
            .is_err());
    }
}
