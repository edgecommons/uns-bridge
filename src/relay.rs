//! # relay — the pure relay decision engine
//!
//! **One-liner purpose**: Decide what to relay — the §2.2 relay matrix (class
//! routing + own-device pinning) and the §2.3 hop-tag loop protection — with no IO,
//! so the whole decision surface is unit-testable without a broker.
//!
//! (Section references are to `docs/platform/DESIGN-uns-bridge.md` in the edgecommons
//! monorepo.)
//!
//! ## Semantics
//! - The relay is **topic-verbatim**: the topic already carries `ecv1/{device}/…`,
//!   so a forwarded message republishes to the identical topic string on the other
//!   connection. The envelope travels untouched except for the hop tag.
//! - **Uplink** (device bus → site broker) relays the six consumer classes
//!   `state | cfg | evt | metric | data | log`; `app` is opt-in (default off).
//!   `cmd` is never uplinked (v1 — no cross-device request/reply).
//! - **Downlink** (site broker → device bus) relays `cmd` only, **pinned to this
//!   bridge's own device token** — a bridge must only pull down commands addressed
//!   to *its* device (which also matches its site-broker ACL scope).
//! - **Hop tag** (`tags._relay`, a JSON array of `{device}/uns-bridge` hop ids):
//!   drop-if-self (own echo), drop when `maxHops` is reached, else append own id.
//! - **Raw (non-envelope) messages** carry no tags to stamp; they relay verbatim
//!   and are protected structurally by the uplink∩downlink class disjointness.
//!
//! The IO wiring that pumps subscriptions through this engine lives in
//! [`crate::io`]; the reply-`reply_to` rewrite (P3-3, [`crate::reply`]) slots in
//! around this engine, reusing [`RelayEngine::stamp_hop`] for the reply back-haul
//! ("a relay like any other", §2.4); the per-class uplink policy / rate caps /
//! evt replay buffer (P3-4, [`crate::policy`]) slot in the same way, downstream
//! of every Forward decision on the uplink.

use edgecommons::messaging::message::{HierEntry, Message, MessageIdentity, MessageTags};
use edgecommons::uns::{Uns, UnsClass, UnsScope};
use edgecommons::Result;
use serde_json::Value;

/// The reserved envelope-tag key carrying the relay hop list (§2.3). The `_` prefix
/// extends the library-reserved token convention (`_bcast`).
pub const RELAY_TAG: &str = "_relay";

/// Default maximum hop count (§2.3 rule 2): defense against a cycle among
/// *distinct* bridges, where drop-if-self never fires on the first lap.
pub const DEFAULT_MAX_HOPS: usize = 4;

/// The bridge's UNS component token (D-U18 — the sanitized short name). The hop id,
/// the LWT topic, and the console all assume exactly this token.
pub const COMPONENT_TOKEN: &str = "uns-bridge";

/// The two §2.5 / DESIGN-uns §9.3 (layer 2) reconnect-rehydration broadcast
/// command names, in publish order — published on the DEVICE bus at the site
/// reconnect rising edge so the site view rehydrates `state`/`cfg` without
/// retain. (The device-side listener is a separate 4-language edgecommons library
/// slice; the broadcast is inert until it lands.)
pub const REHYDRATION_CMDS: [&str; 2] = ["republish-state", "republish-cfg"];

/// The six always-relayed uplink classes (§2.2 — the fleet consumer wildcard set).
/// `app` is deliberately not here: it is opt-in via [`RelayEngine::new`].
pub const UPLINK_CLASSES: [UnsClass; 6] = [
    UnsClass::State,
    UnsClass::Cfg,
    UnsClass::Evt,
    UnsClass::Metric,
    UnsClass::Data,
    UnsClass::Log,
];

/// Which way a message is crossing the bridge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Device bus → site broker.
    Uplink,
    /// Site broker → device bus.
    Downlink,
}

/// The engine's verdict for one would-be relay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayDecision {
    /// Republish these bytes to the same topic on the other connection. For an
    /// envelope the bytes carry the appended hop tag; for a raw message they are
    /// the original payload verbatim.
    Forward(Vec<u8>),
    /// Do not relay.
    Drop(DropReason),
}

/// Why a message was not relayed (feeds the P3-4 drop-counter metrics).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DropReason {
    /// `tags._relay` already contains this bridge's own hop id (§2.3 rule 1).
    OwnEcho,
    /// `tags._relay` already carries `maxHops` hop ids (§2.3 rule 2).
    MaxHopsExceeded,
    /// The topic is not a UNS topic (`ecv1/{device}/{component}/{instance}/{class}…`).
    NotUnsTopic,
    /// The topic's class does not flow in this direction (§2.2 matrix).
    ClassNotRelayed,
    /// A downlink `cmd` addressed to a different device than this bridge's own.
    NotOwnDevice,
    /// Valid JSON that claims to be an envelope but has a malformed `header`/`tags`.
    MalformedEnvelope,
}

/// The pure relay decision engine: class routing, own-device pinning, and hop-tag
/// loop protection. Holds no connections; construct once and share (`Arc`).
#[derive(Debug)]
pub struct RelayEngine {
    device: String,
    hop_id: String,
    max_hops: usize,
    app_enabled: bool,
    uplink_filters: Vec<(UnsClass, String)>,
    downlink_filter: String,
    rehydration_topics: [String; 2],
}

impl RelayEngine {
    /// Build the engine for one device bus.
    ///
    /// * `device` — this bridge's device (thing) token; pins the downlink filter
    ///   and forms the hop id `{device}/uns-bridge`.
    /// * `max_hops` — the §2.3 hop cap (config `maxHops`, default
    ///   [`DEFAULT_MAX_HOPS`]).
    /// * `app_enabled` — whether the optional seventh uplink class `app` is relayed
    ///   (config `uplink.classes.app.enabled`, default `false`).
    ///
    /// Filters are built through the library ([`Uns::filter`]) at construction, so
    /// an invalid device token fails here rather than at subscribe time.
    ///
    /// # Errors
    /// [`edgecommons::EdgeCommonsError::UnsValidation`] / `Messaging` when `device` is empty
    /// or violates the UNS token rule.
    pub fn new(
        device: impl Into<String>,
        max_hops: usize,
        app_enabled: bool,
    ) -> Result<RelayEngine> {
        let device = device.into();
        let identity = MessageIdentity::new(
            vec![HierEntry {
                level: "device".to_string(),
                value: device.clone(),
            }],
            COMPONENT_TOKEN,
            None,
        )?;
        // Rootless grammar (topic.includeRoot=false) — the P3-2 relay target; a
        // rooted site broker is the D-B12 enterprise-tier deferral.
        let uns = Uns::new(identity, false);

        let all = UnsScope::all();
        let mut uplink_filters = Vec::with_capacity(UPLINK_CLASSES.len() + 1);
        for cls in UPLINK_CLASSES {
            uplink_filters.push((cls, uns.filter(cls, &all)?));
        }
        if app_enabled {
            uplink_filters.push((UnsClass::App, uns.filter(UnsClass::App, &all)?));
        }
        let downlink_filter = uns.filter(UnsClass::Cmd, &UnsScope::device(device.clone()))?;

        // The §2.5 reconnect-rehydration broadcast topics
        // (`ecv1/{device}/_bcast/main/cmd/republish-*`) — built through the
        // library like every other topic: `_bcast` is a valid (reserved-token)
        // component position, `cmd` is an open class.
        let bcast = MessageIdentity::new(
            vec![HierEntry {
                level: "device".to_string(),
                value: device.clone(),
            }],
            "_bcast",
            None,
        )?;
        let rehydration_topics = [
            uns.topic_for(&bcast, UnsClass::Cmd, Some(REHYDRATION_CMDS[0]))?,
            uns.topic_for(&bcast, UnsClass::Cmd, Some(REHYDRATION_CMDS[1]))?,
        ];

        let hop_id = format!("{device}/{COMPONENT_TOKEN}");
        Ok(RelayEngine {
            device,
            hop_id,
            max_hops,
            app_enabled,
            uplink_filters,
            downlink_filter,
            rehydration_topics,
        })
    }

    /// This bridge's hop identifier (`{device}/uns-bridge`) — unique per bus.
    pub fn hop_id(&self) -> &str {
        &self.hop_id
    }

    /// This bridge's device (thing) token.
    pub fn device(&self) -> &str {
        &self.device
    }

    /// The uplink subscriptions: each relayed class with its wildcard filter
    /// (`ecv1/+/+/+/state`, …, `ecv1/+/+/+/log/#`, plus `ecv1/+/+/+/app/#` when
    /// opted in). The class rides along so the IO layer can size each queue
    /// per class (`data` gets the deep queue).
    pub fn uplink_subscriptions(&self) -> &[(UnsClass, String)] {
        &self.uplink_filters
    }

    /// The downlink subscription filter, pinned to this bridge's own device:
    /// `ecv1/{device}/+/+/cmd/#` (the `+` component position also covers `_bcast`).
    pub fn downlink_filter(&self) -> &str {
        &self.downlink_filter
    }

    /// The two device-bus `_bcast` topics published at the site-reconnect rising
    /// edge (§2.5 / DESIGN-uns §9.3 layer 2), in [`REHYDRATION_CMDS`] order:
    /// `ecv1/{device}/_bcast/main/cmd/republish-state` and `…/republish-cfg`.
    pub fn rehydration_topics(&self) -> &[String; 2] {
        &self.rehydration_topics
    }

    /// Decide whether (and as what bytes) to relay `payload` seen on `topic` in
    /// `direction`. Pure — no IO, no clock, no state mutation.
    pub fn decide(&self, direction: Direction, topic: &str, payload: &[u8]) -> RelayDecision {
        // 1. Class routing + device pinning (§2.2). The subscription filters
        //    already constrain what arrives; this re-check keeps the decision
        //    surface self-contained (and covers a misconfigured broker-side ACL).
        let segments: Vec<&str> = topic.split('/').collect();
        if segments.len() < 5 || segments[0] != Uns::ROOT {
            return RelayDecision::Drop(DropReason::NotUnsTopic);
        }
        let Some(class) = UnsClass::from_token(segments[4]) else {
            return RelayDecision::Drop(DropReason::NotUnsTopic);
        };
        match direction {
            Direction::Uplink => {
                let relayed =
                    UPLINK_CLASSES.contains(&class) || (self.app_enabled && class == UnsClass::App);
                if !relayed {
                    return RelayDecision::Drop(DropReason::ClassNotRelayed);
                }
            }
            Direction::Downlink => {
                if class != UnsClass::Cmd {
                    return RelayDecision::Drop(DropReason::ClassNotRelayed);
                }
                if segments[1] != self.device {
                    return RelayDecision::Drop(DropReason::NotOwnDevice);
                }
            }
        }

        // 2. Hop-tag loop protection (§2.3).
        let mut msg = match Message::from_slice(payload) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(topic, error = %e, "dropping malformed envelope");
                return RelayDecision::Drop(DropReason::MalformedEnvelope);
            }
        };
        if msg.is_raw() {
            // Raw messages cannot carry the tag; they are protected structurally by
            // the uplink/downlink class disjointness. Relay the ORIGINAL bytes
            // verbatim (never re-serialize a raw as `{"raw": …}`).
            return RelayDecision::Forward(payload.to_vec());
        }
        if let Err(reason) = self.stamp_hop(&mut msg) {
            return RelayDecision::Drop(reason);
        }

        // 3. Re-serialize (structurally identical envelope + the appended hop tag,
        //    D-U22 — serde member order is deterministic).
        match msg.to_vec() {
            Ok(bytes) => RelayDecision::Forward(bytes),
            Err(e) => {
                tracing::warn!(topic, error = %e, "failed to re-serialize relayed envelope");
                RelayDecision::Drop(DropReason::MalformedEnvelope)
            }
        }
    }

    /// Apply the §2.3 hop rules to a parsed envelope **in place**: drop-if-self
    /// (rule 1), drop at `maxHops` (rule 2), else append this bridge's own hop id
    /// (rule 3), creating the `tags`/`_relay` members as needed. A raw message is
    /// an `Ok` no-op — it cannot carry the tag (protected structurally, §2.3).
    ///
    /// Shared by [`Self::decide`] and the reply back-haul
    /// ([`crate::reply::prepare_reply`] — "the reply also gets the hop tag
    /// appended: it is a relay like any other", §2.4).
    ///
    /// # Errors
    /// The [`DropReason`] mandating the drop (`OwnEcho` / `MaxHopsExceeded`).
    pub fn stamp_hop(&self, msg: &mut Message) -> std::result::Result<(), DropReason> {
        if msg.is_raw() {
            return Ok(());
        }
        let tags = msg.tags.get_or_insert_with(MessageTags::default);
        let mut hops = match tags.extra.get(RELAY_TAG) {
            Some(Value::Array(hops)) => hops.clone(),
            Some(other) => {
                // `_`-prefixed tag keys are library-reserved; a non-array `_relay`
                // is a spec violation by a non-conforming relay. Normalize it (the
                // maxHops cap still bounds any residual cycle).
                tracing::warn!(value = %other, "non-array tags._relay; normalizing");
                Vec::new()
            }
            None => Vec::new(),
        };
        if hops
            .iter()
            .any(|h| h.as_str() == Some(self.hop_id.as_str()))
        {
            return Err(DropReason::OwnEcho);
        }
        if hops.len() >= self.max_hops {
            return Err(DropReason::MaxHopsExceeded);
        }
        hops.push(Value::String(self.hop_id.clone()));
        tags.extra.insert(RELAY_TAG.to_string(), Value::Array(hops));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgecommons::messaging::MessageBuilder;
    use serde_json::json;

    const DEVICE: &str = "gw-01";
    const HOP: &str = "gw-01/uns-bridge";

    fn engine() -> RelayEngine {
        RelayEngine::new(DEVICE, DEFAULT_MAX_HOPS, false).unwrap()
    }

    fn envelope(hops: &[&str]) -> Vec<u8> {
        let mut b = MessageBuilder::new("state", "1.0").payload(json!({ "status": "RUNNING" }));
        if !hops.is_empty() {
            b = b.tag(RELAY_TAG, json!(hops));
        }
        b.build().to_vec().unwrap()
    }

    fn forwarded_hops(bytes: &[u8]) -> Vec<String> {
        let v: Value = serde_json::from_slice(bytes).unwrap();
        v["tags"][RELAY_TAG]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| h.as_str().unwrap().to_string())
            .collect()
    }

    // ---- filter construction (built via the library, §2.2) ----

    #[test]
    fn uplink_filters_are_the_six_class_wildcards() {
        let e = engine();
        let filters: Vec<&str> = e
            .uplink_subscriptions()
            .iter()
            .map(|(_, f)| f.as_str())
            .collect();
        assert_eq!(
            filters,
            vec![
                "ecv1/+/+/+/state",
                "ecv1/+/+/+/cfg",
                "ecv1/+/+/+/evt/#",
                "ecv1/+/+/+/metric/#",
                "ecv1/+/+/+/data/#",
                "ecv1/+/+/+/log/#",
            ]
        );
    }

    #[test]
    fn app_opt_in_adds_the_seventh_filter() {
        let e = RelayEngine::new(DEVICE, DEFAULT_MAX_HOPS, true).unwrap();
        let filters: Vec<&str> = e
            .uplink_subscriptions()
            .iter()
            .map(|(_, f)| f.as_str())
            .collect();
        assert_eq!(filters.len(), 7);
        assert_eq!(filters[6], "ecv1/+/+/+/app/#");
    }

    #[test]
    fn downlink_filter_is_pinned_to_own_device() {
        assert_eq!(engine().downlink_filter(), "ecv1/gw-01/+/+/cmd/#");
    }

    #[test]
    fn invalid_device_token_fails_at_construction() {
        // The UNS token rule (D-U26) forbids '/', '+', '#', '\', control chars.
        assert!(RelayEngine::new("gw/01", DEFAULT_MAX_HOPS, false).is_err());
        assert!(RelayEngine::new("gw+01", DEFAULT_MAX_HOPS, false).is_err());
        assert!(RelayEngine::new("", DEFAULT_MAX_HOPS, false).is_err());
    }

    #[test]
    fn hop_id_is_device_slash_component() {
        assert_eq!(engine().hop_id(), HOP);
        assert_eq!(engine().device(), DEVICE);
    }

    #[test]
    fn rehydration_topics_are_the_two_bcast_cmds_on_own_device() {
        // §2.5 / DESIGN-uns §9.3 layer 2 — REHYDRATION_CMDS order.
        assert_eq!(
            engine().rehydration_topics(),
            &[
                "ecv1/gw-01/_bcast/main/cmd/republish-state".to_string(),
                "ecv1/gw-01/_bcast/main/cmd/republish-cfg".to_string(),
            ]
        );
    }

    // ---- class routing (§2.2 matrix) ----

    #[test]
    fn uplink_relays_each_of_the_six_classes() {
        let e = engine();
        for topic in [
            "ecv1/gw-01/opcua-adapter/main/state",
            "ecv1/gw-01/opcua-adapter/main/cfg",
            "ecv1/gw-01/opcua-adapter/main/evt/alarm",
            "ecv1/gw-01/opcua-adapter/main/metric/sys",
            "ecv1/gw-01/opcua-adapter/kep1/data/temp",
            "ecv1/gw-01/opcua-adapter/main/log/tail",
        ] {
            assert!(
                matches!(
                    e.decide(Direction::Uplink, topic, &envelope(&[])),
                    RelayDecision::Forward(_)
                ),
                "expected uplink forward for {topic}"
            );
        }
    }

    #[test]
    fn uplink_never_relays_cmd() {
        // The class disjointness IS the structural loop guard for raw messages.
        let d = engine().decide(
            Direction::Uplink,
            "ecv1/gw-01/opcua-adapter/main/cmd/ping",
            &envelope(&[]),
        );
        assert_eq!(d, RelayDecision::Drop(DropReason::ClassNotRelayed));
    }

    #[test]
    fn uplink_app_is_default_off_and_opt_in() {
        let topic = "ecv1/gw-01/my-app/main/app/chatter";
        assert_eq!(
            engine().decide(Direction::Uplink, topic, &envelope(&[])),
            RelayDecision::Drop(DropReason::ClassNotRelayed)
        );
        let e = RelayEngine::new(DEVICE, DEFAULT_MAX_HOPS, true).unwrap();
        assert!(matches!(
            e.decide(Direction::Uplink, topic, &envelope(&[])),
            RelayDecision::Forward(_)
        ));
    }

    #[test]
    fn downlink_relays_only_own_device_cmd() {
        let e = engine();
        assert!(matches!(
            e.decide(
                Direction::Downlink,
                "ecv1/gw-01/opcua-adapter/main/cmd/reload-config",
                &envelope(&[])
            ),
            RelayDecision::Forward(_)
        ));
        // Broadcast rides the `+` component position of the pinned filter.
        assert!(matches!(
            e.decide(
                Direction::Downlink,
                "ecv1/gw-01/_bcast/main/cmd/republish-state",
                &envelope(&[])
            ),
            RelayDecision::Forward(_)
        ));
        assert_eq!(
            e.decide(
                Direction::Downlink,
                "ecv1/gw-02/opcua-adapter/main/cmd/reload-config",
                &envelope(&[])
            ),
            RelayDecision::Drop(DropReason::NotOwnDevice)
        );
        assert_eq!(
            e.decide(
                Direction::Downlink,
                "ecv1/gw-01/opcua-adapter/main/state",
                &envelope(&[])
            ),
            RelayDecision::Drop(DropReason::ClassNotRelayed)
        );
    }

    #[test]
    fn non_uns_topics_are_dropped() {
        let e = engine();
        for topic in [
            "telemetry/gw-01/alarms",           // not ecv1
            "ecv1/gw-01/comp/main",             // too short (no class position)
            "ecv1/gw-01/comp/main/notaclass/x", // unknown class token
            "edgecommons/reply-abc123",         // reply topics never match (non-ecv1)
        ] {
            assert_eq!(
                e.decide(Direction::Uplink, topic, &envelope(&[])),
                RelayDecision::Drop(DropReason::NotUnsTopic),
                "expected NotUnsTopic for {topic}"
            );
        }
    }

    // ---- hop-tag loop protection (§2.3) ----

    #[test]
    fn first_hop_appends_own_id_creating_the_tag() {
        let d = engine().decide(Direction::Uplink, "ecv1/gw-01/c/main/state", &envelope(&[]));
        let RelayDecision::Forward(bytes) = d else {
            panic!("expected forward")
        };
        assert_eq!(forwarded_hops(&bytes), vec![HOP.to_string()]);
    }

    #[test]
    fn hop_tag_is_created_even_when_envelope_has_no_tags_member() {
        // MessageBuilder without .tag() emits no `tags` member at all.
        let bytes = MessageBuilder::new("state", "1.0")
            .payload(json!({}))
            .build()
            .to_vec()
            .unwrap();
        let input: Value = serde_json::from_slice(&bytes).unwrap();
        assert!(input.get("tags").is_none(), "precondition: no tags member");
        let d = engine().decide(Direction::Uplink, "ecv1/gw-01/c/main/state", &bytes);
        let RelayDecision::Forward(out) = d else {
            panic!("expected forward")
        };
        assert_eq!(forwarded_hops(&out), vec![HOP.to_string()]);
    }

    #[test]
    fn foreign_hops_are_preserved_in_order_and_own_id_appended() {
        let d = engine().decide(
            Direction::Uplink,
            "ecv1/gw-01/c/main/state",
            &envelope(&["gw-99/uns-bridge"]),
        );
        let RelayDecision::Forward(bytes) = d else {
            panic!("expected forward")
        };
        assert_eq!(
            forwarded_hops(&bytes),
            vec!["gw-99/uns-bridge".to_string(), HOP.to_string()]
        );
    }

    #[test]
    fn own_echo_is_dropped() {
        // Rule 1: our own id anywhere in the list — drop silently.
        let d = engine().decide(
            Direction::Uplink,
            "ecv1/gw-01/c/main/state",
            &envelope(&["site/uns-bridge", HOP]),
        );
        assert_eq!(d, RelayDecision::Drop(DropReason::OwnEcho));
    }

    #[test]
    fn max_hops_boundary() {
        let e = engine();
        // 3 foreign hops (< 4): forward, becoming 4.
        let d = e.decide(
            Direction::Uplink,
            "ecv1/gw-01/c/main/state",
            &envelope(&["a/uns-bridge", "b/uns-bridge", "c/uns-bridge"]),
        );
        let RelayDecision::Forward(bytes) = d else {
            panic!("expected forward at 3 hops")
        };
        assert_eq!(forwarded_hops(&bytes).len(), 4);
        // 4 foreign hops (== maxHops): drop (rule 2 — distinct-bridge cycle defense).
        let d = e.decide(
            Direction::Uplink,
            "ecv1/gw-01/c/main/state",
            &envelope(&[
                "a/uns-bridge",
                "b/uns-bridge",
                "c/uns-bridge",
                "d/uns-bridge",
            ]),
        );
        assert_eq!(d, RelayDecision::Drop(DropReason::MaxHopsExceeded));
    }

    #[test]
    fn custom_max_hops_is_honored() {
        let e = RelayEngine::new(DEVICE, 1, false).unwrap();
        assert!(matches!(
            e.decide(Direction::Uplink, "ecv1/gw-01/c/main/state", &envelope(&[])),
            RelayDecision::Forward(_)
        ));
        assert_eq!(
            e.decide(
                Direction::Uplink,
                "ecv1/gw-01/c/main/state",
                &envelope(&["x/uns-bridge"])
            ),
            RelayDecision::Drop(DropReason::MaxHopsExceeded)
        );
    }

    #[test]
    fn non_array_relay_tag_is_normalized() {
        let bytes = MessageBuilder::new("state", "1.0")
            .payload(json!({}))
            .tag(RELAY_TAG, json!("not-an-array"))
            .build()
            .to_vec()
            .unwrap();
        let d = engine().decide(Direction::Uplink, "ecv1/gw-01/c/main/state", &bytes);
        let RelayDecision::Forward(out) = d else {
            panic!("expected forward")
        };
        assert_eq!(forwarded_hops(&out), vec![HOP.to_string()]);
    }

    // ---- envelope fidelity + raw handling ----

    #[test]
    fn relay_is_verbatim_except_the_hop_tag() {
        let bytes = MessageBuilder::new("state", "1.0")
            .payload(json!({ "status": "RUNNING", "nested": { "a": [1, 2, 3] } }))
            .tag("site", json!("dallas"))
            .uuid("00000000-0000-4000-8000-000000000001")
            .timestamp("2026-07-03T12:00:00Z")
            .correlation_id("corr-1")
            .build()
            .to_vec()
            .unwrap();
        let RelayDecision::Forward(out) =
            engine().decide(Direction::Uplink, "ecv1/gw-01/c/main/state", &bytes)
        else {
            panic!("expected forward")
        };
        let mut input: Value = serde_json::from_slice(&bytes).unwrap();
        let output: Value = serde_json::from_slice(&out).unwrap();
        // Structurally identical once the hop tag is added to the input (D-U22).
        input["tags"][RELAY_TAG] = json!([HOP]);
        assert_eq!(output, input);
    }

    #[test]
    fn raw_json_message_relays_original_bytes_verbatim() {
        // A non-envelope object (no header/identity/tags/body) is a raw message.
        let payload = serde_json::to_vec(&json!({ "temperature": 21.5 })).unwrap();
        let d = engine().decide(Direction::Uplink, "ecv1/gw-01/c/main/data/temp", &payload);
        assert_eq!(d, RelayDecision::Forward(payload));
    }

    #[test]
    fn raw_non_json_payload_relays_original_bytes_verbatim() {
        let payload = b"not json at all".to_vec();
        let d = engine().decide(Direction::Uplink, "ecv1/gw-01/c/main/data/blob", &payload);
        assert_eq!(d, RelayDecision::Forward(payload));
    }

    #[test]
    fn downlink_cmd_gets_the_hop_tag_too() {
        let d = engine().decide(
            Direction::Downlink,
            "ecv1/gw-01/opcua-adapter/main/cmd/ping",
            &envelope(&[]),
        );
        let RelayDecision::Forward(bytes) = d else {
            panic!("expected forward")
        };
        assert_eq!(forwarded_hops(&bytes), vec![HOP.to_string()]);
    }

    #[test]
    fn stamp_hop_is_a_noop_for_raw_messages() {
        // The reply back-haul calls stamp_hop directly; a raw reply has nothing
        // to stamp and must pass through untouched.
        let mut msg = Message::raw(json!({ "v": 1 }));
        engine().stamp_hop(&mut msg).unwrap();
        assert!(
            msg.tags.is_none(),
            "a raw message must not grow a tags member"
        );
    }

    #[test]
    fn malformed_envelope_is_dropped() {
        // Valid JSON, claims to be an envelope, but the header is not an object.
        let payload = serde_json::to_vec(&json!({ "header": 42, "body": {} })).unwrap();
        let d = engine().decide(Direction::Uplink, "ecv1/gw-01/c/main/state", &payload);
        assert_eq!(d, RelayDecision::Drop(DropReason::MalformedEnvelope));
    }
}
