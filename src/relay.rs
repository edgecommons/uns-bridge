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
//! - **Hop tag** (`tags._relay`, a protobuf envelope tag whose diagnostic JSON
//!   projection is an array of `{device}/uns-bridge` hop ids): drop-if-self
//!   (own echo), drop when `maxHops` is reached, else append own id.
//! - Normal EdgeCommons messages are protobuf bytes. Foreign or malformed
//!   payloads are dropped by this engine; opaque application bytes belong inside
//!   the protobuf envelope's opaque body lane.
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
    /// Republish these protobuf bytes to the same topic on the other connection.
    /// The bytes carry the appended hop tag, with the selected body lane preserved.
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
    /// Bytes that are not a valid EdgeCommons protobuf envelope.
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
    downlink_filters: Vec<String>,
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
        // D-U28: the instance token is optional, so a fleet relay must subscribe to BOTH the
        // instance-scope wildcard (`ecv1/+/+/+/{class}`) AND the component-scope wildcard
        // (`ecv1/+/+/{class}`) for every class — otherwise it silently drops all component-scope
        // traffic. The two filters are disjoint (an instance is never a class token), so a
        // delivery is never double-counted.
        let mut uplink_filters = Vec::with_capacity((UPLINK_CLASSES.len() + 1) * 2);
        for cls in UPLINK_CLASSES {
            uplink_filters.push((cls, uns.filter_scoped(cls, &all, true)?));
            uplink_filters.push((cls, uns.filter_scoped(cls, &all, false)?));
        }
        if app_enabled {
            uplink_filters.push((UnsClass::App, uns.filter_scoped(UnsClass::App, &all, true)?));
            uplink_filters.push((UnsClass::App, uns.filter_scoped(UnsClass::App, &all, false)?));
        }
        // Downlink `cmd`, pinned to this bridge's own device, at both scopes.
        let device_scope = UnsScope::device(device.clone());
        let downlink_filters = vec![
            uns.filter_scoped(UnsClass::Cmd, &device_scope, true)?,
            uns.filter_scoped(UnsClass::Cmd, &device_scope, false)?,
        ];

        // The §2.5 reconnect-rehydration broadcast topics
        // (`ecv1/{device}/_bcast/cmd/republish-*`) — built through the
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
            downlink_filters,
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

    /// The downlink subscription filters, pinned to this bridge's own device, at both D-U28
    /// scopes: `ecv1/{device}/+/+/cmd/#` (instance) and `ecv1/{device}/+/cmd/#` (component). The
    /// `+` component position also covers `_bcast`.
    pub fn downlink_filters(&self) -> &[String] {
        &self.downlink_filters
    }

    /// The two device-bus `_bcast` topics published at the site-reconnect rising
    /// edge (§2.5 / DESIGN-uns §9.3 layer 2), in [`REHYDRATION_CMDS`] order:
    /// `ecv1/{device}/_bcast/cmd/republish-state` and `…/republish-cfg`.
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
        if segments.len() < 4 || segments[0] != Uns::ROOT {
            return RelayDecision::Drop(DropReason::NotUnsTopic);
        }
        // D-U28: the instance token is optional, so the class sits either directly after
        // {component} (component scope: ecv1/{device}/{component}/{class}, index 3) or after an
        // instance token (instance scope: ecv1/{device}/{component}/{instance}/{class}, index 4).
        // Locate it by the class-token set, never a fixed position — an instance is never a
        // reserved class token, so `segments[3]` being a class token unambiguously means the
        // class is there (component scope).
        let class_index = if UnsClass::from_token(segments[3]).is_some() { 3 } else { 4 };
        let Some(class) = segments.get(class_index).and_then(|t| UnsClass::from_token(t)) else {
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
        if let Err(reason) = self.stamp_hop(&mut msg) {
            return RelayDecision::Drop(reason);
        }

        // 3. Re-serialize as protobuf (structurally identical envelope + the
        //    appended hop tag; the body lane remains protobuf, including opaque bytes).
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
    /// (rule 3), creating the `tags`/`_relay` members as needed. A raw diagnostic
    /// message is an `Ok` no-op; non-protobuf payloads are not normal EdgeCommons
    /// wire data.
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
    use edgecommons::messaging::message::MessageBodyCase;
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
        let msg = Message::from_slice(bytes).unwrap();
        msg.tags.unwrap().extra[RELAY_TAG]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| h.as_str().unwrap().to_string())
            .collect()
    }

    // ---- filter construction (built via the library, §2.2) ----

    #[test]
    fn uplink_filters_cover_both_d_u28_scopes_per_class() {
        let e = engine();
        let filters: Vec<&str> = e
            .uplink_subscriptions()
            .iter()
            .map(|(_, f)| f.as_str())
            .collect();
        // D-U28: each class subscribes at BOTH the instance scope (`ecv1/+/+/+/{class}`) and the
        // component scope (`ecv1/+/+/{class}`) — otherwise component-scope traffic is dropped.
        assert_eq!(
            filters,
            vec![
                "ecv1/+/+/+/state", "ecv1/+/+/state",
                "ecv1/+/+/+/cfg", "ecv1/+/+/cfg",
                "ecv1/+/+/+/evt/#", "ecv1/+/+/evt/#",
                "ecv1/+/+/+/metric/#", "ecv1/+/+/metric/#",
                "ecv1/+/+/+/data/#", "ecv1/+/+/data/#",
                "ecv1/+/+/+/log/#", "ecv1/+/+/log/#",
            ]
        );
    }

    #[test]
    fn app_opt_in_adds_the_seventh_class_at_both_scopes() {
        let e = RelayEngine::new(DEVICE, DEFAULT_MAX_HOPS, true).unwrap();
        let filters: Vec<&str> = e
            .uplink_subscriptions()
            .iter()
            .map(|(_, f)| f.as_str())
            .collect();
        assert_eq!(filters.len(), 14); // 7 classes x 2 scopes
        assert_eq!(&filters[12..], &["ecv1/+/+/+/app/#", "ecv1/+/+/app/#"]);
    }

    #[test]
    fn downlink_filters_are_pinned_to_own_device_at_both_scopes() {
        assert_eq!(
            engine().downlink_filters(),
            &["ecv1/gw-01/+/+/cmd/#".to_string(), "ecv1/gw-01/+/cmd/#".to_string()]
        );
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
                "ecv1/gw-01/_bcast/cmd/republish-state".to_string(),
                "ecv1/gw-01/_bcast/cmd/republish-cfg".to_string(),
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
        // The class disjointness is the structural loop guard for command relays.
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
    fn relays_component_scope_topics_d_u28() {
        // D-U28: a component-scope topic omits the instance token, so the class sits directly
        // after {component} (index 3: ecv1/{device}/{component}/{class}). The old fixed index-4
        // parse required >= 5 segments and dropped every 4-segment component-scope topic as
        // NotUnsTopic. Both scopes must now route.
        let e = engine();
        // uplink, component scope (4 segments, class at index 3)
        assert!(matches!(
            e.decide(Direction::Uplink, "ecv1/gw-01/opcua-adapter/state", &envelope(&[])),
            RelayDecision::Forward(_)
        ));
        // uplink, instance scope still works (main is now an ordinary instance token)
        assert!(matches!(
            e.decide(Direction::Uplink, "ecv1/gw-01/opcua-adapter/inst-7/state", &envelope(&[])),
            RelayDecision::Forward(_)
        ));
        // downlink, component-scope command to own device
        assert!(matches!(
            e.decide(Direction::Downlink, "ecv1/gw-01/opcua-adapter/cmd/reload-config", &envelope(&[])),
            RelayDecision::Forward(_)
        ));
        // downlink, component-scope broadcast rehydration
        assert!(matches!(
            e.decide(Direction::Downlink, "ecv1/gw-01/_bcast/cmd/republish-state", &envelope(&[])),
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
                "ecv1/gw-01/_bcast/cmd/republish-state",
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
        let input = Message::from_slice(&bytes).unwrap();
        assert!(input.tags.is_none(), "precondition: no tags member");
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

    // ---- envelope fidelity + malformed handling ----

    #[test]
    fn relay_preserves_decoded_message_except_the_hop_tag() {
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
        assert!(
            serde_json::from_slice::<Value>(&out).is_err(),
            "relayed wire payload must remain protobuf, not JSON"
        );
        let mut input = serde_json::to_value(Message::from_slice(&bytes).unwrap()).unwrap();
        let output = serde_json::to_value(Message::from_slice(&out).unwrap()).unwrap();
        // Diagnostic projection is identical once the hop tag is added to the input.
        input["tags"][RELAY_TAG] = json!([HOP]);
        assert_eq!(output, input);
    }

    #[test]
    fn foreign_json_payload_is_dropped() {
        let payload = serde_json::to_vec(&json!({ "temperature": 21.5 })).unwrap();
        let d = engine().decide(Direction::Uplink, "ecv1/gw-01/c/main/data/temp", &payload);
        assert_eq!(d, RelayDecision::Drop(DropReason::MalformedEnvelope));
    }

    #[test]
    fn foreign_non_json_payload_is_dropped() {
        let payload = b"not json at all".to_vec();
        let d = engine().decide(Direction::Uplink, "ecv1/gw-01/c/main/data/blob", &payload);
        assert_eq!(d, RelayDecision::Drop(DropReason::MalformedEnvelope));
    }

    #[test]
    fn opaque_body_bytes_survive_hop_tagging() {
        let body = [0x00, 0x01, 0xfe, 0xff, 0x42];
        let bytes = MessageBuilder::new("frame-preview", "1.0")
            .opaque_payload(body, "application/octet-stream")
            .unwrap()
            .tag("site", json!("dallas"))
            .build()
            .to_vec()
            .unwrap();
        let RelayDecision::Forward(out) =
            engine().decide(Direction::Uplink, "ecv1/gw-01/cam/main/data/frame", &bytes)
        else {
            panic!("expected forward")
        };
        let decoded = Message::from_slice(&out).unwrap();
        assert_eq!(decoded.body_case(), MessageBodyCase::Opaque);
        assert_eq!(decoded.opaque_body().unwrap().unwrap(), body);
        assert_eq!(
            decoded.content_type.as_deref(),
            Some("application/octet-stream")
        );
        let tags = decoded.tags.unwrap().extra;
        assert_eq!(tags.get("site"), Some(&json!("dallas")));
        assert_eq!(tags.get(RELAY_TAG), Some(&json!([HOP])));
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
    fn stamp_hop_is_a_noop_for_diagnostic_raw_messages() {
        // Raw Message values can still exist as diagnostic/local values, but they
        // are not normal EdgeCommons wire data.
        let mut msg = Message::raw(json!({ "v": 1 }));
        engine().stamp_hop(&mut msg).unwrap();
        assert!(
            msg.tags.is_none(),
            "a diagnostic Message value with no header must not grow a tags member"
        );
    }

    #[test]
    fn malformed_envelope_is_dropped() {
        // JSON text is not a protobuf EdgeCommonsMessage on the wire.
        let payload = serde_json::to_vec(&json!({ "header": 42, "body": {} })).unwrap();
        let d = engine().decide(Direction::Uplink, "ecv1/gw-01/c/main/state", &payload);
        assert_eq!(d, RelayDecision::Drop(DropReason::MalformedEnvelope));
    }
}
