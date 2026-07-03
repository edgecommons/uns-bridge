//! # observability — the bridge's own observability (§2.8, P3-4b): pure pieces
//!
//! **One-liner purpose**: The **pure, IO-free** halves of the bridge's own
//! observability: the counter→metric mapping (a [`RelaySnapshot`] pair →
//! named measure/value groups for `gg.metrics()`, the §2.5 metric table) and the
//! §2.6/§3.7 **D-B11 LWT startup cross-check** (configured site LWT topic vs the
//! bridge's real state topic).
//!
//! (Section references are to `docs/platform/DESIGN-uns-bridge.md` in the ggcommons
//! monorepo.)
//!
//! The IO halves live elsewhere: [`crate::io`] snapshots the live
//! [`crate::io::RelayCounters`] and emits the groups through the ggcommons
//! `MetricService` on a fixed cadence; [`crate::main`] runs the LWT cross-check at
//! startup (WARN on mismatch — advisory, config stays authoritative, D-B11).
//!
//! ## The metric mapping (§2.5 table + the P3-4b additions)
//!
//! | Metric name | Measures | Kind |
//! |---|---|---|
//! | `relay_uplinked` | per class (`state` … `app`) | counter (interval delta) |
//! | `relay_dropped_disabled` / `relay_dropped_rate` / `relay_dropped_disconnected` | per class | counter (interval delta) |
//! | `relay_downlinked`, `relay_loop_dropped`, `relay_routed_dropped`, `relay_malformed_dropped`, `relay_publish_failed`, `relay_reply_relayed`, `relay_reply_expired`, `relay_reply_stray`, `relay_evt_buffered`, `relay_evt_buffer_dropped`, `relay_evt_replayed` | `count` | counter (interval delta) |
//! | `relay_pending_replies` | `count` | gauge (current) |
//! | `site_connected` | `connected` | gauge (0/1) |
//!
//! Counters emit **interval deltas** (§2.5: "per class, per interval" — deltas sum
//! correctly in CloudWatch/EMF); gauges emit the current value. The mapping is a
//! pure function of two snapshots (previous, current), so it unit-tests with no
//! live infra.

use std::collections::HashMap;

use crate::policy::POLICY_CLASSES;

/// The per-class measure names, in [`POLICY_CLASSES`] order (pinned by a test).
pub const CLASS_MEASURES: [&str; POLICY_CLASSES.len()] =
    ["state", "cfg", "evt", "metric", "data", "log", "app"];

/// The single-measure counter/gauge measure name.
pub const COUNT_MEASURE: &str = "count";
/// The `site_connected` gauge's measure name.
pub const CONNECTED_MEASURE: &str = "connected";

/// The `relay_pending_replies` gauge (§2.5 table).
pub const PENDING_REPLIES_METRIC: &str = "relay_pending_replies";
/// The `site_connected` 0/1 gauge (§2.5 table).
pub const SITE_CONNECTED_METRIC: &str = "site_connected";

/// A point-in-time copy of every relay counter plus the two gauges — the pure
/// input to [`relay_metric_groups`]. Taken by [`crate::io`] from the live
/// [`crate::io::RelayCounters`] (+ the correlation-map size and the site
/// `connected()` signal).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RelaySnapshot {
    /// Messages relayed device → site, per class (`relay_uplinked`).
    pub uplinked: [u64; POLICY_CLASSES.len()],
    /// Uplink drops: class disabled, per class (`relay_dropped_disabled`).
    pub dropped_disabled: [u64; POLICY_CLASSES.len()],
    /// Uplink drops: over the token bucket, per class (`relay_dropped_rate`).
    pub dropped_rate: [u64; POLICY_CLASSES.len()],
    /// Uplink drops: site link down / publish failed, per class
    /// (`relay_dropped_disconnected`).
    pub dropped_disconnected: [u64; POLICY_CLASSES.len()],
    /// Commands relayed site → device (`relay_downlinked`).
    pub downlinked: u64,
    /// Hop-tag guard drops (`relay_loop_dropped`).
    pub loop_dropped: u64,
    /// Class-routing / device-pinning / non-UNS drops (`relay_routed_dropped`).
    pub routed_dropped: u64,
    /// Malformed-envelope drops (`relay_malformed_dropped`).
    pub malformed_dropped: u64,
    /// Failed transport republishes (`relay_publish_failed`).
    pub publish_failed: u64,
    /// Replies relayed through the correlation map (`relay_reply_relayed`).
    pub reply_relayed: u64,
    /// Correlation entries torn down unresolved (`relay_reply_expired`).
    pub reply_expired: u64,
    /// Replies with no live correlation entry (`relay_reply_stray`).
    pub reply_stray: u64,
    /// `evt` pushed into the D-B10 replay buffer (`relay_evt_buffered`).
    pub evt_buffered: u64,
    /// `evt` evicted from the full replay buffer (`relay_evt_buffer_dropped`).
    pub evt_buffer_dropped: u64,
    /// Buffered `evt` replayed after reconnect (`relay_evt_replayed`).
    pub evt_replayed: u64,
    /// GAUGE: in-flight correlation entries (`relay_pending_replies`).
    pub pending_replies: u64,
    /// GAUGE: the site connection state (`site_connected`, 0/1).
    pub site_connected: bool,
}

/// A snapshot accessor for one per-class counter family.
type ClassFamilyField = fn(&RelaySnapshot) -> [u64; POLICY_CLASSES.len()];
/// A snapshot accessor for one scalar counter.
type ScalarField = fn(&RelaySnapshot) -> u64;

/// The four per-class counter families: metric name + snapshot accessor.
const CLASS_FAMILIES: [(&str, ClassFamilyField); 4] = [
    ("relay_uplinked", |s| s.uplinked),
    ("relay_dropped_disabled", |s| s.dropped_disabled),
    ("relay_dropped_rate", |s| s.dropped_rate),
    ("relay_dropped_disconnected", |s| s.dropped_disconnected),
];

/// The scalar counters: metric name + snapshot accessor (measure = `count`).
const SCALAR_COUNTERS: [(&str, ScalarField); 11] = [
    ("relay_downlinked", |s| s.downlinked),
    ("relay_loop_dropped", |s| s.loop_dropped),
    ("relay_routed_dropped", |s| s.routed_dropped),
    ("relay_malformed_dropped", |s| s.malformed_dropped),
    ("relay_publish_failed", |s| s.publish_failed),
    ("relay_reply_relayed", |s| s.reply_relayed),
    ("relay_reply_expired", |s| s.reply_expired),
    ("relay_reply_stray", |s| s.reply_stray),
    ("relay_evt_buffered", |s| s.evt_buffered),
    ("relay_evt_buffer_dropped", |s| s.evt_buffer_dropped),
    ("relay_evt_replayed", |s| s.evt_replayed),
];

/// One metric definition for `gg.metrics().define_metric` (name, its measure
/// names, and the CloudWatch unit).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricDef {
    /// The metric name (becomes the UNS `metric` channel token).
    pub name: &'static str,
    /// The measure names carried by this metric.
    pub measures: Vec<&'static str>,
    /// The CloudWatch unit (`Count` for counters/gauges, `None` for 0/1 flags).
    pub unit: &'static str,
}

/// One emission: the metric name and its measure values — the exact
/// `emit_metric(name, values)` arguments.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricGroup {
    /// The metric name (matches a [`metric_definitions`] entry).
    pub name: &'static str,
    /// Measure name → value.
    pub values: HashMap<String, f64>,
}

/// Every metric the bridge defines, in emission order — the input to
/// `define_metric` at task start. Names and measures are the single source shared
/// with [`relay_metric_groups`] (pinned by a test).
pub fn metric_definitions() -> Vec<MetricDef> {
    let mut defs = Vec::with_capacity(CLASS_FAMILIES.len() + SCALAR_COUNTERS.len() + 2);
    for (name, _) in CLASS_FAMILIES {
        defs.push(MetricDef { name, measures: CLASS_MEASURES.to_vec(), unit: "Count" });
    }
    for (name, _) in SCALAR_COUNTERS {
        defs.push(MetricDef { name, measures: vec![COUNT_MEASURE], unit: "Count" });
    }
    defs.push(MetricDef {
        name: PENDING_REPLIES_METRIC,
        measures: vec![COUNT_MEASURE],
        unit: "Count",
    });
    defs.push(MetricDef {
        name: SITE_CONNECTED_METRIC,
        measures: vec![CONNECTED_MEASURE],
        unit: "None",
    });
    defs
}

/// The pure counter→metric mapping (§2.5 table): counters become **interval
/// deltas** (`curr − prev`, saturating — a restarted counter never yields a
/// negative delta), gauges the current value. One [`MetricGroup`] per metric
/// name, in [`metric_definitions`] order.
pub fn relay_metric_groups(prev: &RelaySnapshot, curr: &RelaySnapshot) -> Vec<MetricGroup> {
    let mut groups = Vec::with_capacity(CLASS_FAMILIES.len() + SCALAR_COUNTERS.len() + 2);
    for (name, field) in CLASS_FAMILIES {
        let (p, c) = (field(prev), field(curr));
        let values = CLASS_MEASURES
            .iter()
            .zip(c.iter().zip(p.iter()))
            .map(|(measure, (c, p))| ((*measure).to_string(), c.saturating_sub(*p) as f64))
            .collect();
        groups.push(MetricGroup { name, values });
    }
    for (name, field) in SCALAR_COUNTERS {
        let delta = field(curr).saturating_sub(field(prev)) as f64;
        groups.push(MetricGroup {
            name,
            values: HashMap::from([(COUNT_MEASURE.to_string(), delta)]),
        });
    }
    groups.push(MetricGroup {
        name: PENDING_REPLIES_METRIC,
        values: HashMap::from([(COUNT_MEASURE.to_string(), curr.pending_replies as f64)]),
    });
    groups.push(MetricGroup {
        name: SITE_CONNECTED_METRIC,
        values: HashMap::from([(
            CONNECTED_MEASURE.to_string(),
            if curr.site_connected { 1.0 } else { 0.0 },
        )]),
    });
    groups
}

/// The verdict of the §2.6/§3.7 **D-B11 LWT startup cross-check**: does the
/// configured site-connection LWT topic equal the bridge's real state topic
/// (`ecv1/{device}/uns-bridge/main/state` — what the heartbeat keepalive
/// actually publishes on)? Advisory only — config stays authoritative.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LwtCrossCheck {
    /// The configured LWT topic equals the bridge's real state topic.
    Match,
    /// The classic misconfig (§2.6): a typo'd / unresolved / unsanitized topic —
    /// the broker would publish `UNREACHABLE` where no consumer listens.
    Mismatch {
        /// The (template-resolved) configured `lwt.topic`.
        configured: String,
        /// The bridge's real state topic.
        expected: String,
    },
    /// No site LWT configured at all — the site cannot detect whole-device
    /// unreachability (the load-bearing D9/§9.3 LWT use is missing).
    NotConfigured {
        /// The topic a site LWT should be configured with.
        expected: String,
    },
}

/// Pure D-B11 cross-check: compare the configured site `lwt.topic` (already
/// template-resolved by the caller) against the bridge's real state topic.
pub fn check_lwt_topic(configured: Option<&str>, expected: &str) -> LwtCrossCheck {
    match configured {
        None => LwtCrossCheck::NotConfigured { expected: expected.to_string() },
        Some(topic) if topic == expected => LwtCrossCheck::Match,
        Some(topic) => LwtCrossCheck::Mismatch {
            configured: topic.to_string(),
            expected: expected.to_string(),
        },
    }
}

/// Log the [`check_lwt_topic`] verdict (§2.6: **WARN on mismatch**, do not fail —
/// the check is advisory and config remains authoritative; a missing LWT also
/// WARNs because whole-device reachability detection then does not work).
pub fn log_lwt_cross_check(check: &LwtCrossCheck) {
    match check {
        LwtCrossCheck::Match => {
            tracing::info!("site LWT topic matches the bridge's state topic (D-B11 cross-check)");
        }
        LwtCrossCheck::Mismatch { configured, expected } => {
            tracing::warn!(
                configured = %configured,
                expected = %expected,
                "site LWT topic does NOT match the bridge's real state topic — the broker \
                 would publish UNREACHABLE where no consumer listens; fix \
                 component.instances[site].lwt.topic (advisory, D-B11 — config stays \
                 authoritative)"
            );
        }
        LwtCrossCheck::NotConfigured { expected } => {
            tracing::warn!(
                expected = %expected,
                "no site LWT configured — the site broker cannot signal whole-device \
                 UNREACHABLE (§2.6/D-B11); add component.instances[site].lwt with this topic"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A snapshot with a unique value in every field, so a wrong accessor in the
    /// mapping tables cannot go unnoticed.
    fn distinct() -> RelaySnapshot {
        RelaySnapshot {
            uplinked: [101, 102, 103, 104, 105, 106, 107],
            dropped_disabled: [201, 202, 203, 204, 205, 206, 207],
            dropped_rate: [301, 302, 303, 304, 305, 306, 307],
            dropped_disconnected: [401, 402, 403, 404, 405, 406, 407],
            downlinked: 501,
            loop_dropped: 502,
            routed_dropped: 503,
            malformed_dropped: 504,
            publish_failed: 505,
            reply_relayed: 506,
            reply_expired: 507,
            reply_stray: 508,
            evt_buffered: 509,
            evt_buffer_dropped: 510,
            evt_replayed: 511,
            pending_replies: 512,
            site_connected: true,
        }
    }

    fn group<'a>(groups: &'a [MetricGroup], name: &str) -> &'a MetricGroup {
        groups.iter().find(|g| g.name == name).unwrap_or_else(|| panic!("missing {name}"))
    }

    // ---- the counter→metric mapping (§2.5 table) ----

    #[test]
    fn class_measures_match_the_policy_class_tokens() {
        let tokens: Vec<&str> = POLICY_CLASSES.iter().map(|c| c.token()).collect();
        assert_eq!(tokens, CLASS_MEASURES.to_vec(), "measure order must be POLICY_CLASSES order");
    }

    #[test]
    fn every_snapshot_field_maps_to_a_group_with_the_design_names() {
        let groups = relay_metric_groups(&RelaySnapshot::default(), &distinct());
        let names: Vec<&str> = groups.iter().map(|g| g.name).collect();
        assert_eq!(
            names,
            vec![
                "relay_uplinked",
                "relay_dropped_disabled",
                "relay_dropped_rate",
                "relay_dropped_disconnected",
                "relay_downlinked",
                "relay_loop_dropped",
                "relay_routed_dropped",
                "relay_malformed_dropped",
                "relay_publish_failed",
                "relay_reply_relayed",
                "relay_reply_expired",
                "relay_reply_stray",
                "relay_evt_buffered",
                "relay_evt_buffer_dropped",
                "relay_evt_replayed",
                "relay_pending_replies",
                "site_connected",
            ]
        );

        // Per-class families carry the seven class measures with the right values.
        let up = group(&groups, "relay_uplinked");
        assert_eq!(up.values.len(), 7);
        assert_eq!(up.values["state"], 101.0);
        assert_eq!(up.values["app"], 107.0);
        assert_eq!(group(&groups, "relay_dropped_disabled").values["cfg"], 202.0);
        assert_eq!(group(&groups, "relay_dropped_rate").values["evt"], 303.0);
        assert_eq!(group(&groups, "relay_dropped_disconnected").values["log"], 406.0);

        // Scalars land on the `count` measure with the right (distinct) values.
        for (name, want) in [
            ("relay_downlinked", 501.0),
            ("relay_loop_dropped", 502.0),
            ("relay_routed_dropped", 503.0),
            ("relay_malformed_dropped", 504.0),
            ("relay_publish_failed", 505.0),
            ("relay_reply_relayed", 506.0),
            ("relay_reply_expired", 507.0),
            ("relay_reply_stray", 508.0),
            ("relay_evt_buffered", 509.0),
            ("relay_evt_buffer_dropped", 510.0),
            ("relay_evt_replayed", 511.0),
        ] {
            let g = group(&groups, name);
            assert_eq!(g.values.len(), 1, "{name} carries exactly the count measure");
            assert_eq!(g.values[COUNT_MEASURE], want, "{name}");
        }

        // Gauges: current values, not deltas.
        assert_eq!(group(&groups, PENDING_REPLIES_METRIC).values[COUNT_MEASURE], 512.0);
        assert_eq!(group(&groups, SITE_CONNECTED_METRIC).values[CONNECTED_MEASURE], 1.0);
    }

    #[test]
    fn counters_emit_interval_deltas_and_gauges_emit_current() {
        let mut prev = distinct();
        let mut curr = distinct();
        curr.uplinked[0] += 5; // state
        curr.downlinked += 3;
        curr.pending_replies = 9; // gauge: absolute
        curr.site_connected = false;
        prev.site_connected = true;

        let groups = relay_metric_groups(&prev, &curr);
        assert_eq!(group(&groups, "relay_uplinked").values["state"], 5.0);
        assert_eq!(group(&groups, "relay_uplinked").values["cfg"], 0.0, "unchanged → 0 delta");
        assert_eq!(group(&groups, "relay_downlinked").values[COUNT_MEASURE], 3.0);
        assert_eq!(
            group(&groups, PENDING_REPLIES_METRIC).values[COUNT_MEASURE],
            9.0,
            "gauge is the CURRENT value, not a delta"
        );
        assert_eq!(group(&groups, SITE_CONNECTED_METRIC).values[CONNECTED_MEASURE], 0.0);
    }

    #[test]
    fn deltas_saturate_instead_of_underflowing() {
        // A counter that appears to go backwards (process restart) must never
        // produce a negative/underflowed delta.
        let groups = relay_metric_groups(&distinct(), &RelaySnapshot::default());
        assert_eq!(group(&groups, "relay_downlinked").values[COUNT_MEASURE], 0.0);
        assert_eq!(group(&groups, "relay_uplinked").values["state"], 0.0);
    }

    #[test]
    fn definitions_and_groups_agree_on_names_measures_and_order() {
        let defs = metric_definitions();
        let groups = relay_metric_groups(&RelaySnapshot::default(), &RelaySnapshot::default());
        assert_eq!(defs.len(), groups.len());
        for (def, group) in defs.iter().zip(groups.iter()) {
            assert_eq!(def.name, group.name);
            let mut defined: Vec<&str> = def.measures.clone();
            let mut emitted: Vec<&str> = group.values.keys().map(String::as_str).collect();
            defined.sort_unstable();
            emitted.sort_unstable();
            assert_eq!(defined, emitted, "{}: defined measures == emitted measures", def.name);
        }
        // Units: 0/1 flag is `None`, everything else counts.
        for def in &defs {
            let want = if def.name == SITE_CONNECTED_METRIC { "None" } else { "Count" };
            assert_eq!(def.unit, want, "{}", def.name);
        }
    }

    // ---- the D-B11 LWT startup cross-check (§2.6/§3.7) ----

    const EXPECTED: &str = "ecv1/gw-01/uns-bridge/main/state";

    #[test]
    fn lwt_cross_check_matches_the_real_state_topic() {
        let check = check_lwt_topic(Some(EXPECTED), EXPECTED);
        assert_eq!(check, LwtCrossCheck::Match);
        log_lwt_cross_check(&check); // the INFO path
    }

    #[test]
    fn lwt_cross_check_flags_a_mismatch_for_the_warn_path() {
        // The classic misconfig: an unresolved template / unsanitized literal.
        let check = check_lwt_topic(Some("ecv1/{ThingName}/uns-bridge/main/state"), EXPECTED);
        assert_eq!(
            check,
            LwtCrossCheck::Mismatch {
                configured: "ecv1/{ThingName}/uns-bridge/main/state".to_string(),
                expected: EXPECTED.to_string(),
            }
        );
        log_lwt_cross_check(&check); // the WARN path (advisory — never fails)
    }

    #[test]
    fn lwt_cross_check_flags_a_missing_lwt() {
        let check = check_lwt_topic(None, EXPECTED);
        assert_eq!(check, LwtCrossCheck::NotConfigured { expected: EXPECTED.to_string() });
        log_lwt_cross_check(&check); // the WARN path
    }

    // ---- the shipped config: schema-valid + derives the expected state topic ----

    #[test]
    fn shipped_config_passes_the_canonical_schema_and_derives_the_state_topic() {
        use ggcommons::config::model::Config;
        use ggcommons::uns::{Uns, UnsClass};

        let raw: serde_json::Value =
            serde_json::from_str(include_str!("../test-configs/config.json")).unwrap();
        // Pins that `-c FILE test-configs/config.json` passes the runtime's
        // canonical-schema validation (GgCommonsBuilder::build validates).
        ggcommons::config::validation::validate(&raw).expect("canonical schema must accept");

        // And that the runtime identity derives EXACTLY the D-B11 state topic the
        // shipped lwt.topic pins (gg.uns().topic(State) — what main cross-checks).
        let cfg = Config::from_value(crate::COMPONENT_NAME, "gw-01", raw).unwrap();
        let uns = Uns::new(cfg.identity().clone(), cfg.topic_include_root());
        let expected = uns.topic(UnsClass::State).unwrap();
        assert_eq!(expected, EXPECTED);
        assert_eq!(check_lwt_topic(Some(EXPECTED), &expected), LwtCrossCheck::Match);
    }
}
