//! # reply — the `reply_to` rewrite: the TTL'd correlation map (§2.4 / D-B9)
//!
//! **One-liner purpose**: Make request/reply survive the bridge — mint a
//! bridge-side reply topic per downlinked `cmd` that carries `header.reply_to`,
//! remember `bridge topic → original site reply topic` in a TTL'd, bounded map,
//! and turn a reply arriving on the bridge topic back into a publish on the
//! original site topic.
//!
//! (Section references are to `docs/platform/DESIGN-uns-bridge.md` in the
//! edgecommons monorepo; the decision-register entry is **D-B9**.)
//!
//! ## Why (§2.4)
//! A site-side requester (e.g. the console) sets
//! `header.reply_to = edgecommons/reply-<uuid>` — an ephemeral topic **on the site
//! broker**. Without rewriting, the device-side responder would `reply()` onto the
//! device bus where nobody is subscribed. The bridge therefore proxies the reply
//! path: rewrite going down, relay back going up.
//!
//! ## Semantics
//! - **Downlink rewrite** ([`ReplyCorrelator::rewrite_downlink`]) decodes the
//!   already-relayed protobuf envelope and happens only when `header.reply_to`
//!   is present. A `cmd` without it is a notification-style command (normative
//!   per the canonical doc §4.3) and relays untouched.
//! - The minted topic uses the core's standard `edgecommons/reply-` prefix
//!   ([`new_reply_topic`]), so it is structurally exempt from the reserved-class
//!   guard (D-U6) and indistinguishable from any other reply topic to the
//!   responder.
//! - **The reply relays with protobuf decode/mutate/re-encode**
//!   ([`prepare_reply`]) — `correlation_id`, body, identity, and every other tag
//!   untouched. The only changes: the hop tag is appended (§2.4: "it is a relay
//!   like any other"), and `header.reply_to` is dropped — a reply carries none,
//!   and a device-bus topic would be meaningless at the site anyway.
//! - **TTL** (`reply.ttlSecs`, default [`DEFAULT_REPLY_TTL_SECS`] = 2× the
//!   framework's 30 s request-deadline default, `messaging.requestTimeoutSeconds`):
//!   the bridge never tears down a reply path before the requester's own deadline
//!   settles it. Deployments raising `requestTimeoutSeconds` must raise the bridge
//!   TTL in step — a documented **paired knob**.
//! - **Bound** (`reply.maxPending`, default [`DEFAULT_MAX_PENDING`]): on overflow
//!   the *oldest* entry is evicted — expired early, counted as
//!   `relay_reply_expired` — rather than refusing the new command: a stuck
//!   responder must not starve fresh traffic.
//! - Cross-device request/reply stays unsupported in v1 (D-B7): `cmd` is never
//!   uplinked, so the only requests crossing the bridge come from the site side.
//!
//! This module is **pure** — no IO, no tasks, no internal clock (`now` is
//! injected) — mirroring the [`crate::relay`]/[`crate::io`] split. The pumping
//! (the bridge-topic subscription, the reply publish, the periodic TTL sweep)
//! lives in [`crate::io`].

use std::time::{Duration, Instant};

use edgecommons::messaging::message::Message;
use edgecommons::messaging::request_reply::new_reply_topic;

use crate::relay::{DropReason, RelayEngine};

/// Default correlation-entry TTL in seconds (§2.4 / D-B9): **60 s = 2×** the
/// framework's 30 s request-deadline default (`messaging.requestTimeoutSeconds`,
/// `model.rs:46` in the core) — the documented paired knob.
pub const DEFAULT_REPLY_TTL_SECS: u64 = 60;

/// Default bound on in-flight correlation entries (§2.4 / D-B9). Overflow evicts
/// the **oldest** entry.
pub const DEFAULT_MAX_PENDING: usize = 1024;

/// Sweep-cadence ceiling: the §2.4 `min(ttl/4, 5 s)` cap.
const SWEEP_MAX: Duration = Duration::from_secs(5);

/// Sweep-cadence floor, so a pathological `ttlSecs: 0` cannot busy-spin the
/// sweep task.
const SWEEP_MIN: Duration = Duration::from_millis(100);

/// One in-flight request crossing the bridge: where its reply goes back to.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Pending {
    /// The requester's original site-side `reply_to` topic.
    site_reply_to: String,
    /// The request's correlation id — diagnostics only (correlation itself
    /// survives inside the relayed envelope, untouched).
    correlation_id: String,
    /// When this entry expires (record time + TTL); expired when `deadline <= now`.
    deadline: Instant,
}

/// What [`ReplyCorrelator::rewrite_downlink`] decided for one forwardable `cmd`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownlinkRewrite {
    /// No `header.reply_to`: relay the input protobuf bytes unchanged, as a
    /// fire-and-forget notification with no reply path to proxy.
    Passthrough,
    /// `header.reply_to` was rewritten to a freshly minted bridge-side topic and
    /// a correlation entry recorded. The caller must subscribe `bridge_topic` on
    /// the DEVICE bus **before** publishing `bytes` (§2.4 sequence), and
    /// unsubscribe + count-expired `evicted` when present.
    Rewritten {
        /// The re-serialized envelope carrying the rewritten `reply_to`.
        bytes: Vec<u8>,
        /// The bridge-minted `edgecommons/reply-<uuid>` topic (the new map key).
        bridge_topic: String,
        /// The oldest entry's bridge topic, evicted because the map was at
        /// `maxPending` (§2.4 evict-oldest): expire it early — unsubscribe it and
        /// count `relay_reply_expired`.
        evicted: Option<String>,
    },
    /// Do not relay this downlink command.
    Drop(DropReason),
}

/// What the reply back-haul should do with a message that arrived on a **live**
/// bridge-side reply topic (see [`prepare_reply`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplyRelay {
    /// Publish these bytes to the original site `reply_to` topic.
    Forward(Vec<u8>),
    /// Do not relay (hop-rule violation or a malformed envelope).
    Drop(DropReason),
}

/// The §2.4 TTL'd correlation map:
/// `bridge reply topic → (original site reply topic, deadline)`.
///
/// Pure and clock-free (every time-dependent method takes `now`); guard it with a
/// mutex and drive the sweep from a task in the IO layer. One-shot by
/// construction: [`Self::take`] removes the entry it resolves.
#[derive(Debug)]
pub struct ReplyCorrelator {
    ttl: Duration,
    max_pending: usize,
    /// Insertion-ordered: the TTL is uniform, so insertion order **is** deadline
    /// order — front = oldest = first to expire / first to evict. `maxPending`
    /// (≈1024) keeps the linear scans trivially cheap.
    pending: Vec<(String, Pending)>,
}

impl ReplyCorrelator {
    /// `ttl` = `reply.ttlSecs`; `max_pending` = `reply.maxPending`. A
    /// `max_pending` of 0 is treated as 1 — the map must be able to hold the
    /// entry it just recorded.
    pub fn new(ttl: Duration, max_pending: usize) -> ReplyCorrelator {
        ReplyCorrelator {
            ttl,
            max_pending: max_pending.max(1),
            pending: Vec::new(),
        }
    }

    /// Inspect one decided-and-forwardable downlink `cmd` (the bytes
    /// [`RelayEngine::decide`] said to forward, hop tag already stamped): when it
    /// carries `header.reply_to`, mint a bridge-side reply topic, rewrite the
    /// header, and record `bridge topic → original site reply topic` with
    /// deadline `now + ttl` — evicting the oldest entry when the map is full.
    ///
    /// A `cmd` without `reply_to` is [`Passthrough`] (`DownlinkRewrite::Passthrough`)
    /// and relayed exactly as before this slice.
    ///
    /// [`Passthrough`]: DownlinkRewrite::Passthrough
    pub fn rewrite_downlink(&mut self, relayed: &[u8], now: Instant) -> DownlinkRewrite {
        let mut msg = match Message::from_slice(relayed) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "malformed relayed cmd envelope");
                return DownlinkRewrite::Drop(DropReason::MalformedEnvelope);
            }
        };
        let Some(site_reply_to) = msg.header.reply_to.clone() else {
            // Fire-and-forget notification cmd (§2.4) — no reply path to proxy.
            return DownlinkRewrite::Passthrough;
        };

        let bridge_topic = new_reply_topic();
        msg.header.reply_to = Some(bridge_topic.clone());
        let bytes = match msg.to_vec() {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "cannot re-serialize rewritten cmd");
                return DownlinkRewrite::Drop(DropReason::MalformedEnvelope);
            }
        };

        let evicted = if self.pending.len() >= self.max_pending {
            // Evict-oldest (§2.4): a stuck responder must not starve fresh
            // traffic. The caller expires the victim (unsubscribe + count).
            Some(self.pending.remove(0).0)
        } else {
            None
        };
        self.pending.push((
            bridge_topic.clone(),
            Pending {
                site_reply_to,
                correlation_id: msg.header.correlation_id.clone(),
                deadline: now + self.ttl,
            },
        ));
        DownlinkRewrite::Rewritten {
            bytes,
            bridge_topic,
            evicted,
        }
    }

    /// Resolve-and-remove: the original site `reply_to` for `bridge_topic`, or
    /// `None` when no live entry exists (expired / evicted / never recorded — a
    /// **stray** reply). One-shot: a second call for the same topic is `None`.
    pub fn take(&mut self, bridge_topic: &str) -> Option<String> {
        let idx = self.pending.iter().position(|(t, _)| t == bridge_topic)?;
        let (_, entry) = self.pending.remove(idx);
        tracing::debug!(
            topic = %bridge_topic,
            correlation_id = %entry.correlation_id,
            "reply correlation entry resolved"
        );
        Some(entry.site_reply_to)
    }

    /// Whether a live entry exists for `bridge_topic`.
    pub fn contains(&self, bridge_topic: &str) -> bool {
        self.pending.iter().any(|(t, _)| t == bridge_topic)
    }

    /// Expire every entry whose deadline has passed (`deadline <= now`),
    /// returning their bridge topics oldest-first — the caller unsubscribes each
    /// and counts `relay_reply_expired`.
    pub fn sweep(&mut self, now: Instant) -> Vec<String> {
        // Insertion order == deadline order (uniform TTL): the stale entries are
        // exactly a prefix.
        let keep_from = self
            .pending
            .iter()
            .position(|(_, p)| p.deadline > now)
            .unwrap_or(self.pending.len());
        self.pending.drain(..keep_from).map(|(t, _)| t).collect()
    }

    /// Remove `bridge_topic` without resolving it — the caller failed to
    /// establish its reply subscription, so the entry must not linger to expiry.
    pub fn abandon(&mut self, bridge_topic: &str) {
        self.pending.retain(|(t, _)| t != bridge_topic);
    }

    /// Remove and return every bridge topic, oldest-first (shutdown: the
    /// unsubscribe-before-exit rule covers the still-pending reply topics too).
    pub fn drain(&mut self) -> Vec<String> {
        self.pending.drain(..).map(|(t, _)| t).collect()
    }

    /// The number of in-flight entries — the `relay_pending_replies` gauge
    /// (§2.5 metric table; published in P3-4).
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Whether the map holds no in-flight entries.
    #[allow(dead_code)] // the idiomatic len/is_empty pair; exercised by the tests
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// The sweep cadence: `min(ttl/4, 5 s)` (§2.4), floored at 100 ms.
    pub fn sweep_interval(&self) -> Duration {
        (self.ttl / 4).clamp(SWEEP_MIN, SWEEP_MAX)
    }
}

/// Turn a message that arrived on a **live** bridge-side reply topic into the
/// bytes to publish on the original site `reply_to` topic.
///
/// Preserved (§2.4 / D-B9) except for exactly two touches:
/// - `header.reply_to` is dropped — a reply carries none, and the bridge-side
///   device topic would be meaningless at the site;
/// - the hop tag is appended via [`RelayEngine::stamp_hop`] ("it is a relay like
///   any other"), whose rules also apply: an own-echo or over-`maxHops` reply is
///   a [`ReplyRelay::Drop`].
///
/// `correlation_id`, body, identity, and every other tag are untouched.
pub fn prepare_reply(engine: &RelayEngine, payload: &[u8]) -> ReplyRelay {
    let mut msg = match Message::from_slice(payload) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(error = %e, "malformed reply envelope");
            return ReplyRelay::Drop(DropReason::MalformedEnvelope);
        }
    };
    msg.header.reply_to = None;
    if let Err(reason) = engine.stamp_hop(&mut msg) {
        return ReplyRelay::Drop(reason);
    }
    match msg.to_vec() {
        Ok(bytes) => ReplyRelay::Forward(bytes),
        Err(e) => {
            tracing::warn!(error = %e, "failed to re-serialize relayed reply");
            ReplyRelay::Drop(DropReason::MalformedEnvelope)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relay::{RelayEngine, DEFAULT_MAX_HOPS, RELAY_TAG};
    use edgecommons::messaging::message::MessageBodyCase;
    use edgecommons::messaging::request_reply::REPLY_TOPIC_PREFIX;
    use edgecommons::messaging::MessageBuilder;
    use serde_json::{json, Value};

    const SITE_REPLY: &str = "edgecommons/reply-site-original";
    const HOP: &str = "gw-01/uns-bridge";

    fn correlator(ttl_secs: u64, max_pending: usize) -> ReplyCorrelator {
        ReplyCorrelator::new(Duration::from_secs(ttl_secs), max_pending)
    }

    fn engine() -> RelayEngine {
        RelayEngine::new("gw-01", DEFAULT_MAX_HOPS, false).unwrap()
    }

    fn cmd(reply_to: Option<&str>) -> Vec<u8> {
        let mut b = MessageBuilder::new("reload-config", "1.0")
            .payload(json!({ "arg": 1 }))
            .correlation_id("corr-9");
        if let Some(r) = reply_to {
            b = b.reply_to(r);
        }
        b.build().to_vec().unwrap()
    }

    fn decoded(bytes: &[u8]) -> Message {
        Message::from_slice(bytes).unwrap()
    }

    fn projected(bytes: &[u8]) -> Value {
        serde_json::to_value(decoded(bytes)).unwrap()
    }

    /// Rewrite `cmd(Some(SITE_REPLY))` and unwrap the `Rewritten` fields.
    fn rewritten(c: &mut ReplyCorrelator, now: Instant) -> (Vec<u8>, String, Option<String>) {
        rewritten_for(c, SITE_REPLY, now)
    }

    fn rewritten_for(
        c: &mut ReplyCorrelator,
        site_reply: &str,
        now: Instant,
    ) -> (Vec<u8>, String, Option<String>) {
        match c.rewrite_downlink(&cmd(Some(site_reply)), now) {
            DownlinkRewrite::Rewritten {
                bytes,
                bridge_topic,
                evicted,
            } => (bytes, bridge_topic, evicted),
            other => panic!("expected Rewritten, got {other:?}"),
        }
    }

    // ---- downlink rewrite (§2.4) ----

    #[test]
    fn cmd_without_reply_to_is_passthrough() {
        let mut c = correlator(60, 1024);
        assert_eq!(
            c.rewrite_downlink(&cmd(None), Instant::now()),
            DownlinkRewrite::Passthrough
        );
        assert!(
            c.is_empty(),
            "a fire-and-forget cmd must not create an entry"
        );
    }

    #[test]
    fn non_protobuf_cmd_is_dropped() {
        let mut c = correlator(60, 1024);
        let raw = serde_json::to_vec(&json!({ "do": "reload" })).unwrap();
        assert_eq!(
            c.rewrite_downlink(&raw, Instant::now()),
            DownlinkRewrite::Drop(DropReason::MalformedEnvelope)
        );
        assert!(c.is_empty());
    }

    #[test]
    fn unparseable_cmd_is_dropped() {
        let mut c = correlator(60, 1024);
        let malformed = serde_json::to_vec(&json!({ "header": 42, "body": {} })).unwrap();
        assert_eq!(
            c.rewrite_downlink(&malformed, Instant::now()),
            DownlinkRewrite::Drop(DropReason::MalformedEnvelope)
        );
        assert!(c.is_empty());
    }

    #[test]
    fn rewrite_mints_bridge_topic_and_records_entry() {
        let mut c = correlator(60, 1024);
        let (bytes, bridge_topic, evicted) = rewritten(&mut c, Instant::now());

        // Bridge topic: the core's standard prefix, distinct from the original.
        assert!(bridge_topic.starts_with(REPLY_TOPIC_PREFIX));
        assert_ne!(bridge_topic, SITE_REPLY);
        assert!(evicted.is_none());

        // The relayed envelope: reply_to rewritten, diagnostic projection otherwise preserved.
        let mut input = projected(&cmd(Some(SITE_REPLY)));
        let output = projected(&bytes);
        input["header"]["reply_to"] = json!(bridge_topic.clone());
        // (uuid/timestamp differ per build — pin them from the output)
        input["header"]["uuid"] = output["header"]["uuid"].clone();
        input["header"]["timestamp"] = output["header"]["timestamp"].clone();
        input["header"]["timestamp_ms"] = output["header"]["timestamp_ms"].clone();
        assert_eq!(output, input, "only header.reply_to may change");
        assert_eq!(output["header"]["correlation_id"], "corr-9");

        // The map entry resolves back to the original site topic — one-shot.
        assert!(c.contains(&bridge_topic));
        assert_eq!(c.len(), 1);
        assert_eq!(c.take(&bridge_topic), Some(SITE_REPLY.to_string()));
        assert!(c.is_empty());
    }

    #[test]
    fn rewrite_downlink_preserves_opaque_body_bytes() {
        let mut c = correlator(60, 1024);
        let opaque = [0xde, 0xad, 0xbe, 0xef];
        let cmd = MessageBuilder::new("send-frame", "1.0")
            .opaque_body(opaque, "application/x-protobuf")
            .unwrap()
            .correlation_id("corr-opaque")
            .reply_to(SITE_REPLY)
            .build()
            .to_vec()
            .unwrap();

        let DownlinkRewrite::Rewritten {
            bytes,
            bridge_topic,
            ..
        } = c.rewrite_downlink(&cmd, Instant::now())
        else {
            panic!("expected rewrite")
        };
        let out = decoded(&bytes);
        assert_eq!(out.header.reply_to.as_deref(), Some(bridge_topic.as_str()));
        assert_eq!(out.header.correlation_id, "corr-opaque");
        assert_eq!(out.body_case(), MessageBodyCase::Opaque);
        assert_eq!(out.content_type.as_deref(), Some("application/x-protobuf"));
        assert_eq!(out.opaque_body().unwrap().unwrap(), opaque);
    }

    #[test]
    fn take_is_one_shot() {
        let mut c = correlator(60, 1024);
        let (_, bridge_topic, _) = rewritten(&mut c, Instant::now());
        assert!(c.take(&bridge_topic).is_some());
        assert_eq!(c.take(&bridge_topic), None, "a second reply is a stray");
        assert_eq!(c.take("edgecommons/reply-never-recorded"), None);
    }

    #[test]
    fn distinct_requests_get_distinct_topics_resolving_to_their_own_site_topic() {
        let mut c = correlator(60, 1024);
        let now = Instant::now();
        let (_, t1, _) = rewritten_for(&mut c, "edgecommons/reply-site-1", now);
        let (_, t2, _) = rewritten_for(&mut c, "edgecommons/reply-site-2", now);
        assert_ne!(t1, t2);
        assert_eq!(c.take(&t2), Some("edgecommons/reply-site-2".to_string()));
        assert_eq!(c.take(&t1), Some("edgecommons/reply-site-1".to_string()));
    }

    // ---- maxPending: evict-oldest (§2.4 / D-B9) ----

    #[test]
    fn overflow_evicts_the_oldest_entry() {
        let mut c = correlator(60, 2);
        let now = Instant::now();
        let (_, t1, e1) = rewritten_for(&mut c, "edgecommons/reply-site-1", now);
        let (_, t2, e2) = rewritten_for(&mut c, "edgecommons/reply-site-2", now);
        assert!(e1.is_none() && e2.is_none());

        let (_, t3, e3) = rewritten_for(&mut c, "edgecommons/reply-site-3", now);
        assert_eq!(
            e3,
            Some(t1.clone()),
            "the OLDEST entry is the eviction victim"
        );
        assert_eq!(c.len(), 2, "the bound holds after eviction");
        assert_eq!(c.take(&t1), None, "the evicted entry is gone");
        assert!(
            c.contains(&t2) && c.contains(&t3),
            "younger entries survive"
        );
    }

    #[test]
    fn max_pending_zero_is_treated_as_one() {
        let mut c = correlator(60, 0);
        let now = Instant::now();
        let (_, t1, e1) = rewritten_for(&mut c, "edgecommons/reply-site-1", now);
        assert!(e1.is_none());
        let (_, _t2, e2) = rewritten_for(&mut c, "edgecommons/reply-site-2", now);
        assert_eq!(e2, Some(t1));
        assert_eq!(c.len(), 1);
    }

    // ---- TTL sweep (§2.4) ----

    #[test]
    fn sweep_expires_only_stale_entries_oldest_first() {
        let mut c = correlator(60, 1024);
        let t0 = Instant::now();
        let (_, old1, _) = rewritten_for(&mut c, "edgecommons/reply-site-1", t0);
        let (_, old2, _) = rewritten_for(&mut c, "edgecommons/reply-site-2", t0);
        let (_, young, _) = rewritten_for(
            &mut c,
            "edgecommons/reply-site-3",
            t0 + Duration::from_secs(30),
        );

        // Just before the first deadline: nothing expires.
        assert!(c.sweep(t0 + Duration::from_secs(59)).is_empty());
        // At/after the first deadline (inclusive: deadline <= now): the two old
        // entries expire, oldest-first; the young one lives on.
        assert_eq!(c.sweep(t0 + Duration::from_secs(60)), vec![old1, old2]);
        assert_eq!(c.len(), 1);
        assert!(c.contains(&young));
        // Past the young entry's deadline too.
        assert_eq!(c.sweep(t0 + Duration::from_secs(91)), vec![young]);
        assert!(c.is_empty());
        assert!(
            c.sweep(t0 + Duration::from_secs(120)).is_empty(),
            "sweeping empty is empty"
        );
    }

    #[test]
    fn abandon_removes_without_resolving() {
        let mut c = correlator(60, 1024);
        let (_, bridge_topic, _) = rewritten(&mut c, Instant::now());
        c.abandon(&bridge_topic);
        assert!(c.is_empty());
        assert_eq!(c.take(&bridge_topic), None);
    }

    #[test]
    fn drain_returns_every_topic_oldest_first() {
        let mut c = correlator(60, 1024);
        let now = Instant::now();
        let (_, t1, _) = rewritten_for(&mut c, "edgecommons/reply-site-1", now);
        let (_, t2, _) = rewritten_for(&mut c, "edgecommons/reply-site-2", now);
        assert_eq!(c.drain(), vec![t1, t2]);
        assert!(c.is_empty());
    }

    #[test]
    fn sweep_interval_is_quarter_ttl_capped_and_floored() {
        assert_eq!(
            correlator(60, 1).sweep_interval(),
            Duration::from_secs(5),
            "min(15s, 5s)"
        );
        assert_eq!(
            correlator(8, 1).sweep_interval(),
            Duration::from_secs(2),
            "ttl/4 below the cap"
        );
        assert_eq!(
            correlator(0, 1).sweep_interval(),
            Duration::from_millis(100),
            "floor"
        );
        assert_eq!(
            correlator(10_000, 1).sweep_interval(),
            Duration::from_secs(5),
            "cap"
        );
    }

    // ---- the reply back-haul transform (prepare_reply) ----

    #[test]
    fn reply_preserves_message_plus_hop_tag_minus_reply_to() {
        let reply = MessageBuilder::new("reload-config-reply", "1.0")
            .payload(json!({ "ok": true, "nested": { "a": [1, 2, 3] } }))
            .tag("site", json!("dallas"))
            .uuid("00000000-0000-4000-8000-000000000002")
            .timestamp("2026-07-03T12:00:00Z")
            .correlation_id("corr-9")
            .reply_to("edgecommons/reply-device-local") // atypical, but must be stripped
            .build()
            .to_vec()
            .unwrap();
        let ReplyRelay::Forward(out) = prepare_reply(&engine(), &reply) else {
            panic!("expected forward")
        };
        let mut input = projected(&reply);
        let output = projected(&out);
        // Exactly two touches: reply_to gone, hop tag appended.
        input["header"].as_object_mut().unwrap().remove("reply_to");
        input["tags"][RELAY_TAG] = json!([HOP]);
        assert_eq!(output, input);
        assert_eq!(
            output["header"]["correlation_id"], "corr-9",
            "correlation_id preserved"
        );
        assert!(
            output["header"].get("reply_to").is_none(),
            "a relayed reply carries no reply_to"
        );
    }

    #[test]
    fn non_protobuf_json_reply_is_dropped() {
        let payload = serde_json::to_vec(&json!({ "ok": true })).unwrap();
        assert_eq!(
            prepare_reply(&engine(), &payload),
            ReplyRelay::Drop(DropReason::MalformedEnvelope)
        );
    }

    #[test]
    fn non_protobuf_reply_is_dropped() {
        let payload = b"plain ack".to_vec();
        assert_eq!(
            prepare_reply(&engine(), &payload),
            ReplyRelay::Drop(DropReason::MalformedEnvelope)
        );
    }

    #[test]
    fn prepare_reply_preserves_opaque_body_bytes() {
        let opaque = [0x10, 0x20, 0x30, 0xff];
        let reply = MessageBuilder::new("frame-reply", "1.0")
            .opaque_body(opaque, "image/jpeg")
            .unwrap()
            .tag("site", json!("dallas"))
            .correlation_id("corr-opaque-reply")
            .reply_to("edgecommons/reply-device-local")
            .build()
            .to_vec()
            .unwrap();
        let ReplyRelay::Forward(out) = prepare_reply(&engine(), &reply) else {
            panic!("expected forward")
        };
        let decoded = decoded(&out);
        assert_eq!(decoded.header.correlation_id, "corr-opaque-reply");
        assert!(decoded.header.reply_to.is_none());
        assert_eq!(decoded.body_case(), MessageBodyCase::Opaque);
        assert_eq!(decoded.content_type.as_deref(), Some("image/jpeg"));
        assert_eq!(decoded.opaque_body().unwrap().unwrap(), opaque);
        let tags = decoded.tags.unwrap().extra;
        assert_eq!(tags.get("site"), Some(&json!("dallas")));
        assert_eq!(tags.get(RELAY_TAG), Some(&json!([HOP])));
    }

    #[test]
    fn reply_carrying_own_hop_id_is_dropped() {
        let reply = MessageBuilder::new("r", "1.0")
            .payload(json!({}))
            .tag(RELAY_TAG, json!([HOP]))
            .build()
            .to_vec()
            .unwrap();
        assert_eq!(
            prepare_reply(&engine(), &reply),
            ReplyRelay::Drop(DropReason::OwnEcho)
        );
    }

    #[test]
    fn reply_at_max_hops_is_dropped() {
        let hops: Vec<String> = (0..DEFAULT_MAX_HOPS)
            .map(|i| format!("gw-{i}/uns-bridge"))
            .collect();
        let reply = MessageBuilder::new("r", "1.0")
            .payload(json!({}))
            .tag(RELAY_TAG, json!(hops))
            .build()
            .to_vec()
            .unwrap();
        assert_eq!(
            prepare_reply(&engine(), &reply),
            ReplyRelay::Drop(DropReason::MaxHopsExceeded)
        );
    }

    #[test]
    fn malformed_reply_envelope_is_dropped() {
        let payload = serde_json::to_vec(&json!({ "header": 42, "body": {} })).unwrap();
        assert_eq!(
            prepare_reply(&engine(), &payload),
            ReplyRelay::Drop(DropReason::MalformedEnvelope)
        );
    }
}
