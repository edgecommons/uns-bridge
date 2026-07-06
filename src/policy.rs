//! # policy — the pure per-class uplink policy (§2.5 / D-B10)
//!
//! **One-liner purpose**: Decide what an uplink-forwardable message is *allowed*
//! to do — per-class enable/disable, per-class token-bucket rate caps, and the
//! D-B10 disconnect behavior (drop + count for every class except `evt`, which
//! gets a bounded drop-oldest replay buffer) — with no IO and no internal clock,
//! so every decision is deterministic under test.
//!
//! (Section references are to `docs/platform/DESIGN-uns-bridge.md` in the
//! edgecommons monorepo; the decision-register entry is **D-B10**.)
//!
//! ## Semantics (§2.5)
//! - **Enable/disable**: each of the seven uplinkable classes
//!   (`state | cfg | evt | metric | data | log | app`) can be switched off; a
//!   disabled class's messages drop + count. Defaults: `app` **off** (opt-in,
//!   as since P3-2), every other class **on** — matching the pre-P3-4 relay
//!   behavior. (§2.5 also *recommends* `log` off by default; the shipped sample
//!   config sets it, but the code default stays ON to keep the P3-2/P3-3
//!   behavior — flipping it is a config choice, not a code default.)
//! - **Rate caps**: a token bucket per rate-capped class (`maxRatePerSec`
//!   refill, `burst` capacity, default `burst = 2×rate`). Exceeding traffic
//!   **drops** (never queues — the live UNS path is explicitly not durable;
//!   durability is the streaming subsystem's job, DESIGN-uns §8), counted per
//!   class. The bucket starts full, so an initial burst of up to `burst`
//!   messages passes immediately.
//! - **Disconnect behavior** (D-B10): while the site link is down, every class
//!   drops + counts — **except `evt`**, which is pushed into a bounded,
//!   memory-only, drop-oldest replay buffer (default on, 1000). Buffered
//!   events replay strictly in order once the link is back (the replay bytes
//!   are the already-hop-stamped forward bytes, so the hop-tag and
//!   topic-verbatim rules hold on replay). To preserve intra-`evt` ordering, a
//!   *live* `evt` arriving while older ones are still queued also rides the
//!   buffer rather than overtaking them.
//! - **NOT here (a later slice)**: the `state`/`cfg` reconnect rehydration via
//!   the `republish-*` `_bcast` broadcasts (§2.5 / DESIGN-uns §9.3 layer 2) —
//!   see the TODO at the connectivity watcher in [`crate::io`].
//!
//! This module is **pure** — no IO, no tasks, no `Instant::now()` (`now` is
//! injected into every time-dependent decision), mirroring the
//! [`crate::reply`]/[`crate::io`] split. The pumping (the `connected()` probe,
//! the site publish, the replay drain) lives in [`crate::io`].

use std::collections::VecDeque;
use std::time::Instant;

use edgecommons::uns::UnsClass;

use crate::config::UplinkConfig;

/// The seven policy-governed uplink classes (§2.5): the six consumer classes
/// plus the opt-in `app`. `cmd` never uplinks (the §2.2 matrix drops it before
/// the policy) and has no policy slot.
pub const POLICY_CLASSES: [UnsClass; 7] = [
    UnsClass::State,
    UnsClass::Cfg,
    UnsClass::Evt,
    UnsClass::Metric,
    UnsClass::Data,
    UnsClass::Log,
    UnsClass::App,
];

/// Default bound of the `evt` replay buffer (D-B10: default **on**, **1000**,
/// drop-oldest, memory-only).
pub const DEFAULT_EVT_BUFFER_MAX: usize = 1000;

/// The [`POLICY_CLASSES`] slot for `class`, or `None` for the one class the
/// policy does not govern (`cmd`). Shared with the per-class counters in
/// [`crate::io`] so both index identically.
pub fn class_index(class: UnsClass) -> Option<usize> {
    match class {
        UnsClass::State => Some(0),
        UnsClass::Cfg => Some(1),
        UnsClass::Evt => Some(2),
        UnsClass::Metric => Some(3),
        UnsClass::Data => Some(4),
        UnsClass::Log => Some(5),
        UnsClass::App => Some(6),
        UnsClass::Cmd => None,
    }
}

/// The policy's verdict for one decided-and-forwardable uplink message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UplinkVerdict {
    /// Publish to the site broker now.
    Forward,
    /// The class is disabled (`uplink.classes.<class>.enabled == false`):
    /// drop + count `dropped_disabled`.
    DropDisabled,
    /// The site link is down and the class does not buffer: drop + count
    /// `dropped_disconnected` (D-B10 — durability is streaming's job).
    DropDisconnected,
    /// Over the class's token bucket: drop + count `dropped_rate`.
    DropRateCapped,
    /// An `evt` that must ride the replay buffer (link down, or older buffered
    /// `evt` still queued): the caller pushes it via [`UplinkPolicy::push_evt`].
    Buffer,
}

/// The outcome of pushing one `evt` into the replay buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvtPush {
    /// Stored; when `evicted_oldest`, the buffer was full and the OLDEST
    /// buffered message was dropped to make room (count `evt_buffer_dropped`).
    Stored {
        /// Whether the push evicted the oldest buffered message (drop-oldest).
        evicted_oldest: bool,
    },
    /// The buffer is disabled (or bounded at 0): nothing stored — the caller
    /// counts the message as `dropped_disconnected` instead.
    Rejected,
}

/// A token bucket (§2.5): `rate_per_sec` refill, `capacity` burst. Pure — the
/// clock is injected into [`Self::try_take`], never read internally.
#[derive(Debug)]
struct TokenBucket {
    rate_per_sec: f64,
    capacity: f64,
    tokens: f64,
    last_refill: Option<Instant>,
}

impl TokenBucket {
    /// `rate` = `maxRatePerSec`; `burst` = capacity. The bucket starts FULL
    /// (an initial burst passes immediately). A rate of 0 never refills, so
    /// after `burst` messages everything drops (prefer `enabled: false`).
    fn new(rate: u32, burst: u32) -> TokenBucket {
        let capacity = f64::from(burst);
        TokenBucket {
            rate_per_sec: f64::from(rate),
            capacity,
            tokens: capacity,
            last_refill: None,
        }
    }

    /// Refill for the time elapsed since the last call (saturating — a
    /// non-monotonic `now` refills nothing rather than panicking), then take
    /// one token if available.
    fn try_take(&mut self, now: Instant) -> bool {
        if let Some(last) = self.last_refill {
            let elapsed = now.saturating_duration_since(last).as_secs_f64();
            self.tokens = (self.tokens + elapsed * self.rate_per_sec).min(self.capacity);
        }
        self.last_refill = Some(now);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// One class's runtime policy state.
#[derive(Debug)]
struct ClassState {
    enabled: bool,
    bucket: Option<TokenBucket>,
}

/// The pure §2.5 uplink policy: per-class enables, per-class token buckets, and
/// the D-B10 `evt` replay buffer. Holds no connections and reads no clock;
/// guard it with a mutex and drive it from the IO layer ([`crate::io`]).
#[derive(Debug)]
pub struct UplinkPolicy {
    /// Indexed by [`class_index`] over [`POLICY_CLASSES`].
    classes: [ClassState; 7],
    /// The bounded drop-oldest `evt` replay buffer: `(topic, hop-stamped
    /// forward bytes)`, front = oldest = first to replay / first to evict.
    evt_buffer: VecDeque<(String, Vec<u8>)>,
    /// The buffer bound; **0 = buffering disabled** (evt drops like the rest).
    evt_buffer_max: usize,
}

impl UplinkPolicy {
    /// Build the runtime policy from the typed `uplink` config block, applying
    /// the §2.5 defaults: every class enabled except `app`; no rate cap unless
    /// `maxRatePerSec` is set (`burst` defaults to `2×rate`); the `evt` replay
    /// buffer ON at [`DEFAULT_EVT_BUFFER_MAX`] unless configured off.
    ///
    /// `bufferWhileDisconnected` on any class other than `evt` is ignored with
    /// a warning — the D-B10 scope call is evt-only.
    pub fn from_config(cfg: &UplinkConfig) -> UplinkPolicy {
        let classes = POLICY_CLASSES.map(|cls| {
            let policy = cfg.classes.get(cls.token());
            let enabled = policy
                .and_then(|p| p.enabled)
                .unwrap_or(cls != UnsClass::App);
            let bucket = policy.and_then(|p| p.max_rate_per_sec).map(|rate| {
                let burst = policy
                    .and_then(|p| p.burst)
                    .unwrap_or_else(|| rate.saturating_mul(2));
                TokenBucket::new(rate, burst)
            });
            if cls != UnsClass::Evt && policy.is_some_and(|p| p.buffer_while_disconnected.is_some())
            {
                tracing::warn!(
                    class = cls.token(),
                    "bufferWhileDisconnected is honored for evt only; ignored"
                );
            }
            ClassState { enabled, bucket }
        });
        let evt_buffer_max = match cfg
            .classes
            .get(UnsClass::Evt.token())
            .and_then(|p| p.buffer_while_disconnected.as_ref())
        {
            Some(b) if !b.enabled => 0,
            Some(b) => b.max_messages,
            None => DEFAULT_EVT_BUFFER_MAX,
        };
        UplinkPolicy {
            classes,
            evt_buffer: VecDeque::new(),
            evt_buffer_max,
        }
    }

    /// The §2.5 admission decision for one forwardable uplink message of
    /// `class`, given the site link state (`connected`) at injected time `now`.
    ///
    /// Check order: **disabled** (a disabled class drops even while
    /// disconnected) → **disconnected** (`evt` buffers, the rest drop; no
    /// token is consumed for a message that is not published) → **replay
    /// ordering** (a live `evt` behind a non-empty buffer rides the buffer so
    /// events never overtake each other) → **rate cap** (the token bucket
    /// meters only actual live publishes; buffered/replayed `evt` bypass it —
    /// `evt` is uncapped by default).
    ///
    /// `cmd` is not policy-governed (it never uplinks — the §2.2 matrix drops
    /// it one step earlier); the policy has no opinion and forwards it.
    pub fn admit(&mut self, class: UnsClass, connected: bool, now: Instant) -> UplinkVerdict {
        let Some(idx) = class_index(class) else {
            return UplinkVerdict::Forward;
        };
        if !self.classes[idx].enabled {
            return UplinkVerdict::DropDisabled;
        }
        let buffering = class == UnsClass::Evt && self.evt_buffer_max > 0;
        if !connected {
            return if buffering {
                UplinkVerdict::Buffer
            } else {
                UplinkVerdict::DropDisconnected
            };
        }
        if buffering && !self.evt_buffer.is_empty() {
            // The link is back but older evt are still queued for replay: join
            // the queue so events reach the site strictly in order.
            return UplinkVerdict::Buffer;
        }
        if let Some(bucket) = &mut self.classes[idx].bucket {
            if !bucket.try_take(now) {
                return UplinkVerdict::DropRateCapped;
            }
        }
        UplinkVerdict::Forward
    }

    /// Push one `evt` (its topic + already-hop-stamped forward bytes) into the
    /// replay buffer: drop-oldest on overflow (D-B10). [`EvtPush::Rejected`]
    /// when buffering is disabled — reachable via the failed-publish path,
    /// where the caller never went through a [`UplinkVerdict::Buffer`] verdict.
    pub fn push_evt(&mut self, topic: String, bytes: Vec<u8>) -> EvtPush {
        if self.evt_buffer_max == 0 {
            return EvtPush::Rejected;
        }
        let evicted_oldest = if self.evt_buffer.len() >= self.evt_buffer_max {
            self.evt_buffer.pop_front();
            true
        } else {
            false
        };
        self.evt_buffer.push_back((topic, bytes));
        EvtPush::Stored { evicted_oldest }
    }

    /// Pop the OLDEST buffered `evt` for replay (front of the queue).
    pub fn pop_evt(&mut self) -> Option<(String, Vec<u8>)> {
        self.evt_buffer.pop_front()
    }

    /// Put a popped-but-unreplayed `evt` back at the FRONT (it is still the
    /// oldest; the next replay attempt retries it first). Returns `false` —
    /// the message is dropped instead — when the buffer refilled to its bound
    /// while the message was in flight: under drop-oldest, the frontmost
    /// message IS the drop victim.
    pub fn requeue_evt_front(&mut self, topic: String, bytes: Vec<u8>) -> bool {
        if self.evt_buffer.len() >= self.evt_buffer_max {
            return false;
        }
        self.evt_buffer.push_front((topic, bytes));
        true
    }

    /// The number of buffered `evt` awaiting replay.
    pub fn buffered_evt(&self) -> usize {
        self.evt_buffer.len()
    }

    /// The replay-buffer capacity (0 = buffering disabled).
    pub fn evt_buffer_capacity(&self) -> usize {
        self.evt_buffer_max
    }

    /// Whether `class` is relayed at all (`cmd` — ungoverned — reads `false`).
    pub fn class_enabled(&self, class: UnsClass) -> bool {
        class_index(class).is_some_and(|i| self.classes[i].enabled)
    }

    /// Whether `class` carries a token-bucket rate cap.
    pub fn has_rate_cap(&self, class: UnsClass) -> bool {
        class_index(class).is_some_and(|i| self.classes[i].bucket.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn cfg(v: serde_json::Value) -> UplinkConfig {
        serde_json::from_value(v).unwrap()
    }

    fn policy(v: serde_json::Value) -> UplinkPolicy {
        UplinkPolicy::from_config(&cfg(v))
    }

    fn item(n: usize) -> (String, Vec<u8>) {
        (format!("ecv1/gw-01/c/main/evt/e{n}"), vec![n as u8])
    }

    // ---- defaults (§2.5 + D-B10) ----

    #[test]
    fn defaults_every_class_on_except_app_no_caps_evt_buffer_on() {
        let p = UplinkPolicy::from_config(&UplinkConfig::default());
        for cls in POLICY_CLASSES {
            assert_eq!(p.class_enabled(cls), cls != UnsClass::App, "{cls:?}");
            assert!(!p.has_rate_cap(cls), "{cls:?} must be uncapped by default");
        }
        assert!(
            !p.class_enabled(UnsClass::Cmd),
            "cmd is not policy-governed"
        );
        assert!(!p.has_rate_cap(UnsClass::Cmd));
        assert_eq!(p.evt_buffer_capacity(), DEFAULT_EVT_BUFFER_MAX);
        assert_eq!(p.buffered_evt(), 0);
    }

    #[test]
    fn class_index_covers_the_seven_policy_classes_and_excludes_cmd() {
        for (i, cls) in POLICY_CLASSES.iter().enumerate() {
            assert_eq!(class_index(*cls), Some(i));
        }
        assert_eq!(class_index(UnsClass::Cmd), None);
    }

    // ---- enable/disable ----

    #[test]
    fn disabled_class_drops_even_while_disconnected() {
        let mut p = policy(serde_json::json!({
            "classes": { "log": { "enabled": false }, "evt": { "enabled": false } }
        }));
        let now = Instant::now();
        assert_eq!(
            p.admit(UnsClass::Log, true, now),
            UplinkVerdict::DropDisabled
        );
        assert_eq!(
            p.admit(UnsClass::Log, false, now),
            UplinkVerdict::DropDisabled
        );
        // Disabled beats the evt buffer: a disabled evt never buffers.
        assert_eq!(
            p.admit(UnsClass::Evt, false, now),
            UplinkVerdict::DropDisabled
        );
        assert_eq!(p.buffered_evt(), 0);
        // The other classes are unaffected.
        assert_eq!(p.admit(UnsClass::State, true, now), UplinkVerdict::Forward);
    }

    #[test]
    fn app_is_opt_in() {
        let mut off = UplinkPolicy::from_config(&UplinkConfig::default());
        assert_eq!(
            off.admit(UnsClass::App, true, Instant::now()),
            UplinkVerdict::DropDisabled
        );
        let mut on = policy(serde_json::json!({ "classes": { "app": { "enabled": true } } }));
        assert_eq!(
            on.admit(UnsClass::App, true, Instant::now()),
            UplinkVerdict::Forward
        );
    }

    #[test]
    fn cmd_is_not_policy_governed() {
        // decide() drops cmd on the uplink one step earlier; the policy has no
        // opinion (and must not misreport it as a policy drop).
        let mut p = UplinkPolicy::from_config(&UplinkConfig::default());
        assert_eq!(
            p.admit(UnsClass::Cmd, true, Instant::now()),
            UplinkVerdict::Forward
        );
        assert_eq!(
            p.admit(UnsClass::Cmd, false, Instant::now()),
            UplinkVerdict::Forward
        );
    }

    // ---- rate caps (deterministic: the clock is injected) ----

    #[test]
    fn token_bucket_caps_deterministically_and_refills_at_rate() {
        let mut p = policy(serde_json::json!({
            "classes": { "data": { "maxRatePerSec": 10, "burst": 2 } }
        }));
        let t0 = Instant::now();
        // The bucket starts full: the burst passes.
        assert_eq!(p.admit(UnsClass::Data, true, t0), UplinkVerdict::Forward);
        assert_eq!(p.admit(UnsClass::Data, true, t0), UplinkVerdict::Forward);
        // Over the cap at the same instant: dropped.
        assert_eq!(
            p.admit(UnsClass::Data, true, t0),
            UplinkVerdict::DropRateCapped
        );
        // 100 ms at 10/s refills exactly one token.
        let t1 = t0 + Duration::from_millis(100);
        assert_eq!(p.admit(UnsClass::Data, true, t1), UplinkVerdict::Forward);
        assert_eq!(
            p.admit(UnsClass::Data, true, t1),
            UplinkVerdict::DropRateCapped
        );
        // A long idle refills to the burst capacity, never beyond.
        let t2 = t1 + Duration::from_secs(3600);
        assert_eq!(p.admit(UnsClass::Data, true, t2), UplinkVerdict::Forward);
        assert_eq!(p.admit(UnsClass::Data, true, t2), UplinkVerdict::Forward);
        assert_eq!(
            p.admit(UnsClass::Data, true, t2),
            UplinkVerdict::DropRateCapped
        );
    }

    #[test]
    fn burst_defaults_to_twice_the_rate() {
        let mut p = policy(serde_json::json!({
            "classes": { "metric": { "maxRatePerSec": 3 } }
        }));
        assert!(p.has_rate_cap(UnsClass::Metric));
        let t0 = Instant::now();
        for i in 0..6 {
            assert_eq!(
                p.admit(UnsClass::Metric, true, t0),
                UplinkVerdict::Forward,
                "burst msg {i}"
            );
        }
        assert_eq!(
            p.admit(UnsClass::Metric, true, t0),
            UplinkVerdict::DropRateCapped
        );
    }

    #[test]
    fn rate_zero_forwards_only_the_burst_then_drops_forever() {
        let mut p = policy(serde_json::json!({
            "classes": { "data": { "maxRatePerSec": 0, "burst": 1 } }
        }));
        let t0 = Instant::now();
        assert_eq!(p.admit(UnsClass::Data, true, t0), UplinkVerdict::Forward);
        assert_eq!(
            p.admit(UnsClass::Data, true, t0),
            UplinkVerdict::DropRateCapped
        );
        // Rate 0 never refills — not even after an hour.
        let t1 = t0 + Duration::from_secs(3600);
        assert_eq!(
            p.admit(UnsClass::Data, true, t1),
            UplinkVerdict::DropRateCapped
        );
    }

    #[test]
    fn a_non_monotonic_now_refills_nothing_and_does_not_panic() {
        let mut p = policy(serde_json::json!({
            "classes": { "data": { "maxRatePerSec": 1000, "burst": 1 } }
        }));
        let t1 = Instant::now() + Duration::from_secs(10);
        assert_eq!(p.admit(UnsClass::Data, true, t1), UplinkVerdict::Forward);
        // Time goes "backwards": saturates to zero elapsed — no refill.
        let t0 = t1 - Duration::from_secs(5);
        assert_eq!(
            p.admit(UnsClass::Data, true, t0),
            UplinkVerdict::DropRateCapped
        );
    }

    #[test]
    fn uncapped_classes_never_rate_drop() {
        let mut p = UplinkPolicy::from_config(&UplinkConfig::default());
        let t0 = Instant::now();
        for _ in 0..10_000 {
            assert_eq!(p.admit(UnsClass::Data, true, t0), UplinkVerdict::Forward);
        }
    }

    // ---- disconnect behavior (D-B10) ----

    #[test]
    fn disconnected_non_evt_drops_and_consumes_no_token() {
        let mut p = policy(serde_json::json!({
            "classes": { "data": { "maxRatePerSec": 0, "burst": 1 } }
        }));
        let now = Instant::now();
        for cls in [
            UnsClass::State,
            UnsClass::Cfg,
            UnsClass::Metric,
            UnsClass::Data,
            UnsClass::Log,
        ] {
            assert_eq!(
                p.admit(cls, false, now),
                UplinkVerdict::DropDisconnected,
                "{cls:?}"
            );
        }
        // No token was consumed while disconnected: the single burst token is
        // still there once the link is back.
        assert_eq!(p.admit(UnsClass::Data, true, now), UplinkVerdict::Forward);
    }

    #[test]
    fn disconnected_evt_buffers() {
        let mut p = UplinkPolicy::from_config(&UplinkConfig::default());
        assert_eq!(
            p.admit(UnsClass::Evt, false, Instant::now()),
            UplinkVerdict::Buffer
        );
        let (topic, bytes) = item(1);
        assert_eq!(
            p.push_evt(topic, bytes),
            EvtPush::Stored {
                evicted_oldest: false
            }
        );
        assert_eq!(p.buffered_evt(), 1);
    }

    #[test]
    fn evt_buffer_disabled_drops_on_disconnect() {
        let mut p = policy(serde_json::json!({
            "classes": { "evt": { "bufferWhileDisconnected": { "enabled": false } } }
        }));
        assert_eq!(p.evt_buffer_capacity(), 0);
        assert_eq!(
            p.admit(UnsClass::Evt, false, Instant::now()),
            UplinkVerdict::DropDisconnected
        );
        let (topic, bytes) = item(1);
        assert_eq!(p.push_evt(topic, bytes), EvtPush::Rejected);
    }

    #[test]
    fn evt_buffer_max_zero_disables_buffering() {
        let mut p = policy(serde_json::json!({
            "classes": { "evt": { "bufferWhileDisconnected": { "maxMessages": 0 } } }
        }));
        assert_eq!(p.evt_buffer_capacity(), 0);
        assert_eq!(
            p.admit(UnsClass::Evt, false, Instant::now()),
            UplinkVerdict::DropDisconnected
        );
    }

    #[test]
    fn live_evt_behind_a_non_empty_buffer_joins_the_queue_in_order() {
        let mut p = UplinkPolicy::from_config(&UplinkConfig::default());
        let now = Instant::now();
        assert_eq!(p.admit(UnsClass::Evt, false, now), UplinkVerdict::Buffer);
        let (t1, b1) = item(1);
        p.push_evt(t1.clone(), b1);
        // Link back, but e1 is still queued: a live e2 must not overtake it.
        assert_eq!(p.admit(UnsClass::Evt, true, now), UplinkVerdict::Buffer);
        let (t2, b2) = item(2);
        p.push_evt(t2.clone(), b2);
        assert_eq!(p.pop_evt().unwrap().0, t1, "oldest first");
        assert_eq!(p.pop_evt().unwrap().0, t2);
        // Buffer drained: live evt forwards again.
        assert_eq!(p.admit(UnsClass::Evt, true, now), UplinkVerdict::Forward);
    }

    // ---- the bounded drop-oldest buffer ----

    #[test]
    fn buffer_overflow_drops_the_oldest() {
        let mut p = policy(serde_json::json!({
            "classes": { "evt": { "bufferWhileDisconnected": { "maxMessages": 2 } } }
        }));
        assert_eq!(p.evt_buffer_capacity(), 2);
        let (t1, b1) = item(1);
        let (t2, b2) = item(2);
        let (t3, b3) = item(3);
        assert_eq!(
            p.push_evt(t1, b1),
            EvtPush::Stored {
                evicted_oldest: false
            }
        );
        assert_eq!(
            p.push_evt(t2.clone(), b2),
            EvtPush::Stored {
                evicted_oldest: false
            }
        );
        assert_eq!(
            p.push_evt(t3.clone(), b3),
            EvtPush::Stored {
                evicted_oldest: true
            }
        );
        assert_eq!(p.buffered_evt(), 2, "the bound holds");
        assert_eq!(p.pop_evt().unwrap().0, t2, "e1 was the drop-oldest victim");
        assert_eq!(p.pop_evt().unwrap().0, t3);
        assert_eq!(p.pop_evt(), None);
    }

    #[test]
    fn requeue_front_restores_replay_order() {
        let mut p = UplinkPolicy::from_config(&UplinkConfig::default());
        let (t1, b1) = item(1);
        let (t2, b2) = item(2);
        p.push_evt(t1.clone(), b1);
        p.push_evt(t2.clone(), b2);
        // A failed replay puts the popped message back at the front.
        let (topic, bytes) = p.pop_evt().unwrap();
        assert_eq!(topic, t1);
        assert!(p.requeue_evt_front(topic, bytes));
        assert_eq!(
            p.pop_evt().unwrap().0,
            t1,
            "the requeued message replays first"
        );
        assert_eq!(p.pop_evt().unwrap().0, t2);
    }

    #[test]
    fn requeue_front_into_a_refilled_full_buffer_drops_the_in_flight_message() {
        let mut p = policy(serde_json::json!({
            "classes": { "evt": { "bufferWhileDisconnected": { "maxMessages": 1 } } }
        }));
        let (t1, b1) = item(1);
        p.push_evt(t1.clone(), b1);
        let (topic, bytes) = p.pop_evt().unwrap();
        // The buffer refills to its bound while the message is in flight: under
        // drop-oldest the in-flight (oldest) message is the victim.
        let (t2, b2) = item(2);
        p.push_evt(t2.clone(), b2);
        assert!(!p.requeue_evt_front(topic, bytes));
        assert_eq!(p.buffered_evt(), 1);
        assert_eq!(p.pop_evt().unwrap().0, t2);
    }

    // ---- config plumbing ----

    #[test]
    fn buffer_config_on_a_non_evt_class_is_ignored() {
        // Warned + ignored (D-B10 scope call is evt-only): data never buffers.
        let mut p = policy(serde_json::json!({
            "classes": { "data": { "bufferWhileDisconnected": { "maxMessages": 5 } } }
        }));
        assert_eq!(
            p.admit(UnsClass::Data, false, Instant::now()),
            UplinkVerdict::DropDisconnected
        );
        assert_eq!(
            p.evt_buffer_capacity(),
            DEFAULT_EVT_BUFFER_MAX,
            "evt keeps its own default"
        );
    }

    #[test]
    fn the_shipped_sample_config_builds_the_expected_policy() {
        // Backward compatibility: the repo's own sample config parses into
        // exactly the §2.5 policy it declares.
        let full: serde_json::Value =
            serde_json::from_str(include_str!("../test-configs/config.json")).unwrap();
        let uplink: UplinkConfig =
            serde_json::from_value(full["component"]["instances"][0]["uplink"].clone()).unwrap();
        let p = UplinkPolicy::from_config(&uplink);
        assert!(p.class_enabled(UnsClass::State));
        assert!(p.class_enabled(UnsClass::Evt));
        assert!(!p.class_enabled(UnsClass::Log), "the sample opts log out");
        assert!(!p.class_enabled(UnsClass::App));
        assert!(p.has_rate_cap(UnsClass::Metric));
        assert!(p.has_rate_cap(UnsClass::Data));
        assert!(!p.has_rate_cap(UnsClass::State));
        assert_eq!(p.evt_buffer_capacity(), 1000);
    }
}
