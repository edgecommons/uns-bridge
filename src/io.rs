//! # io — the relay pumps over two provider-level connections
//!
//! **One-liner purpose**: Wire the pure [`RelayEngine`](crate::relay::RelayEngine)
//! between two [`MessagingProvider`] handles — PRIMARY (the device bus) and SITE
//! (the reused core `MqttProvider` against the site broker) — and pump protobuf
//! edgecommons message bytes.
//!
//! The relay runs at the **raw provider level** (`publish`/`subscribe` bytes,
//! DESIGN-uns-bridge §1.3): the reserved-class guard is a `MessagingService`
//! concern and is simply not in this path. Normal relay payloads are protobuf
//! `EdgeCommonsMessage` bytes; foreign bytes drop as malformed. The site
//! broker's per-device ACL is the durable boundary.
//!
//! One pump task per subscription (`max_concurrency = 1` semantics — serial,
//! ordered per class); the per-class `max_messages` bound lives in the provider
//! subscription queue (overflow drops at the provider with a warning). Shutdown
//! aborts the pumps and **unsubscribes every filter at the broker** before exit.
//!
//! The downlink pump additionally runs every forwardable `cmd` through the §2.4
//! **reply proxy** ([`crate::reply`]): a `cmd` carrying `header.reply_to` gets a
//! bridge-minted reply topic subscribed on the device bus (before the cmd is
//! relayed), a one-shot per-reply pump, and a correlation entry in the TTL'd map
//! swept by a periodic task (`min(ttl/4, 5 s)`).
//!
//! Every uplink-forwardable message additionally passes the §2.5 **uplink
//! policy** ([`crate::policy`], pure) via the [`UplinkGovernor`]: per-class
//! enables, per-class token-bucket rate caps, and the D-B10 disconnect behavior
//! (non-`evt` drops + counts while the site link is down or a publish fails;
//! `evt` rides a bounded drop-oldest replay buffer). A **connectivity watcher**
//! task polls `site.connected()` and drains the buffer, strictly in order,
//! whenever the link is up and something is queued; on the **rising edge**
//! (site reconnect) it first publishes the two §2.5 / DESIGN-uns §9.3 (layer 2)
//! rehydration broadcasts `ecv1/{device}/_bcast/cmd/republish-{state,cfg}`
//! on the DEVICE bus — best-effort, before the `evt` replay — so the site view
//! rehydrates `state`/`cfg` without retain. **Bridge side only**: the device-side
//! listener that answers the broadcast is a separate 4-language edgecommons library
//! slice; until it lands the broadcast is inert (published, answered by nobody).
//!
//! With an [`ObservabilityHook`] (P3-4b, §2.8) a periodic task additionally
//! snapshots the [`RelayCounters`] and emits them as `metric`s through
//! `gg.metrics()` (the pure mapping lives in [`crate::observability`]); the
//! messaging metric target then publishes them on
//! `ecv1/{device}/uns-bridge/metric/<name>` — which matches the bridge's own
//! uplink filters, so the counters ride the bridge's own relay to the site.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use edgecommons::config::model::Config;
use edgecommons::messaging::{Destination, MessageBuilder, MessagingProvider, Qos, Subscription};
use edgecommons::metrics::{MetricBuilder, MetricService};
use tokio::task::JoinHandle;

use crate::config::{QueueConfig, ReplyConfig, UplinkConfig};
use crate::observability::{metric_definitions, relay_metric_groups, RelaySnapshot};
use crate::policy::{class_index, EvtPush, UplinkPolicy, UplinkVerdict, POLICY_CLASSES};
use crate::relay::{Direction, DropReason, RelayDecision, RelayEngine, REHYDRATION_CMDS};
use crate::reply::{prepare_reply, DownlinkRewrite, ReplyCorrelator, ReplyRelay};
use edgecommons::uns::UnsClass;

/// Cadence of the connectivity watcher that replays buffered `evt` once the
/// site link is (back) up (§2.5 / D-B10 reconnect replay).
const CONNECTIVITY_POLL: Duration = Duration::from_millis(250);

/// Cadence of the §2.8 metric emission (the [`RelayCounters`] → `gg.metrics()`
/// task): counters emit as interval deltas every tick, gauges as current values.
const METRIC_EMIT_INTERVAL: Duration = Duration::from_secs(30);

/// Per-class counters over the seven policy-governed uplink classes (the §2.5
/// metric table's per-class measures). `cmd` is not policy-governed and has no
/// slot — counting it is a no-op / reads 0.
#[derive(Debug, Default)]
pub struct ClassCounters([AtomicU64; POLICY_CLASSES.len()]);

impl ClassCounters {
    /// Increment `class`'s counter.
    pub fn incr(&self, class: UnsClass) {
        if let Some(i) = class_index(class) {
            self.0[i].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// `class`'s current count.
    #[allow(dead_code)] // the per-class test seam; production reads via snapshot()/total()
    pub fn get(&self, class: UnsClass) -> u64 {
        class_index(class).map_or(0, |i| self.0[i].load(Ordering::Relaxed))
    }

    /// The sum across every class.
    pub fn total(&self) -> u64 {
        self.0.iter().map(|c| c.load(Ordering::Relaxed)).sum()
    }

    /// A point-in-time copy of every class slot ([`POLICY_CLASSES`] order) — the
    /// per-class half of the §2.8 [`RelaySnapshot`].
    pub fn snapshot(&self) -> [u64; POLICY_CLASSES.len()] {
        std::array::from_fn(|i| self.0[i].load(Ordering::Relaxed))
    }
}

/// Relay counters (observable seams for tests now; surfaced as `metric`s through
/// the normal metric subsystem in P3-4b — §2.5 table).
#[derive(Debug, Default)]
pub struct RelayCounters {
    /// Messages relayed device → site, per class (`relay_uplinked`, §2.5 table).
    pub uplinked: ClassCounters,
    /// Commands relayed site → device.
    pub downlinked: AtomicU64,
    /// Dropped by the hop-tag guard (`relay_loop_dropped`: own echo + maxHops).
    pub loop_dropped: AtomicU64,
    /// Dropped by class routing / device pinning / non-UNS topics.
    pub routed_dropped: AtomicU64,
    /// Dropped as malformed envelopes.
    pub malformed_dropped: AtomicU64,
    /// Forward decisions whose republish failed at the transport (also counts a
    /// downlink cmd not relayed because its bridge reply-topic SUBSCRIBE failed).
    pub publish_failed: AtomicU64,
    /// Replies relayed device → site through the correlation map (§2.4).
    pub reply_relayed: AtomicU64,
    /// Correlation entries torn down unresolved (`relay_reply_expired`): TTL
    /// expiry by the sweep + evict-oldest at `maxPending` (§2.4 counts both).
    pub reply_expired: AtomicU64,
    /// Replies arriving on a bridge reply topic with no live correlation entry
    /// (expired/evicted between delivery and resolution) — dropped.
    pub reply_stray: AtomicU64,
    /// Uplink drops, per class: the class is disabled
    /// (`uplink.classes.<class>.enabled == false`, §2.5).
    pub dropped_disabled: ClassCounters,
    /// Uplink drops, per class: over the class's token bucket
    /// (`relay_dropped_rate`, §2.5).
    pub dropped_rate: ClassCounters,
    /// Uplink drops, per class: the site link was down (or the site publish
    /// failed) and the class does not buffer (`relay_dropped_disconnected`,
    /// §2.5 / D-B10).
    pub dropped_disconnected: ClassCounters,
    /// `evt` pushed into the D-B10 disconnect replay buffer.
    pub evt_buffered: AtomicU64,
    /// `evt` evicted from the FULL replay buffer (drop-oldest).
    pub evt_buffer_dropped: AtomicU64,
    /// Buffered `evt` replayed to the site broker, in order, after reconnect.
    pub evt_replayed: AtomicU64,
}

impl RelayCounters {
    fn count_drop(&self, reason: DropReason) {
        let counter = match reason {
            DropReason::OwnEcho | DropReason::MaxHopsExceeded => &self.loop_dropped,
            DropReason::MalformedEnvelope => &self.malformed_dropped,
            DropReason::NotUnsTopic | DropReason::ClassNotRelayed | DropReason::NotOwnDevice => {
                &self.routed_dropped
            }
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// The §2.4 reply-proxy plumbing shared by the downlink pump, the per-reply
/// pumps, and the TTL sweep task: the correlation map plus the handles they act
/// on. Pure decisions live in [`crate::reply`]; this struct only pumps them.
struct ReplyProxy {
    correlator: Mutex<ReplyCorrelator>,
    engine: Arc<RelayEngine>,
    /// The device bus — bridge reply topics are subscribed/unsubscribed here.
    primary: Arc<dyn MessagingProvider>,
    /// The site broker — resolved replies publish here.
    site: Arc<dyn MessagingProvider>,
    counters: Arc<RelayCounters>,
    /// Live one-shot per-reply pump tasks (pruned of finished handles on spawn).
    reply_tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl ReplyProxy {
    /// Poison-recovering lock — a panicked task must not wedge the map.
    fn correlator(&self) -> MutexGuard<'_, ReplyCorrelator> {
        self.correlator
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Expire one bridge reply topic: unsubscribe it on the device bus and count
    /// `relay_reply_expired`. Serves both the TTL sweep and the evict-oldest
    /// overflow path — §2.4 counts both as expiries.
    async fn expire(&self, bridge_topic: &str) {
        self.counters.reply_expired.fetch_add(1, Ordering::Relaxed);
        if let Err(e) = self
            .primary
            .unsubscribe(bridge_topic, Destination::Local)
            .await
        {
            tracing::warn!(topic = %bridge_topic, error = %e, "expired reply-topic unsubscribe failed");
        }
        tracing::debug!(topic = %bridge_topic, "reply correlation entry expired");
    }

    /// Run one forwardable downlink `cmd` through the §2.4 rewrite: mint +
    /// record + subscribe the bridge reply topic (handling eviction), returning
    /// the bytes to relay to the device bus — or `None` when the cmd must not be
    /// relayed because its reply subscription could not be established.
    async fn rewrite_downlink(self: &Arc<Self>, topic: &str, bytes: Vec<u8>) -> Option<Vec<u8>> {
        let action = self.correlator().rewrite_downlink(&bytes, Instant::now());
        let (bytes, bridge_topic, evicted) = match action {
            DownlinkRewrite::Rewritten {
                bytes,
                bridge_topic,
                evicted,
            } => (bytes, bridge_topic, evicted),
            DownlinkRewrite::Passthrough => {
                return Some(bytes); // fire-and-forget cmd — relayed as before
            }
            DownlinkRewrite::Drop(reason) => {
                self.counters.count_drop(reason);
                tracing::debug!(topic, ?reason, "downlink reply rewrite dropped cmd");
                return None;
            }
        };
        if let Some(oldest) = evicted {
            self.expire(&oldest).await;
        }
        // Subscribe BEFORE publishing the rewritten cmd so a fast responder
        // cannot reply into a not-yet-live subscription (the §2.4 sequence:
        // subscribe R_dev → map → publish). `max_messages = 1` gives the
        // library's own first-reply-wins / at-most-one-reply contract;
        // stragglers drop at the provider (debug-logged there).
        match self
            .primary
            .subscribe(&bridge_topic, Destination::Local, Qos::AtLeastOnce, 1)
            .await
        {
            Ok(sub) => {
                if self.correlator().contains(&bridge_topic) {
                    self.spawn_reply_pump(bridge_topic, sub);
                } else {
                    // Pathological ttl≈0: the sweep expired the entry between
                    // record and SUBACK. The reply path is already gone — still
                    // relay the cmd (same outcome as expiry-before-reply) but
                    // leave no dangling subscription behind.
                    drop(sub);
                    if let Err(e) = self
                        .primary
                        .unsubscribe(&bridge_topic, Destination::Local)
                        .await
                    {
                        tracing::warn!(topic = %bridge_topic, error = %e, "already-expired reply-topic unsubscribe failed");
                    }
                }
                Some(bytes)
            }
            Err(e) => {
                // Fail closed: executing a cmd whose reply can never return is
                // worse than not relaying it — the requester times out either
                // way and can retry.
                self.correlator().abandon(&bridge_topic);
                self.counters.publish_failed.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(topic, reply_topic = %bridge_topic, error = %e, "bridge reply-topic subscribe failed; cmd not relayed");
                None
            }
        }
    }

    /// Spawn the one-shot pump for one bridge reply topic: await the first
    /// message (or the subscription closing on expiry/shutdown) and resolve it.
    fn spawn_reply_pump(self: &Arc<Self>, bridge_topic: String, mut sub: Subscription) {
        let proxy = Arc::clone(self);
        let handle = tokio::spawn(async move {
            if let Some((_topic, payload)) = sub.recv().await {
                proxy.handle_reply(&bridge_topic, &payload).await;
            }
            // recv() == None: the subscription closed (TTL expiry / eviction /
            // shutdown already unsubscribed it) — nothing left to resolve.
        });
        let mut tasks = self
            .reply_tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        tasks.retain(|h| !h.is_finished());
        tasks.push(handle);
    }

    /// Resolve one message seen on a bridge reply topic: look up + remove the
    /// correlation entry, relay the prepared reply UP to the original site
    /// topic, and unsubscribe the bridge topic (one-shot, §2.4).
    async fn handle_reply(&self, bridge_topic: &str, payload: &[u8]) {
        let Some(site_topic) = self.correlator().take(bridge_topic) else {
            // Expired/evicted between delivery and resolution (the unsubscribe
            // already ran when the entry was torn down): a stray reply.
            self.counters.reply_stray.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(topic = %bridge_topic, "stray reply: no correlation entry; dropped");
            return;
        };
        match prepare_reply(&self.engine, payload) {
            ReplyRelay::Forward(bytes) => {
                match self
                    .site
                    .publish(&site_topic, bytes, Destination::Local, Qos::AtLeastOnce)
                    .await
                {
                    Ok(()) => {
                        self.counters.reply_relayed.fetch_add(1, Ordering::Relaxed);
                        tracing::debug!(from = %bridge_topic, to = %site_topic, "reply relayed to site");
                    }
                    Err(e) => {
                        self.counters.publish_failed.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(to = %site_topic, error = %e, "reply publish failed");
                    }
                }
            }
            ReplyRelay::Drop(reason) => {
                self.counters.count_drop(reason);
                tracing::debug!(topic = %bridge_topic, ?reason, "reply not relayed");
            }
        }
        if let Err(e) = self
            .primary
            .unsubscribe(bridge_topic, Destination::Local)
            .await
        {
            tracing::warn!(topic = %bridge_topic, error = %e, "settled reply-topic unsubscribe failed");
        }
    }
}

/// The §2.5 uplink-policy plumbing shared by the uplink pumps and the
/// connectivity watcher: the pure [`UplinkPolicy`] behind a lock, plus the site
/// handle and counters its verdicts act on. Pure decisions live in
/// [`crate::policy`]; this struct only pumps them.
struct UplinkGovernor {
    policy: Mutex<UplinkPolicy>,
    /// The site broker — admitted uplinks (and evt replays) publish here; its
    /// `connected()` is the §2.5 disconnect signal.
    site: Arc<dyn MessagingProvider>,
    counters: Arc<RelayCounters>,
}

impl UplinkGovernor {
    /// Poison-recovering lock — a panicked task must not wedge the policy.
    fn policy(&self) -> MutexGuard<'_, UplinkPolicy> {
        self.policy.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Record the outcome of one push into the `evt` replay buffer.
    fn record_push(&self, push: EvtPush, class: UnsClass) {
        match push {
            EvtPush::Stored { evicted_oldest } => {
                self.counters.evt_buffered.fetch_add(1, Ordering::Relaxed);
                if evicted_oldest {
                    self.counters
                        .evt_buffer_dropped
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::debug!("evt replay buffer full: oldest dropped (drop-oldest)");
                }
            }
            // Buffering disabled: the message drops like every other class
            // (only reachable via the failed-publish path — a Buffer verdict is
            // never issued with the buffer off).
            EvtPush::Rejected => self.counters.dropped_disconnected.incr(class),
        }
    }

    /// Run one decided-and-forwardable uplink message through the §2.5 policy,
    /// then publish / buffer / drop + count per the verdict. A **failed site
    /// publish is the disconnect path** (§2.5): `evt` buffers, everything else
    /// counts `dropped_disconnected`.
    async fn relay_up(&self, class: UnsClass, topic: String, bytes: Vec<u8>) {
        // One lock scope for admit + (possible) buffer push, so the decision is
        // atomic with respect to the watcher's replay drain. No await inside.
        {
            let mut policy = self.policy();
            match policy.admit(class, self.site.connected(), Instant::now()) {
                UplinkVerdict::Forward => {}
                UplinkVerdict::Buffer => {
                    tracing::debug!(
                        topic,
                        "evt buffered for replay (site link down or replay pending)"
                    );
                    let push = policy.push_evt(topic, bytes);
                    self.record_push(push, class);
                    return;
                }
                UplinkVerdict::DropDisabled => {
                    self.counters.dropped_disabled.incr(class);
                    tracing::debug!(
                        topic,
                        class = class.token(),
                        "uplink class disabled; dropped"
                    );
                    return;
                }
                UplinkVerdict::DropDisconnected => {
                    self.counters.dropped_disconnected.incr(class);
                    tracing::debug!(
                        topic,
                        class = class.token(),
                        "site link down; dropped (live path is not durable)"
                    );
                    return;
                }
                UplinkVerdict::DropRateCapped => {
                    self.counters.dropped_rate.incr(class);
                    tracing::debug!(
                        topic,
                        class = class.token(),
                        "over the class rate cap; dropped"
                    );
                    return;
                }
            }
        }
        // Forward: publish to the site broker. Keep an evt copy so a failed
        // publish can still buffer it (the failed publish == disconnect rule).
        let retained = (class == UnsClass::Evt).then(|| (topic.clone(), bytes.clone()));
        match self
            .site
            .publish(&topic, bytes, Destination::Local, Qos::AtLeastOnce)
            .await
        {
            Ok(()) => {
                self.counters.uplinked.incr(class);
                tracing::debug!(topic, "relayed up");
            }
            Err(e) => {
                tracing::warn!(topic, error = %e, "uplink publish failed; applying disconnect policy");
                match retained {
                    Some((topic, bytes)) => {
                        let push = self.policy().push_evt(topic, bytes);
                        self.record_push(push, class);
                    }
                    None => self.counters.dropped_disconnected.incr(class),
                }
            }
        }
    }

    /// Drain the `evt` replay buffer to the site broker, **strictly in order**,
    /// stopping at the first failed publish (the message is requeued at the
    /// front; the next watcher tick retries) or when the link drops again
    /// (D-B10 reconnect replay).
    async fn replay_buffered_evt(&self) {
        while self.site.connected() {
            let Some((topic, bytes)) = self.policy().pop_evt() else {
                return; // drained — the buffer clears as it empties
            };
            match self
                .site
                .publish(&topic, bytes.clone(), Destination::Local, Qos::AtLeastOnce)
                .await
            {
                Ok(()) => {
                    self.counters.evt_replayed.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(topic, "buffered evt replayed");
                }
                Err(e) => {
                    tracing::warn!(topic, error = %e, "evt replay publish failed; will retry");
                    if !self.policy().requeue_evt_front(topic, bytes) {
                        // The buffer refilled to its bound while this message
                        // was in flight: under drop-oldest it IS the victim.
                        self.counters
                            .evt_buffer_dropped
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    return;
                }
            }
        }
    }
}

/// The §2.8 observability wiring handed to [`RelayIo::start`]: the component's
/// `gg.metrics()` service plus the config snapshot the metric definitions stamp
/// their identity/namespace from. `None` runs the relay without the periodic
/// metric emission (tests; the counters stay readable in-process either way).
pub struct ObservabilityHook {
    /// `gg.metrics()` — the emission path (`metricEmission.target`, `messaging`
    /// in the shipped config → `ecv1/{device}/uns-bridge/metric/<name>`).
    pub metrics: Arc<dyn MetricService>,
    /// `gg.config()` — supplies namespace + thingName/componentName dimensions.
    pub config: Arc<Config>,
}

/// A point-in-time [`RelaySnapshot`] of the live counters + gauges (the pure
/// §2.8 mapping input).
fn take_snapshot(
    counters: &RelayCounters,
    pending_replies: usize,
    site_connected: bool,
) -> RelaySnapshot {
    RelaySnapshot {
        uplinked: counters.uplinked.snapshot(),
        dropped_disabled: counters.dropped_disabled.snapshot(),
        dropped_rate: counters.dropped_rate.snapshot(),
        dropped_disconnected: counters.dropped_disconnected.snapshot(),
        downlinked: counters.downlinked.load(Ordering::Relaxed),
        loop_dropped: counters.loop_dropped.load(Ordering::Relaxed),
        routed_dropped: counters.routed_dropped.load(Ordering::Relaxed),
        malformed_dropped: counters.malformed_dropped.load(Ordering::Relaxed),
        publish_failed: counters.publish_failed.load(Ordering::Relaxed),
        reply_relayed: counters.reply_relayed.load(Ordering::Relaxed),
        reply_expired: counters.reply_expired.load(Ordering::Relaxed),
        reply_stray: counters.reply_stray.load(Ordering::Relaxed),
        evt_buffered: counters.evt_buffered.load(Ordering::Relaxed),
        evt_buffer_dropped: counters.evt_buffer_dropped.load(Ordering::Relaxed),
        evt_replayed: counters.evt_replayed.load(Ordering::Relaxed),
        pending_replies: pending_replies as u64,
        site_connected,
    }
}

/// Publish the two §2.5 / DESIGN-uns §9.3 (layer 2) rehydration broadcasts on the
/// DEVICE bus — best-effort (a failed broadcast only degrades the site view's
/// rehydration; the evt replay still runs). Each is a notification-style `cmd`
/// envelope (no `reply_to`, empty body); the exact payload contract is finalized
/// with the 4-language library listener slice — until that lands, the broadcast
/// is inert (published, answered by nobody).
async fn publish_rehydration_bcast(device_bus: &Arc<dyn MessagingProvider>, topics: &[String; 2]) {
    for (cmd, topic) in REHYDRATION_CMDS.iter().zip(topics.iter()) {
        let bytes = match MessageBuilder::new(*cmd, "1.0")
            .payload(serde_json::json!({}))
            .build()
            .to_vec()
        {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::warn!(cmd, error = %e, "rehydration broadcast serialization failed");
                continue;
            }
        };
        match device_bus
            .publish(topic, bytes, Destination::Local, Qos::AtLeastOnce)
            .await
        {
            Ok(()) => {
                tracing::info!(topic = %topic, "rehydration broadcast published (site reconnect)")
            }
            Err(e) => {
                tracing::warn!(topic = %topic, error = %e, "rehydration broadcast publish failed (best-effort)")
            }
        }
    }
}

/// The running relay: pump tasks + the subscriptions they own, with counters.
pub struct RelayIo {
    engine: Arc<RelayEngine>,
    primary: Arc<dyn MessagingProvider>,
    site: Arc<dyn MessagingProvider>,
    counters: Arc<RelayCounters>,
    reply_proxy: Arc<ReplyProxy>,
    governor: Arc<UplinkGovernor>,
    tasks: Vec<JoinHandle<()>>,
}

impl RelayIo {
    /// Subscribe the §2.2 matrix and start the pumps:
    /// - UPLINK: the six class wildcards (+ `app` when opted in) on PRIMARY →
    ///   republish topic-verbatim to SITE;
    /// - DOWNLINK: `cmd` pinned to this bridge's own device on SITE → republish
    ///   to PRIMARY.
    ///
    /// Queue depths come from `queue` (`data` deep, others shallow — §2.2); the
    /// reply-proxy knobs (`ttlSecs`/`maxPending`) come from `reply` (§2.4); the
    /// per-class uplink policy (enables/rate caps/`evt` buffer — §2.5 / D-B10)
    /// comes from `uplink`. Periodic tasks run alongside the pumps: the §2.4 TTL
    /// sweep (`min(ttl/4, 5 s)`), the §2.5 connectivity watcher
    /// ([`CONNECTIVITY_POLL`]) — which publishes the two rehydration broadcasts
    /// on the DEVICE bus at the site-reconnect rising edge, then replays buffered
    /// `evt` — and, with an [`ObservabilityHook`], the §2.8 metric emission
    /// ([`METRIC_EMIT_INTERVAL`]).
    ///
    /// # Errors
    /// A failed SUBSCRIBE on either connection surfaces immediately (the bridge
    /// must not run half-subscribed).
    pub async fn start(
        engine: Arc<RelayEngine>,
        primary: Arc<dyn MessagingProvider>,
        site: Arc<dyn MessagingProvider>,
        queue: &QueueConfig,
        reply: &ReplyConfig,
        uplink: &UplinkConfig,
        observability: Option<ObservabilityHook>,
    ) -> edgecommons::Result<RelayIo> {
        let counters = Arc::new(RelayCounters::default());
        let reply_proxy = Arc::new(ReplyProxy {
            correlator: Mutex::new(ReplyCorrelator::new(reply.ttl(), reply.max_pending)),
            engine: Arc::clone(&engine),
            primary: Arc::clone(&primary),
            site: Arc::clone(&site),
            counters: Arc::clone(&counters),
            reply_tasks: Mutex::new(Vec::new()),
        });
        let policy = UplinkPolicy::from_config(uplink);
        {
            let disabled: Vec<&str> = POLICY_CLASSES
                .iter()
                .filter(|c| !policy.class_enabled(**c))
                .map(|c| c.token())
                .collect();
            let rate_capped: Vec<&str> = POLICY_CLASSES
                .iter()
                .filter(|c| policy.has_rate_cap(**c))
                .map(|c| c.token())
                .collect();
            tracing::info!(
                disabled_classes = ?disabled,
                rate_capped_classes = ?rate_capped,
                evt_buffer = policy.evt_buffer_capacity(),
                "uplink policy active"
            );
        }
        let governor = Arc::new(UplinkGovernor {
            policy: Mutex::new(policy),
            site: Arc::clone(&site),
            counters: Arc::clone(&counters),
        });
        let mut tasks = Vec::new();

        // UPLINK pumps: device bus → site broker, each through the §2.5 policy.
        for (cls, filter) in engine.uplink_subscriptions() {
            let depth = if *cls == UnsClass::Data {
                queue.data
            } else {
                queue.default_depth
            };
            let sub = primary
                .subscribe(filter, Destination::Local, Qos::AtLeastOnce, depth)
                .await?;
            tracing::info!(
                filter,
                depth,
                "uplink subscription established (device bus)"
            );
            tasks.push(tokio::spawn(pump(
                sub,
                Arc::clone(&engine),
                Arc::clone(&counters),
                PumpRole::Uplink {
                    class: *cls,
                    governor: Arc::clone(&governor),
                },
            )));
        }

        // DOWNLINK pump: site broker → device bus (own-device cmd only), running
        // each forwardable cmd through the §2.4 reply proxy.
        let downlink = engine.downlink_filter().to_string();
        let sub = site
            .subscribe(
                &downlink,
                Destination::Local,
                Qos::AtLeastOnce,
                queue.default_depth,
            )
            .await?;
        tracing::info!(filter = %downlink, "downlink subscription established (site broker)");
        tasks.push(tokio::spawn(pump(
            sub,
            Arc::clone(&engine),
            Arc::clone(&counters),
            PumpRole::Downlink {
                proxy: Arc::clone(&reply_proxy),
                device_bus: Arc::clone(&primary),
            },
        )));

        // The §2.5 connectivity watcher: on the RISING EDGE of `site.connected()`
        // (site reconnect) it publishes the two §9.3-layer-2 rehydration
        // broadcasts on the DEVICE bus (best-effort) and then replays the evt
        // buffer; otherwise it drains the buffer whenever the link is up and
        // something is queued (covers the publish-failed-while-nominally-
        // connected case, where no edge ever fires). The baseline is the
        // connected() state at start — the bridge only starts its relay after
        // main established the site link, so startup itself is not an edge
        // (§2.5: "on each site-connection RE-establishment").
        //
        // BRIDGE SIDE ONLY: the device-side `republish-*` listener that answers
        // the broadcast is a separate 4-language edgecommons library slice —
        // until it lands the broadcast is inert (see README "Reconnect
        // rehydration").
        {
            let governor = Arc::clone(&governor);
            let device_bus = Arc::clone(&primary);
            let rehydration = engine.rehydration_topics().clone();
            tasks.push(tokio::spawn(async move {
                let mut tick = tokio::time::interval(CONNECTIVITY_POLL);
                let mut was_connected = governor.site.connected();
                loop {
                    tick.tick().await;
                    let connected = governor.site.connected();
                    if connected && !was_connected {
                        // Rising edge: rehydration broadcast BEFORE the evt
                        // replay (§2.5 — buffered events replay after it).
                        publish_rehydration_bcast(&device_bus, &rehydration).await;
                        governor.replay_buffered_evt().await;
                    } else if connected {
                        // Bind the count first: the policy guard must never be
                        // held across the replay await.
                        let pending = governor.policy().buffered_evt();
                        if pending > 0 {
                            governor.replay_buffered_evt().await;
                        }
                    }
                    was_connected = connected;
                }
            }));
        }

        // The §2.8 metric emission (P3-4b): snapshot the counters every
        // METRIC_EMIT_INTERVAL and emit them through gg.metrics() — counters as
        // interval deltas, gauges as current values (the pure mapping + names
        // live in crate::observability). The messaging metric target puts them
        // on ecv1/{device}/uns-bridge/metric/<name>, where they match the
        // bridge's own uplink filters and ride its own relay to the site (§2.8).
        if let Some(hook) = observability {
            let counters = Arc::clone(&counters);
            let proxy = Arc::clone(&reply_proxy);
            let site = Arc::clone(&site);
            tasks.push(tokio::spawn(async move {
                for def in metric_definitions() {
                    let mut builder = MetricBuilder::create(def.name).with_config(&hook.config);
                    for measure in &def.measures {
                        builder = builder.add_measure(*measure, def.unit, 60);
                    }
                    hook.metrics.define_metric(builder.build());
                }
                let mut prev = RelaySnapshot::default();
                let mut tick = tokio::time::interval(METRIC_EMIT_INTERVAL);
                tick.tick().await; // consume the immediate tick — first emission after one interval
                loop {
                    tick.tick().await;
                    let curr =
                        take_snapshot(&counters, proxy.correlator().len(), site.connected());
                    for group in relay_metric_groups(&prev, &curr) {
                        if let Err(e) = hook.metrics.emit_metric(group.name, group.values).await {
                            // Best-effort: a down device bus must not kill the task.
                            tracing::debug!(metric = group.name, error = %e, "relay metric emit failed");
                        }
                    }
                    prev = curr;
                }
            }));
        }

        // The §2.4 TTL sweep: expire stale correlation entries (unsubscribe +
        // count) every min(ttl/4, 5 s).
        {
            let proxy = Arc::clone(&reply_proxy);
            let period = proxy.correlator().sweep_interval();
            tracing::info!(
                ttl_secs = reply.ttl_secs,
                max_pending = reply.max_pending,
                sweep_period = ?period,
                "reply correlation map active"
            );
            tasks.push(tokio::spawn(async move {
                let mut tick = tokio::time::interval(period);
                loop {
                    tick.tick().await;
                    let expired = proxy.correlator().sweep(Instant::now());
                    for topic in expired {
                        proxy.expire(&topic).await;
                    }
                }
            }));
        }

        Ok(RelayIo {
            engine,
            primary,
            site,
            counters,
            reply_proxy,
            governor,
            tasks,
        })
    }

    /// The live relay counters.
    pub fn counters(&self) -> &RelayCounters {
        &self.counters
    }

    /// The `relay_pending_replies` gauge (§2.5): in-flight correlation entries.
    pub fn pending_replies(&self) -> usize {
        self.reply_proxy.correlator().len()
    }

    /// The number of `evt` currently held in the D-B10 replay buffer.
    pub fn buffered_evt(&self) -> usize {
        self.governor.policy().buffered_evt()
    }

    /// Graceful stop: abort the pumps (incl. the sweep and the per-reply pumps),
    /// then **unsubscribe every filter at both brokers** — the §2.2 matrix plus
    /// every still-pending bridge reply topic (the unsubscribe-before-exit rule)
    /// — best-effort with warnings.
    pub async fn shutdown(self) {
        for task in &self.tasks {
            task.abort();
        }
        for task in self.tasks {
            let _ = task.await; // JoinError(Cancelled) is expected
        }
        let reply_tasks: Vec<JoinHandle<()>> = {
            let mut guard = self
                .reply_proxy
                .reply_tasks
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            guard.drain(..).collect()
        };
        for task in &reply_tasks {
            task.abort();
        }
        for task in reply_tasks {
            let _ = task.await;
        }
        for (_, filter) in self.engine.uplink_subscriptions() {
            if let Err(e) = self.primary.unsubscribe(filter, Destination::Local).await {
                tracing::warn!(filter, error = %e, "uplink unsubscribe failed");
            }
        }
        let downlink = self.engine.downlink_filter();
        if let Err(e) = self.site.unsubscribe(downlink, Destination::Local).await {
            tracing::warn!(filter = %downlink, error = %e, "downlink unsubscribe failed");
        }
        let pending = self.reply_proxy.correlator().drain(); // lock released here
        for topic in pending {
            if let Err(e) = self.primary.unsubscribe(&topic, Destination::Local).await {
                tracing::warn!(topic = %topic, error = %e, "pending reply-topic unsubscribe failed");
            }
        }
        let buffered = self.governor.policy().buffered_evt();
        if buffered > 0 {
            // Memory-only by design (D-B10): the live path is not durable.
            tracing::info!(buffered, "unreplayed buffered evt discarded at shutdown");
        }
        tracing::info!("relay stopped; all filters unsubscribed");
    }
}

/// What a pump does with a [`RelayDecision::Forward`]: uplinks pass the §2.5
/// policy governor; the downlink runs the §2.4 reply proxy, then publishes to
/// the device bus.
enum PumpRole {
    /// Device bus → site broker, one pump per relayed class (the class rides
    /// along so the policy and the per-class counters key correctly).
    Uplink {
        class: UnsClass,
        governor: Arc<UplinkGovernor>,
    },
    /// Site broker → device bus (own-device `cmd` only).
    Downlink {
        proxy: Arc<ReplyProxy>,
        device_bus: Arc<dyn MessagingProvider>,
    },
}

impl PumpRole {
    fn direction(&self) -> Direction {
        match self {
            PumpRole::Uplink { .. } => Direction::Uplink,
            PumpRole::Downlink { .. } => Direction::Downlink,
        }
    }
}

/// One subscription's pump: receive → [`RelayEngine::decide`] → hand the
/// forwardable bytes to the role's path (topic-verbatim republish either way).
/// Serial per subscription (ordered).
async fn pump(
    mut sub: Subscription,
    engine: Arc<RelayEngine>,
    counters: Arc<RelayCounters>,
    role: PumpRole,
) {
    let direction = role.direction();
    while let Some((topic, payload)) = sub.recv().await {
        match engine.decide(direction, &topic, &payload) {
            RelayDecision::Forward(bytes) => match &role {
                PumpRole::Uplink { class, governor } => {
                    governor.relay_up(*class, topic, bytes).await;
                }
                PumpRole::Downlink { proxy, device_bus } => {
                    let Some(bytes) = proxy.rewrite_downlink(&topic, bytes).await else {
                        continue; // reply subscription failed: fail closed
                    };
                    // Topic-verbatim republish; the provider's "Local" destination
                    // is the broker it was built against (here: the device bus).
                    match device_bus
                        .publish(&topic, bytes, Destination::Local, Qos::AtLeastOnce)
                        .await
                    {
                        Ok(()) => {
                            counters.downlinked.fetch_add(1, Ordering::Relaxed);
                            tracing::debug!(topic, "relayed down");
                        }
                        Err(e) => {
                            counters.publish_failed.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(topic, error = %e, "downlink publish failed");
                        }
                    }
                }
            },
            RelayDecision::Drop(reason) => {
                counters.count_drop(reason);
                tracing::debug!(topic, ?direction, ?reason, "not relayed");
            }
        }
    }
    tracing::debug!(?direction, "pump ended (subscription closed)");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relay::{DEFAULT_MAX_HOPS, RELAY_TAG};
    use async_trait::async_trait;
    use edgecommons::messaging::message::{Message, MessageBodyCase};
    use edgecommons::messaging::topic_matches;
    use serde_json::{json, Value};
    use std::sync::Mutex;
    use std::time::Duration;

    /// One registered fake subscription: the filter and its delivery channel.
    type FakeSub = (String, tokio::sync::mpsc::Sender<(String, Vec<u8>)>);

    /// In-memory fake `MessagingProvider` (the library's FakeProvider pattern):
    /// records publishes and routes them into matching registered subscriptions.
    /// Carries a `connected()` toggle (the §2.5 disconnect signal) and a
    /// forced-publish-failure toggle (the failed-publish == disconnect path).
    #[derive(Default)]
    struct FakeProvider {
        subs: Mutex<Vec<FakeSub>>,
        published: Mutex<Vec<(String, Vec<u8>)>>,
        unsubscribed: Mutex<Vec<String>>,
        /// When set, SUBSCRIBEs to filters starting with this prefix fail.
        fail_subscribe_prefix: Mutex<Option<String>>,
        /// Inverted so `Default` (false) means CONNECTED — matching the P3-3 fake.
        disconnected: std::sync::atomic::AtomicBool,
        /// When true, every publish fails (nothing recorded, nothing routed).
        fail_publish: std::sync::atomic::AtomicBool,
    }

    impl FakeProvider {
        fn published(&self) -> Vec<(String, Vec<u8>)> {
            self.published.lock().unwrap().clone()
        }
        fn unsubscribed(&self) -> Vec<String> {
            self.unsubscribed.lock().unwrap().clone()
        }
        /// The currently registered (still-subscribed) filters.
        fn subscriptions(&self) -> Vec<String> {
            self.subs
                .lock()
                .unwrap()
                .iter()
                .map(|(f, _)| f.clone())
                .collect()
        }
        /// Toggle the `connected()` signal (the site-link up/down seam).
        fn set_connected(&self, connected: bool) {
            self.disconnected.store(!connected, Ordering::SeqCst);
        }
        /// Toggle forced publish failure.
        fn set_fail_publish(&self, fail: bool) {
            self.fail_publish.store(fail, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl MessagingProvider for FakeProvider {
        async fn publish(
            &self,
            topic: &str,
            payload: Vec<u8>,
            _dest: Destination,
            _qos: Qos,
        ) -> edgecommons::Result<()> {
            if self.fail_publish.load(Ordering::SeqCst) {
                return Err(edgecommons::EdgeCommonsError::Messaging(format!(
                    "forced publish failure for {topic}"
                )));
            }
            self.published
                .lock()
                .unwrap()
                .push((topic.to_string(), payload.clone()));
            let subs = self.subs.lock().unwrap().clone();
            for (filter, tx) in subs {
                if topic_matches(&filter, topic) {
                    let _ = tx.try_send((topic.to_string(), payload.clone()));
                }
            }
            Ok(())
        }

        async fn subscribe(
            &self,
            filter: &str,
            _dest: Destination,
            _qos: Qos,
            max_messages: usize,
        ) -> edgecommons::Result<Subscription> {
            if let Some(prefix) = self.fail_subscribe_prefix.lock().unwrap().as_deref() {
                if filter.starts_with(prefix) {
                    return Err(edgecommons::EdgeCommonsError::Messaging(format!(
                        "forced subscribe failure for {filter}"
                    )));
                }
            }
            let (tx, rx) = tokio::sync::mpsc::channel(max_messages.max(1));
            self.subs.lock().unwrap().push((filter.to_string(), tx));
            Ok(Subscription::new(rx, Box::new(())))
        }

        async fn unsubscribe(&self, filter: &str, _dest: Destination) -> edgecommons::Result<()> {
            self.unsubscribed.lock().unwrap().push(filter.to_string());
            self.subs.lock().unwrap().retain(|(f, _)| f != filter);
            Ok(())
        }

        fn connected(&self) -> bool {
            !self.disconnected.load(Ordering::SeqCst)
        }
    }

    fn envelope() -> Vec<u8> {
        MessageBuilder::new("state", "1.0")
            .payload(json!({ "status": "RUNNING" }))
            .build()
            .to_vec()
            .unwrap()
    }

    fn decoded_envelope(bytes: &[u8]) -> Message {
        Message::from_slice(bytes).unwrap()
    }

    fn envelope_value(bytes: &[u8]) -> Value {
        serde_json::to_value(decoded_envelope(bytes)).unwrap()
    }

    async fn started() -> (RelayIo, Arc<FakeProvider>, Arc<FakeProvider>) {
        started_opts(ReplyConfig::default(), UplinkConfig::default()).await
    }

    async fn started_with(reply: ReplyConfig) -> (RelayIo, Arc<FakeProvider>, Arc<FakeProvider>) {
        started_opts(reply, UplinkConfig::default()).await
    }

    /// Start with an `uplink` policy block given as its config JSON.
    async fn started_uplink(uplink: Value) -> (RelayIo, Arc<FakeProvider>, Arc<FakeProvider>) {
        started_opts(
            ReplyConfig::default(),
            serde_json::from_value(uplink).unwrap(),
        )
        .await
    }

    async fn started_opts(
        reply: ReplyConfig,
        uplink: UplinkConfig,
    ) -> (RelayIo, Arc<FakeProvider>, Arc<FakeProvider>) {
        let engine =
            Arc::new(RelayEngine::new("gw-01", DEFAULT_MAX_HOPS, uplink.app_enabled()).unwrap());
        let device = Arc::new(FakeProvider::default());
        let site = Arc::new(FakeProvider::default());
        let io = RelayIo::start(
            engine,
            Arc::clone(&device) as Arc<dyn MessagingProvider>,
            Arc::clone(&site) as Arc<dyn MessagingProvider>,
            &QueueConfig::default(),
            &reply,
            &uplink,
            None, // metric emission is main-wired; the pure mapping has its own tests
        )
        .await
        .unwrap();
        (io, device, site)
    }

    /// Poll until `predicate` holds or ~2s elapse.
    async fn wait_until(mut predicate: impl FnMut() -> bool) -> bool {
        for _ in 0..200 {
            if predicate() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        predicate()
    }

    #[tokio::test]
    async fn uplink_relays_topic_verbatim_with_hop_tag() {
        let (io, device, site) = started().await;
        let topic = "ecv1/gw-01/opcua-adapter/main/state";
        device
            .publish(topic, envelope(), Destination::Local, Qos::AtLeastOnce)
            .await
            .unwrap();

        assert!(
            wait_until(|| !site.published().is_empty()).await,
            "uplink did not arrive"
        );
        let (site_topic, bytes) = site.published().remove(0);
        assert_eq!(site_topic, topic, "topic-verbatim relay");
        let v = envelope_value(&bytes);
        assert_eq!(v["tags"][RELAY_TAG], json!(["gw-01/uns-bridge"]));
        assert_eq!(v["body"]["status"], "RUNNING");
        assert_eq!(
            io.counters().uplinked.get(UnsClass::State),
            1,
            "uplinked counts per class"
        );
        assert_eq!(io.counters().uplinked.total(), 1);
        io.shutdown().await;
    }

    #[tokio::test]
    async fn loop_guard_stops_own_echo_end_to_end() {
        let (io, device, site) = started().await;
        // An envelope already stamped with this bridge's own hop id must never
        // reach the site broker (§5 test list: the loop-guard assertion).
        let stamped = MessageBuilder::new("state", "1.0")
            .payload(json!({}))
            .tag(RELAY_TAG, json!(["gw-01/uns-bridge"]))
            .build()
            .to_vec()
            .unwrap();
        device
            .publish(
                "ecv1/gw-01/c/main/state",
                stamped,
                Destination::Local,
                Qos::AtLeastOnce,
            )
            .await
            .unwrap();

        assert!(
            wait_until(|| io.counters().loop_dropped.load(Ordering::Relaxed) == 1).await,
            "loop drop not counted"
        );
        assert!(
            site.published().is_empty(),
            "own echo must not reach the site broker"
        );
        io.shutdown().await;
    }

    #[tokio::test]
    async fn downlink_relays_own_device_cmd_to_the_device_bus() {
        let (io, device, site) = started().await;
        let topic = "ecv1/gw-01/opcua-adapter/main/cmd/reload-config";
        site.publish(topic, envelope(), Destination::Local, Qos::AtLeastOnce)
            .await
            .unwrap();

        assert!(
            wait_until(|| !device.published().is_empty()).await,
            "downlink did not arrive"
        );
        let (dev_topic, bytes) = device.published().remove(0);
        assert_eq!(dev_topic, topic);
        let v = envelope_value(&bytes);
        assert_eq!(v["tags"][RELAY_TAG], json!(["gw-01/uns-bridge"]));
        assert_eq!(io.counters().downlinked.load(Ordering::Relaxed), 1);
        io.shutdown().await;
    }

    #[tokio::test]
    async fn downlink_ignores_other_devices_commands() {
        let (io, device, site) = started().await;
        // The pinned filter would not even match at a real broker; push straight
        // through the fake to prove the engine's own pinning also holds.
        site.publish(
            "ecv1/gw-02/opcua-adapter/main/cmd/reload-config",
            envelope(),
            Destination::Local,
            Qos::AtLeastOnce,
        )
        .await
        .unwrap();

        // gw-02's cmd does not match the gw-01-pinned filter, so nothing arrives.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(device.published().is_empty());
        assert_eq!(io.counters().downlinked.load(Ordering::Relaxed), 0);
        io.shutdown().await;
    }

    #[tokio::test]
    async fn relayed_cmd_is_not_echoed_back_up() {
        // Class disjointness (§2.3): the downlink republish of a cmd on the device
        // bus matches NO uplink filter, so a single bridge can never echo itself.
        let (io, device, site) = started().await;
        let topic = "ecv1/gw-01/opcua-adapter/main/cmd/ping";
        site.publish(topic, envelope(), Destination::Local, Qos::AtLeastOnce)
            .await
            .unwrap();

        assert!(wait_until(|| !device.published().is_empty()).await);
        tokio::time::sleep(Duration::from_millis(50)).await;
        // The site fake recorded exactly the test's own original publish — the
        // bridge added nothing (the relayed cmd on the device bus matches no
        // uplink filter).
        assert_eq!(site.published().len(), 1, "cmd must never be uplinked back");
        io.shutdown().await;
    }

    #[tokio::test]
    async fn non_protobuf_message_is_dropped_and_counted() {
        let (io, device, site) = started().await;
        let payload = serde_json::to_vec(&json!({ "temperature": 21.5 })).unwrap();
        let topic = "ecv1/gw-01/modbus-adapter/main/data/temp";
        device
            .publish(topic, payload.clone(), Destination::Local, Qos::AtLeastOnce)
            .await
            .unwrap();

        assert!(
            wait_until(|| io.counters().malformed_dropped.load(Ordering::Relaxed) == 1).await,
            "malformed protobuf drop not counted"
        );
        assert!(
            site.published().is_empty(),
            "foreign payload must not relay"
        );
        assert_eq!(io.counters().uplinked.get(UnsClass::Data), 0);
        io.shutdown().await;
    }

    #[tokio::test]
    async fn opaque_message_relays_with_body_bytes_intact() {
        let (io, device, site) = started().await;
        let topic = "ecv1/gw-01/camera/main/data/frame";
        let opaque = [0x00, 0x01, 0x02, 0xfe, 0xff];
        let payload = MessageBuilder::new("frame-preview", "1.0")
            .opaque_body(opaque, "application/octet-stream")
            .unwrap()
            .build()
            .to_vec()
            .unwrap();
        device
            .publish(topic, payload, Destination::Local, Qos::AtLeastOnce)
            .await
            .unwrap();

        assert!(
            wait_until(|| !site.published().is_empty()).await,
            "opaque uplink did not arrive"
        );
        let (site_topic, bytes) = site.published().remove(0);
        assert_eq!(site_topic, topic);
        let msg = decoded_envelope(&bytes);
        assert_eq!(msg.body_case(), MessageBodyCase::Opaque);
        assert_eq!(msg.opaque_body().unwrap().unwrap(), opaque);
        assert_eq!(
            msg.tags.unwrap().extra[RELAY_TAG],
            json!(["gw-01/uns-bridge"])
        );
        io.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_unsubscribes_every_filter_on_both_connections() {
        let (io, device, site) = started().await;
        io.shutdown().await;
        let dev_unsubs = device.unsubscribed();
        assert_eq!(
            dev_unsubs,
            vec![
                "ecv1/+/+/+/state",
                "ecv1/+/+/+/cfg",
                "ecv1/+/+/+/evt/#",
                "ecv1/+/+/+/metric/#",
                "ecv1/+/+/+/data/#",
                "ecv1/+/+/+/log/#",
            ]
        );
        assert_eq!(site.unsubscribed(), vec!["ecv1/gw-01/+/+/cmd/#"]);
    }

    // ---- the §2.4 reply proxy (P3-3) ----

    const CMD_TOPIC: &str = "ecv1/gw-01/opcua-adapter/main/cmd/reload-config";
    const SITE_REPLY: &str = "edgecommons/reply-site-original";
    const BRIDGE_PREFIX: &str = "edgecommons/reply-";

    fn cmd_with_reply(reply_to: &str, corr: &str) -> Vec<u8> {
        MessageBuilder::new("reload-config", "1.0")
            .payload(json!({ "why": "test" }))
            .correlation_id(corr)
            .reply_to(reply_to)
            .build()
            .to_vec()
            .unwrap()
    }

    /// The `header.reply_to` of a relayed envelope.
    fn reply_to_of(bytes: &[u8]) -> String {
        decoded_envelope(bytes)
            .header
            .reply_to
            .expect("relayed cmd must carry reply_to")
    }

    /// Relay one cmd-with-reply_to downlink and return its bridge reply topic.
    async fn downlink_request(
        device: &Arc<FakeProvider>,
        site: &Arc<FakeProvider>,
        site_reply: &str,
        corr: &str,
    ) -> String {
        let already = device.published().len();
        site.publish(
            CMD_TOPIC,
            cmd_with_reply(site_reply, corr),
            Destination::Local,
            Qos::AtLeastOnce,
        )
        .await
        .unwrap();
        assert!(
            wait_until(|| device.published().len() > already).await,
            "downlink cmd did not arrive on the device bus"
        );
        reply_to_of(&device.published()[already].1)
    }

    #[tokio::test]
    async fn downlink_cmd_with_reply_to_gets_rewritten_and_a_reply_subscription() {
        let (io, device, site) = started().await;
        let bridge_topic = downlink_request(&device, &site, SITE_REPLY, "corr-1").await;

        // The relayed cmd: topic-verbatim, reply_to rewritten to a bridge-minted
        // topic (the core's standard prefix), correlation + hop tag intact.
        let (dev_topic, bytes) = device.published().remove(0);
        assert_eq!(dev_topic, CMD_TOPIC, "topic-verbatim relay");
        assert!(bridge_topic.starts_with(BRIDGE_PREFIX));
        assert_ne!(
            bridge_topic, SITE_REPLY,
            "the site reply topic must not leak down"
        );
        let v = envelope_value(&bytes);
        assert_eq!(v["header"]["correlation_id"], "corr-1");
        assert_eq!(v["body"]["why"], "test");
        assert_eq!(v["tags"][RELAY_TAG], json!(["gw-01/uns-bridge"]));

        // The bridge reply topic is subscribed on the DEVICE bus, and the
        // correlation entry is live.
        assert!(
            device.subscriptions().contains(&bridge_topic),
            "bridge reply topic must be subscribed on the device bus"
        );
        assert_eq!(io.pending_replies(), 1);
        io.shutdown().await;
    }

    #[tokio::test]
    async fn reply_round_trip_relays_to_the_original_site_topic() {
        let (io, device, site) = started().await;
        let bridge_topic = downlink_request(&device, &site, SITE_REPLY, "corr-1").await;

        // The responder replies on the bridge topic (device bus), correlation id
        // preserved, no reply_to of its own.
        let reply = MessageBuilder::new("reload-config-reply", "1.0")
            .payload(json!({ "ok": true, "n": 42 }))
            .correlation_id("corr-1")
            .build()
            .to_vec()
            .unwrap();
        device
            .publish(&bridge_topic, reply, Destination::Local, Qos::AtLeastOnce)
            .await
            .unwrap();

        // The reply lands on the ORIGINAL site reply topic after protobuf
        // decode/re-encode: correlation id + body preserved, reply_to absent.
        assert!(
            wait_until(|| site.published().iter().any(|(t, _)| t == SITE_REPLY)).await,
            "reply did not reach the site broker"
        );
        let (_, bytes) = site
            .published()
            .into_iter()
            .find(|(t, _)| t == SITE_REPLY)
            .unwrap();
        let v = envelope_value(&bytes);
        assert_eq!(
            v["header"]["correlation_id"], "corr-1",
            "correlation_id preserved"
        );
        assert_eq!(v["body"], json!({ "ok": true, "n": 42 }), "body verbatim");
        assert!(
            v["header"].get("reply_to").is_none(),
            "a relayed reply carries no reply_to"
        );
        assert_eq!(v["tags"][RELAY_TAG], json!(["gw-01/uns-bridge"]));

        // One-shot cleanup: entry removed + bridge topic unsubscribed + counted.
        assert!(
            wait_until(|| device.unsubscribed().contains(&bridge_topic)).await,
            "bridge reply topic must be unsubscribed after settling"
        );
        assert_eq!(io.pending_replies(), 0);
        assert_eq!(io.counters().reply_relayed.load(Ordering::Relaxed), 1);
        assert_eq!(io.counters().reply_expired.load(Ordering::Relaxed), 0);
        io.shutdown().await;
    }

    #[tokio::test]
    async fn downlink_cmd_without_reply_to_is_relayed_untouched() {
        let (io, device, site) = started().await;
        // envelope() carries no reply_to: a fire-and-forget notification cmd.
        site.publish(CMD_TOPIC, envelope(), Destination::Local, Qos::AtLeastOnce)
            .await
            .unwrap();

        assert!(wait_until(|| !device.published().is_empty()).await);
        let (_, bytes) = device.published().remove(0);
        let v = envelope_value(&bytes);
        assert!(
            v["header"].get("reply_to").is_none(),
            "no reply_to must be minted"
        );
        assert_eq!(
            io.pending_replies(),
            0,
            "no correlation entry for a notification cmd"
        );
        assert!(
            !device
                .subscriptions()
                .iter()
                .any(|f| f.starts_with(BRIDGE_PREFIX)),
            "no bridge reply subscription for a notification cmd"
        );
        io.shutdown().await;
    }

    #[tokio::test]
    async fn ttl_expiry_unsubscribes_the_bridge_topic_and_counts() {
        // ttlSecs 0: the entry is expired the moment it is recorded; the sweep
        // (100 ms floor cadence) tears it down.
        let (io, device, site) = started_with(ReplyConfig {
            ttl_secs: 0,
            max_pending: 1024,
        })
        .await;
        let bridge_topic = downlink_request(&device, &site, SITE_REPLY, "corr-1").await;

        assert!(
            wait_until(|| io.counters().reply_expired.load(Ordering::Relaxed) == 1).await,
            "expiry not counted"
        );
        assert!(
            wait_until(|| device.unsubscribed().contains(&bridge_topic)).await,
            "expired bridge topic must be unsubscribed on the device bus"
        );
        assert_eq!(io.pending_replies(), 0);

        // A reply arriving after expiry goes nowhere near the site broker.
        device
            .publish(
                &bridge_topic,
                envelope(),
                Destination::Local,
                Qos::AtLeastOnce,
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            site.published().iter().all(|(t, _)| t != SITE_REPLY),
            "an expired reply path must not deliver"
        );
        assert_eq!(io.counters().reply_relayed.load(Ordering::Relaxed), 0);
        io.shutdown().await;
    }

    #[tokio::test]
    async fn max_pending_overflow_evicts_the_oldest_entry() {
        let (io, device, site) = started_with(ReplyConfig {
            ttl_secs: 60,
            max_pending: 2,
        })
        .await;
        let bridge0 = downlink_request(&device, &site, "edgecommons/reply-site-0", "c0").await;
        let bridge1 = downlink_request(&device, &site, "edgecommons/reply-site-1", "c1").await;
        assert_eq!(io.pending_replies(), 2);

        // The third request overflows the map: the OLDEST entry (bridge0) is
        // evicted — expired early, unsubscribed, counted.
        let _bridge2 = downlink_request(&device, &site, "edgecommons/reply-site-2", "c2").await;
        assert!(
            wait_until(|| io.counters().reply_expired.load(Ordering::Relaxed) == 1).await,
            "eviction must count as reply_expired"
        );
        assert_eq!(io.pending_replies(), 2, "the bound holds");
        assert!(
            wait_until(|| device.unsubscribed().contains(&bridge0)).await,
            "the evicted (oldest) bridge topic must be unsubscribed"
        );
        assert!(
            !device.unsubscribed().contains(&bridge1),
            "younger entries survive"
        );

        // The surviving second entry still round-trips.
        let reply = MessageBuilder::new("r", "1.0")
            .payload(json!({ "ok": 1 }))
            .correlation_id("c1")
            .build()
            .to_vec()
            .unwrap();
        device
            .publish(&bridge1, reply, Destination::Local, Qos::AtLeastOnce)
            .await
            .unwrap();
        assert!(
            wait_until(|| site
                .published()
                .iter()
                .any(|(t, _)| t == "edgecommons/reply-site-1"))
            .await,
            "a surviving entry must still relay its reply"
        );
        io.shutdown().await;
    }

    #[tokio::test]
    async fn failed_reply_subscription_fails_closed_and_abandons_the_entry() {
        let (io, device, site) = started().await;
        // The bridge cannot subscribe the minted reply topic: executing a cmd
        // whose reply can never return is worse than not relaying it.
        *device.fail_subscribe_prefix.lock().unwrap() = Some(BRIDGE_PREFIX.to_string());
        site.publish(
            CMD_TOPIC,
            cmd_with_reply(SITE_REPLY, "corr-1"),
            Destination::Local,
            Qos::AtLeastOnce,
        )
        .await
        .unwrap();

        assert!(
            wait_until(|| io.counters().publish_failed.load(Ordering::Relaxed) == 1).await,
            "the failed reply path must be counted"
        );
        assert!(
            device.published().is_empty(),
            "the cmd must NOT be relayed (fail closed)"
        );
        assert_eq!(
            io.pending_replies(),
            0,
            "the abandoned entry must not linger to expiry"
        );
        io.shutdown().await;
    }

    #[tokio::test]
    async fn stray_reply_with_no_entry_is_dropped_and_counted() {
        let (io, _device, site) = started().await;
        // The stray window: a reply delivered just before expiry/eviction tore
        // the entry down — drive the resolution path directly with no entry.
        io.reply_proxy
            .handle_reply("edgecommons/reply-never-recorded", &envelope())
            .await;
        assert_eq!(io.counters().reply_stray.load(Ordering::Relaxed), 1);
        assert!(
            site.published().is_empty(),
            "a stray reply must not reach the site broker"
        );
        assert_eq!(io.counters().reply_relayed.load(Ordering::Relaxed), 0);
        io.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_unsubscribes_pending_bridge_reply_topics() {
        let (io, device, site) = started().await;
        let bridge_topic = downlink_request(&device, &site, SITE_REPLY, "corr-1").await;
        assert_eq!(io.pending_replies(), 1);
        io.shutdown().await;
        assert!(
            device.unsubscribed().contains(&bridge_topic),
            "an in-flight bridge reply topic must be unsubscribed at shutdown"
        );
    }

    // ---- the §2.5 uplink policy + D-B10 disconnect behavior (P3-4) ----

    const EVT_TOPIC: &str = "ecv1/gw-01/opcua-adapter/main/evt/alarm";

    fn evt_envelope(n: u64) -> Vec<u8> {
        MessageBuilder::new("alarm", "1.0")
            .payload(json!({ "n": n }))
            .build()
            .to_vec()
            .unwrap()
    }

    /// The `body.n` markers of every evt the site broker received, in order.
    fn evt_ns_at_site(site: &FakeProvider) -> Vec<u64> {
        site.published()
            .iter()
            .filter(|(t, _)| t == EVT_TOPIC)
            .map(|(_, b)| {
                let v = envelope_value(b);
                v["body"]["n"].as_u64().unwrap()
            })
            .collect()
    }

    #[tokio::test]
    async fn disabled_class_drops_and_counts_while_others_flow() {
        let (io, device, site) =
            started_uplink(json!({ "classes": { "log": { "enabled": false } } })).await;
        device
            .publish(
                "ecv1/gw-01/c/main/log/tail",
                envelope(),
                Destination::Local,
                Qos::AtLeastOnce,
            )
            .await
            .unwrap();
        assert!(
            wait_until(|| io.counters().dropped_disabled.get(UnsClass::Log) == 1).await,
            "the disabled-class drop must be counted"
        );
        assert!(
            site.published().is_empty(),
            "a disabled class must not reach the site broker"
        );

        // The other classes are unaffected.
        device
            .publish(
                "ecv1/gw-01/c/main/state",
                envelope(),
                Destination::Local,
                Qos::AtLeastOnce,
            )
            .await
            .unwrap();
        assert!(
            wait_until(|| !site.published().is_empty()).await,
            "state must still relay"
        );
        assert_eq!(io.counters().uplinked.total(), 1);
        assert_eq!(io.counters().dropped_disabled.total(), 1);
        io.shutdown().await;
    }

    #[tokio::test]
    async fn app_opt_in_relays_through_engine_and_policy() {
        // Default: app is not even subscribed (engine-level; pinned by the relay
        // tests). Opted in: the seventh filter exists AND the policy forwards.
        let (io, device, site) =
            started_uplink(json!({ "classes": { "app": { "enabled": true } } })).await;
        let topic = "ecv1/gw-01/my-app/main/app/chatter";
        device
            .publish(topic, envelope(), Destination::Local, Qos::AtLeastOnce)
            .await
            .unwrap();
        assert!(
            wait_until(|| !site.published().is_empty()).await,
            "opted-in app must relay"
        );
        assert_eq!(site.published()[0].0, topic);
        io.shutdown().await;
    }

    #[tokio::test]
    async fn rate_capped_class_drops_excess_deterministically_and_counts() {
        // rate 0 / burst 1: exactly one token, never refilled — deterministic
        // regardless of wall-clock timing (the fine-grained refill math is
        // pinned by the pure policy tests with an injected clock).
        let (io, device, site) = started_uplink(json!({
            "classes": { "data": { "maxRatePerSec": 0, "burst": 1 } }
        }))
        .await;
        let topic = "ecv1/gw-01/modbus-adapter/main/data/temp";
        for _ in 0..3 {
            device
                .publish(topic, envelope(), Destination::Local, Qos::AtLeastOnce)
                .await
                .unwrap();
        }
        assert!(
            wait_until(|| io.counters().dropped_rate.get(UnsClass::Data) == 2).await,
            "the over-cap drops must be counted"
        );
        assert_eq!(
            site.published().len(),
            1,
            "only the burst token's message passes"
        );
        assert_eq!(io.counters().uplinked.get(UnsClass::Data), 1);
        io.shutdown().await;
    }

    #[tokio::test]
    async fn disconnect_drops_non_evt_per_class_and_buffers_evt() {
        let (io, device, site) = started().await;
        site.set_connected(false);

        device
            .publish(
                "ecv1/gw-01/c/main/state",
                envelope(),
                Destination::Local,
                Qos::AtLeastOnce,
            )
            .await
            .unwrap();
        device
            .publish(
                "ecv1/gw-01/c/main/data/temp",
                envelope(),
                Destination::Local,
                Qos::AtLeastOnce,
            )
            .await
            .unwrap();
        device
            .publish(
                EVT_TOPIC,
                evt_envelope(1),
                Destination::Local,
                Qos::AtLeastOnce,
            )
            .await
            .unwrap();

        assert!(
            wait_until(|| io.counters().dropped_disconnected.total() == 2
                && io.counters().evt_buffered.load(Ordering::Relaxed) == 1)
            .await,
            "disconnect drops + the evt buffering must be counted"
        );
        assert_eq!(io.counters().dropped_disconnected.get(UnsClass::State), 1);
        assert_eq!(io.counters().dropped_disconnected.get(UnsClass::Data), 1);
        assert_eq!(
            io.counters().dropped_disconnected.get(UnsClass::Evt),
            0,
            "evt buffers instead"
        );
        assert_eq!(io.buffered_evt(), 1);
        assert!(
            site.published().is_empty(),
            "nothing reaches a down site link"
        );
        io.shutdown().await;
    }

    #[tokio::test]
    async fn reconnect_replays_buffered_evt_in_order_then_clears() {
        let (io, device, site) = started().await;
        site.set_connected(false);
        for n in 1..=3 {
            device
                .publish(
                    EVT_TOPIC,
                    evt_envelope(n),
                    Destination::Local,
                    Qos::AtLeastOnce,
                )
                .await
                .unwrap();
        }
        assert!(
            wait_until(|| io.buffered_evt() == 3).await,
            "the three evt must buffer while disconnected"
        );

        site.set_connected(true);
        assert!(
            wait_until(|| io.counters().evt_replayed.load(Ordering::Relaxed) == 3).await,
            "the buffered evt must replay on reconnect"
        );
        assert_eq!(
            evt_ns_at_site(&site),
            vec![1, 2, 3],
            "replay is strictly in order"
        );
        assert_eq!(io.buffered_evt(), 0, "the buffer clears after replay");
        // The replayed bytes are the hop-stamped forward bytes (topic-verbatim
        // + the §2.3 hop tag — stamped before buffering).
        let (topic, bytes) = site.published().remove(0);
        assert_eq!(topic, EVT_TOPIC);
        let v = envelope_value(&bytes);
        assert_eq!(v["tags"][RELAY_TAG], json!(["gw-01/uns-bridge"]));
        io.shutdown().await;
    }

    #[tokio::test]
    async fn evt_buffer_respects_max_dropping_oldest() {
        let (io, device, site) = started_uplink(json!({
            "classes": { "evt": { "bufferWhileDisconnected": { "maxMessages": 2 } } }
        }))
        .await;
        site.set_connected(false);
        for n in 1..=3 {
            device
                .publish(
                    EVT_TOPIC,
                    evt_envelope(n),
                    Destination::Local,
                    Qos::AtLeastOnce,
                )
                .await
                .unwrap();
        }
        assert!(
            wait_until(|| io.counters().evt_buffer_dropped.load(Ordering::Relaxed) == 1).await,
            "the overflow eviction must be counted"
        );
        assert_eq!(io.buffered_evt(), 2, "the bound holds");

        site.set_connected(true);
        assert!(
            wait_until(|| io.counters().evt_replayed.load(Ordering::Relaxed) == 2).await,
            "the surviving evt must replay"
        );
        assert_eq!(
            evt_ns_at_site(&site),
            vec![2, 3],
            "the OLDEST was the drop victim"
        );
        io.shutdown().await;
    }

    #[tokio::test]
    async fn evt_buffer_disabled_drops_on_disconnect_like_any_class() {
        let (io, device, site) = started_uplink(json!({
            "classes": { "evt": { "bufferWhileDisconnected": { "enabled": false } } }
        }))
        .await;
        site.set_connected(false);
        device
            .publish(
                EVT_TOPIC,
                evt_envelope(1),
                Destination::Local,
                Qos::AtLeastOnce,
            )
            .await
            .unwrap();
        assert!(
            wait_until(|| io.counters().dropped_disconnected.get(UnsClass::Evt) == 1).await,
            "with the buffer off, evt drops + counts like every class"
        );
        assert_eq!(io.buffered_evt(), 0);
        assert_eq!(io.counters().evt_buffered.load(Ordering::Relaxed), 0);
        io.shutdown().await;
    }

    #[tokio::test]
    async fn failed_site_publish_is_the_disconnect_path() {
        // connected() still true, but publishes fail: evt buffers, non-evt
        // counts dropped_disconnected (§2.5 "or a publish fails").
        let (io, device, site) = started().await;
        site.set_fail_publish(true);

        device
            .publish(
                "ecv1/gw-01/c/main/state",
                envelope(),
                Destination::Local,
                Qos::AtLeastOnce,
            )
            .await
            .unwrap();
        device
            .publish(
                EVT_TOPIC,
                evt_envelope(7),
                Destination::Local,
                Qos::AtLeastOnce,
            )
            .await
            .unwrap();
        assert!(
            wait_until(
                || io.counters().dropped_disconnected.get(UnsClass::State) == 1
                    && io.counters().evt_buffered.load(Ordering::Relaxed) == 1
            )
            .await,
            "a failed publish must drop-count non-evt and buffer evt"
        );

        // Once publishes work again the watcher drains the buffer (connected()
        // never flipped — the buffered>0 check covers this case too).
        site.set_fail_publish(false);
        assert!(
            wait_until(|| io.counters().evt_replayed.load(Ordering::Relaxed) == 1).await,
            "the buffered evt must replay once publishes succeed"
        );
        assert_eq!(evt_ns_at_site(&site), vec![7]);
        assert_eq!(io.buffered_evt(), 0);
        io.shutdown().await;
    }

    #[tokio::test]
    async fn replay_failure_requeues_front_and_retries_in_order() {
        let (io, device, site) = started().await;
        site.set_connected(false);
        for n in 1..=2 {
            device
                .publish(
                    EVT_TOPIC,
                    evt_envelope(n),
                    Destination::Local,
                    Qos::AtLeastOnce,
                )
                .await
                .unwrap();
        }
        assert!(wait_until(|| io.buffered_evt() == 2).await);

        // Link "up" but publishes still failing: each watcher tick pops the
        // oldest, fails, and requeues it at the front — nothing replays,
        // nothing is lost, order is preserved.
        site.set_fail_publish(true);
        site.set_connected(true);
        tokio::time::sleep(Duration::from_millis(600)).await; // a few watcher ticks
        assert_eq!(io.counters().evt_replayed.load(Ordering::Relaxed), 0);
        assert!(site.published().is_empty());

        site.set_fail_publish(false);
        assert!(
            wait_until(|| io.counters().evt_replayed.load(Ordering::Relaxed) == 2).await,
            "the retried replay must drain the buffer"
        );
        assert_eq!(
            evt_ns_at_site(&site),
            vec![1, 2],
            "requeue-front preserves order"
        );
        assert_eq!(io.buffered_evt(), 0);
        assert_eq!(io.counters().evt_buffer_dropped.load(Ordering::Relaxed), 0);
        io.shutdown().await;
    }

    // ---- the §2.5 / §9.3-layer-2 reconnect rehydration broadcast (P3-4b) ----

    const BCAST_STATE: &str = "ecv1/gw-01/_bcast/cmd/republish-state";
    const BCAST_CFG: &str = "ecv1/gw-01/_bcast/cmd/republish-cfg";

    #[tokio::test]
    async fn reconnect_publishes_the_two_rehydration_bcasts_then_replays_evt() {
        let (io, device, site) = started().await;
        site.set_connected(false);
        device
            .publish(
                EVT_TOPIC,
                evt_envelope(1),
                Destination::Local,
                Qos::AtLeastOnce,
            )
            .await
            .unwrap();
        assert!(
            wait_until(|| io.buffered_evt() == 1).await,
            "the evt must buffer while down"
        );
        let before = device.published().len(); // the test's own evt publish

        site.set_connected(true);
        assert!(
            wait_until(|| io.counters().evt_replayed.load(Ordering::Relaxed) == 1).await,
            "the buffered evt must replay after the rising edge"
        );

        // The DEVICE bus saw exactly the two broadcasts, in REHYDRATION_CMDS
        // order, published on the rising edge (the code path runs them BEFORE
        // the evt replay).
        let bcasts: Vec<(String, Vec<u8>)> = device.published().split_off(before);
        let topics: Vec<&str> = bcasts.iter().map(|(t, _)| t.as_str()).collect();
        assert_eq!(topics, vec![BCAST_STATE, BCAST_CFG]);
        // Notification-style cmd envelopes: named, no reply_to, empty body.
        let v = envelope_value(&bcasts[0].1);
        assert_eq!(v["header"]["name"], "republish-state");
        assert!(
            v["header"].get("reply_to").is_none(),
            "a broadcast expects no reply"
        );
        assert_eq!(v["body"], json!({}));
        let v = envelope_value(&bcasts[1].1);
        assert_eq!(v["header"]["name"], "republish-cfg");

        // And nothing echoed back up: cmd matches no uplink filter (§2.2).
        assert_eq!(evt_ns_at_site(&site), vec![1]);
        assert!(site.published().iter().all(|(t, _)| !t.contains("_bcast")));
        io.shutdown().await;
    }

    #[tokio::test]
    async fn no_rehydration_bcast_without_a_reconnect_edge() {
        // Startup with a connected site link is NOT an edge (the baseline is the
        // state at start), and neither is a mere failed-publish recovery.
        let (io, device, site) = started().await;
        tokio::time::sleep(Duration::from_millis(600)).await; // several watcher ticks
        assert!(
            device.published().is_empty(),
            "no broadcast at plain startup"
        );

        // Failed-publish recovery without a connected() edge: the evt drains via
        // the pending>0 branch — still no broadcast.
        site.set_fail_publish(true);
        device
            .publish(
                EVT_TOPIC,
                evt_envelope(1),
                Destination::Local,
                Qos::AtLeastOnce,
            )
            .await
            .unwrap();
        assert!(wait_until(|| io.buffered_evt() == 1).await);
        site.set_fail_publish(false);
        assert!(wait_until(|| io.counters().evt_replayed.load(Ordering::Relaxed) == 1).await);
        assert_eq!(
            device.published().len(),
            1,
            "only the test's own evt publish — no broadcast without a rising edge"
        );
        io.shutdown().await;
    }
}
