//! # io — the relay pumps over two raw providers
//!
//! **One-liner purpose**: Wire the pure [`RelayEngine`](crate::relay::RelayEngine)
//! between two raw [`MessagingProvider`] handles — PRIMARY (the device bus) and
//! SITE (the reused core `MqttProvider` against the site broker) — and pump bytes.
//!
//! The relay runs at the **raw provider level** (`publish`/`subscribe` bytes,
//! DESIGN-uns-bridge §1.3): the reserved-class guard is a `MessagingService`
//! concern and is simply not in this path — byte relay, not a client-chosen
//! enveloped publish. The site broker's per-device ACL is the durable boundary.
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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Instant;

use ggcommons::messaging::{Destination, MessagingProvider, Qos, Subscription};
use tokio::task::JoinHandle;

use crate::config::{QueueConfig, ReplyConfig};
use crate::relay::{Direction, DropReason, RelayDecision, RelayEngine};
use crate::reply::{prepare_reply, DownlinkRewrite, ReplyCorrelator, ReplyRelay};
use ggcommons::uns::UnsClass;

/// Relay counters (observable seams for tests now; surfaced as `metric`s through
/// the normal metric subsystem in P3-4 — §2.5 table).
#[derive(Debug, Default)]
pub struct RelayCounters {
    /// Messages relayed device → site.
    pub uplinked: AtomicU64,
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
        self.correlator.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Expire one bridge reply topic: unsubscribe it on the device bus and count
    /// `relay_reply_expired`. Serves both the TTL sweep and the evict-oldest
    /// overflow path — §2.4 counts both as expiries.
    async fn expire(&self, bridge_topic: &str) {
        self.counters.reply_expired.fetch_add(1, Ordering::Relaxed);
        if let Err(e) = self.primary.unsubscribe(bridge_topic, Destination::Local).await {
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
        let DownlinkRewrite::Rewritten { bytes, bridge_topic, evicted } = action else {
            return Some(bytes); // fire-and-forget cmd / raw — relayed as before
        };
        if let Some(oldest) = evicted {
            self.expire(&oldest).await;
        }
        // Subscribe BEFORE publishing the rewritten cmd so a fast responder
        // cannot reply into a not-yet-live subscription (the §2.4 sequence:
        // subscribe R_dev → map → publish). `max_messages = 1` gives the
        // library's own first-reply-wins / at-most-one-reply contract;
        // stragglers drop at the provider (debug-logged there).
        match self.primary.subscribe(&bridge_topic, Destination::Local, Qos::AtLeastOnce, 1).await {
            Ok(sub) => {
                if self.correlator().contains(&bridge_topic) {
                    self.spawn_reply_pump(bridge_topic, sub);
                } else {
                    // Pathological ttl≈0: the sweep expired the entry between
                    // record and SUBACK. The reply path is already gone — still
                    // relay the cmd (same outcome as expiry-before-reply) but
                    // leave no dangling subscription behind.
                    drop(sub);
                    if let Err(e) =
                        self.primary.unsubscribe(&bridge_topic, Destination::Local).await
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
        let mut tasks = self.reply_tasks.lock().unwrap_or_else(PoisonError::into_inner);
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
                match self.site.publish(&site_topic, bytes, Destination::Local, Qos::AtLeastOnce).await
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
        if let Err(e) = self.primary.unsubscribe(bridge_topic, Destination::Local).await {
            tracing::warn!(topic = %bridge_topic, error = %e, "settled reply-topic unsubscribe failed");
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
    /// reply-proxy knobs (`ttlSecs`/`maxPending`) come from `reply` (§2.4), and a
    /// sweep task ticking every `min(ttl/4, 5 s)` expires stale correlation
    /// entries.
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
    ) -> ggcommons::Result<RelayIo> {
        let counters = Arc::new(RelayCounters::default());
        let reply_proxy = Arc::new(ReplyProxy {
            correlator: Mutex::new(ReplyCorrelator::new(reply.ttl(), reply.max_pending)),
            engine: Arc::clone(&engine),
            primary: Arc::clone(&primary),
            site: Arc::clone(&site),
            counters: Arc::clone(&counters),
            reply_tasks: Mutex::new(Vec::new()),
        });
        let mut tasks = Vec::new();

        // UPLINK pumps: device bus → site broker.
        for (cls, filter) in engine.uplink_subscriptions() {
            let depth = if *cls == UnsClass::Data { queue.data } else { queue.default_depth };
            let sub = primary
                .subscribe(filter, Destination::Local, Qos::AtLeastOnce, depth)
                .await?;
            tracing::info!(filter, depth, "uplink subscription established (device bus)");
            tasks.push(tokio::spawn(pump(
                sub,
                Arc::clone(&engine),
                Arc::clone(&site),
                Direction::Uplink,
                Arc::clone(&counters),
                None, // replies only ride the downlink (§2.4)
            )));
        }

        // DOWNLINK pump: site broker → device bus (own-device cmd only), running
        // each forwardable cmd through the §2.4 reply proxy.
        let downlink = engine.downlink_filter().to_string();
        let sub = site
            .subscribe(&downlink, Destination::Local, Qos::AtLeastOnce, queue.default_depth)
            .await?;
        tracing::info!(filter = %downlink, "downlink subscription established (site broker)");
        tasks.push(tokio::spawn(pump(
            sub,
            Arc::clone(&engine),
            Arc::clone(&primary),
            Direction::Downlink,
            Arc::clone(&counters),
            Some(Arc::clone(&reply_proxy)),
        )));

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

        Ok(RelayIo { engine, primary, site, counters, reply_proxy, tasks })
    }

    /// The live relay counters.
    pub fn counters(&self) -> &RelayCounters {
        &self.counters
    }

    /// The `relay_pending_replies` gauge (§2.5): in-flight correlation entries.
    pub fn pending_replies(&self) -> usize {
        self.reply_proxy.correlator().len()
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
            let mut guard =
                self.reply_proxy.reply_tasks.lock().unwrap_or_else(PoisonError::into_inner);
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
        tracing::info!("relay stopped; all filters unsubscribed");
    }
}

/// One subscription's pump: receive → [`RelayEngine::decide`] → republish
/// topic-verbatim on the other connection. Serial per subscription (ordered).
/// The downlink pump carries the reply proxy (`reply: Some(…)`) and runs every
/// forwardable `cmd` through the §2.4 `reply_to` rewrite first.
async fn pump(
    mut sub: Subscription,
    engine: Arc<RelayEngine>,
    to: Arc<dyn MessagingProvider>,
    direction: Direction,
    counters: Arc<RelayCounters>,
    reply: Option<Arc<ReplyProxy>>,
) {
    while let Some((topic, payload)) = sub.recv().await {
        match engine.decide(direction, &topic, &payload) {
            RelayDecision::Forward(bytes) => {
                let bytes = match &reply {
                    Some(proxy) => match proxy.rewrite_downlink(&topic, bytes).await {
                        Some(bytes) => bytes,
                        None => continue, // reply subscription failed: fail closed
                    },
                    None => bytes,
                };
                // Topic-verbatim republish. Both providers' "Local" destination is
                // the broker they were built against (device bus vs site broker).
                match to.publish(&topic, bytes, Destination::Local, Qos::AtLeastOnce).await {
                    Ok(()) => {
                        let counter = match direction {
                            Direction::Uplink => &counters.uplinked,
                            Direction::Downlink => &counters.downlinked,
                        };
                        counter.fetch_add(1, Ordering::Relaxed);
                        tracing::debug!(topic, ?direction, "relayed");
                    }
                    Err(e) => {
                        counters.publish_failed.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(topic, ?direction, error = %e, "relay publish failed");
                    }
                }
            }
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
    use ggcommons::messaging::topic_matches;
    use ggcommons::messaging::MessageBuilder;
    use serde_json::{json, Value};
    use std::sync::Mutex;
    use std::time::Duration;

    /// One registered fake subscription: the filter and its delivery channel.
    type FakeSub = (String, tokio::sync::mpsc::Sender<(String, Vec<u8>)>);

    /// In-memory fake `MessagingProvider` (the library's FakeProvider pattern):
    /// records publishes and routes them into matching registered subscriptions.
    #[derive(Default)]
    struct FakeProvider {
        subs: Mutex<Vec<FakeSub>>,
        published: Mutex<Vec<(String, Vec<u8>)>>,
        unsubscribed: Mutex<Vec<String>>,
        /// When set, SUBSCRIBEs to filters starting with this prefix fail.
        fail_subscribe_prefix: Mutex<Option<String>>,
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
            self.subs.lock().unwrap().iter().map(|(f, _)| f.clone()).collect()
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
        ) -> ggcommons::Result<()> {
            self.published.lock().unwrap().push((topic.to_string(), payload.clone()));
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
        ) -> ggcommons::Result<Subscription> {
            if let Some(prefix) = self.fail_subscribe_prefix.lock().unwrap().as_deref() {
                if filter.starts_with(prefix) {
                    return Err(ggcommons::GgError::Messaging(format!(
                        "forced subscribe failure for {filter}"
                    )));
                }
            }
            let (tx, rx) = tokio::sync::mpsc::channel(max_messages.max(1));
            self.subs.lock().unwrap().push((filter.to_string(), tx));
            Ok(Subscription::new(rx, Box::new(())))
        }

        async fn unsubscribe(&self, filter: &str, _dest: Destination) -> ggcommons::Result<()> {
            self.unsubscribed.lock().unwrap().push(filter.to_string());
            self.subs.lock().unwrap().retain(|(f, _)| f != filter);
            Ok(())
        }

        fn connected(&self) -> bool {
            true
        }
    }

    fn envelope() -> Vec<u8> {
        MessageBuilder::new("state", "1.0")
            .payload(json!({ "status": "RUNNING" }))
            .build()
            .to_vec()
            .unwrap()
    }

    async fn started() -> (RelayIo, Arc<FakeProvider>, Arc<FakeProvider>) {
        started_with(ReplyConfig::default()).await
    }

    async fn started_with(reply: ReplyConfig) -> (RelayIo, Arc<FakeProvider>, Arc<FakeProvider>) {
        let engine = Arc::new(RelayEngine::new("gw-01", DEFAULT_MAX_HOPS, false).unwrap());
        let device = Arc::new(FakeProvider::default());
        let site = Arc::new(FakeProvider::default());
        let io = RelayIo::start(
            engine,
            Arc::clone(&device) as Arc<dyn MessagingProvider>,
            Arc::clone(&site) as Arc<dyn MessagingProvider>,
            &QueueConfig::default(),
            &reply,
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

        assert!(wait_until(|| !site.published().is_empty()).await, "uplink did not arrive");
        let (site_topic, bytes) = site.published().remove(0);
        assert_eq!(site_topic, topic, "topic-verbatim relay");
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["tags"][RELAY_TAG], json!(["gw-01/uns-bridge"]));
        assert_eq!(v["body"]["status"], "RUNNING");
        assert_eq!(io.counters().uplinked.load(Ordering::Relaxed), 1);
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
            .publish("ecv1/gw-01/c/main/state", stamped, Destination::Local, Qos::AtLeastOnce)
            .await
            .unwrap();

        assert!(
            wait_until(|| io.counters().loop_dropped.load(Ordering::Relaxed) == 1).await,
            "loop drop not counted"
        );
        assert!(site.published().is_empty(), "own echo must not reach the site broker");
        io.shutdown().await;
    }

    #[tokio::test]
    async fn downlink_relays_own_device_cmd_to_the_device_bus() {
        let (io, device, site) = started().await;
        let topic = "ecv1/gw-01/opcua-adapter/main/cmd/reload-config";
        site.publish(topic, envelope(), Destination::Local, Qos::AtLeastOnce).await.unwrap();

        assert!(wait_until(|| !device.published().is_empty()).await, "downlink did not arrive");
        let (dev_topic, bytes) = device.published().remove(0);
        assert_eq!(dev_topic, topic);
        let v: Value = serde_json::from_slice(&bytes).unwrap();
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
        // bus matches NO uplink filter, so a single bridge can never echo itself
        // even for raw messages.
        let (io, device, site) = started().await;
        let topic = "ecv1/gw-01/opcua-adapter/main/cmd/ping";
        site.publish(topic, envelope(), Destination::Local, Qos::AtLeastOnce).await.unwrap();

        assert!(wait_until(|| !device.published().is_empty()).await);
        tokio::time::sleep(Duration::from_millis(50)).await;
        // The site fake recorded exactly the test's own original publish — the
        // bridge added nothing (the relayed cmd on the device bus matches no
        // uplink filter).
        assert_eq!(site.published().len(), 1, "cmd must never be uplinked back");
        io.shutdown().await;
    }

    #[tokio::test]
    async fn raw_message_relays_verbatim() {
        let (io, device, site) = started().await;
        // A raw (non-envelope) data point carries no tags to hop-stamp: it must
        // relay byte-for-byte (never re-wrapped as `{"raw": …}`).
        let payload = serde_json::to_vec(&json!({ "temperature": 21.5 })).unwrap();
        let topic = "ecv1/gw-01/modbus-adapter/main/data/temp";
        device
            .publish(topic, payload.clone(), Destination::Local, Qos::AtLeastOnce)
            .await
            .unwrap();

        assert!(wait_until(|| !site.published().is_empty()).await, "raw uplink did not arrive");
        let (site_topic, bytes) = site.published().remove(0);
        assert_eq!(site_topic, topic);
        assert_eq!(bytes, payload, "raw payload must relay verbatim");
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
    const SITE_REPLY: &str = "ggcommons/reply-site-original";
    const BRIDGE_PREFIX: &str = "ggcommons/reply-";

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
        let v: Value = serde_json::from_slice(bytes).unwrap();
        v["header"]["reply_to"].as_str().expect("relayed cmd must carry reply_to").to_string()
    }

    /// Relay one cmd-with-reply_to downlink and return its bridge reply topic.
    async fn downlink_request(
        device: &Arc<FakeProvider>,
        site: &Arc<FakeProvider>,
        site_reply: &str,
        corr: &str,
    ) -> String {
        let already = device.published().len();
        site.publish(CMD_TOPIC, cmd_with_reply(site_reply, corr), Destination::Local, Qos::AtLeastOnce)
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
        assert_ne!(bridge_topic, SITE_REPLY, "the site reply topic must not leak down");
        let v: Value = serde_json::from_slice(&bytes).unwrap();
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
        device.publish(&bridge_topic, reply, Destination::Local, Qos::AtLeastOnce).await.unwrap();

        // The reply lands on the ORIGINAL site reply topic, verbatim except the
        // hop tag: correlation id + body preserved, reply_to absent.
        assert!(
            wait_until(|| site.published().iter().any(|(t, _)| t == SITE_REPLY)).await,
            "reply did not reach the site broker"
        );
        let (_, bytes) =
            site.published().into_iter().find(|(t, _)| t == SITE_REPLY).unwrap();
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["header"]["correlation_id"], "corr-1", "correlation_id preserved");
        assert_eq!(v["body"], json!({ "ok": true, "n": 42 }), "body verbatim");
        assert!(v["header"].get("reply_to").is_none(), "a relayed reply carries no reply_to");
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
        site.publish(CMD_TOPIC, envelope(), Destination::Local, Qos::AtLeastOnce).await.unwrap();

        assert!(wait_until(|| !device.published().is_empty()).await);
        let (_, bytes) = device.published().remove(0);
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        assert!(v["header"].get("reply_to").is_none(), "no reply_to must be minted");
        assert_eq!(io.pending_replies(), 0, "no correlation entry for a notification cmd");
        assert!(
            !device.subscriptions().iter().any(|f| f.starts_with(BRIDGE_PREFIX)),
            "no bridge reply subscription for a notification cmd"
        );
        io.shutdown().await;
    }

    #[tokio::test]
    async fn ttl_expiry_unsubscribes_the_bridge_topic_and_counts() {
        // ttlSecs 0: the entry is expired the moment it is recorded; the sweep
        // (100 ms floor cadence) tears it down.
        let (io, device, site) =
            started_with(ReplyConfig { ttl_secs: 0, max_pending: 1024 }).await;
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
            .publish(&bridge_topic, envelope(), Destination::Local, Qos::AtLeastOnce)
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
        let (io, device, site) = started_with(ReplyConfig { ttl_secs: 60, max_pending: 2 }).await;
        let bridge0 = downlink_request(&device, &site, "ggcommons/reply-site-0", "c0").await;
        let bridge1 = downlink_request(&device, &site, "ggcommons/reply-site-1", "c1").await;
        assert_eq!(io.pending_replies(), 2);

        // The third request overflows the map: the OLDEST entry (bridge0) is
        // evicted — expired early, unsubscribed, counted.
        let _bridge2 = downlink_request(&device, &site, "ggcommons/reply-site-2", "c2").await;
        assert!(
            wait_until(|| io.counters().reply_expired.load(Ordering::Relaxed) == 1).await,
            "eviction must count as reply_expired"
        );
        assert_eq!(io.pending_replies(), 2, "the bound holds");
        assert!(
            wait_until(|| device.unsubscribed().contains(&bridge0)).await,
            "the evicted (oldest) bridge topic must be unsubscribed"
        );
        assert!(!device.unsubscribed().contains(&bridge1), "younger entries survive");

        // The surviving second entry still round-trips.
        let reply = MessageBuilder::new("r", "1.0")
            .payload(json!({ "ok": 1 }))
            .correlation_id("c1")
            .build()
            .to_vec()
            .unwrap();
        device.publish(&bridge1, reply, Destination::Local, Qos::AtLeastOnce).await.unwrap();
        assert!(
            wait_until(|| site.published().iter().any(|(t, _)| t == "ggcommons/reply-site-1")).await,
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
        site.publish(CMD_TOPIC, cmd_with_reply(SITE_REPLY, "corr-1"), Destination::Local, Qos::AtLeastOnce)
            .await
            .unwrap();

        assert!(
            wait_until(|| io.counters().publish_failed.load(Ordering::Relaxed) == 1).await,
            "the failed reply path must be counted"
        );
        assert!(device.published().is_empty(), "the cmd must NOT be relayed (fail closed)");
        assert_eq!(io.pending_replies(), 0, "the abandoned entry must not linger to expiry");
        io.shutdown().await;
    }

    #[tokio::test]
    async fn stray_reply_with_no_entry_is_dropped_and_counted() {
        let (io, _device, site) = started().await;
        // The stray window: a reply delivered just before expiry/eviction tore
        // the entry down — drive the resolution path directly with no entry.
        io.reply_proxy.handle_reply("ggcommons/reply-never-recorded", &envelope()).await;
        assert_eq!(io.counters().reply_stray.load(Ordering::Relaxed), 1);
        assert!(site.published().is_empty(), "a stray reply must not reach the site broker");
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
}
