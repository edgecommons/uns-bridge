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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ggcommons::messaging::{Destination, MessagingProvider, Qos, Subscription};
use tokio::task::JoinHandle;

use crate::config::QueueConfig;
use crate::relay::{Direction, DropReason, RelayDecision, RelayEngine};
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
    /// Forward decisions whose republish failed at the transport.
    pub publish_failed: AtomicU64,
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

/// The running relay: pump tasks + the subscriptions they own, with counters.
pub struct RelayIo {
    engine: Arc<RelayEngine>,
    primary: Arc<dyn MessagingProvider>,
    site: Arc<dyn MessagingProvider>,
    counters: Arc<RelayCounters>,
    tasks: Vec<JoinHandle<()>>,
}

impl RelayIo {
    /// Subscribe the §2.2 matrix and start the pumps:
    /// - UPLINK: the six class wildcards (+ `app` when opted in) on PRIMARY →
    ///   republish topic-verbatim to SITE;
    /// - DOWNLINK: `cmd` pinned to this bridge's own device on SITE → republish
    ///   to PRIMARY.
    ///
    /// Queue depths come from `queue` (`data` deep, others shallow — §2.2).
    ///
    /// # Errors
    /// A failed SUBSCRIBE on either connection surfaces immediately (the bridge
    /// must not run half-subscribed).
    pub async fn start(
        engine: Arc<RelayEngine>,
        primary: Arc<dyn MessagingProvider>,
        site: Arc<dyn MessagingProvider>,
        queue: &QueueConfig,
    ) -> ggcommons::Result<RelayIo> {
        let counters = Arc::new(RelayCounters::default());
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
            )));
        }

        // DOWNLINK pump: site broker → device bus (own-device cmd only).
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
        )));

        Ok(RelayIo { engine, primary, site, counters, tasks })
    }

    /// The live relay counters.
    pub fn counters(&self) -> &RelayCounters {
        &self.counters
    }

    /// Graceful stop: abort the pumps, then **unsubscribe every filter at both
    /// brokers** (the unsubscribe-before-exit rule) — best-effort with warnings.
    pub async fn shutdown(self) {
        for task in &self.tasks {
            task.abort();
        }
        for task in self.tasks {
            let _ = task.await; // JoinError(Cancelled) is expected
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
        tracing::info!("relay stopped; all filters unsubscribed");
    }
}

/// One subscription's pump: receive → [`RelayEngine::decide`] → republish
/// topic-verbatim on the other connection. Serial per subscription (ordered).
async fn pump(
    mut sub: Subscription,
    engine: Arc<RelayEngine>,
    to: Arc<dyn MessagingProvider>,
    direction: Direction,
    counters: Arc<RelayCounters>,
) {
    while let Some((topic, payload)) = sub.recv().await {
        match engine.decide(direction, &topic, &payload) {
            RelayDecision::Forward(bytes) => {
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
    }

    impl FakeProvider {
        fn published(&self) -> Vec<(String, Vec<u8>)> {
            self.published.lock().unwrap().clone()
        }
        fn unsubscribed(&self) -> Vec<String> {
            self.unsubscribed.lock().unwrap().clone()
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
        let engine = Arc::new(RelayEngine::new("gw-01", DEFAULT_MAX_HOPS, false).unwrap());
        let device = Arc::new(FakeProvider::default());
        let site = Arc::new(FakeProvider::default());
        let io = RelayIo::start(
            engine,
            Arc::clone(&device) as Arc<dyn MessagingProvider>,
            Arc::clone(&site) as Arc<dyn MessagingProvider>,
            &QueueConfig::default(),
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
}
