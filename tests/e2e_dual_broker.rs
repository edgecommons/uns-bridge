//! # P3-6 — the bridge-level dual-EMQX end-to-end relay proof
//!
//! Runs the REAL `uns-bridge` binary between two REAL EMQX brokers (a device
//! broker and a site broker) and asserts the §2.2 relay matrix over live MQTT:
//!
//! | # | Assertion |
//! |---|---|
//! | A1 | uplink: a `state` **envelope** published on the device bus arrives **topic-verbatim** on the SITE broker, hop tag `tags._relay = ["gw-01/uns-bridge"]` appended |
//! | A2 | uplink: an `evt` envelope (with channel) — same contract |
//! | A3 | uplink: an opaque-body `data` envelope preserves its body bytes |
//! | B  | downlink: a `cmd` for the bridge's own device published on the SITE broker arrives topic-verbatim (hop-tagged) on the DEVICE bus |
//! | C  | a `cmd` for **another** device on the site broker is NOT relayed (own-device pinning) |
//! | D  | request/reply crosses the bridge (§2.4): `header.reply_to` is rewritten to a bridge-minted topic on the way down; the reply returns to the ORIGINAL site reply topic, `correlation_id`/body intact, `reply_to` dropped |
//! | E  | loop protection (§2.3): an envelope already carrying the bridge's own hop id is dropped, never re-relayed |
//! | F1 | the bridge's own heartbeat `state` keepalive appears on the DEVICE bus (§2.8 / P3-4b) |
//! | F2 | the bridge's relay `metric`s appear on the DEVICE bus (first 30 s emission tick) |
//! | G  | forced bridge death publishes the derived site LWT protobuf `state` envelope with `status:"UNREACHABLE"` on the bridge's own state topic |
//!
//! **Gated twice**: `#[ignore]` so a plain `cargo test` never touches it, plus
//! the `UNS_BRIDGE_E2E=1` env var so even `--include-ignored` sweeps skip it
//! without the rig. Run it through `tests/e2e/run.sh`, which boots the
//! two-broker rig (`tests/e2e/docker-compose.e2e.yml`), runs this test with
//! `--ignored`, and tears the rig down.
//!
//! The test clients are the edgecommons core's own `MqttProvider` (the same
//! transport the bridge reuses for its site connection) — no extra MQTT
//! dependency. Negative assertions (C, E) avoid pure sleeps by publishing an
//! ordered **follower** through the same serial path and asserting the
//! must-not-arrive message stayed absent once the follower arrived.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use edgecommons::messaging::config::MessagingConfig;
use edgecommons::messaging::message::{Message, MessageBodyCase};
use edgecommons::messaging::provider::mqtt::MqttProvider;
use edgecommons::messaging::{Destination, MessageBuilder, MessagingProvider, Qos};
use serde_json::{json, Value};

/// The device (thing) token the bridge runs as — drives the bridge's own UNS
/// state topic and derived site Last-Will topic.
const DEVICE: &str = "gw-01";
/// The bridge's §2.3 hop identifier: `{device}/{component}`.
const HOP_ID: &str = "gw-01/uns-bridge";
/// The reserved hop-tag key (`src/relay.rs::RELAY_TAG` — the §2.3 contract).
const RELAY_TAG: &str = "_relay";
/// The core's reply-topic prefix a bridge-minted reply topic must carry (§2.4).
const REPLY_PREFIX: &str = "edgecommons/reply-";

fn env_port(name: &str, default: u16) -> u16 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Build one plaintext test-client config (the core `MessagingConfig` shape).
fn client_config(port: u16, client_id: &str) -> MessagingConfig {
    serde_json::from_value(json!({
        "messaging": { "local": { "host": "localhost", "port": port, "clientId": client_id } }
    }))
    .expect("test-client MessagingConfig")
}

/// Connect a test client, retrying briefly (the rig is health-checked before
/// the test runs, but the listener can lag the healthcheck by a beat).
async fn connect_client(port: u16, client_id: &str) -> Arc<dyn MessagingProvider> {
    let cfg = client_config(port, client_id);
    let mut last_err = String::new();
    for _ in 0..3 {
        match MqttProvider::connect(&cfg).await {
            Ok(p) => return Arc::new(p),
            Err(e) => last_err = e.to_string(),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    panic!(
        "could not connect test client '{client_id}' to localhost:{port} — is the dual-EMQX \
         rig up? (run through tests/e2e/run.sh): {last_err}"
    );
}

/// What a [`Recorder`] has seen so far: `(topic, payload)` in arrival order.
type Seen = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

/// A background collector over one subscription: every `(topic, payload)` the
/// broker delivers lands in `seen`, and assertions poll it.
struct Recorder {
    seen: Seen,
}

impl Recorder {
    async fn start(provider: &Arc<dyn MessagingProvider>, filter: &str, depth: usize) -> Recorder {
        let mut sub = provider
            .subscribe(filter, Destination::Local, Qos::AtLeastOnce, depth)
            .await
            .unwrap_or_else(|e| panic!("subscribing '{filter}': {e}"));
        let seen: Seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        tokio::spawn(async move {
            while let Some(msg) = sub.recv().await {
                sink.lock().expect("recorder lock").push(msg);
            }
        });
        Recorder { seen }
    }

    /// Every payload recorded on exactly `topic`.
    fn on_topic(&self, topic: &str) -> Vec<Vec<u8>> {
        self.seen
            .lock()
            .expect("recorder lock")
            .iter()
            .filter(|(t, _)| t == topic)
            .map(|(_, p)| p.clone())
            .collect()
    }

    /// Distinct topics recorded so far (diagnostics).
    fn topics(&self) -> Vec<String> {
        let mut topics: Vec<String> = self
            .seen
            .lock()
            .expect("recorder lock")
            .iter()
            .map(|(t, _)| t.clone())
            .collect();
        topics.dedup();
        topics
    }

    fn count_on_topic(&self, topic: &str) -> usize {
        self.seen
            .lock()
            .expect("recorder lock")
            .iter()
            .filter(|(t, _)| t == topic)
            .count()
    }

    /// Await the first payload on exactly `topic`, up to `timeout`.
    async fn await_topic(&self, topic: &str, timeout: Duration) -> Option<Vec<u8>> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(p) = self.on_topic(topic).into_iter().next() {
                return Some(p);
            }
            if Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn await_topic_match_after(
        &self,
        topic: &str,
        after: usize,
        pred: impl Fn(&[u8]) -> bool,
        timeout: Duration,
    ) -> Option<Vec<u8>> {
        let deadline = Instant::now() + timeout;
        loop {
            {
                let seen = self.seen.lock().expect("recorder lock");
                if let Some((_, payload)) = seen
                    .iter()
                    .filter(|(t, _)| t == topic)
                    .skip(after)
                    .find(|(_, payload)| pred(payload))
                {
                    return Some(payload.clone());
                }
            }
            if Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Await the first message whose topic satisfies `pred`, up to `timeout`.
    async fn await_match(
        &self,
        pred: impl Fn(&str) -> bool,
        timeout: Duration,
    ) -> Option<(String, Vec<u8>)> {
        let deadline = Instant::now() + timeout;
        loop {
            {
                let seen = self.seen.lock().expect("recorder lock");
                if let Some(m) = seen.iter().find(|(t, _)| pred(t)) {
                    return Some(m.clone());
                }
            }
            if Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// The bridge binary as a child process; killed on drop so a panicking
/// assertion never leaks it.
struct Bridge {
    child: Child,
    log: PathBuf,
}

impl Bridge {
    fn spawn(config: &Path, log: &Path) -> Bridge {
        let log_file = std::fs::File::create(log).expect("creating the bridge log file");
        let child = Command::new(env!("CARGO_BIN_EXE_uns-bridge"))
            .arg("--platform")
            .arg("HOST")
            .arg("--transport")
            .arg("MQTT")
            .arg(config)
            .arg("-c")
            .arg("FILE")
            .arg(config)
            .arg("-t")
            .arg(DEVICE)
            .stdout(Stdio::from(
                log_file.try_clone().expect("cloning the log handle"),
            ))
            .stderr(Stdio::from(log_file))
            .spawn()
            .expect("spawning the uns-bridge binary");
        Bridge {
            child,
            log: log.to_path_buf(),
        }
    }

    /// `Some(status)` when the bridge already exited (it must not).
    fn exited(&mut self) -> Option<String> {
        self.child
            .try_wait()
            .expect("try_wait")
            .map(|s| s.to_string())
    }

    fn log_tail(&self) -> String {
        let text = std::fs::read_to_string(&self.log).unwrap_or_default();
        let lines: Vec<&str> = text.lines().collect();
        let start = lines.len().saturating_sub(60);
        lines[start..].join("\n")
    }

    fn kill_and_wait(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        self.kill_and_wait();
    }
}

/// The e2e bridge config: the shipped sample (`test-configs/config.json`) with
/// only the broker ports/client ids swapped for the rig's — so the e2e
/// exercises the exact committed §2.7 shape.
fn write_e2e_config(dir: &Path, device_port: u16, site_port: u16) -> PathBuf {
    let mut cfg: Value = serde_json::from_str(include_str!("../test-configs/config.json"))
        .expect("shipped sample config parses");
    cfg["messaging"]["local"]["port"] = json!(device_port);
    cfg["messaging"]["local"]["clientId"] = json!("uns-bridge-e2e-local");
    cfg["component"]["instances"][0]["siteBroker"]["port"] = json!(site_port);
    cfg["component"]["instances"][0]["siteBroker"]["clientId"] = json!("uns-bridge-e2e-site");
    std::fs::create_dir_all(dir).expect("creating the e2e work dir");
    let path = dir.join("config.e2e.json");
    std::fs::write(
        &path,
        serde_json::to_string_pretty(&cfg).expect("serializing"),
    )
    .expect("writing the e2e config");
    path
}

fn envelope(msg_type: &str, body: Value) -> Vec<u8> {
    MessageBuilder::new(msg_type, "1.0")
        .payload(body)
        .build()
        .to_vec()
        .expect("envelope")
}

fn parse(bytes: &[u8]) -> Value {
    serde_json::to_value(Message::from_slice(bytes).expect("protobuf EdgeCommons envelope"))
        .expect("diagnostic projection")
}

fn decode(bytes: &[u8]) -> Message {
    Message::from_slice(bytes).expect("protobuf EdgeCommons envelope")
}

fn is_unreachable_lwt(bytes: &[u8]) -> bool {
    let Ok(msg) = Message::from_slice(bytes) else {
        return false;
    };
    if msg.body_case() != MessageBodyCase::StateUpdate {
        return false;
    }
    let bridge_identity = match msg.identity.as_ref() {
        Some(identity) => identity.component() == "uns-bridge" && identity.path() == DEVICE,
        None => false,
    };
    bridge_identity && msg.body.get("status") == Some(&json!("UNREACHABLE"))
}

/// The relayed envelope must carry exactly one hop: this bridge's own id.
fn assert_own_hop(v: &Value) -> Result<(), String> {
    if v["tags"][RELAY_TAG] == json!([HOP_ID]) {
        Ok(())
    } else {
        Err(format!(
            "hop tag wrong: tags.{RELAY_TAG} = {}",
            v["tags"][RELAY_TAG]
        ))
    }
}

/// Collected PASS/FAIL results, printed per assertion; `finish` fails the test
/// (with the bridge log tail) when anything failed.
struct Checks(Vec<(&'static str, Result<(), String>)>);

impl Checks {
    fn record(&mut self, id: &'static str, result: Result<(), String>) {
        match &result {
            Ok(()) => println!("[PASS] {id}"),
            Err(e) => println!("[FAIL] {id}: {e}"),
        }
        self.0.push((id, result));
    }

    fn finish(self, bridge: &Bridge) {
        let failed: Vec<&(&str, Result<(), String>)> =
            self.0.iter().filter(|(_, r)| r.is_err()).collect();
        println!(
            "\n== e2e summary: {}/{} passed ==",
            self.0.len() - failed.len(),
            self.0.len()
        );
        if !failed.is_empty() {
            println!(
                "\n-- bridge log tail ({}) --\n{}",
                bridge.log.display(),
                bridge.log_tail()
            );
            panic!(
                "{} of {} e2e assertions failed: {:?}",
                failed.len(),
                self.0.len(),
                failed.iter().map(|(id, _)| *id).collect::<Vec<_>>()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "live dual-EMQX e2e — run through tests/e2e/run.sh"]
async fn dual_emqx_bridge_level_relay() {
    if std::env::var("UNS_BRIDGE_E2E").as_deref() != Ok("1") {
        eprintln!(
            "SKIP dual_emqx_bridge_level_relay: UNS_BRIDGE_E2E != 1 — run tests/e2e/run.sh \
             (it boots the dual-EMQX rig, sets the gate, and tears down)"
        );
        return;
    }
    let device_port = env_port("E2E_DEVICE_PORT", 21883);
    let site_port = env_port("E2E_SITE_PORT", 21884);
    let work_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("e2e");
    let config = write_e2e_config(&work_dir, device_port, site_port);

    // Test clients: the edgecommons core MqttProvider against each broker.
    let dev: Arc<dyn MessagingProvider> = connect_client(device_port, "e2e-dev-probe").await;
    let site: Arc<dyn MessagingProvider> = connect_client(site_port, "e2e-site-probe").await;

    // Recorders BEFORE the bridge starts, so nothing can race past them.
    let site_all = Recorder::start(&site, "ecv1/#", 512).await;
    let dev_cmd = Recorder::start(&dev, "ecv1/+/+/+/cmd/#", 64).await;
    let dev_state =
        Recorder::start(&dev, &format!("ecv1/{DEVICE}/uns-bridge/main/state"), 64).await;
    let dev_metric = Recorder::start(
        &dev,
        &format!("ecv1/{DEVICE}/uns-bridge/main/metric/#"),
        256,
    )
    .await;
    let site_reply = Recorder::start(&site, "edgecommons/reply-e2e-original", 8).await;

    // The bridge under test — the real binary, real config file, real brokers.
    let started = Instant::now();
    let mut bridge = Bridge::spawn(&config, &work_dir.join("bridge.log"));
    println!(
        "bridge spawned (device :{device_port}, site :{site_port}); log: {}",
        bridge.log.display()
    );

    // Readiness gate: the bridge's own heartbeat `state`, relayed BY the bridge
    // ITSELF, reaching the SITE broker proves runtime + both relay connections +
    // the uplink path are all live.
    let own_state_topic = format!("ecv1/{DEVICE}/uns-bridge/main/state");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if site_all
            .await_topic(&own_state_topic, Duration::from_secs(1))
            .await
            .is_some()
        {
            break;
        }
        if let Some(status) = bridge.exited() {
            println!("-- bridge log tail --\n{}", bridge.log_tail());
            panic!("bridge exited during startup ({status})");
        }
        assert!(
            Instant::now() < deadline,
            "bridge never became ready (own state keepalive not seen on the site broker); \
             log tail:\n{}",
            bridge.log_tail()
        );
    }
    println!(
        "bridge ready after {:?} (own state relayed to the site broker)",
        started.elapsed()
    );

    let mut checks = Checks(Vec::new());
    let wait = Duration::from_secs(10);

    // ---- A: uplink, device -> site, topic-verbatim ----
    let a1_topic = format!("ecv1/{DEVICE}/e2e-comp/main/state");
    dev.publish(
        &a1_topic,
        envelope("state", json!({ "status": "RUNNING", "marker": "e2e-A1" })),
        Destination::Local,
        Qos::AtLeastOnce,
    )
    .await
    .expect("A1 publish");
    checks.record(
        "A1 uplink state envelope — topic-verbatim + hop tag on the site broker",
        match site_all.await_topic(&a1_topic, wait).await {
            None => Err("never arrived on the site broker".into()),
            Some(bytes) => {
                let v = parse(&bytes);
                if v["body"]["marker"] != "e2e-A1" {
                    Err(format!("body mangled: {v}"))
                } else {
                    assert_own_hop(&v)
                }
            }
        },
    );

    let a2_topic = format!("ecv1/{DEVICE}/e2e-comp/main/evt/alarms/high");
    dev.publish(
        &a2_topic,
        envelope(
            "alarm-raised",
            json!({ "severity": "high", "marker": "e2e-A2" }),
        ),
        Destination::Local,
        Qos::AtLeastOnce,
    )
    .await
    .expect("A2 publish");
    checks.record(
        "A2 uplink evt envelope (channel) — topic-verbatim + hop tag",
        match site_all.await_topic(&a2_topic, wait).await {
            None => Err("never arrived on the site broker".into()),
            Some(bytes) => {
                let v = parse(&bytes);
                if v["body"]["marker"] != "e2e-A2" {
                    Err(format!("body mangled: {v}"))
                } else {
                    assert_own_hop(&v)
                }
            }
        },
    );

    let a3_topic = format!("ecv1/{DEVICE}/e2e-comp/main/data/temp");
    let a3_opaque = vec![0x00, 0x01, 0x02, 0xfe, 0xff];
    let a3_payload = MessageBuilder::new("frame-preview", "1.0")
        .opaque_body(&a3_opaque, "application/octet-stream")
        .expect("A3 opaque body")
        .build()
        .to_vec()
        .expect("A3 envelope");
    dev.publish(&a3_topic, a3_payload, Destination::Local, Qos::AtLeastOnce)
        .await
        .expect("A3 publish");
    checks.record(
        "A3 uplink opaque data — body bytes preserved",
        match site_all.await_topic(&a3_topic, wait).await {
            None => Err("never arrived on the site broker".into()),
            Some(bytes) => {
                let msg = decode(&bytes);
                if msg.body_case() != MessageBodyCase::Opaque {
                    Err(format!("body case is not opaque: {:?}", msg.body_case()))
                } else {
                    match msg.opaque_body() {
                        Ok(Some(body)) if body == a3_opaque => assert_own_hop(&parse(&bytes)),
                        Ok(Some(_)) => Err("opaque bytes were not preserved".into()),
                        Ok(None) => Err("opaque body was absent".into()),
                        Err(e) => Err(e.to_string()),
                    }
                }
            }
        },
    );

    // ---- B: downlink, site -> device, own-device cmd ----
    let b_topic = format!("ecv1/{DEVICE}/e2e-comp/main/cmd/do-thing");
    site.publish(
        &b_topic,
        envelope("do-thing", json!({ "marker": "e2e-B" })),
        Destination::Local,
        Qos::AtLeastOnce,
    )
    .await
    .expect("B publish");
    checks.record(
        "B  downlink own-device cmd — topic-verbatim + hop tag on the device bus",
        match dev_cmd.await_topic(&b_topic, wait).await {
            None => Err("never arrived on the device bus".into()),
            Some(bytes) => {
                let v = parse(&bytes);
                if v["body"]["marker"] != "e2e-B" {
                    Err(format!("body mangled: {v}"))
                } else {
                    assert_own_hop(&v)
                }
            }
        },
    );

    // ---- C: a cmd for ANOTHER device must not cross ----
    let c_topic = "ecv1/gw-99/e2e-comp/main/cmd/do-thing";
    site.publish(
        c_topic,
        envelope("do-thing", json!({})),
        Destination::Local,
        Qos::AtLeastOnce,
    )
    .await
    .expect("C publish");
    // Ordered follower: once a LATER own-device cmd crossed, the gw-99 cmd had
    // every chance it will ever get.
    let c_follower = format!("ecv1/{DEVICE}/e2e-comp/main/cmd/after-c");
    site.publish(
        &c_follower,
        envelope("after-c", json!({})),
        Destination::Local,
        Qos::AtLeastOnce,
    )
    .await
    .expect("C follower publish");
    checks.record(
        "C  non-own-device cmd NOT relayed (own-device pinning)",
        match dev_cmd.await_topic(&c_follower, wait).await {
            None => {
                Err("the own-device follower cmd never arrived — cannot prove the negative".into())
            }
            Some(_) => {
                tokio::time::sleep(Duration::from_millis(750)).await; // grace
                if dev_cmd.on_topic(c_topic).is_empty() {
                    Ok(())
                } else {
                    Err("the gw-99 cmd WAS relayed to the device bus".into())
                }
            }
        },
    );

    // ---- D: request/reply across the bridge (§2.4) ----
    let d_topic = format!("ecv1/{DEVICE}/e2e-responder/main/cmd/ping");
    let d_cmd = MessageBuilder::new("ping", "1.0")
        .payload(json!({ "marker": "e2e-D" }))
        .correlation_id("corr-e2e-D")
        .reply_to("edgecommons/reply-e2e-original")
        .build()
        .to_vec()
        .expect("D cmd envelope");
    site.publish(&d_topic, d_cmd, Destination::Local, Qos::AtLeastOnce)
        .await
        .expect("D publish");
    let d_result = match dev_cmd.await_topic(&d_topic, wait).await {
        None => Err("the request cmd never arrived on the device bus".into()),
        Some(bytes) => {
            let v = parse(&bytes);
            let bridge_topic = v["header"]["reply_to"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            if !bridge_topic.starts_with(REPLY_PREFIX) {
                Err(format!(
                    "reply_to not rewritten to a bridge topic: '{bridge_topic}'"
                ))
            } else if bridge_topic == "edgecommons/reply-e2e-original" {
                Err("reply_to NOT rewritten — the site reply topic leaked down".into())
            } else {
                // Fake device-side responder: reply on the bridge-minted topic,
                // on the DEVICE bus, correlation id preserved.
                let reply = MessageBuilder::new("ping-reply", "1.0")
                    .payload(json!({ "ok": true, "marker": "e2e-D-reply" }))
                    .correlation_id("corr-e2e-D")
                    .build()
                    .to_vec()
                    .expect("D reply envelope");
                dev.publish(&bridge_topic, reply, Destination::Local, Qos::AtLeastOnce)
                    .await
                    .expect("D reply publish");
                match site_reply
                    .await_topic("edgecommons/reply-e2e-original", wait)
                    .await
                {
                    None => Err("the reply never returned to the original site reply topic".into()),
                    Some(bytes) => {
                        let v = parse(&bytes);
                        if v["header"]["correlation_id"] != "corr-e2e-D" {
                            Err(format!("correlation_id mangled: {}", v["header"]))
                        } else if v["body"]["ok"] != true {
                            Err(format!("reply body mangled: {}", v["body"]))
                        } else if !v["header"]["reply_to"].is_null() {
                            Err(format!(
                                "reply_to must be dropped from the relayed reply: {}",
                                v["header"]["reply_to"]
                            ))
                        } else {
                            assert_own_hop(&v)
                        }
                    }
                }
            }
        }
    };
    checks.record(
        "D  reply round-trip — reply_to rewritten down, reply returned to the original site topic",
        d_result,
    );

    // ---- E: hop-tag loop-drop (§2.3) ----
    let e_topic = format!("ecv1/{DEVICE}/e2e-loopy/main/state");
    let stamped = MessageBuilder::new("state", "1.0")
        .payload(json!({ "marker": "e2e-E" }))
        .tag(RELAY_TAG, json!([HOP_ID]))
        .build()
        .to_vec()
        .expect("E stamped envelope");
    dev.publish(&e_topic, stamped, Destination::Local, Qos::AtLeastOnce)
        .await
        .expect("E publish");
    // Ordered follower through the SAME serial state pump: once it crossed, the
    // stamped envelope was already decided (and must have been dropped).
    let e_follower = format!("ecv1/{DEVICE}/e2e-after-loop/main/state");
    dev.publish(
        &e_follower,
        envelope("state", json!({ "marker": "e2e-E-follower" })),
        Destination::Local,
        Qos::AtLeastOnce,
    )
    .await
    .expect("E follower publish");
    checks.record(
        "E  hop-tag loop-drop — own-stamped envelope not re-relayed",
        match site_all.await_topic(&e_follower, wait).await {
            None => Err("the follower state never arrived — cannot prove the negative".into()),
            Some(_) => {
                tokio::time::sleep(Duration::from_millis(500)).await; // grace
                if site_all.on_topic(&e_topic).is_empty() {
                    Ok(())
                } else {
                    Err("the own-stamped envelope WAS re-relayed to the site broker".into())
                }
            }
        },
    );

    // ---- F: the bridge's own observability on the DEVICE bus (§2.8) ----
    checks.record(
        "F1 bridge state keepalive on the device bus",
        match dev_state
            .await_topic(&own_state_topic, Duration::from_secs(12))
            .await
        {
            None => Err("no heartbeat state keepalive seen on the device bus".into()),
            Some(bytes) if parse(&bytes)["header"].is_object() => Ok(()),
            Some(bytes) => Err(format!(
                "keepalive is not an envelope: {}",
                String::from_utf8_lossy(&bytes)
            )),
        },
    );

    // The §2.8 RELAY-counter metrics (`relay_uplinked`, …): the emission task's
    // first tick fires one METRIC_EMIT_INTERVAL (30 s) after the relay starts,
    // so wait out the remainder from the bridge's start. (The runtime's own
    // `sys` metrics appear earlier on the same subtree — the predicate pins the
    // P3-4b relay counters specifically.)
    let metric_budget = Duration::from_secs(75).saturating_sub(started.elapsed());
    let metric_prefix = format!("ecv1/{DEVICE}/uns-bridge/main/metric/");
    let is_relay_metric = |t: &str| {
        t.strip_prefix(metric_prefix.as_str())
            .is_some_and(|n| n.starts_with("relay"))
    };
    checks.record(
        "F2 relay-counter metric on the device bus (30 s emission tick)",
        match dev_metric.await_match(is_relay_metric, metric_budget).await {
            None => Err(format!(
                "no relay_* metric under {metric_prefix}# within {metric_budget:?}"
            )),
            Some((topic, bytes)) if parse(&bytes)["header"].is_object() => {
                println!("       first relay metric: {topic}");
                Ok(())
            }
            Some((topic, _)) => Err(format!("metric on {topic} is not an envelope")),
        },
    );

    // Informational: the metrics also ride the bridge's own relay to the site.
    let site_metrics = site_all
        .topics()
        .into_iter()
        .filter(|t| t.starts_with(&metric_prefix))
        .count();
    println!("(info) metric topics also observed on the SITE broker: {site_metrics}");

    if let Some(status) = bridge.exited() {
        println!("-- bridge log tail --\n{}", bridge.log_tail());
        panic!("bridge exited mid-test ({status})");
    }
    let lwt_seen_before_kill = site_all.count_on_topic(&own_state_topic);
    bridge.kill_and_wait();
    checks.record(
        "G  derived site LWT — forced bridge death publishes UNREACHABLE on own state topic",
        match site_all
            .await_topic_match_after(
                &own_state_topic,
                lwt_seen_before_kill,
                is_unreachable_lwt,
                Duration::from_secs(15),
            )
            .await
        {
            Some(_) => Ok(()),
            None => Err("site broker did not publish the derived UNREACHABLE LWT".into()),
        },
    );
    checks.finish(&bridge);
}
