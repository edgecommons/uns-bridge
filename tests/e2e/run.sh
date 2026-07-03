#!/usr/bin/env bash
# P3-6 — the bridge-level dual-EMQX end-to-end test, one command.
#
#   bash tests/e2e/run.sh
#
# Boots two REAL EMQX brokers (docker-compose.e2e.yml: device + site, plaintext/anon,
# dedicated ports 21883/21884), then runs tests/e2e_dual_broker.rs — which builds and
# spawns the REAL bridge binary against them and asserts the relay matrix (A-F, see the
# test's module docs): uplink topic-verbatim + hop tag, own-device cmd downlink,
# non-own-device cmd NOT relayed, the §2.4 reply round-trip, the §2.3 loop-drop, and the
# bridge's own §2.8 state/metric observability. Tears the brokers down on exit either way.
#
# Prereqs: Docker (compose v2), cargo. Runs on Windows Git Bash, Linux, macOS.
# Overrides: E2E_DEVICE_PORT / E2E_SITE_PORT / E2E_IMAGE_TAG (compose + test read the
# same variables). Runtime is dominated by the ~35 s wait for the bridge's first 30 s
# metric emission tick (assertion F2).

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

export E2E_DEVICE_PORT="${E2E_DEVICE_PORT:-21883}"
export E2E_SITE_PORT="${E2E_SITE_PORT:-21884}"

compose() { docker compose -p uns-bridge-e2e -f tests/e2e/docker-compose.e2e.yml "$@"; }
cleanup() { compose down --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "== e2e rig: device broker :${E2E_DEVICE_PORT} / site broker :${E2E_SITE_PORT} =="
compose up -d --wait --wait-timeout 120

echo "== running the bridge-level e2e (tests/e2e_dual_broker.rs) =="
UNS_BRIDGE_E2E=1 cargo test --test e2e_dual_broker -- --ignored --nocapture

echo "== e2e PASSED; tearing the rig down =="
