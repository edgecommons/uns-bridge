#!/usr/bin/env bash
#
# Custom GDK build for the uns-bridge Rust Greengrass component (PACKAGING STUB, P3-2 —
# the telemetry-processor pattern). `gdk component build` invokes this (gdk-config.json ->
# custom_build_command) and expects:
#   - the recipe   in  greengrass-build/recipes/
#   - the artifact in  greengrass-build/artifacts/<ComponentName>/<ComponentVersion>/
#
# Greengrass cores typically run Linux: build on a Linux host or set EDGECOMMONS_TARGET to a
# Linux triple you have a toolchain for.
set -euo pipefail

COMPONENT_NAME="com.mbreissi.edgecommons.UnsBridge"
COMPONENT_VERSION="1.0.0"
BIN_NAME="uns-bridge"

# `standalone` = HOST (device + site both MQTT). `greengrass` = device IPC + site MQTT (it builds on
# `standalone` for the site provider); build it on a Linux toolchain (the IPC provider is C-FFI).
FEATURES="${EDGECOMMONS_FEATURES:-standalone}"
TARGET="${EDGECOMMONS_TARGET:-}"
TARGET_DIR="${CARGO_TARGET_DIR:-target}"

# GREENGRASS IPC stream ceiling. The `aws-greengrass-component-sdk` C-FFI compiles with a default
# GG_IPC_MAX_STREAMS of 16 (an `#ifndef` guard), but the relay opens more concurrent IPC subscription
# streams than that (~18: the six/seven uplink class wildcards on the device bus, both D-U28 scopes,
# the downlink cmd scopes, and the per-request reply-proxy topics), so the nucleus rejects the extra
# streams (NOMEM) and the component crash-loops on start. Raise the ceiling to 64 (comfortable headroom
# over what the relay uses) for the SDK's `cc` build. Set both `CFLAGS` (native builds) and
# `TARGET_CFLAGS` (cross builds via EDGECOMMONS_TARGET) so the `cc` crate picks the define up either way;
# the `#ifndef` makes 64 a supported override. This is baked into the shipped greengrass artifact, so no
# deploy-time env is needed; ANY local greengrass dev build must go through this script (or export the
# same define) to get it.
if [[ " ${FEATURES} " == *greengrass* ]]; then
  export CFLAGS="${CFLAGS:-} -DGG_IPC_MAX_STREAMS=64"
  export TARGET_CFLAGS="${TARGET_CFLAGS:-} -DGG_IPC_MAX_STREAMS=64"
  echo "greengrass build: raising GG_IPC_MAX_STREAMS to 64 (relay opens more IPC streams than the SDK default of 16)"
fi

echo "Building ${BIN_NAME} (release, features=${FEATURES})${TARGET:+ for ${TARGET}}..."
if [[ -n "${TARGET}" ]]; then
  cargo build --release --no-default-features --features "${FEATURES}" --target "${TARGET}"
  BIN_DIR="${TARGET_DIR}/${TARGET}/release"
else
  cargo build --release --no-default-features --features "${FEATURES}"
  BIN_DIR="${TARGET_DIR}/release"
fi

BIN_PATH="${BIN_DIR}/${BIN_NAME}"
[[ -f "${BIN_PATH}" ]] || BIN_PATH="${BIN_DIR}/${BIN_NAME}.exe"
if [[ ! -f "${BIN_PATH}" ]]; then
  echo "error: built binary not found in ${BIN_DIR}" >&2
  exit 1
fi

ARTIFACT_DIR="greengrass-build/artifacts/${COMPONENT_NAME}/${COMPONENT_VERSION}"
RECIPE_DIR="greengrass-build/recipes"
mkdir -p "${ARTIFACT_DIR}" "${RECIPE_DIR}"

cp "${BIN_PATH}" "${ARTIFACT_DIR}/${BIN_NAME}"
chmod +x "${ARTIFACT_DIR}/${BIN_NAME}" || true
cp recipe.yaml "${RECIPE_DIR}/recipe.yaml"

echo "Staged artifact -> ${ARTIFACT_DIR}/${BIN_NAME}"
echo "Staged recipe   -> ${RECIPE_DIR}/recipe.yaml"
