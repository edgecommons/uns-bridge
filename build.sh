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

# P3-2 ships the standalone (dual-MQTT) relay; the greengrass (IPC-primary) variant is a
# documented follow-up.
FEATURES="${EDGECOMMONS_FEATURES:-standalone}"
TARGET="${EDGECOMMONS_TARGET:-}"
TARGET_DIR="${CARGO_TARGET_DIR:-target}"

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
