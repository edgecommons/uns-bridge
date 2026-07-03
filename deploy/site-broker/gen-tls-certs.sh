#!/usr/bin/env bash
# Generate throwaway TLS dev/test certificates for the uns-bridge site broker: a CA, a server cert
# for the broker itself, and TWO client certs exercising the two identity classes the ACL
# (acl.conf) distinguishes — a device bridge (CN=gw-01) and a site consumer (CN=consumer-console).
# Mirrors ggcommons/test-infra/gen-tls-certs.sh's structure; see TLS.md for how these plug in.
#
# Output goes to ./tls-certs/ (gitignored). Safe to re-run. THESE ARE DEV/TEST CERTS ONLY — a real
# deployment issues one client cert per device from a real (or site-private) CA; see TLS.md "prod".
set -euo pipefail
# Stop Git Bash (MSYS) from rewriting the openssl -subj "/CN=..." arguments into Windows paths.
export MSYS_NO_PATHCONV=1
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/tls-certs"
mkdir -p "$DIR"
cd "$DIR"

# --- CA --- (basicConstraints + keyUsage are required; strict TLS stacks, e.g. Python 3.13+/OpenSSL
# 3, reject a CA cert that lacks the keyCertSign key usage — same requirement as the monorepo cert.)
openssl genrsa -out ca.key 2048
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 \
  -subj "/CN=uns-bridge-site-test-ca" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -out ca.crt

# --- Server cert (the site broker itself; CN/SAN localhost for local-dev connections) ---
openssl genrsa -out server.key 2048
openssl req -new -key server.key -subj "/CN=localhost" -out server.csr
cat > server.ext <<'EOF'
basicConstraints = CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost,IP:127.0.0.1
EOF
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days 3650 -sha256 -extfile server.ext

# --- Client cert #1: a device bridge. CN is load-bearing — with mqtt.peer_cert_as_username=cn
# (docker-compose.yml), EMQX sets username="gw-01" for this connection, which is exactly what
# acl.conf's FLEET TEMPLATE / WORKED EXAMPLE sections key on. Match this CN to the bridge's
# `siteBroker.clientId` device token when you point a real config at this broker. ---
openssl genrsa -out client-gw-01.key 2048
openssl req -new -key client-gw-01.key -subj "/CN=gw-01" -out client-gw-01.csr
cat > client.ext <<'EOF'
basicConstraints = CA:FALSE
keyUsage = critical,digitalSignature
extendedKeyUsage = clientAuth
EOF
openssl x509 -req -in client-gw-01.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out client-gw-01.crt -days 3650 -sha256 -extfile client.ext

# --- Client cert #2: a site consumer (console/historian/MES). CN MUST start with "consumer-" to
# match acl.conf's `{username, {re, "^consumer-"}}` rules — anything else falls through to deny. ---
openssl genrsa -out client-consumer-console.key 2048
openssl req -new -key client-consumer-console.key -subj "/CN=consumer-console" -out client-consumer-console.csr
openssl x509 -req -in client-consumer-console.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out client-consumer-console.crt -days 3650 -sha256 -extfile client.ext

rm -f server.csr server.ext client.ext client-gw-01.csr client-consumer-console.csr
echo "Generated dev certs in $DIR:"
ls -1 "$DIR"
echo
echo "Next: TLS.md shows how to point a bridge (or an mqtt client) at the :8884 mTLS listener"
echo "using ca.crt + client-gw-01.{crt,key} (or client-consumer-console.{crt,key})."
