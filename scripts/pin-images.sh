#!/usr/bin/env bash
# Report the immutable digest each floating image tag currently resolves to.
#
# Why: `minio/minio:latest` is not reproducible - the same compose file can produce a
# different stack next month. Pinning by digest is what makes a "clone and run" repo
# actually reproducible. Run this after a successful `make up`, then paste the digests
# into .env.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "Resolving current digests for the floating tags in .env..."
echo

for image in \
  "$(grep -E '^MINIO_IMAGE=' .env | cut -d= -f2-)" \
  "$(grep -E '^MINIO_MC_IMAGE=' .env | cut -d= -f2-)" \
  "$(grep -E '^POSTGRES_IMAGE=' .env | cut -d= -f2-)"
do
  [ -z "$image" ] && continue
  digest=$(docker image inspect "$image" --format '{{index .RepoDigests 0}}' 2>/dev/null || true)
  if [ -z "$digest" ]; then
    echo "  $image -> not pulled yet (run: docker pull $image)"
  else
    echo "  $image"
    echo "      -> $digest"
  fi
done

echo
echo "Paste the '@sha256:...' forms into .env to pin them."
