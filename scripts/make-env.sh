#!/usr/bin/env bash
# Create .env from .env.example, generating fresh secrets.
# Safe to run repeatedly: refuses to overwrite an existing .env.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ -f .env ]; then
  echo ".env already exists - leaving it alone."
  echo "Delete it first if you want fresh secrets."
  exit 0
fi

gen() { uv run --python 3.12 --no-project --quiet "$@"; }

FERNET=$(gen --with cryptography python -c \
  "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
JWT=$(gen python -c "import secrets; print(secrets.token_urlsafe(48))")

sed -e "s|^FERNET_KEY=.*|FERNET_KEY=${FERNET}|" \
    -e "s|^AIRFLOW__API_AUTH__JWT_SECRET=.*|AIRFLOW__API_AUTH__JWT_SECRET=${JWT}|" \
    .env.example > .env

echo "Wrote .env with freshly generated FERNET_KEY and JWT secret."
