#!/usr/bin/env bash
# Build/update osu! scout in place. Run as the service user:  sudo -u scout bash deploy/deploy.sh
set -euo pipefail
cd "$(dirname "$0")/.."

git pull --ff-only

# Python deps for the scout package only (not the old Discord bot)
[ -d venv ] || python3 -m venv venv
./venv/bin/pip install --quiet --upgrade pip
./venv/bin/pip install --quiet -r scout/requirements.txt

# Website
cd web
npm ci
SCOUT_API_URL="${SCOUT_API_URL:-http://127.0.0.1:8001}" npm run build

echo "Built. Restart with: sudo systemctl restart scout-api scout-web"
