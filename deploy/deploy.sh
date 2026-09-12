#!/usr/bin/env bash
#
# Server-side half of a deployment. The GitHub Actions workflow rsyncs the code
# first and then runs this over SSH as the deploy user:
#     bash /opt/rtu-schedule-bot/deploy/deploy.sh
#
# Safe to run by hand on the server too.

set -euo pipefail

APP_DIR="${APP_DIR:-/opt/rtu-schedule-bot}"
SERVICE="rtu-schedule-bot"

cd "$APP_DIR"

if [ ! -f .env ]; then
    echo "ERROR: $APP_DIR/.env is missing - the bot cannot start without TELEGRAM_BOT_TOKEN." >&2
    exit 1
fi

if [ ! -d .venv ]; then
    echo "==> Creating the virtualenv"
    python3 -m venv .venv
fi

echo "==> Installing dependencies"
./.venv/bin/python -m pip install --upgrade pip --quiet
./.venv/bin/python -m pip install -r requirements.txt --quiet

echo "==> Byte-compiling to catch syntax errors before the restart"
./.venv/bin/python -m compileall -q app.py bot.py config.py formatter.py models.py rtu_api.py scheduler.py storage.py

echo "==> Restarting $SERVICE"
sudo systemctl restart "$SERVICE"

# give the process a moment to fail loudly (bad token, bad .env) instead of
# reporting a green deployment for a service that is already crash-looping
for _ in 1 2 3 4 5; do
    sleep 1
    if ! systemctl is-active --quiet "$SERVICE"; then
        echo "ERROR: $SERVICE is not running. Last log lines:" >&2
        journalctl -u "$SERVICE" -n 30 --no-pager >&2 || true
        exit 1
    fi
done

echo "==> $SERVICE is active"
systemctl status "$SERVICE" --no-pager --lines 5 || true
