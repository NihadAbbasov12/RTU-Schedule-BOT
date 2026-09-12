#!/usr/bin/env bash
#
# One-time server bootstrap for the RTU schedule bot.
#
# Run once, as root, on the server:
#     sudo bash deploy/server-setup.sh
#
# It is idempotent: running it again only re-applies what is missing.
# It never touches /opt/rtu-schedule-bot/.env or the SQLite database.

set -euo pipefail

APP_USER="${APP_USER:-deploy}"
APP_DIR="${APP_DIR:-/opt/rtu-schedule-bot}"
SERVICE="rtu-schedule-bot"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$(id -u)" -ne 0 ]; then
    echo "Run this script as root: sudo bash $0" >&2
    exit 1
fi

# On a shared box that already hosts other services, an unattended package
# upgrade can restart them. Set SKIP_APT=1 when the packages are already present.
if [ "${SKIP_APT:-0}" = "1" ]; then
    echo "==> Skipping package installation (SKIP_APT=1)"
    missing=""
    for binary in python3 rsync git; do
        command -v "$binary" >/dev/null 2>&1 || missing="$missing $binary"
    done
    python3 -c "import venv" >/dev/null 2>&1 || missing="$missing python3-venv"
    if [ -n "$missing" ]; then
        echo "ERROR: SKIP_APT=1 but these are missing:$missing" >&2
        exit 1
    fi
else
    echo "==> Installing packages"
    export DEBIAN_FRONTEND=noninteractive
    apt-get update
    apt-get install -y python3 python3-venv python3-pip rsync git
fi

echo "==> Checking the Python version"
python3 - <<'PY'
import sys

if sys.version_info < (3, 10):
    sys.exit(f"Python 3.10+ is required, found {sys.version.split()[0]}")
print(f"Python {sys.version.split()[0]} is fine")
PY

echo "==> Ensuring the '$APP_USER' user exists"
if ! id -u "$APP_USER" >/dev/null 2>&1; then
    adduser --disabled-password --gecos "" "$APP_USER"
else
    echo "    user already exists"
fi

echo "==> Preparing ~/.ssh for '$APP_USER'"
install -d -o "$APP_USER" -g "$APP_USER" -m 700 "/home/$APP_USER/.ssh"
if [ ! -f "/home/$APP_USER/.ssh/authorized_keys" ]; then
    install -o "$APP_USER" -g "$APP_USER" -m 600 /dev/null "/home/$APP_USER/.ssh/authorized_keys"
fi

echo "==> Preparing $APP_DIR"
install -d -o "$APP_USER" -g "$APP_USER" -m 755 "$APP_DIR"
install -d -o "$APP_USER" -g "$APP_USER" -m 755 "$APP_DIR/deploy"

echo "==> Allowing '$APP_USER' to restart the service without a password"
cat > "/etc/sudoers.d/$SERVICE" <<EOF
$APP_USER ALL=(root) NOPASSWD: /usr/bin/systemctl restart $SERVICE, /usr/bin/systemctl start $SERVICE, /usr/bin/systemctl stop $SERVICE, /bin/systemctl restart $SERVICE, /bin/systemctl start $SERVICE, /bin/systemctl stop $SERVICE
EOF
chmod 440 "/etc/sudoers.d/$SERVICE"
visudo -cf "/etc/sudoers.d/$SERVICE"

echo "==> Installing the systemd unit"
install -m 644 "$SCRIPT_DIR/$SERVICE.service" "/etc/systemd/system/$SERVICE.service"
systemctl daemon-reload
systemctl enable "$SERVICE"

echo
echo "Bootstrap done. Remaining steps:"
echo "  1. Put the GitHub Actions public key into /home/$APP_USER/.ssh/authorized_keys"
echo "  2. Create $APP_DIR/.env (chmod 600, owned by $APP_USER) with the bot token"
echo "  3. Push to main, or run the Deploy workflow, to sync the code"
echo "  4. systemctl start $SERVICE  &&  journalctl -u $SERVICE -f"
