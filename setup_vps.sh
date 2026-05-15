#!/bin/bash
# setup_vps.sh - Automates the deployment of alt-scraper systemd units

# --- Configuration ---
INSTALL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Determine the real user (for cases where script is run with sudo)
REAL_USER=$(logname 2>/dev/null || echo $USER)
# This ensures it runs as 'jjphy' even if the script is run with sudo
REPO_OWNER="jjphy"
EXEC_USER=$(stat -c '%U' "$INSTALL_DIR")

# Check for sudo
if [ "$EUID" -ne 0 ]; then
  echo "Please run as root (sudo ./setup_vps.sh)"
  exit 1
fi

echo "--- Alt-Scraper VPS Setup ---"
echo "Installation Directory: $INSTALL_DIR"
echo "Execution User: $EXEC_USER"

# 1. Generate Service File from Template
echo "Generating alt-scraper.service..."
sed -e "s|{{INSTALL_DIR}}|$INSTALL_DIR|g" \
    -e "s|{{USER}}|$EXEC_USER|g" \
    alt-scraper.service.template > alt-scraper.service

# 2. Generate realtime daemon service from template
echo "Generating alt-scraper-realtime.service..."
sed -e "s|{{INSTALL_DIR}}|$INSTALL_DIR|g" \
    -e "s|{{USER}}|$EXEC_USER|g" \
    alt-scraper-realtime.service.template > alt-scraper-realtime.service

# 2b. Generate orderbook daemon service from template
echo "Generating alt-scraper-orderbook.service..."
sed -e "s|{{INSTALL_DIR}}|$INSTALL_DIR|g" \
    -e "s|{{USER}}|$EXEC_USER|g" \
    alt-scraper-orderbook.service.template > alt-scraper-orderbook.service

# 2c. Generate futures klines WebSocket daemon service from template
echo "Generating alt-scraper-klines-ws.service..."
sed -e "s|{{INSTALL_DIR}}|$INSTALL_DIR|g" \
    -e "s|{{USER}}|$EXEC_USER|g" \
    alt-scraper-klines-ws.service.template > alt-scraper-klines-ws.service

# 3. Copy to systemd directory
echo "Installing systemd units..."
# Main service and timer
cp alt-scraper.service /etc/systemd/system/
cp alt-scraper.timer /etc/systemd/system/
# Realtime daemon
cp alt-scraper-realtime.service /etc/systemd/system/
# Orderbook daemon
cp alt-scraper-orderbook.service /etc/systemd/system/
# Futures klines WebSocket daemon
cp alt-scraper-klines-ws.service /etc/systemd/system/
# Notification service
sed "s|{{INSTALL_DIR}}|$INSTALL_DIR|g; s|{{USER}}|$EXEC_USER|g" alt-scraper-notify@.service.template > /etc/systemd/system/alt-scraper-notify@.service

# 3. Set Permissions for vps_run.sh and Directories
echo "Setting permissions for execution user: $EXEC_USER..."
chmod +x "$INSTALL_DIR/vps_run.sh"

# Ensure data and logs directories exist and are owned by the repo owner
mkdir -p "$INSTALL_DIR/data" "$INSTALL_DIR/logs"
chown -R "$EXEC_USER:$EXEC_USER" "$INSTALL_DIR/data" "$INSTALL_DIR/logs"
chmod -R 755 "$INSTALL_DIR/data" "$INSTALL_DIR/logs"

# Ensure venv is owned by the repo owner if it exists
if [ -d "$INSTALL_DIR/venv" ]; then
    echo "Updating venv ownership..."
    chown -R "$EXEC_USER:$EXEC_USER" "$INSTALL_DIR/venv"
fi

# Ensure the app directory is searchable by the systemd service
chmod 755 "$INSTALL_DIR"

# 4. Reload systemd
echo "Reloading systemd and enabling services..."
systemctl daemon-reload

# Daily pipeline timer (runs at 00:15 UTC via alt-scraper.timer)
systemctl enable alt-scraper.timer
systemctl restart alt-scraper.timer

# Realtime daemon (always running, polls every 15 min)
systemctl enable alt-scraper-realtime.service
systemctl restart alt-scraper-realtime.service

# Orderbook daemon (always running, WebSocket + 6 snapshots/day)
systemctl enable alt-scraper-orderbook.service
systemctl restart alt-scraper-orderbook.service

# Futures klines daemon (always running, WebSocket 15m candles + REST reconcile)
systemctl enable alt-scraper-klines-ws.service
systemctl restart alt-scraper-klines-ws.service

echo ""
echo "Setup complete!"
echo ""
echo "Timer status (daily pipeline):"
systemctl status alt-scraper.timer --no-pager
echo ""
echo "Realtime daemon status:"
systemctl status alt-scraper-realtime.service --no-pager
echo ""
echo "Orderbook daemon status:"
systemctl status alt-scraper-orderbook.service --no-pager
echo ""
echo "Futures klines WebSocket daemon status:"
systemctl status alt-scraper-klines-ws.service --no-pager
echo ""
echo "Logs:"
echo "  journalctl -u alt-scraper.service -f           # daily pipeline"
echo "  journalctl -u alt-scraper-realtime.service -f  # realtime daemon"
echo "  journalctl -u alt-scraper-orderbook.service -f # orderbook daemon"
echo "  journalctl -u alt-scraper-klines-ws.service -f # 15m kline WebSocket daemon"
