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

# 2. Copy to systemd directory
echo "Installing systemd units..."
# Main service and timer
cp alt-scraper.service /etc/systemd/system/
cp alt-scraper.timer /etc/systemd/system/
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
echo "Reloading systemd and enabling timer..."
systemctl daemon-reload
systemctl enable alt-scraper.timer
systemctl restart alt-scraper.timer

echo ""
echo "Setup complete!"
echo "Timer status:"
systemctl status alt-scraper.timer --no-pager
echo ""
echo "To view logs, run: journalctl -u alt-scraper.service -f"
