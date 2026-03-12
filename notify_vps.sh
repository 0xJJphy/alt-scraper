#!/bin/bash
# notify_vps.sh - Sends Telegram notifications for different events
# Usage: ./notify_vps.sh <START|SUCCESS|FAILURE> <SERVICE_NAME> [DURATION]

EVENT_TYPE=${1:-"FAILURE"}
UNIT=${2:-"alts-scraper.service"}
DURATION=${3:-""}

# Load environment variables
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
if [ -f "$DIR/.env" ]; then
    export $(grep -v '^#' "$DIR/.env" | xargs)
fi

# Exit if no Telegram config
if [ -z "$TELEGRAM_BOT_TOKEN" ] || [ -z "$TELEGRAM_CHAT_ID" ]; then
    exit 0
fi

HOST=$(hostname)
TIME=$(date "+%Y-%m-%d %H:%M:%S")

case $EVENT_TYPE in
    START)
        MESSAGE="🚀 *Alt-scraper ha empezado a ejecutarse*
*Host:* $HOST
*Hora:* $TIME"
        ;;
    SUCCESS)
        MESSAGE="✅ *Alt-scraper ha terminado correctamente*
*Duración:* $DURATION
*Host:* $HOST
*Hora:* $TIME"
        ;;
    FAILURE)
        MESSAGE="⚠️ *Alt-scraper ha FALLADO* ⚠️
*Unit:* $UNIT
*Host:* $HOST
*Hora:* $TIME

*Últimas 15 líneas del log:*
\`\`\`
$(journalctl -u "$UNIT" -n 15 --no-pager | sed 's/`//g')
\`\`\`"
        ;;
    *)
        MESSAGE="ℹ️ *Notificación de Alt-scraper*
*Evento:* $EVENT_TYPE
*Host:* $HOST"
        ;;
esac

curl -s -X POST "https://api.telegram.org/bot$TELEGRAM_BOT_TOKEN/sendMessage" \
    -d chat_id="$TELEGRAM_CHAT_ID" \
    -d text="$MESSAGE" \
    -d parse_mode="Markdown" > /dev/null
