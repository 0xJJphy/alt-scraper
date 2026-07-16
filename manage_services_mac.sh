#!/bin/bash

# manage_services_mac.sh - Gestión de demonios en macOS usando launchd

BASE_DIR="/Users/pedro/Documents/Github/alt-scraper"
PLIST_DIR="$HOME/Library/LaunchAgents"

case "$1" in
    install)
        echo "🚀 Instalando servicios en LaunchAgents..."
        mkdir -p "$PLIST_DIR"
        cp "$BASE_DIR/services/macos/"*.plist "$PLIST_DIR/"
        
        # Cargar los servicios en launchctl
        launchctl bootstrap gui/$(id -u) "$PLIST_DIR/com.altscraper.realtime.plist"
        launchctl bootstrap gui/$(id -u) "$PLIST_DIR/com.altscraper.klines-ws.plist"
        launchctl bootstrap gui/$(id -u) "$PLIST_DIR/com.altscraper.orderbook.plist"
        
        echo "✅ Servicios instalados y activados."
        echo "   Usa './manage_services_mac.sh status' para verificar."
        ;;
    uninstall)
        echo "🗑️ Desinstalando servicios..."
        launchctl bootout gui/$(id -u) "$PLIST_DIR/com.altscraper.realtime.plist" 2>/dev/null
        launchctl bootout gui/$(id -u) "$PLIST_DIR/com.altscraper.klines-ws.plist" 2>/dev/null
        launchctl bootout gui/$(id -u) "$PLIST_DIR/com.altscraper.orderbook.plist" 2>/dev/null
        rm "$PLIST_DIR/com.altscraper.realtime.plist"
        rm "$PLIST_DIR/com.altscraper.klines-ws.plist"
        rm "$PLIST_DIR/com.altscraper.orderbook.plist"
        echo "✅ Hecho."
        ;;
    start)
        echo "▶️ Arrancando servicios..."
        launchctl enable gui/$(id -u)/com.altscraper.realtime
        launchctl enable gui/$(id -u)/com.altscraper.klines-ws
        launchctl enable gui/$(id -u)/com.altscraper.orderbook
        launchctl kickstart gui/$(id -u)/com.altscraper.realtime
        launchctl kickstart gui/$(id -u)/com.altscraper.klines-ws
        launchctl kickstart gui/$(id -u)/com.altscraper.orderbook
        echo "✅ Hecho."
        ;;
    stop)
        echo "⏹️ Deteniendo servicios..."
        # launchd con KeepAlive=true reinciciará el servicio si solo lo matamos, 
        # así que usamos 'disable' o 'kill' específico.
        launchctl disable gui/$(id -u)/com.altscraper.realtime
        launchctl disable gui/$(id -u)/com.altscraper.klines-ws
        launchctl disable gui/$(id -u)/com.altscraper.orderbook
        echo "✅ Hecho. Los servicios no se reiniciarán automáticamente."
        ;;
    status)
        echo "📊 Estado de los servicios (Launchd):"
        launchctl list | grep altscraper || echo "No hay servicios de altscraper corriendo."
        ;;
    logs)
        echo "📋 Mostrando logs combinados (Ctrl+C para salir):"
        tail -f "$BASE_DIR/realtime_daemon.log" "$BASE_DIR/futures_ws_daemon.log" "$BASE_DIR/orderbook_daemon.log"
        ;;
    *)
        echo "Uso: $0 {install|uninstall|start|stop|status|logs}"
        exit 1
esac
