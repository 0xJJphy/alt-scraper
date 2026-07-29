PYTHON  := venv/bin/python3
LOG_DIR := logs
RUN_DIR := run

.PHONY: services-up services-down services-restart services-status services-logs \
        klines-ws-up klines-ws-down klines-ws-status \
        realtime-up realtime-down realtime-status \
        orderbook-up orderbook-down orderbook-status

$(RUN_DIR):
	@mkdir -p $(RUN_DIR)

# --- klines-ws (futures_ws_daemon.py) ---
klines-ws-up: $(RUN_DIR)
	@if [ -f $(RUN_DIR)/klines-ws.pid ] && kill -0 $$(cat $(RUN_DIR)/klines-ws.pid) 2>/dev/null; then \
		echo "klines-ws already running (PID $$(cat $(RUN_DIR)/klines-ws.pid))"; \
	else \
		nohup $(PYTHON) futures_ws_daemon.py >> $(LOG_DIR)/futures_ws_daemon.log 2>&1 & \
		echo $$! > $(RUN_DIR)/klines-ws.pid; \
		echo "klines-ws started (PID $$(cat $(RUN_DIR)/klines-ws.pid))"; \
	fi

klines-ws-down:
	@if [ -f $(RUN_DIR)/klines-ws.pid ] && kill -0 $$(cat $(RUN_DIR)/klines-ws.pid) 2>/dev/null; then \
		kill $$(cat $(RUN_DIR)/klines-ws.pid) && echo "klines-ws stopped"; \
	else \
		echo "klines-ws not running"; \
	fi; \
	rm -f $(RUN_DIR)/klines-ws.pid

klines-ws-status:
	@if [ -f $(RUN_DIR)/klines-ws.pid ] && kill -0 $$(cat $(RUN_DIR)/klines-ws.pid) 2>/dev/null; then \
		echo "klines-ws: running (PID $$(cat $(RUN_DIR)/klines-ws.pid))"; \
	else \
		echo "klines-ws: stopped"; \
	fi

# --- realtime (realtime_daemon.py) ---
realtime-up: $(RUN_DIR)
	@if [ -f $(RUN_DIR)/realtime.pid ] && kill -0 $$(cat $(RUN_DIR)/realtime.pid) 2>/dev/null; then \
		echo "realtime already running (PID $$(cat $(RUN_DIR)/realtime.pid))"; \
	else \
		nohup $(PYTHON) realtime_daemon.py >> $(LOG_DIR)/realtime_daemon.log 2>&1 & \
		echo $$! > $(RUN_DIR)/realtime.pid; \
		echo "realtime started (PID $$(cat $(RUN_DIR)/realtime.pid))"; \
	fi

realtime-down:
	@if [ -f $(RUN_DIR)/realtime.pid ] && kill -0 $$(cat $(RUN_DIR)/realtime.pid) 2>/dev/null; then \
		kill $$(cat $(RUN_DIR)/realtime.pid) && echo "realtime stopped"; \
	else \
		echo "realtime not running"; \
	fi; \
	rm -f $(RUN_DIR)/realtime.pid

realtime-status:
	@if [ -f $(RUN_DIR)/realtime.pid ] && kill -0 $$(cat $(RUN_DIR)/realtime.pid) 2>/dev/null; then \
		echo "realtime: running (PID $$(cat $(RUN_DIR)/realtime.pid))"; \
	else \
		echo "realtime: stopped"; \
	fi

# --- orderbook (orderbook_daemon.py) ---
orderbook-up: $(RUN_DIR)
	@if [ -f $(RUN_DIR)/orderbook.pid ] && kill -0 $$(cat $(RUN_DIR)/orderbook.pid) 2>/dev/null; then \
		echo "orderbook already running (PID $$(cat $(RUN_DIR)/orderbook.pid))"; \
	else \
		nohup $(PYTHON) orderbook_daemon.py >> $(LOG_DIR)/orderbook_daemon.log 2>&1 & \
		echo $$! > $(RUN_DIR)/orderbook.pid; \
		echo "orderbook started (PID $$(cat $(RUN_DIR)/orderbook.pid))"; \
	fi

orderbook-down:
	@if [ -f $(RUN_DIR)/orderbook.pid ] && kill -0 $$(cat $(RUN_DIR)/orderbook.pid) 2>/dev/null; then \
		kill $$(cat $(RUN_DIR)/orderbook.pid) && echo "orderbook stopped"; \
	else \
		echo "orderbook not running"; \
	fi; \
	rm -f $(RUN_DIR)/orderbook.pid

orderbook-status:
	@if [ -f $(RUN_DIR)/orderbook.pid ] && kill -0 $$(cat $(RUN_DIR)/orderbook.pid) 2>/dev/null; then \
		echo "orderbook: running (PID $$(cat $(RUN_DIR)/orderbook.pid))"; \
	else \
		echo "orderbook: stopped"; \
	fi

services-up: klines-ws-up realtime-up orderbook-up
services-down: klines-ws-down realtime-down orderbook-down
services-restart: services-down services-up
services-status: klines-ws-status realtime-status orderbook-status

services-logs:
	tail -f $(LOG_DIR)/futures_ws_daemon.log $(LOG_DIR)/realtime_daemon.log $(LOG_DIR)/orderbook_daemon.log
