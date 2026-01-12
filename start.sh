#!/bin/bash
# Минимальный скрипт запуска для быстрого старта

# Не используем set -e, чтобы обработать ошибки вручную
set +e

echo "=== Starting ClickHouse Native Runner ===" >&2
echo "PORT=${PORT:-8080}" >&2

# Запускаем ClickHouse в фоне
echo "Starting ClickHouse..." >&2

# Убедимся, что директории логов существуют и имеют правильные права
mkdir -p /var/log/clickhouse-server
chown -R clickhouse:clickhouse /var/log/clickhouse-server 2>/dev/null || true

# Запускаем ClickHouse от пользователя clickhouse
# Используем su для переключения на правильного пользователя
cd /var/lib/clickhouse
su clickhouse -s /bin/bash -c "clickhouse-server --config-file=/etc/clickhouse-server/config.xml > /tmp/clickhouse-startup.log 2>&1" &

# Даем время на запуск
sleep 3

# Проверяем, что процесс clickhouse-server запустился (по имени процесса)
CLICKHOUSE_RUNNING=0
for i in {1..10}; do
    if pgrep -f "clickhouse-server" > /dev/null; then
        CLICKHOUSE_RUNNING=1
        CLICKHOUSE_PID=$(pgrep -f "clickhouse-server" | head -1)
        echo "ClickHouse process started with PID: $CLICKHOUSE_PID" >&2
        break
    fi
    sleep 0.5
done

if [ $CLICKHOUSE_RUNNING -eq 0 ]; then
    echo "ERROR: ClickHouse process failed to start" >&2
    echo "=== ClickHouse startup log ===" >&2
    cat /tmp/clickhouse-startup.log >&2 || echo "Startup log not available" >&2
    echo "=== ClickHouse error log ===" >&2
    tail -100 /var/log/clickhouse-server/clickhouse-server.err.log 2>/dev/null || echo "Error log not available" >&2
    echo "=== ClickHouse server log ===" >&2
    tail -100 /var/log/clickhouse-server/clickhouse-server.log 2>/dev/null || echo "Server log not available" >&2
    exit 1
fi

# Ждем готовности ClickHouse
echo "Waiting for ClickHouse to be ready..." >&2
CLICKHOUSE_READY=0
for i in {1..30}; do
    # Проверяем порт 9000 (нативный протокол)
    if timeout 1 bash -c "echo > /dev/tcp/localhost/9000" 2>/dev/null; then
        echo "ClickHouse is ready on port 9000" >&2
        CLICKHOUSE_READY=1
        break
    fi
    echo "Attempt $i/30: ClickHouse not ready yet..." >&2
    sleep 0.5
done

if [ $CLICKHOUSE_READY -eq 0 ]; then
    echo "WARNING: ClickHouse may not be fully ready, but continuing..." >&2
fi

# Дополнительная проверка через HTTP (если curl доступен, опционально)
if command -v curl > /dev/null 2>&1; then
    for i in {1..10}; do
        if curl -s http://localhost:8123/ping > /dev/null 2>&1; then
            echo "ClickHouse HTTP interface is ready" >&2
            break
        fi
        sleep 0.5
    done
fi

# Запускаем Python HTTP-сервер
echo "Starting Python HTTP server on port ${PORT:-8080}..." >&2
export PORT=${PORT:-8080}
exec python3 /server.py



