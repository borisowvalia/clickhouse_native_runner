FROM clickhouse/clickhouse-server:25.3

# Копируем готовую БД с таблицами и данными
COPY ./clickhouse/storage /var/lib/clickhouse/
COPY ./clickhouse/config/config.xml /etc/clickhouse-server/config.xml
COPY ./clickhouse/config/entrypoint.sh /entrypoint.sh

# Убедимся, что владелец правильный
RUN chown -R clickhouse:clickhouse /var/lib/clickhouse