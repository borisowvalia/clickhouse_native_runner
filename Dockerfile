FROM clickhouse/clickhouse-server:25.3

# Устанавливаем минимальный Python (только runtime, без dev-зависимостей)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3.11 \
        python3-pip \
        python3-setuptools \
        && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --upgrade pip

# Копируем только необходимые файлы
COPY ./clickhouse/storage /var/lib/clickhouse/
COPY ./clickhouse/config/config.xml /etc/clickhouse-server/config.xml
COPY ./clickhouse/config/users.xml /etc/clickhouse-server/users.xml
COPY ./clickhouse/config/entrypoint.sh /entrypoint.sh

# Копируем Python код
COPY ./requirements.txt /requirements.txt
COPY ./server.py /server.py
COPY ./start.sh /start.sh

# Устанавливаем только необходимые Python зависимости
RUN pip3 install --no-cache-dir -r /requirements.txt && \
    chmod +x /start.sh /server.py

# Убедимся, что владелец правильный и директории существуют
RUN chown -R clickhouse:clickhouse /var/lib/clickhouse && \
    mkdir -p /var/log/clickhouse-server && \
    chown -R clickhouse:clickhouse /var/log/clickhouse-server && \
    mkdir -p /tmp && \
    chmod 1777 /tmp

# Запускаем скрипт старта
CMD ["/start.sh"]
