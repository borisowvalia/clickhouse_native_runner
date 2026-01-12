#!/usr/bin/env python3
"""
HTTP-сервер для ClickHouse в Serverless Container.
Использует встроенный http.server для максимальной скорости старта.
"""
import os
import json
import sys
import logging
import time
from datetime import datetime, date
from decimal import Decimal
from logging.config import dictConfig
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from clickhouse_driver import Client

# Конфигурация из переменных окружения (только для дефолтных значений)
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")
SERVER_PORT = int(os.getenv("PORT", "8080"))

# Кэш клиентов по session_id, чтобы сохранять server-side сессию ClickHouse (временные таблицы и т.д.)
# Важно: HTTPServer по умолчанию обрабатывает запросы последовательно, поэтому простой dict ок.
_SESSION_CLIENTS = {}  # key: (user, password, database, session_id) -> {"client": Client, "expires_at": float, "last_used": float}

# Логируем конфигурацию при старте
print(f"Server configuration:", file=sys.stderr)
print(f"  SERVER_PORT: {SERVER_PORT}", file=sys.stderr)
print(f"  CLICKHOUSE_HOST: {CLICKHOUSE_HOST}", file=sys.stderr)
print(f"  CLICKHOUSE_PORT: {CLICKHOUSE_PORT}", file=sys.stderr)
print(f"  CLICKHOUSE_DATABASE: {CLICKHOUSE_DATABASE}", file=sys.stderr)


def json_serialize(obj):
    """
    Конвертирует объекты, которые не сериализуются в JSON по умолчанию.
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, bytes):
        return obj.decode('utf-8', errors='replace')
    elif isinstance(obj, (list, tuple)):
        return [json_serialize(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: json_serialize(value) for key, value in obj.items()}
    return obj


# Кастомный лог-хендлер для сбора трейс-логов
class ListHandler(logging.Handler):
    def __init__(self, logs_list):
        super().__init__()
        self.logs_list = logs_list
    
    def emit(self, record):
        self.logs_list.append(self.format(record))


def setup_trace_logging(logs_list):
    """Настроить логирование для сбора трейс-логов"""
    # Очищаем список логов для нового запроса
    logs_list.clear()
    
    # Настраиваем логирование для clickhouse_driver и всех его подмодулей
    handler = ListHandler(logs_list)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(name)s: %(message)s')
    handler.setFormatter(formatter)
    
    # Настраиваем для основного логгера clickhouse_driver
    logger = logging.getLogger('clickhouse_driver')
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    
    # Также настраиваем для корневого логгера (на случай, если используются другие модули)
    root_logger = logging.getLogger()
    # Добавляем хендлер только если его еще нет
    if not any(isinstance(h, ListHandler) and h.logs_list is logs_list for h in root_logger.handlers):
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.DEBUG)


def get_clickhouse_client(user, password, database=None):
    """Создать клиент ClickHouse с указанными учетными данными"""
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=user,
        password=password,
        database=database or CLICKHOUSE_DATABASE,
        connect_timeout=5,
        send_receive_timeout=30,
    )
    
    return client


class ClickHouseHandler(BaseHTTPRequestHandler):
    """Обработчик HTTP-запросов"""
    
    def _extract_credentials(self):
        """Извлечь учетные данные из запроса"""
        user = None
        password = None
        database = None
        
        # 1. Из HTTP заголовков (приоритет)
        user = self.headers.get("X-ClickHouse-User") or self.headers.get("X-Clickhouse-User")
        password = self.headers.get("X-ClickHouse-Key") or self.headers.get("X-Clickhouse-Key")
        
        # 2. Из Authorization заголовка (формат: Basic base64(user:password))
        if not user and not password:
            auth_header = self.headers.get("Authorization", "")
            if auth_header.startswith("Basic "):
                try:
                    import base64
                    decoded = base64.b64decode(auth_header[6:]).decode("utf-8")
                    if ":" in decoded:
                        user, password = decoded.split(":", 1)
                except Exception:
                    pass
        
        # 3. Из query параметров (для GET запросов)
        parsed = urlparse(self.path)
        query_params = parse_qs(parsed.query)
        if not user:
            user = query_params.get("user", [None])[0]
        if not password:
            password = query_params.get("password", [None])[0]
        if not database:
            database = query_params.get("database", [None])[0]
        
        # 4. Из JSON тела запроса (для POST запросов)
        if self.command == "POST":
            try:
                content_length = int(self.headers.get("Content-Length", 0))
                if content_length > 0:
                    body = self.rfile.read(content_length).decode("utf-8")
                    try:
                        data = json.loads(body)
                        if not user:
                            user = data.get("user")
                        if not password:
                            password = data.get("password")
                        if not database:
                            database = data.get("database")
                    except json.JSONDecodeError:
                        pass
            except Exception:
                pass
        
        return user, password, database

    def _extract_session(self, query_params, body_data):
        """
        Извлечь session_id и session_timeout (сек) из заголовков / query params / JSON body.
        """
        session_id = (
            self.headers.get("X-ClickHouse-Session-Id")
            or self.headers.get("X-Clickhouse-Session-Id")
        )

        if not session_id:
            session_id = (query_params.get("session_id", [None])[0] or query_params.get("sessionId", [None])[0])
        if not session_id and isinstance(body_data, dict):
            session_id = body_data.get("session_id") or body_data.get("sessionId")

        session_timeout = None
        timeout_raw = query_params.get("session_timeout", [None])[0] if query_params else None
        if timeout_raw is None and isinstance(body_data, dict):
            timeout_raw = body_data.get("session_timeout")
        if timeout_raw is not None:
            try:
                session_timeout = int(timeout_raw)
            except Exception:
                session_timeout = None

        # Нормализуем timeout
        if session_timeout is None:
            session_timeout = 120
        session_timeout = max(10, min(session_timeout, 3600))

        return session_id, session_timeout

    def _cleanup_expired_sessions(self):
        now = time.time()
        expired_keys = []
        for key, entry in _SESSION_CLIENTS.items():
            if entry.get("expires_at", 0) <= now:
                expired_keys.append(key)
        for key in expired_keys:
            entry = _SESSION_CLIENTS.pop(key, None)
            if entry and entry.get("client"):
                try:
                    entry["client"].disconnect()
                except Exception:
                    pass
    
    def do_GET(self):
        """Обработка GET запросов"""
        try:
            print(f"GET request to path: {self.path}", file=sys.stderr)
            
            # Парсим путь без query параметров для проверки
            parsed_path = urlparse(self.path)
            path_only = parsed_path.path
            query_params = parse_qs(parsed_path.query)
            
            # Проверяем, есть ли query параметр 'q' или 'query' (как в HTTP API ClickHouse)
            has_query_param = bool(query_params.get("q") or query_params.get("query"))
            
            if path_only == "/health":
                # Простой health check без зависимостей
                try:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self._send_cors_headers()
                    self.end_headers()
                    response = json.dumps({"status": "ok", "service": "clickhouse-proxy"}, ensure_ascii=False)
                    self.wfile.write(response.encode("utf-8"))
                    self.wfile.flush()
                except Exception as e:
                    print(f"Error sending health check response: {e}", file=sys.stderr)
                return
            
            # Обрабатываем запросы на /query или на корневом пути / с query параметрами
            if path_only == "/query" or path_only.startswith("/query") or (path_only == "/" and has_query_param):
                self._handle_query()
                return
            
            # Если корневой путь без query параметров - health check
            if path_only == "/" and not has_query_param:
                try:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self._send_cors_headers()
                    self.end_headers()
                    response = json.dumps({"status": "ok", "service": "clickhouse-proxy"}, ensure_ascii=False)
                    self.wfile.write(response.encode("utf-8"))
                    self.wfile.flush()
                except Exception as e:
                    print(f"Error sending health check response: {e}", file=sys.stderr)
                return
            
            print(f"Path not matched: {path_only}, has_query_param: {has_query_param}", file=sys.stderr)
            self._send_error(404, f"Not Found: {path_only}")
        except Exception as e:
            print(f"Error in do_GET: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
            try:
                self._send_error(500, f"Internal server error: {str(e)}")
            except Exception:
                pass
    
    def do_POST(self):
        """Обработка POST запросов"""
        try:
            print(f"POST request to path: {self.path}", file=sys.stderr)
            
            # Парсим путь без query параметров для проверки
            parsed_path = urlparse(self.path)
            path_only = parsed_path.path
            
            # POST запросы обрабатываем на /query или на корневом пути /
            if path_only == "/query" or path_only.startswith("/query") or path_only == "/":
                self._handle_query()
                return
            
            print(f"Path not matched: {path_only}", file=sys.stderr)
            self._send_error(404, f"Not Found: {path_only}")
        except Exception as e:
            print(f"Error in do_POST: {e}", file=sys.stderr)
            self._send_error(500, f"Internal server error: {str(e)}")
    
    def do_OPTIONS(self):
        """CORS preflight"""
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()
    
    def _handle_query(self):
        """Обработка SQL-запроса"""
        client = None
        use_session_client = False
        logs_list = []
        try:
            print(f"Handling {self.command} request to {self.path}", file=sys.stderr)
            
            # Извлекаем учетные данные
            user, password, database = self._extract_credentials()
            print(f"Extracted credentials: user={user}, database={database}", file=sys.stderr)
            
            if not user:
                self._send_error(401, "User credentials required. Provide X-ClickHouse-User header or 'user' parameter")
                return
            
            # Пароль может быть пустым, но все равно передаем его
            if password is None:
                password = ""
            
            # Читаем тело запроса один раз (для POST)
            body_data = None
            if self.command == "POST":
                content_length = int(self.headers.get("Content-Length", 0))
                if content_length > 0:
                    body = self.rfile.read(content_length).decode("utf-8")
                    try:
                        body_data = json.loads(body)
                    except json.JSONDecodeError:
                        # Если не JSON, считаем что это просто SQL-запрос
                        body_data = {"query": body.strip()}

            parsed = urlparse(self.path)
            query_params = parse_qs(parsed.query)

            # session_id (для сохранения контекста соединения)
            session_id, session_timeout = self._extract_session(query_params, body_data)
            if session_id:
                self._cleanup_expired_sessions()
            
            # Проверяем флаг trace
            enable_trace = False
            if self.command == "GET":
                trace_param = query_params.get("trace", [None])[0]
                enable_trace = trace_param and trace_param.lower() in ("true", "1", "yes")
            elif body_data:
                enable_trace = body_data.get("trace", False)
            
            # Также проверяем заголовок
            if not enable_trace:
                trace_header = self.headers.get("X-ClickHouse-Trace") or self.headers.get("X-Clickhouse-Trace")
                enable_trace = trace_header and trace_header.lower() in ("true", "1", "yes")
            
            # Получаем SQL-запрос
            query = None
            
            if self.command == "GET":
                # Из query параметров (для GET всегда из URL)
                query = query_params.get("q", query_params.get("query", [None]))[0]
                print(f"Extracted query from GET: {query[:50] if query else None}...", file=sys.stderr)
            elif self.command == "POST":
                # Из тела запроса
                if body_data:
                    query = body_data.get("query") or body_data.get("q")
                    print(f"Extracted query from POST body: {query[:50] if query else None}...", file=sys.stderr)
                else:
                    # Если POST без тела, проверяем query параметры в URL
                    query = query_params.get("q", query_params.get("query", [None]))[0]
                    print(f"Extracted query from POST URL params: {query[:50] if query else None}...", file=sys.stderr)
            
            if not query:
                self._send_error(400, "Query parameter 'q' or 'query' is required")
                return
            
            # Настраиваем логирование для трейс-логов (если запрошены)
            if enable_trace:
                setup_trace_logging(logs_list)

            # Создаем/берем клиент. Для session_id — переиспользуем соединение.
            if session_id:
                key = (user, password, database or CLICKHOUSE_DATABASE, session_id)
                entry = _SESSION_CLIENTS.get(key)
                now = time.time()
                if entry and entry.get("client"):
                    client = entry["client"]
                    entry["last_used"] = now
                    entry["expires_at"] = now + session_timeout
                    use_session_client = True
                else:
                    client = get_clickhouse_client(user, password, database)
                    _SESSION_CLIENTS[key] = {"client": client, "last_used": now, "expires_at": now + session_timeout}
                    use_session_client = True
            else:
                client = get_clickhouse_client(user, password, database)
            
            # Выполняем запрос через clickhouse-driver
            exec_settings = {}
            if enable_trace:
                # per-query настройки для логов
                exec_settings["send_logs_level"] = "trace"

            result = client.execute(query, with_column_types=True, settings=exec_settings or None)
            
            # Формируем ответ
            columns = [col[0] for col in result[1]]  # column_types
            rows = result[0]  # data
            column_types = [{"name": col[0], "type": col[1]} for col in result[1]]
            
            # Извлекаем метаинформацию из last_query (аналогично HTTP API ClickHouse)
            statistics = {}
            if hasattr(client, "last_query") and client.last_query:
                last_query = client.last_query
                
                # Progress информация
                if hasattr(last_query, "progress") and last_query.progress:
                    progress = last_query.progress
                    if hasattr(progress, "rows"):
                        statistics["read_rows"] = progress.rows
                    if hasattr(progress, "bytes"):
                        statistics["read_bytes"] = progress.bytes
                    if hasattr(progress, "written_rows"):
                        statistics["written_rows"] = progress.written_rows
                    if hasattr(progress, "written_bytes"):
                        statistics["written_bytes"] = progress.written_bytes
                    if hasattr(progress, "total_rows_to_read"):
                        statistics["total_rows_to_read"] = progress.total_rows_to_read
                
                # Elapsed time (в наносекундах, как в HTTP API)
                elapsed_ns = None
                if hasattr(last_query, "elapsed_ns"):
                    elapsed_ns = last_query.elapsed_ns
                elif hasattr(last_query, "elapsed"):
                    # Конвертируем секунды в наносекунды
                    elapsed_ns = int(last_query.elapsed * 1_000_000_000)
                
                if elapsed_ns is not None:
                    statistics["elapsed_ns"] = elapsed_ns
                    statistics["elapsed_ms"] = elapsed_ns / 1_000_000
                
                # Result information (всегда доступно)
                statistics["result_rows"] = len(rows)
                # Приблизительный размер результата в байтах
                result_bytes = 0
                for row in rows:
                    for val in row:
                        result_bytes += len(str(val).encode("utf-8"))
                statistics["result_bytes"] = result_bytes
            else:
                # Если last_query недоступен, все равно добавляем базовую информацию
                statistics = {
                    "result_rows": len(rows),
                    "result_bytes": sum(len(str(val).encode("utf-8")) for row in rows for val in row) if rows else 0
                }
            
            # Конвертируем данные для JSON сериализации (datetime и другие типы)
            serializable_rows = json_serialize(rows)
            serializable_column_types = json_serialize(column_types)
            serializable_statistics = json_serialize(statistics)
            
            # Формируем ответ в формате, аналогичном HTTP API ClickHouse
            response = {
                "data": serializable_rows,
                "meta": serializable_column_types,
                "rows": len(rows),
                "rows_before_limit_at_least": len(rows),
                "statistics": serializable_statistics
            }
            
            # Добавляем трейс-логи, если запрошены
            if enable_trace and logs_list:
                response["trace"] = logs_list
            
            self._send_json(response)
        
        except Exception as e:
            error_msg = str(e)
            print(f"Error executing query: {error_msg}", file=sys.stderr)
            # Улучшаем сообщения об ошибках
            if "Authentication failed" in error_msg or "Invalid user" in error_msg or "Password" in error_msg:
                self._send_error(401, f"Authentication failed: {error_msg}")
            elif "Connection refused" in error_msg or "Can't connect" in error_msg or "Connection" in error_msg:
                self._send_error(503, f"ClickHouse unavailable: {error_msg}")
            else:
                self._send_error(500, error_msg)
        finally:
            # Закрываем клиент после использования
            if client and not use_session_client:
                try:
                    client.disconnect()
                except Exception:
                    pass
    
    def _send_json(self, data, status=200):
        """Отправить JSON ответ"""
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self._send_cors_headers()
        self.end_headers()
        # Данные уже должны быть сериализуемы, но на всякий случай конвертируем еще раз
        serializable_data = json_serialize(data)
        self.wfile.write(json.dumps(serializable_data, ensure_ascii=False, default=str).encode("utf-8"))
    
    def _send_error(self, status, message):
        """Отправить ошибку"""
        self._send_json({
            "data": [],
            "meta": [],
            "rows": 0,
            "rows_before_limit_at_least": 0,
            "statistics": {},
            "error": message
        }, status)
    
    def _send_cors_headers(self):
        """Добавить CORS заголовки"""
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header(
            "Access-Control-Allow-Headers",
            "Content-Type, X-ClickHouse-User, X-ClickHouse-Key, X-ClickHouse-Trace, X-ClickHouse-Session-Id, Authorization",
        )
    
    def log_message(self, format, *args):
        """Логирование запросов для отладки"""
        print(f"[{self.address_string()}] {format % args}", file=sys.stderr)


def main():
    """Запуск HTTP-сервера"""
    try:
        server = HTTPServer(("0.0.0.0", SERVER_PORT), ClickHouseHandler)
        print(f"ClickHouse HTTP Proxy started on port {SERVER_PORT}", file=sys.stderr)
        print(f"ClickHouse target: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}", file=sys.stderr)
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...", file=sys.stderr)
        server.shutdown()
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
