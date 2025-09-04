#!/bin/bash

set -eo pipefail
shopt -s nullglob

DO_CHOWN=1
if [[ "${CLICKHOUSE_RUN_AS_ROOT:=0}" = "1" || "${CLICKHOUSE_DO_NOT_CHOWN:-0}" = "1" ]]; then
    DO_CHOWN=0
fi

# CLICKHOUSE_UID and CLICKHOUSE_GID are kept for backward compatibility, but deprecated
# One must use either "docker run --user" or CLICKHOUSE_RUN_AS_ROOT=1 to run the process as
# FIXME: Remove ALL CLICKHOUSE_UID CLICKHOUSE_GID before 25.3
if [[ "${CLICKHOUSE_UID:-}" || "${CLICKHOUSE_GID:-}" ]]; then
    echo 'WARNING: Support for CLICKHOUSE_UID/CLICKHOUSE_GID will be removed in a couple of releases.' >&2
    echo 'WARNING: Either use a proper "docker run --user=xxx:xxxx" argument instead of CLICKHOUSE_UID/CLICKHOUSE_GID' >&2
    echo 'WARNING: or set "CLICKHOUSE_RUN_AS_ROOT=1" ENV to run the clickhouse-server as root:root' >&2
fi

# support `docker run --user=xxx:xxxx`
if [[ "$(id -u)" = "0" ]]; then
    if [[ "$CLICKHOUSE_RUN_AS_ROOT" = 1 ]]; then
        USER=0
        GROUP=0
    else
        USER="${CLICKHOUSE_UID:-"$(id -u clickhouse)"}"
        GROUP="${CLICKHOUSE_GID:-"$(id -g clickhouse)"}"
    fi
else
    USER="$(id -u)"
    GROUP="$(id -g)"
    DO_CHOWN=0
fi

# set some vars
CLICKHOUSE_CONFIG="${CLICKHOUSE_CONFIG:-/etc/clickhouse-server/config.xml}"

# get CH directories locations
DATA_DIR="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=path || true)"
TMP_DIR="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=tmp_path || true)"
USER_PATH="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=user_files_path || true)"
LOG_PATH="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=logger.log || true)"
LOG_DIR=""
if [ -n "$LOG_PATH" ]; then LOG_DIR="$(dirname "$LOG_PATH")"; fi
ERROR_LOG_PATH="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=logger.errorlog || true)"
ERROR_LOG_DIR=""
if [ -n "$ERROR_LOG_PATH" ]; then ERROR_LOG_DIR="$(dirname "$ERROR_LOG_PATH")"; fi
FORMAT_SCHEMA_PATH="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=format_schema_path || true)"

readarray -t DISKS_PATHS < <(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key='storage_configuration.disks.*.path' || true)
readarray -t DISKS_METADATA_PATHS < <(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key='storage_configuration.disks.*.metadata_path' || true)

CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD_FILE="${CLICKHOUSE_PASSWORD_FILE:-}"
if [[ -n "${CLICKHOUSE_PASSWORD_FILE}" && -f "${CLICKHOUSE_PASSWORD_FILE}" ]]; then
    CLICKHOUSE_PASSWORD="$(cat "${CLICKHOUSE_PASSWORD_FILE}")"
fi
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-}"
CLICKHOUSE_ACCESS_MANAGEMENT="${CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT:-0}"
CLICKHOUSE_SKIP_USER_SETUP="${CLICKHOUSE_SKIP_USER_SETUP:-0}"

CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS="${CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS:-}"

if [[ $# -lt 1 ]] || [[ "$1" == "--"* ]]; then
    CLICKHOUSE_WATCHDOG_ENABLE=${CLICKHOUSE_WATCHDOG_ENABLE:-0}
    export CLICKHOUSE_WATCHDOG_ENABLE
    cd "$DATA_DIR"

    exec clickhouse su "${USER}:${GROUP}" clickhouse-server --config-file="$CLICKHOUSE_CONFIG" "$@"
fi

exec "$@"
