#!/usr/bin/env bash
# Local dev environment: Postgres 16 and a Temporal dev server with explicit limits.
#
#   scripts/dev.sh up           start Postgres and the Temporal dev server
#   scripts/dev.sh down         stop both
#   scripts/dev.sh status       show what is running
#   scripts/dev.sh temporal-bg  start only the Temporal dev server (CI)
#
# Postgres runs in Docker when a docker daemon is available, otherwise as a
# project-local cluster in .pgdata/ using the postgres 16 binaries on PATH.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PGPORT="${STEPLEDGER_PGPORT:-5432}"
TEMPORAL_DIR="$ROOT/.temporal"
PGDATA_DIR="$ROOT/.pgdata"

# The limits every integration test and bench runs under (build spec, section 0).
TEMPORAL_FLAGS=(
  --db-filename "$TEMPORAL_DIR/dev.db" --ui-port 8233
  --dynamic-config-value limit.blobSize.error=2097152
  --dynamic-config-value limit.blobSize.warn=524288
  --dynamic-config-value limit.historySize.error=52428800
  --dynamic-config-value limit.historySize.warn=10485760
  --dynamic-config-value limit.historyCount.error=51200
  --log-level warn
)

have_docker() { command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; }

pg_find_bin() {
  for d in /usr/local/opt/postgresql@16/bin /opt/homebrew/opt/postgresql@16/bin /usr/lib/postgresql/16/bin; do
    [ -x "$d/pg_ctl" ] && { echo "$d"; return; }
  done
  command -v pg_ctl >/dev/null 2>&1 && dirname "$(command -v pg_ctl)"
}

pg_up() {
  if have_docker; then
    docker compose up -d --wait postgres
    return
  fi
  local bin; bin="$(pg_find_bin)"
  [ -n "$bin" ] || { echo "no docker and no postgres 16 binaries found" >&2; exit 1; }
  if [ ! -f "$PGDATA_DIR/PG_VERSION" ]; then
    "$bin/initdb" -D "$PGDATA_DIR" -U stepledger --auth=trust -E UTF8 >/dev/null
  fi
  if ! "$bin/pg_ctl" -D "$PGDATA_DIR" status >/dev/null 2>&1; then
    LC_ALL=en_US.UTF-8 "$bin/pg_ctl" -D "$PGDATA_DIR" -l "$PGDATA_DIR/server.log" -o "-p $PGPORT -k $PGDATA_DIR" -w start >/dev/null
  fi
  "$bin/psql" -h localhost -p "$PGPORT" -U stepledger -d postgres -tAc \
    "SELECT 1 FROM pg_database WHERE datname='stepledger'" | grep -q 1 \
    || "$bin/createdb" -h localhost -p "$PGPORT" -U stepledger stepledger
  echo "postgres (local cluster) on :$PGPORT"
}

pg_down() {
  if have_docker; then docker compose down; return; fi
  local bin; bin="$(pg_find_bin)"
  [ -f "$PGDATA_DIR/PG_VERSION" ] && "$bin/pg_ctl" -D "$PGDATA_DIR" -m fast stop >/dev/null 2>&1 || true
  echo "postgres stopped"
}

temporal_up() {
  mkdir -p "$TEMPORAL_DIR"
  if temporal operator namespace describe -n default >/dev/null 2>&1; then
    echo "temporal dev server already running"; return
  fi
  nohup temporal server start-dev "${TEMPORAL_FLAGS[@]}" >"$TEMPORAL_DIR/server.log" 2>&1 &
  echo $! >"$TEMPORAL_DIR/server.pid"
  for _ in $(seq 1 60); do
    temporal operator namespace describe -n default >/dev/null 2>&1 && { echo "temporal dev server on :7233 (ui :8233)"; return; }
    sleep 1
  done
  echo "temporal dev server did not come up; see $TEMPORAL_DIR/server.log" >&2; exit 1
}

temporal_down() {
  if [ -f "$TEMPORAL_DIR/server.pid" ]; then
    kill "$(cat "$TEMPORAL_DIR/server.pid")" 2>/dev/null || true
    rm -f "$TEMPORAL_DIR/server.pid"
  fi
  pkill -f "temporal server start-dev --db-filename $TEMPORAL_DIR/dev.db" 2>/dev/null || true
  echo "temporal dev server stopped"
}

case "${1:-}" in
  up) pg_up; temporal_up ;;
  down) temporal_down; pg_down ;;
  temporal-bg) temporal_up ;;
  pg-up) pg_up ;;
  pg-down) pg_down ;;
  status)
    temporal operator namespace describe -n default >/dev/null 2>&1 && echo "temporal: up" || echo "temporal: down"
    pg_isready -h localhost -p "$PGPORT" >/dev/null 2>&1 && echo "postgres: up" || echo "postgres: down"
    ;;
  *) echo "usage: $0 up|down|status|temporal-bg|pg-up|pg-down" >&2; exit 2 ;;
esac
