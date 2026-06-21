#!/usr/bin/env sh
set -eu

service="${GYNE_AGENT_SERVICE:-publisher}"

run_first_available() {
  for candidate in "$@"; do
    if command -v "$candidate" >/dev/null 2>&1; then
      exec "$candidate"
    fi

    if [ -x "$candidate" ]; then
      exec "$candidate"
    fi
  done

  echo "None of these commands were found or executable: $*" >&2
  exit 127
}

case "$service" in
  publisher)
    if [ -z "${PUBLISHER_BIND:-}" ]; then
      export PUBLISHER_BIND="0.0.0.0:${PORT:-8080}"
    fi
    run_first_available gyne-publisher ./bin/publisher ./target/release/publisher
    ;;
  consumer)
    run_first_available gyne-consumer ./bin/consumer ./target/release/consumer
    ;;
  frontend)
    if [ -d /app/frontend ]; then
      cd /app/frontend
    elif [ -d frontend ]; then
      cd frontend
    else
      echo "frontend directory was not found" >&2
      exit 127
    fi

    if [ -x ./node_modules/.bin/vite ]; then
      exec ./node_modules/.bin/vite preview --host 0.0.0.0 --port "${PORT:-3000}"
    fi

    exec npx vite preview --host 0.0.0.0 --port "${PORT:-3000}"
    ;;
  *)
    echo "Unknown GYNE_AGENT_SERVICE: $service" >&2
    echo "Use one of: publisher, consumer, frontend" >&2
    exit 64
    ;;
esac
