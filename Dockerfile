FROM rust:1.85-bookworm AS rust-builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY task-core ./task-core
COPY publisher ./publisher
COPY consumer ./consumer
RUN cargo build --release --locked --workspace

FROM node:22-bookworm-slim AS frontend-builder

WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend ./
ARG VITE_PUBLISHER_WS_URL
ARG VITE_DEFAULT_MODEL
ENV VITE_PUBLISHER_WS_URL=${VITE_PUBLISHER_WS_URL}
ENV VITE_DEFAULT_MODEL=${VITE_DEFAULT_MODEL}
RUN npm run build

FROM node:22-bookworm-slim AS runtime

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates tini \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=rust-builder /app/target/release/publisher /usr/local/bin/gyne-publisher
COPY --from=rust-builder /app/target/release/consumer /usr/local/bin/gyne-consumer
COPY --from=frontend-builder /app/frontend/dist ./frontend/dist
COPY frontend/package.json frontend/package-lock.json ./frontend/
RUN cd frontend && npm ci --omit=dev

COPY scripts/start-container.sh ./scripts/start-container.sh
RUN chmod +x ./scripts/start-container.sh

ENV GYNE_AGENT_SERVICE=publisher
EXPOSE 8080

ENTRYPOINT ["tini", "--"]
CMD ["/app/scripts/start-container.sh"]
