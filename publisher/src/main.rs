use anyhow::{Context, Result};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use redis::streams::StreamReadReply;
use ring::hmac;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::{collections::HashMap, env, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use task_core::{now_millis, ConsumerDiscovery, PublisherResponse, TaskRequest, TaskUpdate};
use tokio::{
    sync::broadcast,
    time::{interval, MissedTickBehavior},
};
use tower_http::{
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};
use tracing::{error, info, warn};

#[derive(Clone)]
struct AppState {
    redis: redis::Client,
    task_stream: String,
    result_stream: String,
    default_model: Option<String>,
    consumer_registry_key: String,
    consumer_stale_after_ms: u64,
    result_block_ms: usize,
    launch_auth: LaunchAuthConfig,
    launch_verify_client: reqwest::Client,
    updates: broadcast::Sender<TaskUpdateEnvelope>,
}

#[derive(Clone)]
struct LaunchAuthConfig {
    secret: Option<String>,
    tool_id: String,
    verify_interval: Duration,
    verify_secret: Option<String>,
    verify_url: Option<String>,
}

#[derive(Clone, Debug)]
struct LaunchSession {
    email: Option<String>,
    install_id: Option<String>,
    issued_at: usize,
    tool_id: String,
    user_id: String,
}

#[derive(Clone, Debug)]
struct TaskUpdateEnvelope {
    install_id: Option<String>,
    update: TaskUpdate,
    user_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LaunchTokenClaims {
    #[serde(default)]
    email: Option<String>,
    exp: usize,
    #[serde(default)]
    install_id: Option<String>,
    iat: usize,
    tool_id: String,
    user_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LaunchSessionVerifyRequest<'a> {
    install_id: &'a str,
    issued_at: usize,
    tool_id: &'a str,
    user_id: &'a str,
}

#[derive(Deserialize)]
struct LaunchSessionVerifyResponse {
    active: bool,
    #[serde(default)]
    error: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let bind_address = publisher_bind_address();
    let bind: SocketAddr = bind_address
        .parse()
        .context("PUBLISHER_BIND must be a valid socket address")?;
    let redis_url = env_or("REDIS_URL", "redis://127.0.0.1/");
    let task_stream = env_or("TASK_STREAM", "openclaw:tasks");
    let result_stream = env_or("RESULT_STREAM", "openclaw:results");
    let default_model = optional_env("DEFAULT_MODEL");
    let consumer_registry_key = env_or("CONSUMER_REGISTRY_KEY", "openclaw:consumers");
    let consumer_stale_after_ms = parse_env("CONSUMER_STALE_AFTER_MS", 15_000)?;
    let result_block_ms = parse_env("RESULT_STREAM_BLOCK_MS", 5_000)?;
    let update_buffer = parse_env("PUBLISHER_UPDATE_BUFFER", 256)?;
    let launch_auth = LaunchAuthConfig::from_env(bind)?;
    let frontend_dist_dir = frontend_dist_dir();
    let (updates, _) = broadcast::channel(update_buffer);

    let state = Arc::new(AppState {
        redis: redis::Client::open(redis_url).context("failed to create Redis client")?,
        task_stream,
        result_stream,
        default_model,
        consumer_registry_key,
        consumer_stale_after_ms,
        result_block_ms,
        launch_auth,
        launch_verify_client: reqwest::Client::new(),
        updates,
    });
    spawn_result_listener(state.clone());

    let frontend_index = frontend_dist_dir.join("index.html");
    if frontend_index.exists() {
        info!(
            frontend_dist_dir = %frontend_dist_dir.display(),
            "serving bundled frontend from publisher"
        );
    } else {
        warn!(
            frontend_dist_dir = %frontend_dist_dir.display(),
            "frontend dist directory is missing; publisher root will return 404"
        );
    }

    let app = Router::new()
        .route("/health", get(health))
        .route("/consumers", get(consumers))
        .route("/ws", get(ws_handler))
        .fallback_service(
            ServeDir::new(&frontend_dist_dir).not_found_service(ServeFile::new(frontend_index)),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("failed to bind publisher on {bind}"))?;
    info!("publisher websocket listening on ws://{bind}/ws");

    axum::serve(listener, app)
        .await
        .context("publisher server failed")
}

async fn health(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(json!({
        "status": "ok",
        "task_stream": state.task_stream,
        "result_stream": state.result_stream,
        "consumer_registry_key": state.consumer_registry_key,
        "has_default_model": state.default_model.is_some(),
        "launch_auth_required": state.launch_auth.is_required(),
        "launch_liveness_required": state.launch_auth.has_session_verifier(),
    }))
}

async fn consumers(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    let session = match launch_session_from_query(&state.launch_auth, &params) {
        Ok(session) => session,
        Err(err) => {
            warn!(error = %err, "consumer discovery auth failed");
            return (
                StatusCode::UNAUTHORIZED,
                "valid 2ndBrain launch token is required",
            )
                .into_response();
        }
    };

    if let Err(err) = verify_launch_session_active(&state, session.as_ref()).await {
        warn!(error = %err, "consumer discovery launch session inactive");
        return (
            StatusCode::UNAUTHORIZED,
            "2ndBrain launch session is no longer active",
        )
            .into_response();
    }

    let response = match state.redis.get_multiplexed_async_connection().await {
        Ok(mut redis) => match list_consumers(&mut redis, &state).await {
            Ok(consumers) => PublisherResponse::Consumers { consumers },
            Err(err) => PublisherResponse::Error {
                message: err.to_string(),
            },
        },
        Err(err) => PublisherResponse::Error {
            message: format!("failed to connect to Redis: {err}"),
        },
    };

    Json(response).into_response()
}

async fn ws_handler(
    Query(params): Query<HashMap<String, String>>,
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    match launch_session_from_query(&state.launch_auth, &params) {
        Ok(session) => {
            if let Err(err) = verify_launch_session_active(&state, session.as_ref()).await {
                warn!(error = %err, "websocket launch session inactive");
                return (
                    StatusCode::UNAUTHORIZED,
                    "2ndBrain launch session is no longer active",
                )
                    .into_response();
            }

            ws.on_upgrade(move |socket| handle_socket(socket, state, session))
                .into_response()
        }
        Err(err) => {
            warn!(error = %err, "websocket launch auth failed");
            (
                StatusCode::UNAUTHORIZED,
                "valid 2ndBrain launch token is required",
            )
                .into_response()
        }
    }
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>, session: Option<LaunchSession>) {
    let (mut sender, mut receiver) = socket.split();
    let mut redis = match state.redis.get_multiplexed_async_connection().await {
        Ok(connection) => connection,
        Err(err) => {
            error!(error = %err, "failed to connect to Redis");
            let _ = send_json(
                &mut sender,
                &PublisherResponse::Error {
                    message: "failed to connect to Redis".to_owned(),
                },
            )
            .await;
            return;
        }
    };
    let mut updates = state.updates.subscribe();
    let mut session_check = interval(state.launch_auth.verify_interval);
    session_check.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = session_check.tick(), if session.is_some() && state.launch_auth.has_session_verifier() => {
                if let Err(err) = verify_launch_session_active(&state, session.as_ref()).await {
                    warn!(error = %err, "closing inactive 2ndBrain launch session");
                    let _ = send_json(
                        &mut sender,
                        &PublisherResponse::Error {
                            message: "2ndBrain session has been logged out. Launch this tool from 2ndBrain again.".to_owned(),
                        },
                    )
                    .await;
                    let _ = sender.send(Message::Close(None)).await;
                    return;
                }
            }
            message = receiver.next() => {
                let Some(message) = message else {
                    return;
                };
                let message = match message {
                    Ok(message) => message,
                    Err(err) => {
                        warn!(error = %err, "websocket receive failed");
                        return;
                    }
                };

                let response = match message {
                    Message::Text(text) => {
                        handle_client_payload(text.to_string().as_bytes(), &state, &mut redis, session.as_ref()).await
                    }
                    Message::Binary(bytes) => {
                        handle_client_payload(bytes.as_ref(), &state, &mut redis, session.as_ref()).await
                    }
                    Message::Ping(bytes) => {
                        if let Err(err) = sender.send(Message::Pong(bytes)).await {
                            warn!(error = %err, "websocket pong failed");
                            return;
                        }
                        continue;
                    }
                    Message::Pong(_) => continue,
                    Message::Close(_) => return,
                };

                if let Err(err) = send_json(&mut sender, &response).await {
                    warn!(error = %err, "websocket send failed");
                    return;
                }
            }
            update = updates.recv() => {
                match update {
                    Ok(update) => {
                        if !task_update_visible_to_session(&update, session.as_ref(), state.launch_auth.is_required()) {
                            continue;
                        }
                        let response = PublisherResponse::TaskUpdate(update.update);
                        if let Err(err) = send_json(&mut sender, &response).await {
                            warn!(error = %err, "websocket update send failed");
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "websocket skipped lagged task updates");
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        }
    }
}

async fn handle_client_payload(
    payload: &[u8],
    state: &AppState,
    redis: &mut redis::aio::MultiplexedConnection,
    session: Option<&LaunchSession>,
) -> PublisherResponse {
    match handle_client_payload_inner(payload, state, redis, session).await {
        Ok(response) => response,
        Err(err) => {
            warn!(error = %err, "publisher request failed");
            PublisherResponse::Error {
                message: err.to_string(),
            }
        }
    }
}

async fn handle_client_payload_inner(
    payload: &[u8],
    state: &AppState,
    redis: &mut redis::aio::MultiplexedConnection,
    session: Option<&LaunchSession>,
) -> Result<PublisherResponse> {
    verify_launch_session_active(state, session).await?;

    let value: Value = serde_json::from_slice(payload).context("invalid publisher JSON")?;
    if value
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|message_type| message_type == "list_consumers")
    {
        return Ok(PublisherResponse::Consumers {
            consumers: list_consumers(redis, state).await?,
        });
    }

    let request_value = if value
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|message_type| message_type == "publish_task")
    {
        value
            .get("task")
            .cloned()
            .context("publish_task messages require a task object")?
    } else {
        value
    };

    let request: TaskRequest =
        serde_json::from_value(request_value).context("invalid task JSON")?;
    let mut task = request
        .into_task(state.default_model.as_deref())
        .context("invalid task payload")?;
    if let Some(session) = session {
        apply_launch_metadata(&mut task, session);
    }
    validate_review_assignment(&task)?;
    let task_id = task.task_id.clone();
    let publish_stream = task_publish_stream(redis, state, &task).await?;
    let serialized = serde_json::to_string(&task).context("failed to serialize task")?;

    let stream_id: String = redis::cmd("XADD")
        .arg(&publish_stream)
        .arg("*")
        .arg("payload")
        .arg(serialized)
        .query_async(redis)
        .await
        .context("failed to append task to Redis stream")?;

    Ok(PublisherResponse::Accepted { task_id, stream_id })
}

impl LaunchAuthConfig {
    fn from_env(bind: SocketAddr) -> Result<Self> {
        let secret = optional_env("GYNE_AGENT_SESSION_SECRET");
        let verify_url = optional_env("MARKETPLACE_LAUNCH_VERIFY_URL");
        let verify_secret = optional_env("MARKETPLACE_LAUNCH_VERIFY_SECRET");
        let verify_interval_seconds: u64 =
            parse_env("MARKETPLACE_LAUNCH_VERIFY_INTERVAL_SECONDS", 15)?;

        if let Some(secret) = secret.as_deref() {
            if secret.as_bytes().len() < 32 {
                anyhow::bail!("GYNE_AGENT_SESSION_SECRET must be at least 32 bytes");
            }
        } else if bind.ip().is_unspecified() {
            warn!(
                bind = %bind,
                "GYNE_AGENT_SESSION_SECRET is not set while publisher is bound publicly; websocket launch auth is disabled"
            );
        }

        if verify_url.is_some() && secret.is_none() {
            anyhow::bail!(
                "GYNE_AGENT_SESSION_SECRET is required when MARKETPLACE_LAUNCH_VERIFY_URL is set"
            );
        }
        if verify_url.is_some() && verify_secret.is_none() {
            anyhow::bail!(
                "MARKETPLACE_LAUNCH_VERIFY_SECRET is required when MARKETPLACE_LAUNCH_VERIFY_URL is set"
            );
        }
        if let Some(secret) = verify_secret.as_deref() {
            if secret.as_bytes().len() < 32 {
                anyhow::bail!("MARKETPLACE_LAUNCH_VERIFY_SECRET must be at least 32 bytes");
            }
        }
        if let Some(url) = verify_url.as_deref() {
            reqwest::Url::parse(url)
                .context("MARKETPLACE_LAUNCH_VERIFY_URL must be a valid URL")?;
        } else if verify_secret.is_some() {
            warn!(
                "MARKETPLACE_LAUNCH_VERIFY_SECRET is set but MARKETPLACE_LAUNCH_VERIFY_URL is missing; logout revocation checks are disabled"
            );
        }

        Ok(Self {
            secret,
            tool_id: env_or("GYNE_AGENT_TOOL_ID", "gyne-agent"),
            verify_interval: Duration::from_secs(verify_interval_seconds.clamp(5, 300)),
            verify_secret,
            verify_url,
        })
    }

    fn is_required(&self) -> bool {
        self.secret.is_some()
    }

    fn has_session_verifier(&self) -> bool {
        self.verify_url.is_some() && self.verify_secret.is_some()
    }
}

fn launch_session_from_query(
    auth: &LaunchAuthConfig,
    params: &HashMap<String, String>,
) -> Result<Option<LaunchSession>> {
    let Some(secret) = auth.secret.as_deref() else {
        return Ok(None);
    };
    let token = params
        .get("token")
        .or_else(|| params.get("launch_token"))
        .or_else(|| params.get("2ndbrain_launch_token"))
        .map(String::as_str)
        .context("launch token is required")?;

    verify_launch_token(auth, secret, token).map(Some)
}

fn verify_launch_token(
    auth: &LaunchAuthConfig,
    secret: &str,
    token: &str,
) -> Result<LaunchSession> {
    let mut parts = token.split('.');
    let header_part = parts.next().context("launch token is missing header")?;
    let payload_part = parts.next().context("launch token is missing payload")?;
    let signature_part = parts.next().context("launch token is missing signature")?;
    if parts.next().is_some() {
        anyhow::bail!("launch token has too many segments");
    }

    let header_bytes = URL_SAFE_NO_PAD
        .decode(header_part)
        .context("launch token header is not base64url")?;
    let header: Value =
        serde_json::from_slice(&header_bytes).context("launch token header is not JSON")?;
    if header.get("alg").and_then(Value::as_str) != Some("HS256") {
        anyhow::bail!("launch token must use HS256");
    }

    let signature = URL_SAFE_NO_PAD
        .decode(signature_part)
        .context("launch token signature is not base64url")?;
    let signing_input = format!("{header_part}.{payload_part}");
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
    hmac::verify(&key, signing_input.as_bytes(), &signature)
        .map_err(|_| anyhow::anyhow!("invalid launch token signature"))?;

    let payload_bytes = URL_SAFE_NO_PAD
        .decode(payload_part)
        .context("launch token payload is not base64url")?;
    let claims: LaunchTokenClaims =
        serde_json::from_slice(&payload_bytes).context("launch token payload is not JSON")?;
    let now_seconds = (now_millis() / 1000) as usize;
    if claims.exp.saturating_add(30) < now_seconds {
        anyhow::bail!("launch token has expired");
    }
    if claims.iat > now_seconds.saturating_add(30) {
        anyhow::bail!("launch token was issued in the future");
    }

    let user_id = claims.user_id.trim();
    let tool_id = claims.tool_id.trim();

    if user_id.is_empty() {
        anyhow::bail!("launch token is missing user_id");
    }
    if tool_id != auth.tool_id {
        anyhow::bail!("launch token tool_id mismatch");
    }

    Ok(LaunchSession {
        email: claims
            .email
            .map(|email| email.trim().to_owned())
            .filter(|email| !email.is_empty()),
        install_id: claims
            .install_id
            .map(|install_id| install_id.trim().to_owned())
            .filter(|install_id| !install_id.is_empty()),
        issued_at: claims.iat,
        tool_id: tool_id.to_owned(),
        user_id: user_id.to_owned(),
    })
}

async fn verify_launch_session_active(
    state: &AppState,
    session: Option<&LaunchSession>,
) -> Result<()> {
    let Some(verify_url) = state.launch_auth.verify_url.as_deref() else {
        return Ok(());
    };
    let verify_secret = state
        .launch_auth
        .verify_secret
        .as_deref()
        .context("MARKETPLACE_LAUNCH_VERIFY_SECRET is required")?;
    let session = session.context("launch session is required")?;
    let install_id = session
        .install_id
        .as_deref()
        .context("launch session is missing install_id")?;
    let response = state
        .launch_verify_client
        .post(verify_url)
        .bearer_auth(verify_secret)
        .json(&LaunchSessionVerifyRequest {
            install_id,
            issued_at: session.issued_at,
            tool_id: &session.tool_id,
            user_id: &session.user_id,
        })
        .send()
        .await
        .context("failed to verify 2ndBrain launch session")?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("failed to read 2ndBrain launch verification response")?;
    let parsed = serde_json::from_str::<LaunchSessionVerifyResponse>(&body).ok();
    let active = parsed.as_ref().is_some_and(|payload| payload.active);

    if !status.is_success() || !active {
        let message = parsed
            .and_then(|payload| payload.error)
            .filter(|message| !message.trim().is_empty())
            .unwrap_or_else(|| format!("2ndBrain launch verification failed with {status}"));

        anyhow::bail!("{message}");
    }

    Ok(())
}

fn apply_launch_metadata(task: &mut task_core::ChatTask, session: &LaunchSession) {
    let mut metadata = match task.metadata.take() {
        Some(Value::Object(metadata)) => metadata,
        Some(value) => {
            let mut metadata = Map::new();
            metadata.insert("client_metadata".to_owned(), value);
            metadata
        }
        None => Map::new(),
    };

    metadata.insert(
        "auth_source".to_owned(),
        Value::String("2ndBrain.ceo".to_owned()),
    );
    metadata.insert("tool_id".to_owned(), Value::String(session.tool_id.clone()));
    metadata.insert("user_id".to_owned(), Value::String(session.user_id.clone()));
    if let Some(install_id) = session.install_id.as_deref() {
        metadata.insert(
            "install_id".to_owned(),
            Value::String(install_id.to_owned()),
        );
    } else {
        metadata.remove("install_id");
    }
    if let Some(email) = session.email.as_deref() {
        metadata.insert("email".to_owned(), Value::String(email.to_owned()));
    } else {
        metadata.remove("email");
    }

    task.metadata = Some(Value::Object(metadata));
}

fn task_update_visible_to_session(
    envelope: &TaskUpdateEnvelope,
    session: Option<&LaunchSession>,
    auth_required: bool,
) -> bool {
    if !auth_required {
        return true;
    }

    let Some(session) = session else {
        return false;
    };
    if envelope.user_id.as_deref() != Some(session.user_id.as_str()) {
        return false;
    }
    if let Some(update_install_id) = envelope.install_id.as_deref() {
        if let Some(session_install_id) = session.install_id.as_deref() {
            return update_install_id == session_install_id;
        }
    }

    true
}

fn validate_review_assignment(task: &task_core::ChatTask) -> Result<()> {
    let Some(metadata) = task.metadata.as_ref() else {
        return Ok(());
    };
    let is_review = metadata
        .get("task_kind")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "review");
    if !is_review {
        return Ok(());
    }

    let Some(assigned_consumer) = task.assigned_consumer.as_deref() else {
        anyhow::bail!("review tasks require an assigned consumer");
    };
    let original_consumer = metadata
        .get("original_consumer")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !original_consumer.is_empty() && original_consumer == assigned_consumer {
        anyhow::bail!("review tasks must be assigned to a different consumer");
    }

    Ok(())
}

async fn task_publish_stream(
    redis: &mut redis::aio::MultiplexedConnection,
    state: &AppState,
    task: &task_core::ChatTask,
) -> Result<String> {
    let Some(assigned_consumer) = task.assigned_consumer.as_deref() else {
        return Ok(state.task_stream.clone());
    };

    let consumers = list_consumers(redis, state).await?;
    consumers
        .into_iter()
        .find(|consumer| consumer.name == assigned_consumer)
        .map(|consumer| consumer.direct_task_stream)
        .with_context(|| format!("assigned consumer is not active: {assigned_consumer}"))
}

async fn list_consumers(
    redis: &mut redis::aio::MultiplexedConnection,
    state: &AppState,
) -> Result<Vec<ConsumerDiscovery>> {
    let now = now_millis();
    let cutoff = now.saturating_sub(state.consumer_stale_after_ms);
    let cutoff_score = cutoff.to_string();

    let _: i64 = redis::cmd("ZREMRANGEBYSCORE")
        .arg(&state.consumer_registry_key)
        .arg("-inf")
        .arg(&cutoff_score)
        .query_async(redis)
        .await
        .context("failed to prune stale consumers")?;

    let names: Vec<String> = redis::cmd("ZRANGEBYSCORE")
        .arg(&state.consumer_registry_key)
        .arg(&cutoff_score)
        .arg("+inf")
        .query_async(redis)
        .await
        .context("failed to list active consumers")?;

    let mut consumers = Vec::with_capacity(names.len());
    for name in names {
        let key = consumer_discovery_key(&state.consumer_registry_key, &name);
        let fields: HashMap<String, String> = redis::cmd("HGETALL")
            .arg(&key)
            .query_async(redis)
            .await
            .with_context(|| format!("failed to read consumer discovery for {name}"))?;

        if fields.is_empty() {
            continue;
        }

        match discovery_from_hash(fields) {
            Some(consumer) if consumer.last_seen_ms >= cutoff => consumers.push(consumer),
            _ => {
                let _: i64 = redis::cmd("ZREM")
                    .arg(&state.consumer_registry_key)
                    .arg(&name)
                    .query_async(redis)
                    .await
                    .context("failed to remove incomplete consumer discovery")?;
            }
        }
    }

    consumers.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(consumers)
}

fn discovery_from_hash(fields: HashMap<String, String>) -> Option<ConsumerDiscovery> {
    Some(ConsumerDiscovery {
        name: fields.get("name")?.to_owned(),
        consumer_group: fields.get("consumer_group")?.to_owned(),
        task_stream: fields.get("task_stream")?.to_owned(),
        direct_task_stream: fields.get("direct_task_stream")?.to_owned(),
        result_stream: fields.get("result_stream")?.to_owned(),
        hostname: fields
            .get("hostname")
            .map(|hostname| hostname.trim().to_owned())
            .filter(|hostname| !hostname.is_empty()),
        status: fields.get("status")?.to_owned(),
        started_at_ms: fields.get("started_at_ms")?.parse().ok()?,
        last_seen_ms: fields.get("last_seen_ms")?.parse().ok()?,
        expires_at_ms: fields.get("expires_at_ms")?.parse().ok()?,
    })
}

fn spawn_result_listener(state: Arc<AppState>) {
    tokio::spawn(async move {
        loop {
            match state.redis.get_multiplexed_async_connection().await {
                Ok(mut redis) => {
                    if let Err(err) = listen_for_results(&mut redis, &state).await {
                        error!(error = %err, "result stream listener failed");
                    }
                }
                Err(err) => {
                    error!(error = %err, "failed to connect to Redis for result listener");
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    });
}

async fn listen_for_results(
    redis: &mut redis::aio::MultiplexedConnection,
    state: &AppState,
) -> Result<()> {
    let mut last_id = "$".to_owned();
    info!(result_stream = %state.result_stream, "publisher result listener started");

    loop {
        let reply: StreamReadReply = redis::cmd("XREAD")
            .arg("BLOCK")
            .arg(state.result_block_ms)
            .arg("COUNT")
            .arg(10)
            .arg("STREAMS")
            .arg(&state.result_stream)
            .arg(&last_id)
            .query_async(redis)
            .await
            .context("failed to read result stream")?;

        for stream in reply.keys {
            for message in stream.ids {
                last_id = message.id.clone();
                match result_update_from_fields(&message.id, &message.map) {
                    Ok(update) => {
                        let _ = state.updates.send(update);
                    }
                    Err(err) => {
                        warn!(result_stream_id = %message.id, error = %err, "failed to parse task result");
                    }
                }
            }
        }
    }
}

fn result_update_from_fields(
    result_stream_id: &str,
    fields: &HashMap<String, redis::Value>,
) -> Result<TaskUpdateEnvelope> {
    let payload = fields
        .get("payload")
        .context("result stream entry is missing payload field")
        .and_then(|value| {
            redis::from_redis_value::<String>(value).context("payload field is not a string")
        })?;
    let value: Value = serde_json::from_str(&payload).context("result payload is not JSON")?;
    Ok(TaskUpdateEnvelope {
        install_id: optional_string(value.pointer("/metadata/install_id")),
        update: result_update_from_value(result_stream_id, &value),
        user_id: optional_string(value.pointer("/metadata/user_id")),
    })
}

fn result_update_from_value(result_stream_id: &str, value: &Value) -> TaskUpdate {
    let raw_status = value
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("completed");
    let response = value.get("response").cloned();
    let response_content = response.as_ref().and_then(completion_text);
    let structured_content = response_content
        .as_deref()
        .and_then(|content| serde_json::from_str::<Value>(content).ok());
    let response_status = response
        .as_ref()
        .and_then(|response| response.get("status"))
        .and_then(Value::as_str)
        .or_else(|| {
            structured_content
                .as_ref()
                .and_then(|content| content.get("status"))
                .and_then(Value::as_str)
        });
    let questions = extract_questions(value);
    let status = if raw_status == "failed" || response_status == Some("failed") {
        "failed"
    } else if raw_status == "needs_input"
        || response_status == Some("needs_input")
        || !questions.is_empty()
    {
        "needs_input"
    } else {
        "done"
    }
    .to_owned();
    let message = value
        .get("error")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            structured_content
                .as_ref()
                .and_then(|content| content.get("message"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .or(response_content)
        .or_else(|| Some(status_message(&status).to_owned()));

    TaskUpdate {
        task_id: optional_string(value.get("task_id")),
        card_id: value
            .pointer("/metadata/card_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        task_kind: value
            .pointer("/metadata/task_kind")
            .and_then(Value::as_str)
            .map(str::to_owned),
        status,
        message,
        questions,
        error: value
            .get("error")
            .and_then(Value::as_str)
            .map(str::to_owned),
        consumer: value
            .get("consumer")
            .and_then(Value::as_str)
            .map(str::to_owned),
        source_stream_id: value
            .get("source_stream_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        result_stream_id: Some(result_stream_id.to_owned()),
        completed_at_ms: value.get("completed_at_ms").and_then(Value::as_u64),
        response,
        telegram: value.get("telegram").cloned(),
    }
}

fn extract_questions(value: &Value) -> Vec<String> {
    let direct: Vec<String> = value
        .get("questions")
        .or_else(|| value.pointer("/response/questions"))
        .and_then(Value::as_array)
        .map(|questions| {
            questions
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();

    if !direct.is_empty() {
        return direct;
    }

    value
        .get("response")
        .and_then(completion_text)
        .and_then(|content| serde_json::from_str::<Value>(&content).ok())
        .and_then(|content| content.get("questions").cloned())
        .and_then(|questions| questions.as_array().cloned())
        .map(|questions| {
            questions
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn completion_text(value: &Value) -> Option<String> {
    value
        .pointer("/choices/0/message/content")
        .and_then(Value::as_str)
        .or_else(|| value.get("output_text").and_then(Value::as_str))
        .or_else(|| value.get("content").and_then(Value::as_str))
        .map(str::to_owned)
}

fn optional_string(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(value) if !value.trim().is_empty() => Some(value.to_owned()),
        _ => None,
    })
}

fn status_message(status: &str) -> &'static str {
    match status {
        "done" => "Task completed",
        "needs_input" => "Task needs more input",
        "failed" => "Task failed",
        _ => "Task updated",
    }
}

async fn send_json<T: Serialize>(
    sender: &mut SplitSink<WebSocket, Message>,
    value: &T,
) -> Result<()> {
    let text = serde_json::to_string(value).context("failed to serialize websocket response")?;
    sender
        .send(Message::Text(text.into()))
        .await
        .context("failed to send websocket response")
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn publisher_bind_address() -> String {
    if let Some(bind) = optional_env("PUBLISHER_BIND") {
        if is_railway_deploy() && is_loopback_bind(&bind) {
            let railway_bind = public_port_bind_address();
            warn!(
                configured_bind = bind,
                railway_bind, "overriding loopback PUBLISHER_BIND for Railway"
            );
            return railway_bind;
        }

        return bind;
    }

    public_port_bind_address()
}

fn public_port_bind_address() -> String {
    let port = env_or("PORT", "8080");
    format!("0.0.0.0:{port}")
}

fn is_railway_deploy() -> bool {
    optional_env("RAILWAY_ENVIRONMENT").is_some()
        || optional_env("RAILWAY_PROJECT_ID").is_some()
        || optional_env("RAILWAY_SERVICE_ID").is_some()
}

fn is_loopback_bind(bind: &str) -> bool {
    bind.parse::<SocketAddr>()
        .map(|address| address.ip().is_loopback())
        .unwrap_or(false)
}

fn frontend_dist_dir() -> PathBuf {
    optional_env("FRONTEND_DIST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("frontend/dist"))
}

fn optional_env(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.trim().is_empty())
}

fn parse_env<T>(key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match optional_env(key) {
        Some(value) => value
            .parse::<T>()
            .with_context(|| format!("{key} must be a valid value")),
        None => Ok(default),
    }
}

fn consumer_discovery_key(registry_key: &str, name: &str) -> String {
    format!("{registry_key}:{name}")
}
