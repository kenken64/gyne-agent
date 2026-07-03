use anyhow::{Context, Result};
use axum::{
    body::Body,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::{header, HeaderMap, HeaderValue, StatusCode, Uri},
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
use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use task_core::{now_millis, ConsumerDiscovery, PublisherResponse, TaskRequest, TaskUpdate};
use tokio::{
    sync::broadcast,
    time::{interval, MissedTickBehavior},
};
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};

#[derive(Clone)]
struct AppState {
    redis: redis::Client,
    task_stream: String,
    result_stream: String,
    default_model: Option<String>,
    consumer_registry_key: String,
    owner_registry_prefix: String,
    consumer_stale_after_ms: u64,
    result_block_ms: usize,
    frontend_dist_dir: PathBuf,
    launch_auth: LaunchAuthConfig,
    launch_verify_client: reqwest::Client,
    updates: broadcast::Sender<TaskUpdateEnvelope>,
    // Server-to-server bridge to 2ndBrain (`/api/brain/*` proxy routes). The browser never holds
    // this secret; it authenticates to the publisher with its launch session cookie instead.
    brain_api_url: Option<String>,
    brain_api_secret: Option<String>,
}

#[derive(Clone)]
struct LaunchAuthConfig {
    secret: Option<String>,
    session_cookie_name: String,
    tool_id: String,
    verify_interval: Duration,
    verify_secret: Option<String>,
    verify_url: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LaunchSession {
    email: Option<String>,
    expires_at: usize,
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
    let owner_registry_prefix = env_or("CONSUMER_OWNER_KEY_PREFIX", "openclaw:owners");
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
        owner_registry_prefix,
        consumer_stale_after_ms,
        result_block_ms,
        frontend_dist_dir: frontend_dist_dir.clone(),
        launch_auth,
        launch_verify_client: reqwest::Client::new(),
        updates,
        brain_api_url: optional_env("GYNE_BRAIN_API_URL"),
        brain_api_secret: optional_env("GYNE_BRAIN_API_SECRET"),
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
        .route("/api/session", get(session_status))
        .route("/api/brain/projects", get(brain_projects))
        .route("/ws", get(ws_handler))
        .fallback(frontend_handler)
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
        "owner_registry_prefix": state.owner_registry_prefix,
        "has_default_model": state.default_model.is_some(),
        "launch_auth_required": state.launch_auth.is_required(),
        "launch_liveness_required": state.launch_auth.has_session_verifier(),
        "frontend_auth_required": state.launch_auth.is_required(),
    }))
}

async fn frontend_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    uri: Uri,
) -> axum::response::Response {
    let set_cookie = match verify_frontend_launch_session(&state, &params, &headers).await {
        Ok(set_cookie) => set_cookie,
        Err(err) => {
            warn!(path = %uri.path(), error = %err, "frontend launch auth failed");
            return launch_required_response(
                &state.launch_auth,
                StatusCode::UNAUTHORIZED,
                "Launch this tool from 2ndBrain to continue.",
                true,
            );
        }
    };

    let path = match frontend_static_path(&state.frontend_dist_dir, uri.path()).await {
        Ok(path) => path,
        Err(err) => {
            warn!(path = %uri.path(), error = %err, "frontend static path failed");
            return launch_required_response(
                &state.launch_auth,
                StatusCode::NOT_FOUND,
                "Requested frontend asset was not found.",
                false,
            );
        }
    };

    let body = match tokio::fs::read(&path).await {
        Ok(body) => body,
        Err(err) => {
            warn!(path = %path.display(), error = %err, "failed to read frontend asset");
            return launch_required_response(
                &state.launch_auth,
                StatusCode::NOT_FOUND,
                "Requested frontend asset was not found.",
                false,
            );
        }
    };

    static_response(
        StatusCode::OK,
        frontend_content_type(&path),
        frontend_cache_control(&path),
        Body::from(body),
        set_cookie,
    )
}

async fn consumers(
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    let session = match launch_session_from_request(&state.launch_auth, &params, &headers) {
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

    let owner = launch_owner_id(session.as_ref());
    let response = match state.redis.get_multiplexed_async_connection().await {
        Ok(mut redis) => match list_consumers(&mut redis, &state, owner).await {
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

/// Proxies the launching user's 2ndBrain project list. The browser authenticates with its
/// launch session cookie; the publisher forwards server-to-server with the bridge secret and
/// the session's user_id, mirroring `verify_launch_session_active`.
async fn brain_projects(
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    let session = match launch_session_from_request(&state.launch_auth, &params, &headers) {
        Ok(Some(session)) => session,
        Ok(None) => {
            // Launch auth disabled (local dev): no user to scope by, so no projects.
            return Json(json!({ "projects": [] })).into_response();
        }
        Err(err) => {
            warn!(error = %err, "brain projects auth failed");
            return (
                StatusCode::UNAUTHORIZED,
                "valid 2ndBrain launch session is required",
            )
                .into_response();
        }
    };

    if let Err(err) = verify_launch_session_active(&state, Some(&session)).await {
        warn!(error = %err, "brain projects launch session inactive");
        return (
            StatusCode::UNAUTHORIZED,
            "2ndBrain launch session is no longer active",
        )
            .into_response();
    }

    let (Some(base_url), Some(secret)) = (
        state.brain_api_url.as_deref(),
        state.brain_api_secret.as_deref(),
    ) else {
        return Json(json!({ "projects": [] })).into_response();
    };

    let url = format!("{}/api/gyne/projects", base_url.trim_end_matches('/'));
    let response = state
        .launch_verify_client
        .get(&url)
        .bearer_auth(secret)
        .query(&[("user_id", session.user_id.as_str())])
        .send()
        .await;

    match response {
        Ok(upstream) => {
            let status = StatusCode::from_u16(upstream.status().as_u16())
                .unwrap_or(StatusCode::BAD_GATEWAY);
            match upstream.text().await {
                Ok(body) => (
                    status,
                    [(header::CONTENT_TYPE, "application/json")],
                    body,
                )
                    .into_response(),
                Err(err) => {
                    warn!(error = %err, "brain projects upstream body failed");
                    (StatusCode::BAD_GATEWAY, "2ndBrain bridge response failed").into_response()
                }
            }
        }
        Err(err) => {
            warn!(error = %err, "brain projects upstream request failed");
            (StatusCode::BAD_GATEWAY, "2ndBrain bridge request failed").into_response()
        }
    }
}

async fn session_status(
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    if !state.launch_auth.is_required() {
        return json_status_response(
            &state.launch_auth,
            StatusCode::OK,
            json!({
                "authenticated": false,
                "launchAuthRequired": false,
            }),
            false,
        );
    }

    let session = match launch_session_from_request(&state.launch_auth, &params, &headers) {
        Ok(Some(session)) => session,
        Ok(None) => {
            return json_status_response(
                &state.launch_auth,
                StatusCode::UNAUTHORIZED,
                json!({
                    "authenticated": false,
                    "error": "Launch this tool from 2ndBrain to continue.",
                    "launchAuthRequired": true,
                }),
                true,
            );
        }
        Err(err) => {
            return json_status_response(
                &state.launch_auth,
                StatusCode::UNAUTHORIZED,
                json!({
                    "authenticated": false,
                    "error": err.to_string(),
                    "launchAuthRequired": true,
                }),
                true,
            );
        }
    };

    if let Err(err) = verify_launch_session_active(&state, Some(&session)).await {
        return json_status_response(
            &state.launch_auth,
            StatusCode::UNAUTHORIZED,
            json!({
                "authenticated": false,
                "error": err.to_string(),
                "launchAuthRequired": true,
            }),
            true,
        );
    }

    json_status_response(
        &state.launch_auth,
        StatusCode::OK,
        json!({
            "authenticated": true,
            "email": session.email,
            "exp": session.expires_at,
            "installId": session.install_id,
            "toolId": session.tool_id,
            "userId": session.user_id,
        }),
        false,
    )
}

async fn ws_handler(
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    match launch_session_from_request(&state.launch_auth, &params, &headers) {
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
            consumers: list_consumers(redis, state, launch_owner_id(session)).await?,
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
    let publish_stream = task_publish_stream(redis, state, &task, launch_owner_id(session)).await?;
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
            session_cookie_name: env_or("GYNE_AGENT_SESSION_COOKIE", "gyne_agent_session"),
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

fn launch_session_from_request(
    auth: &LaunchAuthConfig,
    params: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Result<Option<LaunchSession>> {
    let Some(secret) = auth.secret.as_deref() else {
        return Ok(None);
    };

    if let Some(token) = launch_token_from_params(params) {
        return verify_launch_token(auth, secret, token).map(Some);
    }

    if let Some(cookie) = launch_session_cookie(headers, &auth.session_cookie_name) {
        return verify_launch_session_cookie(auth, secret, cookie).map(Some);
    }

    anyhow::bail!("launch token or active launch session cookie is required");
}

fn launch_token_from_params(params: &HashMap<String, String>) -> Option<&str> {
    params
        .get("token")
        .or_else(|| params.get("launch_token"))
        .or_else(|| params.get("2ndbrain_launch_token"))
        .map(String::as_str)
        .map(str::trim)
        .filter(|token| !token.is_empty())
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
        expires_at: claims.exp,
        install_id: claims
            .install_id
            .map(|install_id| install_id.trim().to_owned())
            .filter(|install_id| !install_id.is_empty()),
        issued_at: claims.iat,
        tool_id: tool_id.to_owned(),
        user_id: user_id.to_owned(),
    })
}

fn verify_launch_session_cookie(
    auth: &LaunchAuthConfig,
    secret: &str,
    cookie: &str,
) -> Result<LaunchSession> {
    let (payload_part, signature_part) = cookie
        .split_once('.')
        .context("launch session cookie is malformed")?;
    if signature_part.contains('.') {
        anyhow::bail!("launch session cookie has too many segments");
    }

    let signature = URL_SAFE_NO_PAD
        .decode(signature_part)
        .context("launch session cookie signature is not base64url")?;
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
    hmac::verify(&key, payload_part.as_bytes(), &signature)
        .map_err(|_| anyhow::anyhow!("invalid launch session cookie signature"))?;

    let payload_bytes = URL_SAFE_NO_PAD
        .decode(payload_part)
        .context("launch session cookie payload is not base64url")?;
    let session: LaunchSession = serde_json::from_slice(&payload_bytes)
        .context("launch session cookie payload is invalid")?;
    let now_seconds = (now_millis() / 1000) as usize;

    if session.expires_at.saturating_add(30) < now_seconds {
        anyhow::bail!("launch session cookie has expired");
    }
    if session.issued_at > now_seconds.saturating_add(30) {
        anyhow::bail!("launch session cookie was issued in the future");
    }
    if session.user_id.trim().is_empty() {
        anyhow::bail!("launch session cookie is missing user_id");
    }
    if session.tool_id != auth.tool_id {
        anyhow::bail!("launch session cookie tool_id mismatch");
    }

    Ok(session)
}

fn sign_launch_session_cookie(secret: &str, session: &LaunchSession) -> Result<String> {
    let payload = serde_json::to_vec(session).context("failed to serialize launch session")?;
    let payload_part = URL_SAFE_NO_PAD.encode(payload);
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
    let signature = hmac::sign(&key, payload_part.as_bytes());
    let signature_part = URL_SAFE_NO_PAD.encode(signature.as_ref());

    Ok(format!("{payload_part}.{signature_part}"))
}

fn launch_session_cookie<'a>(headers: &'a HeaderMap, cookie_name: &str) -> Option<&'a str> {
    headers
        .get(header::COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|cookies| {
            cookies.split(';').find_map(|cookie| {
                let (name, value) = cookie.trim().split_once('=')?;

                if name == cookie_name && !value.trim().is_empty() {
                    Some(value.trim())
                } else {
                    None
                }
            })
        })
}

async fn verify_frontend_launch_session(
    state: &AppState,
    params: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Result<Option<String>> {
    let Some(secret) = state.launch_auth.secret.as_deref() else {
        return Ok(None);
    };

    if let Some(token) = launch_token_from_params(params) {
        let session = verify_launch_token(&state.launch_auth, secret, token)?;
        verify_launch_session_active(state, Some(&session)).await?;
        return Ok(Some(session_cookie_header(
            &state.launch_auth,
            headers,
            secret,
            &session,
        )?));
    }

    let cookie = launch_session_cookie(headers, &state.launch_auth.session_cookie_name)
        .context("launch token or active launch session cookie is required")?;
    let session = verify_launch_session_cookie(&state.launch_auth, secret, cookie)?;
    verify_launch_session_active(state, Some(&session)).await?;

    Ok(None)
}

fn session_cookie_header(
    auth: &LaunchAuthConfig,
    headers: &HeaderMap,
    secret: &str,
    session: &LaunchSession,
) -> Result<String> {
    let now_seconds = (now_millis() / 1000) as usize;
    let max_age = session.expires_at.saturating_sub(now_seconds).max(1);
    let mut attributes = vec![
        format!(
            "{}={}",
            auth.session_cookie_name,
            sign_launch_session_cookie(secret, session)?
        ),
        "HttpOnly".to_owned(),
        "Path=/".to_owned(),
        "SameSite=Lax".to_owned(),
        format!("Max-Age={max_age}"),
    ];

    if secure_request(headers) {
        attributes.push("Secure".to_owned());
    }

    Ok(attributes.join("; "))
}

fn clear_session_cookie_header(auth: &LaunchAuthConfig) -> String {
    format!(
        "{}=; HttpOnly; Path=/; SameSite=Lax; Max-Age=0",
        auth.session_cookie_name
    )
}

fn secure_request(headers: &HeaderMap) -> bool {
    headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|proto| proto.eq_ignore_ascii_case("https"))
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
    owner: Option<&str>,
) -> Result<String> {
    let Some(assigned_consumer) = task.assigned_consumer.as_deref() else {
        return Ok(state.task_stream.clone());
    };

    // `list_consumers` is already scoped to the launching user, so a task can only be routed to a
    // consumer the caller owns; an unowned target surfaces as "not active".
    let consumers = list_consumers(redis, state, owner).await?;
    consumers
        .into_iter()
        .find(|consumer| consumer.name == assigned_consumer)
        .map(|consumer| consumer.direct_task_stream)
        .with_context(|| format!("assigned consumer is not active: {assigned_consumer}"))
}

/// Owner id (2ndBrain `user_id`) used to scope consumer discovery. `None` when launch auth is
/// disabled (local/dev), in which case discovery is unfiltered as before.
fn launch_owner_id(session: Option<&LaunchSession>) -> Option<&str> {
    session.map(|session| session.user_id.as_str())
}

/// Field of a consumer's discovery record that is matched against the owner set.
///
/// Design A ("2ndBrain owns the map"): 2ndBrain writes each owned `CONSUMER_NAME` into
/// `openclaw:owners:{user_id}`, so we match on the registry name. If a deployment cannot control the
/// provisioned `CONSUMER_NAME`, switch this to the hostname the consumer publishes
/// (`consumer.hostname.as_deref().unwrap_or_default()`) and have 2ndBrain populate the owner set with
/// hostnames instead — the rest of the flow is unchanged.
fn consumer_owner_match_key(consumer: &ConsumerDiscovery) -> &str {
    consumer.name.as_str()
}

/// Consumer names (or match keys) that belong to `owner_id`, per the 2ndBrain-maintained owner map.
async fn owned_consumer_keys(
    redis: &mut redis::aio::MultiplexedConnection,
    state: &AppState,
    owner_id: &str,
) -> Result<std::collections::HashSet<String>> {
    let key = format!("{}:{owner_id}", state.owner_registry_prefix);
    let members: Vec<String> = redis::cmd("SMEMBERS")
        .arg(&key)
        .query_async(redis)
        .await
        .with_context(|| format!("failed to list owned consumers for {owner_id}"))?;

    Ok(members.into_iter().collect())
}

async fn list_consumers(
    redis: &mut redis::aio::MultiplexedConnection,
    state: &AppState,
    owner: Option<&str>,
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

    if let Some(owner_id) = owner {
        let owned = owned_consumer_keys(redis, state, owner_id).await?;
        consumers.retain(|consumer| owned.contains(consumer_owner_match_key(consumer)));
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

async fn frontend_static_path(root: &Path, request_path: &str) -> Result<PathBuf> {
    let candidate = frontend_candidate_path(root, request_path)?;

    match tokio::fs::metadata(&candidate).await {
        Ok(metadata) if metadata.is_file() => Ok(candidate),
        _ => {
            let index = root.join("index.html");
            match tokio::fs::metadata(&index).await {
                Ok(metadata) if metadata.is_file() => Ok(index),
                _ => anyhow::bail!("frontend index.html was not found"),
            }
        }
    }
}

fn frontend_candidate_path(root: &Path, request_path: &str) -> Result<PathBuf> {
    let mut path = root.to_path_buf();
    let relative = request_path.trim_start_matches('/');
    let relative = if relative.is_empty() {
        "index.html"
    } else {
        relative
    };

    for segment in relative.split('/') {
        if segment.is_empty() || segment == "." {
            continue;
        }
        if segment == ".." || segment.contains('\\') {
            anyhow::bail!("frontend path is not allowed");
        }

        path.push(segment);
    }

    Ok(path)
}

fn frontend_content_type(path: &Path) -> &'static str {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or_default()
    {
        "css" => "text/css; charset=utf-8",
        "gif" => "image/gif",
        "html" => "text/html; charset=utf-8",
        "ico" => "image/x-icon",
        "jpg" | "jpeg" => "image/jpeg",
        "js" => "text/javascript; charset=utf-8",
        "json" | "map" => "application/json; charset=utf-8",
        "png" => "image/png",
        "svg" => "image/svg+xml",
        "txt" => "text/plain; charset=utf-8",
        "webp" => "image/webp",
        _ => "application/octet-stream",
    }
}

fn frontend_cache_control(path: &Path) -> &'static str {
    if path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension == "html")
    {
        "no-store"
    } else {
        "public, max-age=31536000, immutable"
    }
}

fn static_response(
    status: StatusCode,
    content_type: &'static str,
    cache_control: &'static str,
    body: Body,
    set_cookie: Option<String>,
) -> axum::response::Response {
    let mut builder = axum::response::Response::builder()
        .status(status)
        .header(header::CACHE_CONTROL, cache_control)
        .header(header::CONTENT_TYPE, content_type);

    if let Some(set_cookie) = set_cookie {
        builder = builder.header(header::SET_COOKIE, set_cookie);
    }

    builder.body(body).unwrap_or_else(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to build response",
        )
            .into_response()
    })
}

fn launch_required_response(
    auth: &LaunchAuthConfig,
    status: StatusCode,
    message: &str,
    clear_cookie: bool,
) -> axum::response::Response {
    static_response(
        status,
        "text/html; charset=utf-8",
        "no-store",
        Body::from(launch_required_page(message)),
        clear_cookie.then(|| clear_session_cookie_header(auth)),
    )
}

fn json_status_response(
    auth: &LaunchAuthConfig,
    status: StatusCode,
    payload: Value,
    clear_cookie: bool,
) -> axum::response::Response {
    let mut response = (status, Json(payload)).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));

    if clear_cookie {
        if let Ok(value) = HeaderValue::from_str(&clear_session_cookie_header(auth)) {
            response.headers_mut().insert(header::SET_COOKIE, value);
        }
    }

    response
}

fn launch_required_page(message: &str) -> String {
    format!(
        r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>2ndBrain launch required</title>
    <style>
      :root {{ color: #17202a; background: #eef5f4; font-family: Inter, ui-sans-serif, system-ui, sans-serif; }}
      body {{ display: grid; min-height: 100vh; place-items: center; margin: 0; padding: 24px; }}
      main {{ width: min(100%, 460px); border: 1px solid #d8e2df; border-radius: 10px; background: #fff; padding: 28px; box-shadow: 0 24px 60px rgba(15, 23, 42, 0.14); }}
      p {{ color: #5f6c76; line-height: 1.55; }}
      strong {{ color: #087c70; }}
    </style>
  </head>
  <body>
    <main>
      <strong>2ndBrain launch auth</strong>
      <h1>Launch required</h1>
      <p>{}</p>
    </main>
  </body>
</html>"#,
        escape_html(message)
    )
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
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
