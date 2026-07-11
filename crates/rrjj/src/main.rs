use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, ensure};
use axum::Router;
use axum::extract::State;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response as HttpResponse};
use axum::routing::get;
use clap::{Parser, Subcommand};
use rrjj_core::{Config, CoordinatorHandle};
use rrjj_reader::Session;
use rrjj_schema::{FormatMetadata, SCHEMA_VERSION, SESSION_FORMAT_VERSION};
use rrjj_sinks::{
    BroadcastSink, DirectorySessionSink, NdjsonSink, S3SessionSink, S3SinkConfig, Sink,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt as _, PermissionsExt as _};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(version, about = "Record and control jj-backed filesystem sessions")]
struct Args {
    #[command(subcommand)]
    command: CliCommand,
}

#[derive(Debug, Subcommand)]
#[expect(
    clippy::large_enum_variant,
    reason = "clap subcommands directly own their parsed arguments"
)]
enum CliCommand {
    Daemon {
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        shadow: PathBuf,
        #[arg(long)]
        events: PathBuf,
        #[arg(long)]
        session_dir: Option<PathBuf>,
        #[arg(long, default_value_t = 1_073_741_824)]
        max_spool_bytes: u64,
        #[arg(long, default_value = "/tmp/rrjj.sock")]
        socket: PathBuf,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long, default_value_t = 5_000)]
        max_changes: usize,
        #[arg(long, default_value_t = 1_500)]
        quiescence_ms: u64,
        #[arg(long, default_value_t = 10_000)]
        max_delay_ms: u64,
        #[arg(long = "ignore")]
        ignore: Vec<String>,
        #[arg(long)]
        s3_bucket: Option<String>,
        #[arg(long, default_value = "rrjj")]
        s3_prefix: String,
        #[arg(long, default_value = "us-east-1")]
        s3_region: String,
        #[arg(long)]
        s3_endpoint: Option<String>,
        #[arg(long)]
        http: Option<std::net::SocketAddr>,
        #[arg(long)]
        cors_origin: Option<String>,
    },
    Status {
        #[arg(long, default_value = "/tmp/rrjj.sock")]
        socket: PathBuf,
    },
    Snap {
        #[arg(long, default_value = "/tmp/rrjj.sock")]
        socket: PathBuf,
    },
    Mark {
        label: String,
        #[arg(long, default_value = "{}")]
        meta: String,
        #[arg(long, default_value = "/tmp/rrjj.sock")]
        socket: PathBuf,
    },
    Flush {
        #[arg(long, default_value = "/tmp/rrjj.sock")]
        socket: PathBuf,
    },
    Pause {
        #[arg(long, default_value = "/tmp/rrjj.sock")]
        socket: PathBuf,
    },
    Resume {
        #[arg(long, default_value = "/tmp/rrjj.sock")]
        socket: PathBuf,
    },
    Index {
        session: PathBuf,
    },
    Inspect {
        session: PathBuf,
        id: String,
    },
    Diff {
        session: PathBuf,
        before: String,
        after: String,
    },
    Materialize {
        session: PathBuf,
        op: String,
        destination: PathBuf,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum Request {
    Status,
    Snap,
    Mark {
        label: String,
        meta: Map<String, Value>,
    },
    Flush,
    Pause,
    Resume,
}

#[derive(Debug, Serialize, Deserialize)]
struct Response {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    match Args::parse().command {
        CliCommand::Daemon {
            root,
            shadow,
            events,
            session_dir,
            max_spool_bytes,
            socket,
            session_id,
            max_changes,
            quiescence_ms,
            max_delay_ms,
            ignore,
            s3_bucket,
            s3_prefix,
            s3_region,
            s3_endpoint,
            http,
            cors_origin,
        } => {
            run_daemon(
                root,
                shadow,
                events,
                session_dir,
                socket,
                session_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
                max_changes,
                max_spool_bytes,
                quiescence_ms,
                max_delay_ms,
                ignore,
                s3_bucket,
                s3_prefix,
                s3_region,
                s3_endpoint,
                http,
                cors_origin,
            )
            .await
        }
        CliCommand::Status { socket } => request(&socket, Request::Status).await,
        CliCommand::Snap { socket } => request(&socket, Request::Snap).await,
        CliCommand::Flush { socket } => request(&socket, Request::Flush).await,
        CliCommand::Pause { socket } => request(&socket, Request::Pause).await,
        CliCommand::Resume { socket } => request(&socket, Request::Resume).await,
        CliCommand::Mark {
            label,
            meta,
            socket,
        } => {
            let meta = serde_json::from_str::<Value>(&meta).context("--meta must be valid JSON")?;
            let Value::Object(meta) = meta else {
                return Err(anyhow!("--meta must be a JSON object"));
            };
            request(&socket, Request::Mark { label, meta }).await
        }
        CliCommand::Index { session } => print_json(&Session::open(session)?.index()),
        CliCommand::Inspect { session, id } => {
            let session = Session::open(session)?;
            if id.starts_with("t:") {
                print_json(&session.inspect_tree(&id)?)
            } else {
                print_json(&session.inspect_operation(&id)?)
            }
        }
        CliCommand::Diff {
            session,
            before,
            after,
        } => print_json(&Session::open(session)?.diff(&before, &after).await?),
        CliCommand::Materialize {
            session,
            op,
            destination,
        } => {
            Session::open(session)?
                .materialize(&op, destination)
                .await?;
            print_json(&json!({"materialized": op}))
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "daemon arguments map directly to the public CLI surface"
)]
async fn run_daemon(
    root: PathBuf,
    shadow: PathBuf,
    events: PathBuf,
    session_dir: Option<PathBuf>,
    socket: PathBuf,
    session_id: String,
    max_changes: usize,
    max_spool_bytes: u64,
    quiescence_ms: u64,
    max_delay_ms: u64,
    ignore: Vec<String>,
    s3_bucket: Option<String>,
    s3_prefix: String,
    s3_region: String,
    s3_endpoint: Option<String>,
    http: Option<std::net::SocketAddr>,
    cors_origin: Option<String>,
) -> Result<()> {
    ensure!(max_changes > 0, "--max-changes must be greater than zero");
    ensure!(
        quiescence_ms > 0 && max_delay_ms > 0,
        "watch delays must be greater than zero"
    );
    ensure!(
        session_dir.is_none() || s3_bucket.is_none(),
        "--session-dir and --s3-bucket are mutually exclusive"
    );
    let http_listener = match http {
        Some(address) => Some((
            address,
            tokio::net::TcpListener::bind(address)
                .await
                .with_context(|| format!("bind HTTP listener {address}"))?,
        )),
        None => None,
    };
    if let Some(parent) = shadow.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    if let Some(parent) = events.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    if let Some(parent) = socket.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    remove_stale_socket(&socket).await?;
    let format = FormatMetadata {
        session_format: SESSION_FORMAT_VERSION,
        schema_version: SCHEMA_VERSION,
        rrjj_version: env!("CARGO_PKG_VERSION").into(),
        jj_lib_version: "0.43.0".into(),
        jj_store_version: "jj-lib-0.43.0/git".into(),
    };
    let durable: Arc<dyn Sink> = match (session_dir.as_ref(), s3_bucket) {
        (Some(session_dir), None) => Arc::new(
            DirectorySessionSink::create(
                &events,
                session_dir,
                session_id.clone(),
                format.clone(),
                max_spool_bytes,
            )
            .await?,
        ),
        (None, Some(bucket)) => Arc::new(
            S3SessionSink::create(S3SinkConfig {
                bucket,
                prefix: s3_prefix,
                region: s3_region,
                endpoint: s3_endpoint,
                spool_path: events.clone(),
                max_spool_bytes,
                session_id: session_id.clone(),
                format,
            })
            .await?,
        ),
        (None, None) => Arc::new(NdjsonSink::create(&events).await?),
        (Some(_), Some(_)) => unreachable!("validated above"),
    };
    let (broadcast, live_events) = BroadcastSink::new(durable, 1_024);
    let sink: Arc<dyn Sink> = Arc::new(broadcast);
    let mut excluded_paths = vec![shadow.clone(), events.clone(), socket.clone()];
    if let Some(session_dir) = session_dir {
        excluded_paths.push(session_dir);
    }
    let mut ignore = vec![".git".into(), ".jj".into()]
        .into_iter()
        .chain(ignore)
        .collect::<Vec<_>>();
    ignore.sort();
    ignore.dedup();
    let coordinator = rrjj_core::start(
        Config {
            session_id,
            watched_root: root,
            shadow_root: shadow,
            ignore,
            excluded_paths,
            max_changes_per_event: max_changes,
            quiescence: Duration::from_millis(quiescence_ms),
            max_delay: Duration::from_millis(max_delay_ms),
        },
        sink,
    )
    .await?;
    let listener = UnixListener::bind(&socket)
        .with_context(|| format!("bind control socket {}", socket.display()))?;
    tokio::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))
        .await
        .with_context(|| format!("secure control socket {}", socket.display()))?;
    let http_task = if let Some((address, listener)) = http_listener {
        let app = http_router(
            HttpState {
                coordinator: coordinator.clone(),
                events: live_events,
            },
            address,
            cors_origin,
        )?;
        Some(tokio::spawn(
            async move { axum::serve(listener, app).await },
        ))
    } else {
        None
    };
    let result = serve(listener, coordinator.clone()).await;
    if let Some(task) = http_task {
        task.abort();
    }
    let shutdown = coordinator.shutdown("terminated".into()).await;
    let _ = tokio::fs::remove_file(&socket).await;
    result.and(shutdown)
}

#[derive(Clone)]
struct HttpState {
    coordinator: CoordinatorHandle,
    events: tokio::sync::broadcast::Sender<rrjj_schema::Event>,
}

fn http_router(
    state: HttpState,
    address: std::net::SocketAddr,
    cors_origin: Option<String>,
) -> Result<Router> {
    let router = Router::new()
        .route("/events", get(sse_events))
        .route("/health", get(http_status))
        .route("/manifest/status", get(http_status))
        .with_state(state)
        .merge(static_scrubber_router());
    let cors = match cors_origin {
        Some(origin) => CorsLayer::new().allow_origin(AllowOrigin::exact(origin.parse()?)),
        None if address.ip().is_loopback() => CorsLayer::new().allow_origin(Any),
        None => return Ok(router),
    };
    Ok(router.layer(cors))
}

const SCRUBBER_INDEX: &str = include_str!("../../../ui/scrubber/index.html");
const SCRUBBER_COMPONENT: &str = include_str!("../../../ui/scrubber/rrjj-live.mjs");
const SCRUBBER_MODEL: &str = include_str!("../../../ui/scrubber/timeline-model.mjs");

fn static_scrubber_router() -> Router {
    Router::new()
        .route("/", get(scrubber_index))
        .route("/rrjj-live.mjs", get(scrubber_component))
        .route("/timeline-model.mjs", get(scrubber_model))
}

async fn scrubber_index() -> Html<&'static str> {
    Html(SCRUBBER_INDEX)
}

async fn scrubber_component() -> HttpResponse {
    javascript(SCRUBBER_COMPONENT)
}

async fn scrubber_model() -> HttpResponse {
    javascript(SCRUBBER_MODEL)
}

fn javascript(source: &'static str) -> HttpResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/javascript; charset=utf-8",
        )],
        source,
    )
        .into_response()
}

async fn http_status(
    State(state): State<HttpState>,
) -> Result<axum::Json<rrjj_core::Status>, (axum::http::StatusCode, String)> {
    state
        .coordinator
        .status()
        .await
        .map(axum::Json)
        .map_err(|error| {
            (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                error.to_string(),
            )
        })
}

async fn sse_events(
    State(state): State<HttpState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<SseEvent, std::convert::Infallible>>> {
    let stream = BroadcastStream::new(state.events.subscribe()).map(sse_message);
    Sse::new(stream).keep_alive(KeepAlive::default())
}

fn sse_message(
    message: Result<rrjj_schema::Event, BroadcastStreamRecvError>,
) -> Result<SseEvent, std::convert::Infallible> {
    let event = match message {
        Ok(event) => SseEvent::default()
            .event("event")
            .id(event.seq.to_string())
            .data(serde_json::to_string(&event).expect("schema event serializes")),
        Err(error) => SseEvent::default().event("overflow").data(
            json!({
                "type": "overflow",
                "source": "sse_broadcast",
                "message": error.to_string(),
                "recovery": "reconnect_and_resync"
            })
            .to_string(),
        ),
    };
    Ok(event)
}

async fn serve(listener: UnixListener, coordinator: CoordinatorHandle) -> Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("install SIGTERM handler")?;
    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _) = result?;
                let handle = coordinator.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(stream, handle).await {
                        eprintln!("rrjj control request failed: {error:#}");
                    }
                });
            }
            result = tokio::signal::ctrl_c() => {
                result?;
                return Ok(());
            }
            signal = terminate.recv() => {
                ensure!(signal.is_some(), "SIGTERM handler closed unexpectedly");
                return Ok(());
            }
        }
    }
}

async fn handle_connection(stream: UnixStream, coordinator: CoordinatorHandle) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut line = String::new();
    BufReader::new(reader).read_line(&mut line).await?;
    let request: Request = serde_json::from_str(&line)?;
    let response = match dispatch(request, coordinator).await {
        Ok(data) => Response {
            ok: true,
            data: Some(data),
            error: None,
        },
        Err(error) => Response {
            ok: false,
            data: None,
            error: Some(format!("{error:#}")),
        },
    };
    writer
        .write_all(format!("{}\n", serde_json::to_string(&response)?).as_bytes())
        .await?;
    Ok(())
}

async fn dispatch(request: Request, coordinator: CoordinatorHandle) -> Result<Value> {
    match request {
        Request::Status => Ok(serde_json::to_value(coordinator.status().await?)?),
        Request::Snap => Ok(serde_json::to_value(coordinator.snap().await?)?),
        Request::Mark { label, meta } => {
            coordinator.mark(label, meta).await?;
            Ok(json!({}))
        }
        Request::Flush => Ok(serde_json::to_value(coordinator.flush().await?)?),
        Request::Pause => Ok(serde_json::to_value(coordinator.pause().await?)?),
        Request::Resume => Ok(serde_json::to_value(coordinator.resume().await?)?),
    }
}

fn print_json(value: &impl Serialize) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

async fn request(socket: &Path, request: Request) -> Result<()> {
    let mut stream = UnixStream::connect(socket)
        .await
        .with_context(|| format!("connect to {}", socket.display()))?;
    stream
        .write_all(format!("{}\n", serde_json::to_string(&request)?).as_bytes())
        .await?;
    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response).await?;
    let response: Response = serde_json::from_str(&response)?;
    ensure!(
        response.ok,
        "{}",
        response.error.unwrap_or_else(|| "request failed".into())
    );
    println!(
        "{}",
        serde_json::to_string_pretty(&response.data.unwrap_or(Value::Null))?
    );
    Ok(())
}

async fn remove_stale_socket(path: &Path) -> Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_socket(),
                "{} exists and is not a socket",
                path.display()
            );
            match UnixStream::connect(path).await {
                Ok(_) => return Err(anyhow!("control socket {} is active", path.display())),
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                    ) =>
                {
                    if let Err(error) = tokio::fs::remove_file(path).await
                        && error.kind() != std::io::ErrorKind::NotFound
                    {
                        return Err(error.into());
                    }
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("probe control socket {}", path.display()));
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::{Request, StatusCode, header};
    use tower::ServiceExt as _;

    #[test]
    fn sse_lag_is_an_explicit_overflow_event() {
        let event = sse_message(Err(BroadcastStreamRecvError::Lagged(7))).unwrap();
        let debug = format!("{event:?}");
        assert!(debug.contains("overflow"));
        assert!(debug.contains("reconnect_and_resync"));
    }

    #[tokio::test]
    async fn serves_embedded_scrubber_assets_with_browser_content_types() {
        let cases = [
            ("/", "text/html; charset=utf-8", "<rrjj-live>"),
            (
                "/rrjj-live.mjs",
                "text/javascript; charset=utf-8",
                "customElements.define",
            ),
            (
                "/timeline-model.mjs",
                "text/javascript; charset=utf-8",
                "createLiveTimeline",
            ),
        ];

        for (uri, expected_content_type, expected_text) in cases {
            let response = static_scrubber_router()
                .oneshot(Request::get(uri).body(axum::body::Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            assert_eq!(
                response.headers().get(header::CONTENT_TYPE).unwrap(),
                expected_content_type,
                "{uri}"
            );
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            assert!(
                String::from_utf8(body.to_vec())
                    .unwrap()
                    .contains(expected_text),
                "{uri}"
            );
        }
    }

    #[tokio::test]
    async fn unknown_scrubber_asset_is_not_found() {
        let response = static_scrubber_router()
            .oneshot(
                Request::get("/missing.mjs")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn refuses_to_remove_an_active_control_socket() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("active.sock");
        let _listener = UnixListener::bind(&path).unwrap();

        let error = remove_stale_socket(&path).await.unwrap_err();

        assert!(error.to_string().contains("is active"));
        assert!(path.exists());
    }

    #[tokio::test]
    async fn removes_a_stale_control_socket() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("stale.sock");
        drop(UnixListener::bind(&path).unwrap());

        remove_stale_socket(&path).await.unwrap();

        assert!(!path.exists());
    }
}
