use std::{path::PathBuf, time::Duration};

use baza::{AbortExpiredUploads, Exec as _, Storage};
use baza_api_s3 as s3;
use secrecy::SecretString;
use tracing::Level;

#[tokio::main]
async fn main() -> Result<(), String> {
    let args = <CliOpts as clap::Parser>::parse();

    tracing_subscriber::fmt().with_max_level(args.log_level).init();

    let storage = Storage::new(args.root).await.map_err(|e| {
        format!("Failed to initialize `Storage`: {e}: {}", e.trace())
    })?;

    drop(tokio::spawn(abort_expired_uploads(
        storage.clone(),
        args.multipart_ttl,
    )));

    s3::run_http_server(
        storage,
        ("0.0.0.0", args.port),
        args.access_key,
        args.secret_key,
    )
    .await
    .map_err(|e| format!("Failed to run S3 HTTP server: {e}"))
}

/// Maximum interval between checks for expired multipart uploads.
const MAX_EXPIRED_UPLOADS_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Minimum interval between checks for expired multipart uploads.
const MIN_EXPIRED_UPLOADS_INTERVAL: Duration = Duration::from_secs(60);

/// Periodically aborts multipart uploads of the provided [`Storage`] created
/// more than the provided `ttl` ago, starting right away.
async fn abort_expired_uploads(storage: Storage, ttl: Duration) {
    let mut interval = tokio::time::interval(
        ttl.clamp(MIN_EXPIRED_UPLOADS_INTERVAL, MAX_EXPIRED_UPLOADS_INTERVAL),
    );
    loop {
        _ = interval.tick().await;
        match storage.exec(AbortExpiredUploads { ttl }).await {
            Ok(0) => {}
            Ok(n) => tracing::info!("Aborted {n} expired multipart uploads"),
            Err(e) => tracing::error!(
                "Failed to abort expired multipart uploads: {e}: {}",
                e.trace(),
            ),
        }
    }
}

/// CLI options.
#[derive(Debug, clap::Parser)]
#[command(about)]
struct CliOpts {
    /// Directory where all buckets will be stored.
    #[arg(short, long, default_value = "/var/lib/baza")]
    root: PathBuf,

    /// Logging verbosity level.
    ///
    /// Available values: `error`, `warn`, `info`, `debug`, `trace`.
    #[arg(short, long, default_value = "info")]
    log_level: Level,

    /// Time after which incomplete multipart uploads are aborted.
    ///
    /// Counted from the upload creation. Examples: `24h`, `30m`, `7days`.
    #[arg(
        long,
        env = "BAZA_MULTIPART_TTL",
        default_value = "24h",
        value_parser = humantime::parse_duration,
    )]
    multipart_ttl: Duration,

    /// Port to run S3 HTTP API on.
    #[arg(short, long, default_value_t = 9294)]
    port: u16,

    /// S3 API access key.
    #[arg(long, env = "BAZA_ACCESS_KEY", default_value = "baza")]
    access_key: SecretString,

    /// S3 API secret key.
    #[arg(long, env = "BAZA_SECRET_KEY", default_value = "baza")]
    secret_key: SecretString,
}
