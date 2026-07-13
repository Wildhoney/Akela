use std::path::{Path, PathBuf};

use akela::Hub;
use serde::Deserialize;

/// Contents of `akela.yaml`. Every field is optional &mdash; missing values
/// fall back to environment variables and then to the built-in defaults,
/// so an empty (or absent) file is a valid configuration.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct Config {
    /// Port the HTTP interface listens on. Default `8080`.
    port: Option<u16>,
    /// Redis connection URL. Default `redis://127.0.0.1:6379`.
    redis: Option<String>,
    /// Redis pub/sub channel shared by every instance of this hub.
    /// Default `akela:events`.
    channel: Option<String>,
}

/// Resolves the configuration file nginx-style: an explicit `AKELA_CONFIG`
/// path wins, then `./akela.yaml`, then `/etc/akela/akela.yaml`. Returns
/// `None` when no file exists &mdash; defaults and environment variables
/// carry the configuration alone.
fn locate() -> Option<PathBuf> {
    if let Ok(explicit) = std::env::var("AKELA_CONFIG") {
        return Some(PathBuf::from(explicit));
    }
    let conventional = [Path::new("akela.yaml"), Path::new("/etc/akela/akela.yaml")];
    conventional
        .iter()
        .find(|path| path.exists())
        .map(|path| path.to_path_buf())
}

fn load() -> Result<Config, Box<dyn std::error::Error>> {
    match locate() {
        Some(path) => {
            let contents = std::fs::read_to_string(&path)?;
            let config = serde_yaml::from_str(&contents)?;
            tracing::info!(path = %path.display(), "loaded configuration");
            Ok(config)
        }
        None => {
            tracing::info!("no configuration file found; using defaults");
            Ok(Config::default())
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = load()?;
    let redis = std::env::var("REDIS_URL")
        .ok()
        .or(config.redis)
        .unwrap_or_else(|| String::from("redis://127.0.0.1:6379"));
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|port| port.parse().ok())
        .or(config.port)
        .unwrap_or(8080);
    let channel = std::env::var("CHANNEL")
        .ok()
        .or(config.channel)
        .unwrap_or_else(|| String::from("akela:events"));

    let hub = Hub::connect_on(redis, channel).await?;
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(%port, "akela listening");
    axum::serve(listener, hub.router()).await?;
    Ok(())
}
