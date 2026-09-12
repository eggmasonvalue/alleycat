//! `alleycat-agy-bridge` binary entry point.

use std::path::PathBuf;

use anyhow::Result;

use alleycat_agy_bridge::AgyBridge;
use alleycat_bridge_core::serve_stdio;
#[cfg(unix)]
use alleycat_bridge_core::{ServerOptions, serve_unix};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        "alleycat-agy-bridge starting"
    );

    let bridge = AgyBridge::builder().from_env().build().await?;

    match socket_arg() {
        Some(path) => {
            #[cfg(unix)]
            {
                serve_unix(
                    bridge,
                    ServerOptions {
                        socket_path: path,
                        unlink_stale: true,
                    },
                )
                .await
            }
            #[cfg(not(unix))]
            {
                let _ = bridge;
                anyhow::bail!(
                    "Unix socket transport is not supported on Windows: {}",
                    path.display()
                );
            }
        }
        None => serve_stdio(bridge).await,
    }
}

fn socket_arg() -> Option<PathBuf> {
    let mut args = std::env::args_os().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--socket" || arg == "--listen" {
            return args.next().map(PathBuf::from);
        }
    }
    std::env::var_os("ALLEYCAT_BRIDGE_SOCKET").map(PathBuf::from)
}
