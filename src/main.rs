use std::{
    fs::{File, read_to_string},
    io::Write,
    path::PathBuf,
};

use anyhow::Context;
use async_signal::{Signal, Signals};
use futures_util::StreamExt;
use serde::Deserialize;
use tracing::{info, level_filters::LevelFilter, warn};
use tracing_subscriber::{EnvFilter, fmt::format::FmtSpan};
use zbus::{Connection, conn::Builder};

mod client;
mod distributor;
mod interfaces;

use crate::{
    client::{Client, ClientState},
    distributor::Distributor,
};

fn get_state_dir() -> PathBuf {
    let Ok(p) = std::env::var("XDG_STATE_HOME") else {
        return std::env::home_dir()
            .expect("Failed to find state or home directory!")
            .join(".local/state");
    };
    p.into()
}

fn get_config_dir() -> PathBuf {
    let Ok(p) = std::env::var("XDG_CONFIG_HOME") else {
        return std::env::home_dir()
            .expect("Failed to find state or home directory!")
            .join(".config");
    };
    p.into()
}

// TODO: implement enforcement of TLS and the escape hatch
#[derive(Deserialize)]
struct Config {
    url: String,
    #[serde(default)]
    allow_insecure: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::fmt()
        .with_ansi_sanitization(true)
        .with_level(true)
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_span_events(FmtSpan::NONE)
        .with_line_number(true)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
    let state_file = get_state_dir().join("opush.json");
    let state: Option<ClientState> = read_to_string(&state_file).map_or(None, |s| {
        serde_json::from_str(&s).unwrap_or_else(|_| {
            warn!("state file invalid! starting without state");
            None
        })
    });
    let config_file = get_config_dir().join("opush.json");
    let config: Config =
        serde_json::from_str(&read_to_string(config_file).context("failed to read config file!")?)
            .context("failed to parse config")?;

    //setup sigfd for clean shutdowns
    let mut signals = Signals::new([Signal::Int, Signal::Quit, Signal::Term])
        .context("failed to create signalfd!")?;

    let con = Connection::session().await?;
    let (sender, mut receive) = tokio::sync::mpsc::channel::<client::DistMethods>(32);
    let distr = Distributor::new(sender);
    let _service = Builder::session()?
        .name("org.unifiedpush.Distributor.opush")?
        .serve_at("/org/unifiedpush/Distributor", distr)?
        .build()
        .await?;

    let mut client = Client::new(config.url, state, con);

    loop {
        tokio::select! {
            biased;
            _ = signals.next() =>{
                info!("exit signal received. shutting down...");
                break;
            }
            r=client.poll(&mut receive)=>{
                //exit on critical error
                if r.is_err(){
                    break;
                }
            }
        }
    }
    let (_, state) = client.close().await;
    let Some(state) = state else { return Ok(()) };
    let mut sf = File::create(state_file).context("Failed to create state file")?;
    sf.write_all(&serde_json::to_string(&state)?.into_bytes())?;

    Ok(())
}
