// Adapted from oxigraph_server main.rs, MIT OR Apache-2.0 license

#![allow(clippy::print_stderr, clippy::cast_precision_loss, clippy::use_debug)]
use clap::Parser;
use oxhttp::model::{HeaderName, Response, Status};
use oxhttp::Server;
use oxigraph::store::Store;
use server::{cors, sparql};
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

const HTTP_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Parser)]
#[command(about, version)]
/// kos-kit server
struct Args {
    /// Host and port to listen to.
    #[arg(short, long, default_value = "localhost:7878")]
    bind: String,

    /// Allows cross-origin requests
    #[arg(long)]
    cors: bool,

    /// Directory in which the data should be persisted.
    ///
    /// If not present. An in-memory storage will be used.
    #[arg(short, long)]
    location: Option<PathBuf>,

    /// Start Oxigraph HTTP server in read-only mode.
    #[arg(long)]
    read_only: bool,
}

fn error(status: Status, message: impl fmt::Display) -> Response {
    Response::builder(status)
        .with_header(HeaderName::CONTENT_TYPE, "text/plain; charset=utf-8")
        .unwrap()
        .with_body(message.to_string())
}

pub fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let store = if let Some(location) = args.location {
        Store::open(location)
    } else {
        Store::new()
    }?;

    let mut server = if args.cors {
        Server::new(cors::middleware(move |request| {
            sparql::handle_request(request, store.clone(), args.read_only)
                .unwrap_or_else(|(status, message)| error(status, message))
        }))
    } else {
        Server::new(move |request| {
            sparql::handle_request(request, store.clone(), args.read_only)
                .unwrap_or_else(|(status, message)| error(status, message))
        })
    };
    server.set_global_timeout(HTTP_TIMEOUT);
    server.set_server_name(concat!("kos-kit/server", env!("CARGO_PKG_VERSION")))?;
    // eprintln!("Listening for requests at http://{}", &args.bind);
    server.listen(args.bind)?;
    Ok(())
}
