// Adapted from oxigraph_server main.rs, MIT OR Apache-2.0 license

#![allow(clippy::print_stderr, clippy::cast_precision_loss, clippy::use_debug)]
use clap::Parser;
use kos_kit_server::index::{self};
use kos_kit_server::{cors, init, sparql};
use oxhttp::model::{HeaderName, Response, Status};
use oxhttp::Server;
use oxigraph::store::Store;
use std::path::PathBuf;
use std::time::Duration;
use std::{fmt, fs};
use tantivy::directory::MmapDirectory;
use tantivy::Index;

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

    /// Directory in which the Tantivy index should be persisted.
    /// If not present, use a temporary directory
    #[arg(long)]
    index_data_directory_path: Option<PathBuf>,

    // Path to a .sparql file containing a query to initialize the index
    #[arg(long)]
    index_init_sparql_file_path: Option<PathBuf>,

    // Path to a .sparql file containing a query for each result
    #[arg(long)]
    index_result_sparql_file_path: Option<PathBuf>,

    /// Directory in which Oxigraph data should be persisted.
    ///
    /// If not present, store data in memory.
    #[arg(long)]
    oxigraph_data_directory_path: Option<PathBuf>,

    /// Path to an RDF files or a directory of RDF files to load into Oxigraph
    #[arg(long, required = true)]
    oxigraph_init_path: PathBuf,
}

fn error(status: Status, message: impl fmt::Display) -> Response {
    Response::builder(status)
        .with_header(HeaderName::CONTENT_TYPE, "text/plain; charset=utf-8")
        .unwrap()
        .with_body(message.to_string())
}

pub fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let index_ = if let Some(index_data_directory_path) = args.index_data_directory_path {
        Index::open_or_create(
            MmapDirectory::open(index_data_directory_path)?,
            index::schema()?,
        )?
    } else {
        Index::create_in_ram(index::schema()?)
    };

    let index_init_sparql =
        if let Some(index_init_sparql_file_path) = args.index_init_sparql_file_path {
            fs::read_to_string(index_init_sparql_file_path)?
        } else {
            String::from(
                "\
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT DISTINCT ?iri ?text
WHERE { 
    { ?iri rdfs:label ?text }
    UNION
    { ?iri skos:prefLabel ?text }
    UNION
    { ?iri skos:altLabel ?text }
}",
            )
        };

    let index_result_sparql =
        if let Some(index_result_sparql_file_path) = args.index_result_sparql_file_path {
            fs::read_to_string(index_result_sparql_file_path)?
        } else {
            String::from(
                "\
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

CONSTRUCT WHERE {
    { ?iri rdfs:label ?text }
    UNION
    { ?iri skos:prefLabel ?text }
    UNION
    { ?iri skos:altLabel ?text }
}",
            )
        };

    let store = if let Some(oxigraph_data_directory_path) = args.oxigraph_data_directory_path {
        Store::open(oxigraph_data_directory_path)
    } else {
        Store::new()
    }?;

    if store.is_empty()? {
        init::init(&index_, index_init_sparql, args.oxigraph_init_path, &store)?
    } else {
        eprintln!("Oxigraph/Tantivy is not empty, skipping init")
    }

    let mut server = if args.cors {
        Server::new(cors::middleware(move |request| {
            sparql::handle_request(request, store.clone())
                .unwrap_or_else(|(status, message)| error(status, message))
        }))
    } else {
        Server::new(move |request| {
            sparql::handle_request(request, store.clone())
                .unwrap_or_else(|(status, message)| error(status, message))
        })
    };
    server.set_global_timeout(HTTP_TIMEOUT);
    server.set_server_name(concat!("kos-kit/server", env!("CARGO_PKG_VERSION")))?;
    eprintln!("Listening for requests at http://{}", &args.bind);
    server.listen(args.bind)?;
    Ok(())
}
