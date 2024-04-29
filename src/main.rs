// Adapted from oxigraph_server main.rs, MIT OR Apache-2.0 license

#![allow(clippy::print_stderr, clippy::cast_precision_loss, clippy::use_debug)]
use clap::Parser;
use kos_kit_server::init::{init_oxigraph_store, init_tantivy_index};
use kos_kit_server::{cors, search, sparql};
use oxhttp::model::{HeaderName, Request, Response, Status};
use oxhttp::Server;
use oxigraph::store::Store;
use std::path::PathBuf;
use std::time::Duration;
use std::{fmt, fs};
use tantivy::directory::MmapDirectory;
use tantivy::Index;

type HttpError = (Status, String);

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

    /// Directory in which the Tantivy index should be persisted.
    /// If not present, use a temporary directory
    #[arg(long)]
    tantivy_index_data_directory_path: Option<PathBuf>,
}

fn error(status: Status, message: impl fmt::Display) -> Response {
    Response::builder(status)
        .with_header(HeaderName::CONTENT_TYPE, "text/plain; charset=utf-8")
        .unwrap()
        .with_body(message.to_string())
}

pub fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let oxigraph_store =
        if let Some(oxigraph_data_directory_path) = args.oxigraph_data_directory_path {
            Store::open(oxigraph_data_directory_path)
        } else {
            Store::new()
        }?;

    use tantivy::schema::{Schema, STORED, STRING, TEXT};

    let mut tantivy_index_schema_builder = Schema::builder();
    tantivy_index_schema_builder.add_text_field("iri", STRING | STORED);
    tantivy_index_schema_builder.add_text_field("text", TEXT);
    let tantivy_index_schema = tantivy_index_schema_builder.build();

    let tantivy_index =
        if let Some(index_data_directory_path) = args.tantivy_index_data_directory_path {
            Index::open_or_create(
                MmapDirectory::open(index_data_directory_path)?,
                tantivy_index_schema,
            )?
        } else {
            Index::create_in_ram(tantivy_index_schema)
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

    if oxigraph_store.is_empty()? {
        init_oxigraph_store(args.oxigraph_init_path, &oxigraph_store)?
    } else {
        eprintln!("Oxigraph/Tantivy is not empty, skipping init")
    }

    if tantivy_index.reader()?.searcher().num_docs() == 0 {
        init_tantivy_index(&tantivy_index, index_init_sparql, &oxigraph_store)?
    }

    let mut server = if args.cors {
        Server::new(cors::middleware(move |request| {
            handle_request(
                index_result_sparql.clone(),
                request,
                oxigraph_store.clone(),
                tantivy_index.clone(),
            )
            .unwrap_or_else(|(status, message)| error(status, message))
        }))
    } else {
        Server::new(move |request| {
            handle_request(
                index_result_sparql.clone(),
                request,
                oxigraph_store.clone(),
                tantivy_index.clone(),
            )
            .unwrap_or_else(|(status, message)| error(status, message))
        })
    };
    server.set_global_timeout(HTTP_TIMEOUT);
    server.set_server_name(concat!("kos-kit/server", env!("CARGO_PKG_VERSION")))?;
    eprintln!("Listening for requests at http://{}", &args.bind);
    server.listen(args.bind)?;
    Ok(())
}

pub fn handle_request(
    index_result_sparql: String,
    request: &mut Request,
    oxigraph_store: Store,
    tantivy_index: Index,
) -> Result<Response, HttpError> {
    match request.url().path() {
        "/search" => search::handle_request(index_result_sparql, request, tantivy_index),
        "/sparql" => sparql::handle_request(request, oxigraph_store),
        _ => Err((
            Status::NOT_FOUND,
            format!(
                "{} {} is not supported by this server",
                request.method(),
                request.url().path()
            ),
        )),
    }
}
