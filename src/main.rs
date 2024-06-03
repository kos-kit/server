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
use tantivy::query::QueryParser;
use tantivy::{Index, IndexReader, ReloadPolicy};

type HttpError = (Status, String);

const INDEX_INIT_SPARQL: &str = include_str!("./index_init.sparql");
const INDEX_RESULT_SPARQL: &str = include_str!("./index_result.sparql");
const HTTP_TIMEOUT: Duration = Duration::from_secs(60);
const YASGUI_HTML: &str = include_str!("./yasgui.html");

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

    /// Path to an RDF file or a directory of RDF files to load into Oxigraph
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
            fs::create_dir_all(oxigraph_data_directory_path.clone())?;
            Store::open(oxigraph_data_directory_path)
        } else {
            Store::new()
        }?;

    use tantivy::schema::{Schema, STORED, STRING, TEXT};

    let mut tantivy_index_schema_builder = Schema::builder();
    tantivy_index_schema_builder.add_text_field("iri", STRING | STORED);
    let tantivy_index_text_field = tantivy_index_schema_builder.add_text_field("text", TEXT);
    let tantivy_index_schema = tantivy_index_schema_builder.build();

    let tantivy_index =
        if let Some(index_data_directory_path) = args.tantivy_index_data_directory_path {
            fs::create_dir_all(index_data_directory_path.clone())?;
            Index::open_or_create(
                MmapDirectory::open(index_data_directory_path)?,
                tantivy_index_schema,
            )?
        } else {
            Index::create_in_ram(tantivy_index_schema)
        };

    let index_init_sparql =
        if let Some(index_init_sparql_file_path) = args.index_init_sparql_file_path {
            match fs::read_to_string(index_init_sparql_file_path.clone()) {
                Ok(s) => s,
                Err(e) => panic!(
                    "unable to read index init SPARQL file {}: {}",
                    index_init_sparql_file_path.display(),
                    e
                ),
            }
        } else {
            String::from(INDEX_INIT_SPARQL)
        };

    let index_result_sparql =
        if let Some(index_result_sparql_file_path) = args.index_result_sparql_file_path {
            match fs::read_to_string(index_result_sparql_file_path.clone()) {
                Ok(s) => s,
                Err(e) => panic!(
                    "unable to read index result SPARQL file {}: {}",
                    index_result_sparql_file_path.display(),
                    e
                ),
            }
        } else {
            String::from(INDEX_RESULT_SPARQL)
        };

    if oxigraph_store.is_empty()? {
        init_oxigraph_store(args.oxigraph_init_path, &oxigraph_store)?
    } else {
        eprintln!("Oxigraph store is not empty, skipping init")
    }

    let tantivy_index_reader = tantivy_index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommitWithDelay)
        .try_into()?;

    {
        if tantivy_index_reader.searcher().num_docs() == 0 {
            init_tantivy_index(&tantivy_index, index_init_sparql, &oxigraph_store)?;
            // tantivy_index_reader.reload()?;
            assert!(tantivy_index_reader.searcher().num_docs() == 0);
        } else {
            eprintln!("Tantivy index is not empty, skipping init")
        }
    }

    let tantivy_query_parser =
        QueryParser::for_index(&tantivy_index, vec![tantivy_index_text_field]);

    let mut server = if args.cors {
        Server::new(cors::middleware(move |request| {
            handle_request(
                index_result_sparql.clone(),
                request,
                oxigraph_store.clone(),
                &tantivy_index_reader,
                &tantivy_query_parser,
            )
            .unwrap_or_else(|(status, message)| error(status, message))
        }))
    } else {
        Server::new(move |request| {
            handle_request(
                index_result_sparql.clone(),
                request,
                oxigraph_store.clone(),
                &tantivy_index_reader,
                &tantivy_query_parser,
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
    tantivy_index_reader: &IndexReader,
    tantivy_query_parser: &QueryParser,
) -> Result<Response, HttpError> {
    match request.url().path() {
        "/" => {
            if request.method().as_ref() != "GET" {
                return Err((
                    Status::METHOD_NOT_ALLOWED,
                    format!("{} is not supported by this server", request.method()),
                ));
            }

            return Ok(Response::builder(Status::OK)
                .with_header("Content-Type", String::from("text/html"))
                .unwrap()
                .with_body(YASGUI_HTML));
        }
        "/search" => search::handle_request(
            index_result_sparql,
            oxigraph_store,
            request,
            tantivy_index_reader,
            tantivy_query_parser,
        ),
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
