// Adapted from oxigraph_server main.rs, MIT OR Apache-2.0 license

#![allow(clippy::print_stderr, clippy::cast_precision_loss, clippy::use_debug)]
use anyhow::{bail, Error};
use clap::Parser;
use oxhttp::model::{Body, HeaderName, HeaderValue, Method, Request, Response, Status};
use oxhttp::Server;
use oxigraph::io::{DatasetFormat, DatasetSerializer, GraphFormat, GraphSerializer};
use oxigraph::model::{GraphName, GraphNameRef, IriParseError, NamedNode, NamedOrBlankNode};
use oxigraph::sparql::{Query, QueryResults, Update};
use oxigraph::store::{BulkLoader, LoaderError, Store};
use oxiri::Iri;
use rand::random;
use sparesults::{QueryResultsFormat, QueryResultsSerializer};
use std::borrow::Cow;
use std::cell::RefCell;
use std::cmp::min;
use std::fmt;
use std::io::{self, BufReader, Read, Write};
use std::path::PathBuf;
use std::rc::Rc;
use std::str::FromStr;
use std::time::{Duration, Instant};
use url::form_urlencoded;

const MAX_SPARQL_BODY_SIZE: u64 = 0x0010_0000;
const HTTP_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Parser)]
#[command(about, version)]
/// Oxigraph SPARQL server.
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

pub fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let store = if let Some(location) = args.location {
        Store::open(location)
    } else {
        Store::new()
    }?;

    let mut server = if args.cors {
        Server::new(cors_middleware(move |request| {
            handle_request(request, store.clone(), args.read_only)
                .unwrap_or_else(|(status, message)| error(status, message))
        }))
    } else {
        Server::new(move |request| {
            handle_request(request, store.clone(), args.read_only)
                .unwrap_or_else(|(status, message)| error(status, message))
        })
    };
    server.set_global_timeout(HTTP_TIMEOUT);
    server.set_server_name(concat!("kos-kit/server", env!("CARGO_PKG_VERSION")))?;
    // eprintln!("Listening for requests at http://{}", &args.bind);
    server.listen(args.bind)?;
    Ok(())
}

#[derive(Copy, Clone)]
enum GraphOrDatasetFormat {
    Graph(GraphFormat),
    Dataset(DatasetFormat),
}

impl GraphOrDatasetFormat {
    fn from_extension(name: &str) -> anyhow::Result<Self> {
        Ok(match (GraphFormat::from_extension(name), DatasetFormat::from_extension(name)) {
            (Some(g), Some(d)) => bail!("The file extension '{name}' can be resolved to both '{}' and '{}', not sure what to pick", g.file_extension(), d.file_extension()),
            (Some(g), None) => Self::Graph(g),
            (None, Some(d)) => Self::Dataset(d),
            (None, None) =>
            bail!("The file extension '{name}' is unknown")
        })
    }

    fn from_media_type(name: &str) -> anyhow::Result<Self> {
        Ok(
            match (
                GraphFormat::from_media_type(name),
                DatasetFormat::from_media_type(name),
            ) {
                (Some(g), Some(d)) => bail!(
                "The media type '{name}' can be resolved to both '{}' and '{}', not sure what to pick",
                g.file_extension(),
                d.file_extension()
            ),
                (Some(g), None) => Self::Graph(g),
                (None, Some(d)) => Self::Dataset(d),
                (None, None) => bail!("The media type '{name}' is unknown"),
            },
        )
    }
}

impl FromStr for GraphOrDatasetFormat {
    type Err = Error;

    fn from_str(name: &str) -> anyhow::Result<Self> {
        if let Ok(t) = Self::from_extension(name) {
            return Ok(t);
        }
        if let Ok(t) = Self::from_media_type(name) {
            return Ok(t);
        }
        bail!("The file format '{name}' is unknown")
    }
}

fn cors_middleware(
    on_request: impl Fn(&mut Request) -> Response + Send + Sync + 'static,
) -> impl Fn(&mut Request) -> Response + Send + Sync + 'static {
    let origin = HeaderName::from_str("Origin").unwrap();
    let access_control_allow_origin = HeaderName::from_str("Access-Control-Allow-Origin").unwrap();
    let access_control_request_method =
        HeaderName::from_str("Access-Control-Request-Method").unwrap();
    let access_control_allow_method = HeaderName::from_str("Access-Control-Allow-Methods").unwrap();
    let access_control_request_headers =
        HeaderName::from_str("Access-Control-Request-Headers").unwrap();
    let access_control_allow_headers =
        HeaderName::from_str("Access-Control-Allow-Headers").unwrap();
    let star = HeaderValue::from_str("*").unwrap();
    move |request| {
        if *request.method() == Method::OPTIONS {
            let mut response = Response::builder(Status::NO_CONTENT);
            if request.header(&origin).is_some() {
                response
                    .headers_mut()
                    .append(access_control_allow_origin.clone(), star.clone());
            }
            if let Some(method) = request.header(&access_control_request_method) {
                response
                    .headers_mut()
                    .append(access_control_allow_method.clone(), method.clone());
            }
            if let Some(headers) = request.header(&access_control_request_headers) {
                response
                    .headers_mut()
                    .append(access_control_allow_headers.clone(), headers.clone());
            }
            response.build()
        } else {
            let mut response = on_request(request);
            if request.header(&origin).is_some() {
                response
                    .headers_mut()
                    .append(access_control_allow_origin.clone(), star.clone());
            }
            response
        }
    }
}

type HttpError = (Status, String);

fn handle_request(
    request: &mut Request,
    store: Store,
    read_only: bool,
) -> Result<Response, HttpError> {
    match (request.url().path(), request.method().as_ref()) {
        ("/query", "GET") => {
            configure_and_evaluate_sparql_query(&store, &[url_query(request)], None, request)
        }
        ("/query", "POST") => {
            let content_type =
                content_type(request).ok_or_else(|| bad_request("No Content-Type given"))?;
            if content_type == "application/sparql-query" {
                let mut buffer = String::new();
                request
                    .body_mut()
                    .take(MAX_SPARQL_BODY_SIZE)
                    .read_to_string(&mut buffer)
                    .map_err(bad_request)?;
                configure_and_evaluate_sparql_query(
                    &store,
                    &[url_query(request)],
                    Some(buffer),
                    request,
                )
            } else if content_type == "application/x-www-form-urlencoded" {
                let mut buffer = Vec::new();
                request
                    .body_mut()
                    .take(MAX_SPARQL_BODY_SIZE)
                    .read_to_end(&mut buffer)
                    .map_err(bad_request)?;
                configure_and_evaluate_sparql_query(
                    &store,
                    &[url_query(request), &buffer],
                    None,
                    request,
                )
            } else {
                Err(unsupported_media_type(&content_type))
            }
        }
        ("/update", "POST") => {
            if read_only {
                return Err(the_server_is_read_only());
            }
            let content_type =
                content_type(request).ok_or_else(|| bad_request("No Content-Type given"))?;
            if content_type == "application/sparql-update" {
                let mut buffer = String::new();
                request
                    .body_mut()
                    .take(MAX_SPARQL_BODY_SIZE)
                    .read_to_string(&mut buffer)
                    .map_err(bad_request)?;
                configure_and_evaluate_sparql_update(
                    &store,
                    &[url_query(request)],
                    Some(buffer),
                    request,
                )
            } else if content_type == "application/x-www-form-urlencoded" {
                let mut buffer = Vec::new();
                request
                    .body_mut()
                    .take(MAX_SPARQL_BODY_SIZE)
                    .read_to_end(&mut buffer)
                    .map_err(bad_request)?;
                configure_and_evaluate_sparql_update(
                    &store,
                    &[url_query(request), &buffer],
                    None,
                    request,
                )
            } else {
                Err(unsupported_media_type(&content_type))
            }
        }
        (path, "GET") if path.starts_with("/store") => {
            if let Some(target) = store_target(request)? {
                assert_that_graph_exists(&store, &target)?;
                let format = graph_content_negotiation(request)?;
                let triples = store.quads_for_pattern(
                    None,
                    None,
                    None,
                    Some(GraphName::from(target).as_ref()),
                );
                ReadForWrite::build_response(
                    move |w| {
                        Ok((
                            GraphSerializer::from_format(format).triple_writer(w)?,
                            triples,
                        ))
                    },
                    |(mut writer, mut triples)| {
                        Ok(if let Some(t) = triples.next() {
                            writer.write(&t?.into())?;
                            Some((writer, triples))
                        } else {
                            writer.finish()?;
                            None
                        })
                    },
                    format.media_type(),
                )
            } else {
                let format = dataset_content_negotiation(request)?;
                ReadForWrite::build_response(
                    move |w| {
                        Ok((
                            DatasetSerializer::from_format(format).quad_writer(w)?,
                            store.iter(),
                        ))
                    },
                    |(mut writer, mut quads)| {
                        Ok(if let Some(q) = quads.next() {
                            writer.write(&q?)?;
                            Some((writer, quads))
                        } else {
                            writer.finish()?;
                            None
                        })
                    },
                    format.media_type(),
                )
            }
        }
        (path, "PUT") if path.starts_with("/store") => {
            if read_only {
                return Err(the_server_is_read_only());
            }
            let content_type =
                content_type(request).ok_or_else(|| bad_request("No Content-Type given"))?;
            if let Some(target) = store_target(request)? {
                let format = GraphFormat::from_media_type(&content_type)
                    .ok_or_else(|| unsupported_media_type(&content_type))?;
                let new = !match &target {
                    NamedGraphName::NamedNode(target) => {
                        if store
                            .contains_named_graph(target)
                            .map_err(internal_server_error)?
                        {
                            store.clear_graph(target).map_err(internal_server_error)?;
                            true
                        } else {
                            store
                                .insert_named_graph(target)
                                .map_err(internal_server_error)?;
                            false
                        }
                    }
                    NamedGraphName::DefaultGraph => {
                        store
                            .clear_graph(GraphNameRef::DefaultGraph)
                            .map_err(internal_server_error)?;
                        true
                    }
                };
                web_load_graph(&store, request, format, GraphName::from(target).as_ref())?;
                Ok(Response::builder(if new {
                    Status::CREATED
                } else {
                    Status::NO_CONTENT
                })
                .build())
            } else {
                let format = DatasetFormat::from_media_type(&content_type)
                    .ok_or_else(|| unsupported_media_type(&content_type))?;
                store.clear().map_err(internal_server_error)?;
                web_load_dataset(&store, request, format)?;
                Ok(Response::builder(Status::NO_CONTENT).build())
            }
        }
        (path, "DELETE") if path.starts_with("/store") => {
            if read_only {
                return Err(the_server_is_read_only());
            }
            if let Some(target) = store_target(request)? {
                match target {
                    NamedGraphName::DefaultGraph => store
                        .clear_graph(GraphNameRef::DefaultGraph)
                        .map_err(internal_server_error)?,
                    NamedGraphName::NamedNode(target) => {
                        if store
                            .contains_named_graph(&target)
                            .map_err(internal_server_error)?
                        {
                            store
                                .remove_named_graph(&target)
                                .map_err(internal_server_error)?;
                        } else {
                            return Err((
                                Status::NOT_FOUND,
                                format!("The graph {target} does not exists"),
                            ));
                        }
                    }
                }
            } else {
                store.clear().map_err(internal_server_error)?;
            }
            Ok(Response::builder(Status::NO_CONTENT).build())
        }
        (path, "POST") if path.starts_with("/store") => {
            if read_only {
                return Err(the_server_is_read_only());
            }
            let content_type =
                content_type(request).ok_or_else(|| bad_request("No Content-Type given"))?;
            if let Some(target) = store_target(request)? {
                let format = GraphFormat::from_media_type(&content_type)
                    .ok_or_else(|| unsupported_media_type(&content_type))?;
                let new = assert_that_graph_exists(&store, &target).is_ok();
                web_load_graph(&store, request, format, GraphName::from(target).as_ref())?;
                Ok(Response::builder(if new {
                    Status::CREATED
                } else {
                    Status::NO_CONTENT
                })
                .build())
            } else {
                match GraphOrDatasetFormat::from_media_type(&content_type)
                    .map_err(|_| unsupported_media_type(&content_type))?
                {
                    GraphOrDatasetFormat::Graph(format) => {
                        let graph =
                            resolve_with_base(request, &format!("/store/{:x}", random::<u128>()))?;
                        web_load_graph(&store, request, format, graph.as_ref().into())?;
                        Ok(Response::builder(Status::CREATED)
                            .with_header(HeaderName::LOCATION, graph.into_string())
                            .unwrap()
                            .build())
                    }
                    GraphOrDatasetFormat::Dataset(format) => {
                        web_load_dataset(&store, request, format)?;
                        Ok(Response::builder(Status::NO_CONTENT).build())
                    }
                }
            }
        }
        (path, "HEAD") if path.starts_with("/store") => {
            if let Some(target) = store_target(request)? {
                assert_that_graph_exists(&store, &target)?;
            }
            Ok(Response::builder(Status::OK).build())
        }
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

fn base_url(request: &Request) -> String {
    let mut url = request.url().clone();
    url.set_query(None);
    url.set_fragment(None);
    url.into()
}

fn resolve_with_base(request: &Request, url: &str) -> Result<NamedNode, HttpError> {
    Ok(NamedNode::new_unchecked(
        Iri::parse(base_url(request))
            .map_err(bad_request)?
            .resolve(url)
            .map_err(bad_request)?
            .into_inner(),
    ))
}

fn url_query(request: &Request) -> &[u8] {
    request.url().query().unwrap_or("").as_bytes()
}

fn url_query_parameter<'a>(request: &'a Request, param: &str) -> Option<Cow<'a, str>> {
    request
        .url()
        .query_pairs()
        .find(|(k, _)| k == param)
        .map(|(_, v)| v)
}

fn configure_and_evaluate_sparql_query(
    store: &Store,
    encoded: &[&[u8]],
    mut query: Option<String>,
    request: &Request,
) -> Result<Response, HttpError> {
    let mut default_graph_uris = Vec::new();
    let mut named_graph_uris = Vec::new();
    let mut use_default_graph_as_union = false;
    for encoded in encoded {
        for (k, v) in form_urlencoded::parse(encoded) {
            match k.as_ref() {
                "query" => {
                    if query.is_some() {
                        return Err(bad_request("Multiple query parameters provided"));
                    }
                    query = Some(v.into_owned())
                }
                "default-graph-uri" => default_graph_uris.push(v.into_owned()),
                "union-default-graph" => use_default_graph_as_union = true,
                "named-graph-uri" => named_graph_uris.push(v.into_owned()),
                _ => (),
            }
        }
    }
    let query = query.ok_or_else(|| bad_request("You should set the 'query' parameter"))?;
    evaluate_sparql_query(
        store,
        &query,
        use_default_graph_as_union,
        default_graph_uris,
        named_graph_uris,
        request,
    )
}

fn evaluate_sparql_query(
    store: &Store,
    query: &str,
    use_default_graph_as_union: bool,
    default_graph_uris: Vec<String>,
    named_graph_uris: Vec<String>,
    request: &Request,
) -> Result<Response, HttpError> {
    let mut query = Query::parse(query, Some(&base_url(request))).map_err(bad_request)?;

    if use_default_graph_as_union {
        if !default_graph_uris.is_empty() || !named_graph_uris.is_empty() {
            return Err(bad_request(
                "default-graph-uri or named-graph-uri and union-default-graph should not be set at the same time"
            ));
        }
        query.dataset_mut().set_default_graph_as_union()
    } else if !default_graph_uris.is_empty() || !named_graph_uris.is_empty() {
        query.dataset_mut().set_default_graph(
            default_graph_uris
                .into_iter()
                .map(|e| Ok(NamedNode::new(e)?.into()))
                .collect::<Result<Vec<GraphName>, IriParseError>>()
                .map_err(bad_request)?,
        );
        query.dataset_mut().set_available_named_graphs(
            named_graph_uris
                .into_iter()
                .map(|e| Ok(NamedNode::new(e)?.into()))
                .collect::<Result<Vec<NamedOrBlankNode>, IriParseError>>()
                .map_err(bad_request)?,
        );
    }

    let results = store.query(query).map_err(internal_server_error)?;
    match results {
        QueryResults::Solutions(solutions) => {
            let format = query_results_content_negotiation(request)?;
            ReadForWrite::build_response(
                move |w| {
                    Ok((
                        QueryResultsSerializer::from_format(format)
                            .solutions_writer(w, solutions.variables().to_vec())?,
                        solutions,
                    ))
                },
                |(mut writer, mut solutions)| {
                    Ok(if let Some(solution) = solutions.next() {
                        writer.write(&solution?)?;
                        Some((writer, solutions))
                    } else {
                        writer.finish()?;
                        None
                    })
                },
                format.media_type(),
            )
        }
        QueryResults::Boolean(result) => {
            let format = query_results_content_negotiation(request)?;
            let mut body = Vec::new();
            QueryResultsSerializer::from_format(format)
                .write_boolean_result(&mut body, result)
                .map_err(internal_server_error)?;
            Ok(Response::builder(Status::OK)
                .with_header(HeaderName::CONTENT_TYPE, format.media_type())
                .unwrap()
                .with_body(body))
        }
        QueryResults::Graph(triples) => {
            let format = graph_content_negotiation(request)?;
            ReadForWrite::build_response(
                move |w| {
                    Ok((
                        GraphSerializer::from_format(format).triple_writer(w)?,
                        triples,
                    ))
                },
                |(mut writer, mut triples)| {
                    Ok(if let Some(t) = triples.next() {
                        writer.write(&t?)?;
                        Some((writer, triples))
                    } else {
                        writer.finish()?;
                        None
                    })
                },
                format.media_type(),
            )
        }
    }
}

fn configure_and_evaluate_sparql_update(
    store: &Store,
    encoded: &[&[u8]],
    mut update: Option<String>,
    request: &Request,
) -> Result<Response, HttpError> {
    let mut use_default_graph_as_union = false;
    let mut default_graph_uris = Vec::new();
    let mut named_graph_uris = Vec::new();
    for encoded in encoded {
        for (k, v) in form_urlencoded::parse(encoded) {
            match k.as_ref() {
                "update" => {
                    if update.is_some() {
                        return Err(bad_request("Multiple update parameters provided"));
                    }
                    update = Some(v.into_owned())
                }
                "using-graph-uri" => default_graph_uris.push(v.into_owned()),
                "using-union-graph" => use_default_graph_as_union = true,
                "using-named-graph-uri" => named_graph_uris.push(v.into_owned()),
                _ => (),
            }
        }
    }
    let update = update.ok_or_else(|| bad_request("You should set the 'update' parameter"))?;
    evaluate_sparql_update(
        store,
        &update,
        use_default_graph_as_union,
        default_graph_uris,
        named_graph_uris,
        request,
    )
}

fn evaluate_sparql_update(
    store: &Store,
    update: &str,
    use_default_graph_as_union: bool,
    default_graph_uris: Vec<String>,
    named_graph_uris: Vec<String>,
    request: &Request,
) -> Result<Response, HttpError> {
    let mut update =
        Update::parse(update, Some(base_url(request).as_str())).map_err(bad_request)?;

    if use_default_graph_as_union {
        if !default_graph_uris.is_empty() || !named_graph_uris.is_empty() {
            return Err(bad_request(
                "using-graph-uri or using-named-graph-uri and using-union-graph should not be set at the same time"
            ));
        }
        for using in update.using_datasets_mut() {
            if !using.is_default_dataset() {
                return Err(bad_request(
                    "using-union-graph must not be used with a SPARQL UPDATE containing USING",
                ));
            }
            using.set_default_graph_as_union();
        }
    } else if !default_graph_uris.is_empty() || !named_graph_uris.is_empty() {
        let default_graph_uris = default_graph_uris
            .into_iter()
            .map(|e| Ok(NamedNode::new(e)?.into()))
            .collect::<Result<Vec<GraphName>, IriParseError>>()
            .map_err(bad_request)?;
        let named_graph_uris = named_graph_uris
            .into_iter()
            .map(|e| Ok(NamedNode::new(e)?.into()))
            .collect::<Result<Vec<NamedOrBlankNode>, IriParseError>>()
            .map_err(bad_request)?;
        for using in update.using_datasets_mut() {
            if !using.is_default_dataset() {
                return Err(bad_request(
                        "using-graph-uri and using-named-graph-uri must not be used with a SPARQL UPDATE containing USING",
                    ));
            }
            using.set_default_graph(default_graph_uris.clone());
            using.set_available_named_graphs(named_graph_uris.clone());
        }
    }
    store.update(update).map_err(internal_server_error)?;
    Ok(Response::builder(Status::NO_CONTENT).build())
}

fn store_target(request: &Request) -> Result<Option<NamedGraphName>, HttpError> {
    if request.url().path() == "/store" {
        let mut graph = None;
        let mut default = false;
        for (k, v) in request.url().query_pairs() {
            match k.as_ref() {
                "graph" => graph = Some(v.into_owned()),
                "default" => default = true,
                _ => continue,
            }
        }
        if let Some(graph) = graph {
            if default {
                Err(bad_request(
                    "Both graph and default parameters should not be set at the same time",
                ))
            } else {
                Ok(Some(NamedGraphName::NamedNode(resolve_with_base(
                    request, &graph,
                )?)))
            }
        } else if default {
            Ok(Some(NamedGraphName::DefaultGraph))
        } else {
            Ok(None)
        }
    } else {
        Ok(Some(NamedGraphName::NamedNode(resolve_with_base(
            request, "",
        )?)))
    }
}

fn assert_that_graph_exists(store: &Store, target: &NamedGraphName) -> Result<(), HttpError> {
    if match target {
        NamedGraphName::DefaultGraph => true,
        NamedGraphName::NamedNode(target) => store
            .contains_named_graph(target)
            .map_err(internal_server_error)?,
    } {
        Ok(())
    } else {
        Err((
            Status::NOT_FOUND,
            format!(
                "The graph {} does not exists",
                GraphName::from(target.clone())
            ),
        ))
    }
}

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
enum NamedGraphName {
    NamedNode(NamedNode),
    DefaultGraph,
}

impl From<NamedGraphName> for GraphName {
    fn from(graph_name: NamedGraphName) -> Self {
        match graph_name {
            NamedGraphName::NamedNode(node) => node.into(),
            NamedGraphName::DefaultGraph => Self::DefaultGraph,
        }
    }
}

fn graph_content_negotiation(request: &Request) -> Result<GraphFormat, HttpError> {
    content_negotiation(
        request,
        &[
            GraphFormat::NTriples.media_type(),
            GraphFormat::Turtle.media_type(),
            GraphFormat::RdfXml.media_type(),
        ],
        GraphFormat::from_media_type,
    )
}

fn dataset_content_negotiation(request: &Request) -> Result<DatasetFormat, HttpError> {
    content_negotiation(
        request,
        &[
            DatasetFormat::NQuads.media_type(),
            DatasetFormat::TriG.media_type(),
        ],
        DatasetFormat::from_media_type,
    )
}

fn query_results_content_negotiation(request: &Request) -> Result<QueryResultsFormat, HttpError> {
    content_negotiation(
        request,
        &[
            QueryResultsFormat::Json.media_type(),
            QueryResultsFormat::Xml.media_type(),
            QueryResultsFormat::Csv.media_type(),
            QueryResultsFormat::Tsv.media_type(),
        ],
        QueryResultsFormat::from_media_type,
    )
}

fn content_negotiation<F>(
    request: &Request,
    supported: &[&str],
    parse: impl Fn(&str) -> Option<F>,
) -> Result<F, HttpError> {
    let default = HeaderValue::default();
    let header = request
        .header(&HeaderName::ACCEPT)
        .unwrap_or(&default)
        .to_str()
        .map_err(|_| bad_request("The Accept header should be a valid ASCII string"))?;

    if header.is_empty() {
        return parse(supported.first().unwrap())
            .ok_or_else(|| internal_server_error("Unknown media type"));
    }
    let mut result = None;
    let mut result_score = 0_f32;

    for possible in header.split(',') {
        let (possible, parameters) = possible.split_once(';').unwrap_or((possible, ""));
        let (possible_base, possible_sub) = possible
            .split_once('/')
            .ok_or_else(|| bad_request(format!("Invalid media type: '{possible}'")))?;
        let possible_base = possible_base.trim();
        let possible_sub = possible_sub.trim();

        let mut score = 1.;
        for parameter in parameters.split(';') {
            let parameter = parameter.trim();
            if let Some(s) = parameter.strip_prefix("q=") {
                score = f32::from_str(s.trim())
                    .map_err(|_| bad_request(format!("Invalid Accept media type score: {s}")))?
            }
        }
        if score <= result_score {
            continue;
        }
        for candidate in supported {
            let (candidate_base, candidate_sub) = candidate
                .split_once(';')
                .map_or(*candidate, |(p, _)| p)
                .split_once('/')
                .ok_or_else(|| {
                    internal_server_error(format!("Invalid media type: '{possible}'"))
                })?;
            if (possible_base == candidate_base || possible_base == "*")
                && (possible_sub == candidate_sub || possible_sub == "*")
            {
                result = Some(candidate);
                result_score = score;
                break;
            }
        }
    }

    let result = result.ok_or_else(|| {
        (
            Status::NOT_ACCEPTABLE,
            format!("The available Content-Types are {}", supported.join(", "),),
        )
    })?;

    parse(result).ok_or_else(|| internal_server_error("Unknown media type"))
}

fn content_type(request: &Request) -> Option<String> {
    let value = request.header(&HeaderName::CONTENT_TYPE)?.to_str().ok()?;
    Some(
        value
            .split_once(';')
            .map_or(value, |(b, _)| b)
            .trim()
            .to_ascii_lowercase(),
    )
}

fn web_load_graph(
    store: &Store,
    request: &mut Request,
    format: GraphFormat,
    to_graph_name: GraphNameRef<'_>,
) -> Result<(), HttpError> {
    let base_iri = if let GraphNameRef::NamedNode(graph_name) = to_graph_name {
        Some(graph_name.as_str())
    } else {
        None
    };
    if url_query_parameter(request, "no_transaction").is_some() {
        web_bulk_loader(store, request).load_graph(
            BufReader::new(request.body_mut()),
            format,
            to_graph_name,
            base_iri,
        )
    } else {
        store.load_graph(
            BufReader::new(request.body_mut()),
            format,
            to_graph_name,
            base_iri,
        )
    }
    .map_err(loader_to_http_error)
}

fn web_load_dataset(
    store: &Store,
    request: &mut Request,
    format: DatasetFormat,
) -> Result<(), HttpError> {
    if url_query_parameter(request, "no_transaction").is_some() {
        web_bulk_loader(store, request).load_dataset(
            BufReader::new(request.body_mut()),
            format,
            None,
        )
    } else {
        store.load_dataset(BufReader::new(request.body_mut()), format, None)
    }
    .map_err(loader_to_http_error)
}

fn web_bulk_loader(store: &Store, request: &Request) -> BulkLoader {
    let start = Instant::now();
    let mut loader = store.bulk_loader().on_progress(move |size| {
        let elapsed = start.elapsed();
        eprintln!(
            "{} triples loaded in {}s ({} t/s)",
            size,
            elapsed.as_secs(),
            ((size as f64) / elapsed.as_secs_f64()).round()
        )
    });
    if url_query_parameter(request, "lenient").is_some() {
        loader = loader.on_parse_error(move |e| {
            eprintln!("Parsing error: {e}");
            Ok(())
        })
    }
    loader
}

fn error(status: Status, message: impl fmt::Display) -> Response {
    Response::builder(status)
        .with_header(HeaderName::CONTENT_TYPE, "text/plain; charset=utf-8")
        .unwrap()
        .with_body(message.to_string())
}

fn bad_request(message: impl fmt::Display) -> HttpError {
    (Status::BAD_REQUEST, message.to_string())
}

fn the_server_is_read_only() -> HttpError {
    (Status::FORBIDDEN, "The server is read-only".into())
}

fn unsupported_media_type(content_type: &str) -> HttpError {
    (
        Status::UNSUPPORTED_MEDIA_TYPE,
        format!("No supported content Content-Type given: {content_type}"),
    )
}

fn internal_server_error(message: impl fmt::Display) -> HttpError {
    eprintln!("Internal server error: {message}");
    (Status::INTERNAL_SERVER_ERROR, message.to_string())
}

fn loader_to_http_error(e: LoaderError) -> HttpError {
    match e {
        LoaderError::Parsing(e) => bad_request(e),
        LoaderError::Storage(e) => internal_server_error(e),
    }
}

/// Hacky tool to allow implementing read on top of a write loop
struct ReadForWrite<O, U: (Fn(O) -> io::Result<Option<O>>)> {
    buffer: Rc<RefCell<Vec<u8>>>,
    position: usize,
    add_more_data: U,
    state: Option<O>,
}

impl<O: 'static, U: (Fn(O) -> io::Result<Option<O>>) + 'static> ReadForWrite<O, U> {
    fn build_response(
        initial_state_builder: impl FnOnce(ReadForWriteWriter) -> io::Result<O>,
        add_more_data: U,
        content_type: &'static str,
    ) -> Result<Response, HttpError> {
        let buffer = Rc::new(RefCell::new(Vec::new()));
        let state = initial_state_builder(ReadForWriteWriter {
            buffer: Rc::clone(&buffer),
        })
        .map_err(internal_server_error)?;
        Ok(Response::builder(Status::OK)
            .with_header(HeaderName::CONTENT_TYPE, content_type)
            .unwrap()
            .with_body(Body::from_read(Self {
                buffer,
                position: 0,
                add_more_data,
                state: Some(state),
            })))
    }
}

impl<O, U: (Fn(O) -> io::Result<Option<O>>)> Read for ReadForWrite<O, U> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while self.position == self.buffer.borrow().len() {
            // We read more data
            if let Some(state) = self.state.take() {
                self.buffer.borrow_mut().clear();
                self.position = 0;
                self.state = match (self.add_more_data)(state) {
                    Ok(state) => state,
                    Err(e) => {
                        eprintln!("Internal server error while streaming results: {e}");
                        self.buffer
                            .borrow_mut()
                            .write_all(e.to_string().as_bytes())?;
                        None
                    }
                }
            } else {
                return Ok(0); // End
            }
        }
        let buffer = self.buffer.borrow();
        let len = min(buffer.len() - self.position, buf.len());
        buf[..len].copy_from_slice(&buffer[self.position..self.position + len]);
        self.position += len;
        Ok(len)
    }
}

struct ReadForWriteWriter {
    buffer: Rc<RefCell<Vec<u8>>>,
}

impl Write for ReadForWriteWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.borrow_mut().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.buffer.borrow_mut().write_all(buf)
    }
}
