use std::collections::HashMap;

use oxhttp::model::{HeaderName, Request, Response, Status};
use oxigraph::{
    io::GraphSerializer,
    model::{GraphNameRef, QuadRef},
    sparql::QueryResults,
    store::Store,
};
use tantivy::{
    collector::{Count, TopDocs},
    query::QueryParser,
    schema::Value,
    IndexReader, TantivyDocument,
};
use url::Url;

use crate::sparql::{graph_content_negotiation, ReadForWrite};

type HttpError = (Status, String);

struct ParsedUrl {
    limit: usize,
    offset: usize,
    query: String,
}

impl ParsedUrl {
    fn parse(url: &Url) -> Result<Self, String> {
        let url_query: HashMap<_, _> = url.query_pairs().into_owned().collect();
        Ok(Self {
            limit: (match url_query.get("limit") {
                Some(limit_string) => limit_string.parse::<usize>(),
                None => Ok(10),
            })
            .map_err(|err| format!("error parsing limit: {}", err))?,
            offset: (match url_query.get("offset") {
                Some(offset_string) => offset_string.parse::<usize>(),
                None => Ok(0),
            })
            .map_err(|err| format!("error parsing offset: {}", err))?,
            query: url_query
                .get("query")
                .ok_or("missing query string")?
                .clone(),
        })
    }
}

pub fn handle_request(
    index_result_sparql: String,
    oxigraph_store: Store,
    request: &mut Request,
    tantivy_index_reader: &IndexReader,
    tantivy_query_parser: &QueryParser,
) -> Result<Response, HttpError> {
    if request.method().as_ref() != "GET" {
        return Err((
            Status::METHOD_NOT_ALLOWED,
            format!("{} is not supported by this server", request.method()),
        ));
    }

    let parsed_url =
        ParsedUrl::parse(request.url()).map_err(|err_string| (Status::BAD_REQUEST, err_string))?;

    let query = tantivy_query_parser
        .parse_query(parsed_url.query.as_str())
        .map_err(|err| (Status::BAD_REQUEST, err.to_string()))?;

    let tantivy_index_searcher = tantivy_index_reader.searcher();
    if tantivy_index_searcher.num_docs() == 0 {
        return Err((Status::INTERNAL_SERVER_ERROR, format!("index is empty")));
    }

    assert!(tantivy_index_searcher.num_docs() > 0);

    let count = tantivy_index_searcher
        .search(&query, &Count)
        .map_err(|err| {
            (
                Status::INTERNAL_SERVER_ERROR,
                format!(
                    "error searching index:\nQuery: {}\nError: {}",
                    parsed_url.query, err
                ),
            )
        })?;

    if parsed_url.limit == 0 {
        return Ok(Response::builder(Status::NO_CONTENT)
            .with_header("X-Total-Count", count.to_string())
            .unwrap()
            .build());
    }

    let top_docs = tantivy_index_searcher
        .search(
            &query,
            &TopDocs::with_limit(parsed_url.limit).and_offset(parsed_url.offset),
        )
        .map_err(|err| {
            (
                Status::INTERNAL_SERVER_ERROR,
                format!(
                    "error searching index:\nQuery: {}\nError: {}",
                    parsed_url.query, err
                ),
            )
        })?;

    let iri_field = tantivy_index_searcher
        .schema()
        .get_field("iri")
        .map_err(|err| {
            (
                Status::INTERNAL_SERVER_ERROR,
                format!("error getting IRI field from index: {}", err),
            )
        })?;

    let index_results_oxigraph_store = Store::new().map_err(|err| {
        (
            Status::INTERNAL_SERVER_ERROR,
            format!("error creating search results Oxigraph store: {}", err),
        )
    })?;

    for (_score, doc_address) in top_docs {
        let retrieved_doc = tantivy_index_searcher
            .doc::<TantivyDocument>(doc_address)
            .map_err(|err| (Status::INTERNAL_SERVER_ERROR, err.to_string()))?;
        if let Some(iri_value) = retrieved_doc.get_first(iri_field) {
            if let Some(iri) = iri_value.as_str() {
                // Oxigraph doesn't allow out-of-band variable binding like some SPARQL engines do.
                // oxrdflib just adds a VALUES clause to the end of the query.

                let index_result_sparql_with_values =
                    format!("{}\nVALUES ?iri {{ {} }}", index_result_sparql, iri);

                let index_result_query_results: QueryResults = oxigraph_store
                    .query(index_result_sparql_with_values.as_str())
                    .map_err(|err| {
                        (
                            Status::INTERNAL_SERVER_ERROR,
                            format!(
                                "error executing index result query:\nQuery:\n{}\nError:\n{}",
                                index_result_sparql_with_values, err
                            ),
                        )
                    })?;

                if let QueryResults::Graph(query_triple_iter) = index_result_query_results {
                    for triple in query_triple_iter.filter_map(|t| t.ok()) {
                        index_results_oxigraph_store
                            .insert(QuadRef::new(
                                &triple.subject,
                                &triple.predicate,
                                &triple.object,
                                GraphNameRef::DefaultGraph,
                            ))
                            .map_err(|err| {
                                (
                                    Status::INTERNAL_SERVER_ERROR,
                                    format!("error adding index result query results: {}", err),
                                )
                            })?;
                    }
                } else {
                    return Err((
                        Status::INTERNAL_SERVER_ERROR,
                        String::from(
                            "index result query did not return a graph (is it a CONSTRUCT query?)",
                        ),
                    ));
                }
            }
        }
    }

    if let QueryResults::Graph(triples) = index_results_oxigraph_store
        .query("CONSTRUCT WHERE { ?s ?p ?o }")
        .map_err(|err| {
            (
                Status::INTERNAL_SERVER_ERROR,
                format!("error serializing triples: {}", err),
            )
        })?
    {
        // Borrow content negotation code from SPARQL
        let format = graph_content_negotiation(request)?;
        return ReadForWrite::build_response(
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
        .map(|mut response| {
            response
                .append_header("X-Total-Count", count.to_string())
                .unwrap();
            response
        });
    } else {
        return Err((
            Status::INTERNAL_SERVER_ERROR,
            String::from("CONSTRUCT query should always return triples"),
        ));
    }
}
