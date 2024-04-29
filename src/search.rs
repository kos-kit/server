use std::collections::HashMap;

use oxhttp::model::{Request, Response, Status};
use oxigraph::store::Store;
use tantivy::{
    collector::TopDocs, query::QueryParser, schema::Value, Document, IndexReader, TantivyDocument,
};
use url::Url;

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
            .map_err(|err| "error parsing limit")?,
            offset: (match url_query.get("offset") {
                Some(offset_string) => offset_string.parse::<usize>(),
                None => Ok(0),
            })
            .map_err(|err| "error parsing offset")?,
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
    tantivy_index_reader: IndexReader,
    tantivy_query_parser: QueryParser,
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
    let top_docs = tantivy_index_searcher
        .search(
            &query,
            &TopDocs::with_limit(parsed_url.limit).and_offset(parsed_url.offset),
        )
        .map_err(|err| (Status::INTERNAL_SERVER_ERROR, err.to_string()))?;

    let iri_field = tantivy_index_searcher
        .schema()
        .get_field("iri")
        .map_err(|err| (Status::INTERNAL_SERVER_ERROR, err.to_string()))?;

    for (_score, doc_address) in top_docs {
        let retrieved_doc = tantivy_index_searcher
            .doc::<TantivyDocument>(doc_address)
            .map_err(|err| (Status::INTERNAL_SERVER_ERROR, err.to_string()))?;
        if let Some(iri_value) = retrieved_doc.get_first(iri_field) {
            if let Some(iri) = iri_value.as_str() {
                eprintln!("IRI: {}", iri);
            }
        }
    }

    Ok(
        Response::builder(Status::OK).build(), // .with_header(HeaderName::CONTENT_TYPE, content_type)
                                               // .unwrap()
                                               // .with_body(Body::from_read(Self {
                                               //     buffer,
                                               //     position: 0,
                                               //     add_more_data,
                                               //     state: Some(state),
                                               // }))
    )
}
