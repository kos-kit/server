use oxhttp::model::{Request, Response, Status};
use tantivy::Index;

type HttpError = (Status, String);

pub fn handle_request(
    index_result_sparql: String,
    request: &mut Request,
    tantivy_index: Index,
) -> Result<Response, HttpError> {
    if request.method().as_ref() != "GET" {
        return Err((
            Status::METHOD_NOT_ALLOWED,
            format!("{} is not supported by this server", request.method()),
        ));
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
