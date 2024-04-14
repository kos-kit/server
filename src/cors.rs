use oxhttp::model::{HeaderName, HeaderValue, Method, Request, Response, Status};
use std::str::FromStr;

pub fn cors_middleware(
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
