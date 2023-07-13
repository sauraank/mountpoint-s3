use mountpoint_s3_crt::common::allocator::Allocator;
use mountpoint_s3_crt::common::uri::Uri;
use mountpoint_s3_crt::s3::endpoint_resolver::{RequestContext, ResolverError, RuleEngine};
use std::ffi::OsStr;
use std::os::unix::prelude::OsStrExt;
use thiserror::Error;

use crate::EndpointConfig;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Endpoint {
    uri: Uri,
}

impl Endpoint {
    /// get uri from Endpoint
    fn get_uri(&self) -> Uri {
        self.uri.clone()
    }

    /// Create a new endpoint URI for the given Endpoint Configuration. This method automatically resolves the right
    /// endpoint URI to target.
    pub fn set_endpoint(endpoint_config: EndpointConfig) -> Result<Uri, EndpointError> {
        let allocator = Allocator::default();
        let mut endpoint_request_context = RequestContext::new(&allocator).unwrap();
        let endpoint_rule_engine = RuleEngine::new(&allocator).unwrap();
        endpoint_request_context
            .add_string(&allocator, "Region", endpoint_config.get_region())
            .unwrap();
        if let Some(endpoint_uri) = endpoint_config.get_endpoint() {
            endpoint_request_context
                .add_string(&allocator, "Endpoint", endpoint_uri.get_uri().as_os_str())
                .unwrap()
        };
        if let Some(bucket) = endpoint_config.get_bucket() {
            // TODO: Handle the case of Invalid bucket name/ Alias/ ARN
            endpoint_request_context
                .add_string(&allocator, "Bucket", bucket)
                .unwrap()
        };
        if endpoint_config.is_accelerate() {
            endpoint_request_context
                .add_boolean(&allocator, "UseFIPS", true)
                .unwrap()
        };
        if endpoint_config.is_dual_stack() {
            endpoint_request_context
                .add_boolean(&allocator, "UseDualStack", true)
                .unwrap()
        };
        if endpoint_config.is_accelerate() {
            endpoint_request_context
                .add_boolean(&allocator, "Accelerate", true)
                .unwrap()
        };
        if endpoint_config.get_addresssing_style() == AddressingStyle::Path {
            endpoint_request_context
                .add_boolean(&allocator, "ForcePathStyle", true)
                .unwrap()
        };

        let resolved_endpoint = endpoint_rule_engine
            .resolve(endpoint_request_context)
            .map_err(EndpointError::UnresolvedEndpoint)?;
        let endpoint_uri = resolved_endpoint.get_url();
        Uri::new_from_str(&allocator, &endpoint_uri)
            .map_err(|e| EndpointError::InvalidUri(InvalidUriError::CouldNotParse(e)))
    }

    /// Create a new endpoint with a manually specified URI.
    pub fn from_uri(uri: &str) -> Result<Self, EndpointError> {
        let parsed_uri = Uri::new_from_str(&Allocator::default(), OsStr::from_bytes(uri.as_bytes()))
            .map_err(InvalidUriError::CouldNotParse)?;
        tracing::debug!(endpoint=?parsed_uri.as_os_str());
        Ok(Self { uri: parsed_uri })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AddressingStyle {
    /// Use virtual addressing if possible, but fall back to path addressing if necessary
    #[default]
    Automatic,
    /// Always use virtual addressing
    Virtual,
    /// Always use path addressing
    Path,
}

#[derive(Debug, Error)]
pub enum EndpointError {
    #[error("invalid URI")]
    InvalidUri(#[from] InvalidUriError),
    #[error("endpoint URI cannot include path or query string")]
    InvalidEndpoint,
    #[error("endpoint could not be resolved")]
    UnresolvedEndpoint(#[from] ResolverError),
}

#[derive(Debug, Error)]
pub enum InvalidUriError {
    #[error("URI could not be parsed")]
    CouldNotParse(#[from] mountpoint_s3_crt::common::error::Error),
    #[error("URI cannot include path or query string")]
    CannotContainPathOrQueryString,
    #[error("URI is not valid UTF-8")]
    InvalidUtf8,
}
