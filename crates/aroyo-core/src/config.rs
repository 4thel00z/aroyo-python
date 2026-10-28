//! S3 connection settings and the store they build.

use std::sync::Arc;

use object_store::aws::{AmazonS3, AmazonS3Builder};

use crate::error::Result;

/// Settings for one S3-compatible endpoint.
///
/// With `read_env` set, the `AWS_*` environment variables are read first and every
/// field set here overrides them. Without it, only the fields set here apply.
#[derive(Clone, Debug, Default)]
pub struct S3Config {
    pub read_env: bool,
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub allow_http: Option<bool>,
    pub virtual_hosted_style: Option<bool>,
}

impl S3Config {
    /// A config that reads the `AWS_*` environment variables.
    pub fn from_env() -> Self {
        Self {
            read_env: true,
            ..Self::default()
        }
    }

    /// Builds the store. Credentials are resolved lazily by the store on first use.
    pub fn build(&self) -> Result<Arc<AmazonS3>> {
        let mut builder = match self.read_env {
            true => AmazonS3Builder::from_env(),
            false => AmazonS3Builder::new(),
        };
        if let Some(bucket) = &self.bucket {
            builder = builder.with_bucket_name(bucket);
        }
        if let Some(region) = &self.region {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(key) = &self.access_key_id {
            builder = builder.with_access_key_id(key);
        }
        if let Some(secret) = &self.secret_access_key {
            builder = builder.with_secret_access_key(secret);
        }
        if let Some(token) = &self.session_token {
            builder = builder.with_token(token);
        }
        if let Some(allow_http) = self.allow_http {
            builder = builder.with_allow_http(allow_http);
        }
        if let Some(virtual_hosted) = self.virtual_hosted_style {
            builder = builder.with_virtual_hosted_style_request(virtual_hosted);
        }
        Ok(Arc::new(builder.build()?))
    }
}
