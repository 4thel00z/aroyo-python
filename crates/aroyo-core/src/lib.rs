//! Streams Arrow record batches into crash-resumable Parquet multipart uploads on
//! S3-compatible storage, and reads Parquet objects back with parallel range requests.
//!
//! The write path follows the method of Arroyo's filesystem sink: one multipart
//! upload per file, a checkpoint that carries the parts uploaded so far plus the
//! trailing bytes (unsent bytes and a footer), and a finish step that another
//! process can run after a crash. No conditional writes are used.

pub mod config;
pub mod error;
pub mod footer;
pub mod reader;
pub mod state;
pub mod upload;
pub mod writer;

pub use config::S3Config;
pub use error::{Error, Result};
pub use reader::{open, ParquetStream, ReadOptions};
pub use state::{UploadedPart, WriterState};
pub use upload::{abort, finish, Store, UploadLimits};
pub use writer::{ParquetStreamWriter, WriterOptions};
