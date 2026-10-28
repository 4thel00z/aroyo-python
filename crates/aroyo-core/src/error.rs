//! The error type shared by every aroyo-core operation.

/// Every failure aroyo-core reports.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("object store: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("object path: {0}")]
    Path(#[from] object_store::path::Error),
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("arrow: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("upload task: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("invalid state: {0}")]
    InvalidState(String),
    #[error("state encoding: {0}")]
    StateEncoding(String),
    #[error(
        "upload {upload_id} for {path} would need part {part_number}; the store allows at most {max_parts} parts"
    )]
    PartLimit {
        path: String,
        upload_id: String,
        part_number: u32,
        max_parts: u32,
    },
    #[error("{path} holds {actual} bytes, expected {expected}; finishing failed with: {cause}")]
    SizeMismatch {
        path: String,
        expected: u64,
        actual: u64,
        cause: String,
    },
}

/// Result alias over [`Error`].
pub type Result<T> = std::result::Result<T, Error>;
