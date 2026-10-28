//! Checkpoint state: everything a new process needs to finish an upload.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

const MAGIC: &[u8; 4] = b"ARYO";
const FORMAT_VERSION: u8 = 1;

/// One part that the store has acknowledged.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadedPart {
    /// S3 part number, counted from 1.
    pub number: u32,
    pub etag: String,
}

/// The state of one file's upload at a checkpoint or after `close`.
///
/// `trailing_bytes` holds the bytes not yet uploaded followed by a Parquet footer
/// for the row groups written so far. After `close` it is empty: the real footer
/// was uploaded as the last part and only the completion is pending.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterState {
    pub path: String,
    pub upload_id: String,
    pub part_size: u64,
    pub parts: Vec<UploadedPart>,
    pub uploaded_bytes: u64,
    #[serde(with = "serde_bytes")]
    pub trailing_bytes: Vec<u8>,
    pub rows: u64,
}

impl WriterState {
    /// Size of the object once the upload is finished.
    pub fn expected_size(&self) -> u64 {
        self.uploaded_bytes + self.trailing_bytes.len() as u64
    }

    /// True after `close`: nothing left to upload, only the completion is pending.
    pub fn is_pending_commit(&self) -> bool {
        self.trailing_bytes.is_empty()
    }

    /// Serializes to a versioned binary blob.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let body = postcard::to_allocvec(self).map_err(|e| Error::StateEncoding(e.to_string()))?;
        let mut out = Vec::with_capacity(MAGIC.len() + 1 + body.len());
        out.extend_from_slice(MAGIC);
        out.push(FORMAT_VERSION);
        out.extend_from_slice(&body);
        Ok(out)
    }

    /// Parses a blob produced by [`Self::to_bytes`].
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let header_len = MAGIC.len() + 1;
        if bytes.len() < header_len || &bytes[..MAGIC.len()] != MAGIC {
            return Err(Error::StateEncoding(
                "not an aroyo writer state".to_string(),
            ));
        }
        let version = bytes[MAGIC.len()];
        if version != FORMAT_VERSION {
            return Err(Error::StateEncoding(format!(
                "unsupported state format version {version}, expected {FORMAT_VERSION}"
            )));
        }
        let state: Self = postcard::from_bytes(&bytes[header_len..])
            .map_err(|e| Error::StateEncoding(e.to_string()))?;
        state.validate()?;
        Ok(state)
    }

    /// Checks the invariants `finish` relies on.
    pub fn validate(&self) -> Result<()> {
        if self.path.is_empty() || self.upload_id.is_empty() {
            return Err(Error::InvalidState(
                "state has an empty path or upload id".to_string(),
            ));
        }
        if self.part_size == 0 {
            return Err(Error::InvalidState(
                "state has a zero part size".to_string(),
            ));
        }
        let numbered = self
            .parts
            .iter()
            .enumerate()
            .all(|(index, part)| part.number as usize == index + 1);
        if !numbered {
            return Err(Error::InvalidState(
                "part numbers must run from 1 without gaps".to_string(),
            ));
        }
        Ok(())
    }
}
