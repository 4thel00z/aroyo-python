//! One multipart upload: part numbering, bounded concurrent part uploads, finish and abort.

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{MultipartId, ObjectStore, ObjectStoreExt};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use crate::error::{Error, Result};
use crate::state::{UploadedPart, WriterState};

/// Smallest part S3 accepts, except for the last one.
pub const S3_MIN_PART_SIZE: u64 = 5 * 1024 * 1024;
/// Largest part S3 accepts.
pub const S3_MAX_PART_SIZE: u64 = 5 * 1024 * 1024 * 1024;
/// Most parts one S3 upload may hold.
pub const S3_MAX_PARTS: u32 = 10_000;

/// A store that supports both plain object access and the low-level multipart API.
pub trait Store: MultipartStore + ObjectStore {}

impl<T: MultipartStore + ObjectStore> Store for T {}

/// Part size and part count limits of the target store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UploadLimits {
    pub min_part_size: u64,
    pub max_part_size: u64,
    pub max_parts: u32,
}

impl UploadLimits {
    /// The limits Amazon S3 documents; S3-compatible stores match them.
    pub const S3: Self = Self {
        min_part_size: S3_MIN_PART_SIZE,
        max_part_size: S3_MAX_PART_SIZE,
        max_parts: S3_MAX_PARTS,
    };

    /// Rejects a part size the store would refuse.
    pub fn check_part_size(&self, part_size: u64) -> Result<()> {
        if part_size < self.min_part_size || part_size > self.max_part_size {
            return Err(Error::InvalidArgument(format!(
                "part_size {part_size} is outside the allowed range {}..={}",
                self.min_part_size, self.max_part_size
            )));
        }
        Ok(())
    }
}

impl Default for UploadLimits {
    fn default() -> Self {
        Self::S3
    }
}

struct InFlight {
    number: u32,
    len: u64,
    task: JoinHandle<object_store::Result<PartId>>,
}

/// An open multipart upload with at most `max_in_flight` parts uploading at once.
pub struct Upload<S> {
    store: Arc<S>,
    path: Path,
    upload_id: MultipartId,
    part_size: u64,
    limits: UploadLimits,
    max_in_flight: usize,
    parts: Vec<UploadedPart>,
    uploaded_bytes: u64,
    in_flight: VecDeque<InFlight>,
    handle: Handle,
}

impl<S: Store> Upload<S> {
    /// Starts a new multipart upload at `path`.
    pub async fn create(
        store: Arc<S>,
        path: Path,
        part_size: u64,
        limits: UploadLimits,
        max_in_flight: usize,
        handle: Handle,
    ) -> Result<Self> {
        limits.check_part_size(part_size)?;
        if max_in_flight == 0 {
            return Err(Error::InvalidArgument(
                "max_in_flight_parts must be at least 1".to_string(),
            ));
        }
        let upload_id = store.create_multipart(&path).await?;
        Ok(Self {
            store,
            path,
            upload_id,
            part_size,
            limits,
            max_in_flight,
            parts: Vec::new(),
            uploaded_bytes: 0,
            in_flight: VecDeque::new(),
            handle,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn upload_id(&self) -> &str {
        &self.upload_id
    }

    /// Parts acknowledged plus parts still uploading.
    pub fn part_count(&self) -> usize {
        self.parts.len() + self.in_flight.len()
    }

    /// Bytes in acknowledged parts.
    pub fn uploaded_bytes(&self) -> u64 {
        self.uploaded_bytes
    }

    /// Uploads `data` as the next part in the background. Waits for the oldest
    /// part when `max_in_flight` parts are already uploading.
    pub async fn put_part(&mut self, data: Bytes) -> Result<()> {
        let number = self.next_part_number()?;
        if self.in_flight.len() >= self.max_in_flight {
            self.settle_oldest().await?;
        }
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        let upload_id = self.upload_id.clone();
        let len = data.len() as u64;
        let task = self.handle.spawn(async move {
            store
                .put_part(&path, &upload_id, (number - 1) as usize, data.into())
                .await
        });
        self.in_flight.push_back(InFlight { number, len, task });
        Ok(())
    }

    /// Waits until every part in flight is acknowledged.
    pub async fn drain(&mut self) -> Result<()> {
        while !self.in_flight.is_empty() {
            self.settle_oldest().await?;
        }
        Ok(())
    }

    /// The checkpoint state once every part is acknowledged.
    pub fn state(&self, trailing_bytes: Vec<u8>, rows: u64) -> Result<WriterState> {
        if !self.in_flight.is_empty() {
            return Err(Error::InvalidState(
                "parts are still uploading; drain before taking the state".to_string(),
            ));
        }
        Ok(WriterState {
            path: self.path.to_string(),
            upload_id: self.upload_id.clone(),
            part_size: self.part_size,
            parts: self.parts.clone(),
            uploaded_bytes: self.uploaded_bytes,
            trailing_bytes,
            rows,
        })
    }

    /// Aborts the upload; parts in flight are abandoned.
    pub async fn abort(mut self) -> Result<()> {
        for part in self.in_flight.drain(..) {
            part.task.abort();
        }
        self.store
            .abort_multipart(&self.path, &self.upload_id)
            .await?;
        Ok(())
    }

    fn next_part_number(&self) -> Result<u32> {
        let number = self.part_count() as u32 + 1;
        if number > self.limits.max_parts {
            return Err(Error::PartLimit {
                path: self.path.to_string(),
                upload_id: self.upload_id.clone(),
                part_number: number,
                max_parts: self.limits.max_parts,
            });
        }
        Ok(number)
    }

    async fn settle_oldest(&mut self) -> Result<()> {
        let Some(oldest) = self.in_flight.pop_front() else {
            return Ok(());
        };
        let part_id = oldest.task.await??;
        self.parts.push(UploadedPart {
            number: oldest.number,
            etag: part_id.content_id,
        });
        self.uploaded_bytes += oldest.len;
        Ok(())
    }
}

/// Uploads the trailing bytes as the last part(s) and completes the upload.
///
/// Safe to call again after a crash or a partial failure: when completing fails,
/// an object at `path` whose size equals the expected size counts as success.
pub async fn finish<S: Store>(store: &S, state: &WriterState, limits: &UploadLimits) -> Result<()> {
    state.validate()?;
    let path = Path::parse(&state.path)?;
    match complete(store, &path, state, limits).await {
        Ok(()) => Ok(()),
        Err(cause) => validate_already_there(store, &path, state.expected_size(), cause).await,
    }
}

/// Aborts the upload described by `state`. Parts already uploaded are discarded.
pub async fn abort<S: Store>(store: &S, state: &WriterState) -> Result<()> {
    let path = Path::parse(&state.path)?;
    store.abort_multipart(&path, &state.upload_id).await?;
    Ok(())
}

async fn complete<S: Store>(
    store: &S,
    path: &Path,
    state: &WriterState,
    limits: &UploadLimits,
) -> Result<()> {
    let mut parts = state.parts.clone();
    let trailing = Bytes::from(state.trailing_bytes.clone());
    let chunk_len = state.part_size as usize;
    let mut offset = 0;
    while offset < trailing.len() {
        let end = (offset + chunk_len).min(trailing.len());
        let number = parts.len() as u32 + 1;
        if number > limits.max_parts {
            return Err(Error::PartLimit {
                path: state.path.clone(),
                upload_id: state.upload_id.clone(),
                part_number: number,
                max_parts: limits.max_parts,
            });
        }
        let part_id = store
            .put_part(
                path,
                &state.upload_id,
                (number - 1) as usize,
                trailing.slice(offset..end).into(),
            )
            .await?;
        parts.push(UploadedPart {
            number,
            etag: part_id.content_id,
        });
        offset = end;
    }
    let part_ids = parts
        .into_iter()
        .map(|part| PartId {
            content_id: part.etag,
        })
        .collect();
    store
        .complete_multipart(path, &state.upload_id, part_ids)
        .await?;
    Ok(())
}

async fn validate_already_there<S: ObjectStore>(
    store: &S,
    path: &Path,
    expected: u64,
    cause: Error,
) -> Result<()> {
    let Ok(meta) = store.head(path).await else {
        return Err(cause);
    };
    if meta.size == expected {
        return Ok(());
    }
    Err(Error::SizeMismatch {
        path: path.to_string(),
        expected,
        actual: meta.size,
        cause: cause.to_string(),
    })
}
