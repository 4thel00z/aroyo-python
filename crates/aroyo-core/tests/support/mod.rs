//! In-memory store whose multipart uploads accept parts in any order.
//!
//! `object_store::memory::InMemory` 0.13 indexes past its part list when a part
//! arrives ahead of its predecessor, which concurrent uploads do.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartId, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

#[derive(Debug, Default)]
pub struct MemoryStore {
    inner: InMemory,
    uploads: Mutex<HashMap<MultipartId, BTreeMap<usize, Bytes>>>,
    next_upload: AtomicU64,
}

fn failure(message: String) -> Error {
    Error::Generic {
        store: "MemoryStore",
        source: message.into(),
    }
}

impl fmt::Display for MemoryStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MemoryStore")
    }
}

#[async_trait]
impl ObjectStore for MemoryStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[async_trait]
impl MultipartStore for MemoryStore {
    async fn create_multipart(&self, _path: &Path) -> Result<MultipartId> {
        let id = self.next_upload.fetch_add(1, Ordering::Relaxed).to_string();
        self.uploads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id.clone(), BTreeMap::new());
        Ok(id)
    }

    async fn put_part(
        &self,
        _path: &Path,
        id: &MultipartId,
        part_idx: usize,
        payload: PutPayload,
    ) -> Result<PartId> {
        let mut uploads = self.uploads.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(parts) = uploads.get_mut(id) else {
            return Err(failure(format!("unknown upload {id}")));
        };
        parts.insert(part_idx, Bytes::from(payload));
        Ok(PartId {
            content_id: format!("{id}-{part_idx}"),
        })
    }

    async fn complete_multipart(
        &self,
        path: &Path,
        id: &MultipartId,
        _parts: Vec<PartId>,
    ) -> Result<PutResult> {
        let parts = self
            .uploads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(id)
            .ok_or_else(|| failure(format!("unknown upload {id}")))?;
        let gap = parts
            .keys()
            .enumerate()
            .find(|(expected, idx)| expected != *idx);
        if let Some((expected, _)) = gap {
            return Err(failure(format!("upload {id} is missing part {expected}")));
        }
        let mut object = Vec::with_capacity(parts.values().map(Bytes::len).sum());
        for part in parts.values() {
            object.extend_from_slice(part);
        }
        self.inner
            .put_opts(path, PutPayload::from(object), PutOptions::default())
            .await
    }

    async fn abort_multipart(&self, _path: &Path, id: &MultipartId) -> Result<()> {
        self.uploads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(id)
            .map(|_| ())
            .ok_or_else(|| failure(format!("unknown upload {id}")))
    }
}
