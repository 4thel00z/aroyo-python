//! Reads a Parquet object back as Arrow batches with parallel range requests.

use std::ops::Range;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use futures::FutureExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use tokio::runtime::Handle;
use tokio::sync::mpsc::{self, Receiver};

use crate::error::{Error, Result};

/// Tuning knobs for [`open`].
#[derive(Clone, Debug)]
pub struct ReadOptions {
    /// Top-level columns to read, in file order. `None` reads every column.
    pub columns: Option<Vec<String>>,
    /// Row groups fetched and decoded at the same time.
    pub concurrency: usize,
    /// Rows per returned batch.
    pub batch_size: usize,
    /// Bytes fetched from the end of the object to parse the footer in one request.
    pub footer_size_hint: usize,
    /// Decoded batches buffered ahead per row group.
    pub prefetch_batches: usize,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            columns: None,
            concurrency: 4,
            batch_size: 64 * 1024,
            footer_size_hint: 512 * 1024,
            prefetch_batches: 2,
        }
    }
}

/// Batches of one object, delivered in file order.
///
/// Up to `concurrency` row groups are fetched and decoded ahead on the runtime
/// while the consumer drains the current one.
pub struct ParquetStream {
    schema: SchemaRef,
    row_groups: BoxStream<'static, Result<Receiver<Result<RecordBatch>>>>,
    current: Option<Receiver<Result<RecordBatch>>>,
}

impl ParquetStream {
    /// The schema after column projection.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// The next batch, or `None` after the last one.
    pub async fn next(&mut self) -> Option<Result<RecordBatch>> {
        loop {
            if let Some(current) = &mut self.current {
                if let Some(item) = current.recv().await {
                    return Some(item);
                }
                self.current = None;
            }
            match self.row_groups.next().await? {
                Ok(receiver) => self.current = Some(receiver),
                Err(err) => return Some(Err(err)),
            }
        }
    }
}

/// Opens `path` and starts fetching its row groups on `handle`.
pub async fn open(
    store: Arc<dyn ObjectStore>,
    path: &str,
    options: ReadOptions,
    handle: Handle,
) -> Result<ParquetStream> {
    if options.concurrency == 0 || options.batch_size == 0 || options.prefetch_batches == 0 {
        return Err(Error::InvalidArgument(
            "concurrency, batch_size and prefetch_batches must be positive".to_string(),
        ));
    }
    let path = Path::parse(path)?;
    let size = store.head(&path).await?.size;
    let mut reader = ObjectReader {
        store,
        path,
        size,
        footer_size_hint: options.footer_size_hint,
    };
    let metadata = ArrowReaderMetadata::load_async(&mut reader, ArrowReaderOptions::new()).await?;
    let (mask, schema) = projection(&metadata, options.columns.as_deref())?;
    let row_group_count = metadata.metadata().num_row_groups();
    let batch_size = options.batch_size;
    let prefetch = options.prefetch_batches;
    let spawner = handle.clone();
    let row_groups = futures::stream::iter(0..row_group_count)
        .map(move |index| {
            spawner.spawn(read_row_group(
                reader.clone(),
                metadata.clone(),
                mask.clone(),
                batch_size,
                prefetch,
                index,
            ))
        })
        .buffered(options.concurrency)
        .map(|joined| joined.map_err(Error::from).and_then(|result| result))
        .boxed();
    Ok(ParquetStream {
        schema,
        row_groups,
        current: None,
    })
}

async fn read_row_group(
    reader: ObjectReader,
    metadata: ArrowReaderMetadata,
    mask: ProjectionMask,
    batch_size: usize,
    prefetch: usize,
    index: usize,
) -> Result<Receiver<Result<RecordBatch>>> {
    let mut stream = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, metadata)
        .with_row_groups(vec![index])
        .with_projection(mask)
        .with_batch_size(batch_size)
        .build()?;
    let (sender, receiver) = mpsc::channel(prefetch);
    let Some(decoder) = stream.next_row_group().await? else {
        return Ok(receiver);
    };
    tokio::task::spawn_blocking(move || {
        for item in decoder {
            if sender.blocking_send(item.map_err(Error::from)).is_err() {
                return;
            }
        }
    });
    Ok(receiver)
}

/// The projection mask and the projected Arrow schema, both in file column order.
fn projection(
    metadata: &ArrowReaderMetadata,
    columns: Option<&[String]>,
) -> Result<(ProjectionMask, SchemaRef)> {
    let schema = metadata.schema();
    let Some(columns) = columns else {
        return Ok((ProjectionMask::all(), Arc::clone(schema)));
    };
    let mut indices = columns
        .iter()
        .map(|name| {
            schema
                .index_of(name)
                .map_err(|_| Error::InvalidArgument(format!("column {name:?} not found")))
        })
        .collect::<Result<Vec<_>>>()?;
    indices.sort_unstable();
    indices.dedup();
    let mask = ProjectionMask::roots(metadata.parquet_schema(), indices.iter().copied());
    let projected = Arc::new(schema.project(&indices)?);
    Ok((mask, projected))
}

#[derive(Clone)]
struct ObjectReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    size: u64,
    footer_size_hint: usize,
}

fn external(err: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(err))
}

impl AsyncFileReader for ObjectReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            self.store
                .get_range(&self.path, range)
                .await
                .map_err(external)
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        async move {
            self.store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(external)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let size = self.size;
            let hint = self.footer_size_hint.min(size as usize);
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .with_prefetch_hint(Some(hint))
                .load_and_finish(self, size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}
