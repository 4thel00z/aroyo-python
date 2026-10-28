//! Python bindings for aroyo, compiled as the extension module `aroyo._aroyo`.
//!
//! Every call that touches the network releases the GIL and runs on one shared
//! tokio runtime. Core errors map to Python exceptions in [`to_py`] only.

use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use object_store::aws::AmazonS3;
use object_store::ObjectStore;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyTuple};
use pyo3_arrow::input::AnyRecordBatch;
use pyo3_arrow::{PyRecordBatchReader, PySchema};
use tokio::runtime::{Builder, Handle, Runtime};

use aroyo_core::upload::UploadLimits;
use aroyo_core::{ParquetStream, ParquetStreamWriter as CoreWriter, ReadOptions, WriterOptions};

create_exception!(
    aroyo,
    AroyoError,
    PyException,
    "Raised for any storage, Parquet or checkpoint-state error."
);

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn runtime() -> PyResult<&'static Runtime> {
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime);
    }
    let built = Builder::new_multi_thread()
        .enable_all()
        .thread_name("aroyo-io")
        .build()
        .map_err(|e| AroyoError::new_err(format!("tokio runtime: {e}")))?;
    Ok(RUNTIME.get_or_init(|| built))
}

/// Maps every core error to a Python exception.
fn to_py(err: aroyo_core::Error) -> PyErr {
    match err {
        aroyo_core::Error::InvalidArgument(message) => PyValueError::new_err(message),
        other => AroyoError::new_err(other.to_string()),
    }
}

fn parse_compression(name: &str) -> PyResult<parquet::basic::Compression> {
    use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
    let compression = match name.to_ascii_lowercase().as_str() {
        "uncompressed" | "none" => Compression::UNCOMPRESSED,
        "snappy" => Compression::SNAPPY,
        "zstd" => Compression::ZSTD(ZstdLevel::default()),
        "lz4" | "lz4_raw" => Compression::LZ4_RAW,
        "gzip" => Compression::GZIP(GzipLevel::default()),
        "brotli" => Compression::BROTLI(BrotliLevel::default()),
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown compression {other:?}; use uncompressed, snappy, zstd, lz4, gzip or brotli"
            )))
        }
    };
    Ok(compression)
}

/// Connection settings for one S3-compatible endpoint.
#[pyclass(frozen, module = "aroyo")]
#[derive(Clone)]
struct S3Config {
    inner: aroyo_core::S3Config,
}

impl S3Config {
    fn store(&self) -> PyResult<Arc<AmazonS3>> {
        self.inner.build().map_err(to_py)
    }

    #[allow(clippy::too_many_arguments)]
    fn assemble(
        read_env: bool,
        bucket: Option<String>,
        region: Option<String>,
        endpoint: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        session_token: Option<String>,
        allow_http: Option<bool>,
        virtual_hosted_style: Option<bool>,
    ) -> Self {
        Self {
            inner: aroyo_core::S3Config {
                read_env,
                bucket,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
                session_token,
                allow_http,
                virtual_hosted_style,
            },
        }
    }
}

#[pymethods]
impl S3Config {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (*, read_env=false, bucket=None, region=None, endpoint=None, access_key_id=None, secret_access_key=None, session_token=None, allow_http=None, virtual_hosted_style=None))]
    fn new(
        read_env: bool,
        bucket: Option<String>,
        region: Option<String>,
        endpoint: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        session_token: Option<String>,
        allow_http: Option<bool>,
        virtual_hosted_style: Option<bool>,
    ) -> Self {
        Self::assemble(
            read_env,
            bucket,
            region,
            endpoint,
            access_key_id,
            secret_access_key,
            session_token,
            allow_http,
            virtual_hosted_style,
        )
    }

    /// Reads `AWS_*` environment variables; keyword arguments override them.
    #[staticmethod]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (*, bucket=None, region=None, endpoint=None, access_key_id=None, secret_access_key=None, session_token=None, allow_http=None, virtual_hosted_style=None))]
    fn from_env(
        bucket: Option<String>,
        region: Option<String>,
        endpoint: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        session_token: Option<String>,
        allow_http: Option<bool>,
        virtual_hosted_style: Option<bool>,
    ) -> Self {
        Self::assemble(
            true,
            bucket,
            region,
            endpoint,
            access_key_id,
            secret_access_key,
            session_token,
            allow_http,
            virtual_hosted_style,
        )
    }

    #[getter]
    fn bucket(&self) -> Option<String> {
        self.inner.bucket.clone()
    }

    #[getter]
    fn region(&self) -> Option<String> {
        self.inner.region.clone()
    }

    #[getter]
    fn endpoint(&self) -> Option<String> {
        self.inner.endpoint.clone()
    }

    /// Pickle support: rebuilds the config through the constructor, secrets included.
    fn __getnewargs_ex__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyTuple>, Bound<'py, PyDict>)> {
        let kwargs = PyDict::new(py);
        kwargs.set_item("read_env", self.inner.read_env)?;
        kwargs.set_item("bucket", &self.inner.bucket)?;
        kwargs.set_item("region", &self.inner.region)?;
        kwargs.set_item("endpoint", &self.inner.endpoint)?;
        kwargs.set_item("access_key_id", &self.inner.access_key_id)?;
        kwargs.set_item("secret_access_key", &self.inner.secret_access_key)?;
        kwargs.set_item("session_token", &self.inner.session_token)?;
        kwargs.set_item("allow_http", self.inner.allow_http)?;
        kwargs.set_item("virtual_hosted_style", self.inner.virtual_hosted_style)?;
        Ok((PyTuple::empty(py), kwargs))
    }

    fn __repr__(&self) -> String {
        format!(
            "S3Config(bucket={:?}, region={:?}, endpoint={:?}, read_env={})",
            self.inner.bucket, self.inner.region, self.inner.endpoint, self.inner.read_env
        )
    }
}

/// Checkpoint state of one upload; see `WriterState.to_bytes`.
#[pyclass(frozen, module = "aroyo")]
#[derive(Clone)]
struct WriterState {
    inner: aroyo_core::WriterState,
}

#[pymethods]
impl WriterState {
    #[getter]
    fn path(&self) -> String {
        self.inner.path.clone()
    }

    #[getter]
    fn upload_id(&self) -> String {
        self.inner.upload_id.clone()
    }

    #[getter]
    fn part_size(&self) -> u64 {
        self.inner.part_size
    }

    /// `(part_number, etag)` for every acknowledged part, in order.
    #[getter]
    fn parts(&self) -> Vec<(u32, String)> {
        self.inner
            .parts
            .iter()
            .map(|part| (part.number, part.etag.clone()))
            .collect()
    }

    #[getter]
    fn uploaded_bytes(&self) -> u64 {
        self.inner.uploaded_bytes
    }

    /// Rows the finished file will hold.
    #[getter]
    fn rows(&self) -> u64 {
        self.inner.rows
    }

    /// Size of the object once finished.
    #[getter]
    fn expected_size(&self) -> u64 {
        self.inner.expected_size()
    }

    #[getter]
    fn trailing_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.inner.trailing_bytes)
    }

    /// True after `close`: only the completion is pending.
    #[getter]
    fn is_pending_commit(&self) -> bool {
        self.inner.is_pending_commit()
    }

    fn to_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = self.inner.to_bytes().map_err(to_py)?;
        Ok(PyBytes::new(py, &bytes))
    }

    #[staticmethod]
    fn from_bytes(data: &Bound<'_, PyBytes>) -> PyResult<Self> {
        let inner = aroyo_core::WriterState::from_bytes(data.as_bytes()).map_err(to_py)?;
        Ok(Self { inner })
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.inner == other.inner
    }

    fn __repr__(&self) -> String {
        format!(
            "WriterState(path={:?}, upload_id={:?}, parts={}, uploaded_bytes={}, trailing_bytes={}, rows={})",
            self.inner.path,
            self.inner.upload_id,
            self.inner.parts.len(),
            self.inner.uploaded_bytes,
            self.inner.trailing_bytes.len(),
            self.inner.rows
        )
    }
}

/// A closed file whose upload still has to be completed.
#[pyclass(frozen, module = "aroyo")]
struct PendingCommit {
    state: aroyo_core::WriterState,
    store: Arc<AmazonS3>,
}

#[pymethods]
impl PendingCommit {
    /// Completes the multipart upload. Safe to call again after a failure.
    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        let runtime = runtime()?;
        let store = Arc::clone(&self.store);
        let state = self.state.clone();
        py.detach(|| {
            runtime.block_on(aroyo_core::finish(
                store.as_ref(),
                &state,
                &UploadLimits::S3,
            ))
        })
        .map_err(to_py)
    }

    #[getter]
    fn state(&self) -> WriterState {
        WriterState {
            inner: self.state.clone(),
        }
    }

    #[getter]
    fn path(&self) -> String {
        self.state.path.clone()
    }

    #[getter]
    fn rows(&self) -> u64 {
        self.state.rows
    }

    #[getter]
    fn expected_size(&self) -> u64 {
        self.state.expected_size()
    }

    /// The same blob as `WriterState.to_bytes`; load it with `WriterState.from_bytes`.
    fn to_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = self.state.to_bytes().map_err(to_py)?;
        Ok(PyBytes::new(py, &bytes))
    }

    fn __repr__(&self) -> String {
        format!(
            "PendingCommit(path={:?}, parts={}, rows={})",
            self.state.path,
            self.state.parts.len(),
            self.state.rows
        )
    }
}

/// Streams record batches into one Parquet multipart upload.
#[pyclass(module = "aroyo")]
struct ParquetStreamWriter {
    inner: Mutex<Option<CoreWriter<AmazonS3>>>,
    store: Arc<AmazonS3>,
}

impl ParquetStreamWriter {
    fn with_writer<T>(
        &self,
        f: impl FnOnce(&mut CoreWriter<AmazonS3>) -> aroyo_core::Result<T>,
    ) -> PyResult<T> {
        let mut guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let writer = guard
            .as_mut()
            .ok_or_else(|| AroyoError::new_err("writer is closed"))?;
        f(writer).map_err(to_py)
    }

    fn take_writer(&self) -> PyResult<CoreWriter<AmazonS3>> {
        let mut guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        guard
            .take()
            .ok_or_else(|| AroyoError::new_err("writer is closed"))
    }
}

#[pymethods]
impl ParquetStreamWriter {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (config, path, schema, *, part_size=16 * 1024 * 1024, row_group_size=128 * 1024 * 1024, max_row_group_rows=1024 * 1024, max_in_flight_parts=4, compression="snappy"))]
    fn new(
        py: Python<'_>,
        config: &S3Config,
        path: String,
        schema: PySchema,
        part_size: u64,
        row_group_size: usize,
        max_row_group_rows: usize,
        max_in_flight_parts: usize,
        compression: &str,
    ) -> PyResult<Self> {
        let store = config.store()?;
        let options = WriterOptions {
            part_size,
            row_group_size,
            max_row_group_rows,
            max_in_flight_parts,
            compression: parse_compression(compression)?,
            limits: UploadLimits::S3,
        };
        let runtime = runtime()?;
        let schema = schema.into_inner();
        let created = Arc::clone(&store);
        let writer = py
            .detach(|| {
                runtime.block_on(CoreWriter::create(
                    created,
                    &path,
                    schema,
                    options,
                    runtime.handle().clone(),
                ))
            })
            .map_err(to_py)?;
        Ok(Self {
            inner: Mutex::new(Some(writer)),
            store,
        })
    }

    /// Writes a record batch, a table or a record-batch stream.
    fn write(&self, py: Python<'_>, data: AnyRecordBatch) -> PyResult<()> {
        let runtime = runtime()?;
        let reader = data.into_reader()?;
        py.detach(|| {
            self.with_writer(|writer| {
                runtime.block_on(async {
                    for batch in reader {
                        writer.write(batch?).await?;
                    }
                    Ok(())
                })
            })
        })
    }

    /// Closes the row group in progress, waits for the parts in flight and
    /// returns the state that finishes this file from here. The upload stays open.
    fn checkpoint(&self, py: Python<'_>) -> PyResult<WriterState> {
        let runtime = runtime()?;
        let inner =
            py.detach(|| self.with_writer(|writer| runtime.block_on(writer.checkpoint())))?;
        Ok(WriterState { inner })
    }

    /// Writes the final footer as the last part. Completing is left to `PendingCommit.commit`.
    fn close(&self, py: Python<'_>) -> PyResult<PendingCommit> {
        let runtime = runtime()?;
        let writer = self.take_writer()?;
        let state = py
            .detach(|| runtime.block_on(writer.close()))
            .map_err(to_py)?;
        Ok(PendingCommit {
            state,
            store: Arc::clone(&self.store),
        })
    }

    /// Aborts the upload and discards every part.
    fn abort(&self, py: Python<'_>) -> PyResult<()> {
        let runtime = runtime()?;
        let writer = self.take_writer()?;
        py.detach(|| runtime.block_on(writer.abort()))
            .map_err(to_py)
    }

    #[getter]
    fn path(&self) -> PyResult<String> {
        self.with_writer(|writer| Ok(writer.path().to_string()))
    }

    #[getter]
    fn upload_id(&self) -> PyResult<String> {
        self.with_writer(|writer| Ok(writer.upload_id().to_string()))
    }

    /// Rows in closed row groups.
    #[getter]
    fn rows_written(&self) -> PyResult<u64> {
        self.with_writer(|writer| Ok(writer.rows_written()))
    }

    /// Rows buffered for the row group in progress.
    #[getter]
    fn rows_pending(&self) -> PyResult<usize> {
        self.with_writer(|writer| Ok(writer.rows_pending()))
    }

    /// Encoded Parquet bytes produced so far.
    #[getter]
    fn bytes_written(&self) -> PyResult<u64> {
        self.with_writer(|writer| Ok(writer.bytes_written()))
    }

    /// Parts uploaded or uploading.
    #[getter]
    fn part_count(&self) -> PyResult<usize> {
        self.with_writer(|writer| Ok(writer.part_count()))
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Aborts the upload when the block exits with an exception and the writer is still open.
    #[pyo3(signature = (exc_type, exc_value, traceback))]
    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Option<&Bound<'_, PyAny>>,
        exc_value: Option<&Bound<'_, PyAny>>,
        traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let _ = (exc_value, traceback);
        let still_open = self
            .inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some();
        if exc_type.is_some() && still_open {
            self.abort(py)?;
        }
        Ok(false)
    }
}

/// Uploads the trailing bytes of `state` and completes the upload.
///
/// Call it from a new process after a crash, with the last recorded state. The
/// writer that produced the state must be stopped first. Safe to call again:
/// an object of the expected size counts as success.
#[pyfunction]
fn finish(py: Python<'_>, config: &S3Config, state: &WriterState) -> PyResult<()> {
    let store = config.store()?;
    let runtime = runtime()?;
    let state = state.inner.clone();
    py.detach(|| {
        runtime.block_on(aroyo_core::finish(
            store.as_ref(),
            &state,
            &UploadLimits::S3,
        ))
    })
    .map_err(to_py)
}

/// Aborts the upload described by `state`, discarding every part.
#[pyfunction]
fn abort(py: Python<'_>, config: &S3Config, state: &WriterState) -> PyResult<()> {
    let store = config.store()?;
    let runtime = runtime()?;
    let state = state.inner.clone();
    py.detach(|| runtime.block_on(aroyo_core::abort(store.as_ref(), &state)))
        .map_err(to_py)
}

struct BlockingBatchReader {
    handle: Handle,
    stream: ParquetStream,
}

impl Iterator for BlockingBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.handle
            .block_on(self.stream.next())
            .map(|item| item.map_err(|e| ArrowError::ExternalError(Box::new(e))))
    }
}

impl RecordBatchReader for BlockingBatchReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(self.stream.schema())
    }
}

/// Opens a Parquet object and returns a record-batch stream (`__arrow_c_stream__`).
///
/// `concurrency` row groups are fetched and decoded ahead of the consumer.
/// Columns come back in file order whatever the order of `columns`.
#[pyfunction]
#[pyo3(signature = (config, path, *, columns=None, concurrency=4, batch_size=64 * 1024))]
fn read_parquet(
    py: Python<'_>,
    config: &S3Config,
    path: String,
    columns: Option<Vec<String>>,
    concurrency: usize,
    batch_size: usize,
) -> PyResult<PyRecordBatchReader> {
    let store: Arc<dyn ObjectStore> = config.store()?;
    let runtime = runtime()?;
    let options = ReadOptions {
        columns,
        concurrency,
        batch_size,
        ..ReadOptions::default()
    };
    let handle = runtime.handle().clone();
    let stream = py
        .detach(|| runtime.block_on(aroyo_core::open(store, &path, options, handle)))
        .map_err(to_py)?;
    let reader = BlockingBatchReader {
        handle: runtime.handle().clone(),
        stream,
    };
    Ok(PyRecordBatchReader::new(Box::new(reader)))
}

#[pymodule]
fn _aroyo(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("AroyoError", m.py().get_type::<AroyoError>())?;
    m.add_class::<S3Config>()?;
    m.add_class::<WriterState>()?;
    m.add_class::<PendingCommit>()?;
    m.add_class::<ParquetStreamWriter>()?;
    m.add_function(wrap_pyfunction!(finish, m)?)?;
    m.add_function(wrap_pyfunction!(abort, m)?)?;
    m.add_function(wrap_pyfunction!(read_parquet, m)?)?;
    Ok(())
}
