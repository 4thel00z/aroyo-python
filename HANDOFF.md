# Handoff: aroyo-python

Python bindings (Rust + PyO3) for streaming Arrow data to S3 and back, using the
crash-resumable multipart-upload method that Arroyo's filesystem sink uses.

This file is the starting point. It says what to build, why, which code to
copy from, and what is still unverified.

## 1. Goal

A Python package that:

1. **Writes** Arrow record batches to an S3-compatible store as Parquet, as one
   multipart upload per file. The writer can **checkpoint**: it returns a small
   serializable state. After a crash, a new process takes that state and
   finishes the same file. Nothing already uploaded is sent again.
2. **Reads** Parquet objects back as an Arrow stream, using parallel range GETs.
3. Needs **no conditional writes** (`If-Match` / `If-None-Match`). Some
   S3-compatible providers accept these headers and ignore them. Each file has
   exactly one writer, and the commit order replaces compare-and-set.

There is no Python library that does (1) today:

| Library | Streams a multipart upload | Resumes an upload after a crash |
|---|---|---|
| pyarrow `S3FileSystem.open_output_stream` | yes | no |
| obstore `put` with a file-like object / `open_writer` | yes | no |
| Arroyo | yes | yes, but only inside the Arroyo server (Rust, no Python client) |
| SlateDB / opendata Log and Buffer | n/a | rely on conditional writes |

## 2. Background: the method

Source: <https://www.arroyo.dev/blog/streaming-to-s3-is-hard/> (Arroyo, Jackson Newhouse).

- S3 objects cannot be appended to. A file that stays open across checkpoints
  must be a multipart upload (MPU).
- MPU limits: every part except the last is 5 MiB to 5 GiB. One upload has at
  most 10,000 parts. A part number can be uploaded again; only the last ETag
  for that number counts.
- At a checkpoint the writer saves:
  - `path`, `upload_id`
  - `[(part_number, etag)]` for the parts already uploaded
  - **trailing bytes**: the bytes not yet uploaded, followed by the Parquet
    footer for the row groups written so far. This is at most one part's
    worth of data plus the footer.
- Recovery: upload the trailing bytes as the final part(s), then complete the
  upload. The result is a valid Parquet file holding every row up to the
  checkpoint. New rows go into a new file.
- Completing an MPU cannot be undone. Arroyo completes it only in the second
  phase of a two-phase commit, after the checkpoint is recorded.
- If completing fails during recovery, the upload may already be complete
  from an earlier attempt. Arroyo then sends a `HEAD` and accepts the object
  if its size matches the expected size. See `validate_already_there` below.

## 3. Where the code is

Arroyo repo: <https://github.com/ArroyoSystems/arroyo>, read at commit
`90d978fe96589909c038b3545113e007d6eb3dec` (2026-10-27). Dual-licensed
MIT / Apache-2.0, so copying with attribution is allowed.

| What | File |
|---|---|
| Parquet buffering writer, `get_trailing_bytes_for_checkpoint`, `close` | `crates/arroyo-connectors/src/filesystem/sink/parquet.rs` |
| Init / part / last-parts / finalize futures, `validate_already_there` | `crates/arroyo-connectors/src/filesystem/sink/v2/uploads.rs` |
| Per-file state machine | `crates/arroyo-connectors/src/filesystem/sink/v2/open_file.rs` |
| Sink driver, checkpoint data types (`trailing_bytes`) | `crates/arroyo-connectors/src/filesystem/sink/v2/mod.rs`, `sink/mod.rs` |
| Two-phase commit | `crates/arroyo-connectors/src/filesystem/sink/two_phase_committer.rs` |
| `start_multipart` / `add_multipart` / `close_multipart` on `object_store::multipart::MultipartStore` | `crates/arroyo-storage/src/lib.rs` |
| JSON sink with the same checkpoint interface (useful for tests) | `crates/arroyo-connectors/src/filesystem/sink/json.rs` |

### The arrow-rs fork

Arroyo pins `parquet` to <https://github.com/ArroyoSystems/arrow-rs>, branch
`55.2.0/parquet`. On top of upstream 55.2.0 it adds three small commits:

- `102420c8b5` parquet: add method to get final bytes
- `d00f572b22` Don't mark file as closed when getting parquet metadata
- `d1d2dd8edf` Return FileMetaData with trailing bytes

Together they add:

```rust
// ArrowWriter
pub fn get_trailing_bytes(&mut self, target: W) -> Result<(W, FileMetaData)>

// SerializedFileWriter: swap self.buf for `target`, call write_metadata(false)
// (writes the footer without setting `finished`), swap back.
pub(crate) fn write_trailing_bytes(&mut self, target: W) -> Result<(W, FileMetaData)>
```

Arroyo calls `writer.flush()` (closes the current row group), then
`get_trailing_bytes(SharedBuffer::new(0))`. It then copies the unsent bytes
from its shared buffer and appends the returned footer bytes.

## 4. Decisions to make first

### 4.1 Footer bytes without a fork (try this first)

Maintaining a fork of `parquet` is the most expensive part of the design.
Upstream `parquet` (latest stable 60.0.0 on 2026-10-27) has
`ArrowWriter::flushed_row_groups()` and `ParquetMetaDataWriter`. Building
the footer from those two, without touching the writer, could replace the
fork. **Not verified.** Check:

- Row-group column-chunk offsets are absolute file offsets, so a footer built
  from them is valid at the current `bytes_written()`.
- Bloom filters, column indexes and offset indexes. Upstream `write_metadata`
  writes these out and takes them from the writer. A footer that is written
  more than once must not lose them. Arroyo's fork has the same problem, so
  check whether it turns them off. The simplest first version: disable bloom
  filters and the page index in `WriterProperties`.
- Round trip: write N batches, checkpoint, build the file from the uploaded
  parts plus the trailing bytes, read it with pyarrow, and compare row counts
  and values.

If this fails, vendor the three commits as a patch on a pinned `parquet`
version.

### 4.2 Access to the multipart upload ID

`ObjectStore::put_multipart` returns a `MultipartUpload` that does **not**
expose the upload ID, so it cannot resume. Use the lower-level
`object_store::multipart::MultipartStore` trait (`create_multipart`,
`put_part`, `complete_multipart`, `abort_multipart`), as Arroyo does. It is
implemented on the concrete `AmazonS3` type, not on `dyn ObjectStore`.

### 4.3 Accepting obstore stores from Python

`pyo3-object_store` (crate, 0.13.0) lets a PyO3 function accept a store
object created with `obstore`, so users configure credentials once. **Check
whether it gives you the concrete `AmazonS3`.** You need that for
`MultipartStore` (4.2). If it doesn't, build `AmazonS3` from your own
config, read with `AmazonS3Builder::from_env()` plus keyword overrides.

### 4.4 Arrow across the Python interface

Use `pyo3-arrow` (0.20.0). Input: anything that implements the Arrow
PyCapsule interface (`__arrow_c_stream__` / `__arrow_c_array__`), so pyarrow,
polars and others work without copying. Output: return an object that
implements `__arrow_c_stream__`, which `pyarrow.RecordBatchReader.from_stream`
can consume.

## 5. Proposed Python API (draft)

```python
from aroyo import S3Config, ParquetStreamWriter, PendingCommit, WriterState, read_parquet

config = S3Config.from_env()  # AWS_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_ALLOW_HTTP

writer = ParquetStreamWriter(
    config,
    path="events/part-0001.parquet",
    schema=schema,                  # anything with __arrow_c_schema__
    part_size=16 * 1024 * 1024,     # >= 5 MiB
    row_group_size=128 * 1024 * 1024,
)
writer.write(batch)                 # anything with __arrow_c_array__ / __arrow_c_stream__

# Checkpoint: the upload stays open. Closes the current row group, uploads
# full parts, returns upload_id + [(part, etag)] + trailing bytes.
state: WriterState = writer.checkpoint()
blob: bytes = state.to_bytes()      # caller stores this with its own checkpoint
writer.write(next_batch)            # same file, same upload

# Roll: write the real footer as the last part. The upload is not completed yet.
pending: PendingCommit = writer.close()
# ... caller records its checkpoint, including pending.to_bytes() ...
pending.commit()                    # completes the MPU (second phase of the two-phase commit)

# Another process, after a crash, from the last recorded checkpoint:
recovered = WriterState.from_bytes(blob)
ParquetStreamWriter.finish(config, recovered)  # uploads trailing bytes, completes; HEAD-size check if already done

reader = read_parquet(config, "events/part-0001.parquet", concurrency=16, columns=["a", "b"])
table = pyarrow.RecordBatchReader.from_stream(reader).read_all()
```

A file normally stays open across many checkpoints. Only a file that was
closed (rolled by size or time) before a checkpoint is completed in that
checkpoint's commit phase. Completing at every checkpoint would bring back
the small-file problem this method avoids. Every `checkpoint()` closes a row
group, so frequent checkpoints mean small row groups.

Async variants (`await writer.write(...)`) can come later; start
synchronous and release the GIL around I/O
(`py.allow_threads` / `Python::detach` in newer PyO3).

## 6. Build layout

Follow `4thel00z/pdfboss` (maturin + PyO3):

```
Cargo.toml                      # workspace
crates/aroyo-core/              # Rust: writer, state, reader (no PyO3)
crates/aroyo-py/                # PyO3 module, thin
python/aroyo/__init__.py        # re-exports, type stubs (.pyi)
tests/                          # pytest
pyproject.toml                  # maturin: python-source = "python", module-name = "aroyo._aroyo"
```

Current versions (crates.io / PyPI, 2026-10-27): `pyo3` 0.30.1,
`pyo3-arrow` 0.20.0, `pyo3-object_store` 0.13.0, `object_store` 0.14.2,
`parquet` 60.0.0. Check that `parquet` and `object_store` resolve to one
`object_store` version. Arroyo itself is on `object_store` 0.12.3 and
`parquet` 55.2.0.

## 7. Tests that prove it works

1. **Unit, no network:** use `object_store::memory::InMemory` for the reader,
   and a fake `MultipartStore` that records parts, for the writer.
2. **Crash recovery:** write, checkpoint, drop the writer without commit,
   `finish(state)` in a new process (`multiprocessing`). Assert the row count
   equals the rows written before the checkpoint, and that pyarrow reads the
   file without error.
3. **Double finish:** call `finish(state)` twice. The second call must succeed
   through the HEAD-size check.
4. **Part limits:** when the next part would be number 10,001, the writer
   rolls to a new file or raises a clear error. It never sends that part.
5. **Integration:** MinIO in Docker (CI). Also run against a provider that
   ignores conditional headers. Nothing here may depend on them.
6. **Throughput benchmark:** one payload (for example 2 GB of mixed
   int/float/string columns). Report write MB/s and read MB/s against
   pyarrow `open_output_stream` + `iter_batches` and obstore as baselines.
   Record the machine and the endpoint location with every number.

## 8. Known risks

- **No compare-and-set means the caller must stop old writers.** Arroyo's
  controller makes sure the old worker is dead before recovery runs. This
  library can't do that. Only one process may hold a given state, and the
  old writer must be stopped before `finish(state)` runs. Otherwise a writer
  that is still running can complete the upload with a different part list
  and race `finish`. One of them wins, and the other fails the HEAD size
  check. Document this as a rule for callers.

- Multipart uploads that are never completed or aborted keep their parts and
  cost storage. Document an `AbortIncompleteMultipartUpload` lifecycle rule,
  and give users an `abort(state)` call.
- An upload ID can expire on some providers if it stays open for days. Keep
  files short-lived (roll by size or time).
- The last part may be smaller than 5 MiB, but only the last one. Recovery
  can produce several final parts if the trailing bytes exceed `part_size`.
  Arroyo's `create_last_parts_upload_future` handles this case.
- Footer correctness (section 4.1) is the main technical risk. Do it first.
