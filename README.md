# aroyo-python

Stream Arrow data to S3-compatible object storage as Parquet, with checkpoints that
survive a crash, and read it back with parallel range requests. Rust core, Python
bindings.

The write path follows the multipart-upload method from Arroyo's filesystem sink
([Streaming to S3 is hard](https://www.arroyo.dev/blog/streaming-to-s3-is-hard/)). It
needs no conditional writes, so it works on S3-compatible stores that ignore
`If-Match` / `If-None-Match`.

## What it does

- One multipart upload per Parquet file. Row groups are encoded column-parallel and
  cut into fixed-size parts that upload in the background while the next batches
  are encoded.
- `checkpoint()` returns a small state: the parts acknowledged so far and the
  *trailing bytes* (bytes not yet uploaded plus a Parquet footer for the row groups
  written so far). Nothing already uploaded is sent again.
- `finish(config, state)` runs in any process. It uploads the trailing bytes as the
  last part(s) and completes the upload. The result is a valid Parquet file holding
  every row up to that checkpoint. Calling it twice is safe: an object of the
  expected size counts as success.
- `close()` writes the real footer as the last part and returns a `PendingCommit`;
  `commit()` completes the upload. That split is the second phase of a two-phase
  commit: record the pending state first, then commit.
- `read_parquet` streams a file back through the Arrow C stream interface, fetching
  and decoding several row groups ahead of the consumer.

## Install

```sh
pip install aroyo
```

Python 3.12+. Wheels are built with maturin; building from source needs a Rust
toolchain.

## Quickstart

```python
import pyarrow as pa
import aroyo

config = aroyo.S3Config.from_env(bucket="events")   # AWS_ENDPOINT, AWS_ACCESS_KEY_ID, ...

writer = aroyo.ParquetStreamWriter(
    config,
    "events/part-0001.parquet",
    schema,                            # anything with __arrow_c_schema__
    part_size=16 * 1024 * 1024,        # 5 MiB .. 5 GiB
    row_group_size=128 * 1024 * 1024,  # buffered Arrow bytes that close a row group
)
writer.write(batch)                    # RecordBatch, Table, RecordBatchReader, polars ...

state = writer.checkpoint()            # closes the row group, waits for parts in flight
blob = state.to_bytes()                # store this with your own checkpoint
writer.write(next_batch)               # same file, same upload

pending = writer.close()               # real footer uploaded, upload not yet completed
record(pending.to_bytes())             # your durable checkpoint
pending.commit()                       # completes the multipart upload

# Another process, after a crash, from the last recorded blob:
aroyo.finish(config, aroyo.WriterState.from_bytes(blob))

reader = aroyo.read_parquet(config, "events/part-0001.parquet", columns=["a", "b"], concurrency=8)
table = pa.RecordBatchReader.from_stream(reader).read_all()
```

## Rules for callers

- **One writer per state.** There is no compare-and-set. Stop the old writer before
  `finish(state)` runs in a new process. A writer that is still running can complete
  the upload with a different part list; one of the two wins and the other fails the
  size check with `AroyoError`.
- **Abandoned uploads cost storage.** Configure an `AbortIncompleteMultipartUpload`
  lifecycle rule on the bucket, and call `aroyo.abort(config, state)` or
  `writer.abort()` for uploads you give up on. The writer aborts its upload when a
  `with` block exits with an exception.
- **Keep files short-lived.** Some providers expire upload IDs after days. Roll files
  by size or time. A file may hold at most 10,000 parts; the writer raises before it
  would send part 10,001.
- Every `checkpoint()` closes a row group, so frequent checkpoints mean small row
  groups.

## Options

| `ParquetStreamWriter` | default | meaning |
|---|---|---|
| `part_size` | 16 MiB | bytes per part; every part except the last has exactly this size |
| `row_group_size` | 128 MiB | buffered Arrow bytes that close a row group |
| `max_row_group_rows` | 1,048,576 | rows that close a row group |
| `max_in_flight_parts` | 4 | parts uploading at once before `write` waits |
| `compression` | `"snappy"` | `uncompressed`, `snappy`, `zstd`, `lz4`, `gzip`, `brotli` |

| `read_parquet` | default | meaning |
|---|---|---|
| `columns` | all | top-level columns; returned in file order |
| `concurrency` | 4 | row groups fetched and decoded ahead of the consumer |
| `batch_size` | 65,536 | rows per returned batch |

Memory for a read is about `concurrency` row groups' worth of compressed bytes plus
a couple of decoded batches per row group.

## Design notes

**Footer without a fork.** Arroyo patches `parquet` to write a footer without closing
the writer. This crate builds the same footer from upstream
`SerializedFileWriter::flushed_row_groups()` with `ParquetMetaDataWriter`. Column
chunk offsets in row-group metadata are absolute file offsets, so the footer is valid
at the current `bytes_written()`. The upstream writer assigns page-index offsets only
in the final footer, so a checkpoint footer carries no page index while a file closed
with `close()` keeps it. Both are valid Parquet; the Rust tests read recovered files
back with the Arrow schema and nested columns intact.

**Upload ID access.** `ObjectStore::put_multipart` hides the upload ID, so the crate
uses `object_store::multipart::MultipartStore` on the concrete `AmazonS3`. Stores are
built from `S3Config`; accepting an `obstore` store object is not supported yet.

**Throughput.** Batches are buffered until a row group is full, then every leaf
column is encoded on its own rayon task. The encoded bytes are sliced into parts
without copying and uploaded on a tokio runtime with bounded concurrency. The GIL is
released around every network call. Reads fetch one coalesced range request per row
group and decode on the blocking pool, `concurrency` row groups ahead.

**Versions.** `pyo3-arrow` 0.20 pins `arrow` 59, so the crate uses `parquet` 59 and
`object_store` 0.13, one version of each.

## Development

```sh
uv venv && source .venv/bin/activate
uv pip install maturin pytest pyarrow "moto[server]"
make develop     # maturin develop
make ci          # cargo fmt --check, clippy -D warnings, cargo test
make test-py     # pytest against an in-process moto S3
```

Set `AROYO_TEST_ENDPOINT`, `AROYO_TEST_BUCKET` and the `AWS_*` variables to run
pytest against MinIO or another store instead of moto. `make minio` starts one.

`benchmarks/bench_throughput.py` compares write and read MiB/s against pyarrow's
`S3FileSystem` and obstore; it prints the machine and endpoint with every number.

## License

MIT OR Apache-2.0. The write method follows Arroyo (MIT OR Apache-2.0).
