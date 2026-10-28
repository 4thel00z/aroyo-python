//! End-to-end tests against the in-memory object store.
#![cfg(test)]

use std::sync::Arc;

use arrow_array::builder::{Int64Builder, ListBuilder};
use arrow_array::{
    ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use arrow_select::concat::concat_batches;
use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ARROW_SCHEMA_META_KEY;
use parquet::basic::Compression;
use tokio::runtime::Handle;

use aroyo_core::{
    finish, open, Error, ParquetStreamWriter, ReadOptions, UploadLimits, WriterOptions, WriterState,
};

const PART_SIZE: u64 = 64 * 1024;

fn limits() -> UploadLimits {
    UploadLimits {
        min_part_size: 1024,
        max_part_size: 1 << 30,
        max_parts: 10_000,
    }
}

fn options() -> WriterOptions {
    WriterOptions {
        part_size: PART_SIZE,
        row_group_size: 1 << 20,
        max_row_group_rows: 1000,
        max_in_flight_parts: 3,
        compression: Compression::UNCOMPRESSED,
        limits: limits(),
    }
}

fn schema() -> SchemaRef {
    let point = Fields::from(vec![
        Field::new("x", DataType::Int64, false),
        Field::new("y", DataType::Int64, true),
    ]);
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, true),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
        Field::new("point", DataType::Struct(point), false),
    ]))
}

fn noise(seed: i64) -> i64 {
    seed.wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407)
        >> 17
}

fn batch(start: i64, rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..rows as i64).map(|i| start + i).collect();
    let values: Vec<Option<f64>> = ids
        .iter()
        .map(|id| (id % 7 != 0).then_some(*id as f64 / 3.0))
        .collect();
    let names: Vec<String> = ids.iter().map(|id| format!("n{:x}", noise(*id))).collect();
    let timestamps: Vec<Option<i64>> = ids
        .iter()
        .map(|id| (id % 5 != 0).then_some(1_700_000_000_000_000 + id * 1_000_000))
        .collect();
    let mut tags = ListBuilder::new(Int64Builder::new());
    for id in &ids {
        for k in 0..(id % 4) {
            tags.values().append_value(noise(id + k));
        }
        tags.append(id % 9 != 0);
    }
    let point = StructArray::from(vec![
        (
            Arc::new(Field::new("x", DataType::Int64, false)),
            Arc::new(Int64Array::from(ids.clone())) as ArrayRef,
        ),
        (
            Arc::new(Field::new("y", DataType::Int64, true)),
            Arc::new(Int64Array::from(
                ids.iter()
                    .map(|id| (id % 3 != 0).then_some(-id))
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
    ]);
    let timestamps = TimestampMicrosecondArray::from(timestamps).with_timezone("UTC");
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(names)),
            Arc::new(timestamps),
            Arc::new(tags.finish()),
            Arc::new(point),
        ],
    )
    .unwrap()
}

async fn writer(store: &Arc<InMemory>, path: &str) -> ParquetStreamWriter<InMemory> {
    ParquetStreamWriter::create(
        Arc::clone(store),
        path,
        schema(),
        options(),
        Handle::current(),
    )
    .await
    .unwrap()
}

async fn object_bytes(store: &InMemory, path: &str) -> Bytes {
    store
        .get(&Path::from(path))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
}

fn read_all(bytes: Bytes) -> RecordBatch {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    let schema = builder.schema().clone();
    let batches: Vec<RecordBatch> = builder
        .with_batch_size(123)
        .build()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    concat_batches(&schema, &batches).unwrap()
}

fn expected(batches: &[RecordBatch], rows: usize) -> RecordBatch {
    concat_batches(&schema(), batches).unwrap().slice(0, rows)
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_footer_makes_a_valid_file_up_to_the_checkpoint() {
    let store = Arc::new(InMemory::new());
    let batches: Vec<RecordBatch> = (0..5).map(|i| batch(i * 700, 700)).collect();
    let mut writer = writer(&store, "events/a.parquet").await;
    for batch in &batches[..5] {
        writer.write(batch.clone()).await.unwrap();
    }
    let state = writer.checkpoint().await.unwrap();
    assert_eq!(state.rows, 3500);
    assert!(state.parts.len() >= 2, "parts: {:?}", state.parts);
    assert!(!state.trailing_bytes.is_empty());
    assert!(!state.is_pending_commit());
    assert_eq!(state.part_size, PART_SIZE);
    writer.write(batch(5000, 10)).await.unwrap();
    drop(writer);

    finish(store.as_ref(), &state, &limits()).await.unwrap();

    let bytes = object_bytes(&store, "events/a.parquet").await;
    assert_eq!(bytes.len() as u64, state.expected_size());
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
    let file_meta = builder.metadata().file_metadata();
    assert_eq!(file_meta.num_rows(), 3500);
    assert_eq!(builder.metadata().num_row_groups(), 4);
    let keys: Vec<&str> = file_meta
        .key_value_metadata()
        .unwrap()
        .iter()
        .map(|kv| kv.key.as_str())
        .collect();
    assert!(keys.contains(&ARROW_SCHEMA_META_KEY));
    assert!(file_meta.created_by().unwrap().starts_with("aroyo version"));
    assert_eq!(builder.schema().as_ref(), schema().as_ref());

    assert_eq!(read_all(bytes), expected(&batches, 3500));
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_after_a_checkpoint_continue_the_same_upload() {
    let store = Arc::new(InMemory::new());
    let batches: Vec<RecordBatch> = (0..6).map(|i| batch(i * 900, 900)).collect();
    let mut writer = writer(&store, "events/b.parquet").await;
    for batch in &batches[..2] {
        writer.write(batch.clone()).await.unwrap();
    }
    let first = writer.checkpoint().await.unwrap();
    for batch in &batches[2..] {
        writer.write(batch.clone()).await.unwrap();
    }
    let second = writer.checkpoint().await.unwrap();
    assert_eq!(first.upload_id, second.upload_id);
    assert_eq!(second.rows, 5400);
    assert!(second.parts.starts_with(&first.parts));
    assert!(second.parts.len() > first.parts.len());

    let pending = writer.close().await.unwrap();
    assert!(pending.is_pending_commit());
    assert!(pending.parts.starts_with(&second.parts));
    finish(store.as_ref(), &pending, &limits()).await.unwrap();

    let bytes = object_bytes(&store, "events/b.parquet").await;
    assert_eq!(bytes.len() as u64, pending.expected_size());
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
    let first_column = builder.metadata().row_group(0).column(0);
    assert!(
        first_column.offset_index_offset().is_some(),
        "a closed file keeps its page index"
    );
    assert_eq!(read_all(bytes), expected(&batches, 5400));
}

#[tokio::test(flavor = "multi_thread")]
async fn finish_is_idempotent() {
    let store = Arc::new(InMemory::new());
    let mut writer = writer(&store, "events/c.parquet").await;
    writer.write(batch(0, 2500)).await.unwrap();
    let state = writer.checkpoint().await.unwrap();
    drop(writer);
    finish(store.as_ref(), &state, &limits()).await.unwrap();
    finish(store.as_ref(), &state, &limits()).await.unwrap();
    let meta = store.head(&Path::from("events/c.parquet")).await.unwrap();
    assert_eq!(meta.size, state.expected_size());
}

#[tokio::test(flavor = "multi_thread")]
async fn finish_reports_a_size_mismatch_instead_of_a_silent_success() {
    let store = Arc::new(InMemory::new());
    let mut writer = writer(&store, "events/d.parquet").await;
    writer.write(batch(0, 2500)).await.unwrap();
    let state = writer.checkpoint().await.unwrap();
    drop(writer);
    finish(store.as_ref(), &state, &limits()).await.unwrap();
    let mut forged = state.clone();
    forged.trailing_bytes.truncate(10);
    let err = finish(store.as_ref(), &forged, &limits())
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::SizeMismatch { expected, actual, .. } if actual == state.expected_size() && expected == forged.expected_size()),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn reader_streams_row_groups_in_order_with_projection() {
    let store = Arc::new(InMemory::new());
    let batches: Vec<RecordBatch> = (0..7).map(|i| batch(i * 1000, 1000)).collect();
    let mut writer = writer(&store, "events/e.parquet").await;
    for batch in &batches {
        writer.write(batch.clone()).await.unwrap();
    }
    let pending = writer.close().await.unwrap();
    finish(store.as_ref(), &pending, &limits()).await.unwrap();

    let reader_store: Arc<dyn ObjectStore> = Arc::clone(&store) as Arc<dyn ObjectStore>;
    let options = ReadOptions {
        columns: Some(vec![
            "name".to_string(),
            "id".to_string(),
            "name".to_string(),
        ]),
        concurrency: 3,
        batch_size: 333,
        ..ReadOptions::default()
    };
    let mut stream = open(reader_store, "events/e.parquet", options, Handle::current())
        .await
        .unwrap();
    let names: Vec<&str> = stream
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(names, ["id", "name"]);
    let mut got = Vec::new();
    while let Some(item) = stream.next().await {
        got.push(item.unwrap());
    }
    let got = concat_batches(stream.schema(), &got).unwrap();
    let want = expected(&batches, 7000).project(&[0, 2]).unwrap();
    assert_eq!(got, want);

    let reader_store: Arc<dyn ObjectStore> = Arc::clone(&store) as Arc<dyn ObjectStore>;
    let err = open(
        reader_store,
        "events/e.parquet",
        ReadOptions {
            columns: Some(vec!["missing".to_string()]),
            ..ReadOptions::default()
        },
        Handle::current(),
    )
    .await
    .err()
    .unwrap();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn part_limit_is_refused_before_sending() {
    let store = Arc::new(InMemory::new());
    let mut options = options();
    options.limits.max_parts = 2;
    options.part_size = 1024;
    let mut writer = ParquetStreamWriter::create(
        Arc::clone(&store),
        "events/f.parquet",
        schema(),
        options,
        Handle::current(),
    )
    .await
    .unwrap();
    let err = writer.write(batch(0, 1000)).await.unwrap_err();
    assert!(
        matches!(
            err,
            Error::PartLimit {
                part_number: 3,
                max_parts: 2,
                ..
            }
        ),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn part_size_outside_the_store_limits_is_refused() {
    let store = Arc::new(InMemory::new());
    let mut options = options();
    options.limits = UploadLimits::S3;
    options.part_size = 1024;
    let err = ParquetStreamWriter::create(
        Arc::clone(&store),
        "events/g.parquet",
        schema(),
        options,
        Handle::current(),
    )
    .await
    .err()
    .unwrap();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn batches_that_do_not_fit_the_schema_are_refused() {
    let store = Arc::new(InMemory::new());
    let mut writer = writer(&store, "events/h.parquet").await;
    let other =
        RecordBatch::try_from_iter([("id", Arc::new(StringArray::from(vec!["x"])) as ArrayRef)])
            .unwrap();
    let err = writer.write(other).await.unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn state_round_trips_through_bytes() {
    let store = Arc::new(InMemory::new());
    let mut writer = writer(&store, "events/i.parquet").await;
    writer.write(batch(0, 1500)).await.unwrap();
    let state = writer.checkpoint().await.unwrap();
    let blob = state.to_bytes().unwrap();
    assert_eq!(WriterState::from_bytes(&blob).unwrap(), state);
    assert!(matches!(
        WriterState::from_bytes(b"nope").unwrap_err(),
        Error::StateEncoding(_)
    ));
    let mut gap = state.clone();
    gap.parts[0].number = 7;
    assert!(matches!(
        gap.validate().unwrap_err(),
        Error::InvalidState(_)
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_file_closes_and_finishes() {
    let store = Arc::new(InMemory::new());
    let writer = writer(&store, "events/j.parquet").await;
    let pending = writer.close().await.unwrap();
    finish(store.as_ref(), &pending, &limits()).await.unwrap();
    let got = read_all(object_bytes(&store, "events/j.parquet").await);
    assert_eq!(got.num_rows(), 0);
    assert_eq!(got.schema().as_ref(), schema().as_ref());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_checkpoint_before_any_write_still_finishes() {
    let store = Arc::new(InMemory::new());
    let mut writer = writer(&store, "events/k.parquet").await;
    let state = writer.checkpoint().await.unwrap();
    assert!(state.parts.is_empty());
    drop(writer);
    finish(store.as_ref(), &state, &limits()).await.unwrap();
    assert_eq!(
        read_all(object_bytes(&store, "events/k.parquet").await).num_rows(),
        0
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn abort_discards_the_upload() {
    let store = Arc::new(InMemory::new());
    let mut writer = writer(&store, "events/l.parquet").await;
    writer.write(batch(0, 2000)).await.unwrap();
    let state = writer.checkpoint().await.unwrap();
    writer.abort().await.unwrap();
    assert!(finish(store.as_ref(), &state, &limits()).await.is_err());
    assert!(store.head(&Path::from("events/l.parquet")).await.is_err());
}
