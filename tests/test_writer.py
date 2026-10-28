"""Write, checkpoint, finish and read back through the Python API."""

import multiprocessing
import os
import pickle
from multiprocessing.connection import Connection

import pyarrow as pa
import pytest

import aroyo
from conftest import PART_SIZE, batch, schema, table

ROWS = 60_000


def read_table(config: aroyo.S3Config, path: str, **kwargs: object) -> pa.Table:
    return pa.RecordBatchReader.from_stream(aroyo.read_parquet(config, path, **kwargs)).read_all()


def writer(config: aroyo.S3Config, path: str, **kwargs: object) -> aroyo.ParquetStreamWriter:
    options: dict[str, object] = {
        "part_size": PART_SIZE,
        "max_row_group_rows": 50_000,
        "compression": "uncompressed",
    }
    options.update(kwargs)
    return aroyo.ParquetStreamWriter(config, path, schema(), **options)


def test_checkpoint_then_finish_keeps_rows_up_to_the_checkpoint(config: aroyo.S3Config, path: str) -> None:
    batches = [batch(i * ROWS, ROWS) for i in range(4)]
    stream = writer(config, path)
    for item in batches[:3]:
        stream.write(item)
    state = stream.checkpoint()
    assert state.rows == 3 * ROWS
    assert state.parts, state
    assert not state.is_pending_commit
    assert state.part_size == PART_SIZE
    assert stream.rows_written == 3 * ROWS
    assert stream.rows_pending == 0
    stream.write(batches[3])
    assert stream.rows_written + stream.rows_pending == 4 * ROWS
    del stream

    blob = state.to_bytes()
    recovered = aroyo.WriterState.from_bytes(blob)
    assert recovered == state
    aroyo.finish(config, recovered)
    aroyo.finish(config, recovered)

    got = read_table(config, path)
    assert got.equals(table(batches[:3]))


def test_close_and_commit_keeps_every_row(config: aroyo.S3Config, path: str) -> None:
    batches = [batch(i * ROWS, ROWS) for i in range(3)]
    with writer(config, path) as stream:
        stream.write(table(batches[:2]))
        stream.write(batches[2])
        pending = stream.close()
    assert pending.rows == 3 * ROWS
    assert pending.state.is_pending_commit
    pending.commit()
    pending.commit()

    got = read_table(config, path, concurrency=2, batch_size=7_000)
    assert got.equals(table(batches))
    assert got.schema.equals(schema())


def test_read_parquet_projects_columns_in_file_order(config: aroyo.S3Config, path: str) -> None:
    batches = [batch(0, ROWS)]
    stream = writer(config, path)
    stream.write(batches[0])
    stream.close().commit()
    got = read_table(config, path, columns=["name", "id"])
    assert got.column_names == ["id", "name"]
    assert got.equals(table(batches).select(["id", "name"]))
    with pytest.raises(ValueError, match="missing"):
        aroyo.read_parquet(config, path, columns=["missing"])


def test_a_recovered_file_keeps_its_arrow_types(config: aroyo.S3Config, path: str) -> None:
    batches = [batch(0, ROWS), batch(ROWS, ROWS)]
    stream = writer(config, path, compression="zstd")
    stream.write(batches[0])
    stream.write(batches[1])
    state = stream.checkpoint()
    del stream
    aroyo.finish(config, state)
    got = read_table(config, path)
    assert got.schema.equals(schema())
    assert got.equals(table(batches))


def crash_after_checkpoint(config: aroyo.S3Config, path: str, pipe: Connection) -> None:
    stream = writer(config, path)
    stream.write(batch(0, ROWS))
    stream.write(batch(ROWS, ROWS))
    state = stream.checkpoint()
    pipe.send_bytes(state.to_bytes())
    stream.write(batch(2 * ROWS, ROWS))
    pipe.close()
    os._exit(0)


def test_another_process_finishes_after_a_crash(config: aroyo.S3Config, path: str) -> None:
    parent, child = multiprocessing.Pipe()
    process = multiprocessing.get_context("spawn").Process(
        target=crash_after_checkpoint, args=(config, path, child)
    )
    process.start()
    blob = parent.recv_bytes()
    process.join(timeout=120)
    assert process.exitcode == 0

    state = aroyo.WriterState.from_bytes(blob)
    aroyo.finish(config, state)
    got = read_table(config, path)
    assert got.num_rows == 2 * ROWS
    assert got.equals(table([batch(0, ROWS), batch(ROWS, ROWS)]))


def test_part_size_below_the_s3_minimum_is_refused(config: aroyo.S3Config, path: str) -> None:
    with pytest.raises(ValueError, match="part_size"):
        aroyo.ParquetStreamWriter(config, path, schema(), part_size=1024)


def test_batches_that_do_not_fit_the_schema_are_refused(config: aroyo.S3Config, path: str) -> None:
    stream = writer(config, path)
    with pytest.raises(ValueError, match="columns"):
        stream.write(pa.record_batch({"id": pa.array([1, 2])}))
    stream.abort()


def test_a_closed_writer_refuses_more_work(config: aroyo.S3Config, path: str) -> None:
    stream = writer(config, path)
    pending = stream.close()
    with pytest.raises(aroyo.AroyoError, match="closed"):
        stream.write(batch(0, 10))
    with pytest.raises(aroyo.AroyoError, match="closed"):
        stream.checkpoint()
    pending.commit()
    assert read_table(config, path).num_rows == 0


def test_abort_discards_the_upload(config: aroyo.S3Config, path: str) -> None:
    stream = writer(config, path)
    stream.write(batch(0, ROWS))
    state = stream.checkpoint()
    stream.abort()
    with pytest.raises(aroyo.AroyoError):
        aroyo.finish(config, state)
    with pytest.raises(aroyo.AroyoError):
        aroyo.read_parquet(config, path)


def test_the_context_manager_aborts_on_error(config: aroyo.S3Config, path: str) -> None:
    with pytest.raises(RuntimeError, match="boom"):
        with writer(config, path) as stream:
            stream.write(batch(0, 100))
            raise RuntimeError("boom")
    with pytest.raises(aroyo.AroyoError, match="closed"):
        stream.checkpoint()


def test_state_blob_rejects_garbage() -> None:
    with pytest.raises(aroyo.AroyoError, match="not an aroyo"):
        aroyo.WriterState.from_bytes(b"garbage")


def test_unknown_compression_is_refused(config: aroyo.S3Config, path: str) -> None:
    with pytest.raises(ValueError, match="compression"):
        aroyo.ParquetStreamWriter(config, path, schema(), compression="lzma")


def test_config_repr_hides_secrets() -> None:
    config = aroyo.S3Config(bucket="b", access_key_id="AKIA", secret_access_key="hunter2")
    assert "hunter2" not in repr(config)
    assert "AKIA" not in repr(config)
    assert config.bucket == "b"


def test_config_round_trips_through_pickle(config: aroyo.S3Config) -> None:
    copy = pickle.loads(pickle.dumps(config))
    assert copy.bucket == config.bucket
    assert copy.endpoint == config.endpoint
    assert repr(copy) == repr(config)
