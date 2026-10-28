"""Write and read throughput of aroyo against pyarrow's S3FileSystem and obstore.

Environment: AROYO_BENCH_ENDPOINT, AROYO_BENCH_BUCKET, AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY, AWS_REGION (default us-east-1). The payload is a mix of
int64, float64 and string columns. Every number is printed with the machine and
the endpoint so results can be compared.
"""

import argparse
import json
import os
import platform
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

import aroyo

MIB = 1024 * 1024


@dataclass(frozen=True)
class Result:
    name: str
    seconds: float
    payload_bytes: int

    @property
    def mib_per_second(self) -> float:
        return self.payload_bytes / MIB / self.seconds


def payload(rows: int, seed: int) -> pa.Table:
    rng = np.random.default_rng(seed)
    words = np.array([f"w{i:05d}" for i in range(50_000)])
    return pa.table(
        {
            "id": pa.array(np.arange(rows, dtype=np.int64)),
            "a": pa.array(rng.integers(0, 1 << 40, rows, dtype=np.int64)),
            "b": pa.array(rng.random(rows)),
            "c": pa.array(rng.random(rows) * 1000),
            "s": pa.array(words[rng.integers(0, len(words), rows)]),
        }
    )


def timed(name: str, payload_bytes: int, run: Callable[[], None]) -> Result:
    start = time.perf_counter()
    run()
    return Result(name, time.perf_counter() - start, payload_bytes)


def aroyo_write(config: aroyo.S3Config, path: str, table: pa.Table, batch_rows: int, part_size: int) -> None:
    writer = aroyo.ParquetStreamWriter(config, path, table.schema, part_size=part_size)
    for batch in table.to_batches(max_chunksize=batch_rows):
        writer.write(batch)
    writer.close().commit()


def aroyo_read(config: aroyo.S3Config, path: str, concurrency: int) -> int:
    reader = pa.RecordBatchReader.from_stream(aroyo.read_parquet(config, path, concurrency=concurrency))
    return sum(batch.num_rows for batch in reader)


def pyarrow_fs(endpoint: str) -> "pa.fs.S3FileSystem":
    from pyarrow import fs

    return fs.S3FileSystem(
        access_key=os.environ["AWS_ACCESS_KEY_ID"],
        secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region=os.environ.get("AWS_REGION", "us-east-1"),
        endpoint_override=endpoint,
        scheme="http" if endpoint.startswith("http://") else "https",
    )


def pyarrow_write(filesystem: "pa.fs.S3FileSystem", key: str, table: pa.Table, batch_rows: int) -> None:
    with filesystem.open_output_stream(key) as sink:
        with pq.ParquetWriter(sink, table.schema) as writer:
            for batch in table.to_batches(max_chunksize=batch_rows):
                writer.write_batch(batch)


def pyarrow_read(filesystem: "pa.fs.S3FileSystem", key: str) -> int:
    with filesystem.open_input_file(key) as source:
        return sum(batch.num_rows for batch in pq.ParquetFile(source).iter_batches())


class ObstoreFile:
    """Adapts obstore's file objects for pyarrow, which expects ``closed`` as a property."""

    def __init__(self, inner: object) -> None:
        self.inner = inner
        self.closed = False

    def close(self) -> None:
        self.closed = True
        self.inner.close()

    def __getattr__(self, name: str) -> object:
        return getattr(self.inner, name)


def obstore_write(store: object, path: str, table: pa.Table, batch_rows: int) -> None:
    import obstore

    sink = ObstoreFile(obstore.open_writer(store, path))
    with pq.ParquetWriter(sink, table.schema) as writer:
        for batch in table.to_batches(max_chunksize=batch_rows):
            writer.write_batch(batch)
    sink.close()


def obstore_read(store: object, path: str) -> int:
    import obstore

    source = ObstoreFile(obstore.open_reader(store, path))
    rows = sum(batch.num_rows for batch in pq.ParquetFile(source).iter_batches())
    source.close()
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=20_000_000, help="rows in the payload")
    parser.add_argument("--batch-rows", type=int, default=250_000)
    parser.add_argument("--part-size", type=int, default=16 * MIB)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--json", type=Path, default=None, help="also write the results to this file")
    args = parser.parse_args()

    endpoint = os.environ["AROYO_BENCH_ENDPOINT"]
    bucket = os.environ["AROYO_BENCH_BUCKET"]
    config = aroyo.S3Config.from_env(endpoint=endpoint, bucket=bucket, allow_http=endpoint.startswith("http://"))
    table = payload(args.rows, seed=7)
    payload_bytes = table.nbytes
    run_id = uuid.uuid4().hex[:8]

    print(f"machine: {platform.platform()} cpus={os.cpu_count()} python={platform.python_version()}")
    print(f"endpoint: {endpoint} bucket={bucket}")
    print(f"payload: {args.rows} rows, {payload_bytes / MIB:.0f} MiB of Arrow data, pyarrow {pa.__version__}, aroyo {aroyo.__version__}")

    results: list[Result] = []
    for _ in range(args.repeat):
        key = f"bench/{run_id}/aroyo.parquet"
        results.append(timed("aroyo write", payload_bytes, lambda: aroyo_write(config, key, table, args.batch_rows, args.part_size)))
        results.append(timed(f"aroyo read (concurrency={args.concurrency})", payload_bytes, lambda: aroyo_read(config, key, args.concurrency)))

        try:
            filesystem = pyarrow_fs(endpoint)
            key_pa = f"{bucket}/bench/{run_id}/pyarrow.parquet"
            results.append(timed("pyarrow write", payload_bytes, lambda: pyarrow_write(filesystem, key_pa, table, args.batch_rows)))
            results.append(timed("pyarrow read", payload_bytes, lambda: pyarrow_read(filesystem, key_pa)))
        except ImportError as error:
            print(f"pyarrow S3FileSystem unavailable: {error}")

        try:
            from obstore.store import S3Store

            store = S3Store(
                bucket,
                endpoint=endpoint,
                region=os.environ.get("AWS_REGION", "us-east-1"),
                access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                client_options={"allow_http": endpoint.startswith("http://")},
            )
            key_ob = f"bench/{run_id}/obstore.parquet"
            results.append(timed("obstore write", payload_bytes, lambda: obstore_write(store, key_ob, table, args.batch_rows)))
            results.append(timed("obstore read", payload_bytes, lambda: obstore_read(store, key_ob)))
        except ImportError:
            print("obstore not installed; skipping that baseline")

    print()
    print(f"{'case':40} {'seconds':>9} {'MiB/s':>9}")
    for result in results:
        print(f"{result.name:40} {result.seconds:9.2f} {result.mib_per_second:9.1f}")

    if not args.json:
        return
    report = {
        "machine": platform.platform(),
        "cpus": os.cpu_count(),
        "python": platform.python_version(),
        "pyarrow": pa.__version__,
        "aroyo": aroyo.__version__,
        "rows": args.rows,
        "payload_bytes": payload_bytes,
        "results": [asdict(result) | {"mib_per_second": result.mib_per_second} for result in results],
    }
    args.json.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
