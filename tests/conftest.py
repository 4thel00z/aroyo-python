"""Fixtures: an S3 endpoint (moto in-process, or AROYO_TEST_ENDPOINT) and test data."""

import os
import socket
import urllib.error
import urllib.request
import uuid
from collections.abc import Iterator

import pyarrow as pa
import pytest

import aroyo

MIB = 1024 * 1024
PART_SIZE = 5 * MIB


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def create_bucket(endpoint: str, bucket: str) -> None:
    request = urllib.request.Request(f"{endpoint}/{bucket}", method="PUT")
    try:
        urllib.request.urlopen(request).close()
    except urllib.error.HTTPError as error:
        if error.code != 409:
            raise


@pytest.fixture(scope="session")
def endpoint() -> Iterator[str]:
    external = os.environ.get("AROYO_TEST_ENDPOINT")
    if external:
        yield external
        return
    from moto.server import ThreadedMotoServer

    port = free_port()
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    server.start()
    yield f"http://127.0.0.1:{port}"
    server.stop()


@pytest.fixture(scope="session")
def config(endpoint: str) -> aroyo.S3Config:
    bucket = os.environ.get("AROYO_TEST_BUCKET", "aroyo-test")
    if not os.environ.get("AROYO_TEST_ENDPOINT"):
        create_bucket(endpoint, bucket)
    return aroyo.S3Config(
        endpoint=endpoint,
        bucket=bucket,
        region=os.environ.get("AWS_REGION", "us-east-1"),
        access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "testing"),
        secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "testing"),
        allow_http=True,
    )


@pytest.fixture
def path() -> str:
    return f"tests/{uuid.uuid4().hex}.parquet"


def schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("value", pa.float64()),
            pa.field("name", pa.string(), nullable=False),
            pa.field("ts", pa.timestamp("us", tz="UTC")),
            pa.field("tags", pa.list_(pa.int64())),
            pa.field("point", pa.struct([("x", pa.int64()), ("y", pa.int64())])),
        ]
    )


def batch(start: int, rows: int) -> pa.RecordBatch:
    ids = list(range(start, start + rows))
    return pa.RecordBatch.from_pydict(
        {
            "id": ids,
            "value": [None if i % 7 == 0 else i / 3.0 for i in ids],
            "name": [f"n{(i * 2654435761) % 2**32:08x}" for i in ids],
            "ts": [None if i % 5 == 0 else 1_700_000_000_000_000 + i * 1_000_000 for i in ids],
            "tags": [None if i % 9 == 0 else list(range(i % 4)) for i in ids],
            "point": [{"x": i, "y": None if i % 3 == 0 else -i} for i in ids],
        },
        schema=schema(),
    )


def table(batches: list[pa.RecordBatch]) -> pa.Table:
    return pa.Table.from_batches(batches, schema=schema())
