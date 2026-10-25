"""Opt-in end-to-end coverage for a dedicated development cluster.

Never run this against production: it creates and drops one uniquely named
database and writes/deletes objects beneath one generated S3 prefix.
"""

import io
import os
import subprocess
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
import urllib3


ROOT = Path(__file__).resolve().parents[1]


def _connection(name):
    """Read only the requested client connection; never include XML in errors."""
    candidates = [
        Path.home() / ".clickhouse-client" / "config.xml",
        Path.home() / ".clickhouse-client" / "clickhouse-client.conf",
        Path.home() / ".config" / "clickhouse-client" / "config.xml",
    ]
    for directory in (Path.home() / ".clickhouse-client" / "config.d",):
        candidates.extend(sorted(directory.glob("*.xml")))
    for path in candidates:
        if not path.is_file():
            continue
        try:
            root = ET.parse(path).getroot()
        except (ET.ParseError, OSError):
            continue
        for node in root.iter("connection"):
            values = {child.tag: (child.text or "").strip() for child in node}
            if values.get("name") == name:
                return values
    pytest.fail("named ClickHouse connection was not found in standard client config")


def _sql(client, sql):
    try:
        return client.command(sql)
    except Exception as exc:
        detail = str(exc)
        password = os.getenv("S3GC_DEV_CH_PASSWORD", "")
        if password:
            detail = detail.replace(password, "<redacted>")
        pytest.fail(f"ClickHouse setup/verification query failed: {detail[:1000]}")


def _run_s3gc(args, env, password):
    result = subprocess.run(
        ["python3", str(ROOT / "s3gc.py"), *args],
        text=True, capture_output=True, check=False, timeout=300, env=env,
    )
    output = result.stdout + result.stderr
    if password:
        output = output.replace(password, "<redacted>")
    assert result.returncode == 0, f"s3gc failed with exit {result.returncode}:\n{output}"
    return output


def _assert_s3_object_missing(client, bucket, key):
    from minio.error import S3Error

    try:
        client.stat_object(bucket, key)
    except S3Error as exc:
        if exc.code == "NoSuchKey":
            return
        raise AssertionError(f"expected S3 NoSuchKey, got {exc.code!r}") from exc
    raise AssertionError("S3 object is still present")


def test_s3_object_absence_requires_no_such_key():
    from minio.error import S3Error

    class FakeS3:
        def __init__(self, *, result=None, error_code=None, exception=None):
            self.result = result
            self.error_code = error_code
            self.exception = exception

        def stat_object(self, _bucket, _key):
            if self.exception:
                raise self.exception
            if self.error_code:
                raise S3Error(None, self.error_code, "fixture error", "", "", "")
            return self.result

    _assert_s3_object_missing(FakeS3(error_code="NoSuchKey"), "bucket", "missing")
    with pytest.raises(AssertionError, match="AccessDenied"):
        _assert_s3_object_missing(FakeS3(error_code="AccessDenied"), "bucket", "present")
    with pytest.raises(AssertionError, match="still present"):
        _assert_s3_object_missing(FakeS3(result=object()), "bucket", "present")
    with pytest.raises(TimeoutError):
        _assert_s3_object_missing(FakeS3(exception=TimeoutError("network timeout")), "bucket", "unknown")


@pytest.mark.dev_cluster
@pytest.mark.skipif(
    os.getenv("S3GC_DEV_E2E") != "1",
    reason="set S3GC_DEV_E2E=1 to opt in to the isolated dev-cluster test",
)
def test_replicated_inventory_collect_dryrun_delete_verify():
    required = (
        "S3GC_DEV_CH_CLUSTER", "S3GC_DEV_CH_HTTP_ENDPOINTS",
        "S3GC_DEV_S3_BUCKET", "S3GC_DEV_S3_HOST", "S3GC_DEV_S3_REGION",
    )
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        pytest.fail("missing required dev-test settings: " + ", ".join(missing))
    if os.getenv("S3GC_DEV_DELETE_CONFIRMATION") != "DELETE_ORPHANS":
        pytest.fail("set S3GC_DEV_DELETE_CONFIRMATION=DELETE_ORPHANS to enable fixture deletion")

    from minio import Minio
    from minio.credentials import IamAwsProvider
    from clickhouse_connect import get_client

    connection_name = os.getenv("S3GC_DEV_CH_CONNECTION", "")
    if os.getenv("S3GC_DEV_CH_USER") is not None and os.getenv("S3GC_DEV_CH_PASSWORD") is not None:
        connection = {
            "user": os.environ["S3GC_DEV_CH_USER"],
            "password": os.environ["S3GC_DEV_CH_PASSWORD"],
        }
    elif connection_name:
        connection = _connection(connection_name)
    else:
        pytest.fail("provide a named client connection or Secret-backed CH_USER/CH_PASSWORD")
    cluster = os.environ["S3GC_DEV_CH_CLUSTER"]
    bucket = os.environ["S3GC_DEV_S3_BUCKET"]
    host = os.environ["S3GC_DEV_S3_HOST"]
    region = os.environ["S3GC_DEV_S3_REGION"]
    endpoints = [item.strip() for item in os.environ["S3GC_DEV_CH_HTTP_ENDPOINTS"].split(",")]
    parsed_endpoints = []
    for endpoint in endpoints:
        address, separator, port_text = endpoint.rpartition(":")
        if not separator or not address:
            pytest.fail("S3GC_DEV_CH_HTTP_ENDPOINTS must be comma-separated host:port values")
        parsed_endpoints.append((address, int(port_text)))
    assert len(parsed_endpoints) == 2 and len(set(parsed_endpoints)) == 2
    username, password = connection.get("user", "default"), connection.get("password", "")

    token = uuid.uuid4().hex
    database = "s3gc_e2e_" + token[:20]
    table = "inventory_s3"
    prefix = f"s3gc-e2e/{token}/"
    keys = [f"{prefix}fixture-{index:04d}.bin" for index in range(256)]
    iam_http = urllib3.PoolManager(
        timeout=urllib3.Timeout(connect=5, read=10),
        retries=False,
    )
    iam_provider = IamAwsProvider(http_client=iam_http, region=region)
    # Fail promptly when pytest is not running in a workload with AWS identity;
    # the provider's default metadata HTTP client can otherwise wait for minutes.
    iam_provider.retrieve()
    client = Minio(
        f"{host}:443", credentials=iam_provider, secure=True, region=region,
        http_client=iam_http,
    )
    ch_clients = [get_client(host=host_, port=port, username=username, password=password)
                  for host_, port in parsed_endpoints]
    env = os.environ.copy()
    env.update({
        "S3GC_CHHOST": parsed_endpoints[0][0], "S3GC_CHUSER": username, "S3GC_CHPASS": password,
        "S3GC_CHPORT": str(parsed_endpoints[0][1]), "S3GC_S3IP": host, "S3GC_S3PORT": "443",
        "S3GC_S3BUCKET": bucket, "S3GC_S3PATH": prefix, "S3GC_S3REGION": region,
        "S3GC_S3AUTH": "iam", "S3GC_S3SECURE_FLAG": "true",
        "S3GC_S3DISKNAME": "s3", "S3GC_CLUSTERNAME": cluster,
        "S3GC_EXPECTED_REPLICAS": "2", "S3GC_COLLECTTABLEPREFIX": f"{database}.inventory_",
        "S3GC_COLLECTDATABASE": database, "S3GC_SAMPLES": "1", "S3GC_RUNLOG_FLAG": "false",
        "S3GC_KEEPDATA_FLAG": "true", "S3GC_USE_REMOVE_OBJECTS": "true",
    })

    created = []
    database_created = False
    try:
        fixture = b"s3gc-dev-e2e-fixture"
        for key in keys:
            client.put_object(bucket, key, io.BytesIO(fixture), len(fixture),
                              content_type="application/octet-stream")
            created.append(key)

        _sql(ch_clients[0], f"CREATE DATABASE `{database}` ON CLUSTER `{cluster}` ENGINE = Replicated")
        database_created = True
        # Replicated databases replicate their own DDL; ON CLUSTER is rejected
        # for CREATE TABLE in this database engine.
        _sql(ch_clients[0], f"CREATE TABLE `{database}`.`{table}` "
             "(objpath String, size Int64, last_modified DateTime, active Bool) "
             "ENGINE = ReplicatedReplacingMergeTree ORDER BY objpath PARTITION BY CRC32(objpath) % 1")
        for ch in ch_clients:
            _sql(ch, f"SYSTEM SYNC DATABASE REPLICA `{database}`")
        _run_s3gc(["--collect-only", "--non-interactive"], env, password)
        _sql(ch_clients[0], f"SYSTEM SYNC REPLICA ON CLUSTER `{cluster}` `{database}`.`{table}` LIGHTWEIGHT")

        # Backdate only this generated fixture inventory; keep s3gc's production
        # minimum age guard (24h) intact for the dry-run and delete phases.
        _sql(ch_clients[0], f"ALTER TABLE `{database}`.`{table}` UPDATE last_modified = toDateTime('2020-01-01 00:00:00') "
             f"WHERE startsWith(objpath, '{prefix}') SETTINGS mutations_sync=2")
        _sql(ch_clients[0], f"SYSTEM SYNC REPLICA ON CLUSTER `{cluster}` `{database}`.`{table}` LIGHTWEIGHT")

        for ch in ch_clients:
            rows = ch.query(f"SELECT count() FROM `{database}`.`{table}` WHERE startsWith(objpath, '{prefix}') SETTINGS final=1").result_rows[0][0]
            assert rows == len(keys)

        dryrun = _run_s3gc(["--use-collected", "--dry-run", "--debug", "--non-interactive"], env, password)
        candidates = {
            line.split("would remove if no dryrun flag", 1)[1].split(" of size", 1)[0].strip()
            for line in dryrun.splitlines() if "would remove if no dryrun flag" in line
        }
        assert candidates == set(keys)

        _run_s3gc(["--use-collected", "--non-interactive"], env, password)
        _sql(ch_clients[0], f"SYSTEM SYNC REPLICA ON CLUSTER `{cluster}` `{database}`.`{table}` LIGHTWEIGHT")
        for ch in ch_clients:
            remaining = ch.query(f"SELECT count() FROM `{database}`.`{table}` WHERE startsWith(objpath, '{prefix}') AND active SETTINGS final=1").result_rows[0][0]
            assert remaining == 0
        for key in keys:
            _assert_s3_object_missing(client, bucket, key)
    finally:
        try:
            if database_created:
                _sql(ch_clients[0], f"DROP DATABASE IF EXISTS `{database}` ON CLUSTER `{cluster}` SYNC")
        finally:
            for key in created:
                try:
                    client.remove_object(bucket, key)
                except Exception:
                    pass
            for ch in ch_clients:
                ch.close()
