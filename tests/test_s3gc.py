import os
import subprocess
import sys
import types
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


class QueryResult:
    def __init__(self, value):
        self.result_rows = [(value,)]


class FakeStream:
    def __init__(self, blocks):
        self.blocks = blocks

    def __enter__(self):
        return iter(self.blocks)

    def __exit__(self, *args):
        return False


class FakeCH:
    def __init__(self, cluster="cluster", replicas=2, blocks=()):
        self.cluster = cluster
        self.replicas = replicas
        self.blocks = blocks
        self.inserts = []
        self.stream_query = ""

    def query(self, query):
        if "getMacro" in query:
            return QueryResult(self.cluster)
        if "clusterAllReplicas" in query and "system.one" in query:
            return QueryResult(self.replicas)
        raise AssertionError(query)

    def command(self, query):
        if "COUNT(1)" in query:
            return 1
        raise AssertionError(query)

    def query_row_block_stream(self, query):
        self.stream_query = query
        return FakeStream(self.blocks)

    def insert(self, table, rows, column_names):
        self.inserts.append((table, rows, column_names))


class DeleteError:
    def __init__(self, name):
        self.name = name


class FailingMinio:
    def remove_objects(self, bucket, objects):
        return iter([DeleteError("bad-object")])


@pytest.fixture
def args_factory():
    def make_args(**overrides):
        values = {
            "clustername": "cluster",
            "expected_replicas": 2,
            "dryrun_flag": False,
            "s3diskname": "s3",
            "useafter": None,
            "useage": 24,
            "usetotal": None,
            "samples": 1,
            "deletebatchsize": 1000,
            "order_by_objpath": False,
            "interactive_flag": False,
            "use_remove_objects": True,
            "s3bucket": "bucket",
            "keepdata_flag": True,
            "silent_flag": True,
        }
        values.update(overrides)
        return types.SimpleNamespace(**values)

    return make_args


def test_preflight_rejects_wrong_cluster(s3gc_module, args_factory, monkeypatch):
    namespace = s3gc_module["preflight_cluster"].__globals__
    monkeypatch.setitem(namespace, "args", args_factory(clustername="expected"))
    monkeypatch.setitem(namespace, "ch_client", FakeCH(cluster="actual"))

    with pytest.raises(RuntimeError, match="cluster preflight failed"):
        s3gc_module["preflight_cluster"]()


def test_batch_errors_checkpoint_only_confirmed_deletes(
    s3gc_module, args_factory, monkeypatch
):
    namespace = s3gc_module["do_use"].__globals__
    client = FakeCH(blocks=[[("good-object", 10, "time"), ("bad-object", 20, "time")]])
    monkeypatch.setitem(namespace, "args", args_factory())
    monkeypatch.setitem(namespace, "ch_client", client)
    monkeypatch.setitem(namespace, "minio_client", FailingMinio())

    with pytest.raises(s3gc_module["S3DeletionError"]):
        s3gc_module["do_use"]()

    assert client.inserts == [
        (
            "`s3objects_for_s3`",
            [["good-object", 10, "time", False]],
            ["objpath", "size", "last_modified", "active"],
        )
    ]


def test_delete_batches_are_checkpointed_independently(
    s3gc_module, args_factory, monkeypatch
):
    namespace = s3gc_module["do_use"].__globals__
    client = FakeCH(blocks=[[("object-a", 10, "time"), ("object-b", 20, "time")]])

    class SuccessfulMinio:
        def remove_objects(self, bucket, objects):
            return iter(())

    monkeypatch.setitem(namespace, "args", args_factory(deletebatchsize=1))
    monkeypatch.setitem(namespace, "ch_client", client)
    monkeypatch.setitem(namespace, "minio_client", SuccessfulMinio())
    s3gc_module["do_use"]()

    assert [insert[1] for insert in client.inserts] == [
        [["object-a", 10, "time", False]],
        [["object-b", 20, "time", False]],
    ]


def test_delete_entrypoint_requires_confirmation():
    result = subprocess.run(
        ["sh", str(ROOT / "docker/kubernetes-entrypoint.sh")],
        env={
            **os.environ,
            "S3GC_PHASE": "delete",
            "S3GC_CLUSTERNAME": "cluster",
            "S3GC_EXPECTED_REPLICAS": "2",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 64
    assert "Refusing delete" in result.stderr


def test_dev_automation_entrypoint_runs_collect_dry_run_and_delete(tmp_path):
    calls_path = tmp_path / "calls"
    fake_python = tmp_path / "python"
    fake_python.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$*\" >> \"$CALLS_PATH\"\n"
    )
    fake_python.chmod(0o755)

    result = subprocess.run(
        ["sh", str(ROOT / "docker/kubernetes-entrypoint.sh")],
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "CALLS_PATH": str(calls_path),
            "S3GC_PHASE": "dev-automation",
            "S3GC_DELETE_CONFIRMATION": "DELETE_ORPHANS",
            "S3GC_CLUSTERNAME": "cluster",
            "S3GC_EXPECTED_REPLICAS": "2",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert calls_path.read_text().splitlines() == [
        "/app/s3gc.py --collectonly --keepdata --drop-collecttable",
        "/app/s3gc.py --usecollected --dry-run",
        "/app/s3gc.py --usecollected --keepdata --non-interactive",
    ]


def test_dev_automation_entrypoint_stops_after_an_error(tmp_path):
    calls_path = tmp_path / "calls"
    fake_python = tmp_path / "python"
    fake_python.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$*\" >> \"$CALLS_PATH\"\n"
        "case \"$*\" in *--dry-run) exit 42 ;; esac\n"
    )
    fake_python.chmod(0o755)

    result = subprocess.run(
        ["sh", str(ROOT / "docker/kubernetes-entrypoint.sh")],
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "CALLS_PATH": str(calls_path),
            "S3GC_PHASE": "dev-automation",
            "S3GC_DELETE_CONFIRMATION": "DELETE_ORPHANS",
            "S3GC_CLUSTERNAME": "cluster",
            "S3GC_EXPECTED_REPLICAS": "2",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 42
    assert calls_path.read_text().splitlines() == [
        "/app/s3gc.py --collectonly --keepdata --drop-collecttable",
        "/app/s3gc.py --usecollected --dry-run",
    ]


@pytest.mark.parametrize(
    ("replacement", "message"),
    [
        (("PHASE=dry-run", "PHASE=delete"), "delete requires"),
        (("PHASE=dry-run", "PHASE=dev-automation"), "dev-automation requires"),
        (("ORDER_BY_OBJPATH=false", "ORDER_BY_OBJPATH=yes"), "ORDER_BY_OBJPATH"),
    ],
)
def test_renderer_rejects_invalid_configuration(tmp_path, replacement, message):
    source = (ROOT / "deploy/kubernetes/example.env").read_text()
    config_path = tmp_path / "invalid.env"
    config_path.write_text(source.replace(*replacement))

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 64
    assert message in result.stderr


def test_renderer_accepts_confirmed_dev_automation(tmp_path):
    source = (ROOT / "deploy/kubernetes/example.env").read_text()
    config_path = tmp_path / "dev-automation.env"
    config_path.write_text(
        source.replace("PHASE=dry-run", "PHASE=dev-automation").replace(
            "DELETE_CONFIRMATION=", "DELETE_CONFIRMATION=DELETE_ORPHANS"
        )
    )

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert 's3gc.altinity.com/phase: "dev-automation"' in result.stdout


def test_kubernetes_default_antijoin_does_not_globally_sort(
    s3gc_module, args_factory, monkeypatch
):
    namespace = s3gc_module["do_use"].__globals__
    client = FakeCH()
    monkeypatch.setitem(namespace, "args", args_factory(dryrun_flag=True))
    monkeypatch.setitem(namespace, "ch_client", client)

    s3gc_module["do_use"]()

    assert "ORDER BY s3o.objpath" not in client.stream_query


def test_antijoin_ordering_is_an_explicit_opt_in(s3gc_module, args_factory, monkeypatch):
    namespace = s3gc_module["do_use"].__globals__
    client = FakeCH()
    monkeypatch.setitem(
        namespace, "args", args_factory(dryrun_flag=True, order_by_objpath=True)
    )
    monkeypatch.setitem(namespace, "ch_client", client)

    s3gc_module["do_use"]()

    assert "ORDER BY s3o.objpath" in client.stream_query


def test_delete_transport_failure_reconnects_once(
    s3gc_module, args_factory, monkeypatch
):
    namespace = s3gc_module["remove_objects_reconnecting"].__globals__
    attempts = []

    class TransportFailingMinio:
        def remove_objects(self, bucket, objects):
            attempts.append("failed")
            raise s3gc_module["urllib3"].exceptions.ReadTimeoutError(
                None, "https://s3.example", "timed out"
            )

    class SuccessfulMinio:
        def remove_objects(self, bucket, objects):
            attempts.append("success")
            return iter(())

    def reconnect():
        namespace["minio_client"] = SuccessfulMinio()

    monkeypatch.setitem(namespace, "args", args_factory())
    monkeypatch.setitem(namespace, "minio_client", TransportFailingMinio())
    monkeypatch.setitem(namespace, "connect_to_s3", reconnect)

    assert s3gc_module["remove_objects_reconnecting"]([("object-a", 10, "time")]) == []
    assert attempts == ["failed", "success"]
