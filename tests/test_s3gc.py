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


# ---------------------------------------------------------------------------
# Regression tests for defects found in production (SUP-30408).
# ---------------------------------------------------------------------------


def _load_with_env(monkeypatch, **env):
    """Load s3gc.py with the given S3GC_* environment, as a Kubernetes Job would."""
    import runpy

    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(sys, "argv", [str(Path(__file__).resolve().parents[1] / "s3gc.py")])
    return runpy.run_path(
        str(Path(__file__).resolve().parents[1] / "s3gc.py"), run_name="s3gc_test"
    )


@pytest.mark.parametrize(
    "value, expected",
    [
        ("false", False),
        ("False", False),
        ("no", False),
        ("off", False),
        ("0", False),
        ("", False),
        ("true", True),
        ("True", True),
        ("yes", True),
        ("on", True),
        ("1", True),
    ],
)
def test_coerce_bool_parses_env_spellings(s3gc_module, value, expected):
    assert s3gc_module["coerce_bool"](value) is expected


def test_coerce_bool_passes_through_real_bools_and_none(s3gc_module):
    assert s3gc_module["coerce_bool"](True) is True
    assert s3gc_module["coerce_bool"](False) is False
    assert s3gc_module["coerce_bool"](None) is False


def test_coerce_bool_rejects_nonsense(s3gc_module):
    with pytest.raises(ValueError):
        s3gc_module["coerce_bool"]("maybe")


@pytest.mark.parametrize(
    "dest, env_name",
    [
        ("s3useiam", "S3GC_S3USEIAM"),
        ("s3secure_flag", "S3GC_S3SECURE_FLAG"),
        ("dryrun_flag", "S3GC_DRYRUN_FLAG"),
        ("keepdata_flag", "S3GC_KEEPDATA_FLAG"),
        ("order_by_objpath", "S3GC_ORDER_BY_OBJPATH"),
        ("verbose_flag", "S3GC_VERBOSE_FLAG"),
    ],
)
def test_boolean_env_false_is_false(monkeypatch, dest, env_name):
    """S3GC_*=false used to be the truthy string 'false'.

    S3GC_S3USEIAM=false selected the IAM credential provider and hung a
    Kubernetes Job indefinitely with no error, no exception and no log line.
    """
    module = _load_with_env(monkeypatch, **{env_name: "false"})
    assert getattr(module["args"], dest) is False


@pytest.mark.parametrize("value", ["true", "1", "yes"])
def test_boolean_env_true_is_true(monkeypatch, value):
    module = _load_with_env(monkeypatch, S3GC_S3USEIAM=value)
    assert module["args"].s3useiam is True


def test_boolean_env_zero_is_false(monkeypatch):
    """'0' must not be truthy either, and must not raise (type=bool would)."""
    module = _load_with_env(monkeypatch, S3GC_S3USEIAM="0")
    assert module["args"].s3useiam is False


def test_bare_cli_flag_still_enables(monkeypatch):
    """Coercion must not break `--dryrun` used as a bare flag."""
    import runpy

    root = Path(__file__).resolve().parents[1]
    monkeypatch.setattr(sys, "argv", [str(root / "s3gc.py"), "--dryrun"])
    module = runpy.run_path(str(root / "s3gc.py"), run_name="s3gc_test")
    assert module["args"].dryrun_flag is True


def test_collect_age_filter_uses_total_seconds(s3gc_module, args_factory, monkeypatch):
    """--age 24 must keep a 30-day-old object.

    The filter used timedelta.seconds (the sub-day remainder, 0..86399), so
    computed age never exceeded 23 h and --age 24 collected nothing at all,
    leaving an empty aux table and a dry-run that reported a clean bucket.
    """
    import datetime

    namespace = s3gc_module["do_collect"].__globals__
    now = datetime.datetime.now(datetime.timezone.utc)

    class Obj:
        def __init__(self, name, age):
            self.object_name = name
            self.size = 1
            self.last_modified = now - age

    old = Obj("thirty-days-old", datetime.timedelta(days=30, hours=5))
    fresh = Obj("one-hour-old", datetime.timedelta(hours=1))

    class Minio:
        def list_objects(self, bucket, prefix, recursive, start_after):
            return iter([old, fresh])

    class CH:
        def __init__(self):
            self.rows = []

        def command(self, query):
            return None

        def insert(self, table, rows, column_names):
            self.rows.extend(rows)

    ch = CH()
    monkeypatch.setitem(
        namespace,
        "args",
        args_factory(
            age=24,
            collectbatchsize=10,
            total=None,
            collectafter="",
            s3path="",
            s3bucket="bucket",
            createdatabase_flag=False,
            drop_collecttable_flag=False,
        ),
    )
    monkeypatch.setitem(namespace, "minio_client", Minio())
    monkeypatch.setitem(namespace, "ch_client", ch)
    monkeypatch.setitem(namespace, "tname", "`aux`")

    s3gc_module["do_collect"]()

    collected = [row[0] for row in ch.rows]
    assert "thirty-days-old" in collected
    assert "one-hour-old" not in collected


def test_usecollected_without_aux_table_fails_loudly(
    s3gc_module, args_factory, monkeypatch
):
    """An absent/empty aux table used to exit 0 — indistinguishable from success.

    That is exactly what a load-balanced CHHOST produces, because the aux table
    is a node-local ReplacingMergeTree.
    """
    namespace = s3gc_module["do_use"].__globals__

    class EmptyCH(FakeCH):
        def command(self, query):
            return 0

    monkeypatch.setitem(namespace, "args", args_factory(dryrun_flag=True, chhost="replica-1"))
    monkeypatch.setitem(namespace, "ch_client", EmptyCH())
    monkeypatch.setitem(namespace, "tname", "`aux`")

    with pytest.raises(RuntimeError, match="does not exist or is empty"):
        s3gc_module["do_use"]()


def test_samples_mismatch_warns(s3gc_module, args_factory, monkeypatch, caplog):
    """--samples must match the aux table's PARTITION BY or pruning is lost."""
    namespace = s3gc_module["check_samples_match_partitioning"].__globals__

    class PartitionedCH:
        def query(self, query):
            return QueryResult("CRC32(objpath) % 4")

    monkeypatch.setitem(namespace, "args", args_factory(samples=3))
    monkeypatch.setitem(namespace, "ch_client", PartitionedCH())
    monkeypatch.setitem(namespace, "tname", "`aux`")

    with caplog.at_level("WARNING"):
        s3gc_module["check_samples_match_partitioning"]()

    assert "does not match" in caplog.text


def test_gcs_endpoint_disables_batch_delete(s3gc_module, args_factory, monkeypatch):
    """GCS has no batch DeleteObjects; remove_objects() fails there."""
    namespace = s3gc_module["connect_to_s3"].__globals__
    parsed = args_factory(
        s3ip="storage.googleapis.com",
        s3port=443,
        use_remove_objects=True,
        s3useiam=False,
        s3secure_flag=True,
        s3accesskey="k",
        s3secretkey="s",
        s3region="auto",
        s3sslcertfile="",
        s3_connect_timeout=15,
        s3_read_timeout=120,
        s3_retries=3,
    )
    monkeypatch.setitem(namespace, "args", parsed)
    monkeypatch.setitem(namespace, "Minio", lambda *a, **k: object())

    s3gc_module["connect_to_s3"]()

    assert parsed.use_remove_objects is False


def test_renderer_omits_empty_image_pull_secret(tmp_path):
    """A public image needs no pull secret; `- name: ""` would be meaningless."""
    config_path = tmp_path / "public.env"
    config_path.write_text((ROOT / "deploy/kubernetes/example.env").read_text())

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "imagePullSecrets" not in result.stdout
    assert 'name: ""' not in result.stdout


def test_renderer_keeps_configured_image_pull_secret(tmp_path):
    """A private mirror must still be able to set one."""
    source = (ROOT / "deploy/kubernetes/example.env").read_text()
    config_path = tmp_path / "private.env"
    config_path.write_text(
        source.replace("IMAGE_PULL_SECRET=", "IMAGE_PULL_SECRET=my-mirror-pull")
    )

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "imagePullSecrets:" in result.stdout
    assert "- name: \"my-mirror-pull\"" in result.stdout
