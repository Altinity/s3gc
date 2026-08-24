from argparse import ArgumentError
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
            "s3path": "data/",
            "chhost": "replica-0",
            "keepdata_flag": True,
            "silent_flag": True,
            # Durable run log. run_log_enabled starts False at module level, so
            # tests that do not opt in are unaffected by these.
            "runlog_flag": True,
            "runid": "test-run",
            "dev_allow_short_useage": False,
            # S3 auth surface (static | aws | iam)
            "s3auth": "static",
            "s3profile": "",
            "s3accesskey": "",
            "s3secretkey": "",
            "s3sessiontoken": "",
            "s3region": "eu-central-1",
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
    monkeypatch.setitem(namespace, "ch_writer", client)
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
    monkeypatch.setitem(namespace, "ch_writer", client)
    monkeypatch.setitem(namespace, "minio_client", SuccessfulMinio())
    s3gc_module["do_use"]()

    assert [insert[1] for insert in client.inserts] == [
        [["object-a", 10, "time", False]],
        [["object-b", 20, "time", False]],
    ]


class SessionIsLocked(RuntimeError):
    """Stand-in for ClickHouse error 373."""


class StreamingCH(FakeCH):
    """A client that rejects writes while one of its result streams is open.

    Models ClickHouse's one-query-per-session rule. clickhouse-connect gives
    each client an auto-generated session_id, and `insert()` issues its own
    `DESCRIBE TABLE` before writing — a second concurrent query on the session
    the anti-join stream is still holding.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.streaming = False

    def query_row_block_stream(self, query):
        inner = super().query_row_block_stream(query)
        owner = self

        class Guarded:
            def __enter__(self):
                owner.streaming = True
                return inner.__enter__()

            def __exit__(self, *exc):
                owner.streaming = False
                return inner.__exit__(*exc)

        return Guarded()

    def insert(self, *args, **kwargs):
        if self.streaming:
            raise SessionIsLocked("Session is locked by a concurrent client")
        return super().insert(*args, **kwargs)


def test_tombstones_are_written_off_the_streaming_session(
    s3gc_module, args_factory, monkeypatch
):
    """Regression: the delete phase died on its first batch with SESSION_IS_LOCKED.

    Objects were already gone from S3 when the tombstone insert was rejected, so
    the deletion went unrecorded and the job could not resume cleanly. Tombstones
    must therefore be written on a client that is not holding the anti-join stream.
    """
    namespace = s3gc_module["do_use"].__globals__
    streaming = StreamingCH(blocks=[[("object-a", 10, "time")]])
    writer = FakeCH()

    class SuccessfulMinio:
        def remove_objects(self, bucket, objects):
            return iter(())

    monkeypatch.setitem(namespace, "args", args_factory())
    monkeypatch.setitem(namespace, "ch_client", streaming)
    monkeypatch.setitem(namespace, "ch_writer", writer)
    monkeypatch.setitem(namespace, "minio_client", SuccessfulMinio())

    s3gc_module["do_use"]()

    assert streaming.inserts == []
    assert [insert[1] for insert in writer.inserts] == [
        [["object-a", 10, "time", False]]
    ]


def test_connect_to_ch_builds_a_separate_writer_client(s3gc_module, monkeypatch):
    """The two clients must be distinct, or the session lock comes straight back."""
    namespace = s3gc_module["connect_to_ch"].__globals__
    built = []

    class FakeConnect:
        @staticmethod
        def get_client(**kwargs):
            client = object()
            built.append(client)
            return client

    monkeypatch.setitem(namespace, "clickhouse_connect", FakeConnect)
    monkeypatch.setitem(
        namespace,
        "args",
        types.SimpleNamespace(
            chhost="host",
            chport=8123,
            chuser="user",
            chpass="pass",
            chtimeout=60,
            s3path="path",
            s3bucket="bucket",
        ),
    )

    s3gc_module["connect_to_ch"]()

    assert len(built) == 2
    assert namespace["ch_client"] is not namespace["ch_writer"]


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
        "/app/s3gc.py --usecollected --dry-run --dev-allow-short-useage=true",
        "/app/s3gc.py --usecollected --keepdata --non-interactive --dev-allow-short-useage=true",
    ]


def test_dev_automation_entrypoint_stops_after_an_error(tmp_path):
    calls_path = tmp_path / "calls"
    fake_python = tmp_path / "python"
    fake_python.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$*\" >> \"$CALLS_PATH\"\n"
        "case \"$*\" in *--dry-run*) exit 42 ;; esac\n"
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
        "/app/s3gc.py --usecollected --dry-run --dev-allow-short-useage=true",
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
        ("s3secure_flag", "S3GC_S3SECURE_FLAG"),
        ("dryrun_flag", "S3GC_DRYRUN_FLAG"),
        ("keepdata_flag", "S3GC_KEEPDATA_FLAG"),
        ("order_by_objpath", "S3GC_ORDER_BY_OBJPATH"),
        ("verbose_flag", "S3GC_VERBOSE_FLAG"),
    ],
)
def test_boolean_env_false_is_false(monkeypatch, dest, env_name):
    """S3GC_*=false must not remain the truthy string 'false'."""
    module = _load_with_env(monkeypatch, **{env_name: "false"})
    assert getattr(module["args"], dest) is False


def test_bare_cli_flag_still_enables(monkeypatch):
    """Coercion must not break `--dryrun` used as a bare flag."""
    import runpy

    root = Path(__file__).resolve().parents[1]
    monkeypatch.setattr(sys, "argv", [str(root / "s3gc.py"), "--dryrun"])
    module = runpy.run_path(str(root / "s3gc.py"), run_name="s3gc_test")
    assert module["args"].dryrun_flag is True


def test_removed_s3useiam_cli_flag_is_rejected(monkeypatch):
    import runpy

    root = Path(__file__).resolve().parents[1]
    monkeypatch.setattr(sys, "argv", [str(root / "s3gc.py"), "--s3useiam"])
    with pytest.raises(ArgumentError, match="Unrecognized arguments: --s3useiam"):
        runpy.run_path(str(root / "s3gc.py"), run_name="s3gc_test")


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


def test_renderer_omits_unset_usetotal(tmp_path):
    """An empty USETOTAL must render no variable at all.

    s3gc parses S3GC_USETOTAL as an integer, so `value: ""` would fail the run
    at startup rather than mean "no limit".
    """
    config_path = tmp_path / "full.env"
    config_path.write_text((ROOT / "deploy/kubernetes/example.env").read_text())

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "S3GC_USETOTAL" not in result.stdout


def test_renderer_keeps_configured_usetotal(tmp_path):
    """A bounded trial run must reach the container."""
    source = (ROOT / "deploy/kubernetes/example.env").read_text()
    config_path = tmp_path / "bounded.env"
    config_path.write_text(source.replace("USETOTAL=", "USETOTAL=5000"))

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "S3GC_USETOTAL" in result.stdout
    assert '"5000"' in result.stdout


@pytest.mark.parametrize("value", ["0", "-1", "all", "1.5"])
def test_renderer_rejects_invalid_usetotal(tmp_path, value):
    source = (ROOT / "deploy/kubernetes/example.env").read_text()
    config_path = tmp_path / "bad.env"
    config_path.write_text(source.replace("USETOTAL=", f"USETOTAL={value}"))

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 64
    assert "USETOTAL must be a positive integer when set" in result.stderr


def test_renderer_accepts_an_env_file_without_usetotal(tmp_path):
    """USETOTAL is optional: pre-existing env files must keep rendering."""
    source = (ROOT / "deploy/kubernetes/example.env").read_text()
    without = "\n".join(
        line for line in source.splitlines() if not line.startswith("USETOTAL=")
    )
    config_path = tmp_path / "legacy.env"
    config_path.write_text(without)

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "S3GC_USETOTAL" not in result.stdout


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


# ---------------------------------------------------------------------------
# S3 authentication modes (merged from Altinity/s3gc PR #2, unified with iam).
# ---------------------------------------------------------------------------


def _resolve(s3gc_module, monkeypatch, **overrides):
    namespace = s3gc_module["resolve_s3_credentials"].__globals__
    args = types.SimpleNamespace(
        s3auth="static", s3profile="",
        s3accesskey="", s3secretkey="", s3sessiontoken="", s3region="eu-central-1",
    )
    for key, value in overrides.items():
        setattr(args, key, value)
    monkeypatch.setitem(namespace, "args", args)
    return s3gc_module["resolve_s3_credentials"]()


def test_static_mode_returns_supplied_keys(s3gc_module, monkeypatch):
    result = _resolve(s3gc_module, monkeypatch, s3accesskey="AK", s3secretkey="SK")
    assert result[0] == "AK" and result[1] == "SK"
    assert result[2] is None          # no session token
    assert result[4] == "static"


def test_static_mode_carries_session_token(s3gc_module, monkeypatch):
    result = _resolve(
        s3gc_module, monkeypatch, s3accesskey="AK", s3secretkey="SK", s3sessiontoken="TOKEN"
    )
    assert result[2] == "TOKEN"


def test_static_mode_without_keys_is_anonymous(s3gc_module, monkeypatch):
    assert _resolve(s3gc_module, monkeypatch)[4] == "anonymous"


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"s3accesskey": "AK"}, "must be specified together"),
        ({"s3secretkey": "SK"}, "must be specified together"),
        ({"s3sessiontoken": "TOKEN"}, "requires s3accesskey"),
    ],
)
def test_static_mode_rejects_incomplete_credentials(s3gc_module, monkeypatch, overrides, message):
    with pytest.raises(ValueError, match=message):
        _resolve(s3gc_module, monkeypatch, **overrides)


def test_iam_mode_defers_to_the_provider(s3gc_module, monkeypatch):
    """iam returns no keys: MinIO gets the provider so it can refresh them."""
    result = _resolve(s3gc_module, monkeypatch, s3auth="iam")
    assert result[:3] == (None, None, None)
    assert result[4] == "iam"


def test_s3profile_implies_aws_mode(s3gc_module, monkeypatch):
    calls = []
    namespace = s3gc_module["resolve_s3_credentials"].__globals__
    monkeypatch.setitem(
        namespace, "resolve_aws_s3_credentials",
        lambda: calls.append("aws") or (None, None, None, "eu-central-1", "aws"),
    )
    assert _resolve(s3gc_module, monkeypatch, s3profile="sso")[4] == "aws"
    assert calls == ["aws"]


def test_unknown_auth_mode_is_rejected(s3gc_module, monkeypatch):
    with pytest.raises(ValueError, match="s3auth must be one of"):
        _resolve(s3gc_module, monkeypatch, s3auth="magic")


@pytest.mark.parametrize(
    "overrides",
    [
        {"s3auth": "iam", "s3profile": "sso"},      # profile implies aws, conflicts with iam
    ],
)
def test_contradictory_auth_settings_error(s3gc_module, monkeypatch, overrides):
    """A contradiction must fail, not silently pick a winner and send the wrong identity."""
    with pytest.raises(ValueError, match="conflicts with"):
        _resolve(s3gc_module, monkeypatch, **overrides)


def test_aws_mode_rejects_explicit_keys(s3gc_module, monkeypatch):
    with pytest.raises(ValueError, match="cannot be combined with explicit"):
        _resolve(s3gc_module, monkeypatch, s3auth="aws", s3accesskey="AK", s3secretkey="SK")


def test_aws_mode_without_boto3_is_user_visible(s3gc_module, monkeypatch):
    """A missing optional dependency must not surface as a bare ImportError."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *rest):
        if name == "boto3":
            raise ImportError("no boto3")
        return real_import(name, *rest)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(s3gc_module["UserVisibleError"], match="boto3 is required"):
        _resolve(s3gc_module, monkeypatch, s3auth="aws")


def test_format_s3_list_error_names_the_permission(s3gc_module, args_factory, monkeypatch):
    """The listing failure must tell the operator exactly what to grant."""
    namespace = s3gc_module["format_s3_list_error"].__globals__
    monkeypatch.setitem(
        namespace, "args", args_factory(s3bucket="my-bucket", s3path="pre/fix/", s3profile="sso")
    )

    class Err:
        code = "AccessDenied"
        message = "denied"

    text = s3gc_module["format_s3_list_error"](Err())
    assert "s3:ListBucket" in text
    assert "my-bucket" in text and "pre/fix/" in text
    assert "even with --dry-run" in text
    assert "--profile sso" in text


def test_iam_mode_keeps_the_hardened_transport(s3gc_module, args_factory, monkeypatch):
    """The merge must not revert to a bare PoolManager: that hung a run for 2h19m."""
    namespace = s3gc_module["connect_to_s3"].__globals__
    captured = {}
    monkeypatch.setitem(namespace, "Minio", lambda endpoint, **kw: captured.update(kw) or object())
    monkeypatch.setitem(
        namespace, "args",
        args_factory(s3auth="iam", s3ip="s3.eu-central-1.amazonaws.com", s3port=443,
                     s3secure_flag=True, s3sslcertfile="", s3_connect_timeout=15,
                     s3_read_timeout=120, s3_retries=3),
    )
    s3gc_module["connect_to_s3"]()

    assert "credentials" in captured            # provider, not frozen keys
    assert "access_key" not in captured
    timeout = captured["http_client"].connection_pool_kw["timeout"]
    assert timeout.read_timeout == 120 and timeout.connect_timeout == 15


@pytest.mark.parametrize(
    ("replacement", "message"),
    [
        (("S3AUTH=iam", "S3AUTH=magic"), "S3AUTH must be static, aws or iam"),
        (("S3PROFILE=", "S3PROFILE=sso"), "S3PROFILE requires S3AUTH=aws"),
    ],
)
def test_renderer_validates_auth_configuration(tmp_path, replacement, message):
    source = (ROOT / "deploy/kubernetes/example.env").read_text()
    config_path = tmp_path / "auth.env"
    config_path.write_text(source.replace(*replacement))

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True, text=True, check=False,
    )

    assert result.returncode == 64
    assert message in result.stderr


def test_renderer_wires_auth_env_into_the_job(tmp_path):
    """PR #2's flags were unreachable from a Job until the template exposed them."""
    source = (ROOT / "deploy/kubernetes/example.env").read_text()
    config_path = tmp_path / "aws.env"
    config_path.write_text(source.replace("S3AUTH=iam", "S3AUTH=aws").replace("S3PROFILE=", "S3PROFILE=sso"))

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True, text=True, check=False,
    )

    assert result.returncode == 0
    assert 'name: S3GC_S3AUTH' in result.stdout
    assert 'value: "aws"' in result.stdout
    assert 'value: "sso"' in result.stdout


# ---------------------------------------------------------------------------
# Deletion-scope invariants.
#
# The tests above prove a candidate list is deleted, batched and checkpointed
# correctly. They cannot prove the list contains only orphans, because
# FakeCH.query_row_block_stream ignores the SQL and returns pre-canned blocks.
#
# Mutation testing confirmed the gap: turning LEFT ANTI JOIN into a plain LEFT
# JOIN, pointing disk_name at a nonexistent disk, dropping the join key,
# dropping the clusterAllReplicas fan-out, dropping the --useage window, and
# even making --dry-run delete for real ALL left the suite fully green.
#
# These tests pin the scope itself: what may be considered for deletion, and
# what must be excluded. They assert on the generated anti-join because that
# single statement is the whole safety boundary.
# ---------------------------------------------------------------------------


def _antijoin_sql(s3gc_module, args_factory, monkeypatch, **overrides):
    """Return the anti-join s3gc would stream, whitespace-normalised."""
    namespace = s3gc_module["do_use"].__globals__
    client = FakeCH()
    overrides.setdefault("dryrun_flag", True)
    monkeypatch.setitem(namespace, "args", args_factory(**overrides))
    monkeypatch.setitem(namespace, "ch_client", client)

    s3gc_module["do_use"]()

    return " ".join(client.stream_query.split())


def test_candidates_are_only_unreferenced_objects(s3gc_module, args_factory, monkeypatch):
    """The join must be LEFT ANTI: keep rows with NO match in remote_data_paths.

    A plain LEFT JOIN keeps matched rows too, so every referenced object becomes
    a deletion candidate. That is the disaster case and it must not be reachable
    by an edit that keeps the tests green.
    """
    sql = _antijoin_sql(s3gc_module, args_factory, monkeypatch)

    assert "LEFT ANTI JOIN" in sql


def test_candidates_come_only_from_the_collected_inventory(
    s3gc_module, args_factory, monkeypatch
):
    """Deletion can only ever consider rows collected into the auxiliary table.

    This is what makes objects created after the collect phase safe: they are
    absent from the table, so the delete phase cannot see them at all.
    """
    sql = _antijoin_sql(s3gc_module, args_factory, monkeypatch)

    assert "FROM `s3objects_for_s3` AS s3o" in sql


def test_reference_check_matches_on_object_path(s3gc_module, args_factory, monkeypatch):
    """Losing the join key makes nothing match, so every object looks orphaned."""
    sql = _antijoin_sql(s3gc_module, args_factory, monkeypatch)

    assert "rdp.remote_path = s3o.objpath" in sql


@pytest.mark.parametrize("disk", ["s3", "gcs"])
def test_reference_check_is_scoped_to_the_configured_disk(
    s3gc_module, args_factory, monkeypatch, disk
):
    """A wrong disk name matches no reference at all, orphaning the whole bucket."""
    sql = _antijoin_sql(s3gc_module, args_factory, monkeypatch, s3diskname=disk)

    assert f"rdp.disk_name='{disk}'" in sql


def test_cluster_cleanup_checks_every_replica(s3gc_module, args_factory, monkeypatch):
    """With a cluster configured, references must be read from ALL replicas.

    Reading only the local replica orphans blobs that another replica still
    references — the zero-copy replication disaster case.
    """
    sql = _antijoin_sql(s3gc_module, args_factory, monkeypatch, clustername="prod")

    assert "clusterAllReplicas('prod', system.remote_data_paths)" in sql


def test_without_a_cluster_only_the_local_replica_is_consulted(
    s3gc_module, args_factory, monkeypatch
):
    """Documents the narrowed scope of an unclustered run.

    Deleting with no --cluster only sees one replica's references. The
    Kubernetes entrypoint requires CLUSTERNAME for the delete phase; a direct
    CLI run does not, so the narrowing is real and deliberate here.
    """
    sql = _antijoin_sql(s3gc_module, args_factory, monkeypatch, clustername="")

    assert "clusterAllReplicas" not in sql
    assert "system.remote_data_paths AS rdp" in sql


def test_tombstoned_objects_are_not_reconsidered(s3gc_module, args_factory, monkeypatch):
    """active=true excludes rows already tombstoned by an earlier delete attempt."""
    sql = _antijoin_sql(s3gc_module, args_factory, monkeypatch)

    assert "s3o.active=true" in sql


def test_age_guard_excludes_recently_written_objects(
    s3gc_module, args_factory, monkeypatch
):
    """--useage is the ONLY protection against deleting a part mid-write.

    ClickHouse uploads a part's blobs to S3 and registers them in
    remote_data_paths a moment later. In that window a live blob is absent from
    remote_data_paths and looks orphaned. There is no per-object re-check before
    the S3 delete, so this clause is the whole safety margin.
    """
    sql = _antijoin_sql(s3gc_module, args_factory, monkeypatch, useage=24)

    assert "s3o.last_modified < now() - interval 24 hour" in sql


@pytest.mark.parametrize("hours", [0, 1, 23])
def test_useage_below_the_floor_is_refused(
    s3gc_module, args_factory, monkeypatch, hours
):
    """The age window is a floor, not a default to be talked down.

    Below 24 hours a run can delete a part between its upload to S3 and its
    registration in remote_data_paths. Refused outright rather than warned
    about, and refused before the cluster preflight or any S3 call.
    """
    namespace = s3gc_module["do_use"].__globals__
    monkeypatch.setitem(namespace, "args", args_factory(useage=hours))
    monkeypatch.setitem(namespace, "ch_client", FakeCH())

    with pytest.raises(s3gc_module["UserVisibleError"], match="below the 24 hour minimum"):
        s3gc_module["do_use"]()


@pytest.mark.parametrize("hours", [0, 1, 23])
def test_useage_below_the_floor_is_refused_for_dry_run_too(
    s3gc_module, args_factory, monkeypatch, hours
):
    """A preview wider than the delete would honour is worse than no preview."""
    namespace = s3gc_module["do_use"].__globals__
    monkeypatch.setitem(
        namespace, "args", args_factory(useage=hours, dryrun_flag=True)
    )
    monkeypatch.setitem(namespace, "ch_client", FakeCH())

    with pytest.raises(s3gc_module["UserVisibleError"], match="below the 24 hour minimum"):
        s3gc_module["do_use"]()


def test_useage_at_the_floor_is_accepted(s3gc_module, args_factory, monkeypatch):
    sql = _antijoin_sql(s3gc_module, args_factory, monkeypatch, useage=24)

    assert "s3o.last_modified < now() - interval 24 hour" in sql


def test_useage_above_the_floor_is_accepted(s3gc_module, args_factory, monkeypatch):
    """The parameter stays useful upward: more caution must remain possible."""
    sql = _antijoin_sql(s3gc_module, args_factory, monkeypatch, useage=168)

    assert "s3o.last_modified < now() - interval 168 hour" in sql


def test_default_useage_is_the_safe_floor(monkeypatch):
    """A bare run must not opt itself out of the guard."""
    module = _load_with_env(monkeypatch)

    assert module["args"].useage == 24
    assert module["MINIMUM_USEAGE_HOURS"] == 24


def test_dev_flag_permits_a_short_window_but_says_so(
    s3gc_module, args_factory, monkeypatch, caplog
):
    """Development automation seeds and deletes fixtures within minutes.

    The escape hatch is reachable only through the dev-automation entrypoint
    phase, and a run that uses it must be impossible to mistake for a normal
    one -- hence the warning and the durable run-log row.
    """
    namespace = s3gc_module["do_use"].__globals__
    writer = RecordingCH()
    monkeypatch.setitem(
        namespace,
        "args",
        args_factory(useage=0, dryrun_flag=True, dev_allow_short_useage=True),
    )
    monkeypatch.setitem(namespace, "ch_client", FakeCH())
    monkeypatch.setitem(namespace, "ch_writer", writer)
    monkeypatch.setitem(namespace, "run_log_enabled", True)
    monkeypatch.setitem(namespace, "run_id", "dev-run")
    monkeypatch.setitem(namespace, "log_tname", "`aux_log`")

    with caplog.at_level("WARNING", logger="s3gc_test"):
        s3gc_module["do_use"]()

    assert "dev-allow-short-useage" in caplog.text
    events = [
        (row[3], row[4])
        for table, rows, _ in writer.inserts
        if table == "`aux_log`"
        for row in rows
    ]
    assert any(
        event == "warning" and "below the 24h minimum" in message
        for event, message in events
    )


def test_dry_run_performs_no_s3_operations(s3gc_module, args_factory, monkeypatch):
    """A dry run must be incapable of touching S3, even with candidates present."""
    namespace = s3gc_module["do_use"].__globals__

    class ExplodingMinio:
        def __getattr__(self, name):
            raise AssertionError(f"dry run must not call S3: {name}")

    client = FakeCH(blocks=[[("orphan-a", 10, "time"), ("orphan-b", 20, "time")]])
    monkeypatch.setitem(namespace, "args", args_factory(dryrun_flag=True))
    monkeypatch.setitem(namespace, "ch_client", client)
    monkeypatch.setitem(namespace, "minio_client", ExplodingMinio())

    s3gc_module["do_use"]()

    assert client.inserts == []


def test_delete_runs_preflight_before_touching_s3(
    s3gc_module, args_factory, monkeypatch
):
    """The cluster/replica preflight must gate the delete path, not decorate it."""
    namespace = s3gc_module["do_use"].__globals__

    class ExplodingMinio:
        def __getattr__(self, name):
            raise AssertionError(f"preflight must run before S3: {name}")

    def refuse():
        raise RuntimeError("preflight refused")

    monkeypatch.setitem(namespace, "args", args_factory(dryrun_flag=False))
    monkeypatch.setitem(namespace, "ch_client", FakeCH(blocks=[[("orphan", 1, "time")]]))
    monkeypatch.setitem(namespace, "minio_client", ExplodingMinio())
    monkeypatch.setitem(namespace, "preflight_cluster", refuse)

    with pytest.raises(RuntimeError, match="preflight refused"):
        s3gc_module["do_use"]()


def test_preflight_rejects_unexpected_replica_count(
    s3gc_module, args_factory, monkeypatch
):
    """A topology that does not match must fail closed before any deletion."""
    namespace = s3gc_module["preflight_cluster"].__globals__
    monkeypatch.setitem(namespace, "args", args_factory(expected_replicas=2))
    monkeypatch.setitem(namespace, "ch_client", FakeCH(cluster="cluster", replicas=3))

    with pytest.raises(RuntimeError, match="replica preflight failed"):
        s3gc_module["preflight_cluster"]()


def test_useafter_is_quoted_as_a_string_literal(
    s3gc_module, args_factory, monkeypatch
):
    sql = _antijoin_sql(s3gc_module, args_factory, monkeypatch, useafter="some/object")

    assert "s3o.objpath > 'some/object'" in sql


# ---------------------------------------------------------------------------
# Durable run log.
#
# Pod logs are not a record: the kubelet rotates container output and
# ttlSecondsAfterFinished deletes the Job with everything it printed. The same
# events are therefore appended to a ClickHouse table that outlives the pod.
#
# This is bookkeeping attached to an irreversible operation, so the tests below
# are mostly about what it must NOT do: never fail a run, never share the
# streaming session, never carry a credential into a table.
# ---------------------------------------------------------------------------


class RecordingCH(FakeCH):
    """A client that records DDL and TRUNCATE instead of rejecting them."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.commands = []

    def command(self, query):
        self.commands.append(query)
        if "COUNT(1)" in query:
            return 1
        return None


def _enable_run_log(s3gc_module, monkeypatch, writer, **arg_overrides):
    """Turn the run log on the way init_run_log() would, without a server."""
    namespace = s3gc_module["run_log"].__globals__
    monkeypatch.setitem(namespace, "ch_writer", writer)
    monkeypatch.setitem(namespace, "run_log_enabled", True)
    monkeypatch.setitem(namespace, "run_id", "test-run")
    return namespace


def test_run_log_table_is_created_beside_the_auxiliary_table(
    s3gc_module, args_factory, monkeypatch
):
    """One COLLECTTABLEPREFIX still identifies one cleanup."""
    namespace = s3gc_module["init_run_log"].__globals__
    writer = RecordingCH()
    monkeypatch.setitem(namespace, "args", args_factory())
    monkeypatch.setitem(namespace, "ch_writer", writer)
    monkeypatch.setitem(namespace, "log_tname", "`s3objects_for_s3_log`")

    s3gc_module["init_run_log"]()

    ddl = " ".join(writer.commands[0].split())
    assert "CREATE TABLE IF NOT EXISTS `s3objects_for_s3_log`" in ddl
    assert "ENGINE = MergeTree ORDER BY (run_id, event_time)" in ddl
    assert namespace["run_log_enabled"] is True


def test_run_log_uses_the_configured_run_id(s3gc_module, args_factory, monkeypatch):
    """The Kubernetes template passes JOB_NAME, so a row traces to its Job."""
    namespace = s3gc_module["init_run_log"].__globals__
    monkeypatch.setitem(namespace, "args", args_factory(runid="s3gc-delete-42"))
    monkeypatch.setitem(namespace, "ch_writer", RecordingCH())
    monkeypatch.setitem(namespace, "log_tname", "`aux_log`")

    s3gc_module["init_run_log"]()

    assert namespace["run_id"] == "s3gc-delete-42"


def test_missing_create_grant_degrades_to_stdout_only(
    s3gc_module, args_factory, monkeypatch, caplog
):
    """A cleanup must not fail because it could not create its own log table."""
    namespace = s3gc_module["init_run_log"].__globals__

    class NoGrantCH:
        def command(self, query):
            raise RuntimeError("Not enough privileges")

    monkeypatch.setitem(namespace, "args", args_factory())
    monkeypatch.setitem(namespace, "ch_writer", NoGrantCH())
    monkeypatch.setitem(namespace, "log_tname", "`aux_log`")

    with caplog.at_level("WARNING"):
        s3gc_module["init_run_log"]()

    assert namespace["run_log_enabled"] is False
    assert "run log unavailable" in caplog.text


def test_run_log_disabled_creates_no_table(s3gc_module, args_factory, monkeypatch):
    """--runlog false must not touch the cluster at all."""
    namespace = s3gc_module["init_run_log"].__globals__
    writer = RecordingCH()
    monkeypatch.setitem(namespace, "args", args_factory(runlog_flag=False))
    monkeypatch.setitem(namespace, "ch_writer", writer)

    s3gc_module["init_run_log"]()

    assert writer.commands == []
    assert namespace["run_log_enabled"] is False


def test_a_failed_run_log_write_never_fails_the_run(
    s3gc_module, args_factory, monkeypatch, caplog
):
    """The audit trail must not be able to kill a delete that is mid-flight."""
    attempts = []

    class BrokenWriter:
        def insert(self, *a, **k):
            attempts.append(1)
            raise RuntimeError("table went away")

    namespace = _enable_run_log(s3gc_module, monkeypatch, BrokenWriter())
    monkeypatch.setitem(namespace, "args", args_factory())
    monkeypatch.setitem(namespace, "log_tname", "`aux_log`")

    with caplog.at_level("WARNING"):
        s3gc_module["run_log"]("checkpoint", "batch done")
        s3gc_module["run_log"]("checkpoint", "another batch")

    # Disabled after the first failure, not retried once per batch for hours.
    assert attempts == [1]
    assert namespace["run_log_enabled"] is False
    assert "disabling run log" in caplog.text


def test_run_log_redacts_secrets(s3gc_module, args_factory, monkeypatch):
    """A credential must not reach a table that outlives the run."""
    writer = RecordingCH()
    namespace = _enable_run_log(s3gc_module, monkeypatch, writer)
    monkeypatch.setitem(namespace, "args", args_factory())
    monkeypatch.setitem(namespace, "log_tname", "`aux_log`")
    monkeypatch.setattr(
        s3gc_module["LogFormatter"], "filter_strings", ["super-secret-key"]
    )

    s3gc_module["run_log"]("error", "failed with key super-secret-key in it")

    message = writer.inserts[0][1][0][4]
    assert "super-secret-key" not in message
    assert "****" in message


def test_run_log_writes_off_the_streaming_session(
    s3gc_module, args_factory, monkeypatch
):
    """Same rule as tombstones: never ch_client while the anti-join streams.

    A run-log insert on the streaming client is a second query on a held
    session, which ClickHouse rejects with SESSION_IS_LOCKED (373) — and it
    would do so mid-delete, the worst possible moment.
    """
    namespace = s3gc_module["do_use"].__globals__
    streaming = StreamingCH(blocks=[[("orphan-a", 10, "time")]])
    writer = RecordingCH()

    class SuccessfulMinio:
        def remove_objects(self, bucket, objects):
            return iter(())

    monkeypatch.setitem(namespace, "args", args_factory())
    monkeypatch.setitem(namespace, "ch_client", streaming)
    monkeypatch.setitem(namespace, "ch_writer", writer)
    monkeypatch.setitem(namespace, "minio_client", SuccessfulMinio())
    monkeypatch.setitem(namespace, "run_log_enabled", True)
    monkeypatch.setitem(namespace, "run_id", "test-run")
    monkeypatch.setitem(namespace, "log_tname", "`s3objects_for_s3_log`")

    s3gc_module["do_use"]()

    assert streaming.inserts == []
    tables = [insert[0] for insert in writer.inserts]
    assert "`s3objects_for_s3`" in tables          # tombstone
    assert "`s3objects_for_s3_log`" in tables      # run-log rows


def test_delete_batches_are_recorded_durably(s3gc_module, args_factory, monkeypatch):
    """If the pod is gone, these rows are what say how far the delete got."""
    namespace = s3gc_module["do_use"].__globals__
    client = RecordingCH(blocks=[[("orphan-a", 10, "time"), ("orphan-b", 20, "time")]])

    class SuccessfulMinio:
        def remove_objects(self, bucket, objects):
            return iter(())

    monkeypatch.setitem(namespace, "args", args_factory(deletebatchsize=1))
    monkeypatch.setitem(namespace, "ch_client", client)
    monkeypatch.setitem(namespace, "ch_writer", client)
    monkeypatch.setitem(namespace, "minio_client", SuccessfulMinio())
    monkeypatch.setitem(namespace, "run_log_enabled", True)
    monkeypatch.setitem(namespace, "run_id", "test-run")
    monkeypatch.setitem(namespace, "log_tname", "`aux_log`")

    s3gc_module["do_use"]()

    events = [
        (row[3], row[5], row[6])
        for table, rows, _ in client.inserts
        if table == "`aux_log`"
        for row in rows
    ]
    checkpoints = [event for event in events if event[0] == "checkpoint"]
    assert len(checkpoints) == 2
    # Running totals, so a truncated log still shows how far it got.
    assert checkpoints[0][1] == 1 and checkpoints[1][1] == 2
    assert checkpoints[-1][2] == 30
    assert any(event[0] == "finish" for event in events)


def test_run_log_table_is_never_truncated(s3gc_module, args_factory, monkeypatch):
    """The aux table is scratch space; the run log is the record. Only one is wiped."""
    namespace = s3gc_module["do_use"].__globals__
    client = RecordingCH(blocks=[])

    monkeypatch.setitem(
        namespace, "args", args_factory(keepdata_flag=False, dryrun_flag=False)
    )
    monkeypatch.setitem(namespace, "ch_client", client)
    monkeypatch.setitem(namespace, "ch_writer", client)
    monkeypatch.setitem(namespace, "run_log_enabled", True)
    monkeypatch.setitem(namespace, "run_id", "test-run")
    monkeypatch.setitem(namespace, "log_tname", "`aux_log`")

    s3gc_module["do_use"]()

    truncates = [query for query in client.commands if "TRUNCATE" in query]
    assert truncates == ["TRUNCATE TABLE `s3objects_for_s3`"]
    assert not any("aux_log" in query for query in truncates)


def test_run_log_records_the_scope_of_the_run(s3gc_module, args_factory, monkeypatch):
    """Every row is self-describing evidence, not just a message."""
    writer = RecordingCH()
    namespace = _enable_run_log(s3gc_module, monkeypatch, writer)
    monkeypatch.setitem(
        namespace,
        "args",
        args_factory(s3bucket="the-bucket", s3path="pre/fix/", s3diskname="gcs",
                     clustername="prod", dryrun_flag=True, chhost="replica-0"),
    )
    monkeypatch.setitem(namespace, "log_tname", "`aux_log`")

    s3gc_module["run_log"]("start", "dry-run phase started", phase="run")

    row = dict(zip(s3gc_module["RUN_LOG_COLUMNS"], writer.inserts[0][1][0]))
    assert row["run_id"] == "test-run"
    assert row["phase"] == "run" and row["event"] == "start"
    assert row["s3bucket"] == "the-bucket" and row["s3path"] == "pre/fix/"
    assert row["s3diskname"] == "gcs" and row["clustername"] == "prod"
    assert row["dryrun"] is True and row["chhost"] == "replica-0"


def test_collect_reports_progress_for_long_runs(
    s3gc_module, args_factory, monkeypatch, caplog
):
    """A multi-hour collect used to emit nothing at INFO until it finished."""
    import datetime

    namespace = s3gc_module["do_collect"].__globals__
    now = datetime.datetime.now(datetime.timezone.utc)

    class Obj:
        def __init__(self, name):
            self.object_name = name
            self.size = 1
            self.last_modified = now - datetime.timedelta(days=2)

    class Minio:
        def list_objects(self, bucket, prefix, recursive, start_after):
            return iter(Obj(f"object-{index}") for index in range(200_000))

    writer = RecordingCH()
    monkeypatch.setitem(
        namespace,
        "args",
        args_factory(age=0, collectbatchsize=50_000, total=None, collectafter="",
                     s3path="", createdatabase_flag=False,
                     drop_collecttable_flag=False, samples=4),
    )
    monkeypatch.setitem(namespace, "minio_client", Minio())
    monkeypatch.setitem(namespace, "ch_client", RecordingCH())
    monkeypatch.setitem(namespace, "ch_writer", writer)
    monkeypatch.setitem(namespace, "tname", "`aux`")
    monkeypatch.setitem(namespace, "log_tname", "`aux_log`")
    monkeypatch.setitem(namespace, "run_log_enabled", True)
    monkeypatch.setitem(namespace, "run_id", "test-run")

    with caplog.at_level("INFO", logger="s3gc_test"):
        s3gc_module["do_collect"]()

    assert "collect progress" in caplog.text
    events = [
        row[3] for table, rows, _ in writer.inserts if table == "`aux_log`" for row in rows
    ]
    assert events.count("progress") == 2      # throttled to every 100k
    assert events[-1] == "finish"


@pytest.mark.parametrize(
    ("replacement", "message"),
    [
        (("RUNLOG=true", "RUNLOG=maybe"), "RUNLOG must be true or false"),
        (("RUNLOG=true", "RUNLOG="), "RUNLOG must be true or false"),
    ],
)
def test_renderer_validates_runlog(tmp_path, replacement, message):
    source = (ROOT / "deploy/kubernetes/example.env").read_text()
    config_path = tmp_path / "runlog.env"
    config_path.write_text(source.replace(*replacement))

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True, text=True, check=False,
    )

    assert result.returncode == 64
    assert message in result.stderr


def test_renderer_defaults_runlog_on_for_legacy_env_files(tmp_path):
    """Pre-existing env files must keep rendering, with the run log enabled."""
    source = (ROOT / "deploy/kubernetes/example.env").read_text()
    without = "\n".join(
        line for line in source.splitlines() if not line.startswith("RUNLOG=")
    )
    config_path = tmp_path / "legacy.env"
    config_path.write_text(without)

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True, text=True, check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "S3GC_RUNLOG_FLAG" in result.stdout
    assert 'value: "true"' in result.stdout


def test_renderer_uses_the_job_name_as_the_run_id(tmp_path):
    """A run-log row must trace back to the Job that wrote it."""
    config_path = tmp_path / "runid.env"
    config_path.write_text((ROOT / "deploy/kubernetes/example.env").read_text())

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True, text=True, check=False,
    )

    assert result.returncode == 0
    assert "- name: S3GC_RUNID" in result.stdout
    assert 'value: "s3gc-example-dry-run"' in result.stdout


@pytest.mark.parametrize("hours", ["0", "1", "23"])
def test_renderer_rejects_useage_below_the_floor(tmp_path, hours):
    """A bad window must fail at render time, not after a Job is applied."""
    source = (ROOT / "deploy/kubernetes/example.env").read_text()
    config_path = tmp_path / "short.env"
    config_path.write_text(source.replace("USEAGE_HOURS=24", f"USEAGE_HOURS={hours}"))

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True, text=True, check=False,
    )

    assert result.returncode == 64
    assert "USEAGE_HOURS must be at least 24" in result.stderr


def test_renderer_allows_a_short_window_for_dev_automation(tmp_path):
    """The one non-production phase, already documented as such."""
    source = (ROOT / "deploy/kubernetes/example.env").read_text()
    config_path = tmp_path / "dev.env"
    config_path.write_text(
        source.replace("USEAGE_HOURS=24", "USEAGE_HOURS=0")
        .replace("PHASE=dry-run", "PHASE=dev-automation")
        .replace("DELETE_CONFIRMATION=", "DELETE_CONFIRMATION=DELETE_ORPHANS")
    )

    result = subprocess.run(
        [sys.executable, str(ROOT / "deploy/kubernetes/render.py"), config_path],
        capture_output=True, text=True, check=False,
    )

    assert result.returncode == 0, result.stderr
    assert 'value: "0"' in result.stdout


def test_only_dev_automation_passes_the_short_window_flag(tmp_path):
    """The prod phases must never hand s3gc the escape hatch."""
    calls_path = tmp_path / "calls"
    fake_python = tmp_path / "python"
    fake_python.write_text(
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$CALLS_PATH\"\n"
    )
    fake_python.chmod(0o755)

    def run(phase):
        calls_path.write_text("")
        subprocess.run(
            ["sh", str(ROOT / "docker/kubernetes-entrypoint.sh")],
            env={
                **os.environ,
                "PATH": f"{tmp_path}:{os.environ['PATH']}",
                "CALLS_PATH": str(calls_path),
                "S3GC_PHASE": phase,
                "S3GC_DELETE_CONFIRMATION": "DELETE_ORPHANS",
                "S3GC_CLUSTERNAME": "cluster",
                "S3GC_EXPECTED_REPLICAS": "2",
            },
            capture_output=True, text=True, check=False,
        )
        return calls_path.read_text()

    for phase in ("collect", "dry-run", "delete"):
        assert "--dev-allow-short-useage" not in run(phase), phase
    dev_calls = run("dev-automation")
    assert dev_calls.count("--dev-allow-short-useage=true") == 2
    assert "--dev-allow-short-useage\n" not in dev_calls


def test_useafter_escapes_quotes(s3gc_module, args_factory, monkeypatch):
    """An object name containing a quote must not terminate the literal."""
    sql = _antijoin_sql(
        s3gc_module, args_factory, monkeypatch, useafter="odd'name"
    )

    assert r"s3o.objpath > 'odd\'name'" in sql


def test_topology_is_rechecked_for_every_sample(
    s3gc_module, args_factory, monkeypatch
):
    """The preflight is point-in-time; this loop can run for hours.

    A replica that drops out mid-run takes its references with it, so blobs it
    alone holds start looking orphaned. Re-check before each sample and fail
    closed rather than delete against a shrunken reference scope.
    """
    namespace = s3gc_module["do_use"].__globals__

    class ShrinkingCluster(FakeCH):
        """Loses a replica after the first sample's preflight."""

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.preflights = 0

        def query(self, query):
            if "clusterAllReplicas" in query and "system.one" in query:
                self.preflights += 1
                return QueryResult(2 if self.preflights == 1 else 1)
            return super().query(query)

    class SuccessfulMinio:
        def remove_objects(self, bucket, objects):
            return iter(())

    client = ShrinkingCluster(blocks=[])
    monkeypatch.setitem(namespace, "args", args_factory(samples=2))
    monkeypatch.setitem(namespace, "ch_client", client)
    monkeypatch.setitem(namespace, "ch_writer", RecordingCH())
    monkeypatch.setitem(namespace, "minio_client", SuccessfulMinio())

    with pytest.raises(RuntimeError, match="replica preflight failed"):
        s3gc_module["do_use"]()

    # Once up front, then again before sample 0 -- the earliest re-check, which
    # is where the shrunken topology is caught. The run never reaches sample 1.
    assert client.preflights == 2


def test_dry_run_does_not_require_a_cluster_preflight(
    s3gc_module, args_factory, monkeypatch
):
    """Reading is safe; only the destructive path needs the topology check."""
    namespace = s3gc_module["do_use"].__globals__

    def refuse():
        raise AssertionError("dry run must not require preflight")

    monkeypatch.setitem(namespace, "args", args_factory(dryrun_flag=True, samples=2))
    monkeypatch.setitem(namespace, "ch_client", FakeCH())
    monkeypatch.setitem(namespace, "preflight_cluster", refuse)

    s3gc_module["do_use"]()


def test_image_disables_stdout_buffering():
    """A Job killed at activeDeadlineSeconds used to lose its buffered tail.

    stdout is a pipe under Kubernetes so print() is block-buffered, and Python's
    default SIGTERM handling exits without flushing -- taking the closing
    "s3gc: OK" with it, and interleaving the dev-automation shell echoes wrongly
    against the Python output.
    """
    dockerfile = (ROOT / "docker/Dockerfile").read_text()

    assert "ENV PYTHONUNBUFFERED=1" in dockerfile
