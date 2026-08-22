"""
The script removes orphaned objects from s3 object storage
  Ones that are not mentioned in system.remote_data_paths table

There are two stages:
1. Collecting.
     Paths of all objects found in object storage are put in auxiliary ClickHouse table.
       It's name is a concatenation of 's3objects_for_' and disk name by default.
       Created in the same ClickHouse instance where data from system.remote_data_paths selected
2. Removing.
     All objects that exist in s3 and not used according to system.remote_data_paths
       are removed from object storage.

It is possible to split these stages or do everything at one go.
"""

import os
import sys
from io import StringIO
from minio import Minio
from minio.deleteobjects import DeleteObject
from minio.credentials import IamAwsProvider
from minio.error import S3Error
from contextlib import redirect_stdout
import clickhouse_connect

from jsonargparse import (
    ArgumentParser,
    ActionConfigFile,
)
from jsonargparse.typing import Optional

import urllib3
import logging
import datetime
import socket
import uuid

usage = """
    s3 garbage collector for ClickHouse
    example: $ ./s3gc.py
"""


def strtobool(value):
    """Minimal stdlib-compatible replacement for distutils.util.strtobool."""
    normalized = value.lower()
    if normalized in {"y", "yes", "t", "true", "on", "1"}:
        return 1
    if normalized in {"n", "no", "f", "false", "off", "0"}:
        return 0
    raise ValueError(f"invalid truth value {value!r}")


def coerce_bool(value):
    """Normalise anything an option may arrive as into a real bool.

    Flags declared with action="store_true" are set to a real bool on the command
    line, but jsonargparse populates them from the environment as the RAW STRING.
    Every non-empty string is truthy in Python, so S3GC_DRYRUN_FLAG=false used to
    mean *true*. Treat unset/empty as false and parse the usual spellings.
    """
    if isinstance(value, bool):
        return value
    if value is None or value == "":
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    return bool(strtobool(str(value)))

parser = ArgumentParser(
    usage=usage, env_prefix="S3GC", default_env=True, exit_on_error=False
)

parser.add_argument(
    "--chhost",
    "--ch-host",
    dest="chhost",
    default="localhost",
    help="ClickHouse host to connect to",
)
parser.add_argument(
    "--chport",
    "--ch-port",
    dest="chport",
    default=8123,
    help="ClickHouse port to connect to",
)
parser.add_argument(
    "--chuser",
    "--ch-user-name",
    "--chusername",
    dest="chuser",
    default="default",
    help="ClickHouse user name",
)
parser.add_argument(
    "--chpass",
    "--ch-pass",
    "--ch-password",
    dest="chpass",
    default="",
    help="ClickHouse user password",
)
parser.add_argument(
    "--s3ip",
    "--s3-ip",
    dest="s3ip",
    default="127.0.0.1",
    help="S3 API ip address or host",
)
parser.add_argument(
    "--s3port", "--s3-port", dest="s3port", default=9001, help="S3 API port"
)
parser.add_argument(
    "--s3bucket", "--s3-bucket", dest="s3bucket", default="root", help="S3 bucket name"
)
parser.add_argument(
    "--s3path", "--s3-path", dest="s3path", default="data/", help="S3 path prefix"
)
parser.add_argument(
    "--s3-access-key",
    "--s3accesskey",
    dest="s3accesskey",
    default="",
    help="S3 access key",
)
parser.add_argument(
    "--s3-secret-key",
    "--s3secretkey",
    dest="s3secretkey",
    default="",
    help="S3 secret key",
)
parser.add_argument(
    "--s3-session-token",
    "--s3sessiontoken",
    dest="s3sessiontoken",
    default="",
    help="S3 session token for explicit temporary credentials",
)
parser.add_argument(
    "--s3auth",
    "--s3-auth",
    dest="s3auth",
    default="static",
    help=(
        "S3 auth mode: static, aws or iam. static uses explicit keys (optionally with a "
        "session token); aws uses the boto3 credential chain, including AWS SSO profiles; "
        "iam uses MinIO's workload identity provider (IRSA/IMDS/ECS) and needs no boto3"
    ),
)
parser.add_argument(
    "--s3profile",
    "--s3-profile",
    dest="s3profile",
    default="",
    help="AWS profile name for S3 auth. Setting this enables aws auth mode",
)
parser.add_argument(
    "--s3secure",
    "--s3-secure",
    action="store_true",
    dest="s3secure_flag",
    default=False,
    help="S3 secure mode",
)
parser.add_argument(
    "--s3secureflag",
    "--s3-secure-flag",
    type=coerce_bool,
    dest="s3secure_flag",
    default=False,
    help="S3 secure mode",
)
parser.add_argument(
    "--s3sslcertfile",
    "--s3-ssl-cert-file",
    dest="s3sslcertfile",
    default="",
    help="SSL certificate for S3",
)
parser.add_argument(
    "--s3region",
    "--s3-region",
    dest="s3region",
    type=Optional[str],
    help="S3 Region",
)
parser.add_argument(
    "--s3diskname",
    "--s3-disk-name",
    dest="s3diskname",
    default="s3",
    help="S3 disk name",
)
parser.add_argument(
    "--keepdata",
    "--keep-data",
    action="store_true",
    dest="keepdata_flag",
    default=False,
    help="keep auxiliary data in ClickHouse table",
)
parser.add_argument(
    "--keepdataflag",
    "--keep-data-flag",
    type=coerce_bool,
    dest="keepdata_flag",
    default=False,
    help="keep auxiliary data in ClickHouse table",
)
parser.add_argument(
    "--collectonly",
    "--collect-only",
    action="store_true",
    dest="collectonly_flag",
    default=False,
    help="put object names to auxiliary table",
)
parser.add_argument(
    "--collectonlyflag",
    "--collect-only-flag",
    type=coerce_bool,
    dest="collectonly_flag",
    default=False,
    help="put object names to auxiliary table",
)
parser.add_argument(
    "--usecollected",
    "--use-collected",
    action="store_true",
    dest="usecollected_flag",
    default=False,
    help="auxiliary data is already collected in ClickHouse table",
)
parser.add_argument(
    "--usecollectedflag",
    "--use-collected-flag",
    type=coerce_bool,
    dest="usecollected_flag",
    default=False,
    help="auxiliary data is already collected in ClickHouse table",
)
parser.add_argument(
    "--collecttableprefix",
    "--collect-table-prefix",
    dest="collecttableprefix",
    default="s3objects_for_",
    help="prefix for table name to keep data about objects (database is allowed, if not exists, specify --create-database)",
)
parser.add_argument(
    "--collectbatchsize",
    "--collect-batch-size",
    dest="collectbatchsize",
    type=int,
    default=1024,
    help="number of rows to insert to ClickHouse at once",
)
parser.add_argument(
    "--total",
    "--collecttotal",
    "--collect-total",
    "--total-num",
    dest="total",
    type=Optional[int],
    help="Number of objects to collect. Can be used in conjunction with start-after",
)
parser.add_argument(
    "--collectafter",
    "--collect-after",
    dest="collectafter",
    type=Optional[str],
    help="Object name to start after. If not specified, traversing objects from the beginning",
)
parser.add_argument(
    "--useafter",
    "--use-after",
    dest="useafter",
    type=Optional[str],
    help="Object name to start processing already collected objects after. If not specified, traversing objects from the beginning",
)
parser.add_argument(
    "--usetotal",
    "--use-total",
    dest="usetotal",
    type=Optional[int],
    help="Number of already collected objects to process. Can be used in conjunction with use-after",
)
parser.add_argument(
    "--dryrun",
    "--dry-run",
    action="store_true",
    dest="dryrun_flag",
    help="Calculate objects to remove without actual removing",
)
parser.add_argument(
    "--dryrunflag",
    "--dryrun-flag",
    "--dry-run-flag",
    type=coerce_bool,
    dest="dryrun_flag",
    default=False,
    help="Calculate objects to remove without actual removing",
)
parser.add_argument(
    "--cluster",
    "--cluster-name",
    "--clustername",
    dest="clustername",
    default="",
    help="Consider an objects unused if there is no host in the cluster refers the object",
)
parser.add_argument(
    "--expected-replicas",
    dest="expected_replicas",
    type=Optional[int],
    help="Fail before deleting when clusterAllReplicas() does not return this many replicas",
)
parser.add_argument(
    "--age",
    "--hours",
    "--age-hours",
    "--collectage",
    "--collecthours",
    "--age-hours",
    dest="age",
    type=int,
    default=0,
    help="Process only objects older than specified number of hours",
)
parser.add_argument(
    "--useage",
    "--usehours",
    "--useage-hours",
    dest="useage",
    type=int,
    default=24,
    help=(
        "Process only already collected objects older than specified number of "
        "hours. Minimum 24: below that a run can delete a part between its blob "
        "upload and its registration in system.remote_data_paths"
    ),
)
parser.add_argument(
    "--samples",
    dest="samples",
    type=int,
    default=4,
    help="Number of partitions in auxiliary table",
)
parser.add_argument(
    "--deletebatchsize",
    "--delete-batch-size",
    dest="deletebatchsize",
    type=int,
    default=1000,
    help="S3 objects to delete and checkpoint per progress batch",
)
parser.add_argument(
    "--order-by-objpath",
    action="store_true",
    dest="order_by_objpath",
    default=False,
    help="Order anti-join output by object path (costly for large Kubernetes Jobs)",
)
parser.add_argument(
    "--order-by-objpath-flag",
    dest="order_by_objpath",
    type=coerce_bool,
    default=False,
    help="Order anti-join output by object path (costly for large Kubernetes Jobs)",
)
parser.add_argument(
    "--s3-connect-timeout",
    dest="s3_connect_timeout",
    type=int,
    default=15,
    help="S3 connection timeout in seconds",
)
parser.add_argument(
    "--s3-read-timeout",
    dest="s3_read_timeout",
    type=int,
    default=120,
    help="S3 read timeout in seconds",
)
parser.add_argument(
    "--s3-retries",
    dest="s3_retries",
    type=int,
    default=3,
    help="S3 HTTP retries for transient failures",
)
parser.add_argument(
    "--chtimeout",
    "--ch-timeout",
    "--send-receive-timeout",
    "--ch-send-receive-timeout",
    dest="chtimeout",
    type=int,
    default=1800,
    help="clickhouse send/receive timeout in seconds",
)
parser.add_argument(
    "--create-database",
    "--createdatabase",
    action="store_true",
    dest="createdatabase_flag",
    default=False,
    help="create database for collecttable",
)
parser.add_argument(
    "--create-database-flag",
    "--createdatabase-flag",
    dest="createdatabase_flag",
    type=coerce_bool,
    default=False,
    help="create database for collecttable",
)
parser.add_argument(
    "--drop-collecttable",
    "--dropcollecttable",
    action="store_true",
    dest="drop_collecttable_flag",
    default=False,
    help="drop collecttable and recreate; beware of ClickHouse DROP TABLE constraints",
)
parser.add_argument(
    "--drop-collecttable-flag",
    "--dropcollecttable-flag",
    dest="drop_collecttable_flag",
    type=coerce_bool,
    default=False,
    help="drop collecttable and recreate; beware of ClickHouse DROP TABLE constraints",
)
parser.add_argument(
    "--useremoveobjects",
    "--use-remove-objects",
    dest="use_remove_objects",
    type=coerce_bool,
    default=True,
    help="use remove_objects (not supported by GCE). Set it to false to use remove_object",
)
parser.add_argument(
    "--non-interactive",
    "--noninteractive",
    action="store_false",
    dest="interactive_flag",
    default=True,
    help="confirm deleting",
)
parser.add_argument(
    "--interactive-flag",
    dest="interactive_flag",
    type=coerce_bool,
    default=True,
    help="confirm deleting",
)
parser.add_argument(
    "--verbose",
    action="store_true",
    dest="verbose_flag",
    default=False,
    help="debug output",
)
parser.add_argument(
    "--verboseflag",
    "--verbose-flag",
    type=coerce_bool,
    dest="verbose_flag",
    default=False,
    help="debug output",
)
parser.add_argument(
    "--debug",
    action="store_true",
    dest="debug_flag",
    default=False,
    help="trace output (more verbose)",
)
parser.add_argument(
    "--debugflag",
    "--debug-flag",
    type=coerce_bool,
    dest="debug_flag",
    default=False,
    help="trace output (more verbose)",
)
parser.add_argument(
    "--silent", action="store_true", dest="silent_flag", default=False, help="no log"
)
parser.add_argument(
    "--silentflag",
    "--silent-flag",
    dest="silent_flag",
    type=coerce_bool,
    default=False,
    help="no log",
)
parser.add_argument(
    "--listoptions",
    "--list-options",
    action="store_true",
    dest="listoptions",
    default=False,
    help="list all command line options for internal purposes",
)

parser.add_argument(
    "--dev-allow-short-useage",
    dest="dev_allow_short_useage",
    type=coerce_bool,
    default=False,
    help=(
        "development only: permit --useage below the 24 hour minimum. Passed only "
        "by the dev-automation entrypoint phase, which seeds and deletes its own "
        "fixtures within minutes. Never set this for customer or production work"
    ),
)
parser.add_argument(
    "--runlog",
    "--run-log",
    dest="runlog_flag",
    type=coerce_bool,
    default=True,
    help=(
        "record durable run events in a ClickHouse table alongside the auxiliary "
        "table. Pod logs are ephemeral: they are rotated by the kubelet and deleted "
        "with the Job. Set false to keep stdout as the only record"
    ),
)
parser.add_argument(
    "--runid",
    "--run-id",
    dest="runid",
    default="",
    help=(
        "identifier recorded with every run-log row. Defaults to a generated "
        "timestamped id; the Kubernetes Job template passes the Job name so a row "
        "traces back to the Job that wrote it"
    ),
)

parser.add_argument("--cfg", action=ActionConfigFile)


# out = get_parse_args_stdout(parser, ["--print_config"])
# print(out)

args = parser.parse_args()

# Every flag declared with action="store_true" arrives from the environment as a
# raw string, and every non-empty string is truthy. Normalise all boolean
# options in one place, immediately after parsing, so the rest of the program
# can rely on real bools.
BOOLEAN_DESTS = (
    "s3secure_flag",
    "runlog_flag",
    "dev_allow_short_useage",
    "use_remove_objects",
    "keepdata_flag",
    "collectonly_flag",
    "usecollected_flag",
    "dryrun_flag",
    "order_by_objpath",
    "createdatabase_flag",
    "drop_collecttable_flag",
    "verbose_flag",
    "debug_flag",
    "silent_flag",
    "listoptions",
)

for _dest in BOOLEAN_DESTS:
    if not hasattr(args, _dest):
        continue
    _raw = getattr(args, _dest)
    try:
        setattr(args, _dest, coerce_bool(_raw))
    except ValueError:
        parser.error(
            f"invalid boolean value {_raw!r} for {_dest} "
            f"(environment variable S3GC_{_dest.upper()}); "
            "use one of true/false, yes/no, on/off, 1/0"
        )

if args.listoptions:
    with redirect_stdout(StringIO()) as f:
        try:
            parser.parse_args(["--print_config"])
        except SystemExit:
            pass

    print("ENV ", end="")
    backslash = False
    for ln in f.getvalue().splitlines():
        (key, value) = ln.split(": ")
        # if (key in ['listoptions', 'verbose', 'debug', 'silent', 'keepdata', 'collectonly', 'usecollected']):
        if key in ["listoptions"]:
            continue
        if backslash:
            print(" \\")
        print(f" S3GC_{key.upper()}={value}", end="")

        backslash = True

    print()
    exit()

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)  # set logger level
if args.verbose_flag:
    logger.setLevel(logging.INFO)
if args.debug_flag:
    logger.setLevel(logging.DEBUG)
if args.silent_flag:
    logger.setLevel(logging.CRITICAL)


##############################################################
class LogFormatter(logging.Formatter):
    """Formatter that wipes passwords if they are longer than 3 characters."""

    def get_filter_strings():
        filter_strings = []
        for secret in [
            args.chpass,
            args.s3accesskey,
            args.s3secretkey,
            args.s3sessiontoken,
        ]:
            if len(secret) > 3:
                filter_strings.append(secret)
        return filter_strings

    filter_strings = get_filter_strings()

    @staticmethod
    def _filter(s):
        for fs in LogFormatter.filter_strings:
            s = s.replace(fs, "****")

        return s

    def format(self, record):
        original = logging.Formatter.format(self, record)
        return self._filter(original) if len(self.filter_strings) else original


logFormatter = LogFormatter("%(asctime)s %(levelname)s %(message)s")


def graceful_exit():
    if not args.silent_flag:
        print("s3gc: OK")
    exit()


##############################################################


consoleHandler = logging.StreamHandler(sys.stdout)  # set streamhandler to stdout
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

logger.debug(f"Parameters: {args}")

tname = ""
dbname = None

dbparts = args.collecttableprefix.split(".")
if len(dbparts) > 2:
    raise ValueError("invalid collecttableprefix")
elif len(dbparts) == 2:
    dbname = f"`{dbparts[0]}`"
    tname = f"{dbname}.`{dbparts[1]}{args.s3diskname}`"
else:
    tname = f"`{dbparts[0]}{args.s3diskname}`"

# The run-log table lives beside the auxiliary table and follows the same naming
# convention, so one COLLECTTABLEPREFIX still identifies one cleanup. Unlike the
# auxiliary table it is NEVER truncated: it is the durable record of what the
# cleanup did after the pod and its logs are gone.
if dbname:
    log_tname = f"{dbname}.`{dbparts[1]}{args.s3diskname}_log`"
else:
    log_tname = f"`{dbparts[0]}{args.s3diskname}_log`"

minio_client = None
ch_client = None
# A second ClickHouse client, used only for writes issued while a result
# stream from ch_client is still open. See connect_to_ch().
ch_writer = None


# ClickHouse uploads a part's blobs to S3 and registers them in
# system.remote_data_paths a moment later. In that window a live blob is absent
# from the reference table and looks orphaned, and there is no per-object
# re-check before the S3 delete — so the age window is the ONLY thing standing
# between a run and live data.
#
# 24 hours is far longer than any part write, and short enough to stay useful.
# It is a floor, not a default to be talked down: a run configured below it is
# refused rather than warned about.
MINIMUM_USEAGE_HOURS = 24


class S3DeletionError(RuntimeError):
    """A delete failed after successful deletions were checkpointed."""


##############################################################
# Durable run log.
#
# Pod logs are not a record. The kubelet rotates container output (10Mi by
# default), so `kubectl logs` cannot return the beginning of a long run, and
# ttlSecondsAfterFinished deletes the Job and its pods along with everything
# they printed. A cleanup that reclaimed terabytes left no evidence of what it
# did once that window closed.
#
# So the same events are appended to a ClickHouse table. Three rules, each with
# a test, because this is bookkeeping attached to an irreversible operation:
#
#   1. Writes go on ch_writer, NEVER ch_client. do_use() holds ch_client's
#      session for the whole anti-join stream, and a second query on a held
#      session is SESSION_IS_LOCKED (373). Same reason tombstones live there.
#   2. A logging failure NEVER fails the run. One failure disables the run log
#      for the remainder of the process rather than retrying every batch: a
#      delete that is mid-flight must not die over its own audit trail.
#   3. Messages are redacted through LogFormatter._filter before insert, so a
#      credential cannot reach a table that outlives the run.
##############################################################

RUN_LOG_COLUMNS = [
    "event_time",
    "run_id",
    "phase",
    "event",
    "message",
    "objects",
    "bytes",
    "s3bucket",
    "s3path",
    "s3diskname",
    "clustername",
    "dryrun",
    "chhost",
    "hostname",
]

run_log_enabled = False
run_id = ""


def resolve_run_id():
    """A stable identifier for every row this process writes."""
    if args.runid:
        return args.runid
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid.uuid4().hex[:8]}"


def init_run_log():
    """Create the run-log table. Degrade to stdout-only rather than failing."""
    global run_log_enabled, run_id
    if not args.runlog_flag:
        logger.debug("run log disabled by --runlog false")
        return

    run_id = resolve_run_id()
    try:
        ch_writer.command(
            f"""CREATE TABLE IF NOT EXISTS {log_tname} (
            event_time DateTime64(3),
            run_id String,
            phase LowCardinality(String),
            event LowCardinality(String),
            message String,
            objects UInt64,
            bytes UInt64,
            s3bucket String,
            s3path String,
            s3diskname LowCardinality(String),
            clustername String,
            dryrun Bool,
            chhost String,
            hostname String
            ) ENGINE = MergeTree ORDER BY (run_id, event_time)"""
        )
    except Exception as exc:
        # Most likely a missing CREATE TABLE grant. The cleanup itself is
        # unaffected, so say so once and carry on without the durable record.
        logger.warning(
            f"run log unavailable, continuing with stdout only: {exc}. "
            f"Grant CREATE TABLE on {log_tname} to record durable run history, "
            "or pass --runlog false to silence this."
        )
        return

    run_log_enabled = True
    logger.info(f"run log: {log_tname}, run_id={run_id}")


def run_log(event, message="", objects=0, bytes_=0, phase="run"):
    """Append one durable event row. Never raises, never fails the run."""
    global run_log_enabled
    if not run_log_enabled:
        return
    try:
        ch_writer.insert(
            log_tname,
            [[
                datetime.datetime.now(datetime.timezone.utc),
                run_id,
                phase,
                event,
                LogFormatter._filter(str(message)),
                int(objects),
                int(bytes_),
                args.s3bucket,
                args.s3path,
                args.s3diskname,
                args.clustername,
                bool(args.dryrun_flag),
                args.chhost,
                socket.gethostname(),
            ]],
            column_names=RUN_LOG_COLUMNS,
        )
    except Exception as exc:
        # Disable rather than retry: a per-batch failure would otherwise repeat
        # for every batch of a multi-hour delete.
        run_log_enabled = False
        logger.warning(f"run log write failed, disabling run log for this run: {exc}")


def current_phase():
    """The phase name an operator would recognise from the Job that is running."""
    if args.collectonly_flag:
        return "collect"
    if args.dryrun_flag:
        return "dry-run"
    if args.usecollected_flag:
        return "delete"
    return "collect+use"


def quote_sql_string(value):
    """Render a Python string as a ClickHouse string literal.

    --useafter was interpolated bare, so an operator-supplied value landed as an
    identifier rather than a literal. Fail-closed in practice, but it was the
    only unquoted value in the anti-join WHERE clause.
    """
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _query_single_value(query):
    result = ch_client.query(query)
    if not result.result_rows or not result.result_rows[0]:
        raise RuntimeError(f"ClickHouse returned no result for preflight query: {query}")
    return result.result_rows[0][0]


def preflight_cluster():
    """Make destructive cluster-wide cleanup fail closed when topology is unexpected."""
    if not args.expected_replicas:
        return
    if not args.clustername:
        raise ValueError("--expected-replicas requires --cluster")

    actual_cluster = _query_single_value("SELECT getMacro('cluster')")
    if actual_cluster != args.clustername:
        raise RuntimeError(
            f"cluster preflight failed: expected local cluster macro {args.clustername!r}, "
            f"got {actual_cluster!r}"
        )

    cluster_name = args.clustername.replace("'", "\\\\'")
    actual_replicas = _query_single_value(
        f"SELECT count() FROM clusterAllReplicas('{cluster_name}', system.one)"
    )
    if actual_replicas != args.expected_replicas:
        raise RuntimeError(
            f"replica preflight failed: expected {args.expected_replicas}, got {actual_replicas}"
        )

    logger.info(
        f"cluster preflight passed: cluster={args.clustername}, replicas={actual_replicas}"
    )


class UserVisibleError(RuntimeError):
    """An operator-facing failure: reported without a traceback."""


def connect_to_ch():
    logger.info(
        f"Connecting to ClickHouse, host={args.chhost}, port={args.chport}, username={args.chuser}, password={args.chpass}, s3path={args.s3path}, bucket={args.s3bucket}, s3path={args.s3path}"
    )
    global ch_client, ch_writer

    def new_client():
        return clickhouse_connect.get_client(
            host=args.chhost,
            port=args.chport,
            username=args.chuser,
            password=args.chpass,
            send_receive_timeout=args.chtimeout,
        )

    ch_client = new_client()
    # Deliberately a second connection, not a convenience.
    #
    # clickhouse-connect gives each client its own auto-generated session_id, and
    # ClickHouse allows one query at a time per session. do_use() holds ch_client's
    # session for the whole anti-join while it consumes query_row_block_stream, and
    # insert() issues its own DESCRIBE TABLE before writing. On the shared client
    # that DESCRIBE is a second concurrent query on a held session, which the server
    # rejects with SESSION_IS_LOCKED (373), killing the delete phase after its first
    # successful batch — objects already gone from S3, no tombstone recorded.
    #
    # Keep tombstone writes on this client. Do not "simplify" it away.
    ch_writer = new_client()


def resolve_static_s3_credentials():
    if bool(args.s3accesskey) != bool(args.s3secretkey):
        raise ValueError("s3accesskey and s3secretkey must be specified together")
    if args.s3sessiontoken and not args.s3accesskey:
        raise ValueError("s3sessiontoken requires s3accesskey and s3secretkey")

    if args.s3accesskey:
        return args.s3accesskey, args.s3secretkey, args.s3sessiontoken or None, args.s3region, "static"

    return None, None, None, args.s3region, "anonymous"


def resolve_aws_s3_credentials():
    if args.s3accesskey or args.s3secretkey or args.s3sessiontoken:
        raise ValueError("s3auth=aws cannot be combined with explicit S3 access keys")

    try:
        import boto3
    except ImportError as exc:
        raise UserVisibleError("boto3 is required for s3auth=aws") from exc

    session = boto3.Session(
        profile_name=args.s3profile or None,
        region_name=args.s3region,
    )
    credentials = session.get_credentials()
    if credentials is None:
        profile_hint = f" profile {args.s3profile}" if args.s3profile else ""
        raise UserVisibleError(f"unable to resolve AWS credentials{profile_hint}")

    frozen_credentials = credentials.get_frozen_credentials()
    if not frozen_credentials.access_key or not frozen_credentials.secret_key:
        profile_hint = f" profile {args.s3profile}" if args.s3profile else ""
        raise UserVisibleError(f"resolved AWS credentials{profile_hint} are incomplete")

    return (
        frozen_credentials.access_key,
        frozen_credentials.secret_key,
        frozen_credentials.token,
        args.s3region or session.region_name,
        "aws",
    )


AUTH_MODES = ("static", "aws", "iam")


def resolve_iam_s3_credentials():
    """Workload identity via MinIO's own provider — IRSA, IMDS or ECS task role.

    Kept as a first-class mode rather than folded into `aws`: it needs no boto3,
    and it is what the validated Kubernetes deployments use. `credentials=` is
    returned instead of keys so MinIO can refresh the temporary credentials.
    """
    return None, None, None, args.s3region, "iam"


def resolve_s3_credentials():
    auth_mode = args.s3auth.lower()
    if auth_mode not in AUTH_MODES:
        raise ValueError(f"s3auth must be one of {', '.join(AUTH_MODES)}")

    # Implied modes. Both are conveniences, so a contradiction is an error rather
    # than a silent winner: picking one would send credentials nobody asked for.
    if args.s3profile:
        if auth_mode not in ("static", "aws"):
            raise ValueError(f"s3profile implies s3auth=aws, which conflicts with s3auth={auth_mode}")
        auth_mode = "aws"
    if auth_mode == "aws":
        return resolve_aws_s3_credentials()
    if auth_mode == "iam":
        return resolve_iam_s3_credentials()

    return resolve_static_s3_credentials()


def connect_to_s3():
    if args.s3secure_flag:
        logger.debug(f"using SSL certificate {args.s3sslcertfile}")
        os.environ["SSL_CERT_FILE"] = args.s3sslcertfile

    access_key, secret_key, session_token, s3_region, s3_auth = resolve_s3_credentials()
    logger.info(
        f"Connecting to S3, host:port={args.s3ip}:{args.s3port}, auth={s3_auth}, "
        f"secure={args.s3secure_flag}, region={s3_region}"
    )

    # Google Cloud Storage's S3-compatible API has no batch DeleteObjects, so
    # remove_objects() fails there. Switch to the per-object path automatically
    # rather than letting every delete fail at run time.
    if "storage.googleapis.com" in args.s3ip and args.use_remove_objects:
        logger.warning(
            "GCS endpoint detected: batch remove_objects is not supported there, "
            "falling back to per-object remove_object. This is markedly slower "
            "(one request per object); pass --use-remove-objects false to silence this."
        )
        args.use_remove_objects = False
    global minio_client
    connection_options = {
        "secure": args.s3secure_flag,
        "region": s3_region,
        "http_client": urllib3.PoolManager(
            cert_reqs="CERT_NONE",
            timeout=urllib3.Timeout(
                connect=args.s3_connect_timeout, read=args.s3_read_timeout
            ),
            retries=urllib3.Retry(
                total=args.s3_retries,
                connect=args.s3_retries,
                read=args.s3_retries,
                status=args.s3_retries,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=frozenset({"DELETE", "GET", "HEAD", "POST"}),
            ),
        ),
    }
    if s3_auth == "iam":
        # Hand MinIO the provider, not frozen keys, so it can refresh the
        # temporary credentials for the lifetime of a long collect or delete.
        connection_options["credentials"] = IamAwsProvider()
    else:
        # static, aws and anonymous all arrive here as resolved values;
        # session_token is None unless temporary credentials were supplied.
        connection_options["access_key"] = access_key
        connection_options["secret_key"] = secret_key
        connection_options["session_token"] = session_token
    minio_client = Minio(
        f"{args.s3ip}:{args.s3port}",
        **connection_options,
    )


def remove_objects_reconnecting(batch_rows):
    """Delete one batch, reconnecting once if the S3 transport is stale.

    DeleteObject requests are idempotent: retrying after an interrupted response
    can only leave the object absent, never delete a different object.
    """
    for attempt in range(2):
        try:
            return list(
                minio_client.remove_objects(
                    args.s3bucket, [DeleteObject(row[0]) for row in batch_rows]
                )
            )
        except (S3Error, urllib3.exceptions.HTTPError) as exc:
            if attempt:
                raise
            logger.warning(
                "S3 delete transport failed (%s); reconnecting and retrying once", exc
            )
            connect_to_s3()

    raise AssertionError("unreachable")


def format_s3_list_error(exc):
    code = getattr(exc, "code", "unknown")
    message = getattr(exc, "message", str(exc))
    profile_arg = f" --profile {args.s3profile}" if args.s3profile else ""
    return (
        f"unable to list S3 objects for bucket={args.s3bucket!r}, prefix={args.s3path!r}: "
        f"{code}: {message}. "
        f"s3gc collection requires s3:ListBucket on arn:aws:s3:::{args.s3bucket} "
        f"for this prefix, even with --dry-run. Verify the same credentials with: "
        f"aws sts get-caller-identity{profile_arg}; "
        f"aws s3api list-objects-v2 --bucket {args.s3bucket} --prefix {args.s3path} --max-keys 1{profile_arg}"
    )


def next_s3_object(objects):
    try:
        return next(objects)
    except S3Error as exc:
        raise UserVisibleError(format_s3_list_error(exc)) from exc


def do_collect():
    logger.debug(f"start_after {args.collectafter}")
    objects = minio_client.list_objects(
        args.s3bucket, args.s3path, recursive=True, start_after=args.collectafter
    )

    if args.createdatabase_flag:
        parts = args.collecttableprefix.split(".")
        if dbname:
            logger.info(f"creating database {dbname}")
            ch_client.command(f"CREATE DATABASE IF NOT EXISTS {dbname}")
            logger.debug(f"database created")
        else:
            raise ValueError(
                "database must be a part of collecttableprefix if createdatabase flag is set"
            )

    if args.drop_collecttable_flag:
        logger.info(f"dropping table {tname}")
        ch_client.command(f"DROP TABLE IF EXISTS {tname}")
        logger.debug(f"table dropped")

    logger.info(f"creating table {tname}")
    ch_client.command(
        f"CREATE TABLE IF NOT EXISTS {tname} (objpath String, size Int64, last_modified DateTime, active Bool) ENGINE ReplacingMergeTree ORDER BY objpath PARTITION BY CRC32(objpath) % {args.samples}"
    )
    logger.debug(f"table created")
    go_on = True
    rest_row_nums = args.total  # None if not set
    num_inserted = 0
    total_size = 0
    # Progress is throttled rather than per-batch: at the default
    # collectbatchsize=1024 a ten-million-object bucket would otherwise write
    # ten thousand rows to say the same thing.
    RUN_LOG_EVERY = 100_000
    next_progress = RUN_LOG_EVERY
    while go_on:
        objs = []
        for batch_element in range(0, args.collectbatchsize):
            try:
                obj = next_s3_object(objects)
                delta = datetime.datetime.now(datetime.timezone.utc) - obj.last_modified
                # total_seconds(), not .seconds: the latter is the sub-day
                # remainder (0..86399), so any object older than a day reported
                # at most 23 hours and --age 24 collected nothing at all.
                hours = int(delta.total_seconds() // 3600)
                if hours >= args.age:
                    objs.append([obj.object_name, obj.size, obj.last_modified, True])
                    total_size += obj.size
            except StopIteration:
                go_on = False
        ch_client.insert(tname, objs, column_names=["objpath", "size", "last_modified", "active"])
        logger.debug(f"{len(objs)} rows inserted in {tname}")
        num_inserted += len(objs)
        if num_inserted >= next_progress:
            # The collect phase is otherwise silent at INFO for hours.
            logger.info(f"collect progress: {num_inserted} objects, {total_size} bytes")
            run_log(
                "progress",
                f"{num_inserted} objects listed",
                objects=num_inserted,
                bytes_=total_size,
                phase="collect",
            )
            next_progress += RUN_LOG_EVERY
        if rest_row_nums is not None:
            rest_row_nums -= len(objs)
            if rest_row_nums == 0 or go_on == False:
                go_on = False
                if not args.silent_flag:
                    if len(objs):
                        print(f"s3gc: {objs[-1]}")
                    else:
                        print(f"s3gc: No object")
                break
    logger.info(
        f"information about {num_inserted} objects of total size {total_size} is inserted in {tname}"
    )
    run_log(
        "finish",
        f"collected {num_inserted} objects into {tname}",
        objects=num_inserted,
        bytes_=total_size,
        phase="collect",
    )


def check_samples_match_partitioning():
    """Warn when --samples disagrees with the aux table's PARTITION BY.

    The table is created as PARTITION BY CRC32(objpath) % <samples> at COLLECT
    time. Running the use phase with a different --samples silently loses
    partition pruning: on one production cluster the matching case scanned a
    sample in ~2 min where the mismatching case took ~26 min.
    """
    try:
        rows = ch_client.query(
            "SELECT partition_key FROM system.tables "
            f"WHERE database = currentDatabase() AND name = '{tname.strip('`').split('.')[-1]}'"
        ).result_rows
    except Exception as exc:
        logger.debug(f"could not read partition_key for {tname}: {exc}")
        return
    if not rows or not rows[0][0]:
        return
    partition_key = rows[0][0]
    expected = f"% {args.samples}"
    if "CRC32" in partition_key and expected not in partition_key.replace(" ", " "):
        logger.warning(
            f"--samples {args.samples} does not match the auxiliary table's "
            f"partitioning ({partition_key}). Partition pruning will be lost; "
            "use the same --samples value that the collect phase used."
        )
        run_log(
            "warning",
            f"--samples {args.samples} does not match partitioning {partition_key}",
            phase="use",
        )


def do_use():
    if args.useage < MINIMUM_USEAGE_HOURS:
        if not args.dev_allow_short_useage:
            # Refused for --dry-run too, so the reviewed preview is exactly the
            # set a delete would remove. A dry run that previews a wider set
            # than the delete honours is worse than no preview at all.
            raise UserVisibleError(
                f"--useage {args.useage} is below the {MINIMUM_USEAGE_HOURS} hour minimum. "
                "The age window is the only protection against deleting a part between "
                "its upload to S3 and its registration in system.remote_data_paths; "
                f"use --useage {MINIMUM_USEAGE_HOURS} or greater."
            )
        # Reachable only through the dev-automation phase, which seeds and
        # deletes its own fixtures. Say so loudly and put it in the durable run
        # log, so a run that did this can never be mistaken for a normal one.
        logger.warning(
            f"--useage {args.useage} is below the {MINIMUM_USEAGE_HOURS} hour minimum and is "
            "permitted ONLY because --dev-allow-short-useage is set. This run can "
            "delete a part that is still being written. Never use this against "
            "customer or production data."
        )
        run_log(
            "warning",
            f"useage {args.useage} below the {MINIMUM_USEAGE_HOURS}h minimum, "
            "permitted by --dev-allow-short-useage",
            phase="use",
        )

    if not args.dryrun_flag:
        preflight_cluster()

    srdp = "system.remote_data_paths"
    if args.clustername:
        srdp = f"clusterAllReplicas('{args.clustername}', {srdp})"

    num_rows = 0
    try:
        count_query = f"SELECT COUNT(1) FROM {tname}"
        logger.debug(count_query)
        result = ch_client.command(count_query)
        num_rows = result
    except Exception as exc:
        logger.info(f"exception selecting from {tname}, {exc}")
        pass
    if num_rows == 0:
        # Exiting 0 here reads as success, but with --usecollected an absent or
        # empty auxiliary table means the collect never ran, ran against another
        # host, or was truncated. The table is a NODE-LOCAL ReplacingMergeTree, so
        # a load-balanced ClickHouse Service can collect on one replica and land
        # here on the other. Fail loudly instead of reporting a clean bucket.
        raise RuntimeError(
            f"auxiliary table {tname} does not exist or is empty on {args.chhost}. "
            "Run the collect phase first, and make sure every phase targets the SAME "
            "replica: the table is node-local, so a load-balanced Service will not do."
        )

    check_samples_match_partitioning()

    def make_antijoin(calc_only=False, sample=None):
        after_condition = (
            f"AND s3o.objpath > {quote_sql_string(args.useafter)} " if args.useafter else ""
        )
        age_condition = f"AND s3o.last_modified < now() - interval {args.useage} hour " if args.useage else ""
        limit = f" LIMIT {args.usetotal} " if args.usetotal else ""

        sample_condition = " "
        if not calc_only:
            sample_condition = f"CRC32(s3o.objpath) % {args.samples} = {sample} AND "

        order_by = " ORDER BY s3o.objpath" if args.order_by_objpath else ""
        antijoin = f"""
        SELECT s3o.objpath, s3o.size as size, s3o.last_modified as last_modified FROM {tname} AS s3o LEFT ANTI JOIN {srdp} AS rdp ON
        (rdp.remote_path = s3o.objpath AND rdp.disk_name='{args.s3diskname}')
        WHERE {sample_condition} s3o.active=true {after_condition} {age_condition}
        {order_by} {limit} SETTINGS final = 1"""

        if calc_only:
            countantijoin = f"SELECT COUNT(1), SUM(size) FROM ({antijoin}) q"
            return countantijoin
        else:
            return antijoin

    if (
        args.interactive_flag
        and not args.dryrun_flag
        and os.isatty(sys.stdout.fileno())
        and os.isatty(sys.stdin.fileno())
    ):
        countantijoin = make_antijoin(calc_only=True)
        logger.debug(f"count antijoin {countantijoin}")
        result = ch_client.query(countantijoin)
        logger.debug(result.result_rows)
        num_rows, total_size = result.result_rows[0]
        if num_rows == 0:
            logger.info("Nothing to do")
            run_log("finish", "nothing to do", phase="use")
            graceful_exit()

        while True:
            answer = input(
                f"Proceed with removing {num_rows} objects of total size {total_size}? (Enter y/n) "
            )
            try:
                if not strtobool(answer):
                    graceful_exit()
                break
            except ValueError:
                pass

    num_removed = 0
    total_size = 0
    if not args.dryrun_flag and args.deletebatchsize < 1:
        raise ValueError("--deletebatchsize must be a positive integer")
    for sample in range(0, args.samples):
        # Re-checked per sample, not once per run. The preflight above is a
        # point-in-time check while this loop can run for hours; a replica that
        # drops out mid-run would otherwise take its references with it and make
        # blobs it alone holds look orphaned. ch_client is free here -- the
        # previous sample's stream has closed.
        if not args.dryrun_flag:
            preflight_cluster()

        antijoin = make_antijoin(sample=sample)
        logger.info(f"antijoin {antijoin}")
        run_log(
            "progress",
            f"sample {sample} of {args.samples} started",
            objects=num_removed,
            bytes_=total_size,
            phase="use",
        )

        with ch_client.query_row_block_stream(antijoin) as stream:
            for block in stream:
                selected_rows = []
                for row in block:
                    logger.debug(
                        f"{'removing' if not args.dryrun_flag else 'would remove if no dryrun flag'}  {row[0]} of size {row[1]}"
                    )
                    selected_rows.append(row)

                if args.dryrun_flag:
                    num_removed += len(selected_rows)
                    total_size += sum(row[1] for row in selected_rows)
                    continue

                for offset in range(0, len(selected_rows), args.deletebatchsize):
                    batch_rows = selected_rows[offset : offset + args.deletebatchsize]
                    errors = []
                    if args.use_remove_objects:
                        errors = remove_objects_reconnecting(batch_rows)
                        for error in errors:
                            logger.info(f"error occurred when deleting object via remove_objects {error}")

                        failed_names = {
                            getattr(error, "object_name", None) or getattr(error, "name", None)
                            for error in errors
                        }
                        if None in failed_names:
                            # Do not tombstone any object for an uncorrelatable batch error.
                            successful_rows = []
                        else:
                            successful_rows = [
                                row for row in batch_rows if row[0] not in failed_names
                            ]
                    else:
                        successful_rows = []
                        for row in batch_rows:
                            try:
                                minio_client.remove_object(args.s3bucket, row[0])
                                successful_rows.append(row)
                            except Exception as error:
                                logger.info(f"error occurred when deleting object {row[0]} via remove_object {error}")
                                errors.append(error)

                    if successful_rows:
                        tombstones = [
                            [row[0], row[1], row[2], False] for row in successful_rows
                        ]
                        # ch_writer, not ch_client: the anti-join stream above still
                        # holds ch_client's session. See connect_to_ch().
                        ch_writer.insert(
                            tname,
                            tombstones,
                            column_names=["objpath", "size", "last_modified", "active"],
                        )
                        num_removed += len(successful_rows)
                        total_size += sum(row[1] for row in successful_rows)
                        logger.info(
                            f"delete checkpoint: {num_removed} objects / {total_size} bytes removed so far"
                        )
                        # The durable twin of the checkpoint above: if the pod is
                        # gone, this row is what says how far the delete got.
                        run_log(
                            "checkpoint",
                            f"sample {sample}: {len(successful_rows)} objects deleted in this batch",
                            objects=num_removed,
                            bytes_=total_size,
                            phase="use",
                        )

                    if errors:
                        run_log(
                            "error",
                            f"{len(errors)} S3 deletion error(s) in sample {sample}",
                            objects=num_removed,
                            bytes_=total_size,
                            phase="use",
                        )
                        raise S3DeletionError(
                            f"{len(errors)} S3 deletion error(s); successful deletes were checkpointed"
                        )

    # "this attempt", not "this run": a resumed run leaves earlier attempts'
    # deletions out of these counters, so the line understated one aps1 run by
    # 16.61 TiB. The cumulative truth is the tombstone count in the aux table.
    logger.info(
        f"{num_removed} objects of total size {total_size} "
        f"{'are removed' if not args.dryrun_flag else 'would be removed but for dryrun flag'} "
        "in this attempt"
    )
    run_log(
        "finish",
        f"{num_removed} objects "
        f"{'removed' if not args.dryrun_flag else 'would be removed (dry run)'} in this attempt",
        objects=num_removed,
        bytes_=total_size,
        phase="use",
    )
    if not args.dryrun_flag:
        try:
            cumulative = ch_client.query(
                f"SELECT count(), sum(size) FROM {tname} FINAL WHERE active = false"
            ).result_rows[0]
            logger.info(
                f"cumulative for this auxiliary table: {cumulative[0]} objects / "
                f"{cumulative[1]} bytes tombstoned"
            )
            run_log(
                "progress",
                "cumulative tombstones for this auxiliary table",
                objects=cumulative[0] or 0,
                bytes_=cumulative[1] or 0,
                phase="use",
            )
        except Exception as exc:  # never fail a completed run over a status query
            logger.info(f"could not read cumulative tombstone count: {exc}")

    if not args.keepdata_flag and not args.dryrun_flag:
        logger.info(f"truncating {tname}")
        ch_client.command(f"TRUNCATE TABLE {tname}")


def main():
    try:
        connect_to_ch()
        init_run_log()
        run_log("start", f"{current_phase()} phase started")
        if not (args.usecollected_flag and args.dryrun_flag):
            connect_to_s3()
        if not args.usecollected_flag:
            do_collect()
        if not args.collectonly_flag:
            do_use()

        run_log("finish", f"{current_phase()} phase completed")
        graceful_exit()
    except UserVisibleError as exc:
        run_log("error", str(exc))
        if args.debug_flag:
            logger.exception(str(exc))
        else:
            logger.error(str(exc))
        sys.exit(1)
    except Exception as exc:
        # A crash is exactly the case where the pod log is least likely to
        # survive, so record it and then let it propagate unchanged. SystemExit
        # is not an Exception, so graceful_exit() does not land here.
        run_log("error", f"{type(exc).__name__}: {exc}")
        raise


if __name__ == "__main__":
    main()
