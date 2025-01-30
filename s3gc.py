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
from distutils.util import strtobool

usage = """
    s3 garbage collector for ClickHouse
    example: $ ./s3gc.py
"""

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
    type=bool,
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
    type=bool,
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
    type=bool,
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
    type=bool,
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
    type=bool,
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
    default=0,
    help="Process only already collected objects older than specified number of hours",
)
parser.add_argument(
    "--samples",
    dest="samples",
    type=int,
    default=4,
    help="Number of partitions in auxiliary table",
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
    type=bool,
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
    type=bool,
    default=False,
    help="drop collecttable and recreate; beware of ClickHouse DROP TABLE constraints",
)
parser.add_argument(
    "--useremoveobjects",
    "--use-remove-objects",
    dest="use_remove_objects",
    type=bool,
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
    type=bool,
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
    type=bool,
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
    type=bool,
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
    type=bool,
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

parser.add_argument("--cfg", action=ActionConfigFile)


# out = get_parse_args_stdout(parser, ["--print_config"])
# print(out)

args = parser.parse_args()

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
            print(" \\ ")
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
        if len(args.chpass) > 3:
            filter_strings.append(args.chpass)
        if len(args.s3secretkey) > 3:
            filter_strings.append(args.s3secretkey)
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

minio_client = None
ch_client = None


def connect_to_ch():
    logger.info(
        f"Connecting to ClickHouse, host={args.chhost}, port={args.chport}, username={args.chuser}, password={args.chpass}, s3path={args.s3path}, bucket={args.s3bucket}, s3path={args.s3path}"
    )
    global ch_client
    ch_client = clickhouse_connect.get_client(
        host=args.chhost,
        port=args.chport,
        username=args.chuser,
        password=args.chpass,
        send_receive_timeout=args.chtimeout,
    )


def connect_to_s3():
    if args.s3secure_flag:
        logger.debug(f"using SSL certificate {args.s3sslcertfile}")
        os.environ["SSL_CERT_FILE"] = args.s3sslcertfile

    logger.info(
        f"Connecting to S3, host:port={args.s3ip}:{args.s3port}, access_key={args.s3accesskey}, secret_key={args.s3secretkey}, secure={args.s3secure_flag}, region={args.s3region}"
    )
    global minio_client
    minio_client = Minio(
        f"{args.s3ip}:{args.s3port}",
        access_key=args.s3accesskey,
        secret_key=args.s3secretkey,
        secure=args.s3secure_flag,
        region=args.s3region,
        http_client=urllib3.PoolManager(cert_reqs="CERT_NONE"),
    )


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
    while go_on:
        objs = []
        for batch_element in range(0, args.collectbatchsize):
            try:
                obj = next(objects)
                delta = datetime.datetime.now(datetime.timezone.utc) - obj.last_modified
                hours = int(delta.seconds / 3600)
                if hours >= args.age:
                    objs.append([obj.object_name, obj.size, obj.last_modified, True])
                    total_size += obj.size
            except StopIteration:
                go_on = False
        ch_client.insert(tname, objs, column_names=["objpath", "size", "last_modified", "active"])
        logger.debug(f"{len(objs)} rows inserted in {tname}")
        num_inserted += len(objs)
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


def do_use():
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
        logger.info(f"auxiliary table {tname} does not exist or empty, nothing to do")

        graceful_exit()

    def make_antijoin(calc_only=False, sample=None):
        after_condition = f"AND s3o.objpath > {args.useafter} " if args.useafter else ""
        age_condition = f"AND s3o.last_modified < now() - interval {args.useage} hour " if args.useage else ""
        limit = f" LIMIT {args.usetotal} " if args.usetotal else ""

        sample_condition = " "
        if not calc_only:
            sample_condition = f"CRC32(s3o.objpath) % {args.samples} = {sample} AND "

        antijoin = f"""
        SELECT s3o.objpath, s3o.size as size, s3o.last_modified as last_modified FROM {tname} AS s3o LEFT ANTI JOIN {srdp} AS rdp ON
        (rdp.remote_path = s3o.objpath AND rdp.disk_name='{args.s3diskname}')
        WHERE {sample_condition} s3o.active=true {after_condition} {age_condition}
        ORDER BY s3o.objpath {limit} SETTINGS final = 1"""

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
    objs = []

    for sample in range(0, args.samples):
        antijoin = make_antijoin(sample=sample)
        logger.info(f"antijoin {antijoin}")

        with ch_client.query_row_block_stream(antijoin) as stream:
            for block in stream:
                objects_to_remove = []
                object_to_remove = []
                for row in block:
                    logger.debug(
                        f"{'removing' if not args.dryrun_flag else 'would remove if no dryrun flag'}  {row[0]} of size {row[1]}"
                    )
                    if args.use_remove_objects:
                        objects_to_remove.append(DeleteObject(row[0]))
                    else:
                        object_to_remove.append(row[0])
                    objs.append([row[0], row[1], row[2], False])
                    total_size += row[1]
                if not args.dryrun_flag:
                    if args.use_remove_objects:
                        errors = minio_client.remove_objects(
                            args.s3bucket, objects_to_remove
                        )
                        for error in errors:
                            logger.info(f"error occurred when deleting object via remove_objects {error}")
                    else:
                        try:
                            for object_path in object_to_remove:
                                minio_client.remove_object(
                                    args.s3bucket, object_path
                                )
                        except Exception as error:
                            logger.info(f"error occurred when deleting object {object_path} via remove_object {error}")

                num_removed += len(objects_to_remove)

        if not args.dryrun_flag:
            ch_client.insert(tname, objs, column_names=["objpath", "size", "last_modified", "active"])

    logger.info(
        f"{num_removed} objects of total size {total_size} {'are removed' if not args.dryrun_flag else 'would be removed but for dryrun flag'}"
    )

    if not args.keepdata_flag and not args.dryrun_flag:
        logger.info(f"truncating {tname}")
        ch_client.command(f"TRUNCATE TABLE {tname}")


def main():
    connect_to_ch()
    if not (args.usecollected_flag and args.dryrun_flag):
        connect_to_s3()
    if not args.usecollected_flag:
        do_collect()
    if not args.collectonly_flag:
        do_use()

    graceful_exit()


if __name__ == "__main__":
    main()
