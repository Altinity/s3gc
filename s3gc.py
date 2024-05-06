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

usage = """
    s3 garbage collector for ClickHouse
    example: $ ./s3gc.py
"""

parser = ArgumentParser(usage=usage, env_prefix="S3GC", default_env=True, exit_on_error=False)

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
    help="S3 ip address"
)
parser.add_argument(
    "--s3port",
    "--s3-port",
    dest="s3port",
    default=9001,
    help="S3 API port"
)
parser.add_argument(
    "--s3bucket",
    "--s3-bucket",
    dest="s3bucket",
    default="root",
    help="S3 bucker name"
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
    help="S3 secure mode"
)
parser.add_argument(
    "--s3secureflag",
    "--s3-secure-flag",
    type=bool,
    dest="s3secure_flag",
    default=False,
    help="S3 secure mode"
)
parser.add_argument(
    "--s3sslcertfile",
    "--s3-ssl-cert-file",
    dest="s3sslcertfile",
    default="",
    help="SSL certificate for S3",
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
    help="prefix for table name to keep data about objects (tablespace is allowed)",
)
parser.add_argument(
    "--collectbatchsize",
    "--collect-batch-size",
    dest="collectbatchsize",
    type = int,
    default=1024,
    help="number of rows to insert to ClickHouse at once",
)
parser.add_argument(
    "--total",
    "--total-num",
    dest="total",
    type = Optional[int],
    help="Number of objects to process. Can be used in conjunction with start-after",
)
parser.add_argument(
    "--collectafter",
    "--collect-after",
    dest="collectafter",
    help="Object name to start after. If not specified, traversing objects from the beginning",
)
parser.add_argument(
    "--useafter",
    "--use-after",
    dest="useafter",
    help="Object name to start processing already collected objects after. If not specified, traversing objects from the beginning",
)
parser.add_argument(
    "--usetotal",
    "--use-total",
    dest="usetotal",
    help="Number of already collected objects to process. Can be used in conjunction with use-after",
)
parser.add_argument(
    "--cluster",
    "--cluster-name",
    "--clustername",
    dest="clustername",
    default="",
    help="consider an objects unused if there is no host in the cluster refers the object",
)
parser.add_argument(
    "--age",
    "--hours",
    "--age-hours",
    dest="age",
    type=int,
    default=0,
    help="process only objects older than specified, it is assumed that timezone is UTC",
)
parser.add_argument(
    "--samples",
    dest="samples",
    type=int,
    default=4,
    help="process only objects older than specified, it is assumed that timezone is UTC",
)
parser.add_argument(
    "--verbose",
    action="store_true",
    dest="verbose_flag",
    default=False,
    help="debug output"
)
parser.add_argument(
    "--verboseflag",
    "--verbose-flag",
    type=bool,
    dest="verbose_flag",
    default=False,
    help="debug output"
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
    "--silent",
    action="store_true",
    dest="silent_flag",
    default=False,
    help="no log"
)
parser.add_argument(
    "--silentflag",
    "--silent-flag",
    dest="silent_flag",
    type=bool,
    default=False,
    help="no log"
)
parser.add_argument(
    "--listoptions",
    "--list-options",
    action="store_true",
    dest="listoptions",
    default=False,
    help="list all command line options for internal purposes"
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

    print("ENV ", end='')
    backslash = False
    for ln in f.getvalue().splitlines():

        (key, value) = ln.split(': ')
        # if (key in ['listoptions', 'verbose', 'debug', 'silent', 'keepdata', 'collectonly', 'usecollected']):
        if (key in ['listoptions']):
            continue
        if backslash:
            print(" \\ ")
        print(f" S3GC_{key.upper()}={value}", end='')

        backslash = True

    print()
    exit()




logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) # set logger level
# logger.setLevel(logging.WARNING)
if args.verbose_flag:
    print("verbose")
    logger.setLevel(logging.INFO)
if args.debug_flag:
    print("debug")
    logger.setLevel(logging.DEBUG)
if args.silent_flag:
    print("silent")
    logger.setLevel(logging.CRITICAL)


logFormatter = logging.Formatter\
("%(asctime)s %(levelname)s %(message)s")
consoleHandler = logging.StreamHandler(sys.stdout) #set streamhandler to stdout
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)




logger.info(
    f"Connecting to ClickHouse, host={args.chhost}, port={args.chport}, username={args.chuser}, password={args.chpass}"
)
ch_client = clickhouse_connect.get_client(
    host=args.chhost,
    port=args.chport,
    username=args.chuser,
    password=args.chpass,
)

if args.s3secure_flag:
    logger.debug(f"using SSL certificate {args.s3sslcertfile}")
    os.environ["SSL_CERT_FILE"] = args.s3sslcertfile

tname = f"{args.collecttableprefix}{args.s3diskname}"

if not args.usecollected_flag:
    logger.info(
        f"Connecting to S3, host:port={args.s3ip}:{args.s3port}, access_key={args.s3accesskey}, secret_key={args.s3secretkey}, secure={args.s3secure_flag}"
    )
    minio_client = Minio(
        f"{args.s3ip}:{args.s3port}",
        access_key=args.s3accesskey,
        secret_key=args.s3secretkey,
        secure=args.s3secure_flag,
        http_client=urllib3.PoolManager(cert_reqs="CERT_NONE"),
    )

    objects = minio_client.list_objects(args.s3bucket, "data/", recursive=True, start_after=args.collectafter)

    logger.info(f"creating {tname}")
    ch_client.command(
        f"CREATE TABLE IF NOT EXISTS {tname} (objpath String, active Bool) ENGINE ReplacingMergeTree ORDER BY objpath PARTITION BY CRC32(objpath) % {args.samples}"
    )
    go_on = True
    rest_row_nums = args.total # None if not set
    while go_on:
        objs = []
        for batch_element in range(0, args.collectbatchsize):
            try:
                obj = next(objects)
                delta = datetime.datetime.now(datetime.timezone.utc) - obj.last_modified
                hours = (int(delta.seconds / 3600))
                if hours >= args.age:
                    objs.append([obj.object_name, True])
            except StopIteration:
                go_on = False
        ch_client.insert(tname, objs, column_names=["objpath", "active"])
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

if not args.collectonly_flag:
    srdp = "system.remote_data_paths"
    if args.clustername:
        srdp = f"clusterAllReplicas({args.clustername}, {srdp})"

    num_rows=0
    try:
        result = client.query(f"SELECT COUNT(1) FROM {tname}")
    except Exception:
        pass
    if num_rows==0:
        logger.info(
            f"auxiliary table {tname} does not exist or empty, nothing to do"
        )
        if not args.silent_flag:
            print("s3gc: OK")

        exit()


    num_removed = 0
    objs = []
    for sample in range(0, args.samples):

        antijoin = f"""
        SELECT s3o.objpath FROM {tname} AS s3o LEFT ANTI JOIN {srdp} AS rdp ON
        (rdp.remote_path = s3o.objpath AND rdp.disk_name='{args.s3diskname}')
        WHERE CRC32(s3o.objpath) % {args.samples} = {sample} AND s3o.active=true
        ORDER BY s3o.objpath  SETTINGS final = 1"""
        logger.info(antijoin)

        with ch_client.query_row_block_stream(antijoin) as stream:
            for block in stream:
                objects_to_remove=[]
                for row in block:
                    logger.debug(f"removing {row[0]}")
                    objects_to_remove.append(DeleteObject(row[0]))
                    objects_to_remove.append(DeleteObject(row[0] + 'ss'))
                    errors = minio_client.remove_objects(args.s3bucket, objects_to_remove)
                    for error in errors:
                        logger.info(f"error occurred when deleting object {error}")

                    num_removed += len(objects_to_remove)
                    objs.append([row[0], False])

        ch_client.insert(tname, objs, column_names=["objpath", "active"])


    logger.info(f"{num_removed} objects are removed")

    if not args.keepdata_flag:
        logger.info(f"truncating {tname}")
        ch_client.command(f"TRUNCATE TABLE {tname}")

if not args.silent_flag:
    print("s3gc: OK")
