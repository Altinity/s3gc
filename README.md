# s3gc
Garbage collector for ClickHouse S3 disks

## description
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

Besides this, it is possible to calculate objects to remove without actual removing AKA dry run.
If dryrun is set together with usecollected, it uses collected data.
If dryrun is set together with collectonly, error is raised.

It is important to use `--s3diskname` if your disk name is not `s3` which is by default.

WARNING!: Please use `--dry-run` to check and compare results of what is going to be deleted, just to be on the safe side. 

## script invocation
### install
Install the Python dependencies, ideally in a virtualenv:
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
On Debian/Ubuntu/WSL you may first need `sudo apt install -y python3-pip python3-venv`.
Without a virtualenv, use `pip install --user -r requirements.txt` (add
`--break-system-packages` if pip refuses on an externally-managed Python).

### help
```
python3 s3gc.py --help
```

### typical usage
#### all together with dry-run
for https://altinity-clickhouse-data-demo20565656565620663600000001.s3.amazonaws.com/github
```
S3GC_S3ACCESSKEY=sdfasfaerasasf \
S3GC_S3SECRETKEY=werqwsdfqwersdfasf \
S3GC_S3IP=s3.amazonaws.com \
S3GC_S3PORT=443 \
S3GC_S3REGION=us-east-1 \
S3GC_S3BUCKET=altinity-clickhouse-data-demo20565656565620663600000001 \
S3GC_S3PATH=github/ \
S3GC_S3SECURE_FLAG=true \
python3 ./s3gc.py --verbose --dry-run
```
#### AWS SSO or AWS profile credentials
Authenticate with AWS CLI first, then let `s3gc` resolve temporary credentials through the boto3 credential chain.
```
aws sso login --profile my-sso-profile

S3GC_S3AUTH=aws \
S3GC_S3PROFILE=my-sso-profile \
S3GC_S3IP=s3.amazonaws.com \
S3GC_S3PORT=443 \
S3GC_S3REGION=us-east-1 \
S3GC_S3BUCKET=altinity-clickhouse-data-demo20565656565620663600000001 \
S3GC_S3PATH=github/ \
S3GC_S3SECURE_FLAG=true \
python3 ./s3gc.py --verbose --dry-run
```

`S3GC_S3ACCESSKEY` and `S3GC_S3SECRETKEY` are not used with `S3GC_S3AUTH=aws`.
The selected credentials must allow `s3:ListBucket` on the bucket for
`S3GC_S3PATH`, even for `--dry-run`. Verify the same profile with:
```
aws sts get-caller-identity --profile my-sso-profile
aws s3api list-objects-v2 --bucket altinity-clickhouse-data-demo20565656565620663600000001 --prefix github/ --max-keys 1 --profile my-sso-profile
```

#### GCS and object storage that do not support batch delete operations
```
S3GC_S3ACCESSKEY=GOOG1xxxxxxxxx \
S3GC_S3SECRETKEY=xxxxxxxxxxx \
S3GC_S3IP=storage.googleapis.com \
S3GC_S3PORT=443 \
S3GC_S3BUCKET=clickhouse-altinity-main-disk \
S3GC_S3PATH=chi-main-main-0-0/ \
S3GC_S3SECURE_FLAG=true \
S3GC_S3DISKNAME=gcs \
python3 ./s3gc.py --verbose --use-remove-objects=false
```

GCS_HMAC_KEY = S3GC_S3ACCESSKEY
GCS_HMAC_SECRET = S3GC_S3SECRETKEY


#### collect only
```
S3GC_S3PORT=19000  S3GC_S3ACCESSKEY=minio99  S3GC_S3SECRETKEY=minio123  python3 ./s3gc.py --verbose --collectonly
```
#### use collected
```
S3GC_S3PORT=19000  S3GC_S3ACCESSKEY=minio99  S3GC_S3SECRETKEY=minio123 S3GC_USECOLLECTED=true  python3 ./s3gc.py --debug
```

## docker
There is a docker image for the script.

### rebuild
```
make
sudo docker buildx build --platform linux/arm/v7,linux/arm64/v8,linux/amd64 -t ilejn/s3gc .
```

### usage
```
sudo docker run ilejn/s3gc --help
sudo docker run --network="host" -e S3GC_S3PORT=19000 -e S3GC_S3ACCESSKEY=minio99 -e S3GC_S3SECRETKEY=minio123 ilejn/s3gc
```

## changelog

### v_0.1 Wed Jun 12 2024

- object last modified in auxiliary table
- useage command line parameter
  
### v_0.2 Fri Jan 31 2025
- added option to avoid batch deletion for services like GCS

### v_0.3 Mon Jun 15 2026
- added s3 profile option

## to do list
~~1. option to avoid `remove_objects` which is reportedly not supported by GCE~~

- concurrency / async
