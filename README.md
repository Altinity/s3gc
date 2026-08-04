# s3gc
Garbage collector for ClickHouse S3 disks

## Repository layout

- `s3gc.py` — collector and deletion logic.
- `docker/` — reproducible Python 3.11 container packaging.
- `deploy/kubernetes/` — generic one-shot Kubernetes Job renderer and operator runbook.
- `tests/` — safety and renderer unit tests.

The repository is intentionally public and contains no target-cluster details,
credentials, rendered manifests, or environment configuration. Operators keep
those values outside Git and deploy an image pinned by digest.

## Testing

Install test-only dependencies with
`uv pip install --python .venv/bin/python -r requirements-dev.txt`, then run
the isolated unit suite with:

```
.venv/bin/python -m pytest -v
```

Tests marked `dev_cluster` require the dedicated development Kubernetes cluster
and are never run by CI. They must be selected explicitly with
`pytest -m dev_cluster` after reviewing their fixture scope.

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

Development, CI, and the image use Python 3.11. With `uv` installed, create the
local environment with `uv venv --python 3.11 .venv`, then install the pinned
requirements with `uv pip install --python .venv/bin/python -r requirements.txt`.

### rebuild
```
docker buildx build --platform linux/amd64,linux/arm64 -f docker/Dockerfile -t altinity/s3gc .
```

### usage
```
docker run altinity/s3gc --help
docker run --network="host" -e S3GC_S3PORT=19000 -e S3GC_S3ACCESSKEY=minio99 -e S3GC_S3SECRETKEY=minio123 altinity/s3gc
```

## Kubernetes

`deploy/kubernetes/` contains a plain-template one-shot Job runner for running
the collector inside the ClickHouse namespace. It has separate `collect`,
`dry-run`, and guarded `delete` phases and does not create or contain secrets.
See [deploy/kubernetes/README.md](deploy/kubernetes/README.md) for the render
contract and safety requirements.

Images are published only by the GitHub Actions workflow after a trusted push to
protected `master`. Pull requests run tests without registry credentials and do
not publish an image.

## changelog

### v_0.1 Wed Jun 12 2024

- object last modified in auxiliary table
- useage command line parameter
  
### v_0.2 Fri Jan 31 2025
- added option to avoid batch deletion for services like GCS

## to do list
~~1. option to avoid `remove_objects` which is reportedly not supported by GCE~~

- concurrency / async
