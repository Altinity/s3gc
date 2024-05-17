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


## script invocation
### help
```
python3 s3gc.py --help
```
### typical usage
#### all together
for https://altinity-clickhouse-data-demo20565656565620663600000001.s3.amazonaws.com/github
```
S3GC_S3ACCESSKEY=sdfasfaerasasf  S3GC_S3SECRETKEY=werqwsdfqwersdfasf  S3GC_S3IP=s3.amazonaws.com S3GC_S3PORT=443 S3GC_S3REGION=us-east-1 S3GC_S3BUCKET=altinity-clickhouse-data-demo20565656565620663600000001 S3GC_S3PATH=github/ S3GC_S3SECURE_FLAG=true python3 ./s3gc.py --verbose
```
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
sudo docker build -t ilejn/s3gc .
```

### usage
```
sudo docker run ilejn/s3gc --help
sudo docker run --network="host" -e S3GC_S3PORT=19000 -e S3GC_S3ACCESSKEY=minio99 -e S3GC_S3SECRETKEY=minio123 ilejn/s3gc
```
