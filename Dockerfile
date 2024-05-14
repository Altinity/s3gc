FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV  S3GC_CHHOST=localhost \ 
 S3GC_CHPORT=8123 \ 
 S3GC_CHUSER=default \ 
 S3GC_CHPASS='' \ 
 S3GC_S3IP=127.0.0.1 \ 
 S3GC_S3PORT=9001 \ 
 S3GC_S3BUCKET=root \ 
 S3GC_S3PATH=data/ \ 
 S3GC_S3ACCESSKEY='' \ 
 S3GC_S3SECRETKEY='' \ 
 S3GC_S3SECURE_FLAG=false \ 
 S3GC_S3SSLCERTFILE='' \ 
 S3GC_S3REGION=null \ 
 S3GC_S3DISKNAME=s3 \ 
 S3GC_KEEPDATA_FLAG=false \ 
 S3GC_COLLECTONLY_FLAG=false \ 
 S3GC_USECOLLECTED_FLAG=false \ 
 S3GC_COLLECTTABLEPREFIX=s3objects_for_ \ 
 S3GC_COLLECTBATCHSIZE=1024 \ 
 S3GC_TOTAL=null \ 
 S3GC_COLLECTAFTER=null \ 
 S3GC_USEAFTER=null \ 
 S3GC_USETOTAL=null \ 
 S3GC_DRYRUN=null \ 
 S3GC_CLUSTERNAME='' \ 
 S3GC_AGE=0 \ 
 S3GC_SAMPLES=4 \ 
 S3GC_VERBOSE_FLAG=false \ 
 S3GC_DEBUG_FLAG=false \ 
 S3GC_SILENT_FLAG=false


ENTRYPOINT ["python", "./s3gc.py"]
# CMD ["--help" ]
