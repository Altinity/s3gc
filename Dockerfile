FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV  CHHOST=localhost \ 
 CHPORT=8123 \ 
 CHUSER=default \ 
 CHPASS= \ 
 S3IP=127.0.0.1 \ 
 S3PORT=9001 \ 
 S3BUCKET=root \ 
 S3ACCESSKEY=127.0.0.1 \ 
 S3SECRETKEY=127.0.0.1 \ 
 S3SECURE=False \ 
 S3SSLCERTFILE= \ 
 S3DISKNAME=s3 \ 
 KEEPDATA=False \ 
 COLLECTONLY=False \ 
 USECOLLECTED=False \ 
 COLLECTTABLEPREFIX=s3objects_for_ \ 
 BATCHSIZE=1024 \ 
 TOTAL=None \ 
 AFTER=None \ 
 CLUSTERNAME= \ 
 AGE=0 \ 
 VERBOSE=False \ 
 DEBUG=False \ 
 SILENT=False

ENTRYPOINT ["python", "./s3gc.py"]
CMD ["--help" ]
