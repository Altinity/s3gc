FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir --disable-pip-version-check -r requirements.txt

COPY s3gc.py ./
COPY docker/kubernetes-entrypoint.sh ./kubernetes-entrypoint.sh
RUN chmod 0555 /app/kubernetes-entrypoint.sh

USER 65532:65532

ENTRYPOINT ["python", "/app/s3gc.py"]
