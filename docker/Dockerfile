FROM python:3.11-slim

WORKDIR /app

# stdout is a pipe under Kubernetes, so print() is block-buffered and Python's
# default SIGTERM handling exits without flushing. A Job killed at
# activeDeadlineSeconds therefore lost its buffered tail, including the closing
# "s3gc: OK", and the shell echoes in the dev-automation phase interleaved
# wrongly against it.
ENV PYTHONUNBUFFERED=1

COPY requirements.txt ./
RUN pip install --no-cache-dir --disable-pip-version-check -r requirements.txt

COPY s3gc.py ./
COPY docker/kubernetes-entrypoint.sh ./kubernetes-entrypoint.sh
RUN chmod 0555 /app/kubernetes-entrypoint.sh

USER 65532:65532

ENTRYPOINT ["python", "/app/s3gc.py"]
