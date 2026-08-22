#!/usr/bin/env sh
set -eu

phase="${S3GC_PHASE:-dry-run}"

case "${phase}" in
  collect)
    set -- --collectonly --keepdata
    if [ "${S3GC_FRESH_RUN:-false}" = "true" ]; then
      set -- "$@" --drop-collecttable
    fi
    ;;
  dry-run)
    set -- --usecollected --dry-run
    ;;
  delete)
    if [ "${S3GC_DELETE_CONFIRMATION:-}" != "DELETE_ORPHANS" ]; then
      echo "Refusing delete: set S3GC_DELETE_CONFIRMATION=DELETE_ORPHANS" >&2
      exit 64
    fi
    if [ -z "${S3GC_CLUSTERNAME:-}" ] || [ -z "${S3GC_EXPECTED_REPLICAS:-}" ]; then
      echo "Refusing delete: S3GC_CLUSTERNAME and S3GC_EXPECTED_REPLICAS are required" >&2
      exit 64
    fi
    set -- --usecollected --keepdata --non-interactive
    ;;
  dev-automation)
    if [ "${S3GC_DELETE_CONFIRMATION:-}" != "DELETE_ORPHANS" ]; then
      echo "Refusing dev automation: set S3GC_DELETE_CONFIRMATION=DELETE_ORPHANS" >&2
      exit 64
    fi
    if [ -z "${S3GC_CLUSTERNAME:-}" ] || [ -z "${S3GC_EXPECTED_REPLICAS:-}" ]; then
      echo "Refusing dev automation: S3GC_CLUSTERNAME and S3GC_EXPECTED_REPLICAS are required" >&2
      exit 64
    fi

    # A fresh collection avoids mixing prior runs and their tombstones into an
    # automated development run. `set -e` stops subsequent stages on error.
    # Development fixtures are seeded and deleted within minutes, so this phase
    # -- and ONLY this phase -- may run below the 24 hour age minimum that
    # protects a part between its upload to S3 and its registration in
    # system.remote_data_paths. The prod phases above never pass this flag.
    echo "s3gc dev automation: collect"
    python /app/s3gc.py --collectonly --keepdata --drop-collecttable
    echo "s3gc dev automation: dry-run"
    python /app/s3gc.py --usecollected --dry-run --dev-allow-short-useage
    echo "s3gc dev automation: delete"
    exec python /app/s3gc.py --usecollected --keepdata --non-interactive --dev-allow-short-useage
    ;;
  *)
    echo "Invalid S3GC_PHASE=${phase}; use collect, dry-run, delete, or dev-automation" >&2
    exit 64
    ;;
esac

exec python /app/s3gc.py "$@"
