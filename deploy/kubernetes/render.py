#!/usr/bin/env python3
"""Render the plain s3gc Job template from a non-secret KEY=VALUE file."""

import re
import sys
from pathlib import Path
from string import Template


ROOT = Path(__file__).parent
TEMPLATE = ROOT / "job.yaml.tmpl"
REQUIRED = {
    "ACTIVE_DEADLINE_SECONDS",
    "CHHOST",
    "CHPORT",
    "CLUSTERNAME",
    "COLLECTTABLEPREFIX",
    "CREDENTIALS_SECRET",
    "DELETE_BATCH_SIZE",
    "EXPECTED_REPLICAS",
    "IMAGE",
    "IMAGE_PULL_SECRET",
    "JOB_NAME",
    "MEMORY_LIMIT",
    "MEMORY_REQUEST",
    "NAMESPACE",
    "ORDER_BY_OBJPATH",
    "PHASE",
    "S3BUCKET",
    "S3DISKNAME",
    "S3IP",
    "S3PATH",
    "S3PORT",
    "S3REGION",
    "S3SECURE_FLAG",
    "S3USEIAM",
    "SAMPLES",
    "SERVICE_ACCOUNT",
    "TTL_SECONDS_AFTER_FINISHED",
    "USEAGE_HOURS",
    "VERBOSE",
}
DELETE_CONFIRMATION = "DELETE_ORPHANS"
JOB_NAME_RE = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")


def read_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line_number, line in enumerate(path.read_text().splitlines(), start=1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError(f"{path}:{line_number}: expected KEY=VALUE")
        key, value = line.split("=", 1)
        if not key.isidentifier():
            raise ValueError(f"{path}:{line_number}: invalid key {key!r}")
        values[key] = value
    return values


def validate(values: dict[str, str]) -> None:
    missing = sorted(REQUIRED - values.keys())
    if missing:
        raise ValueError("missing required values: " + ", ".join(missing))
    if values["PHASE"] not in {"collect", "dry-run", "delete", "dev-automation"}:
        raise ValueError("PHASE must be collect, dry-run, delete, or dev-automation")
    if values["PHASE"] in {"delete", "dev-automation"} and values.get("DELETE_CONFIRMATION") != DELETE_CONFIRMATION:
        raise ValueError(
            f"{values['PHASE']} requires DELETE_CONFIRMATION={DELETE_CONFIRMATION}"
        )
    if not JOB_NAME_RE.fullmatch(values["JOB_NAME"]) or len(values["JOB_NAME"]) > 63:
        raise ValueError("JOB_NAME must be a DNS label of at most 63 characters")
    if "@sha256:" not in values["IMAGE"]:
        raise ValueError("IMAGE must be pinned by digest (for example, altinity/s3gc@sha256:...)")
    for key, value in values.items():
        if any(character in value for character in ('"', "\\n", "\\r")):
            raise ValueError(f"{key} may not contain quotes or newlines")
    for numeric_key in ("DELETE_BATCH_SIZE", "EXPECTED_REPLICAS", "SAMPLES", "ACTIVE_DEADLINE_SECONDS", "TTL_SECONDS_AFTER_FINISHED"):
        if not values[numeric_key].isdigit() or int(values[numeric_key]) < 1:
            raise ValueError(f"{numeric_key} must be a positive integer")
    if not values["USEAGE_HOURS"].isdigit() or int(values["USEAGE_HOURS"]) < 0:
        raise ValueError("USEAGE_HOURS must be a non-negative integer")
    if values["S3USEIAM"] not in {"true", "false"}:
        raise ValueError("S3USEIAM must be true or false")
    if values["VERBOSE"] not in {"true", "false"}:
        raise ValueError("VERBOSE must be true or false")
    if values["ORDER_BY_OBJPATH"] not in {"true", "false"}:
        raise ValueError("ORDER_BY_OBJPATH must be true or false")


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {Path(sys.argv[0]).name} CONFIG.env", file=sys.stderr)
        return 64
    try:
        values = read_values(Path(sys.argv[1]))
        validate(values)
        sys.stdout.write(Template(TEMPLATE.read_text()).substitute(values))
    except (OSError, ValueError, KeyError) as exc:
        print(f"render error: {exc}", file=sys.stderr)
        return 64
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
