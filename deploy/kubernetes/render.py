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
    "S3AUTH",
    "S3PROFILE",
    "SAMPLES",
    "SERVICE_ACCOUNT",
    "TTL_SECONDS_AFTER_FINISHED",
    "USEAGE_HOURS",
    "VERBOSE",
}
DELETE_CONFIRMATION = "DELETE_ORPHANS"
# Mirrors MINIMUM_USEAGE_HOURS in s3gc.py. Enforced here as well so a bad value
# fails at render time rather than after a Job has been applied to a cluster.
MINIMUM_USEAGE_HOURS = 24
# Optional keys and their defaults. An empty value renders no environment
# variable at all, because s3gc parses S3GC_USETOTAL as an integer and would
# reject an empty string.
# RUNLOG defaults on: the durable run-log table is the only record that
# outlives the Job, whose pod and logs ttlSecondsAfterFinished deletes.
OPTIONAL = {"USETOTAL": "", "RUNLOG": "true"}
# Environment variables dropped from the manifest when they render empty.
OPTIONAL_ENV = ("S3GC_USETOTAL",)
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
    for key, default in OPTIONAL.items():
        values.setdefault(key, default)
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
    if not values["USEAGE_HOURS"].isdigit():
        raise ValueError("USEAGE_HOURS must be a non-negative integer")
    # dev-automation seeds and deletes its own fixtures within minutes, and is
    # already documented as non-production. Every other phase gets the floor.
    if (
        int(values["USEAGE_HOURS"]) < MINIMUM_USEAGE_HOURS
        and values["PHASE"] != "dev-automation"
    ):
        raise ValueError(
            f"USEAGE_HOURS must be at least {MINIMUM_USEAGE_HOURS} for PHASE={values['PHASE']}: "
            "the age window is the only protection against deleting a part between "
            "its upload to S3 and its registration in system.remote_data_paths"
        )
    if values["S3AUTH"] not in {"static", "aws", "iam"}:
        raise ValueError("S3AUTH must be static, aws or iam")
    if values["S3PROFILE"] and values["S3AUTH"] != "aws":
        raise ValueError("S3PROFILE requires S3AUTH=aws")
    if values["VERBOSE"] not in {"true", "false"}:
        raise ValueError("VERBOSE must be true or false")
    if values["ORDER_BY_OBJPATH"] not in {"true", "false"}:
        raise ValueError("ORDER_BY_OBJPATH must be true or false")
    if values["RUNLOG"] not in {"true", "false"}:
        raise ValueError("RUNLOG must be true or false")
    if values["USETOTAL"] and (
        not values["USETOTAL"].isdigit() or int(values["USETOTAL"]) < 1
    ):
        raise ValueError("USETOTAL must be a positive integer when set")


def drop_empty_image_pull_secret(manifest: str) -> str:
    """Remove the imagePullSecrets block when no secret was configured.

    The published image is public, so most deployments need no pull secret at
    all — and rendering `- name: ""` would be both meaningless and rejected.
    string.Template has no conditionals, so this is done after substitution.
    """
    lines = manifest.splitlines(keepends=True)
    out = []
    index = 0
    while index < len(lines):
        if lines[index].strip() == "imagePullSecrets:" and index + 1 < len(lines):
            following = lines[index + 1].strip()
            if following in ('- name: ""', "- name: ''", "- name:"):
                index += 2
                continue
        out.append(lines[index])
        index += 1
    return "".join(out)


def drop_empty_optional_env(manifest: str) -> str:
    """Remove optional `S3GC_*` env entries that rendered with an empty value.

    string.Template has no conditionals, so an unset optional key would emit
    `value: ""`. s3gc parses these as integers and rejects the empty string, so
    the variable must be absent rather than empty.
    """
    lines = manifest.splitlines(keepends=True)
    out = []
    index = 0
    while index < len(lines):
        name = lines[index].strip()
        if (
            any(name == f"- name: {var}" for var in OPTIONAL_ENV)
            and index + 1 < len(lines)
            and lines[index + 1].strip() in ('value: ""', "value: ''", "value:")
        ):
            index += 2
            continue
        out.append(lines[index])
        index += 1
    return "".join(out)


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {Path(sys.argv[0]).name} CONFIG.env", file=sys.stderr)
        return 64
    try:
        values = read_values(Path(sys.argv[1]))
        validate(values)
        rendered = Template(TEMPLATE.read_text()).substitute(values)
        sys.stdout.write(drop_empty_optional_env(drop_empty_image_pull_secret(rendered)))
    except (OSError, ValueError, KeyError) as exc:
        print(f"render error: {exc}", file=sys.stderr)
        return 64
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
