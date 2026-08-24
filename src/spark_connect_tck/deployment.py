"""Validation and process invocation for the Spark Connect TCK deployment adapter."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

DEPLOYMENT_SCHEMA_VERSION = "SC-TCK-DEPLOYMENT-1"
ADAPTER_PROTOCOL_VERSION = "SC-TCK-ADAPTER-1"
PROFILE_ID = "SC-1.0-P1"

_DESCRIPTOR_FIELDS = frozenset(
    {
        "schema_version",
        "profile_id",
        "tck_commit",
        "adapter_protocol_version",
        "adapter_id",
        "adapter_version",
        "adapter_artifact_digest",
        "required_actions",
        "session_eviction_reasons",
        "optional_actions",
    }
)
_REQUIRED_ACTIONS = ("restart_server", "wait_ready")
_VALID_EVICTION_REASONS = (
    (),
    ("IDLE_TIMEOUT",),
    ("RESOURCE_PRESSURE",),
    ("IDLE_TIMEOUT", "RESOURCE_PRESSURE"),
)
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")
_TCK_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
_ADAPTER_ID = re.compile(r"[A-Za-z][A-Za-z0-9._-]{0,63}\Z")
_ADAPTER_VERSION = re.compile(r"[0-9A-Za-z][0-9A-Za-z._+-]{0,63}\Z")
_ADAPTER_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")


class DeploymentContractError(ValueError):
    """A deployment descriptor or adapter message violates its closed schema."""


class DeploymentAdapterError(RuntimeError):
    """The deployment adapter could not complete an action successfully."""


def _json_object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DeploymentContractError(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def _decode_json_bytes(data: bytes, *, label: str) -> Any:
    if data.startswith(b"\xef\xbb\xbf"):
        raise DeploymentContractError(f"{label} must not contain a UTF-8 byte-order mark")
    if not data.endswith(b"\n") or data.endswith(b"\n\n"):
        raise DeploymentContractError(f"{label} must end with exactly one LF")
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as error:
        raise DeploymentContractError(f"{label} must be valid UTF-8") from error
    if "\r" in text:
        raise DeploymentContractError(f"{label} must not contain CR line endings")
    try:
        return json.loads(text[:-1], object_pairs_hook=_json_object_without_duplicates)
    except DeploymentContractError:
        raise
    except (json.JSONDecodeError, TypeError) as error:
        raise DeploymentContractError(f"{label} must contain exactly one JSON value") from error


def _require_nonempty_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise DeploymentContractError(f"{field} must be a non-empty JSON string")
    return value


def _require_matching_string(value: Any, field: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise DeploymentContractError(f"{field} does not match its required safe-ASCII grammar")
    return value


def _require_canonical_uuid(value: Any, field: str) -> str:
    if not isinstance(value, str) or _UUID.fullmatch(value) is None:
        raise DeploymentContractError(f"{field} must be a canonical lowercase UUID string")
    return value


def _require_exact_fields(value: Any, expected: set[str] | frozenset[str], label: str) -> None:
    if not isinstance(value, dict):
        raise DeploymentContractError(f"{label} must be a JSON object")
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(repr(field) for field in actual - expected)
        raise DeploymentContractError(f"{label} fields differ: missing={missing}, extra={extra}")


@dataclass(frozen=True)
class DeploymentDescriptor:
    """One validated ``SC-TCK-DEPLOYMENT-1`` descriptor."""

    tck_commit: str
    adapter_id: str
    adapter_version: str
    adapter_artifact_digest: str
    session_eviction_reasons: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_matching_string(self.tck_commit, "tck_commit", _TCK_COMMIT)
        _require_matching_string(self.adapter_id, "adapter_id", _ADAPTER_ID)
        _require_matching_string(self.adapter_version, "adapter_version", _ADAPTER_VERSION)
        _require_matching_string(
            self.adapter_artifact_digest, "adapter_artifact_digest", _ADAPTER_DIGEST
        )
        if (
            type(self.session_eviction_reasons) is not tuple
            or self.session_eviction_reasons not in _VALID_EVICTION_REASONS
        ):
            raise DeploymentContractError("session_eviction_reasons is not a canonical array")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> DeploymentDescriptor:
        """Validate the closed descriptor schema and return its typed representation."""
        if not isinstance(value, dict):
            raise DeploymentContractError("deployment descriptor must be a JSON object")
        _require_exact_fields(value, _DESCRIPTOR_FIELDS, "deployment descriptor")

        constants = {
            "schema_version": DEPLOYMENT_SCHEMA_VERSION,
            "profile_id": PROFILE_ID,
            "adapter_protocol_version": ADAPTER_PROTOCOL_VERSION,
        }
        for field, expected in constants.items():
            if type(value[field]) is not str or value[field] != expected:
                raise DeploymentContractError(f"{field} must be exactly {expected!r}")

        required_actions = value["required_actions"]
        if type(required_actions) is not list or required_actions != list(_REQUIRED_ACTIONS):
            raise DeploymentContractError(
                f"required_actions must be exactly {list(_REQUIRED_ACTIONS)!r}"
            )

        reasons = value["session_eviction_reasons"]
        if type(reasons) is not list or tuple(reasons) not in _VALID_EVICTION_REASONS:
            raise DeploymentContractError("session_eviction_reasons is not a canonical array")

        expected_optional = ["evict_session"] if reasons else []
        optional_actions = value["optional_actions"]
        if type(optional_actions) is not list or optional_actions != expected_optional:
            raise DeploymentContractError(f"optional_actions must be exactly {expected_optional!r}")

        return cls(
            tck_commit=value["tck_commit"],
            adapter_id=value["adapter_id"],
            adapter_version=value["adapter_version"],
            adapter_artifact_digest=value["adapter_artifact_digest"],
            session_eviction_reasons=tuple(reasons),
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> DeploymentDescriptor:
        """Parse and require the exact deterministic descriptor serialization."""
        value = _decode_json_bytes(data, label="deployment descriptor")
        descriptor = cls.from_mapping(value)
        if descriptor.canonical_bytes() != data:
            raise DeploymentContractError("deployment descriptor bytes are not canonical JSON")
        return descriptor

    @classmethod
    def from_path(cls, path: str | os.PathLike[str]) -> DeploymentDescriptor:
        """Read and validate a deployment descriptor from disk."""
        return cls.from_bytes(Path(path).read_bytes())

    @property
    def declared_actions(self) -> tuple[str, ...]:
        """Return actions that this descriptor permits the runner to invoke."""
        return _REQUIRED_ACTIONS + (("evict_session",) if self.session_eviction_reasons else ())

    def as_dict(self) -> dict[str, Any]:
        """Return the closed JSON object represented by this descriptor."""
        return {
            "schema_version": DEPLOYMENT_SCHEMA_VERSION,
            "profile_id": PROFILE_ID,
            "tck_commit": self.tck_commit,
            "adapter_protocol_version": ADAPTER_PROTOCOL_VERSION,
            "adapter_id": self.adapter_id,
            "adapter_version": self.adapter_version,
            "adapter_artifact_digest": self.adapter_artifact_digest,
            "required_actions": list(_REQUIRED_ACTIONS),
            "session_eviction_reasons": list(self.session_eviction_reasons),
            "optional_actions": ["evict_session"] if self.session_eviction_reasons else [],
        }

    def canonical_bytes(self) -> bytes:
        """Serialize with the unique line layout required by draft v0.41."""
        value = self.as_dict()
        keys = sorted(value)
        lines = ["{"]
        for index, key in enumerate(keys):
            encoded_value = json.dumps(value[key], ensure_ascii=True, separators=(", ", ": "))
            comma = "," if index < len(keys) - 1 else ""
            lines.append(f'  "{key}": {encoded_value}{comma}')
        lines.append("}")
        return ("\n".join(lines) + "\n").encode("ascii")

    @property
    def sha256(self) -> str:
        """Return the descriptor's bare 64-hex whole-file digest."""
        return hashlib.sha256(self.canonical_bytes()).hexdigest()


def adapter_artifact_digest(path: str | os.PathLike[str]) -> str:
    """Return the prefixed SHA-256 digest required for an adapter artifact."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return f"sha256:{digest.hexdigest()}"


def build_adapter_request(
    descriptor: DeploymentDescriptor,
    action: str,
    arguments: Mapping[str, Any],
    *,
    request_id: str | None = None,
) -> dict[str, Any]:
    """Build one strictly validated ``SC-TCK-ADAPTER-1`` request."""
    if type(action) is not str or action not in descriptor.declared_actions:
        raise DeploymentContractError(f"adapter action is not declared: {action!r}")
    if not isinstance(arguments, dict):
        raise DeploymentContractError("arguments must be a JSON object")

    if action == "restart_server":
        _require_exact_fields(arguments, set(), "restart_server arguments")
    elif action == "wait_ready":
        _require_exact_fields(arguments, {"timeout_ms"}, "wait_ready arguments")
        timeout_ms = arguments["timeout_ms"]
        if type(timeout_ms) is not int or timeout_ms <= 0:
            raise DeploymentContractError("timeout_ms must be a JSON integer greater than zero")
    elif action == "evict_session":
        _require_exact_fields(arguments, {"session_id", "reason"}, "evict_session arguments")
        _require_canonical_uuid(arguments["session_id"], "session_id")
        reason = arguments["reason"]
        if type(reason) is not str or reason not in descriptor.session_eviction_reasons:
            raise DeploymentContractError("reason must be declared in session_eviction_reasons")

    canonical_request_id = _require_canonical_uuid(
        str(uuid4()) if request_id is None else request_id, "request_id"
    )
    return {
        "protocol_version": ADAPTER_PROTOCOL_VERSION,
        "request_id": canonical_request_id,
        "action": action,
        "arguments": dict(arguments),
    }


def parse_adapter_response(data: bytes, *, request_id: str) -> dict[str, Any]:
    """Parse one exact OK or ERROR response and verify its request identity."""
    expected_request_id = _require_canonical_uuid(request_id, "request_id")
    value = _decode_json_bytes(data, label="deployment adapter response")
    if not isinstance(value, dict):
        raise DeploymentContractError("deployment adapter response must be a JSON object")

    status = value.get("status")
    if status == "OK":
        expected_fields = {"protocol_version", "request_id", "status"}
    elif status == "ERROR":
        expected_fields = {"protocol_version", "request_id", "status", "error_code", "message"}
    else:
        raise DeploymentContractError("status must be the JSON string 'OK' or 'ERROR'")
    _require_exact_fields(value, expected_fields, "deployment adapter response")

    if type(value["protocol_version"]) is not str or (
        value["protocol_version"] != ADAPTER_PROTOCOL_VERSION
    ):
        raise DeploymentContractError(
            f"protocol_version must be exactly {ADAPTER_PROTOCOL_VERSION!r}"
        )
    if type(value["request_id"]) is not str or value["request_id"] != expected_request_id:
        raise DeploymentContractError("deployment adapter response request_id does not match")
    if status == "ERROR":
        _require_nonempty_string(value["error_code"], "error_code")
        _require_nonempty_string(value["message"], "message")
    return value


def invoke_adapter(
    executable: str | os.PathLike[str],
    descriptor: DeploymentDescriptor,
    action: str,
    arguments: Mapping[str, Any],
    *,
    timeout_seconds: float = 30,
) -> dict[str, Any]:
    """Run one adapter process and require one matching successful response."""
    if not isinstance(timeout_seconds, int | float) or isinstance(timeout_seconds, bool):
        raise DeploymentContractError("timeout_seconds must be a positive number")
    if timeout_seconds <= 0:
        raise DeploymentContractError("timeout_seconds must be a positive number")

    executable_path = Path(executable)
    actual_digest = adapter_artifact_digest(executable_path)
    if actual_digest != descriptor.adapter_artifact_digest:
        raise DeploymentAdapterError("deployment adapter artifact digest does not match descriptor")

    request = build_adapter_request(descriptor, action, arguments)
    request_data = (
        json.dumps(request, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
    ).encode()
    try:
        completed = subprocess.run(
            [os.fspath(executable_path)],
            input=request_data,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise DeploymentAdapterError("deployment adapter timed out") from error
    except OSError as error:
        raise DeploymentAdapterError("deployment adapter could not be started") from error

    if completed.returncode != 0:
        raise DeploymentAdapterError(
            f"deployment adapter exited with status {completed.returncode}"
        )
    response = parse_adapter_response(completed.stdout, request_id=request["request_id"])
    if response["status"] == "ERROR":
        raise DeploymentAdapterError(
            f"deployment adapter returned ERROR {response['error_code']}: {response['message']}"
        )
    return response
