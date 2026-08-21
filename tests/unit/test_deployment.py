"""Unit coverage for the draft v0.37 deployment descriptor and adapter protocol."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import pytest

from spark_connect_tck.deployment import (
    ADAPTER_PROTOCOL_VERSION,
    DEPLOYMENT_SCHEMA_VERSION,
    PROFILE_ID,
    DeploymentAdapterError,
    DeploymentContractError,
    DeploymentDescriptor,
    adapter_artifact_digest,
    build_adapter_request,
    invoke_adapter,
    parse_adapter_response,
)

REQUEST_ID = "00112233-4455-6677-8899-aabbccddeeff"
SESSION_ID = "ffeeddcc-bbaa-9988-7766-554433221100"


def _descriptor_mapping(
    *, reasons: list[str] | None = None, adapter_digest: str = f"sha256:{'1' * 64}"
) -> dict[str, object]:
    eviction_reasons = [] if reasons is None else reasons
    return {
        "schema_version": DEPLOYMENT_SCHEMA_VERSION,
        "profile_id": PROFILE_ID,
        "tck_commit": "a" * 40,
        "adapter_protocol_version": ADAPTER_PROTOCOL_VERSION,
        "adapter_id": "spark-connect-test-adapter",
        "adapter_version": "1.0.0",
        "adapter_artifact_digest": adapter_digest,
        "required_actions": ["restart_server", "wait_ready"],
        "session_eviction_reasons": eviction_reasons,
        "optional_actions": ["evict_session"] if eviction_reasons else [],
    }


def _canonical_bytes(value: dict[str, object]) -> bytes:
    keys = sorted(value)
    lines = ["{"]
    for index, key in enumerate(keys):
        encoded_value = json.dumps(value[key], ensure_ascii=True, separators=(", ", ": "))
        comma = "," if index < len(keys) - 1 else ""
        lines.append(f'  "{key}": {encoded_value}{comma}')
    lines.append("}")
    return ("\n".join(lines) + "\n").encode("ascii")


def test_descriptor_requires_and_reproduces_canonical_bytes(tmp_path: Path) -> None:
    data = _canonical_bytes(_descriptor_mapping(reasons=["IDLE_TIMEOUT", "RESOURCE_PRESSURE"]))
    descriptor = DeploymentDescriptor.from_bytes(data)
    path = tmp_path / "deployment.json"
    path.write_bytes(data)

    assert DeploymentDescriptor.from_path(path) == descriptor
    assert descriptor.canonical_bytes() == data
    assert descriptor.sha256 == hashlib.sha256(data).hexdigest()
    assert len(descriptor.sha256) == 64
    assert not descriptor.sha256.startswith("sha256:")
    assert descriptor.declared_actions == ("restart_server", "wait_ready", "evict_session")
    assert len(data.splitlines()) == 12
    assert b'["restart_server", "wait_ready"]' in data
    assert b'["IDLE_TIMEOUT", "RESOURCE_PRESSURE"]' in data


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda value: value.update(extra=True), "fields differ"),
        (lambda value: value.update(schema_version="SC-TCK-DEPLOYMENT-2"), "schema_version"),
        (lambda value: value.update(profile_id=1), "profile_id"),
        (lambda value: value.update(tck_commit=""), "tck_commit"),
        (lambda value: value.update(tck_commit="A" * 40), "tck_commit"),
        (lambda value: value.update(adapter_id="1adapter"), "adapter_id"),
        (lambda value: value.update(adapter_id="spärk"), "adapter_id"),
        (lambda value: value.update(adapter_version="version/1"), "adapter_version"),
        (lambda value: value.update(adapter_version="a" * 65), "adapter_version"),
        (
            lambda value: value.update(adapter_artifact_digest="1" * 64),
            "adapter_artifact_digest",
        ),
        (lambda value: value.update(required_actions=["wait_ready", "restart_server"]), "required"),
        (
            lambda value: value.update(
                session_eviction_reasons=["RESOURCE_PRESSURE", "IDLE_TIMEOUT"],
                optional_actions=["evict_session"],
            ),
            "canonical array",
        ),
        (
            lambda value: value.update(
                session_eviction_reasons=["IDLE_TIMEOUT"], optional_actions=[]
            ),
            "optional_actions",
        ),
    ],
)
def test_descriptor_rejects_invalid_closed_schema(
    change: Callable[[dict[str, object]], object], message: str
) -> None:
    value = _descriptor_mapping()
    change(value)

    with pytest.raises(DeploymentContractError, match=message):
        DeploymentDescriptor.from_mapping(value)


@pytest.mark.parametrize(
    "data",
    [
        b"\xef\xbb\xbf" + _canonical_bytes(_descriptor_mapping()),
        _canonical_bytes(_descriptor_mapping()).replace(b"\n", b"\r\n"),
        _canonical_bytes(_descriptor_mapping()) + b"\n",
        (json.dumps(_descriptor_mapping(), sort_keys=False) + "\n").encode(),
        (json.dumps(_descriptor_mapping(), indent=2, sort_keys=True) + "\n").encode(),
        _canonical_bytes(_descriptor_mapping()).replace(
            b"spark-connect-test-adapter", rb"spark-connect-test-\u0061dapter"
        ),
        _canonical_bytes(_descriptor_mapping()).replace(
            b'  "adapter_id": "spark-connect-test-adapter",\n',
            b'  "adapter_id": "duplicate",\n  "adapter_id": "spark-connect-test-adapter",\n',
        ),
    ],
)
def test_descriptor_rejects_noncanonical_serialization(data: bytes) -> None:
    with pytest.raises(DeploymentContractError):
        DeploymentDescriptor.from_bytes(data)


def test_adapter_requests_have_exact_action_arguments() -> None:
    descriptor = DeploymentDescriptor.from_mapping(_descriptor_mapping(reasons=["IDLE_TIMEOUT"]))

    assert build_adapter_request(descriptor, "restart_server", {}, request_id=REQUEST_ID) == {
        "protocol_version": ADAPTER_PROTOCOL_VERSION,
        "request_id": REQUEST_ID,
        "action": "restart_server",
        "arguments": {},
    }
    assert build_adapter_request(
        descriptor, "wait_ready", {"timeout_ms": 1000}, request_id=REQUEST_ID
    )["arguments"] == {"timeout_ms": 1000}
    assert build_adapter_request(
        descriptor,
        "evict_session",
        {"session_id": SESSION_ID, "reason": "IDLE_TIMEOUT"},
        request_id=REQUEST_ID,
    )["arguments"] == {"session_id": SESSION_ID, "reason": "IDLE_TIMEOUT"}


@pytest.mark.parametrize(
    ("action", "arguments", "message"),
    [
        ("restart_server", {"unexpected": True}, "fields differ"),
        ("wait_ready", {}, "fields differ"),
        ("wait_ready", {"timeout_ms": True}, "integer"),
        ("wait_ready", {"timeout_ms": 0}, "greater than zero"),
        ("evict_session", {"session_id": SESSION_ID, "reason": "IDLE_TIMEOUT"}, "declared"),
        ("unknown", {}, "not declared"),
    ],
)
def test_adapter_request_rejects_invalid_fields(
    action: str, arguments: dict[str, object], message: str
) -> None:
    descriptor = DeploymentDescriptor.from_mapping(_descriptor_mapping())

    with pytest.raises(DeploymentContractError, match=message):
        build_adapter_request(descriptor, action, arguments, request_id=REQUEST_ID)


@pytest.mark.parametrize(
    "request_id",
    [
        "00112233-4455-6677-8899-AABBCCDDEEFF",
        "{00112233-4455-6677-8899-aabbccddeeff}",
        1,
        "",
    ],
)
def test_explicit_adapter_request_id_must_be_a_canonical_string(request_id: object) -> None:
    descriptor = DeploymentDescriptor.from_mapping(_descriptor_mapping())

    with pytest.raises(DeploymentContractError, match="request_id"):
        build_adapter_request(
            descriptor,
            "restart_server",
            {},
            request_id=request_id,  # type: ignore[arg-type]
        )


def test_adapter_response_accepts_only_exact_ok_and_error_shapes() -> None:
    ok = {
        "protocol_version": ADAPTER_PROTOCOL_VERSION,
        "request_id": REQUEST_ID,
        "status": "OK",
    }
    error = {
        **ok,
        "status": "ERROR",
        "error_code": "RESTART_FAILED",
        "message": "replacement did not start",
    }

    assert parse_adapter_response((json.dumps(ok) + "\n").encode(), request_id=REQUEST_ID) == ok
    assert (
        parse_adapter_response((json.dumps(error) + "\n").encode(), request_id=REQUEST_ID) == error
    )


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda value: value.update(extra="no"), "fields differ"),
        (lambda value: value.pop("protocol_version"), "fields differ"),
        (lambda value: value.update(protocol_version=1), "protocol_version"),
        (lambda value: value.update(request_id=SESSION_ID), "does not match"),
        (lambda value: value.update(status=True), "status"),
        (lambda value: value.update(status="ERROR", error_code="", message="bad"), "error_code"),
    ],
)
def test_adapter_response_rejects_missing_extra_or_incorrectly_typed_fields(
    change: Callable[[dict[str, object]], object], message: str
) -> None:
    value: dict[str, object] = {
        "protocol_version": ADAPTER_PROTOCOL_VERSION,
        "request_id": REQUEST_ID,
        "status": "OK",
    }
    change(value)

    with pytest.raises(DeploymentContractError, match=message):
        parse_adapter_response((json.dumps(value) + "\n").encode(), request_id=REQUEST_ID)


def test_adapter_response_rejects_duplicate_fields_and_stdout_diagnostics() -> None:
    duplicate = (
        '{"protocol_version":"SC-TCK-ADAPTER-1","request_id":"'
        + REQUEST_ID
        + '","status":"OK","status":"OK"}\n'
    ).encode()
    diagnostic = (
        json.dumps(
            {
                "protocol_version": ADAPTER_PROTOCOL_VERSION,
                "request_id": REQUEST_ID,
                "status": "OK",
            }
        )
        + "\ndiagnostic\n"
    ).encode()

    with pytest.raises(DeploymentContractError, match="duplicate"):
        parse_adapter_response(duplicate, request_id=REQUEST_ID)
    with pytest.raises(DeploymentContractError):
        parse_adapter_response(diagnostic, request_id=REQUEST_ID)


def test_invoke_adapter_checks_digest_and_matching_success(tmp_path: Path) -> None:
    adapter = tmp_path / "adapter"
    adapter.write_text(
        "#!/usr/bin/env python3\n"
        "import json, sys\n"
        "request = json.loads(sys.stdin.read())\n"
        "print(json.dumps({\n"
        "    'protocol_version': 'SC-TCK-ADAPTER-1',\n"
        "    'request_id': request['request_id'],\n"
        "    'status': 'OK',\n"
        "}))\n"
    )
    adapter.chmod(0o755)
    descriptor = DeploymentDescriptor.from_mapping(
        _descriptor_mapping(adapter_digest=adapter_artifact_digest(adapter))
    )

    response = invoke_adapter(adapter, descriptor, "restart_server", {})
    assert response["status"] == "OK"

    mismatched = replace(descriptor, adapter_artifact_digest=f"sha256:{'0' * 64}")
    with pytest.raises(DeploymentAdapterError, match="digest"):
        invoke_adapter(adapter, mismatched, "restart_server", {})


def test_invoke_adapter_rejects_error_response(tmp_path: Path) -> None:
    adapter = tmp_path / "error-adapter"
    adapter.write_text(
        "#!/usr/bin/env python3\n"
        "import json, sys\n"
        "request = json.loads(sys.stdin.read())\n"
        "print(json.dumps({\n"
        "    'protocol_version': 'SC-TCK-ADAPTER-1',\n"
        "    'request_id': request['request_id'],\n"
        "    'status': 'ERROR',\n"
        "    'error_code': 'RESTART_FAILED',\n"
        "    'message': 'replacement did not start',\n"
        "}))\n"
    )
    adapter.chmod(0o755)
    descriptor = DeploymentDescriptor.from_mapping(
        _descriptor_mapping(adapter_digest=adapter_artifact_digest(adapter))
    )

    with pytest.raises(DeploymentAdapterError, match="ERROR RESTART_FAILED"):
        invoke_adapter(adapter, descriptor, "restart_server", {})


def test_invoke_adapter_rejects_nonzero_exit_and_start_failure(tmp_path: Path) -> None:
    adapter = tmp_path / "failing-adapter"
    adapter.write_text("#!/usr/bin/env python3\nraise SystemExit(7)\n")
    adapter.chmod(0o755)
    descriptor = DeploymentDescriptor.from_mapping(
        _descriptor_mapping(adapter_digest=adapter_artifact_digest(adapter))
    )

    with pytest.raises(DeploymentAdapterError, match="status 7"):
        invoke_adapter(adapter, descriptor, "restart_server", {})

    adapter.chmod(0o644)
    with pytest.raises(DeploymentAdapterError, match="could not be started"):
        invoke_adapter(adapter, descriptor, "restart_server", {})


def test_invoke_adapter_rejects_timeout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = tmp_path / "slow-adapter"
    adapter.write_text("adapter artifact")
    descriptor = DeploymentDescriptor.from_mapping(
        _descriptor_mapping(adapter_digest=adapter_artifact_digest(adapter))
    )

    def time_out(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise subprocess.TimeoutExpired(cmd=os.fspath(adapter), timeout=1)

    monkeypatch.setattr("spark_connect_tck.deployment.subprocess.run", time_out)
    with pytest.raises(DeploymentAdapterError, match="timed out"):
        invoke_adapter(adapter, descriptor, "restart_server", {}, timeout_seconds=1)


def test_adapter_artifact_digest_is_prefixed_lowercase_hex(tmp_path: Path) -> None:
    artifact = tmp_path / "adapter.bin"
    artifact.write_bytes(os.urandom(32))

    digest = adapter_artifact_digest(artifact)
    assert digest.startswith("sha256:")
    assert len(digest) == len("sha256:") + 64
    assert digest == digest.lower()
