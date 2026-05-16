"""Edge ingestion for local-only sources pushing into server vadimgest."""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from filelock import FileLock

from .config import get_data_dir
from .store import DataStore


MAX_EDGE_BATCH_EVENTS = 1000
_SOURCE_SAFE = re.compile(r"[^a-zA-Z0-9_-]+")
EDGE_TOKEN_PREFIX = "vg_edge_"


class EdgeIngestError(ValueError):
    """Raised for invalid edge ingest payloads."""


@dataclass
class EdgeIngestResult:
    accepted: int = 0
    skipped: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)
    records: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": not self.errors,
            "accepted": self.accepted,
            "skipped": self.skipped,
            "errors": self.errors,
            "records": self.records,
        }


class EdgeAuthError(PermissionError):
    """Raised when an edge token is missing, invalid, or revoked."""


@dataclass
class EdgeTokenIssue:
    token: str
    metadata: dict[str, Any]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _edge_dir(base_path: str | Path | None = None) -> Path:
    path = Path(base_path).expanduser() if base_path else get_data_dir()
    edge_dir = path / "edge"
    edge_dir.mkdir(parents=True, exist_ok=True)
    return edge_dir


def _token_registry_path(base_path: str | Path | None = None) -> Path:
    return _edge_dir(base_path) / "tokens.json"


def _load_token_registry(base_path: str | Path | None = None) -> dict[str, Any]:
    path = _token_registry_path(base_path)
    if not path.exists():
        return {"tokens": []}
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError:
        return {"tokens": []}
    if not isinstance(data, dict) or not isinstance(data.get("tokens"), list):
        return {"tokens": []}
    return data


def _save_token_registry(data: dict[str, Any], base_path: str | Path | None = None):
    path = _token_registry_path(base_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, path)
        os.chmod(path, 0o600)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


def list_edge_tokens(base_path: str | Path | None = None) -> list[dict[str, Any]]:
    """List edge token metadata without plaintext token or hash."""
    registry = _load_token_registry(base_path)
    result = []
    for item in registry.get("tokens", []):
        public = {k: v for k, v in item.items() if k != "hash"}
        public["active"] = not bool(item.get("revoked_at"))
        result.append(public)
    return result


def create_edge_token(label: str = "", base_path: str | Path | None = None) -> EdgeTokenIssue:
    """Create an edge bearer token. Plaintext is returned once."""
    token = EDGE_TOKEN_PREFIX + secrets.token_urlsafe(32)
    token_id = secrets.token_hex(8)
    now = _now()
    metadata = {
        "id": token_id,
        "label": str(label or "").strip() or "Edge device",
        "hash": _token_hash(token),
        "created_at": now,
        "last_seen_at": None,
        "revoked_at": None,
    }
    path = _token_registry_path(base_path)
    with FileLock(str(path) + ".lock"):
        registry = _load_token_registry(base_path)
        registry.setdefault("tokens", []).append(metadata)
        _save_token_registry(registry, base_path)
    public = {k: v for k, v in metadata.items() if k != "hash"}
    public["active"] = True
    return EdgeTokenIssue(token=token, metadata=public)


def revoke_edge_token(token_id: str, base_path: str | Path | None = None) -> bool:
    """Mark an edge token as revoked."""
    path = _token_registry_path(base_path)
    with FileLock(str(path) + ".lock"):
        registry = _load_token_registry(base_path)
        for item in registry.get("tokens", []):
            if item.get("id") == token_id:
                if not item.get("revoked_at"):
                    item["revoked_at"] = _now()
                    _save_token_registry(registry, base_path)
                return True
    return False


def verify_edge_token(token: str, base_path: str | Path | None = None, *, touch: bool = True) -> dict[str, Any]:
    """Verify a bearer token and return public token metadata."""
    token = str(token or "").strip()
    if not token:
        raise EdgeAuthError("edge token required")
    digest = _token_hash(token)
    path = _token_registry_path(base_path)
    with FileLock(str(path) + ".lock"):
        registry = _load_token_registry(base_path)
        for item in registry.get("tokens", []):
            if item.get("hash") == digest:
                if item.get("revoked_at"):
                    raise EdgeAuthError("edge token revoked")
                if touch:
                    item["last_seen_at"] = _now()
                    _save_token_registry(registry, base_path)
                public = {k: v for k, v in item.items() if k != "hash"}
                public["active"] = True
                return public
    raise EdgeAuthError("invalid edge token")


def sanitize_source(source: str) -> str:
    cleaned = _SOURCE_SAFE.sub("_", str(source or "").strip().lower()).strip("_")
    if not cleaned:
        raise EdgeIngestError("source required")
    if cleaned in {".", ".."}:
        raise EdgeIngestError("invalid source")
    return cleaned[:80]


def _stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _event_id(source: str, event: dict[str, Any]) -> str:
    explicit = str(event.get("event_id") or event.get("id") or "").strip()
    if explicit:
        return explicit
    source_uri = str(event.get("source_uri") or "").strip()
    if source_uri:
        basis = f"{source}:{source_uri}"
    else:
        basis = f"{source}:{_stable_json(event)}"
    return "edge_" + hashlib.sha256(basis.encode("utf-8")).hexdigest()[:24]


def normalize_edge_event(
    event: dict[str, Any],
    *,
    batch_source: str | None = None,
    device_id: str | None = None,
    received_at: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """Normalize one edge event to a DataStore source name and record dict."""
    if not isinstance(event, dict):
        raise EdgeIngestError("event must be an object")

    source = sanitize_source(str(event.get("source") or batch_source or ""))
    event_id = _event_id(source, event)
    source_uri = str(event.get("source_uri") or "").strip()
    observed_at = (
        event.get("observed_at")
        or event.get("timestamp")
        or event.get("ts")
        or event.get("date")
        or received_at
        or _now()
    )

    raw_privacy = event.get("privacy") if isinstance(event.get("privacy"), dict) else {}
    attachments = event.get("attachments")
    if attachments is None:
        attachments = []
    if not isinstance(attachments, list):
        raise EdgeIngestError("attachments must be a list")

    record = dict(event)
    record.pop("source", None)
    record["id"] = event_id
    record["type"] = record.get("type") or "edge_event"
    record["source_uri"] = source_uri
    record["observed_at"] = observed_at
    record["timestamp"] = record.get("timestamp") or observed_at
    record["actor"] = record.get("actor") or record.get("sender") or ""
    record["text"] = record.get("text") or record.get("content") or ""
    record["attachments"] = attachments
    record["privacy"] = {
        "raw_uploaded": bool(raw_privacy.get("raw_uploaded", True)),
        "redaction": raw_privacy.get("redaction") or "none",
    }
    edge_meta = record.get("edge") if isinstance(record.get("edge"), dict) else {}
    edge_meta.update({
        "device_id": device_id or event.get("device_id") or edge_meta.get("device_id") or "",
        "received_at": received_at or _now(),
        "source": source,
    })
    record["edge"] = edge_meta
    if not isinstance(record.get("meta"), dict):
        record["meta"] = {}

    return source, record


def ingest_edge_batch(store: DataStore, payload: dict[str, Any]) -> EdgeIngestResult:
    """Ingest a local edge-agent batch into DataStore idempotently."""
    if not isinstance(payload, dict):
        raise EdgeIngestError("payload must be an object")

    events = payload.get("events")
    if not isinstance(events, list):
        raise EdgeIngestError("events must be a list")
    if len(events) > MAX_EDGE_BATCH_EVENTS:
        raise EdgeIngestError(f"events exceeds max batch size {MAX_EDGE_BATCH_EVENTS}")

    batch_source = payload.get("source")
    device_id = str(payload.get("device_id") or "").strip()
    received_at = _now()
    result = EdgeIngestResult()

    for idx, event in enumerate(events):
        try:
            source, record = normalize_edge_event(
                event,
                batch_source=batch_source,
                device_id=device_id,
                received_at=received_at,
            )
            if store.exists(source, record["id"]):
                result.skipped += 1
                result.records.append({
                    "index": idx,
                    "source": source,
                    "id": record["id"],
                    "status": "skipped",
                })
                continue
            appended = store.append(source, record)
            result.accepted += 1
            result.records.append({
                "index": idx,
                "source": source,
                "id": record["id"],
                "line": appended._line,
                "status": "accepted",
            })
        except EdgeIngestError as exc:
            result.errors.append({"index": idx, "error": str(exc)})

    return result
