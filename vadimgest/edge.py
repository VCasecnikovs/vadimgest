"""Edge ingestion for local-only sources pushing into server vadimgest."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .store import DataStore


MAX_EDGE_BATCH_EVENTS = 1000
_SOURCE_SAFE = re.compile(r"[^a-zA-Z0-9_-]+")


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


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


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

    record = {
        "id": event_id,
        "type": event.get("type") or "edge_event",
        "source_uri": source_uri,
        "observed_at": observed_at,
        "timestamp": observed_at,
        "actor": event.get("actor") or event.get("sender") or "",
        "text": event.get("text") or event.get("content") or "",
        "attachments": attachments,
        "privacy": {
            "raw_uploaded": bool(raw_privacy.get("raw_uploaded", True)),
            "redaction": raw_privacy.get("redaction") or "none",
        },
        "edge": {
            "device_id": device_id or event.get("device_id") or "",
            "received_at": received_at or _now(),
            "source": source,
        },
        "meta": event.get("meta") if isinstance(event.get("meta"), dict) else {},
    }

    for optional in ("thread_id", "conversation_id", "external_id"):
        if event.get(optional):
            record[optional] = event[optional]

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
