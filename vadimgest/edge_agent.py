"""Local edge-agent runtime for pushing vadimgest records to a server."""

from __future__ import annotations

import json
import os
import signal
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .config import get_data_dir, get_edge_config, get_source_config
from .ingest.sources import all_source_names, get_load_error, get_syncer_class
from .store import DataStore


UploadTransport = Callable[[str, str, dict[str, Any], int], tuple[int, dict[str, Any]]]


class EdgeAgentError(RuntimeError):
    """Raised when edge-agent configuration or upload fails."""


@dataclass
class EdgeSourceResult:
    source: str
    synced: int = 0
    uploaded: int = 0
    skipped: int = 0
    failed: int = 0
    pending: int = 0
    checkpoint: int = 0
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "synced": self.synced,
            "uploaded": self.uploaded,
            "skipped": self.skipped,
            "failed": self.failed,
            "pending": self.pending,
            "checkpoint": self.checkpoint,
            "error": self.error,
        }


@dataclass
class EdgeRunResult:
    ok: bool = True
    device_id: str = ""
    server_url: str = ""
    sources: list[EdgeSourceResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok and not any(s.error for s in self.sources),
            "device_id": self.device_id,
            "server_url": self.server_url,
            "sources": [s.to_dict() for s in self.sources],
        }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_transport(url: str, token: str, payload: dict[str, Any], timeout: int) -> tuple[int, dict[str, Any]]:
    body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "vadimgest-edge-agent/1",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, json.loads(raw or "{}")
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8")
        try:
            data = json.loads(raw or "{}")
        except json.JSONDecodeError:
            data = {"ok": False, "error": raw or str(e)}
        return e.code, data


class EdgeAgent:
    """Sync local enabled sources and upload pending records to the server."""

    def __init__(
        self,
        store: DataStore,
        config: dict[str, Any] | None = None,
        *,
        token: str | None = None,
        transport: UploadTransport | None = None,
    ):
        self.store = store
        self.config = config or get_edge_config()
        self.token = token if token is not None else os.environ.get("VADIMGEST_EDGE_TOKEN", "")
        self.transport = transport or _default_transport
        self.state_file = self.store.base_path / "edge_state.json"

    def selected_sources(self) -> list[str]:
        if not self.config.get("enabled", False):
            return []
        configured = self.config.get("sources")
        if configured:
            return [str(s) for s in configured if get_syncer_class(str(s)) is not None]
        result = []
        for source in all_source_names():
            source_config = get_source_config(source)
            if source_config.get("enabled", False) and get_syncer_class(source) is not None:
                result.append(source)
        return result

    def validate_config(self):
        if not self.config.get("server_url"):
            raise EdgeAgentError("edge.server_url is required")
        if not self.token:
            raise EdgeAgentError("VADIMGEST_EDGE_TOKEN is required")

    def _load_state(self) -> dict[str, Any]:
        if not self.state_file.exists():
            return {"sources": {}}
        try:
            data = json.loads(self.state_file.read_text())
        except json.JSONDecodeError:
            return {"sources": {}}
        if not isinstance(data, dict):
            return {"sources": {}}
        data.setdefault("sources", {})
        return data

    def _save_state(self, state: dict[str, Any]):
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=self.state_file.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(state, f, indent=2, ensure_ascii=False)
            os.replace(tmp_path, self.state_file)
        except Exception:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise

    def _get_checkpoint(self, state: dict[str, Any], source: str) -> int:
        item = state.setdefault("sources", {}).get(source, {})
        try:
            return int(item.get("uploaded_line") or 0)
        except (TypeError, ValueError):
            return 0

    def _set_checkpoint(self, state: dict[str, Any], source: str, line: int):
        state.setdefault("sources", {})[source] = {
            "uploaded_line": int(line),
            "updated_at": _now(),
        }

    def _sync_source(self, source: str) -> tuple[int, str | None]:
        cls = get_syncer_class(source)
        if cls is None:
            return 0, get_load_error(source) or "unavailable"
        syncer = cls(self.store, get_source_config(source))
        try:
            count, summary = syncer.sync(limit=10000)
            syncer.log_run("ok", count=count, summary=summary)
            return count, None
        except Exception as e:
            syncer.log_run("error", error=str(e))
            return 0, str(e)

    def _record_to_event(self, record) -> dict[str, Any]:
        event = dict(record.data)
        event["source"] = record._source
        event.setdefault("source_uri", f"vadimgest://{record._source}/{event.get('id') or record._line}")
        edge_upload = event.get("edge_upload") if isinstance(event.get("edge_upload"), dict) else {}
        edge_upload.update({
            "line": record._line,
            "ingested_at": record._ingested_at,
        })
        event["edge_upload"] = edge_upload
        return event

    def _upload_batch(self, source: str, records: list[Any]) -> tuple[int, int, int, int, dict[str, Any]]:
        if not records:
            return 0, 0, 0, 0, {"ok": True, "records": []}
        url = self.config["server_url"].rstrip("/") + "/api/edge/events/batch"
        payload = {
            "device_id": self.config.get("device_id") or "",
            "source": source,
            "events": [self._record_to_event(r) for r in records],
        }
        status, data = self.transport(url, self.token, payload, 30)
        if status not in (200, 207):
            raise EdgeAgentError(data.get("error") or f"upload failed with HTTP {status}")

        failed_indices = {int(e.get("index", -1)) for e in data.get("errors", []) if isinstance(e, dict)}
        accepted = int(data.get("accepted") or 0)
        skipped = int(data.get("skipped") or 0)
        failed = len(failed_indices)
        prefix = 0
        for idx in range(len(records)):
            if idx in failed_indices:
                break
            prefix += 1
        return accepted, skipped, failed, prefix, data

    def run_once(self) -> EdgeRunResult:
        self.validate_config()
        state = self._load_state()
        result = EdgeRunResult(
            device_id=self.config.get("device_id") or "",
            server_url=self.config.get("server_url") or "",
        )

        batch_size = max(1, int(self.config.get("batch_size") or 100))
        for source in self.selected_sources():
            source_result = EdgeSourceResult(source=source)
            synced, sync_error = self._sync_source(source)
            source_result.synced = synced
            if sync_error:
                source_result.error = sync_error
                result.ok = False
                result.sources.append(source_result)
                continue

            checkpoint = self._get_checkpoint(state, source)
            total = self.store.count(source)
            source_result.pending = max(0, total - checkpoint)
            next_line = checkpoint + 1

            try:
                while next_line <= total:
                    end_line = min(total, next_line + batch_size - 1)
                    records = list(self.store.read_range(source, next_line, end_line))
                    if not records:
                        break
                    accepted, skipped, failed, prefix, _ = self._upload_batch(source, records)
                    source_result.uploaded += accepted
                    source_result.skipped += skipped
                    source_result.failed += failed
                    if prefix <= 0:
                        break
                    checkpoint = records[prefix - 1]._line
                    self._set_checkpoint(state, source, checkpoint)
                    self._save_state(state)
                    next_line = checkpoint + 1
                    if failed:
                        break
            except Exception as e:
                source_result.error = str(e)
                result.ok = False

            source_result.checkpoint = checkpoint
            result.sources.append(source_result)

        return result

    def run_forever(self):
        interval = max(1, int(self.config.get("interval_seconds") or 300))
        stop = threading.Event()

        def handle_signal(signum, frame):
            stop.set()

        signal.signal(signal.SIGTERM, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)

        print(f"vadimgest edge-agent starting (interval={interval}s)")
        print(f"Data: {self.store.base_path}")
        print(f"Server: {self.config.get('server_url') or '(not configured)'}")
        print(f"Device: {self.config.get('device_id') or '(none)'}")
        print(f"Sources: {', '.join(self.selected_sources()) or '(none enabled)'}")
        print(f"PID: {os.getpid()}")
        print()

        while not stop.is_set():
            try:
                result = self.run_once()
                payload = result.to_dict()
                total_uploaded = sum(s["uploaded"] for s in payload["sources"])
                total_skipped = sum(s["skipped"] for s in payload["sources"])
                errors = [s for s in payload["sources"] if s.get("error")]
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] "
                    f"uploaded={total_uploaded} skipped={total_skipped} errors={len(errors)}",
                    flush=True,
                )
            except Exception as e:
                print(f"edge-agent cycle error: {e}", flush=True)
            stop.wait(interval)


def run_edge_agent(*, once: bool = False):
    store = DataStore(get_data_dir())
    agent = EdgeAgent(store)
    if once:
        result = agent.run_once().to_dict()
        print(json.dumps(result, indent=2, ensure_ascii=False))
        if not result.get("ok"):
            sys.exit(1)
    else:
        agent.run_forever()
