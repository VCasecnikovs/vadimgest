"""Data store for vadimgest - combines ingest and consumer functionality."""

import json
import os
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Iterator
from filelock import FileLock

from .models import Record, SourceState, ConsumerCheckpoint


def _normalize_ts(ts) -> str:
    """Normalize a timestamp value to a lexicographically-comparable ISO-like string.

    Handles RFC 2822 ("Wed, 8 Apr 2026 20:10:08 -0700"), Twitter's
    ("Wed Apr 29 13:28:24 +0000 2026"), ISO ("2026-04-19T..."),
    "YYYY-MM-DD HH:MM" and numeric epoch values. Returns the original string if
    nothing parses so callers can still do a last-resort string compare.
    """
    if ts is None:
        return ""
    if isinstance(ts, (int, float)):
        try:
            return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
        except (OverflowError, OSError, ValueError):
            return str(ts)
    s = str(ts).strip()
    if not s:
        return ""
    # RFC 2822 starts with a weekday name + comma
    if re.match(r'^[A-Za-z]{3},', s):
        try:
            dt = parsedate_to_datetime(s)
            if dt is not None:
                return dt.astimezone(timezone.utc).isoformat()
        except (TypeError, ValueError):
            pass
    # Twitter / asctime-with-tz: "Wed Apr 29 13:28:24 +0000 2026"
    if re.match(r'^[A-Za-z]{3} [A-Za-z]{3} ', s):
        try:
            dt = datetime.strptime(s, "%a %b %d %H:%M:%S %z %Y")
            return dt.astimezone(timezone.utc).isoformat()
        except ValueError:
            pass
    # ISO-ish — normalize space separator to T so all ISO values compare uniformly
    try:
        dt = datetime.fromisoformat(s.replace(" ", "T", 1))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except ValueError:
        pass
    return s


class DataStore:
    """
    Append-only data store.

    Ingest: append records, dedup, state tracking.
    Consumer: read records, checkpoints, markdown export.
    """

    def __init__(self, base_path: str | Path):
        self.base_path = Path(base_path).expanduser()
        self.sources_dir = self.base_path / "sources"
        self.checkpoints_dir = self.base_path / "checkpoints"
        self.state_file = self.base_path / "state.json"

        self.sources_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoints_dir.mkdir(parents=True, exist_ok=True)

    # ── State Management ──

    def get_state(self, source: str) -> SourceState:
        state = self._load_state()
        if source in state:
            return SourceState.from_dict(state[source])
        return SourceState()

    def set_state(self, source: str, state: SourceState):
        all_state = self._load_state()
        all_state[source] = state.to_dict()
        self._save_state(all_state)

    def _load_state(self) -> dict:
        if self.state_file.exists():
            return json.loads(self.state_file.read_text())
        return {}

    def _save_state(self, state: dict):
        import tempfile
        tmp_fd, tmp_path = tempfile.mkstemp(dir=self.base_path, suffix=".tmp")
        try:
            with os.fdopen(tmp_fd, "w") as f:
                json.dump(state, f, indent=2, ensure_ascii=False)
            os.replace(tmp_path, self.state_file)
        except Exception:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise

    # ── Ingest: Writing Records ──

    def append(self, source: str, data: dict) -> Record:
        """Append a record to a source JSONL file."""
        source_file = self.sources_dir / f"{source}.jsonl"
        lock_file = self.sources_dir / f"{source}.lock"

        with FileLock(lock_file):
            # O(1) line number from state instead of counting file lines
            state = self.get_state(source)
            line_num = state.total_records + 1

            record = Record(
                _line=line_num,
                _ingested_at=datetime.now(timezone.utc).isoformat(),
                _source=source,
                data=data
            )

            with open(source_file, "a") as f:
                f.write(record.to_jsonl() + "\n")

            # Update ID cache
            rec_id = data.get("id")
            if rec_id and hasattr(self, "_id_caches") and source in self._id_caches:
                self._id_caches[source].add(rec_id)

            # Update state
            state.total_records = line_num
            state.last_id = data.get("id")
            new_ts = (data.get("period_end") or data.get("modified_at") or
                      data.get("ended_at") or data.get("updated_at") or
                      data.get("date") or data.get("timestamp") or
                      data.get("ts"))
            if new_ts:
                new_norm = _normalize_ts(new_ts)
                cur_norm = _normalize_ts(state.last_ts) if state.last_ts else ""
                if not cur_norm or new_norm > cur_norm:
                    state.last_ts = new_ts
            self.set_state(source, state)

            return record

    def append_batch(self, source: str, records: list[dict]) -> int:
        count = 0
        for data in records:
            self.append(source, data)
            count += 1
        return count

    # ── Reading Records ──

    def read_all(self, source: str) -> Iterator[Record]:
        source_file = self.sources_dir / f"{source}.jsonl"
        if not source_file.exists():
            return
        with open(source_file, "r") as f:
            for line in f:
                if line.strip():
                    yield Record.from_jsonl(line)

    def read_from(self, source: str, line: int) -> Iterator[Record]:
        source_file = self.sources_dir / f"{source}.jsonl"
        if not source_file.exists():
            return
        with open(source_file, "r") as f:
            for i, raw_line in enumerate(f, 1):
                if i >= line and raw_line.strip():
                    yield Record.from_jsonl(raw_line)

    def read_range(self, source: str, start_line: int, end_line: int) -> Iterator[Record]:
        """Read records in [start_line, end_line] range (1-indexed, inclusive)."""
        source_file = self.sources_dir / f"{source}.jsonl"
        if not source_file.exists():
            return
        with open(source_file, "r") as f:
            for i, raw_line in enumerate(f, 1):
                if i > end_line:
                    break
                if i >= start_line and raw_line.strip():
                    yield Record.from_jsonl(raw_line)

    def count(self, source: str) -> int:
        state = self.get_state(source)
        return state.total_records

    # ── Consumer: backward-compat shims (real logic in consumer/reader.py) ──

    def read_new(self, source: str, consumer: str) -> Iterator[Record]:
        from .consumer.reader import ConsumerReader
        return ConsumerReader(self).read_new(source, consumer)

    def get_checkpoint(self, consumer: str) -> ConsumerCheckpoint:
        from .consumer.reader import ConsumerReader
        return ConsumerReader(self).get_checkpoint(consumer)

    def commit(self, source: str, consumer: str, line: int | None = None, record_id: str | None = None):
        from .consumer.reader import ConsumerReader
        ConsumerReader(self).commit(source, consumer, line, record_id)

    def commit_all(self, consumer: str):
        from .consumer.reader import ConsumerReader
        ConsumerReader(self).commit_all(consumer)

    # ── Utilities ──

    def sources(self) -> list[str]:
        return [f.stem for f in self.sources_dir.glob("*.jsonl")]

    def exists(self, source: str, record_id: str) -> bool:
        return record_id in self._get_id_cache(source)

    def _get_id_cache(self, source: str) -> set:
        if not hasattr(self, "_id_caches"):
            self._id_caches = {}
        if source not in self._id_caches:
            ids = set()
            source_file = self.sources_dir / f"{source}.jsonl"
            if source_file.exists():
                with open(source_file, "r") as f:
                    for line in f:
                        if line.strip():
                            try:
                                data = json.loads(line)
                                rec_data = data.get("data", data)
                                rid = rec_data.get("id")
                                if rid:
                                    ids.add(rid)
                            except json.JSONDecodeError:
                                continue
            self._id_caches[source] = ids
        return self._id_caches[source]

    def _invalidate_id_cache(self, source: str):
        if hasattr(self, "_id_caches") and source in self._id_caches:
            del self._id_caches[source]

    def stats(self) -> dict:
        result = {}
        for source in self.sources():
            state = self.get_state(source)
            result[source] = {
                "records": state.total_records,
                "last_id": state.last_id,
                "last_ts": state.last_ts
            }
        return result
