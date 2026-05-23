"""Consumer reader - checkpoint-based reading from JSONL sources."""

import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterator

from ..models import Record, ConsumerCheckpoint

CHAT_SOURCES = {"telegram", "signal", "whatsapp", "imessage"}


class ConsumerReader:
    """Reads new records using checkpoint tracking.

    Usage:
        store = DataStore("data/")
        reader = ConsumerReader(store)
        for record in reader.read_new("telegram", "heartbeat"):
            process(record)
        reader.commit_all("heartbeat")
    """

    def __init__(self, store):
        self.store = store
        self.checkpoints_dir = store.checkpoints_dir

    def read_new(self, source: str, consumer: str) -> Iterator[Record]:
        """Read records since consumer's last checkpoint."""
        checkpoint = self.get_checkpoint(consumer)
        pos = checkpoint.positions.get(source, {})
        last_line = pos.get("line", 0)
        yield from self.store.read_from(source, last_line + 1)

    def read_with_context(self, source: str, consumer: str, context: int = 0) -> tuple[list[Record], list[Record]]:
        """Read new records with older context from same chats.

        For chat sources, scans backward from checkpoint to find up to
        `context` older records per chat that has new messages.

        Returns (context_records, new_records).
        """
        new_records = list(self.read_new(source, consumer))
        if not context or not new_records or source not in CHAT_SOURCES:
            return [], new_records

        new_chats = {r.data.get("chat") for r in new_records if r.data.get("chat")}
        if not new_chats:
            return [], new_records

        checkpoint = self.get_checkpoint(consumer)
        pos = checkpoint.positions.get(source, {})
        last_line = pos.get("line", 0)
        if last_line == 0:
            return [], new_records

        scan_start = max(1, last_line - context * 20)
        older_records = list(self.store.read_range(source, scan_start, last_line))

        per_chat: dict[str, list[Record]] = defaultdict(list)
        for r in older_records:
            chat = r.data.get("chat")
            if chat and chat in new_chats:
                per_chat[chat].append(r)

        context_records = []
        for records in per_chat.values():
            context_records.extend(records[-context:])

        return context_records, new_records

    def get_checkpoint(self, consumer: str) -> ConsumerCheckpoint:
        """Load checkpoint for a consumer."""
        checkpoint_file = self.checkpoints_dir / f"{consumer}.json"
        if checkpoint_file.exists():
            data = json.loads(checkpoint_file.read_text())
            return ConsumerCheckpoint.from_dict(data)
        return ConsumerCheckpoint(consumer=consumer)

    def _file_line_count(self, source: str) -> int:
        """Count actual lines in JSONL file, bypassing stale SourceState cache."""
        source_file = self.store.sources_dir / f"{source}.jsonl"
        if not source_file.exists():
            return 0
        count = 0
        with open(source_file, "r") as f:
            for line in f:
                if line.strip():
                    count += 1
        return count

    def commit(self, source: str, consumer: str, line: int | None = None, record_id: str | None = None):
        """Advance consumer's checkpoint for a source."""
        checkpoint = self.get_checkpoint(consumer)
        if line is None:
            # Use actual file line count, not stale SourceState.total_records.
            # Daemon-written sources (telegram) can add records without updating
            # SourceState, causing the checkpoint to lag behind the real file end.
            line = self._file_line_count(source)
        checkpoint.positions[source] = {"line": line, "id": record_id}
        checkpoint.updated_at = datetime.now(timezone.utc).isoformat()
        checkpoint_file = self.checkpoints_dir / f"{consumer}.json"
        checkpoint_file.write_text(json.dumps(checkpoint.to_dict(), indent=2, ensure_ascii=False))

    def commit_all(self, consumer: str):
        """Advance consumer's checkpoint to end of all sources."""
        for source_file in self.store.sources_dir.glob("*.jsonl"):
            self.commit(source_file.stem, consumer)
