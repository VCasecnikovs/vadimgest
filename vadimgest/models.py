"""Data models for vadimgest ETL."""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any
import json


@dataclass
class Message:
    """Single message in a conversation."""
    ts: str  # ISO timestamp
    sender: str
    text: str


@dataclass
class Conversation:
    """Chat conversation resource."""
    id: str
    type: str = "conversation"
    chat: str = ""
    folder: str | None = None
    period_start: str = ""
    period_end: str = ""
    messages: list[dict] = field(default_factory=list)
    meta: dict = field(default_factory=dict)


@dataclass
class Document:
    """Document resource (Obsidian, etc.)."""
    id: str
    type: str = "document"
    path: str = ""
    title: str = ""
    modified_at: str = ""
    frontmatter: dict | None = None
    content: str = ""
    meta: dict = field(default_factory=dict)


@dataclass
class Meeting:
    """Meeting/call resource (Granola)."""
    id: str
    type: str = "meeting"
    title: str = ""
    started_at: str = ""
    ended_at: str = ""
    duration_minutes: int = 0
    participants: list[str] = field(default_factory=list)
    transcript: str = ""
    summary: str = ""
    meta: dict = field(default_factory=dict)


@dataclass
class Activity:
    """Activity resource (Dayflow)."""
    id: str
    type: str = "activity"
    app: str = ""
    window_title: str = ""
    started_at: str = ""
    ended_at: str = ""
    duration_seconds: int = 0
    meta: dict = field(default_factory=dict)


@dataclass
class Record:
    """Wrapper for any resource with ETL metadata."""
    _line: int
    _ingested_at: str
    _source: str
    data: dict

    def to_jsonl(self) -> str:
        """Serialize to JSONL format."""
        obj = {
            "_line": self._line,
            "_ingested_at": self._ingested_at,
            "_source": self._source,
            **self.data
        }
        return json.dumps(obj, ensure_ascii=False, default=str)

    @classmethod
    def from_jsonl(cls, line: str) -> "Record":
        """Deserialize from JSONL."""
        obj = json.loads(line)
        return cls(
            _line=obj.pop("_line"),
            _ingested_at=obj.pop("_ingested_at"),
            _source=obj.pop("_source"),
            data=obj
        )


@dataclass
class SourceState:
    """State for a single source."""
    last_id: str | None = None
    last_ts: str | None = None
    total_records: int = 0
    extra: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "SourceState":
        # Handle extra field missing from old state files
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in known})


@dataclass
class ConsumerCheckpoint:
    """Checkpoint for a consumer reading from sources."""
    consumer: str
    positions: dict[str, dict] = field(default_factory=dict)  # source -> {line, id}
    updated_at: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "ConsumerCheckpoint":
        return cls(**d)
