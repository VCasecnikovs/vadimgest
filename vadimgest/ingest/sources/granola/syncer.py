"""Granola Syncer - sync meeting notes from Granola app cache."""

import json
from datetime import datetime
from pathlib import Path
from typing import Iterator

from ..base import CronSyncer
from ....store import DataStore
from ....models import SourceState
from ....config import get_source_config


class GranolaSyncer(CronSyncer):
    """Meeting notes syncer - reads from Granola app cache."""

    source_name = "granola"
    display_name = "Granola"
    description = "Meeting notes and transcripts from Granola app"
    category = "meetings"
    dependencies = {
        "python": [],
        "cli": [],
        "credentials": [],
        "os": ["macos"],
    }
    config_schema = {
        "cache_path": {"type": "path", "default": "~/Library/Application Support/Granola/cache-v3.json", "description": "Path to Granola cache file", "advanced": True, "auto_detected": True},
    }

    def __init__(self, store: DataStore, config: dict | None = None):
        config = config or get_source_config("granola")
        super().__init__(store, config)

        self.cache_path = Path(
            config.get("cache_path")
            or Path.home() / "Library/Application Support/Granola/cache-v3.json"
        )

    def _load_cache(self) -> dict:
        """Load and parse Granola cache (nested JSON structure)."""
        if not self.cache_path.exists():
            raise FileNotFoundError(f"Granola cache not found: {self.cache_path}")

        with open(self.cache_path, encoding="utf-8") as f:
            data = json.load(f)

        # Cache has nested JSON string: {"cache": "<stringified JSON>"}
        cache_str = data.get("cache", "{}")
        return json.loads(cache_str)

    def fetch_new(self, state: SourceState, limit: int = 1000) -> Iterator[dict]:
        """Fetch new meetings from Granola cache AND Hlopya."""
        yielded = 0

        # Source 1: Granola app cache
        try:
            cache = self._load_cache()
            state_data = cache.get("state", {})
            documents = state_data.get("documents", {})
            transcripts = state_data.get("transcripts", {})
            self.log(f"Granola: {len(documents)} documents in cache")

            last_ts = None
            if state.last_ts:
                last_ts = datetime.fromisoformat(state.last_ts.replace("Z", "+00:00")).replace(tzinfo=None)

            sorted_docs = []
            for doc_id, doc in documents.items():
                updated_at = self._parse_ts(doc.get("updated_at") or doc.get("created_at"))
                sorted_docs.append((doc_id, doc, updated_at))

            sorted_docs.sort(key=lambda x: x[2] or datetime.min)

            for doc_id, doc, updated_at in sorted_docs:
                if yielded >= limit:
                    break
                if last_ts and updated_at and updated_at <= last_ts:
                    continue
                if doc.get("deleted_at"):
                    continue

                transcript_segments = transcripts.get(doc_id, [])
                record = self._doc_to_record(doc_id, doc, transcript_segments)
                if record:
                    yield record
                    yielded += 1

        except Exception as e:
            self.log(f"Granola cache: {e}")

    def _doc_to_record(self, doc_id: str, doc: dict, transcript_segments: list) -> dict | None:
        """Convert document to record."""
        # Get notes (prefer markdown)
        notes = doc.get("notes_markdown") or doc.get("notes_plain") or ""

        # Skip empty documents without transcripts
        if not notes.strip() and not transcript_segments:
            return None

        created_at = self._parse_ts(doc.get("created_at"))
        updated_at = self._parse_ts(doc.get("updated_at"))

        # Parse participants from people dict
        participants = []
        people_data = doc.get("people", {})
        if isinstance(people_data, dict):
            # Attendees is usually a list
            attendees = people_data.get("attendees", [])
            if isinstance(attendees, list):
                for att in attendees:
                    if isinstance(att, str):
                        participants.append(att)
                    elif isinstance(att, dict):
                        name = att.get("name") or att.get("email") or "Unknown"
                        participants.append(name)
            # Also add creator
            creator = people_data.get("creator")
            if creator and creator not in participants:
                participants.append(creator)

        # Parse calendar event for duration (if available in doc)
        duration_minutes = 0
        cal_event = doc.get("google_calendar_event")
        if cal_event and isinstance(cal_event, dict):
            start = self._parse_ts(cal_event.get("start"))
            end = self._parse_ts(cal_event.get("end"))
            if start and end:
                duration_minutes = int((end - start).total_seconds() / 60)

        # Build transcript text from segments
        transcript_text = ""
        if isinstance(transcript_segments, list) and transcript_segments:
            lines = []
            for seg in transcript_segments:
                if isinstance(seg, dict):
                    text = seg.get("text", "").strip()
                    if text:
                        lines.append(text)
            transcript_text = "\n".join(lines)

        return {
            "id": f"meeting_{doc_id}",
            "type": "meeting",
            "title": doc.get("title") or "Untitled Meeting",
            "created_at": created_at.isoformat() if created_at else None,
            "updated_at": updated_at.isoformat() if updated_at else None,
            "duration_minutes": duration_minutes,
            "participants": participants,
            "notes": notes,
            "transcript": transcript_text if transcript_text else None,
            "meta": {
                "source_id": doc_id,
                "has_transcript": bool(transcript_text),
                "segment_count": len(transcript_segments) if isinstance(transcript_segments, list) else 0,
                "valid_meeting": doc.get("valid_meeting", False),
            },
        }

    def _parse_ts(self, ts) -> datetime | None:
        """Parse timestamp (always returns naive UTC datetime)."""
        if not ts:
            return None
        if isinstance(ts, datetime):
            # Strip timezone to keep everything naive
            return ts.replace(tzinfo=None)
        try:
            dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            return dt.replace(tzinfo=None)
        except Exception:
            return None


if __name__ == "__main__":
    from ...store import DataStore
    from ...config import get_data_dir as DATA_DIR_fn
    DATA_DIR = DATA_DIR_fn()

    store = DataStore(DATA_DIR)
    syncer = GranolaSyncer(store)
    count, _ = syncer.sync()
    print(f"Synced {count} records")
