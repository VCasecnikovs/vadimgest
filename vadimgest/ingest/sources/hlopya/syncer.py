"""Hlopya Syncer - sync meeting recordings from ~/recordings/."""

import json
from datetime import datetime
from pathlib import Path
from typing import Iterator

from ..base import CronSyncer
from ....store import DataStore
from ....models import SourceState
from ....config import get_source_config


class HlopyaSyncer(CronSyncer):
    """Meeting recorder syncer - reads directly from ~/recordings/{session_id}/."""

    source_name = "hlopya"
    display_name = "Hlopya"
    description = "Meeting recordings and transcripts from Hlopya app"
    category = "meetings"
    dependencies = {
        "python": [],
        "cli": [],
        "credentials": [],
        "os": ["macos"],
    }
    config_schema = {
        "recordings_dir": {"type": "path", "default": "~/recordings", "description": "Path to recordings directory"},
    }

    def __init__(self, store: DataStore, config: dict | None = None):
        config = config or get_source_config("hlopya")
        super().__init__(store, config)
        recordings_cfg = config.get("recordings_dir")
        if recordings_cfg:
            self.recordings_dir = Path(recordings_cfg).expanduser()
        else:
            self.recordings_dir = Path.home() / "recordings"

    def fetch_new(self, state: SourceState, limit: int = 1000) -> Iterator[dict]:
        if not self.recordings_dir.exists():
            self.log(f"Recordings dir not found: {self.recordings_dir}")
            return

        # Collect sessions sorted by id (chronological)
        session_dirs = sorted(
            [d for d in self.recordings_dir.iterdir() if d.is_dir() and not d.name.startswith(".")],
            key=lambda d: d.name,
        )
        self.log(f"Found {len(session_dirs)} session dirs")

        yielded = 0
        for session_dir in session_dirs:
            if yielded >= limit:
                break

            session_id = session_dir.name

            # Dedup handled by store.exists() in base sync()

            meta = self._read_json(session_dir / "meta.json")
            if not meta:
                continue

            # Only ingest completed sessions
            if meta.get("status") != "done":
                continue

            record = self._build_record(session_id, session_dir, meta)
            if record:
                yield record
                yielded += 1

    def _build_record(self, session_id: str, session_dir: Path, meta: dict) -> dict | None:
        notes = self._read_json(session_dir / "notes.json") or {}
        transcript = self._read_json(session_dir / "transcript.json")
        personal_notes = self._read_text(session_dir / "personal_notes.md")

        # Build markdown content from notes
        md_parts = []
        if notes.get("summary"):
            md_parts.append(f"## Summary\n\n{notes['summary']}")
        if notes.get("enriched_notes"):
            md_parts.append(f"## Meeting Notes\n\n{notes['enriched_notes']}")
        if notes.get("topics"):
            topics_md = "\n".join(
                f"### {t.get('topic', '?')}\n{t.get('details', '')}"
                for t in notes["topics"]
            )
            md_parts.append(f"## Topics\n\n{topics_md}")
        if notes.get("action_items"):
            items_md = "\n".join(
                f"- [ ] **{item.get('owner', '?')}**: {item.get('task', '')}"
                + (f" (due: {item['deadline']})" if item.get("deadline") else "")
                for item in notes["action_items"]
            )
            md_parts.append(f"## Action Items\n\n{items_md}")
        if notes.get("decisions"):
            decisions_md = "\n".join(f"- {d}" for d in notes["decisions"])
            md_parts.append(f"## Decisions\n\n{decisions_md}")
        if notes.get("insights"):
            insights_md = "\n".join(f"- {i}" for i in notes["insights"])
            md_parts.append(f"## Insights\n\n{insights_md}")
        if notes.get("follow_ups"):
            fups_md = "\n".join(f"- {f}" for f in notes["follow_ups"])
            md_parts.append(f"## Follow-ups\n\n{fups_md}")
        if personal_notes:
            md_parts.append(f"## Personal Notes\n\n{personal_notes}")

        # Transcript text
        transcript_text = ""
        if transcript:
            transcript_text = transcript.get("full_text") or transcript.get("fullText", "")

        # Parse date from session_id (YYYY-MM-DD_HH-MM-SS)
        created_at = None
        try:
            created_at = datetime.strptime(session_id, "%Y-%m-%d_%H-%M-%S").isoformat()
        except ValueError:
            pass

        return {
            "id": f"hlopya_{session_id}",
            "type": "meeting",
            "title": meta.get("title") or notes.get("title") or session_id,
            "created_at": created_at,
            "updated_at": created_at,
            "duration_minutes": round(meta.get("duration", 0) / 60) if meta.get("duration") else 0,
            "participants": meta.get("participants") or notes.get("participants") or [],
            "participant_names": meta.get("participant_names", {}),
            "notes": "\n\n".join(md_parts) if md_parts else "",
            "transcript": transcript_text or None,
            "meta": {
                "source_id": session_id,
                "has_transcript": bool(transcript_text),
                "has_notes": bool(notes),
                "has_personal_notes": bool(personal_notes),
                "model_used": notes.get("model_used"),
                "recorder": "hlopya",
            },
        }

    def _read_json(self, path: Path) -> dict | None:
        if not path.exists():
            return None
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def _read_text(self, path: Path) -> str | None:
        if not path.exists():
            return None
        try:
            return path.read_text(encoding="utf-8").strip() or None
        except Exception:
            return None
