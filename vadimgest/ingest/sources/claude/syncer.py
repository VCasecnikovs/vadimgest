"""Claude Syncer - sync Claude Code sessions."""

import json
from datetime import datetime
from pathlib import Path
from typing import Iterator

from ..base import CronSyncer
from ....store import DataStore
from ....models import SourceState
from ....config import get_source_config


class ClaudeSyncer(CronSyncer):
    """Claude Code sessions syncer."""

    source_name = "claude"
    display_name = "Claude Sessions"
    description = "Claude Code session transcripts and summaries"
    category = "activity"
    dependencies = {
        "python": [],
        "cli": [],
        "credentials": [],
        "os": [],
    }
    config_schema = {
        "projects_dir": {"type": "path", "default": "~/.claude/projects", "description": "Path to Claude projects directory", "advanced": True, "auto_detected": True},
    }

    def __init__(self, store: DataStore, config: dict | None = None):
        config = config or get_source_config("claude")
        super().__init__(store, config)

        self.projects_dir = Path(
            config.get("projects_dir")
            or Path.home() / ".claude/projects"
        )

    def fetch_new(self, state: SourceState, limit: int = 1000) -> Iterator[dict]:
        """Fetch new/modified sessions from Claude projects."""
        if not self.projects_dir.exists():
            self.log(f"Claude projects dir not found: {self.projects_dir}")
            return

        last_ts = None
        if state.last_ts:
            last_ts = datetime.fromisoformat(state.last_ts.replace("Z", "+00:00"))

        # Find all sessions-index.json files
        sessions_found = []

        for index_file in self.projects_dir.rglob("sessions-index.json"):
            try:
                index_data = json.loads(index_file.read_text())
                entries = index_data.get("entries", [])
                project_path = index_data.get("originalPath", "")

                for entry in entries:
                    modified = self._parse_ts(entry.get("modified"))
                    if last_ts and modified and modified <= last_ts:
                        continue

                    sessions_found.append({
                        "entry": entry,
                        "project_path": project_path,
                        "index_dir": index_file.parent,
                        "modified": modified,
                    })
            except Exception as e:
                self.log(f"Error reading {index_file}: {e}")

        # Sort by modified time
        sessions_found.sort(key=lambda x: x["modified"] or datetime.min)
        self.log(f"Found {len(sessions_found)} modified sessions")

        yielded = 0
        for session_info in sessions_found:
            if yielded >= limit:
                break

            record = self._session_to_record(session_info)
            if record:
                yield record
                yielded += 1

    def _session_to_record(self, session_info: dict) -> dict | None:
        """Convert session to record."""
        entry = session_info["entry"]
        index_dir = session_info["index_dir"]

        session_id = entry.get("sessionId")
        if not session_id:
            return None

        # Read session JSONL
        session_file = index_dir / f"{session_id}.jsonl"
        if not session_file.exists():
            # Try fullPath
            full_path = entry.get("fullPath")
            if full_path:
                session_file = Path(full_path)

        if not session_file.exists():
            return None

        # Parse messages from JSONL (user messages only - skip assistant/tool noise)
        messages = []
        try:
            with open(session_file) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                        if msg.get("type") != "user":
                            continue

                        content = msg.get("message", {}).get("content", "")
                        if isinstance(content, list):
                            texts = []
                            for block in content:
                                if isinstance(block, dict) and block.get("type") == "text":
                                    texts.append(block.get("text", ""))
                                elif isinstance(block, str):
                                    texts.append(block)
                            content = "\n".join(texts)

                        # Skip empty (tool results have no text blocks)
                        if not content.strip():
                            continue

                        messages.append({
                            "role": "user",
                            "content": content[:5000],
                        })

                    except json.JSONDecodeError:
                        pass
        except Exception as e:
            self.log(f"Error reading session {session_id}: {e}")
            return None

        if not messages:
            return None

        # Extract first prompt as title
        first_prompt = entry.get("firstPrompt", "")
        title = first_prompt[:100] if first_prompt else "Untitled Session"

        created_at = self._parse_ts(entry.get("created"))
        modified_at = self._parse_ts(entry.get("modified"))

        return {
            "id": f"session_{session_id}",
            "type": "session",
            "title": title,
            "created_at": created_at.isoformat() if created_at else None,
            "modified_at": modified_at.isoformat() if modified_at else None,
            "project_path": session_info["project_path"],
            "git_branch": entry.get("gitBranch"),
            "messages": messages,
            "meta": {
                "session_id": session_id,
                "message_count": len(messages),
                "original_message_count": entry.get("messageCount", 0),
                "is_sidechain": entry.get("isSidechain", False),
            },
        }

    def _parse_ts(self, ts) -> datetime | None:
        """Parse timestamp."""
        if not ts:
            return None
        if isinstance(ts, datetime):
            return ts
        try:
            return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            return None


if __name__ == "__main__":
    from ...store import DataStore
    from ...config import get_data_dir as DATA_DIR_fn
    DATA_DIR = DATA_DIR_fn()

    store = DataStore(DATA_DIR)
    syncer = ClaudeSyncer(store)
    count, _ = syncer.sync()
    print(f"Synced {count} records")
