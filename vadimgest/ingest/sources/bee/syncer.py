"""Bee Syncer - sync conversations, facts, todos, and daily summaries from bee.computer wearable."""

import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Iterator

from ..base import CronSyncer
from ....store import DataStore
from ....models import SourceState
from ....config import get_source_config


class BeeSyncer(CronSyncer):
    """Bee wearable syncer - facts, conversations, todos, daily summaries."""

    source_name = "bee"
    display_name = "Bee"
    description = "Conversations, facts, todos and daily summaries from bee.computer wearable"
    category = "activity"
    dependencies = {
        "python": [],
        "cli": [],
        "credentials": [],
        "os": [],
    }
    config_schema = {
        "bee_bin": {
            "type": "path",
            "default": "bee",
            "description": "Path to the bee CLI binary",
            "advanced": True,
        },
        "sync_dir": {
            "type": "path",
            "default": "~/.local/share/vadimgest/bee-sync",
            "description": "Directory where bee sync output is stored",
            "advanced": True,
        },
        "recent_days": {
            "type": "int",
            "default": 7,
            "description": "Number of recent days to sync on each run",
        },
    }

    def __init__(self, store: DataStore, config: dict | None = None):
        config = config or get_source_config("bee")
        super().__init__(store, config)

        self.bee_bin = config.get("bee_bin") or "bee"
        self.sync_dir = Path(
            config.get("sync_dir") or Path.home() / ".local/share/vadimgest/bee-sync"
        ).expanduser()
        self.recent_days = int(config.get("recent_days") or 7)

    @classmethod
    def check_ready(cls) -> dict:
        import shutil
        # Try configured path first, then default
        try:
            cfg = get_source_config("bee")
            bee_bin = cfg.get("bee_bin") or "bee"
        except Exception:
            bee_bin = "bee"

        if shutil.which(bee_bin) is None:
            # Also check common install locations
            fallbacks = [
                "/srv/codex-klava/data/bee-npm/node_modules/.bin/bee",
                str(Path.home() / ".local/bin/bee"),
            ]
            found = next((p for p in fallbacks if shutil.which(p) or Path(p).exists()), None)
            if not found:
                return {"ok": False, "missing": [f"bee CLI not found (tried '{bee_bin}' and common paths)"]}
        return {"ok": True}

    def _run_sync(self):
        """Run bee sync to refresh local markdown export."""
        self.sync_dir.mkdir(parents=True, exist_ok=True)
        result = subprocess.run(
            [self.bee_bin, "sync", "--output", str(self.sync_dir),
             "--recent-days", str(self.recent_days)],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            self.log(f"bee sync warning: {result.stderr.strip()}")

    def fetch_new(self, state: SourceState, limit: int = 1000) -> Iterator[dict]:
        """Fetch new records from bee sync output."""
        try:
            self._run_sync()
        except FileNotFoundError:
            self.log(f"bee binary not found: {self.bee_bin}")
            return

        last_ts = None
        if state.last_ts:
            try:
                last_ts = datetime.fromisoformat(state.last_ts.replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                pass

        yielded = 0

        # Facts
        for rec in self._parse_facts():
            if yielded >= limit:
                return
            rec_ts = self._parse_ts(rec.get("timestamp"))
            if last_ts and rec_ts and rec_ts <= last_ts:
                continue
            yield rec
            yielded += 1

        # Todos
        for rec in self._parse_todos():
            if yielded >= limit:
                return
            yield rec
            yielded += 1

        # Conversations
        existing_conversations = self._existing_conversation_states()
        for rec in self._parse_conversations():
            if yielded >= limit:
                return
            rec_id = rec.get("id", "")
            completed_id = f"{rec_id}_completed"
            states = existing_conversations.get(rec_id, set())
            if (
                rec.get("state") == "COMPLETED"
                and "CAPTURING" in states
                and not self.store.exists(self.source_name, completed_id)
            ):
                rec = {**rec, "id": completed_id, "conversation_id": rec_id}
                yield rec
                yielded += 1
                continue
            if self.store.exists(self.source_name, rec_id):
                continue
            rec_ts = self._parse_ts(rec.get("start_time"))
            if last_ts and rec_ts and rec_ts <= last_ts:
                continue
            yield rec
            yielded += 1

        # Daily summaries
        for rec in self._parse_daily_summaries():
            if yielded >= limit:
                return
            rec_ts = self._parse_ts(rec.get("date"))
            if last_ts and rec_ts and rec_ts <= last_ts:
                continue
            yield rec
            yielded += 1

    def _parse_facts(self) -> Iterator[dict]:
        facts_path = self.sync_dir / "facts.md"
        if not facts_path.exists():
            return
        section = "unknown"
        for line in facts_path.read_text().splitlines():
            if line.startswith("## "):
                section = line[3:].strip().lower()
                continue
            m = re.match(r"^- (.+?) \[([^\]]*)\] \(([^,]+),\s*id (\d+)\)$", line.strip())
            if m:
                content, tags, ts, fact_id = m.groups()
                yield {
                    "id": f"bee_fact_{fact_id}",
                    "type": "bee_fact",
                    "status": section,
                    "text": content,
                    "tags": [t.strip() for t in tags.split(",") if t.strip()],
                    "timestamp": ts,
                }

    def _parse_todos(self) -> Iterator[dict]:
        todos_path = self.sync_dir / "todos.md"
        if not todos_path.exists():
            return
        for line in todos_path.read_text().splitlines():
            m = re.match(r"^- \[( |x)\] (.+?)(?:\s+\(id (\d+)\))?$", line.strip())
            if m:
                done, content, todo_id = m.groups()
                yield {
                    "id": f"bee_todo_{todo_id or abs(hash(content))}",
                    "type": "bee_todo",
                    "done": done == "x",
                    "text": content,
                }

    def _parse_conversations(self) -> Iterator[dict]:
        conv_dir = self.sync_dir / "conversations"
        if not conv_dir.exists():
            return
        for conv_file in sorted(conv_dir.glob("*/*.md")):
            rec = self._parse_conversation_file(conv_file)
            if rec:
                yield rec

    def _existing_conversation_states(self) -> dict[str, set[str]]:
        states = {}
        for record in self.store.read_all(self.source_name):
            data = record.data
            if data.get("type") != "bee_conversation":
                continue
            rec_id = data.get("conversation_id") or data.get("id")
            if not rec_id:
                continue
            states.setdefault(rec_id, set()).add(data.get("state", ""))
        return states

    def _parse_conversation_file(self, path: Path) -> dict | None:
        text = path.read_text()
        meta = {}
        transcript_lines = []
        in_transcript = False

        for line in text.splitlines():
            if line.startswith("- start_time:"):
                meta["start_time"] = line.split(":", 1)[1].strip()
            elif line.startswith("- end_time:"):
                meta["end_time"] = line.split(":", 1)[1].strip()
            elif line.startswith("- state:"):
                meta["state"] = line.split(":", 1)[1].strip()
            elif line.startswith("## Transcriptions"):
                in_transcript = True
            elif in_transcript and line.startswith("- ") and ": " in line:
                parts = line[2:].split(": ", 1)
                if len(parts) == 2:
                    transcript_lines.append(f"{parts[0]}: {parts[1]}")

        if not transcript_lines:
            return None

        transcript = "\n".join(transcript_lines)
        date = path.parent.name
        conv_id = path.stem

        return {
            "id": f"bee_conv_{conv_id}",
            "type": "bee_conversation",
            "date": date,
            "start_time": meta.get("start_time", ""),
            "end_time": meta.get("end_time", ""),
            "state": meta.get("state", ""),
            "text": transcript,
        }

    def _parse_daily_summaries(self) -> Iterator[dict]:
        daily_dir = self.sync_dir / "daily"
        if not daily_dir.exists():
            return
        for summary_path in sorted(daily_dir.glob("*/summary.md")):
            date = summary_path.parent.name
            text = summary_path.read_text().strip()
            if not text:
                continue
            yield {
                "id": f"bee_daily_{date}",
                "type": "bee_daily_summary",
                "date": date,
                "text": text,
            }

    def _parse_ts(self, ts) -> datetime | None:
        if not ts:
            return None
        try:
            return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            return None


if __name__ == "__main__":
    from ....store import DataStore
    from ....config import get_data_dir
    store = DataStore(get_data_dir())
    syncer = BeeSyncer(store)
    count, _ = syncer.sync()
    print(f"Synced {count} records")
