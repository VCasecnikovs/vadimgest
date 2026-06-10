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

        # Parse meaningful transcript facts without storing hidden reasoning or
        # huge tool output. Keep the record session-shaped for existing readers.
        messages = []
        tool_calls = []
        errors = []
        first_line = None
        assistant_count = 0
        try:
            with open(session_file) as f:
                for line_no, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                        if first_line is None:
                            first_line = line_no
                        parsed = self._parse_message_line(msg, line_no)
                        if parsed.get("message"):
                            messages.append(parsed["message"])
                            if parsed["message"]["role"] == "assistant":
                                assistant_count += 1
                        tool_calls.extend(parsed.get("tool_calls", []))
                        errors.extend(parsed.get("errors", []))

                    except json.JSONDecodeError:
                        pass
        except Exception as e:
            self.log(f"Error reading session {session_id}: {e}")
            return None

        if not any(m.get("role") == "user" for m in messages):
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
            "source_uri": f"file://{session_file}#L{first_line or 1}",
            "messages": messages,
            "tool_calls": tool_calls,
            "errors": errors,
            "meta": {
                "session_id": session_id,
                "message_count": len(messages),
                "assistant_message_count": assistant_count,
                "tool_call_count": len(tool_calls),
                "error_count": len(errors),
                "original_message_count": entry.get("messageCount", 0),
                "is_sidechain": entry.get("isSidechain", False),
                "source_file": str(session_file),
            },
        }

    def _parse_message_line(self, msg: dict, line_no: int) -> dict:
        """Extract user/assistant text and compact tool metadata from one JSONL row."""
        row_type = msg.get("type")
        message = msg.get("message") if isinstance(msg.get("message"), dict) else {}
        content = message.get("content", "")
        parsed = {"message": None, "tool_calls": [], "errors": []}

        if row_type == "user":
            text, tool_errors = self._content_to_text(content, include_tool_result_errors=True)
            if text.strip():
                parsed["message"] = {
                    "role": "user",
                    "content": text[:5000],
                    "line": line_no,
                }
            parsed["errors"].extend(tool_errors)
            return parsed

        if row_type == "assistant":
            text, tool_calls = self._assistant_content(content, line_no)
            if text.strip():
                parsed["message"] = {
                    "role": "assistant",
                    "content": text[:5000],
                    "line": line_no,
                }
            parsed["tool_calls"].extend(tool_calls)
            return parsed

        if row_type in {"result", "error"}:
            err = msg.get("error") or msg.get("message") or msg.get("result")
            if err:
                parsed["errors"].append(str(err)[:1000])
            return parsed

        return parsed

    def _content_to_text(self, content, include_tool_result_errors: bool = False) -> tuple[str, list[str]]:
        errors = []
        if isinstance(content, str):
            return content, errors
        if not isinstance(content, list):
            return "", errors
        texts = []
        for block in content:
            if isinstance(block, str):
                texts.append(block)
            elif isinstance(block, dict):
                btype = block.get("type")
                if btype == "text":
                    texts.append(block.get("text", ""))
                elif include_tool_result_errors and btype == "tool_result" and block.get("is_error"):
                    tool_text, _ = self._content_to_text(block.get("content", ""))
                    if tool_text.strip():
                        errors.append(tool_text[:1000])
        return "\n".join(t for t in texts if t), errors

    def _assistant_content(self, content, line_no: int) -> tuple[str, list[dict]]:
        if isinstance(content, str):
            return content, []
        if not isinstance(content, list):
            return "", []
        texts = []
        tools = []
        for block in content:
            if isinstance(block, str):
                texts.append(block)
            elif isinstance(block, dict):
                btype = block.get("type")
                if btype == "text":
                    texts.append(block.get("text", ""))
                elif btype == "tool_use":
                    tools.append({
                        "name": block.get("name") or "tool",
                        "id": block.get("id"),
                        "line": line_no,
                        "input_summary": str(block.get("input") or "")[:500],
                    })
        return "\n".join(t for t in texts if t), tools

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
