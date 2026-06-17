"""Codex Syncer - sync Codex local session turns as sourced records."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from ..base import CronSyncer
from ....config import get_source_config
from ....models import SourceState
from ....store import DataStore


class CodexSyncer(CronSyncer):
    """Codex Desktop/CLI sessions syncer."""

    source_name = "codex"
    display_name = "Codex Sessions"
    description = "Codex session turns with prompts, final messages, tool summaries, and metadata"
    category = "dev"
    dependencies = {
        "python": [],
        "cli": [],
        "credentials": [],
        "os": [],
    }
    config_schema = {
        "codex_dir": {"type": "path", "default": "~/.codex", "description": "Path to Codex home directory", "advanced": True, "auto_detected": True},
        "include_archived": {"type": "bool", "default": True, "description": "Include archived_sessions JSONL transcripts", "advanced": True},
        "include_sqlite_metadata": {"type": "bool", "default": True, "description": "Read thread and goal metadata from Codex SQLite state", "advanced": True},
        "compress_long_messages": {"type": "bool", "default": False, "description": "Compress long user/assistant messages before final truncation", "advanced": True},
        "compression_min_chars": {"type": "int", "default": 12000, "description": "Minimum turn text size before compression is attempted", "advanced": True},
        "max_user_chars": {"type": "int", "default": 8000, "description": "Maximum characters stored per user message", "advanced": True},
        "max_assistant_chars": {"type": "int", "default": 8000, "description": "Maximum characters stored per assistant message", "advanced": True},
        "max_tool_output_chars": {"type": "int", "default": 1200, "description": "Maximum characters stored per tool output summary", "advanced": True},
    }

    def __init__(self, store: DataStore, config: dict | None = None):
        config = config or get_source_config("codex")
        super().__init__(store, config)

        self.codex_dir = Path(config.get("codex_dir") or Path.home() / ".codex").expanduser()
        self.include_archived = bool(config.get("include_archived", True))
        self.include_sqlite_metadata = bool(config.get("include_sqlite_metadata", True))
        self.compress_long_messages = bool(config.get("compress_long_messages", False))
        self.compression_min_chars = int(config.get("compression_min_chars") or 12000)
        self.max_user_chars = int(config.get("max_user_chars") or 8000)
        self.max_assistant_chars = int(config.get("max_assistant_chars") or 8000)
        self.max_tool_output_chars = int(config.get("max_tool_output_chars") or 1200)

    def fetch_new(self, state: SourceState, limit: int = 1000) -> Iterator[dict]:
        """Fetch new/modified Codex turns from local JSONL transcripts."""
        if not self.codex_dir.exists():
            self.log(f"Codex dir not found: {self.codex_dir}")
            return

        last_ts = self._parse_ts(state.last_ts)
        metadata = self._load_metadata() if self.include_sqlite_metadata else {}
        files = self._session_files()
        seen_files = self._seen_session_filenames() if last_ts else set()
        yielded = 0

        for session_file in files:
            if yielded >= limit:
                break
            modified = datetime.fromtimestamp(session_file.stat().st_mtime, tz=timezone.utc)
            if last_ts and modified <= last_ts and session_file.name in seen_files:
                continue
            for record in self._records_from_session_file(session_file, metadata):
                if yielded >= limit:
                    break
                yield record
                yielded += 1

    def _session_files(self) -> list[Path]:
        files: list[Path] = []
        sessions_dir = self.codex_dir / "sessions"
        if sessions_dir.exists():
            files.extend(
                p for p in sessions_dir.rglob("*.jsonl")
                if "index" not in p.relative_to(sessions_dir).parts
            )
        if self.include_archived:
            archived = self.codex_dir / "archived_sessions"
            if archived.exists():
                files.extend(archived.glob("*.jsonl"))
        return sorted(set(files), key=lambda p: p.stat().st_mtime)

    def _seen_session_filenames(self) -> set[str]:
        """Return transcript filenames already represented in the raw codex store."""
        names: set[str] = set()
        source_file = self.store.sources_dir / f"{self.source_name}.jsonl"
        if not source_file.exists():
            return names
        for _, row in self._iter_jsonl(source_file):
            rec = row.get("data") if isinstance(row.get("data"), dict) else row
            source_uri = str(rec.get("source_uri") or "")
            if not source_uri.startswith("file://"):
                continue
            path_part = source_uri[7:].split("#", 1)[0]
            if path_part:
                names.add(Path(path_part).name)
        return names

    def _records_from_session_file(self, path: Path, metadata: dict[str, Any] | None = None) -> list[dict]:
        metadata = metadata or {}
        session_meta: dict[str, Any] = {}
        current: dict[str, Any] | None = None
        records: list[dict] = []
        tool_calls_by_id: dict[str, dict[str, Any]] = {}

        def ensure_turn(turn_id: str | None, line_no: int) -> dict[str, Any]:
            nonlocal current, tool_calls_by_id
            if current and (not turn_id or current.get("turn_id") == turn_id):
                return current
            if current:
                record = self._finalize_turn(path, current, session_meta, metadata)
                if record:
                    records.append(record)
            current = {
                "turn_id": turn_id or f"line_{line_no}",
                "start_line": line_no,
                "end_line": line_no,
                "user_messages": [],
                "assistant_messages": [],
                "tool_calls": [],
                "errors": [],
                "token_usage": {},
                "context": {},
            }
            tool_calls_by_id = {}
            return current

        for line_no, raw in self._iter_jsonl(path):
            top_type = raw.get("type")
            payload = raw.get("payload") if isinstance(raw.get("payload"), dict) else raw

            if not top_type and raw.get("id") and raw.get("timestamp"):
                session_meta.update(self._session_meta(raw))
                continue

            if raw.get("record_type") == "state":
                continue

            if top_type == "session_meta":
                session_meta.update(self._session_meta(payload))
                continue

            if top_type == "turn_context":
                turn = ensure_turn(str(payload.get("turn_id") or ""), line_no)
                turn["end_line"] = line_no
                turn["context"].update({
                    "cwd": payload.get("cwd"),
                    "model": payload.get("model"),
                    "approval_policy": payload.get("approval_policy"),
                    "sandbox_policy": payload.get("sandbox_policy"),
                    "git_branch": payload.get("git", {}).get("branch") if isinstance(payload.get("git"), dict) else None,
                })
                continue

            ptype = payload.get("type")
            if top_type == "event_msg" and ptype == "task_started":
                turn = ensure_turn(str(payload.get("turn_id") or ""), line_no)
                turn["started_at"] = self._epoch_to_iso(payload.get("started_at"))
                turn["context"]["model_context_window"] = payload.get("model_context_window")
                continue

            if top_type in {"message", "function_call", "function_call_output", "reasoning"}:
                if top_type == "reasoning":
                    continue
                turn = ensure_turn(str(payload.get("turn_id") or "") if payload.get("turn_id") else None, line_no)
                turn["end_line"] = line_no
                self._consume_response_item(turn, payload, tool_calls_by_id)
                continue

            turn = ensure_turn(str(payload.get("turn_id") or "") if payload.get("turn_id") else None, line_no)
            turn["end_line"] = line_no

            if top_type == "event_msg":
                self._consume_event_msg(turn, payload)
            elif top_type == "response_item":
                self._consume_response_item(turn, payload, tool_calls_by_id)

        if current:
            record = self._finalize_turn(path, current, session_meta, metadata)
            if record:
                records.append(record)
        return records

    def _consume_event_msg(self, turn: dict[str, Any], payload: dict[str, Any]) -> None:
        ptype = payload.get("type")
        if ptype == "user_message":
            text = str(payload.get("message") or "").strip()
            if text:
                turn["user_messages"].append({
                    "text": text,
                    "line": turn["end_line"],
                    "images": len(payload.get("images") or []) + len(payload.get("local_images") or []),
                })
        elif ptype == "agent_message":
            text = str(payload.get("message") or "").strip()
            if text:
                turn["assistant_messages"].append({
                    "text": text,
                    "phase": payload.get("phase"),
                    "line": turn["end_line"],
                })
        elif ptype == "token_count":
            info = payload.get("info")
            if isinstance(info, dict):
                turn["token_usage"].update(info)
        elif ptype == "task_complete":
            text = str(payload.get("last_agent_message") or "").strip()
            if text:
                turn["assistant_messages"].append({
                    "text": text,
                    "phase": "final",
                    "line": turn["end_line"],
                })

    def _consume_response_item(
        self,
        turn: dict[str, Any],
        payload: dict[str, Any],
        tool_calls_by_id: dict[str, dict[str, Any]],
    ) -> None:
        ptype = payload.get("type")
        if ptype == "reasoning":
            return
        if ptype == "message":
            role = payload.get("role")
            if role == "developer":
                return
            text = self._content_text(payload.get("content")).strip()
            if not text:
                return
            if role == "assistant":
                turn["assistant_messages"].append({
                    "text": text,
                    "phase": payload.get("phase"),
                    "line": turn["end_line"],
                })
            elif role == "user" and not self._looks_like_codex_context(text):
                turn["user_messages"].append({
                    "text": text,
                    "line": turn["end_line"],
                    "images": 0,
                })
        elif ptype == "function_call":
            tool = {
                "name": payload.get("name") or "tool",
                "call_id": payload.get("call_id"),
                "arguments_summary": self._truncate(str(payload.get("arguments") or ""), 500),
                "line": turn["end_line"],
            }
            turn["tool_calls"].append(tool)
            if tool.get("call_id"):
                tool_calls_by_id[str(tool["call_id"])] = tool
        elif ptype == "function_call_output":
            call_id = str(payload.get("call_id") or "")
            output = str(payload.get("output") or "")
            summary = self._truncate(output, self.max_tool_output_chars)
            target = tool_calls_by_id.get(call_id)
            if target is not None:
                target["output_summary"] = summary
            elif call_id:
                turn["tool_calls"].append({
                    "name": "tool_output",
                    "call_id": call_id,
                    "output_summary": summary,
                    "line": turn["end_line"],
                })
            if self._looks_like_error(output):
                turn["errors"].append(summary)

    def _finalize_turn(
        self,
        path: Path,
        turn: dict[str, Any],
        session_meta: dict[str, Any],
        metadata: dict[str, Any],
    ) -> dict | None:
        if not (turn["user_messages"] or turn["assistant_messages"] or turn["tool_calls"] or turn["errors"]):
            return None

        compression_meta = self._compress_turn_messages(turn)
        self._truncate_turn_messages(turn)

        session_id = session_meta.get("session_id") or path.stem
        thread_meta = metadata.get("threads", {}).get(session_id, {})
        turn_id = turn.get("turn_id") or f"line_{turn['start_line']}"
        created_at = self._coerce_ts(
            turn.get("started_at") or session_meta.get("created_at") or thread_meta.get("created_at")
        )
        updated_at = self._coerce_ts(thread_meta.get("updated_at")) or self._mtime_iso(path)
        title = self._title(turn, session_meta, thread_meta)
        cwd = turn["context"].get("cwd") or session_meta.get("cwd") or thread_meta.get("cwd")
        git_branch = turn["context"].get("git_branch") or thread_meta.get("git_branch")
        model = turn["context"].get("model") or thread_meta.get("model")

        meta = {
            "source_file": str(path),
            "start_line": turn["start_line"],
            "end_line": turn["end_line"],
            "originator": session_meta.get("originator"),
            "cli_version": session_meta.get("cli_version"),
            "model_provider": session_meta.get("model_provider") or thread_meta.get("model_provider"),
            "source": session_meta.get("source") or thread_meta.get("source"),
            "thread_source": session_meta.get("thread_source") or thread_meta.get("thread_source"),
        }
        if compression_meta:
            meta["compression"] = compression_meta

        return {
            "id": f"codex_{session_id}_{turn_id}_{turn['start_line']}",
            "type": "agent_turn",
            "platform": "codex",
            "title": title,
            "created_at": created_at,
            "updated_at": updated_at,
            "source_uri": f"file://{path}#L{turn['start_line']}",
            "session_id": session_id,
            "thread_id": session_id,
            "turn_id": turn_id,
            "cwd": cwd,
            "git_branch": git_branch,
            "model": model,
            "user_messages": turn["user_messages"],
            "assistant_messages": self._dedupe_messages(turn["assistant_messages"]),
            "tool_calls": turn["tool_calls"],
            "errors": turn["errors"],
            "token_usage": turn["token_usage"],
            "parent_thread_ids": metadata.get("parents", {}).get(session_id, []),
            "child_thread_ids": metadata.get("children", {}).get(session_id, []),
            "goals": metadata.get("goals", {}).get(session_id, []),
            "meta": meta,
        }

    def _load_metadata(self) -> dict[str, Any]:
        metadata = {"threads": {}, "parents": {}, "children": {}, "goals": {}}
        self._load_state_metadata(metadata)
        self._load_goal_metadata(metadata)
        self._load_session_index(metadata)
        return metadata

    def _load_state_metadata(self, metadata: dict[str, Any]) -> None:
        db = self.codex_dir / "state_5.sqlite"
        if not db.exists():
            return
        try:
            con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
            con.row_factory = sqlite3.Row
            for row in con.execute(
                "select id, created_at, updated_at, source, model_provider, cwd, title, "
                "git_branch, model, thread_source, preview from threads"
            ):
                metadata["threads"][row["id"]] = dict(row)
            for row in con.execute("select parent_thread_id, child_thread_id from thread_spawn_edges"):
                metadata["children"].setdefault(row["parent_thread_id"], []).append(row["child_thread_id"])
                metadata["parents"].setdefault(row["child_thread_id"], []).append(row["parent_thread_id"])
            con.close()
        except sqlite3.Error as e:
            self.log(f"Could not read Codex state sqlite metadata: {e}")

    def _load_goal_metadata(self, metadata: dict[str, Any]) -> None:
        db = self.codex_dir / "goals_1.sqlite"
        if not db.exists():
            return
        try:
            con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
            con.row_factory = sqlite3.Row
            for row in con.execute(
                "select thread_id, goal_id, objective, status, token_budget, tokens_used from thread_goals"
            ):
                metadata["goals"].setdefault(row["thread_id"], []).append({
                    "goal_id": row["goal_id"],
                    "objective": row["objective"],
                    "status": row["status"],
                    "token_budget": row["token_budget"],
                    "tokens_used": row["tokens_used"],
                })
            con.close()
        except sqlite3.Error as e:
            self.log(f"Could not read Codex goals sqlite metadata: {e}")

    def _load_session_index(self, metadata: dict[str, Any]) -> None:
        index = self.codex_dir / "session_index.jsonl"
        if not index.exists():
            return
        for _, row in self._iter_jsonl(index):
            session_id = row.get("id") or row.get("session_id") or row.get("thread_id")
            if not session_id:
                continue
            entry = metadata["threads"].setdefault(str(session_id), {})
            for key in ("title", "preview", "cwd", "model", "git_branch", "updated_at", "created_at"):
                if row.get(key) and not entry.get(key):
                    entry[key] = row[key]

    def _session_meta(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "session_id": payload.get("id"),
            "created_at": payload.get("timestamp"),
            "cwd": payload.get("cwd"),
            "originator": payload.get("originator"),
            "cli_version": payload.get("cli_version"),
            "source": payload.get("source"),
            "thread_source": payload.get("thread_source"),
            "model_provider": payload.get("model_provider"),
        }

    def _iter_jsonl(self, path: Path) -> Iterator[tuple[int, dict[str, Any]]]:
        try:
            with open(path) as f:
                for line_no, line in enumerate(f, 1):
                    if not line.strip():
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(obj, dict):
                        yield line_no, obj
        except OSError as e:
            self.log(f"Error reading {path}: {e}")

    def _content_text(self, content: Any) -> str:
        if isinstance(content, str):
            return content
        if not isinstance(content, list):
            return ""
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                if item.get("type") in {"input_text", "output_text", "text"}:
                    parts.append(str(item.get("text") or ""))
        return "\n".join(p for p in parts if p)

    def _title(self, turn: dict[str, Any], session_meta: dict[str, Any], thread_meta: dict[str, Any]) -> str:
        for source in (
            thread_meta.get("title"),
            thread_meta.get("preview"),
            session_meta.get("title"),
        ):
            if source:
                return str(source)[:120]
        if turn["user_messages"]:
            return turn["user_messages"][0]["text"].replace("\n", " ")[:120]
        if turn["assistant_messages"]:
            return turn["assistant_messages"][-1]["text"].replace("\n", " ")[:120]
        return "Codex turn"

    def _dedupe_messages(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[tuple[str, str | None]] = set()
        out: list[dict[str, Any]] = []
        for msg in messages:
            key = (msg.get("text", ""), msg.get("phase"))
            if key in seen:
                continue
            seen.add(key)
            out.append(msg)
        return out

    def _truncate_turn_messages(self, turn: dict[str, Any]) -> None:
        for msg in turn["user_messages"]:
            msg["text"] = self._truncate(str(msg.get("text") or ""), self.max_user_chars)
        for msg in turn["assistant_messages"]:
            msg["text"] = self._truncate(str(msg.get("text") or ""), self.max_assistant_chars)

    def _compress_turn_messages(self, turn: dict[str, Any]) -> dict[str, Any] | None:
        if not self.compress_long_messages:
            return None

        refs: list[dict[str, Any]] = []
        messages: list[dict[str, str]] = []
        for key, role in (("user_messages", "user"), ("assistant_messages", "assistant")):
            for msg in turn[key]:
                text = str(msg.get("text") or "")
                if not text:
                    continue
                refs.append(msg)
                messages.append({"role": role, "content": text})

        raw_chars = sum(len(msg["content"]) for msg in messages)
        meta: dict[str, Any] = {
            "provider": "headroom",
            "raw_chars": raw_chars,
            "min_chars": self.compression_min_chars,
        }
        if raw_chars < self.compression_min_chars:
            meta["skipped"] = "below_min_chars"
            return meta

        try:
            result = self._compress_messages_with_headroom(messages)
        except Exception as e:
            meta["error"] = f"{type(e).__name__}: {str(e)[:200]}"
            return meta

        compressed_messages = getattr(result, "messages", result)
        if not isinstance(compressed_messages, list) or len(compressed_messages) != len(refs):
            meta["error"] = "compressor returned unexpected message shape"
            return meta

        changed = 0
        compressed_chars = 0
        for ref, compressed in zip(refs, compressed_messages):
            content = compressed.get("content") if isinstance(compressed, dict) else None
            text = str(content or "")
            if text and len(text) < len(str(ref.get("text") or "")):
                ref["text"] = text
                changed += 1
            compressed_chars += len(str(ref.get("text") or ""))

        meta.update({
            "compressed_messages": changed,
            "compressed_chars": compressed_chars,
            "tokens_before": getattr(result, "tokens_before", None),
            "tokens_after": getattr(result, "tokens_after", None),
            "tokens_saved": getattr(result, "tokens_saved", None),
            "compression_ratio": getattr(result, "compression_ratio", None),
            "transforms_applied": getattr(result, "transforms_applied", None),
        })
        return meta

    def _compress_messages_with_headroom(self, messages: list[dict[str, str]]) -> Any:
        from headroom import CompressConfig, compress

        config = CompressConfig(
            compress_user_messages=True,
            protect_recent=0,
            min_tokens_to_compress=250,
        )
        return compress(messages, config=config)

    def _looks_like_codex_context(self, text: str) -> bool:
        stripped = text.lstrip()
        return (
            stripped.startswith("# AGENTS.md instructions")
            or stripped.startswith("<permissions instructions>")
            or stripped.startswith("<environment_context>")
        )

    def _looks_like_error(self, output: str) -> bool:
        lowered = output.lower()
        return "traceback" in lowered or "error:" in lowered or "exception" in lowered

    def _truncate(self, text: str, limit: int) -> str:
        if len(text) <= limit:
            return text
        return text[:limit] + "\n[truncated]"

    def _mtime_iso(self, path: Path) -> str:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()

    def _epoch_to_iso(self, value: Any) -> str | None:
        if value is None:
            return None
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()
        except (TypeError, ValueError, OSError, OverflowError):
            return None

    def _coerce_ts(self, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return self._epoch_to_iso(value)
        s = str(value).strip()
        if not s:
            return None
        try:
            return self._epoch_to_iso(float(s))
        except ValueError:
            return s

    def _parse_ts(self, ts) -> datetime | None:
        if not ts:
            return None
        if isinstance(ts, datetime):
            return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        if isinstance(ts, (int, float)):
            return self._epoch_to_iso(ts) and datetime.fromtimestamp(float(ts), tz=timezone.utc)
        try:
            dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            try:
                return datetime.fromtimestamp(float(ts), tz=timezone.utc)
            except (TypeError, ValueError, OSError, OverflowError):
                return None


if __name__ == "__main__":
    from ....config import get_data_dir as DATA_DIR_fn

    store = DataStore(DATA_DIR_fn())
    syncer = CodexSyncer(store)
    count, _ = syncer.sync()
    print(f"Synced {count} records")
