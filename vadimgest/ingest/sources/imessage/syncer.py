"""iMessage Syncer - incremental sync from macOS Messages database.

Uses imessage-export binary (with Full Disk Access) to copy chat.db
to a temp location, then reads from the copy. Same pattern as sigtop for Signal.
"""

import os
import subprocess
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Iterator

from ..base import CronSyncer
from ....store import DataStore
from ....models import SourceState
from ....config import get_source_config, get_conversation_settings

# Apple Core Data epoch: 2001-01-01 00:00:00 UTC
APPLE_EPOCH = 978307200

EXPORT_BINARY = Path(__file__).parent / "imessage-export"
TEMP_DB = Path("/tmp/vadimgest_imessage.db")


def _apple_date_to_datetime(date_val: int | None) -> datetime | None:
    """Convert Apple Core Data timestamp to datetime."""
    if date_val is None or date_val == 0:
        return None
    # macOS High Sierra+ uses nanoseconds, older uses seconds
    if date_val > 1_000_000_000:
        unix_ts = (date_val / 1_000_000_000) + APPLE_EPOCH
    else:
        unix_ts = date_val + APPLE_EPOCH
    return datetime.fromtimestamp(unix_ts)


def _datetime_to_apple_date(dt: datetime) -> int:
    """Convert datetime to Apple nanosecond timestamp."""
    return int((dt.timestamp() - APPLE_EPOCH) * 1_000_000_000)


class IMessageSyncer(CronSyncer):
    """iMessage syncer using imessage-export binary."""

    source_name = "imessage"
    display_name = "iMessage"
    description = "Messages from macOS iMessage/SMS database"
    category = "messaging"
    dependencies = {
        "python": [],
        "cli": [],
        "credentials": [],
        "os": ["macos:full_disk_access"],
    }
    config_schema = {}

    @classmethod
    def check_ready(cls) -> dict:
        missing = []
        export_bin = Path(__file__).parent / "imessage-export"
        if not export_bin.exists():
            missing.append("imessage-export binary not found")
        import sys
        if sys.platform != "darwin":
            missing.append("macOS required (Full Disk Access needed)")
        return {"ok": not missing, "missing": missing} if missing else {"ok": True}

    def __init__(self, store: DataStore, config: dict | None = None):
        config = config or get_source_config("imessage")
        super().__init__(store, config)

    def _export_db(self) -> sqlite3.Connection:
        """Export iMessage database using imessage-export binary."""
        if not EXPORT_BINARY.exists():
            raise FileNotFoundError(
                f"imessage-export binary not found at {EXPORT_BINARY}. "
                f"Compile with: swiftc -O export.swift -o imessage-export"
            )

        # Remove old export
        if TEMP_DB.exists():
            os.remove(TEMP_DB)

        result = subprocess.run(
            [str(EXPORT_BINARY), str(TEMP_DB)],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"imessage-export failed: {result.stderr.strip()}. "
                f"Grant Full Disk Access to {EXPORT_BINARY} in "
                f"System Settings > Privacy & Security"
            )

        return sqlite3.connect(TEMP_DB)

    def fetch_new(self, state: SourceState, limit: int = 1000) -> Iterator[dict]:
        """Fetch new messages from iMessage database."""
        self.log("Exporting iMessage database...")

        try:
            conn = self._export_db()
        except (FileNotFoundError, RuntimeError) as e:
            self.log(f"Failed to export iMessage DB: {e}")
            return

        conn.row_factory = sqlite3.Row

        try:
            # Build handle map: ROWID -> phone/email
            handle_map = {}
            for row in conn.execute("SELECT ROWID, id FROM handle"):
                handle_map[row["ROWID"]] = row["id"]

            # Build chat name map: chat_identifier -> display_name
            chat_name_map = {}
            for row in conn.execute("SELECT chat_identifier, display_name FROM chat"):
                if row["display_name"]:
                    chat_name_map[row["chat_identifier"]] = row["display_name"]

            # Fetch messages since last sync
            query = """
                SELECT
                    m.ROWID,
                    m.guid,
                    m.text,
                    m.date,
                    m.is_from_me,
                    m.handle_id,
                    m.cache_roomnames,
                    c.chat_identifier,
                    c.display_name as chat_display_name
                FROM message m
                LEFT JOIN chat_message_join cmj ON m.ROWID = cmj.message_id
                LEFT JOIN chat c ON cmj.chat_id = c.ROWID
                WHERE m.text IS NOT NULL AND m.text != ''
            """
            params = []

            if state.last_ts:
                last_dt = datetime.fromisoformat(state.last_ts.replace("Z", "+00:00"))
                last_apple = _datetime_to_apple_date(last_dt)
                query += " AND m.date > ?"
                params.append(last_apple)

            query += " ORDER BY m.date ASC LIMIT ?"
            params.append(limit * get_conversation_settings()["max_messages_per_chunk"])

            rows = [dict(row) for row in conn.execute(query, params).fetchall()]

            if not rows:
                self.log("No new messages")
                return

            self.log(f"Found {len(rows)} new messages")

            # Group into conversations
            chunks = self._group_into_chunks(rows, handle_map, chat_name_map)
            self.log(f"Grouped into {len(chunks)} conversation chunks")

            for chunk in chunks[:limit]:
                yield chunk

        finally:
            conn.close()
            if TEMP_DB.exists():
                os.remove(TEMP_DB)

    def _get_chat_name(self, row: dict, handle_map: dict, chat_name_map: dict) -> str:
        """Determine chat name from message row."""
        # Group chat: use display_name or cache_roomnames
        if row.get("chat_display_name"):
            return row["chat_display_name"]
        if row.get("cache_roomnames"):
            name = chat_name_map.get(row["cache_roomnames"], row["cache_roomnames"])
            return name

        # DM: use handle identifier (phone/email)
        handle_id = row.get("handle_id")
        if handle_id and handle_id in handle_map:
            return handle_map[handle_id]

        return row.get("chat_identifier") or "Unknown"

    def _is_group_chat(self, row: dict) -> bool:
        """Check if message is from a group chat."""
        return bool(row.get("cache_roomnames") or (
            row.get("chat_identifier") and row["chat_identifier"].startswith("chat")
        ))

    def _group_into_chunks(self, rows: list[dict], handle_map: dict, chat_name_map: dict) -> list[dict]:
        """Group messages into conversation chunks."""
        if not rows:
            return []

        # Group by chat
        by_chat: dict[str, list[dict]] = {}
        for row in rows:
            chat_key = row.get("chat_identifier") or row.get("cache_roomnames") or \
                handle_map.get(row.get("handle_id"), "unknown")
            if chat_key not in by_chat:
                by_chat[chat_key] = []
            by_chat[chat_key].append(row)

        chunks = []
        window_ns = get_conversation_settings()["time_window_hours"] * 3600 * 1_000_000_000
        min_msgs = get_conversation_settings()["min_messages_per_chunk"]
        max_msgs = get_conversation_settings()["max_messages_per_chunk"]

        for chat_key, messages in by_chat.items():
            messages.sort(key=lambda x: x["date"] or 0)

            current_chunk = []
            chunk_start = None

            for msg in messages:
                msg_date = msg["date"] or 0

                if chunk_start is None:
                    chunk_start = msg_date
                    current_chunk = [msg]
                elif (msg_date - chunk_start) <= window_ns and len(current_chunk) < max_msgs:
                    current_chunk.append(msg)
                else:
                    if len(current_chunk) >= min_msgs:
                        chunks.append(self._chunk_to_record(
                            current_chunk, chat_key, handle_map, chat_name_map
                        ))
                    chunk_start = msg_date
                    current_chunk = [msg]

            # Flush remaining - use min 1 for iMessage (fewer messages than Telegram)
            if len(current_chunk) >= min(min_msgs, 1):
                chunks.append(self._chunk_to_record(
                    current_chunk, chat_key, handle_map, chat_name_map
                ))

        return chunks

    def _chunk_to_record(self, chunk: list[dict], chat_key: str,
                         handle_map: dict, chat_name_map: dict) -> dict:
        """Convert message chunk to conversation record."""
        first = chunk[0]
        last = chunk[-1]

        start_dt = _apple_date_to_datetime(first["date"])
        end_dt = _apple_date_to_datetime(last["date"])

        chat_name = self._get_chat_name(first, handle_map, chat_name_map)
        is_group = self._is_group_chat(first)

        messages = []
        for msg in chunk:
            msg_dt = _apple_date_to_datetime(msg["date"])
            ts_str = msg_dt.isoformat() if msg_dt else ""

            if msg.get("is_from_me"):
                sender = "Me"
            else:
                handle_id = msg.get("handle_id")
                sender = handle_map.get(handle_id, "Unknown") if handle_id else "Unknown"

            messages.append({
                "ts": ts_str,
                "sender": sender,
                "text": msg["text"],
            })

        start_str = start_dt.isoformat() if start_dt else ""
        end_str = end_dt.isoformat() if end_dt else ""
        record_id = f"imsg_{chat_key}_{first['date']}_{last['date']}"

        return {
            "id": record_id,
            "type": "conversation",
            "chat": chat_name,
            "period_start": start_str,
            "period_end": end_str,
            "messages": messages,
            "meta": {
                "chat_identifier": chat_key,
                "is_group": is_group,
                "message_count": len(messages),
            },
        }
