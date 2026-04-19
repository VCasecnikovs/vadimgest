"""Signal Syncer - incremental sync using sigtop CLI.

Exports Signal Desktop database and extracts new messages.
"""

import subprocess
import sqlite3
import os
from datetime import datetime
from pathlib import Path
from typing import Iterator

from ..base import CronSyncer
from ....store import DataStore
from ....models import SourceState
from ....config import get_source_config, get_conversation_settings


class SignalSyncer(CronSyncer):
    """Signal syncer using sigtop CLI."""

    source_name = "signal"
    display_name = "Signal"
    description = "Messages from Signal Desktop groups and conversations"
    category = "messaging"
    dependencies = {
        "python": [],
        "cli": ["sigtop"],
        "credentials": [],
        "os": ["macos"],
    }
    config_schema = {}

    def __init__(self, store: DataStore, config: dict | None = None):
        config = config or get_source_config("signal")
        super().__init__(store, config)

        self.temp_db_path = Path("/tmp/vadimgest_signal_export.db")

    def _export_signal_db(self) -> sqlite3.Connection:
        """Export Signal Desktop database using sigtop."""
        # Remove old export
        if self.temp_db_path.exists():
            os.remove(self.temp_db_path)

        result = subprocess.run(
            ["sigtop", "export-database", str(self.temp_db_path)],
            capture_output=True,
            text=True,
            timeout=60,
        )

        if result.returncode != 0:
            raise Exception(f"sigtop error: {result.stderr}")

        return sqlite3.connect(self.temp_db_path)

    def fetch_new(self, state: SourceState, limit: int = 1000) -> Iterator[dict]:
        """Fetch new messages from Signal."""
        self.log("Exporting Signal database...")

        try:
            conn = self._export_signal_db()
        except Exception as e:
            self.log(f"Failed to export Signal DB: {e}")
            return

        conn.row_factory = sqlite3.Row

        try:
            # Get conversations with profile names
            conv_map = {}
            # Also build serviceId -> name map for resolving group chat senders
            service_id_names = {}
            cursor = conn.execute("""
                SELECT id, name, type, profileFullName, profileName, serviceId
                FROM conversations
            """)
            for row in cursor:
                # Priority: name > profileFullName > profileName > id
                display_name = (
                    row["name"] or
                    row["profileFullName"] or
                    row["profileName"] or
                    row["id"]
                )
                conv_map[row["id"]] = {
                    "name": display_name,
                    "type": row["type"],
                }
                # Map serviceId to display name for sender resolution
                if row["serviceId"]:
                    service_id_names[row["serviceId"]] = display_name

            # Get messages since last sync (include attachment-only messages)
            # Include sourceServiceId for resolving actual sender in group chats
            query = """
                SELECT rowid, id, conversationId, sent_at, type, body, source,
                       hasAttachments, sourceServiceId
                FROM messages
                WHERE (body IS NOT NULL AND body != '') OR hasAttachments = 1
            """

            params = []
            if state.last_ts:
                last_dt = datetime.fromisoformat(state.last_ts.replace("Z", "+00:00"))
                last_ms = int(last_dt.timestamp() * 1000)
                query += " AND sent_at > ?"
                params.append(last_ms)

            query += " ORDER BY sent_at ASC LIMIT ?"
            params.append(limit * get_conversation_settings()["max_messages_per_chunk"])

            cursor = conn.execute(query, params)
            rows = [dict(row) for row in cursor.fetchall()]

            if not rows:
                self.log("No new messages")
                return

            self.log(f"Found {len(rows)} new messages")

            # Fetch attachment metadata for messages that have them
            att_msg_ids = [r["id"] for r in rows if r.get("hasAttachments")]
            attachments_map: dict[str, list[dict]] = {}
            if att_msg_ids:
                # Batch query attachments
                placeholders = ",".join("?" * len(att_msg_ids))
                att_cursor = conn.execute(f"""
                    SELECT messageId, contentType, fileName, size
                    FROM message_attachments
                    WHERE messageId IN ({placeholders})
                    ORDER BY messageId, orderInMessage
                """, att_msg_ids)
                for att_row in att_cursor:
                    mid = att_row["messageId"]
                    if mid not in attachments_map:
                        attachments_map[mid] = []
                    attachments_map[mid].append({
                        "content_type": att_row["contentType"] or "",
                        "file_name": att_row["fileName"] or "",
                        "size": att_row["size"] or 0,
                    })
                # Merge into rows
                for row in rows:
                    row["_attachments"] = attachments_map.get(row["id"], [])

            # Group into conversations
            chunks = self._group_into_chunks(rows, conv_map, service_id_names)
            self.log(f"Grouped into {len(chunks)} conversation chunks")

            for chunk in chunks[:limit]:
                yield chunk

        finally:
            conn.close()
            # Cleanup temp file
            if self.temp_db_path.exists():
                os.remove(self.temp_db_path)

    def _group_into_chunks(self, rows: list[dict], conv_map: dict,
                           service_id_names: dict | None = None) -> list[dict]:
        """Group messages into conversation chunks."""
        if not rows:
            return []

        by_conv: dict[str, list[dict]] = {}
        for row in rows:
            conv_id = row["conversationId"]
            if conv_id not in by_conv:
                by_conv[conv_id] = []
            by_conv[conv_id].append(row)

        chunks = []
        window_ms = get_conversation_settings()["time_window_hours"] * 3600 * 1000
        min_msgs = get_conversation_settings()["min_messages_per_chunk"]
        max_msgs = get_conversation_settings()["max_messages_per_chunk"]

        for conv_id, messages in by_conv.items():
            messages.sort(key=lambda x: x["sent_at"])

            current_chunk = []
            chunk_start = None

            for msg in messages:
                msg_ts = msg["sent_at"]

                if chunk_start is None:
                    chunk_start = msg_ts
                    current_chunk = [msg]
                elif (msg_ts - chunk_start) <= window_ms and len(current_chunk) < max_msgs:
                    current_chunk.append(msg)
                else:
                    if len(current_chunk) >= min_msgs:
                        chunks.append(self._chunk_to_record(
                            current_chunk, conv_id, conv_map, service_id_names))
                    chunk_start = msg_ts
                    current_chunk = [msg]

            if len(current_chunk) >= min_msgs:
                chunks.append(self._chunk_to_record(
                    current_chunk, conv_id, conv_map, service_id_names))

        return chunks

    def _chunk_to_record(self, chunk: list[dict], conv_id: str, conv_map: dict,
                         service_id_names: dict | None = None) -> dict:
        """Convert message chunk to record."""
        first = chunk[0]
        last = chunk[-1]

        start_ts = datetime.fromtimestamp(first["sent_at"] / 1000)
        end_ts = datetime.fromtimestamp(last["sent_at"] / 1000)

        conv_info = conv_map.get(conv_id, {"name": conv_id, "type": "unknown"})
        is_group = conv_info.get("type") == "group"

        messages = []
        for msg in chunk:
            msg_ts = datetime.fromtimestamp(msg["sent_at"] / 1000)
            # Signal type: "outgoing" = me, "incoming" = them
            if msg.get("type") == "outgoing":
                sender = "Me"
            elif is_group and service_id_names and msg.get("sourceServiceId"):
                # For group chats, resolve actual sender from serviceId
                sender = service_id_names.get(
                    msg["sourceServiceId"], conv_info["name"])
            else:
                # For 1:1 chats, conversation name IS the sender
                sender = conv_info["name"]
            msg_data = {
                "ts": msg_ts.isoformat(),
                "sender": sender,
                "text": msg.get("body") or "",
            }
            if msg.get("_attachments"):
                msg_data["attachments"] = msg["_attachments"]
            messages.append(msg_data)

        record_id = f"{conv_id}_{first['sent_at']}_{last['sent_at']}"

        return {
            "id": record_id,
            "type": "conversation",
            "chat": conv_info["name"],
            "period_start": start_ts.isoformat(),
            "period_end": end_ts.isoformat(),
            "messages": messages,
            "meta": {
                "conversation_id": conv_id,
                "message_count": len(messages),
            },
        }


if __name__ == "__main__":
    from ...store import DataStore
    from ...config import get_data_dir as DATA_DIR_fn
    DATA_DIR = DATA_DIR_fn()

    store = DataStore(DATA_DIR)
    syncer = SignalSyncer(store)
    count = syncer.run()
    print(f"Synced {count} records")
