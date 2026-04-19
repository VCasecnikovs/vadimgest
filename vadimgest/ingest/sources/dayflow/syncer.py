"""Dayflow Syncer - sync activity data from Dayflow SQLite."""

import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import Iterator

from ..base import CronSyncer
from ....store import DataStore
from ....models import SourceState
from ....config import get_source_config


class DayflowSyncer(CronSyncer):
    """Dayflow screen activity syncer."""

    source_name = "dayflow"
    display_name = "Dayflow"
    description = "Screen activity and app usage tracking from Dayflow"
    category = "activity"
    dependencies = {
        "python": [],
        "cli": [],
        "credentials": [],
        "os": ["macos"],
    }
    config_schema = {
        "db_path": {"type": "path", "default": "~/Library/Application Support/Dayflow/chunks.sqlite", "description": "Path to Dayflow database", "advanced": True, "auto_detected": True},
    }

    def __init__(self, store: DataStore, config: dict | None = None):
        config = config or get_source_config("dayflow")
        super().__init__(store, config)

        self.db_path = Path(
            config.get("db_path")
            or Path.home() / "Library/Application Support/Dayflow/chunks.sqlite"
        )

    def fetch_new(self, state: SourceState, limit: int = 1000) -> Iterator[dict]:
        """Fetch new activity cards from Dayflow."""
        if not self.db_path.exists():
            self.log(f"Dayflow database not found: {self.db_path}")
            return

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        try:
            query = """
                SELECT
                    id, start, end, start_ts, end_ts, day,
                    title, summary, category, subcategory,
                    detailed_summary, metadata
                FROM timeline_cards
                WHERE is_deleted = 0
                  AND title IS NOT NULL
            """

            params = []
            if state.last_ts:
                last_dt = datetime.fromisoformat(state.last_ts.replace("Z", "+00:00"))
                last_unix = int(last_dt.timestamp())
                query += " AND start_ts > ?"
                params.append(last_unix)

            query += " ORDER BY start_ts ASC LIMIT ?"
            params.append(limit)

            cursor = conn.execute(query, params)

            count = 0
            for row in cursor:
                record = self._row_to_record(dict(row))
                if record:
                    yield record
                    count += 1

            self.log(f"Found {count} new activity cards")

        finally:
            conn.close()

    def _row_to_record(self, row: dict) -> dict | None:
        """Convert database row to record."""
        start_ts = row.get("start_ts")
        end_ts = row.get("end_ts")

        started_at = datetime.fromtimestamp(start_ts) if start_ts else None
        ended_at = datetime.fromtimestamp(end_ts) if end_ts else None

        duration_seconds = 0
        if start_ts and end_ts:
            duration_seconds = end_ts - start_ts

        # Parse metadata for distractions
        distractions = None
        if row.get("metadata"):
            try:
                metadata = json.loads(row["metadata"])
                distractions = metadata.get("distractions")
            except Exception:
                pass

        return {
            "id": f"card_{row['id']}",
            "type": "activity",
            "title": row.get("title", ""),
            "summary": row.get("summary", ""),
            "detailed_summary": row.get("detailed_summary", ""),
            "category": row.get("category", ""),
            "subcategory": row.get("subcategory"),
            "day": row.get("day"),
            "time_start": row.get("start"),
            "time_end": row.get("end"),
            "started_at": started_at.isoformat() if started_at else None,
            "ended_at": ended_at.isoformat() if ended_at else None,
            "duration_seconds": duration_seconds,
            "meta": {
                "source_id": row["id"],
                "distractions": distractions,
            },
        }


if __name__ == "__main__":
    from ...store import DataStore
    from ...config import get_data_dir as DATA_DIR_fn
    DATA_DIR = DATA_DIR_fn()

    store = DataStore(DATA_DIR)
    syncer = DayflowSyncer(store)
    count = syncer.run()
    print(f"Synced {count} records")
