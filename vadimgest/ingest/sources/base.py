"""Base class for source syncers."""

import json
import os
import shutil
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from ...store import DataStore
from ...models import SourceState


class BaseSyncer(ABC):
    """
    Base class for data source syncers.

    Each syncer is responsible for:
    1. Connecting to the primary data source
    2. Fetching new data since last sync
    3. Converting to unified format
    4. Writing to the store

    Subclasses MUST define manifest class attributes:
    - display_name: Human-readable name (e.g. "Telegram")
    - description: What this source syncs (one sentence)
    - category: One of messaging|email|calendar|files|dev|activity|meetings|social|knowledge
    - dependencies: dict with keys python, cli, credentials, os
    - config_schema: dict of configurable fields with type/default/description
    """

    source_name: str = "base"

    # --- Manifest (override in subclasses) ---
    display_name: str = ""
    description: str = ""
    category: str = ""  # messaging|email|calendar|files|dev|activity|meetings|social|knowledge
    dependencies: dict = {"python": [], "cli": [], "credentials": [], "os": []}
    config_schema: dict = {}
    credential_help: dict = {}

    @classmethod
    def check_ready(cls) -> dict:
        """Check if this source can run on the current system.

        Returns {"ok": True} or {"ok": False, "missing": ["reason1", ...]}.
        Default implementation checks CLI tools and Python packages.
        """
        missing = []

        # Check CLI tools
        for tool in cls.dependencies.get("cli", []):
            if shutil.which(tool) is None:
                missing.append(f"CLI tool '{tool}' not found in PATH")

        # Check Python packages
        for pkg in cls.dependencies.get("python", []):
            try:
                __import__(pkg)
            except ImportError:
                missing.append(f"Python package '{pkg}' not installed")

        # Check credentials (env vars)
        for var in cls.dependencies.get("credentials", []):
            if not os.environ.get(var):
                missing.append(f"Environment variable '{var}' not set")

        if missing:
            return {"ok": False, "missing": missing}
        return {"ok": True}

    def __init__(self, store: DataStore, config: dict):
        self.store = store
        self.config = config
        self._log_file = store.base_path / "sync.log"

    @abstractmethod
    def fetch_new(self, state: SourceState, limit: int = 1000) -> Iterator[dict]:
        """
        Fetch new records from the primary source.

        Args:
            state: Current sync state (last_id, last_ts)
            limit: Max records to fetch

        Yields:
            dict records ready for storage
        """
        pass

    def sync(self, limit: int = 10000) -> tuple[int, list[str]]:
        """
        Sync new data from source to store.

        Returns:
            Tuple of (count of records added, list of summary labels)
        """
        state = self.store.get_state(self.source_name)
        count = 0
        summaries = []

        for record in self.fetch_new(state, limit):
            record_id = record.get("id")

            if record_id and self.store.exists(self.source_name, record_id):
                continue

            self.store.append(self.source_name, record)
            count += 1

            if len(summaries) < 5:
                label = self._extract_label(record)
                if label and label not in summaries:
                    summaries.append(label)

        return count, summaries

    @staticmethod
    def _extract_label(record: dict) -> str:
        """Extract a human-readable label from a record for sync summaries."""
        rtype = record.get("type", "")
        if rtype == "conversation":
            return record.get("chat", "")
        elif rtype == "email":
            return record.get("subject", "")[:60]
        elif rtype in ("meeting", "document", "task", "session"):
            return record.get("title", "")[:60]
        elif rtype == "issue":
            num = record.get("number", "")
            title = record.get("title", "")[:50]
            return f"#{num} {title}" if num else title
        elif rtype == "browsing_session":
            return record.get("domain", "")
        elif rtype == "activity":
            return record.get("title", "")[:60]
        elif rtype == "calendar_event":
            return record.get("title", record.get("summary", ""))[:60]
        elif rtype == "drive_file":
            return record.get("name", record.get("title", ""))[:60]
        return record.get("title", record.get("chat", record.get("name", "")))[:60]

    def log(self, msg: str):
        """Log message with timestamp and source name."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}] [{self.source_name}] {msg}"
        print(line)
        # Also write to log file
        try:
            with open(self._log_file, "a") as f:
                f.write(line + "\n")
        except Exception:
            pass

    def log_run(self, status: str, count: int = 0, error: str = None,
                duration: float = 0, summary: list[str] | None = None):
        """Log a sync run to sync_runs.jsonl."""
        runs_file = self.store.base_path / "sync_runs.jsonl"
        run = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "source": self.source_name,
            "status": status,
            "count": count,
            "duration_sec": round(duration, 2),
        }
        if error:
            run["error"] = error
        if summary:
            run["summary"] = summary[:5]

        try:
            with open(runs_file, "a") as f:
                f.write(json.dumps(run, ensure_ascii=False) + "\n")
        except Exception:
            pass


class CronSyncer(BaseSyncer):
    """Base class for cron-based syncers."""

    def run(self, limit: int = 10000) -> int:
        """Run a single sync cycle."""
        self.log(f"Starting sync...")
        count, summary = self.sync(limit)
        if summary:
            self.log(f"Synced {count} records ({', '.join(summary[:3])})")
        else:
            self.log(f"Synced {count} records")
        return count
