"""Browser History Syncer - sync browsing data from Arc (Chromium-based)."""

import hashlib
import shutil
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Iterator
from urllib.parse import urlparse

from ..base import CronSyncer
from ....store import DataStore
from ....models import SourceState
from ....config import get_source_config

# Chrome epoch: microseconds since 1601-01-01
_CHROME_EPOCH_OFFSET = 11644473600

# URLs to skip
_NOISE_PREFIXES = (
    "chrome-extension://",
    "arc://",
    "chrome://",
    "about:",
    "data:",
    "blob:",
    "javascript:",
    "file://",
    "devtools://",
)

_NOISE_DOMAINS = frozenset({
    "localhost",
    "127.0.0.1",
    "0.0.0.0",
    "newtab",
    "extensions",
})

# Transition types (Chromium source)
_TRANSITIONS = {
    0: "link",
    1: "typed",
    2: "auto_bookmark",
    5: "form_submit",
    6: "reload",
    7: "search",
    8: "keyword_generated",
}

# Skip auto_subframe (3) and other noise transitions
_SKIP_TRANSITIONS = frozenset({3, 4})

_DEFAULT_DB = Path.home() / "Library/Application Support/Arc/User Data/Default/History"

# Auto-detection paths for Chromium-based browsers on macOS (ordered by preference).
_AUTODETECT_PATHS: list[tuple[str, Path]] = [
    ("Arc", Path.home() / "Library/Application Support/Arc/User Data/Default/History"),
    ("Chrome", Path.home() / "Library/Application Support/Google/Chrome/Default/History"),
    ("Chrome Canary", Path.home() / "Library/Application Support/Google/Chrome Canary/Default/History"),
    ("Brave", Path.home() / "Library/Application Support/BraveSoftware/Brave-Browser/Default/History"),
    ("Edge", Path.home() / "Library/Application Support/Microsoft Edge/Default/History"),
    ("Vivaldi", Path.home() / "Library/Application Support/Vivaldi/Default/History"),
]


def _autodetect_profiles() -> list[dict]:
    """Return profiles found on disk - used when user hasn't configured any."""
    found = []
    for name, path in _AUTODETECT_PATHS:
        if path.exists():
            found.append({"name": name, "path": str(path)})
    return found


class BrowserSyncer(CronSyncer):
    """Arc browser history syncer."""

    source_name = "browser"
    display_name = "Browser History"
    description = "Browsing history from Arc or Chromium-based browsers"
    category = "activity"
    dependencies = {
        "python": [],
        "cli": [],
        "credentials": [],
        "os": ["macos"],
    }
    config_schema = {
        "profiles": {
            "type": "list",
            "item_type": "object",
            "item_fields": [
                {"key": "name", "type": "str", "placeholder": "Default"},
                {"key": "path", "type": "path", "placeholder": "~/Library/Application Support/Arc/..."},
            ],
            "default": [],
            "advanced": True,
            "description": "Browser profiles — auto-detected from Arc/Chrome standard locations",
        },
        "session_window_minutes": {"type": "int", "default": 30, "description": "Group visits into sessions by time gap"},
    }

    def __init__(self, store: DataStore, config: dict | None = None):
        config = config or get_source_config("browser")
        super().__init__(store, config)

        # Fall back to auto-detection if no profiles configured, so a user who
        # hits "Enable" without touching advanced settings still gets data.
        configured = config.get("profiles") or []
        if not configured:
            configured = _autodetect_profiles()
            if not configured:
                configured = [{"name": "Default", "path": str(_DEFAULT_DB)}]
        self.profiles = configured
        self.session_window_sec = config.get("session_window_minutes", 30) * 60
        self.temp_dir = Path("/tmp/vadimgest_browser")

    def fetch_new(self, state: SourceState, limit: int = 1000) -> Iterator[dict]:
        """Fetch new browsing sessions from Arc history."""
        yielded = 0

        for profile in self.profiles:
            if yielded >= limit:
                break

            db_path = Path(profile["path"]).expanduser()
            if not db_path.exists():
                self.log(f"DB not found: {db_path}")
                continue

            # Copy DB to temp (Arc locks it while running)
            self.temp_dir.mkdir(parents=True, exist_ok=True)
            temp_db = self.temp_dir / f"{profile['name']}_history.db"

            try:
                shutil.copy2(db_path, temp_db)
            except (OSError, PermissionError) as e:
                self.log(f"Failed to copy DB: {e}")
                continue

            try:
                conn = sqlite3.connect(temp_db)
                conn.row_factory = sqlite3.Row

                chrome_ts_filter = self._iso_to_chrome_ts(state.last_ts) if state.last_ts else 0
                visits = self._fetch_visits(conn, chrome_ts_filter, limit - yielded)
                sessions = self._group_into_sessions(visits, profile["name"])

                for session in sessions:
                    yield session
                    yielded += 1
                    if yielded >= limit:
                        break

                self.log(f"Profile {profile['name']}: {len(sessions)} browsing sessions")

            except sqlite3.Error as e:
                self.log(f"SQLite error for {profile['name']}: {e}")
            finally:
                conn.close()
                temp_db.unlink(missing_ok=True)

        self.log(f"Total: {yielded} browsing sessions")

    def _fetch_visits(self, conn: sqlite3.Connection, since_chrome_ts: int, limit: int) -> list[dict]:
        """Fetch visits joined with URLs, filtered and cleaned."""
        query = """
            SELECT
                u.url, u.title, u.visit_count,
                v.visit_time, v.visit_duration, v.transition
            FROM visits v
            JOIN urls u ON v.url = u.id
            WHERE v.visit_time > ?
              AND u.hidden = 0
            ORDER BY v.visit_time ASC
            LIMIT ?
        """
        # Fetch more than limit to allow grouping
        cursor = conn.execute(query, (since_chrome_ts, limit * 20))

        visits = []
        for row in cursor:
            url = row["url"]
            transition = row["transition"] & 0xFF  # Core transition (lower 8 bits)

            if transition in _SKIP_TRANSITIONS:
                continue
            if any(url.startswith(p) for p in _NOISE_PREFIXES):
                continue

            parsed = urlparse(url)
            domain = parsed.hostname or ""
            if domain in _NOISE_DOMAINS:
                continue

            visit_time = self._chrome_ts_to_datetime(row["visit_time"])
            duration_us = row["visit_duration"] or 0
            duration_sec = duration_us // 1_000_000

            visits.append({
                "url": url,
                "title": row["title"] or "",
                "domain": domain,
                "visit_time": visit_time,
                "duration_sec": duration_sec,
                "transition": _TRANSITIONS.get(transition, "other"),
            })

        return visits

    def _group_into_sessions(self, visits: list[dict], profile_name: str) -> list[dict]:
        """Group visits by domain within time windows."""
        # Group by domain
        by_domain: dict[str, list[dict]] = defaultdict(list)
        for v in visits:
            by_domain[v["domain"]].append(v)

        sessions = []
        for domain, domain_visits in by_domain.items():
            domain_visits.sort(key=lambda x: x["visit_time"])

            # Split into time windows
            current_window: list[dict] = []
            window_start: datetime | None = None

            for v in domain_visits:
                if window_start is None:
                    window_start = v["visit_time"]
                    current_window = [v]
                elif (v["visit_time"] - window_start).total_seconds() <= self.session_window_sec:
                    current_window.append(v)
                else:
                    if current_window:
                        sessions.append(self._window_to_record(domain, current_window, profile_name))
                    window_start = v["visit_time"]
                    current_window = [v]

            if current_window:
                sessions.append(self._window_to_record(domain, current_window, profile_name))

        sessions.sort(key=lambda x: x["period_start"])
        return sessions

    def _window_to_record(self, domain: str, visits: list[dict], profile: str) -> dict:
        """Convert a time window of visits to a record."""
        first_ts = visits[0]["visit_time"]
        last_ts = visits[-1]["visit_time"]
        total_duration = sum(v["duration_sec"] for v in visits)

        # Pick best title (longest, non-empty)
        best_title = max(
            (v["title"] for v in visits if v["title"]),
            key=len,
            default=domain,
        )

        # Build pages list (dedup by URL, keep first visit)
        seen_urls = set()
        pages = []
        for v in visits:
            if v["url"] not in seen_urls:
                seen_urls.add(v["url"])
                pages.append({
                    "url": v["url"],
                    "title": v["title"],
                    "visit_time": v["visit_time"].isoformat(),
                    "duration_sec": v["duration_sec"],
                })

        window_id = int(first_ts.timestamp())
        record_id = f"arc_{domain}_{window_id}"

        return {
            "id": record_id,
            "type": "browsing_session",
            "domain": domain,
            "title": best_title,
            "pages": pages[:20],  # Cap at 20 pages per session
            "period_start": first_ts.isoformat(),
            "period_end": last_ts.isoformat(),
            "total_visits": len(visits),
            "total_duration_sec": total_duration,
            "meta": {
                "profile": profile,
                "unique_pages": len(pages),
            },
        }

    @staticmethod
    def _chrome_ts_to_datetime(chrome_ts: int) -> datetime:
        """Convert Chrome timestamp (microseconds since 1601-01-01) to datetime."""
        unix_ts = (chrome_ts / 1_000_000) - _CHROME_EPOCH_OFFSET
        try:
            return datetime.fromtimestamp(unix_ts)
        except (OSError, ValueError):
            return datetime(2020, 1, 1)

    @staticmethod
    def _iso_to_chrome_ts(iso_ts: str) -> int:
        """Convert ISO timestamp to Chrome timestamp."""
        dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        unix_ts = dt.timestamp()
        return int((unix_ts + _CHROME_EPOCH_OFFSET) * 1_000_000)


if __name__ == "__main__":
    from ...store import DataStore
    from ...config import get_data_dir as DATA_DIR_fn

    DATA_DIR = DATA_DIR_fn()
    store = DataStore(DATA_DIR)
    syncer = BrowserSyncer(store)
    count = syncer.run()
    print(f"Synced {count} records")
