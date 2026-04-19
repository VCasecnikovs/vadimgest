"""Google Drive Syncer - sync files from Google Drive via gog CLI."""

from datetime import datetime
from typing import Iterator

from ..base import CronSyncer
from ..gog_utils import gog_call
from ....store import DataStore
from ....models import SourceState
from ....config import get_source_config

# MIME types that support content extraction
_TEXT_MIME_TYPES = frozenset({
    "application/vnd.google-apps.document",
    "application/vnd.google-apps.spreadsheet",
    "application/vnd.google-apps.presentation",
    "text/plain",
    "text/markdown",
    "text/csv",
    "text/html",
    "application/pdf",
    "application/json",
})

# Skip these MIME types entirely
_SKIP_MIME_TYPES = frozenset({
    "application/vnd.google-apps.folder",
    "application/vnd.google-apps.shortcut",
    "application/vnd.google-apps.form",
})


class GDriveSyncer(CronSyncer):
    """Google Drive file syncer via gog CLI."""

    source_name = "gdrive"
    display_name = "Google Drive"
    description = "Files and documents from Google Drive"
    category = "files"
    dependencies = {
        "python": [],
        "cli": ["gog"],
        "credentials": [],
        "os": [],
    }
    config_schema = {
        "accounts": {"type": "list", "default": [], "description": "Google accounts to sync"},
        "max_results": {"type": "int", "default": 50, "description": "Max files to fetch"},
    }

    def __init__(self, store: DataStore, config: dict | None = None):
        config = config or get_source_config("gdrive")
        super().__init__(store, config)

        self.accounts = config.get("accounts", ["user@example.com"])
        self.max_results = config.get("max_results", 50)
        self.content_preview_size = config.get("content_preview_size", 5000)

    def fetch_new(self, state: SourceState, limit: int = 1000) -> Iterator[dict]:
        """Fetch recently modified files from Google Drive."""
        if not self.accounts:
            self.log("No Drive accounts configured")
            return

        yielded = 0
        for account in self.accounts:
            if yielded >= limit:
                break

            self.log(f"Searching Drive for {account}...")
            files = self._search_files(account, state.last_ts)

            if not files:
                self.log(f"No modified files in {account}")
                continue

            self.log(f"Found {len(files)} files in {account}")

            for file_info in files:
                if yielded >= limit:
                    break

                record = self._file_to_record(file_info, account)
                if record:
                    yield record
                    yielded += 1

        self.log(f"Total: {yielded} file records")

    def _search_files(self, account: str, last_ts: str | None) -> list[dict]:
        """Search for recently modified files."""
        # Build query
        query = "trashed = false"
        if last_ts:
            ts = last_ts.replace("+00:00", "Z")
            if not ts.endswith("Z"):
                ts = ts + "Z"
            query += f" and modifiedTime > '{ts}'"

        try:
            result = gog_call("drive", "search", [query], account=account)
        except Exception as e:
            self.log(f"Drive search failed for {account}: {e}")
            return []

        return result.get("files", [])

    def _file_to_record(self, file_info: dict, account: str) -> dict | None:
        """Convert file info to vadimgest record."""
        file_id = file_info.get("id", "")
        if not file_id:
            return None

        mime_type = file_info.get("mimeType", file_info.get("mime_type", ""))

        # Skip folders and other non-file types
        if mime_type in _SKIP_MIME_TYPES:
            return None

        name = file_info.get("name", "")
        modified_at = file_info.get("modifiedTime", file_info.get("modified_at", ""))
        owner = file_info.get("owner", account)
        web_link = file_info.get("webViewLink", file_info.get("web_link", ""))

        # Fetch content preview for text-compatible types
        content_preview = ""
        if mime_type in _TEXT_MIME_TYPES:
            content_preview = self._get_content_preview(account, file_id, mime_type)

        return {
            "id": f"gdrive_{file_id}",
            "type": "drive_file",
            "name": name,
            "mime_type": mime_type,
            "modified_at": modified_at,
            "owner": owner,
            "web_link": web_link,
            "content_preview": content_preview,
            "meta": {
                "file_id": file_id,
                "drive_account": account,
            },
        }

    def _get_content_preview(self, account: str, file_id: str, mime_type: str = "") -> str:
        """Fetch content preview for a file."""
        try:
            if mime_type == "application/vnd.google-apps.document":
                result = gog_call("docs", "cat", [file_id], account=account)
            else:
                result = gog_call("drive", "download", [file_id, "--stdout"], account=account)
        except Exception:
            return ""

        if isinstance(result, str):
            return result[:self.content_preview_size]
        if isinstance(result, dict):
            content = result.get("content", result.get("text", ""))
            if isinstance(content, str):
                return content[:self.content_preview_size]
        return ""


if __name__ == "__main__":
    from ...store import DataStore
    from ...config import get_data_dir as DATA_DIR_fn

    DATA_DIR = DATA_DIR_fn()
    store = DataStore(DATA_DIR)
    syncer = GDriveSyncer(store)
    count = syncer.run()
    print(f"Synced {count} records")
