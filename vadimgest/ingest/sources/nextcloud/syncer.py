"""Nextcloud Syncer - sync files from Nextcloud via WebDAV API."""

import hashlib
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Iterator

import requests

from ..base import CronSyncer
from ....store import DataStore
from ....models import SourceState
from ....config import get_source_config

_DAV_NS = "DAV:"
_NC_NS = "http://nextcloud.org/ns"
_OC_NS = "http://owncloud.org/ns"

_PROPFIND_BODY = """<?xml version="1.0" encoding="UTF-8"?>
<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:nc="http://nextcloud.org/ns">
  <d:prop>
    <d:getlastmodified/>
    <d:getcontentlength/>
    <d:getcontenttype/>
    <d:getetag/>
    <d:resourcetype/>
    <oc:fileid/>
  </d:prop>
</d:propfind>"""

# MIME types we can extract text from
_TEXT_MIMES = frozenset({
    "text/plain",
    "text/markdown",
    "text/csv",
    "text/html",
    "text/xml",
    "application/json",
    "application/xml",
})

# Prefixes that indicate text-extractable content
_TEXT_MIME_PREFIXES = ("text/",)

# Max content preview size
_MAX_PREVIEW_BYTES = 5000

# Skip these file extensions
_SKIP_EXTENSIONS = frozenset({
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg", ".webp", ".ico",
    ".mp4", ".avi", ".mkv", ".mov", ".wmv", ".mp3", ".wav", ".flac",
    ".zip", ".tar", ".gz", ".7z", ".rar",
    ".exe", ".dmg", ".pkg", ".deb",
    ".db", ".sqlite",
})


class NextcloudSyncer(CronSyncer):
    """Nextcloud file syncer via WebDAV."""

    source_name = "nextcloud"
    display_name = "Nextcloud"
    description = "Files and documents from Nextcloud via WebDAV"
    category = "files"
    dependencies = {
        "python": ["requests"],
        "cli": [],
        "credentials": [],
        "os": [],
    }
    config_schema = {
        "server": {"type": "str", "default": "", "description": "Nextcloud server URL including protocol", "placeholder": "https://cloud.example.com"},
        "username": {"type": "str", "default": "", "description": "Nextcloud login username", "placeholder": "admin"},
        "token": {"type": "str", "default": "", "description": "Nextcloud app password (generate in Settings > Security)", "sensitive": True, "placeholder": "xxxxx-xxxxx-xxxxx-xxxxx-xxxxx"},
        "max_results": {"type": "int", "default": 200, "description": "Maximum number of files to index per sync cycle", "min": 1, "max": 10000, "placeholder": "200"},
    }

    def __init__(self, store: DataStore, config: dict | None = None):
        config = config or get_source_config("nextcloud")
        super().__init__(store, config)

        self.server = config.get("server", "https://cloud.example.com")
        self.username = config.get("username") or os.environ.get("NEXTCLOUD_USER", "")
        self.token = config.get("token") or os.environ.get("NEXTCLOUD_TOKEN", "")
        self.max_results = config.get("max_results", 200)
        self.content_preview = config.get("content_preview", True)
        self.skip_dirs = set(config.get("skip_dirs", [".Trash", ".versions"]))

        self.dav_url = f"{self.server}/remote.php/dav/files/{self.username}/"

    def fetch_new(self, state: SourceState, limit: int = 1000) -> Iterator[dict]:
        """Fetch recently modified files from Nextcloud."""
        if not self.username or not self.token:
            self.log("Nextcloud credentials not configured (NEXTCLOUD_USER/NEXTCLOUD_TOKEN)")
            return

        last_modified = None
        if state.last_ts:
            try:
                last_modified = datetime.fromisoformat(state.last_ts.replace("Z", "+00:00"))
            except ValueError:
                pass

        # PROPFIND to get file listing
        try:
            files = self._list_files()
        except Exception as e:
            self.log(f"Failed to list files: {e}")
            return

        self.log(f"Found {len(files)} files in Nextcloud")

        # Filter by modification time
        if last_modified:
            files = [f for f in files if f.get("modified") and f["modified"] > last_modified]

        # Sort by modification time, newest last
        files.sort(key=lambda f: f.get("modified") or datetime.min)

        yielded = 0
        for file_info in files:
            if yielded >= limit:
                break

            record = self._file_to_record(file_info)
            if record:
                yield record
                yielded += 1

        self.log(f"Yielded {yielded} file records")

    def _list_files(self) -> list[dict]:
        """PROPFIND to list all files recursively."""
        resp = requests.request(
            "PROPFIND",
            self.dav_url,
            auth=(self.username, self.token),
            headers={
                "Depth": "infinity",
                "Content-Type": "application/xml",
            },
            data=_PROPFIND_BODY,
            timeout=60,
        )
        resp.raise_for_status()

        return self._parse_propfind(resp.text)

    def _parse_propfind(self, xml_text: str) -> list[dict]:
        """Parse PROPFIND XML response into file dicts."""
        root = ET.fromstring(xml_text)
        files = []

        for response in root.findall(f"{{{_DAV_NS}}}response"):
            href_el = response.find(f"{{{_DAV_NS}}}href")
            if href_el is None or href_el.text is None:
                continue

            href = href_el.text
            # Strip the DAV prefix to get relative path
            path = href.split(f"/remote.php/dav/files/{self.username}/", 1)[-1]
            if not path:
                continue  # Root directory

            # Skip configured directories
            if any(path.startswith(d + "/") or path == d for d in self.skip_dirs):
                continue

            propstat = response.find(f"{{{_DAV_NS}}}propstat")
            if propstat is None:
                continue

            prop = propstat.find(f"{{{_DAV_NS}}}prop")
            if prop is None:
                continue

            # Check if it's a collection (directory)
            resource_type = prop.find(f"{{{_DAV_NS}}}resourcetype")
            if resource_type is not None and resource_type.find(f"{{{_DAV_NS}}}collection") is not None:
                continue  # Skip directories

            # Skip by extension
            ext = Path(path).suffix.lower()
            if ext in _SKIP_EXTENSIONS:
                continue

            # Parse properties
            modified = None
            mod_el = prop.find(f"{{{_DAV_NS}}}getlastmodified")
            if mod_el is not None and mod_el.text:
                try:
                    modified = parsedate_to_datetime(mod_el.text)
                except (ValueError, TypeError):
                    pass

            size = 0
            size_el = prop.find(f"{{{_DAV_NS}}}getcontentlength")
            if size_el is not None and size_el.text:
                try:
                    size = int(size_el.text)
                except ValueError:
                    pass

            mime_type = ""
            mime_el = prop.find(f"{{{_DAV_NS}}}getcontenttype")
            if mime_el is not None and mime_el.text:
                mime_type = mime_el.text

            etag = ""
            etag_el = prop.find(f"{{{_DAV_NS}}}getetag")
            if etag_el is not None and etag_el.text:
                etag = etag_el.text.strip('"')

            file_id = ""
            fid_el = prop.find(f"{{{_OC_NS}}}fileid")
            if fid_el is not None and fid_el.text:
                file_id = fid_el.text

            files.append({
                "path": path,
                "name": Path(path).name,
                "mime_type": mime_type,
                "modified": modified,
                "size_bytes": size,
                "etag": etag,
                "file_id": file_id,
            })

        return files

    def _file_to_record(self, file_info: dict) -> dict:
        """Convert file info to vadimgest record."""
        path = file_info["path"]
        path_hash = hashlib.md5(path.encode()).hexdigest()[:12]

        # Optionally fetch content preview
        content_preview = ""
        if self.content_preview and self._is_text_type(file_info.get("mime_type", "")):
            content_preview = self._get_content_preview(path)

        modified = file_info.get("modified")

        return {
            "id": f"nc_{path_hash}",
            "type": "cloud_file",
            "name": file_info["name"],
            "path": f"/{path}",
            "mime_type": file_info.get("mime_type", ""),
            "modified_at": modified.isoformat() if modified else None,
            "size_bytes": file_info.get("size_bytes", 0),
            "content_preview": content_preview,
            "meta": {
                "etag": file_info.get("etag", ""),
                "file_id": file_info.get("file_id", ""),
                "server": self.server,
            },
        }

    def _is_text_type(self, mime_type: str) -> bool:
        """Check if MIME type is text-extractable."""
        if mime_type in _TEXT_MIMES:
            return True
        return any(mime_type.startswith(p) for p in _TEXT_MIME_PREFIXES)

    def _get_content_preview(self, path: str) -> str:
        """Download and extract text preview for a file."""
        url = f"{self.dav_url}{path}"
        try:
            resp = requests.get(
                url,
                auth=(self.username, self.token),
                headers={"Range": f"bytes=0-{_MAX_PREVIEW_BYTES}"},
                timeout=15,
            )
            if resp.status_code in (200, 206):
                return resp.text[:_MAX_PREVIEW_BYTES]
        except Exception:
            pass
        return ""


if __name__ == "__main__":
    from ...store import DataStore
    from ...config import get_data_dir as DATA_DIR_fn

    DATA_DIR = DATA_DIR_fn()
    store = DataStore(DATA_DIR)
    syncer = NextcloudSyncer(store)
    count = syncer.run()
    print(f"Synced {count} records")
