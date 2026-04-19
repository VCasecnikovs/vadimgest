"""Obsidian Syncer - sync markdown files from Obsidian vault."""

import re
from datetime import datetime
from pathlib import Path
from typing import Iterator

import yaml

# Match [[Target]] and [[Target|Display]] - capture only target
WIKILINK_RE = re.compile(r'\[\[([^\]|#]+?)(?:[|#][^\]]+)?\]\]')

from ..base import CronSyncer
from ....store import DataStore
from ....models import SourceState
from ....config import get_source_config


class ObsidianSyncer(CronSyncer):
    """Obsidian vault syncer."""

    source_name = "obsidian"
    display_name = "Obsidian"
    description = "Markdown notes from Obsidian vault"
    category = "knowledge"
    dependencies = {
        "python": [],
        "cli": [],
        "credentials": [],
        "os": [],
    }
    config_schema = {
        "vault_path": {"type": "path", "default": "~/Documents/Notes", "description": "Absolute path to your Obsidian vault directory", "placeholder": "~/Documents/Notes"},
        "skip_dirs": {"type": "list", "default": [".obsidian", ".trash", ".git"], "description": "Directory names to exclude from scanning (one per line)", "placeholder": ".obsidian\n.trash\n.git"},
        "include_extensions": {"type": "list", "default": [".md"], "description": "Only index files with these extensions", "placeholder": ".md\n.markdown"},
    }

    def __init__(self, store: DataStore, config: dict | None = None):
        config = config or get_source_config("obsidian")
        super().__init__(store, config)

        self.vault_path = Path(
            config.get("vault_path") or Path.home() / "Documents/Notes"
        )
        self.skip_dirs = set(config.get("skip_dirs", [".obsidian", ".trash", ".git", "templates"]))
        self.include_extensions = set(config.get("include_extensions", [".md", ".markdown"]))

    def fetch_new(self, state: SourceState, limit: int = 1000) -> Iterator[dict]:
        """Fetch new/modified documents from Obsidian vault."""
        if not self.vault_path.exists():
            self.log(f"Obsidian vault not found: {self.vault_path}")
            return

        last_mtime = None
        if state.last_ts:
            last_mtime = datetime.fromisoformat(state.last_ts.replace("Z", "+00:00"))

        # Collect files with mtime
        files_with_mtime = []
        for path in self.vault_path.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix.lower() not in self.include_extensions:
                continue
            if self._should_skip(path):
                continue

            mtime = datetime.fromtimestamp(path.stat().st_mtime)
            if last_mtime is None or mtime > last_mtime:
                files_with_mtime.append((path, mtime))

        # Sort by mtime
        files_with_mtime.sort(key=lambda x: x[1])
        self.log(f"Found {len(files_with_mtime)} modified files")

        yielded = 0
        for path, mtime in files_with_mtime:
            if yielded >= limit:
                break

            record = self._file_to_record(path, mtime)
            if record:
                yield record
                yielded += 1

    def _should_skip(self, path: Path) -> bool:
        """Check if path should be skipped."""
        try:
            rel_path = path.relative_to(self.vault_path)
        except ValueError:
            return True

        for part in rel_path.parts:
            if part in self.skip_dirs:
                return True
            # Also skip hidden directories
            if part.startswith("."):
                return True

        return False

    def _extract_links(self, text: str) -> list[str]:
        """Extract unique wikilink targets from text, preserving order."""
        matches = WIKILINK_RE.findall(text)
        seen = set()
        links = []
        for m in matches:
            m = m.strip()
            if m and m not in seen:
                seen.add(m)
                links.append(m)
        return links

    def _extract_frontmatter_links(self, frontmatter: dict | None) -> list[str]:
        """Extract wikilinks from frontmatter string values."""
        if not frontmatter or not isinstance(frontmatter, dict):
            return []
        links = []
        for v in frontmatter.values():
            if isinstance(v, str):
                links.extend(WIKILINK_RE.findall(v))
            elif isinstance(v, list):
                for item in v:
                    if isinstance(item, str):
                        links.extend(WIKILINK_RE.findall(item))
        return [l.strip() for l in links if l.strip()]

    def _file_to_record(self, path: Path, mtime: datetime) -> dict | None:
        """Convert markdown file to record."""
        try:
            content = path.read_text(encoding="utf-8")
        except Exception:
            return None

        # Parse frontmatter
        frontmatter = None
        body = content

        if content.startswith("---"):
            parts = content.split("---", 2)
            if len(parts) >= 3:
                try:
                    frontmatter = yaml.safe_load(parts[1])
                    body = parts[2].strip()
                except Exception:
                    pass

        # Get relative path as ID
        rel_path = path.relative_to(self.vault_path)
        record_id = str(rel_path)

        # Extract title
        title = None
        if frontmatter and isinstance(frontmatter, dict):
            title = frontmatter.get("title")
        if not title:
            title = path.stem

        # Extract wikilinks from body + frontmatter
        body_links = self._extract_links(body)
        fm_links = self._extract_frontmatter_links(frontmatter)
        # Merge, deduplicate, body links first
        seen = set(body_links)
        all_links = list(body_links)
        for l in fm_links:
            if l not in seen:
                seen.add(l)
                all_links.append(l)

        # Limit content size
        max_content_size = 50000
        if len(body) > max_content_size:
            body = body[:max_content_size] + "\n... [truncated]"

        return {
            "id": record_id,
            "type": "document",
            "path": str(path),
            "title": title,
            "modified_at": mtime.isoformat(),
            "frontmatter": frontmatter,
            "content": body,
            "links": all_links,
            "meta": {
                "size_bytes": len(content),
                "folder": str(rel_path.parent) if rel_path.parent != Path(".") else None,
            },
        }


if __name__ == "__main__":
    from ...store import DataStore
    from ...config import get_data_dir as DATA_DIR_fn
    DATA_DIR = DATA_DIR_fn()

    store = DataStore(DATA_DIR)
    syncer = ObsidianSyncer(store)
    count = syncer.run()
    print(f"Synced {count} records")
