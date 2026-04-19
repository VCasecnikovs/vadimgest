#!/usr/bin/env python3
"""Migrate Signal markdown folders from UUID to proper names.

This script:
1. Exports Signal DB and builds UUID -> name mapping
2. Renames markdown folders from UUID to proper chat names
3. Merges content if target folder already exists
"""

import subprocess
import sqlite3
import shutil
import re
from pathlib import Path

try:
    from vadimgest.config import get_data_dir
    DATA_DIR = get_data_dir()
except ImportError:
    DATA_DIR = Path(__file__).parent.parent / "data"
MARKDOWN_DIR = DATA_DIR / "markdown/signal"
TEMP_DB = Path("/tmp/vadimgest_signal_migrate.db")

# UUID pattern
UUID_PATTERN = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')


def safe_filename(name: str) -> str:
    """Convert name to safe filename."""
    # Replace problematic chars
    safe = re.sub(r'[<>:"/\\|?*]', '_', name)
    safe = safe.replace(' ', '_')
    # Remove leading/trailing dots and spaces
    safe = safe.strip('. ')
    return safe or "unknown"


def get_uuid_to_name_mapping() -> dict[str, str]:
    """Export Signal DB and build UUID -> name mapping."""
    print("Exporting Signal database...")

    if TEMP_DB.exists():
        TEMP_DB.unlink()

    result = subprocess.run(
        ["sigtop", "export-database", str(TEMP_DB)],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise Exception(f"sigtop error: {result.stderr}")

    conn = sqlite3.connect(TEMP_DB)
    conn.row_factory = sqlite3.Row

    mapping = {}
    cursor = conn.execute("""
        SELECT id, name, profileFullName, profileName, type
        FROM conversations
    """)

    for row in cursor:
        conv_id = row["id"]
        # Priority: name > profileFullName > profileName
        display_name = (
            row["name"] or
            row["profileFullName"] or
            row["profileName"]
        )

        if display_name and UUID_PATTERN.match(conv_id):
            mapping[conv_id] = display_name

    conn.close()
    TEMP_DB.unlink()

    return mapping


def migrate_folders(mapping: dict[str, str], dry_run: bool = True):
    """Rename UUID folders to proper names."""
    if not MARKDOWN_DIR.exists():
        print(f"Markdown dir not found: {MARKDOWN_DIR}")
        return

    changes = []

    for folder in MARKDOWN_DIR.iterdir():
        if not folder.is_dir():
            continue

        folder_name = folder.name

        # Check if folder name is UUID
        if not UUID_PATTERN.match(folder_name):
            continue

        # Check if we have a mapping
        if folder_name not in mapping:
            print(f"  No mapping for {folder_name}")
            continue

        new_name = safe_filename(mapping[folder_name])
        new_path = MARKDOWN_DIR / new_name

        changes.append({
            "old": folder,
            "new": new_path,
            "uuid": folder_name,
            "name": mapping[folder_name],
        })

    if not changes:
        print("No folders to migrate")
        return

    print(f"\nFound {len(changes)} folders to migrate:")
    for c in changes:
        exists = " (merge)" if c["new"].exists() else ""
        print(f"  {c['uuid'][:20]}... -> {c['name']}{exists}")

    if dry_run:
        print("\n[DRY RUN] No changes made. Run with --apply to migrate.")
        return

    print("\nMigrating...")
    for c in changes:
        old_path = c["old"]
        new_path = c["new"]

        if new_path.exists():
            # Merge: move files from old to new
            print(f"  Merging {c['uuid'][:20]}... -> {c['name']}")
            for f in old_path.iterdir():
                target = new_path / f.name
                if not target.exists():
                    shutil.move(str(f), str(target))
                else:
                    print(f"    Skip existing: {f.name}")
            # Remove empty old folder
            try:
                old_path.rmdir()
            except OSError:
                print(f"    Could not remove {old_path} (not empty)")
        else:
            # Rename
            print(f"  Renaming {c['uuid'][:20]}... -> {c['name']}")
            old_path.rename(new_path)

    print("\nDone!")


def main():
    import sys

    dry_run = "--apply" not in sys.argv

    print("Signal markdown folder migration")
    print("=" * 40)

    mapping = get_uuid_to_name_mapping()
    print(f"Found {len(mapping)} UUID -> name mappings")

    migrate_folders(mapping, dry_run=dry_run)


if __name__ == "__main__":
    main()
