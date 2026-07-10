"""FTS5 + vector indexer for Obsidian vault, skills, and vadimgest JSONL."""

import hashlib
import json
import sqlite3
import sys
from pathlib import Path

from ..config import get_search_config


def _configured_search_paths() -> tuple[Path, Path, Path]:
    config = get_search_config()
    return (
        Path(config["vault_path"]),
        Path(config["skills_dir"]),
        Path(config["index_db"]),
    )


DEFAULT_VAULT, DEFAULT_SKILLS_DIR, DEFAULT_DB = _configured_search_paths()
DEFAULT_JSONL_DIR = Path(__file__).parent.parent / "data" / "sources"

SCHEMA_VERSION = 5  # keep at 5 - use ALTER TABLE for new columns
DEFAULT_EMBED_SOURCES = ("obsidian", "skills")


def get_db(db_path: Path = DEFAULT_DB) -> sqlite3.Connection:
    """Open or create the search database."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")

    # Check schema version
    conn.execute("CREATE TABLE IF NOT EXISTS schema_info (key TEXT PRIMARY KEY, value TEXT)")
    row = conn.execute("SELECT value FROM schema_info WHERE key = 'version'").fetchone()
    current = int(row[0]) if row else 0

    if current < SCHEMA_VERSION:
        # Drop old tables and recreate
        conn.execute("DROP TABLE IF EXISTS docs")
        conn.execute("DROP TABLE IF EXISTS meta")
        conn.execute("DROP TABLE IF EXISTS source_state")
        conn.execute("INSERT OR REPLACE INTO schema_info VALUES ('version', ?)", (str(SCHEMA_VERSION),))

    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS docs USING fts5(
            path,
            source,
            title,
            content,
            chat UNINDEXED,
            folder UNINDEXED,
            tokenize='unicode61 remove_diacritics 2'
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            path TEXT PRIMARY KEY,
            source TEXT,
            mtime REAL,
            size INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS source_state (
            source TEXT PRIMARY KEY,
            last_line INTEGER DEFAULT 0,
            byte_offset INTEGER
        )
    """)
    # Migrate: add byte_offset column if missing
    cols = {r[1] for r in conn.execute("PRAGMA table_info(source_state)")}
    if "byte_offset" not in cols:
        conn.execute("ALTER TABLE source_state ADD COLUMN byte_offset INTEGER")
    # Migrate: add content_hash column to meta if missing
    meta_cols = {r[1] for r in conn.execute("PRAGMA table_info(meta)")}
    if "content_hash" not in meta_cols:
        conn.execute("ALTER TABLE meta ADD COLUMN content_hash TEXT")
    conn.commit()
    return conn


def get_vec_db(db_path: Path = DEFAULT_DB):
    """Open DB with sqlite-vec extension for vector search."""
    import sqlite_vec

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))

    if not hasattr(conn, "enable_load_extension"):
        conn.close()
        raise RuntimeError(
            "Python's sqlite3 was compiled without extension loading support. "
            "Use a Python build with --enable-loadable-sqlite-extensions "
            "(e.g. /opt/homebrew/bin/python3 on macOS)."
        )

    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    conn.enable_load_extension(False)
    conn.execute("PRAGMA journal_mode=WAL")

    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS vec_docs USING vec0(
            doc_id integer primary key,
            embedding float[768]
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS vec_meta(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


def _extract_title(text: str, path: Path) -> str:
    """Extract title from markdown: first # heading or filename."""
    for line in text.split("\n", 20):
        line = line.strip()
        if line.startswith("# "):
            return line[2:].strip()
        if line == "---":
            continue
    return path.stem


def _extract_jsonl_text(record: dict) -> tuple[str, str]:
    """Extract (title, searchable_text) from a JSONL record."""
    rtype = record.get("type", "")

    if rtype == "conversation":
        chat = record.get("chat", "unknown")
        folder = record.get("folder", "")
        period = record.get("period_end", "")[:10] if record.get("period_end") else ""
        title = f"{folder}/{chat} {period}".strip()
        lines = []
        for m in record.get("messages", []):
            sender = m.get("sender", "")
            text = m.get("text") or ""
            if text:
                lines.append(f"{sender}: {text}")
        return title, "\n".join(lines)

    if rtype == "message":
        sender = record.get("sender", "")
        chat = record.get("chat", "")
        text = record.get("text") or ""
        return f"{chat} - {sender}", text

    if rtype == "meeting":
        title = record.get("title", "Meeting")
        parts = []
        if record.get("notes"):
            parts.append(record["notes"])
        if record.get("transcript"):
            parts.append(record["transcript"])
        return title, "\n".join(parts)

    if rtype == "email":
        subject = record.get("subject", "")
        body = record.get("body") or ""
        return subject, body

    if rtype == "issue":
        title = record.get("title", "")
        body = record.get("body") or ""
        return f"#{record.get('number', '')} {title}", body

    if rtype == "task":
        title = record.get("title", "")
        notes = record.get("notes") or ""
        return title, notes

    if rtype == "activity":
        title = record.get("title", "")
        summary = record.get("summary") or ""
        return title, summary

    if rtype == "document":
        title = record.get("title", "")
        content = record.get("content") or ""
        return title, content

    # Fallback: stringify the whole record
    title = record.get("title") or record.get("subject") or record.get("chat") or rtype
    return title, json.dumps(record, ensure_ascii=False)


def _extract_jsonl_meta(record: dict) -> tuple[str, str]:
    """Extract (chat, folder) metadata from a JSONL record."""
    return record.get("chat") or "", record.get("folder") or ""


def index_obsidian(conn: sqlite3.Connection, vault: Path) -> dict:
    """Index Obsidian vault .md files. Returns stats."""
    vault = vault.expanduser().resolve()
    existing = {}
    for row in conn.execute("SELECT path, mtime FROM meta WHERE source = 'obsidian'"):
        existing[row[0]] = row[1]

    md_files = list(vault.rglob("*.md"))
    current_paths = set()
    added = updated = unchanged = 0

    for md_file in md_files:
        rel = str(md_file.relative_to(vault))
        path_key = f"obsidian:{rel}"
        current_paths.add(path_key)
        mtime = md_file.stat().st_mtime

        if path_key in existing and abs(existing[path_key] - mtime) < 0.01:
            unchanged += 1
            continue

        try:
            text = md_file.read_text(errors="replace")
        except OSError:
            continue

        title = _extract_title(text, md_file)
        folder = str(md_file.relative_to(vault).parent)
        if folder == ".":
            folder = ""

        if path_key in existing:
            conn.execute("DELETE FROM docs WHERE path = ?", (path_key,))
            updated += 1
        else:
            added += 1

        conn.execute(
            "INSERT INTO docs (path, source, title, content, chat, folder) VALUES (?, 'obsidian', ?, ?, '', ?)",
            (path_key, title, text, folder)
        )
        conn.execute(
            "INSERT OR REPLACE INTO meta (path, source, mtime, size) VALUES (?, 'obsidian', ?, ?)",
            (path_key, mtime, len(text))
        )

    # Remove deleted files
    deleted = set(existing.keys()) - current_paths
    for dp in deleted:
        conn.execute("DELETE FROM docs WHERE path = ?", (dp,))
        conn.execute("DELETE FROM meta WHERE path = ?", (dp,))

    return {"total": len(md_files), "added": added, "updated": updated,
            "unchanged": unchanged, "removed": len(deleted)}


def index_skills(conn: sqlite3.Connection, skills_dir: Path = DEFAULT_SKILLS_DIR) -> dict:
    """Index SKILL.md files from skills directory."""
    skills_dir = skills_dir.expanduser().resolve()
    if not skills_dir.exists():
        return {"total": 0, "added": 0, "unchanged": 0, "removed": 0}

    existing = {}
    for row in conn.execute("SELECT path, mtime FROM meta WHERE source = 'skills'"):
        existing[row[0]] = row[1]

    skill_files = list(skills_dir.rglob("SKILL.md"))
    current_paths = set()
    added = updated = unchanged = 0

    for skill_file in skill_files:
        rel = str(skill_file.relative_to(skills_dir))
        path_key = f"skills:{rel}"
        current_paths.add(path_key)
        mtime = skill_file.stat().st_mtime

        if path_key in existing and abs(existing[path_key] - mtime) < 0.01:
            unchanged += 1
            continue

        try:
            text = skill_file.read_text(errors="replace")
        except OSError:
            continue

        # Strip YAML frontmatter
        text_body = text
        if text.startswith("---"):
            end = text.find("---", 3)
            if end > 0:
                text_body = text[end + 3:].strip()

        title = skill_file.parent.name  # folder name = skill name
        folder = str(skill_file.relative_to(skills_dir).parent)

        if path_key in existing:
            conn.execute("DELETE FROM docs WHERE path = ?", (path_key,))
            updated += 1
        else:
            added += 1

        conn.execute(
            "INSERT INTO docs (path, source, title, content, chat, folder) VALUES (?, 'skills', ?, ?, '', ?)",
            (path_key, title, text_body, folder)
        )
        conn.execute(
            "INSERT OR REPLACE INTO meta (path, source, mtime, size) VALUES (?, 'skills', ?, ?)",
            (path_key, mtime, len(text_body))
        )

    deleted = set(existing.keys()) - current_paths
    for dp in deleted:
        conn.execute("DELETE FROM docs WHERE path = ?", (dp,))
        conn.execute("DELETE FROM meta WHERE path = ?", (dp,))

    return {"total": len(skill_files), "added": added, "updated": updated,
            "unchanged": unchanged, "removed": len(deleted)}


def _content_hash(text: str) -> str:
    """Short hash for change detection."""
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def index_embeddings(db_path: Path = DEFAULT_DB, provider: str = "gemini",
                     batch_size: int = 10, limit: int | None = None,
                     rebuild: bool = False,
                     sources: tuple[str, ...] | None = None) -> dict:
    """Generate embeddings for docs that don't have them yet.

    Uses content hash to skip unchanged docs. Writes to vec_docs table.
    """
    from .embedder import get_embedder, Embedder

    embedder = get_embedder(provider)

    # Use single pysqlite3 connection for everything (avoids WAL lock conflicts)
    conn = get_vec_db(db_path)

    model = getattr(embedder, "_MODEL", None) or getattr(embedder, "model", None)
    model = model or embedder.__class__.__name__
    desired_space = f"{provider}:{model}:{embedder.dim}"
    current_row = conn.execute(
        "SELECT value FROM vec_meta WHERE key = 'embedding_space'"
    ).fetchone()
    current_space = current_row[0] if current_row else None
    vector_count = conn.execute("SELECT COUNT(*) FROM vec_docs").fetchone()[0]
    requested_sources = tuple(dict.fromkeys(s for s in (sources or DEFAULT_EMBED_SOURCES) if s))
    if not requested_sources:
        conn.close()
        raise ValueError("at least one embedding source is required")
    sources_row = conn.execute(
        "SELECT value FROM vec_meta WHERE key = 'embedding_sources'"
    ).fetchone()
    persisted_sources = tuple(json.loads(sources_row[0])) if sources_row else ()

    if vector_count and current_space != desired_space:
        if not rebuild:
            conn.close()
            actual = current_space or "unknown legacy space"
            raise RuntimeError(
                f"embedding space mismatch: index={actual}, requested={desired_space}. "
                "Re-run with rebuild=True / --rebuild to replace all vectors."
            )
        conn.execute("DELETE FROM vec_docs")
        vector_count = 0
        persisted_sources = ()
    elif rebuild and vector_count:
        conn.execute("DELETE FROM vec_docs")
        vector_count = 0
        persisted_sources = ()

    # Corpus scope only expands during incremental runs. This prevents a legacy
    # md-only refresh from silently deleting vectors for critical raw sources.
    active_sources = tuple(dict.fromkeys((*persisted_sources, *requested_sources)))

    conn.execute(
        "INSERT OR REPLACE INTO vec_meta(key, value) VALUES ('embedding_space', ?)",
        (desired_space,),
    )
    conn.execute(
        "INSERT OR REPLACE INTO vec_meta(key, value) VALUES ('embedding_sources', ?)",
        (json.dumps(active_sources),),
    )
    conn.commit()

    # Get existing embeddings
    existing_vec = set()
    try:
        for row in conn.execute("SELECT doc_id FROM vec_docs"):
            existing_vec.add(row[0])
    except Exception:
        pass

    placeholders = ','.join('?' * len(active_sources))
    rows = conn.execute(f"""
        SELECT docs.rowid, docs.path, docs.source, docs.content, meta.content_hash
        FROM docs JOIN meta ON docs.path = meta.path
        WHERE docs.source IN ({placeholders})
        ORDER BY docs.rowid
    """, active_sources).fetchall()

    valid_vec_ids = {row[0] for row in rows}
    stale_vec_ids = existing_vec - valid_vec_ids
    for doc_id in stale_vec_ids:
        conn.execute("DELETE FROM vec_docs WHERE doc_id = ?", (doc_id,))
    if stale_vec_ids:
        conn.commit()
        existing_vec -= stale_vec_ids
    pruned = len(stale_vec_ids)

    to_embed = []
    for rowid, path, source, content, old_hash in rows:
        if limit and len(to_embed) >= limit:
            break
        h = _content_hash(content)
        if rowid in existing_vec and old_hash == h:
            continue
        to_embed.append((rowid, path, source, content, h))

    if not to_embed:
        conn.close()
        return {
            "total": len(rows),
            "embedded": 0,
            "skipped": len(rows),
            "pruned": pruned,
            "embedding_space": desired_space,
            "sources": list(active_sources),
        }

    embedded = 0
    for i in range(0, len(to_embed), batch_size):
        batch = to_embed[i:i + batch_size]
        texts = [item[3][:8000] for item in batch]

        try:
            vectors = embedder.embed(texts)
        except Exception as e:
            print(f"  Embedding error at batch {i}: {e}", file=sys.stderr)
            continue

        for (rowid, path, source, content, h), vec in zip(batch, vectors):
            blob = Embedder.serialize(vec)
            conn.execute("DELETE FROM vec_docs WHERE doc_id = ?", (rowid,))
            conn.execute(
                "INSERT INTO vec_docs(doc_id, embedding) VALUES (?, ?)",
                (rowid, blob)
            )
            conn.execute(
                "UPDATE meta SET content_hash = ? WHERE path = ?", (h, path)
            )

        embedded += len(batch)
        if embedded % 200 == 0 or i + batch_size >= len(to_embed):
            print(f"  Embedded {embedded}/{len(to_embed)}...", file=sys.stderr, flush=True)

        conn.commit()

    conn.close()
    return {
        "total": len(rows),
        "embedded": embedded,
        "skipped": len(rows) - len(to_embed),
        "pruned": pruned,
        "embedding_space": desired_space,
        "sources": list(active_sources),
    }


def embed_stats(db_path: Path = DEFAULT_DB) -> dict:
    """Get embedding coverage stats."""
    if not db_path.exists():
        return {"total_docs": 0, "embedded": 0, "coverage": 0}

    conn = get_db(db_path)
    total = conn.execute("SELECT COUNT(*) FROM docs").fetchone()[0]
    conn.close()

    try:
        conn_vec = get_vec_db(db_path)
        embedded = conn_vec.execute("SELECT COUNT(*) FROM vec_docs").fetchone()[0]
        row = conn_vec.execute(
            "SELECT value FROM vec_meta WHERE key = 'embedding_space'"
        ).fetchone()
        embedding_space = row[0] if row else None
        source_row = conn_vec.execute(
            "SELECT value FROM vec_meta WHERE key = 'embedding_sources'"
        ).fetchone()
        embedding_sources = json.loads(source_row[0]) if source_row else []
        source_counts = dict(conn_vec.execute("""
            SELECT docs.source, COUNT(*)
            FROM vec_docs JOIN docs ON docs.rowid = vec_docs.doc_id
            GROUP BY docs.source
            ORDER BY docs.source
        """).fetchall())
        conn_vec.close()
    except Exception:
        embedded = 0
        embedding_space = None
        embedding_sources = []
        source_counts = {}

    pct = (embedded / total * 100) if total > 0 else 0
    return {
        "total_docs": total,
        "embedded": embedded,
        "coverage": round(pct, 1),
        "embedding_space": embedding_space,
        "embedding_sources": embedding_sources,
        "source_counts": source_counts,
    }


def _count_lines(path: Path) -> int:
    """Fast line count using raw binary read."""
    count = 0
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):  # 1MB chunks
            count += chunk.count(b"\n")
    return count


def index_jsonl(conn: sqlite3.Connection, source: str, jsonl_path: Path) -> dict:
    """Index a JSONL source file. Returns stats."""
    if not jsonl_path.exists():
        return {"total": 0, "added": 0, "skipped": 0}

    # Get last processed byte offset and line number
    row = conn.execute(
        "SELECT last_line, byte_offset FROM source_state WHERE source = ?", (source,)
    ).fetchone()
    last_line = row[0] if row else 0
    byte_offset = row[1] if row and row[1] is not None else None

    added = 0
    total = 0
    line_num = last_line

    total = _count_lines(jsonl_path)

    with open(jsonl_path, "rb") as f:
        # Seek to last known byte offset if available
        if byte_offset is not None and byte_offset > 0:
            f.seek(byte_offset)
        elif last_line > 0:
            # No byte offset - skip lines the old way (one-time migration)
            for _ in range(last_line):
                f.readline()

        for raw_line in f:
            line_stripped = raw_line.strip()
            if not line_stripped:
                line_num += 1
                continue

            try:
                record = json.loads(line_stripped)
            except (json.JSONDecodeError, ValueError):
                line_num += 1
                continue

            title, text = _extract_jsonl_text(record)
            if not text or len(text) < 5:
                line_num += 1
                continue

            chat, folder = _extract_jsonl_meta(record)
            path_key = f"{source}:{line_num}"
            conn.execute(
                "INSERT INTO docs (path, source, title, content, chat, folder) VALUES (?, ?, ?, ?, ?, ?)",
                (path_key, source, title, text, chat, folder)
            )
            conn.execute(
                "INSERT OR REPLACE INTO meta (path, source, mtime, size) VALUES (?, ?, ?, ?)",
                (path_key, source, 0, len(text))
            )
            added += 1
            line_num += 1

        new_byte_offset = f.tell()

    skipped = total - added

    # Save position with byte offset
    conn.execute(
        "INSERT OR REPLACE INTO source_state (source, last_line, byte_offset) VALUES (?, ?, ?)",
        (source, line_num, new_byte_offset)
    )

    return {"total": total, "added": added, "skipped": skipped}


def reindex_stale(db_path: Path = DEFAULT_DB, jsonl_dir: Path = DEFAULT_JSONL_DIR) -> dict:
    """Fast incremental reindex: only JSONL sources with new data since last index.

    Compares each JSONL file size against stored byte_offset.
    Only reads new lines - typically <100ms for a few hundred new records.
    """
    if not db_path.exists() or not jsonl_dir.exists():
        return {}

    conn = get_db(db_path)
    results = {}

    for jsonl_file in sorted(jsonl_dir.glob("*.jsonl")):
        source = jsonl_file.stem
        if source in ("obsidian", "skills"):
            continue

        file_size = jsonl_file.stat().st_size
        row = conn.execute(
            "SELECT byte_offset FROM source_state WHERE source = ?", (source,)
        ).fetchone()
        stored_offset = row[0] if row and row[0] is not None else 0

        if file_size > stored_offset:
            r = index_jsonl(conn, source, jsonl_file)
            if r.get("added", 0) > 0:
                results[source] = r

    if results:
        conn.commit()
    conn.close()
    return results


def index(vault: Path = DEFAULT_VAULT, jsonl_dir: Path = DEFAULT_JSONL_DIR,
          db_path: Path = DEFAULT_DB, rebuild: bool = False,
          exclude: set[str] | None = None,
          skills_dir: Path = DEFAULT_SKILLS_DIR,
          md_only: bool = False) -> dict:
    """Index all sources. Returns stats dict."""
    conn = get_db(db_path)

    if exclude is None:
        exclude = set()

    if rebuild:
        conn.execute("DELETE FROM docs")
        conn.execute("DELETE FROM meta")
        conn.execute("DELETE FROM source_state")
        conn.commit()

    results = {}

    # Obsidian vault
    if "obsidian" not in exclude:
        results["obsidian"] = index_obsidian(conn, vault)

    # Skills (.md files from skills dir)
    if "skills" not in exclude:
        results["skills"] = index_skills(conn, skills_dir)

    # JSONL sources (skip obsidian and skills - indexed from files directly)
    jsonl_skip = exclude | {"obsidian", "skills"}
    if not md_only and jsonl_dir.exists():
        for jsonl_file in sorted(jsonl_dir.glob("*.jsonl")):
            source = jsonl_file.stem
            if source in jsonl_skip:
                continue
            results[source] = index_jsonl(conn, source, jsonl_file)

    conn.commit()
    conn.close()
    return results


def stats(db_path: Path = DEFAULT_DB) -> dict:
    """Get index statistics per source."""
    if not db_path.exists():
        return {"sources": {}, "total": 0, "db_size": 0}

    conn = get_db(db_path)
    rows = conn.execute("SELECT source, COUNT(*) FROM meta GROUP BY source").fetchall()
    conn.close()

    sources = {r[0]: r[1] for r in rows}
    return {
        "sources": sources,
        "total": sum(sources.values()),
        "db_size": db_path.stat().st_size,
    }
