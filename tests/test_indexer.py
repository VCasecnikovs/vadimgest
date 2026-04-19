"""Tests for vadimgest/search/indexer.py - FTS5 indexing for Obsidian, skills, and JSONL."""

import hashlib
import json
import sqlite3
import pytest
from pathlib import Path

from vadimgest.search.indexer import (
    _extract_title,
    _extract_jsonl_text,
    _extract_jsonl_meta,
    _content_hash,
    _count_lines,
    get_db,
    index,
    index_obsidian,
    index_skills,
    index_jsonl,
    stats,
    SCHEMA_VERSION,
)


# ── _extract_title ──


class TestExtractTitle:
    def test_h1_heading(self, tmp_path):
        text = "# My Title\nSome content"
        assert _extract_title(text, tmp_path / "note.md") == "My Title"

    def test_h1_with_leading_frontmatter(self, tmp_path):
        text = "---\ntags: test\n---\n# Real Title\nContent"
        assert _extract_title(text, tmp_path / "note.md") == "Real Title"

    def test_falls_back_to_filename_stem(self, tmp_path):
        text = "No heading here, just content\nMore lines"
        assert _extract_title(text, tmp_path / "my-note.md") == "my-note"

    def test_empty_content(self, tmp_path):
        assert _extract_title("", tmp_path / "empty.md") == "empty"

    def test_only_frontmatter_dashes(self, tmp_path):
        text = "---\n---\nno heading"
        assert _extract_title(text, tmp_path / "fallback.md") == "fallback"

    def test_h1_with_extra_spaces(self, tmp_path):
        text = "#   Spaced Title  \nContent"
        assert _extract_title(text, tmp_path / "x.md") == "Spaced Title"

    def test_h2_not_matched(self, tmp_path):
        """Only # headings are matched, not ##."""
        text = "## H2 Title\nContent"
        assert _extract_title(text, tmp_path / "h2.md") == "h2"

    def test_heading_beyond_20_lines(self, tmp_path):
        """_extract_title only checks first ~20 lines."""
        lines = ["line"] * 25 + ["# Late Title"]
        text = "\n".join(lines)
        assert _extract_title(text, tmp_path / "late.md") == "late"


# ── _extract_jsonl_text ──


class TestExtractJsonlText:
    def test_conversation(self):
        record = {
            "type": "conversation",
            "chat": "Dev Chat",
            "folder": "Work",
            "period_end": "2026-03-15T12:00:00",
            "messages": [
                {"sender": "Alice", "text": "Hello"},
                {"sender": "Bob", "text": "Hi there"},
            ],
        }
        title, text = _extract_jsonl_text(record)
        assert "Dev Chat" in title
        assert "Work" in title
        assert "2026-03-15" in title
        assert "Alice: Hello" in text
        assert "Bob: Hi there" in text

    def test_conversation_empty_messages(self):
        record = {"type": "conversation", "chat": "Empty", "messages": []}
        title, text = _extract_jsonl_text(record)
        assert "Empty" in title
        assert text == ""

    def test_conversation_message_no_text(self):
        record = {
            "type": "conversation",
            "chat": "test",
            "messages": [{"sender": "X", "text": None}],
        }
        _, text = _extract_jsonl_text(record)
        assert text == ""

    def test_message(self):
        record = {"type": "message", "sender": "John", "chat": "Group", "text": "Hey"}
        title, text = _extract_jsonl_text(record)
        assert title == "Group - John"
        assert text == "Hey"

    def test_message_empty_text(self):
        record = {"type": "message", "sender": "X", "chat": "Y", "text": None}
        _, text = _extract_jsonl_text(record)
        assert text == ""

    def test_meeting(self):
        record = {
            "type": "meeting",
            "title": "Standup",
            "notes": "Discussed X",
            "transcript": "Full transcript",
        }
        title, text = _extract_jsonl_text(record)
        assert title == "Standup"
        assert "Discussed X" in text
        assert "Full transcript" in text

    def test_meeting_only_notes(self):
        record = {"type": "meeting", "title": "Call", "notes": "Notes only"}
        title, text = _extract_jsonl_text(record)
        assert title == "Call"
        assert text == "Notes only"

    def test_meeting_no_notes_no_transcript(self):
        record = {"type": "meeting", "title": "Empty Meeting"}
        _, text = _extract_jsonl_text(record)
        assert text == ""

    def test_email(self):
        record = {"type": "email", "subject": "Re: Proposal", "body": "Thanks"}
        title, text = _extract_jsonl_text(record)
        assert title == "Re: Proposal"
        assert text == "Thanks"

    def test_issue(self):
        record = {"type": "issue", "number": 42, "title": "Bug fix", "body": "Details"}
        title, text = _extract_jsonl_text(record)
        assert "#42" in title
        assert "Bug fix" in title
        assert text == "Details"

    def test_task(self):
        record = {"type": "task", "title": "Do thing", "notes": "Notes here"}
        title, text = _extract_jsonl_text(record)
        assert title == "Do thing"
        assert text == "Notes here"

    def test_activity(self):
        record = {"type": "activity", "title": "Coding", "summary": "Wrote code"}
        title, text = _extract_jsonl_text(record)
        assert title == "Coding"
        assert text == "Wrote code"

    def test_document(self):
        record = {"type": "document", "title": "Doc", "content": "Some content"}
        title, text = _extract_jsonl_text(record)
        assert title == "Doc"
        assert text == "Some content"

    def test_fallback_unknown_type(self):
        record = {"type": "custom", "title": "Custom Thing", "data": "xyz"}
        title, text = _extract_jsonl_text(record)
        assert title == "Custom Thing"
        assert "xyz" in text  # JSON-stringified

    def test_fallback_no_title(self):
        record = {"type": "unknown", "chat": "FallbackChat"}
        title, text = _extract_jsonl_text(record)
        assert title == "FallbackChat"

    def test_fallback_no_title_no_chat(self):
        record = {"type": "weird"}
        title, text = _extract_jsonl_text(record)
        assert title == "weird"  # falls back to type

    def test_conversation_no_period_end(self):
        record = {"type": "conversation", "chat": "X", "messages": []}
        title, _ = _extract_jsonl_text(record)
        assert "X" in title


# ── _extract_jsonl_meta ──


class TestExtractJsonlMeta:
    def test_with_chat_and_folder(self):
        record = {"chat": "Dev", "folder": "Work"}
        chat, folder = _extract_jsonl_meta(record)
        assert chat == "Dev"
        assert folder == "Work"

    def test_empty_record(self):
        chat, folder = _extract_jsonl_meta({})
        assert chat == ""
        assert folder == ""

    def test_none_values(self):
        record = {"chat": None, "folder": None}
        chat, folder = _extract_jsonl_meta(record)
        assert chat == ""
        assert folder == ""


# ── _content_hash ──


class TestContentHash:
    def test_deterministic(self):
        h1 = _content_hash("hello world")
        h2 = _content_hash("hello world")
        assert h1 == h2

    def test_different_content_different_hash(self):
        assert _content_hash("abc") != _content_hash("def")

    def test_length_16(self):
        assert len(_content_hash("anything")) == 16

    def test_matches_sha256_prefix(self):
        text = "test content"
        expected = hashlib.sha256(text.encode()).hexdigest()[:16]
        assert _content_hash(text) == expected


# ── _count_lines ──


class TestCountLines:
    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.txt"
        f.write_bytes(b"")
        assert _count_lines(f) == 0

    def test_single_line_no_newline(self, tmp_path):
        f = tmp_path / "one.txt"
        f.write_bytes(b"hello")
        assert _count_lines(f) == 0

    def test_single_line_with_newline(self, tmp_path):
        f = tmp_path / "one.txt"
        f.write_bytes(b"hello\n")
        assert _count_lines(f) == 1

    def test_multiple_lines(self, tmp_path):
        f = tmp_path / "multi.txt"
        f.write_bytes(b"line1\nline2\nline3\n")
        assert _count_lines(f) == 3

    def test_large_content(self, tmp_path):
        """Test with content larger than the 1MB chunk size."""
        f = tmp_path / "large.txt"
        # 2000 lines of 1000 chars each
        content = (b"x" * 999 + b"\n") * 2000
        f.write_bytes(content)
        assert _count_lines(f) == 2000


# ── get_db ──


class TestGetDb:
    def test_creates_db_and_tables(self, tmp_path):
        db_path = tmp_path / "subdir" / "test.db"
        conn = get_db(db_path)
        assert db_path.exists()

        # Check tables exist
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'table')"
        ).fetchall()}
        assert "schema_info" in tables
        assert "meta" in tables
        assert "source_state" in tables

        # Check FTS5 virtual table
        vtables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()}
        assert "docs" in vtables

        conn.close()

    def test_schema_version_stored(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = get_db(db_path)
        row = conn.execute("SELECT value FROM schema_info WHERE key = 'version'").fetchone()
        assert int(row[0]) == SCHEMA_VERSION
        conn.close()

    def test_wal_mode(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = get_db(db_path)
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"
        conn.close()

    def test_idempotent_open(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn1 = get_db(db_path)
        conn1.execute(
            "INSERT INTO meta (path, source, mtime, size) VALUES (?, ?, ?, ?)",
            ("test:1", "test", 0.0, 10),
        )
        conn1.commit()
        conn1.close()

        conn2 = get_db(db_path)
        row = conn2.execute("SELECT path FROM meta WHERE path = 'test:1'").fetchone()
        assert row is not None
        conn2.close()

    def test_byte_offset_column_exists(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = get_db(db_path)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(source_state)")}
        assert "byte_offset" in cols
        conn.close()

    def test_content_hash_column_exists(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = get_db(db_path)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(meta)")}
        assert "content_hash" in cols
        conn.close()


# ── index_obsidian ──


class TestIndexObsidian:
    def test_indexes_md_files(self, tmp_path):
        vault = tmp_path / "vault"
        vault.mkdir()
        (vault / "note1.md").write_text("# Title One\nContent here")
        (vault / "sub").mkdir()
        (vault / "sub" / "note2.md").write_text("# Title Two\nMore content")

        db_path = tmp_path / "test.db"
        conn = get_db(db_path)
        result = index_obsidian(conn, vault)
        conn.commit()

        assert result["total"] == 2
        assert result["added"] == 2
        assert result["unchanged"] == 0

        # Verify docs are searchable
        rows = conn.execute("SELECT path, title FROM docs WHERE source = 'obsidian'").fetchall()
        paths = {r[0] for r in rows}
        assert "obsidian:note1.md" in paths
        assert "obsidian:sub/note2.md" in paths

        conn.close()

    def test_skips_unchanged_files(self, tmp_path):
        vault = tmp_path / "vault"
        vault.mkdir()
        (vault / "note.md").write_text("# Title\nContent")

        db_path = tmp_path / "test.db"
        conn = get_db(db_path)

        r1 = index_obsidian(conn, vault)
        conn.commit()
        assert r1["added"] == 1

        r2 = index_obsidian(conn, vault)
        conn.commit()
        assert r2["unchanged"] == 1
        assert r2["added"] == 0

        conn.close()

    def test_detects_deleted_files(self, tmp_path):
        vault = tmp_path / "vault"
        vault.mkdir()
        note = vault / "to_delete.md"
        note.write_text("# Delete Me")

        db_path = tmp_path / "test.db"
        conn = get_db(db_path)

        index_obsidian(conn, vault)
        conn.commit()

        note.unlink()
        r = index_obsidian(conn, vault)
        conn.commit()

        assert r["removed"] == 1

        rows = conn.execute("SELECT * FROM docs WHERE path = 'obsidian:to_delete.md'").fetchall()
        assert len(rows) == 0

        conn.close()

    def test_folder_extraction(self, tmp_path):
        vault = tmp_path / "vault"
        (vault / "People").mkdir(parents=True)
        (vault / "People" / "John.md").write_text("# John\nBio")

        db_path = tmp_path / "test.db"
        conn = get_db(db_path)
        index_obsidian(conn, vault)
        conn.commit()

        row = conn.execute(
            "SELECT folder FROM docs WHERE path = 'obsidian:People/John.md'"
        ).fetchone()
        assert row[0] == "People"
        conn.close()

    def test_root_folder_is_empty(self, tmp_path):
        vault = tmp_path / "vault"
        vault.mkdir()
        (vault / "root.md").write_text("# Root Note")

        db_path = tmp_path / "test.db"
        conn = get_db(db_path)
        index_obsidian(conn, vault)
        conn.commit()

        row = conn.execute(
            "SELECT folder FROM docs WHERE path = 'obsidian:root.md'"
        ).fetchone()
        assert row[0] == ""
        conn.close()


# ── index_skills ──


class TestIndexSkills:
    def test_indexes_skill_files(self, tmp_path):
        skills = tmp_path / "skills"
        skill1 = skills / "my-skill"
        skill1.mkdir(parents=True)
        (skill1 / "SKILL.md").write_text("---\nname: test\n---\n# My Skill\nDoes stuff")

        db_path = tmp_path / "test.db"
        conn = get_db(db_path)
        result = index_skills(conn, skills)
        conn.commit()

        assert result["total"] == 1
        assert result["added"] == 1

        row = conn.execute(
            "SELECT title, content FROM docs WHERE source = 'skills'"
        ).fetchone()
        assert row[0] == "my-skill"  # folder name as title
        assert "# My Skill" in row[1]
        assert "---" not in row[1]  # frontmatter stripped

        conn.close()

    def test_nonexistent_skills_dir(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = get_db(db_path)
        result = index_skills(conn, tmp_path / "nonexistent")
        assert result["total"] == 0
        conn.close()

    def test_strips_yaml_frontmatter(self, tmp_path):
        skills = tmp_path / "skills" / "s1"
        skills.mkdir(parents=True)
        (skills / "SKILL.md").write_text("---\nkey: value\n---\nBody content here")

        db_path = tmp_path / "test.db"
        conn = get_db(db_path)
        index_skills(conn, tmp_path / "skills")
        conn.commit()

        row = conn.execute("SELECT content FROM docs WHERE source = 'skills'").fetchone()
        assert row[0] == "Body content here"
        assert "key: value" not in row[0]
        conn.close()


# ── index_jsonl ──


class TestIndexJsonl:
    def _make_jsonl(self, path, records):
        with open(path, "w") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")

    def test_indexes_records(self, tmp_path):
        jsonl = tmp_path / "telegram.jsonl"
        self._make_jsonl(jsonl, [
            {"type": "message", "sender": "Alice", "chat": "Dev", "text": "Hello world from Alice"},
            {"type": "message", "sender": "Bob", "chat": "Dev", "text": "Hi from Bob, how are you?"},
        ])

        db_path = tmp_path / "test.db"
        conn = get_db(db_path)
        result = index_jsonl(conn, "telegram", jsonl)
        conn.commit()

        assert result["added"] == 2
        assert result["total"] == 2

        rows = conn.execute("SELECT title, content, chat FROM docs WHERE source = 'telegram'").fetchall()
        assert len(rows) == 2
        conn.close()

    def test_skips_short_text(self, tmp_path):
        jsonl = tmp_path / "test.jsonl"
        self._make_jsonl(jsonl, [
            {"type": "message", "sender": "X", "chat": "Y", "text": "Hi"},  # too short (<5 chars)
            {"type": "message", "sender": "X", "chat": "Y", "text": "Hello there!"},
        ])

        db_path = tmp_path / "test.db"
        conn = get_db(db_path)
        result = index_jsonl(conn, "test", jsonl)
        conn.commit()

        assert result["added"] == 1
        conn.close()

    def test_skips_invalid_json(self, tmp_path):
        jsonl = tmp_path / "bad.jsonl"
        jsonl.write_text('{"type": "message", "text": "valid line yes"}\nnot json\n{"type": "message", "text": "also valid text"}\n')

        db_path = tmp_path / "test.db"
        conn = get_db(db_path)
        result = index_jsonl(conn, "bad", jsonl)
        conn.commit()

        assert result["added"] == 2
        conn.close()

    def test_nonexistent_file(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = get_db(db_path)
        result = index_jsonl(conn, "missing", tmp_path / "nope.jsonl")
        assert result["total"] == 0
        assert result["added"] == 0
        conn.close()

    def test_incremental_indexing(self, tmp_path):
        """Second call should only process new lines."""
        jsonl = tmp_path / "inc.jsonl"
        self._make_jsonl(jsonl, [
            {"type": "message", "sender": "A", "chat": "C", "text": "First message here"},
        ])

        db_path = tmp_path / "test.db"
        conn = get_db(db_path)
        r1 = index_jsonl(conn, "inc", jsonl)
        conn.commit()
        assert r1["added"] == 1

        # Append more records
        with open(jsonl, "a") as f:
            f.write(json.dumps({"type": "message", "sender": "B", "chat": "C", "text": "Second message here"}) + "\n")

        r2 = index_jsonl(conn, "inc", jsonl)
        conn.commit()
        assert r2["added"] == 1  # only the new line

        total = conn.execute("SELECT COUNT(*) FROM docs WHERE source = 'inc'").fetchone()[0]
        assert total == 2
        conn.close()

    def test_saves_source_state(self, tmp_path):
        jsonl = tmp_path / "state.jsonl"
        self._make_jsonl(jsonl, [
            {"type": "message", "sender": "X", "chat": "Y", "text": "State test message"},
        ])

        db_path = tmp_path / "test.db"
        conn = get_db(db_path)
        index_jsonl(conn, "state", jsonl)
        conn.commit()

        row = conn.execute(
            "SELECT last_line, byte_offset FROM source_state WHERE source = 'state'"
        ).fetchone()
        assert row is not None
        assert row[0] >= 1  # at least 1 line processed
        assert row[1] > 0  # byte offset saved
        conn.close()

    def test_empty_lines_skipped(self, tmp_path):
        jsonl = tmp_path / "blanks.jsonl"
        jsonl.write_text('{"type": "message", "text": "valid text here"}\n\n\n{"type": "message", "text": "another valid text"}\n')

        db_path = tmp_path / "test.db"
        conn = get_db(db_path)
        result = index_jsonl(conn, "blanks", jsonl)
        conn.commit()

        assert result["added"] == 2
        conn.close()


# ── index (full pipeline) ──


class TestIndex:
    def test_full_index(self, tmp_path):
        vault = tmp_path / "vault"
        vault.mkdir()
        (vault / "note.md").write_text("# Hello\nWorld content here")

        jsonl_dir = tmp_path / "sources"
        jsonl_dir.mkdir()
        with open(jsonl_dir / "telegram.jsonl", "w") as f:
            f.write(json.dumps({"type": "message", "sender": "X", "chat": "Y", "text": "Telegram message text"}) + "\n")

        skills_dir = tmp_path / "skills"
        skills_dir.mkdir()

        db_path = tmp_path / "index.db"
        result = index(
            vault=vault,
            jsonl_dir=jsonl_dir,
            db_path=db_path,
            skills_dir=skills_dir,
        )

        assert "obsidian" in result
        assert "telegram" in result
        assert result["obsidian"]["added"] == 1
        assert result["telegram"]["added"] == 1

    def test_rebuild_clears_data(self, tmp_path):
        vault = tmp_path / "vault"
        vault.mkdir()
        (vault / "a.md").write_text("# A\nContent A here")

        db_path = tmp_path / "index.db"
        skills_dir = tmp_path / "skills"
        skills_dir.mkdir()

        index(vault=vault, jsonl_dir=tmp_path / "empty", db_path=db_path, skills_dir=skills_dir)

        # Now rebuild
        result = index(
            vault=vault,
            jsonl_dir=tmp_path / "empty",
            db_path=db_path,
            rebuild=True,
            skills_dir=skills_dir,
        )
        assert result["obsidian"]["added"] == 1  # re-added after clear

    def test_exclude_sources(self, tmp_path):
        vault = tmp_path / "vault"
        vault.mkdir()
        (vault / "n.md").write_text("# Note\nContent")

        jsonl_dir = tmp_path / "sources"
        jsonl_dir.mkdir()
        with open(jsonl_dir / "telegram.jsonl", "w") as f:
            f.write(json.dumps({"type": "message", "text": "Test message from TG"}) + "\n")

        db_path = tmp_path / "index.db"
        skills_dir = tmp_path / "skills"
        skills_dir.mkdir()

        result = index(
            vault=vault,
            jsonl_dir=jsonl_dir,
            db_path=db_path,
            exclude={"obsidian"},
            skills_dir=skills_dir,
        )

        assert "obsidian" not in result
        assert "telegram" in result

    def test_exclude_jsonl_source(self, tmp_path):
        vault = tmp_path / "vault"
        vault.mkdir()

        jsonl_dir = tmp_path / "sources"
        jsonl_dir.mkdir()
        with open(jsonl_dir / "telegram.jsonl", "w") as f:
            f.write(json.dumps({"type": "message", "text": "TG text long enough"}) + "\n")
        with open(jsonl_dir / "signal.jsonl", "w") as f:
            f.write(json.dumps({"type": "message", "text": "Signal text long enough"}) + "\n")

        db_path = tmp_path / "index.db"
        skills_dir = tmp_path / "skills"
        skills_dir.mkdir()

        result = index(
            vault=vault,
            jsonl_dir=jsonl_dir,
            db_path=db_path,
            exclude={"telegram"},
            skills_dir=skills_dir,
        )

        assert "telegram" not in result
        assert "signal" in result


# ── stats ──


class TestStats:
    def test_nonexistent_db(self, tmp_path):
        result = stats(tmp_path / "nonexistent.db")
        assert result["total"] == 0
        assert result["sources"] == {}
        assert result["db_size"] == 0

    def test_with_data(self, tmp_path):
        vault = tmp_path / "vault"
        vault.mkdir()
        (vault / "a.md").write_text("# A\nContent A")
        (vault / "b.md").write_text("# B\nContent B")

        db_path = tmp_path / "test.db"
        skills_dir = tmp_path / "skills"
        skills_dir.mkdir()
        index(vault=vault, jsonl_dir=tmp_path / "empty", db_path=db_path, skills_dir=skills_dir)

        result = stats(db_path)
        assert result["total"] == 2
        assert result["sources"]["obsidian"] == 2
        assert result["db_size"] > 0

    def test_multiple_sources(self, tmp_path):
        vault = tmp_path / "vault"
        vault.mkdir()
        (vault / "note.md").write_text("# Note\nStuff here")

        jsonl_dir = tmp_path / "sources"
        jsonl_dir.mkdir()
        with open(jsonl_dir / "telegram.jsonl", "w") as f:
            f.write(json.dumps({"type": "message", "text": "TG message content"}) + "\n")

        db_path = tmp_path / "test.db"
        skills_dir = tmp_path / "skills"
        skills_dir.mkdir()
        index(vault=vault, jsonl_dir=jsonl_dir, db_path=db_path, skills_dir=skills_dir)

        result = stats(db_path)
        assert "obsidian" in result["sources"]
        assert "telegram" in result["sources"]
        assert result["total"] >= 2
