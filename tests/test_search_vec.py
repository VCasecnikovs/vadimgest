"""Tests for vector search: embedding, indexing, semantic search, hybrid search."""

import struct
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from vadimgest.search.indexer import get_db, get_vec_db, index_embeddings, embed_stats, _content_hash
from vadimgest.search.searcher import search, search_semantic, search_hybrid, Result
from vadimgest.search.embedder import Embedder, GeminiEmbedder, get_embedder


# -- Fixtures --

@pytest.fixture
def tmp_db(tmp_path):
    """Create a temp DB with FTS5 index and some docs."""
    db_path = tmp_path / "test.db"
    conn = get_db(db_path)

    # Insert test docs (obsidian source)
    docs = [
        ("obsidian:People/Alice.md", "obsidian", "Alice Smith", "Alice is a robotics engineer at Boston Dynamics", "", "People"),
        ("obsidian:People/Bob.md", "obsidian", "Bob Jones", "Bob works on machine learning models for NLP", "", "People"),
        ("obsidian:Deals/RobotDeal.md", "obsidian", "Robot Deal", "Deal with Figure for robotics training data at $5/hr", "", "Deals"),
        ("skills:heartbeat/SKILL.md", "skills", "heartbeat", "Intake pipeline runs every 30min reading vadimgest", "", "heartbeat"),
        ("telegram:42", "telegram", "Team Chat", "Hey team, the robotics demo went great", "Team Chat", ""),
    ]
    for path, source, title, content, chat, folder in docs:
        conn.execute(
            "INSERT INTO docs (path, source, title, content, chat, folder) VALUES (?, ?, ?, ?, ?, ?)",
            (path, source, title, content, chat, folder)
        )
        conn.execute(
            "INSERT OR REPLACE INTO meta (path, source, mtime, size, content_hash) VALUES (?, ?, 1.0, ?, ?)",
            (path, source, len(content), _content_hash(content))
        )

    conn.commit()
    conn.close()
    return db_path


class FakeEmbedder(Embedder):
    """Deterministic embedder for testing - uses content hash as seed."""

    def embed(self, texts: list[str], task: str = "document") -> list[list[float]]:
        results = []
        for t in texts:
            # Create a deterministic but unique vector based on text content
            h = hash(t) % 10000
            base = [(h + i) % 1000 / 1000.0 for i in range(self.dim)]
            # Normalize
            norm = sum(x * x for x in base) ** 0.5
            results.append([x / norm for x in base])
        return results


# -- Embedder unit tests --

class TestEmbedder:
    def test_serialize_roundtrip(self):
        vec = [0.1, 0.2, 0.3] * 256  # 768 floats
        blob = Embedder.serialize(vec)
        assert isinstance(blob, bytes)
        assert len(blob) == 768 * 4  # 4 bytes per float32
        unpacked = struct.unpack(f"{768}f", blob)
        for orig, decoded in zip(vec, unpacked):
            assert abs(orig - decoded) < 1e-6

    def test_fake_embedder_deterministic(self):
        emb = FakeEmbedder()
        v1 = emb.embed_one("hello")
        v2 = emb.embed_one("hello")
        assert v1 == v2

    def test_fake_embedder_different_texts(self):
        emb = FakeEmbedder()
        v1 = emb.embed_one("robotics data")
        v2 = emb.embed_one("machine learning")
        assert v1 != v2

    def test_fake_embedder_dimension(self):
        emb = FakeEmbedder()
        vec = emb.embed_one("test")
        assert len(vec) == 768

    def test_embed_batch(self):
        emb = FakeEmbedder()
        vecs = emb.embed(["text1", "text2", "text3"])
        assert len(vecs) == 3
        assert all(len(v) == 768 for v in vecs)

    def test_get_embedder_unknown_provider(self):
        with pytest.raises(ValueError, match="Unknown provider"):
            get_embedder("nonexistent")

    def test_get_embedder_gemini_requires_key(self):
        with patch.dict("os.environ", {}, clear=True):
            # Remove GEMINI_API_KEY from env
            import os
            old = os.environ.pop("GEMINI_API_KEY", None)
            try:
                with pytest.raises(ValueError, match="GEMINI_API_KEY"):
                    get_embedder("gemini")
            finally:
                if old:
                    os.environ["GEMINI_API_KEY"] = old


# -- get_vec_db tests --

class TestGetVecDb:
    def test_creates_vec_table(self, tmp_path):
        db_path = tmp_path / "vec_test.db"
        conn = get_vec_db(db_path)
        # vec_docs table should exist - insert a test row
        vec = [0.0] * 768
        blob = Embedder.serialize(vec)
        conn.execute("INSERT INTO vec_docs(doc_id, embedding) VALUES (1, ?)", (blob,))
        conn.commit()
        row = conn.execute("SELECT doc_id FROM vec_docs").fetchone()
        assert row[0] == 1
        conn.close()

    def test_creates_parent_dir(self, tmp_path):
        db_path = tmp_path / "subdir" / "deep" / "test.db"
        conn = get_vec_db(db_path)
        assert db_path.exists()
        conn.close()

    def test_knn_search(self, tmp_path):
        db_path = tmp_path / "knn.db"
        conn = get_vec_db(db_path)

        # Insert 3 vectors
        emb = FakeEmbedder()
        for i, text in enumerate(["robotics", "cooking", "space"], 1):
            vec = emb.embed_one(text)
            blob = Embedder.serialize(vec)
            conn.execute("INSERT INTO vec_docs(doc_id, embedding) VALUES (?, ?)", (i, blob))
        conn.commit()

        # Search for "robotics" - should return itself as nearest
        query_vec = emb.embed_one("robotics")
        query_blob = Embedder.serialize(query_vec)
        rows = conn.execute(
            "SELECT doc_id, distance FROM vec_docs WHERE embedding MATCH ? AND k = 3 ORDER BY distance",
            (query_blob,)
        ).fetchall()

        assert len(rows) == 3
        assert rows[0][0] == 1  # robotics doc_id
        assert rows[0][1] == pytest.approx(0.0, abs=1e-6)  # exact match = 0 distance
        conn.close()


# -- index_embeddings tests --

class TestIndexEmbeddings:
    def test_embeds_only_md_sources(self, tmp_db):
        """Only obsidian + skills sources should be embedded, not telegram."""
        with patch("vadimgest.search.embedder.get_embedder", return_value=FakeEmbedder()):
            result = index_embeddings(db_path=tmp_db, provider="fake")

        # 4 obsidian+skills docs, 1 telegram doc (excluded)
        assert result["total"] == 4
        assert result["embedded"] == 4
        assert result["skipped"] == 0

    def test_skips_already_embedded(self, tmp_db):
        """Second run should skip docs that haven't changed."""
        with patch("vadimgest.search.embedder.get_embedder", return_value=FakeEmbedder()):
            r1 = index_embeddings(db_path=tmp_db, provider="fake")
            r2 = index_embeddings(db_path=tmp_db, provider="fake")

        assert r1["embedded"] == 4
        assert r2["embedded"] == 0
        assert r2["skipped"] == 4

    def test_prunes_vectors_for_removed_documents(self, tmp_db):
        with patch("vadimgest.search.embedder.get_embedder", return_value=FakeEmbedder()):
            index_embeddings(db_path=tmp_db, provider="fake")

            conn = get_db(tmp_db)
            conn.execute("DELETE FROM docs WHERE path = ?", ("obsidian:People/Alice.md",))
            conn.execute("DELETE FROM meta WHERE path = ?", ("obsidian:People/Alice.md",))
            conn.commit()
            conn.close()

            result = index_embeddings(db_path=tmp_db, provider="fake")

        conn = get_vec_db(tmp_db)
        assert conn.execute("SELECT COUNT(*) FROM vec_docs").fetchone()[0] == 3
        conn.close()
        assert result["pruned"] == 1

    def test_rejects_mixed_or_unknown_embedding_space(self, tmp_db):
        with patch("vadimgest.search.embedder.get_embedder", return_value=FakeEmbedder()):
            index_embeddings(db_path=tmp_db, provider="provider-a")
            with pytest.raises(RuntimeError, match="embedding space"):
                index_embeddings(db_path=tmp_db, provider="provider-b")

    def test_rebuild_allows_intentional_provider_change(self, tmp_db):
        with patch("vadimgest.search.embedder.get_embedder", return_value=FakeEmbedder()):
            index_embeddings(db_path=tmp_db, provider="provider-a")
            result = index_embeddings(
                db_path=tmp_db,
                provider="provider-b",
                rebuild=True,
            )

        assert result["embedded"] == 4
        conn = get_vec_db(tmp_db)
        space = conn.execute(
            "SELECT value FROM vec_meta WHERE key = 'embedding_space'"
        ).fetchone()[0]
        conn.close()
        assert space.startswith("provider-b:")

    def test_limit_parameter(self, tmp_db):
        """Limit should cap the number of docs embedded."""
        with patch("vadimgest.search.embedder.get_embedder", return_value=FakeEmbedder()):
            result = index_embeddings(db_path=tmp_db, provider="fake", limit=2)

        assert result["embedded"] == 2

    def test_reembeds_on_content_change(self, tmp_db):
        """Changed content hash should trigger re-embedding."""
        with patch("vadimgest.search.embedder.get_embedder", return_value=FakeEmbedder()):
            index_embeddings(db_path=tmp_db, provider="fake")

        # Change content of one doc
        conn = get_db(tmp_db)
        conn.execute("DELETE FROM docs WHERE path = 'obsidian:People/Alice.md'")
        new_content = "Alice now works at TechCorp on humanoid robots"
        conn.execute(
            "INSERT INTO docs (path, source, title, content, chat, folder) VALUES (?, 'obsidian', 'Alice Smith', ?, '', 'People')",
            ("obsidian:People/Alice.md", new_content)
        )
        conn.execute(
            "UPDATE meta SET content_hash = ? WHERE path = 'obsidian:People/Alice.md'",
            (_content_hash(new_content),)
        )
        conn.commit()
        conn.close()

        with patch("vadimgest.search.embedder.get_embedder", return_value=FakeEmbedder()):
            result = index_embeddings(db_path=tmp_db, provider="fake")

        # Only the changed doc should be re-embedded
        assert result["embedded"] == 1


# -- embed_stats tests --

class TestEmbedStats:
    def test_empty_db(self, tmp_path):
        db_path = tmp_path / "nonexistent.db"
        s = embed_stats(db_path)
        assert s["total_docs"] == 0
        assert s["embedded"] == 0
        assert s["coverage"] == 0

    def test_with_embeddings(self, tmp_db):
        with patch("vadimgest.search.embedder.get_embedder", return_value=FakeEmbedder()):
            index_embeddings(db_path=tmp_db, provider="fake")

        s = embed_stats(tmp_db)
        assert s["total_docs"] == 5  # all docs including telegram
        assert s["embedded"] == 4  # only obsidian + skills
        assert s["coverage"] == pytest.approx(80.0, abs=0.1)


# -- search_semantic tests --

class TestSearchSemantic:
    def _setup_embeddings(self, tmp_db):
        with patch("vadimgest.search.embedder.get_embedder", return_value=FakeEmbedder()):
            index_embeddings(db_path=tmp_db, provider="fake")

    def test_basic_semantic_search(self, tmp_db):
        self._setup_embeddings(tmp_db)
        with patch("vadimgest.search.embedder.get_embedder", return_value=FakeEmbedder()):
            results = search_semantic("robotics engineer", n=5, db_path=tmp_db, md=True, provider="fake")

        assert len(results) > 0
        assert all(isinstance(r, Result) for r in results)
        assert all(r.source in ("obsidian", "skills") for r in results)

    def test_md_filter(self, tmp_db):
        self._setup_embeddings(tmp_db)
        with patch("vadimgest.search.embedder.get_embedder", return_value=FakeEmbedder()):
            results = search_semantic("test", n=10, db_path=tmp_db, md=True, provider="fake")

        # Should only return obsidian + skills
        for r in results:
            assert r.source in ("obsidian", "skills")

    def test_result_fields(self, tmp_db):
        self._setup_embeddings(tmp_db)
        with patch("vadimgest.search.embedder.get_embedder", return_value=FakeEmbedder()):
            results = search_semantic("data", n=1, db_path=tmp_db, md=True, provider="fake")

        if results:
            r = results[0]
            assert r.path
            assert r.source
            assert r.title
            assert r.snippet
            assert isinstance(r.rank, float)

    def test_folder_filter(self, tmp_db):
        self._setup_embeddings(tmp_db)
        with patch("vadimgest.search.embedder.get_embedder", return_value=FakeEmbedder()):
            results = search_semantic("data", n=10, db_path=tmp_db, md=True, provider="fake", folder="Deals")

        for r in results:
            assert "Deals" in r.folder


# -- search_hybrid tests --

class TestSearchHybrid:
    def _setup_embeddings(self, tmp_db):
        with patch("vadimgest.search.embedder.get_embedder", return_value=FakeEmbedder()):
            index_embeddings(db_path=tmp_db, provider="fake")

    def test_hybrid_combines_results(self, tmp_db):
        self._setup_embeddings(tmp_db)
        with patch("vadimgest.search.embedder.get_embedder", return_value=FakeEmbedder()):
            results = search_hybrid("robotics", n=5, db_path=tmp_db, md=True, provider="fake")

        assert len(results) > 0
        # RRF scores should be positive
        for r in results:
            assert r.rank > 0

    def test_hybrid_respects_n(self, tmp_db):
        self._setup_embeddings(tmp_db)
        with patch("vadimgest.search.embedder.get_embedder", return_value=FakeEmbedder()):
            results = search_hybrid("data", n=2, db_path=tmp_db, md=True, provider="fake")

        assert len(results) <= 2

    def test_hybrid_accepts_hyphenated_natural_language(self, tmp_db):
        """Regression: `write-time` was parsed as an FTS column expression."""
        self._setup_embeddings(tmp_db)
        with patch("vadimgest.search.embedder.get_embedder", return_value=FakeEmbedder()):
            results = search_hybrid(
                "write-time memory",
                n=5,
                db_path=tmp_db,
                md=True,
                provider="fake",
            )

        assert isinstance(results, list)

    def test_hybrid_uses_memory_score_as_bounded_tiebreaker(self, tmp_db):
        conn = get_db(tmp_db)
        conn.execute(
            "UPDATE docs SET content = content || ? WHERE path = ?",
            ("\n- **memory-score:** `2.0`", "obsidian:People/Alice.md"),
        )
        conn.execute(
            "UPDATE docs SET content = content || ? WHERE path = ?",
            ("\n- **memory-score:** `9.0`", "obsidian:People/Bob.md"),
        )
        conn.commit()
        conn.close()

        alice = Result("obsidian:People/Alice.md", "obsidian", "Alice", "", 0.0)
        bob = Result("obsidian:People/Bob.md", "obsidian", "Bob", "", 0.0)
        with patch("vadimgest.search.searcher.search", return_value=[alice, bob]), patch(
            "vadimgest.search.searcher.search_semantic", return_value=[bob, alice]
        ):
            results = search_hybrid("query", n=2, db_path=tmp_db, md=True)

        assert [result.title for result in results] == ["Bob", "Alice"]


# -- content_hash tests --

class TestContentHash:
    def test_deterministic(self):
        assert _content_hash("hello") == _content_hash("hello")

    def test_different_for_different_content(self):
        assert _content_hash("hello") != _content_hash("world")

    def test_length(self):
        h = _content_hash("test")
        assert len(h) == 16  # SHA256[:16]


# -- FTS5 search tests (for completeness) --

class TestFTS5Search:
    def test_basic_search(self, tmp_db):
        results = search("robotics", n=5, db_path=tmp_db, md=True)
        assert len(results) > 0
        assert any("robotics" in r.snippet.lower() or "robotics" in r.title.lower() for r in results)

    def test_source_filter(self, tmp_db):
        results = search("robotics", n=5, db_path=tmp_db, md=False, raw=True)
        for r in results:
            assert r.source not in ("obsidian", "skills")

    def test_chat_filter(self, tmp_db):
        results = search("demo", n=5, db_path=tmp_db, md=False, raw=True, chat="Team")
        for r in results:
            assert "Team" in r.chat

    def test_no_results(self, tmp_db):
        results = search("xyznonexistentterm123", n=5, db_path=tmp_db, md=True)
        assert results == []


# -- Integration test with Gemini (skipped by default) --

class TestGeminiIntegration:
    """Real Gemini API tests - only run with GEMINI_API_KEY set and --run-integration flag."""

    @pytest.fixture(autouse=True)
    def _skip_without_key(self):
        import os
        if not os.environ.get("GEMINI_API_KEY"):
            pytest.skip("GEMINI_API_KEY not set")

    def test_gemini_embed_one(self):
        emb = GeminiEmbedder()
        vec = emb.embed_one("test query", task="query")
        assert len(vec) == 768
        assert all(isinstance(v, float) for v in vec)

    def test_gemini_embed_batch(self):
        emb = GeminiEmbedder()
        vecs = emb.embed(["hello", "world"], task="document")
        assert len(vecs) == 2
        assert all(len(v) == 768 for v in vecs)

    def test_full_pipeline(self, tmp_db):
        """End-to-end: embed docs, then search semantically."""
        result = index_embeddings(db_path=tmp_db, provider="gemini")
        assert result["embedded"] > 0

        results = search_semantic("robotics", n=3, db_path=tmp_db, md=True, provider="gemini")
        assert len(results) > 0
        # The robotics-related docs should rank high
        titles = [r.title.lower() for r in results]
        assert any("robot" in t or "alice" in t for t in titles)
