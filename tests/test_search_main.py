"""Tests for vadimgest/search/__main__.py - search CLI argument parsing and commands."""

import json
import sys
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from dataclasses import dataclass


# Create a mock Result matching searcher.Result
@dataclass
class MockResult:
    path: str
    source: str
    title: str
    snippet: str
    rank: float
    chat: str = ""
    folder: str = ""


# Import the module under test
from vadimgest.search.__main__ import (
    _ensure_index,
    _print_results,
    cmd_search,
    cmd_search_vec,
    cmd_search_hybrid,
    cmd_index,
    cmd_stats,
    cmd_embed,
    cmd_embed_stats,
    main,
)


# ── _ensure_index ──


class TestEnsureIndex:
    def test_skips_when_db_exists(self, tmp_path):
        db = tmp_path / "index.db"
        db.write_text("x")
        with patch("vadimgest.search.__main__.index") as mock_index, patch(
            "vadimgest.search.__main__.reindex_stale",
            return_value={},
        ):
            _ensure_index(db)
            mock_index.assert_not_called()

    def test_builds_index_when_missing(self, tmp_path):
        db = tmp_path / "index.db"
        with patch("vadimgest.search.__main__.index", return_value={"md": {"added": 100}}) as mock_index:
            _ensure_index(db)
            mock_index.assert_called_once_with(db_path=db)


# ── _print_results ──


class TestPrintResults:
    def test_no_results(self, capsys):
        _print_results([])
        out = capsys.readouterr().out
        assert "No results" in out

    def test_none_results(self, capsys):
        _print_results(None)
        out = capsys.readouterr().out
        assert "No results" in out

    def test_json_output(self, capsys):
        results = [MockResult(path="test.md", source="md", title="Test",
                              snippet="hello", rank=1.0, chat="", folder="People")]
        _print_results(results, as_json=True)
        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert len(parsed) == 1
        assert parsed[0]["title"] == "Test"
        assert parsed[0]["folder"] == "People"

    def test_json_multiple_results(self, capsys):
        results = [
            MockResult(path="a.md", source="md", title="A", snippet="x", rank=1.0),
            MockResult(path="b.md", source="telegram", title="B", snippet="y", rank=2.0, chat="Group"),
        ]
        _print_results(results, as_json=True)
        parsed = json.loads(capsys.readouterr().out)
        assert len(parsed) == 2
        assert parsed[1]["chat"] == "Group"

    def test_text_output(self, capsys):
        results = [MockResult(path="People/John.md", source="md", title="John Doe",
                              snippet="some >>>match<<< here", rank=1.0)]
        _print_results(results, as_json=False)
        out = capsys.readouterr().out
        assert "John Doe" in out
        assert "People/John.md" in out

    def test_text_output_with_chat(self, capsys):
        results = [MockResult(path="tg:123", source="telegram", title="Message",
                              snippet="hello", rank=1.0, chat="Dev Chat")]
        _print_results(results)
        out = capsys.readouterr().out
        assert "Dev Chat" in out

    def test_text_output_colon_path(self, capsys):
        results = [MockResult(path="telegram:12345", source="telegram", title="Msg",
                              snippet="test", rank=1.0)]
        _print_results(results)
        out = capsys.readouterr().out
        assert "12345" in out

    def test_text_output_no_snippet(self, capsys):
        results = [MockResult(path="a.md", source="md", title="Title",
                              snippet="", rank=1.0)]
        _print_results(results)
        out = capsys.readouterr().out
        assert "Title" in out

    def test_text_multiline_snippet(self, capsys):
        snippet = "line1\nline2\nline3\nline4\nline5"
        results = [MockResult(path="a.md", source="md", title="T",
                              snippet=snippet, rank=1.0)]
        _print_results(results)
        out = capsys.readouterr().out
        assert "line1" in out
        assert "line3" in out


# ── cmd_search ──


class TestCmdSearch:
    @patch("vadimgest.search.__main__._print_results")
    @patch("vadimgest.search.__main__.search", return_value=[])
    @patch("vadimgest.search.__main__._ensure_index")
    def test_basic_search(self, mock_ensure, mock_search, mock_print, tmp_path):
        db = tmp_path / "test.db"
        cmd_search("hello", md=True, db_path=db)
        mock_ensure.assert_called_once_with(db)
        mock_search.assert_called_once()
        mock_print.assert_called_once_with([], False)

    @patch("vadimgest.search.__main__._print_results")
    @patch("vadimgest.search.__main__.search", return_value=[])
    @patch("vadimgest.search.__main__._ensure_index")
    def test_search_with_filters(self, mock_ensure, mock_search, mock_print, tmp_path):
        db = tmp_path / "test.db"
        cmd_search("query", n=5, source="telegram", md=False, raw=True,
                   full=True, as_json=True, chat="Dev", folder="People", db_path=db)
        mock_search.assert_called_once_with(
            "query", n=5, db_path=db, source="telegram", sources=None,
            md=False, raw=True,
            full=True, chat="Dev", folder="People"
        )
        mock_print.assert_called_once_with([], True)

    @patch("vadimgest.search.__main__._print_results")
    @patch("vadimgest.search.__main__.search", return_value=[])
    @patch("vadimgest.search.__main__._ensure_index")
    def test_search_with_multiple_sources(self, mock_ensure, mock_search, mock_print, tmp_path):
        db = tmp_path / "test.db"
        cmd_search(
            "query",
            sources=("telegram", "signal", "gmail", "bee"),
            db_path=db,
        )

        assert mock_search.call_args.kwargs["sources"] == (
            "telegram",
            "signal",
            "gmail",
            "bee",
        )


class TestCmdSearchVec:
    @patch("vadimgest.search.__main__._print_results")
    @patch("vadimgest.search.__main__.search_semantic", return_value=[])
    @patch("vadimgest.search.__main__._ensure_index")
    def test_vec_search(self, mock_ensure, mock_semantic, mock_print, tmp_path):
        db = tmp_path / "test.db"
        cmd_search_vec("query", md=True, provider="gemini", db_path=db)
        mock_semantic.assert_called_once()
        assert mock_semantic.call_args[1]["provider"] == "gemini"


class TestCmdSearchHybrid:
    @patch("vadimgest.search.__main__._print_results")
    @patch("vadimgest.search.__main__.search_hybrid", return_value=[])
    @patch("vadimgest.search.__main__._ensure_index")
    def test_hybrid_search(self, mock_ensure, mock_hybrid, mock_print, tmp_path):
        db = tmp_path / "test.db"
        cmd_search_hybrid("query", md=True, provider="openai", db_path=db)
        mock_hybrid.assert_called_once()
        assert mock_hybrid.call_args[1]["provider"] == "openai"


class TestCmdEmbed:
    @patch("vadimgest.search.indexer.index_embeddings", return_value={
        "embedded": 0,
        "skipped": 0,
        "total": 0,
    })
    def test_rebuild_is_explicit(self, mock_index, tmp_path):
        cmd_embed("local", rebuild=True, db_path=tmp_path / "db")
        assert mock_index.call_args.kwargs["rebuild"] is True

    @patch("vadimgest.search.indexer.index_embeddings", return_value={
        "embedded": 0,
        "skipped": 0,
        "total": 0,
    })
    def test_sources_and_batch_size_are_forwarded(self, mock_index, tmp_path):
        cmd_embed(
            "local",
            sources=("telegram", "signal"),
            batch_size=64,
            db_path=tmp_path / "db",
        )

        assert mock_index.call_args.kwargs["sources"] == ("telegram", "signal")
        assert mock_index.call_args.kwargs["batch_size"] == 64
        assert mock_index.call_args.kwargs["max_batch_chars"] == 64_000


# ── cmd_index ──


class TestCmdIndex:
    @patch("vadimgest.search.__main__.index", return_value={
        "md": {"added": 100, "total": 500, "unchanged": 400},
        "telegram": {"added": 50, "total": 1000, "updated": 10, "removed": 5},
    })
    def test_index_basic(self, mock_index, capsys, tmp_path):
        cmd_index(vault=tmp_path, jsonl_dir=tmp_path, db_path=tmp_path / "db")
        out = capsys.readouterr().out
        assert "Indexing" in out
        assert "Done" in out
        assert "md" in out
        assert "telegram" in out

    @patch("vadimgest.search.__main__.index", return_value={
        "md": {"added": 0, "total": 100, "skipped": 10},
    })
    def test_index_with_skipped(self, mock_index, capsys, tmp_path):
        cmd_index(vault=tmp_path, jsonl_dir=tmp_path, db_path=tmp_path / "db")
        out = capsys.readouterr().out
        assert "skipped" in out

    @patch("vadimgest.search.__main__.index", return_value={})
    def test_index_rebuild(self, mock_index, capsys, tmp_path):
        cmd_index(rebuild=True, vault=tmp_path, jsonl_dir=tmp_path, db_path=tmp_path / "db")
        mock_index.assert_called_once()
        assert mock_index.call_args[1]["rebuild"] is True

    @patch("vadimgest.search.__main__.index", return_value={})
    def test_index_with_exclude(self, mock_index, capsys, tmp_path):
        cmd_index(exclude={"telegram", "signal"}, vault=tmp_path, jsonl_dir=tmp_path, db_path=tmp_path / "db")
        out = capsys.readouterr().out
        assert "Excluding" in out

    @patch("vadimgest.search.__main__.index", return_value={})
    def test_index_md_only(self, mock_index, tmp_path):
        cmd_index(
            vault=tmp_path,
            jsonl_dir=tmp_path,
            db_path=tmp_path / "db",
            md_only=True,
        )
        assert mock_index.call_args.kwargs["md_only"] is True


# ── cmd_stats ──


class TestCmdStats:
    @patch("vadimgest.search.__main__.stats", return_value={
        "total": 0, "db_size": 0, "sources": {}
    })
    def test_empty_index(self, mock_stats, capsys):
        cmd_stats()
        out = capsys.readouterr().out
        assert "No index" in out

    @patch("vadimgest.search.__main__.stats", return_value={
        "total": 1500,
        "db_size": 10 * 1024 * 1024,
        "sources": {"md": 500, "telegram": 1000}
    })
    def test_with_data(self, mock_stats, capsys):
        cmd_stats()
        out = capsys.readouterr().out
        assert "1500" in out
        assert "md" in out
        assert "telegram" in out
        assert "MB" in out


# ── cmd_embed_stats ──


class TestCmdEmbedStats:
    @patch("vadimgest.search.indexer.embed_stats", return_value={
        "total_docs": 1000, "embedded": 500, "coverage": 50.0
    })
    def test_embed_stats(self, mock_embed_stats, capsys):
        cmd_embed_stats()
        out = capsys.readouterr().out
        assert "1000" in out
        assert "500" in out
        assert "50" in out


# ── main() CLI argument parsing ──


class TestMain:
    def test_help(self, capsys):
        with patch("sys.argv", ["vadimgest search", "-h"]):
            main()
            out = capsys.readouterr().out
            assert "vadimgest search" in out

    def test_no_args(self, capsys):
        with patch("sys.argv", ["vadimgest search"]):
            main()
            out = capsys.readouterr().out
            assert "vadimgest search" in out

    @patch("vadimgest.search.__main__.cmd_index")
    def test_index_command(self, mock_cmd):
        with patch("sys.argv", ["vadimgest search", "index"]):
            main()
            mock_cmd.assert_called_once_with(
                rebuild=False,
                exclude=None,
                md_only=False,
                rebuild_sources=None,
            )

    @patch("vadimgest.search.__main__.cmd_index")
    def test_index_rebuild(self, mock_cmd):
        with patch("sys.argv", ["vadimgest search", "index", "--rebuild"]):
            main()
            mock_cmd.assert_called_once_with(
                rebuild=True,
                exclude=None,
                md_only=False,
                rebuild_sources=None,
            )

    @patch("vadimgest.search.__main__.cmd_index")
    def test_index_with_exclude(self, mock_cmd):
        with patch("sys.argv", ["vadimgest search", "index", "--exclude", "telegram", "--exclude", "signal"]):
            main()
            mock_cmd.assert_called_once_with(
                rebuild=False,
                exclude={"telegram", "signal"},
                md_only=False,
                rebuild_sources=None,
            )

    @patch("vadimgest.search.__main__.cmd_index")
    def test_index_md_only_command(self, mock_cmd):
        with patch("sys.argv", ["vadimgest search", "index", "--md-only"]):
            main()
            mock_cmd.assert_called_once_with(
                rebuild=False,
                exclude=None,
                md_only=True,
                rebuild_sources=None,
            )

    @patch("vadimgest.search.__main__.cmd_index")
    def test_index_rebuild_source(self, mock_cmd):
        with patch("sys.argv", [
            "vadimgest search",
            "index",
            "--rebuild-source",
            "gmail",
        ]):
            main()
            assert mock_cmd.call_args.kwargs["rebuild_sources"] == {"gmail"}

    @patch("vadimgest.search.__main__.cmd_stats")
    def test_stats_command(self, mock_cmd):
        with patch("sys.argv", ["vadimgest search", "stats"]):
            main()
            mock_cmd.assert_called_once()

    @patch("vadimgest.search.__main__.cmd_embed_stats")
    def test_embed_stats(self, mock_cmd):
        with patch("sys.argv", ["vadimgest search", "embed", "--stats"]):
            main()
            mock_cmd.assert_called_once()

    @patch("vadimgest.search.__main__.cmd_embed")
    def test_embed_with_provider(self, mock_cmd):
        with patch("sys.argv", ["vadimgest search", "embed", "--provider", "gemini"]):
            main()
            mock_cmd.assert_called_once_with(
                provider="gemini",
                limit=None,
                rebuild=False,
                sources=None,
                batch_size=10,
                max_batch_chars=64_000,
            )

    @patch("vadimgest.search.__main__.cmd_embed")
    def test_embed_with_limit(self, mock_cmd):
        with patch("sys.argv", ["vadimgest search", "embed", "--provider", "ollama", "--limit", "100"]):
            main()
            mock_cmd.assert_called_once_with(
                provider="ollama",
                limit=100,
                rebuild=False,
                sources=None,
                batch_size=10,
                max_batch_chars=64_000,
            )

    @patch("vadimgest.search.__main__.cmd_embed")
    def test_embed_rebuild(self, mock_cmd):
        with patch("sys.argv", [
            "vadimgest search",
            "embed",
            "--provider",
            "local",
            "--rebuild",
        ]):
            main()
            mock_cmd.assert_called_once_with(
                provider="local",
                limit=None,
                rebuild=True,
                sources=None,
                batch_size=10,
                max_batch_chars=64_000,
            )

    @patch("vadimgest.search.__main__.cmd_embed")
    def test_embed_sources_and_batch_size(self, mock_cmd):
        with patch("sys.argv", [
            "vadimgest search",
            "embed",
            "--provider",
            "local",
            "--sources",
            "obsidian,skills,telegram,signal,gmail,bee",
            "--batch-size",
            "64",
        ]):
            main()
            mock_cmd.assert_called_once_with(
                provider="local",
                limit=None,
                rebuild=False,
                sources=("obsidian", "skills", "telegram", "signal", "gmail", "bee"),
                batch_size=64,
                max_batch_chars=64_000,
            )

    def test_embed_no_provider(self):
        with patch("sys.argv", ["vadimgest search", "embed"]):
            with pytest.raises(SystemExit):
                main()

    def test_search_no_scope(self):
        with patch("sys.argv", ["vadimgest search", "hello"]):
            with pytest.raises(SystemExit):
                main()

    @patch("vadimgest.search.__main__.cmd_search")
    def test_search_md(self, mock_cmd):
        with patch("sys.argv", ["vadimgest search", "hello", "--md"]):
            main()
            mock_cmd.assert_called_once()
            kwargs = mock_cmd.call_args
            assert kwargs[1]["md"] is True if isinstance(kwargs[1], dict) else True

    @patch("vadimgest.search.__main__.cmd_search")
    def test_search_raw(self, mock_cmd):
        with patch("sys.argv", ["vadimgest search", "hello", "--raw"]):
            main()
            mock_cmd.assert_called_once()

    @patch("vadimgest.search.__main__.cmd_search")
    def test_search_source(self, mock_cmd):
        with patch("sys.argv", ["vadimgest search", "hello", "-s", "telegram"]):
            main()
            mock_cmd.assert_called_once()

    @patch("vadimgest.search.__main__.cmd_search")
    def test_search_sources(self, mock_cmd):
        with patch("sys.argv", [
            "vadimgest search",
            "hello",
            "--sources",
            "telegram,signal,gmail,bee",
        ]):
            main()
            assert mock_cmd.call_args.kwargs["sources"] == (
                "telegram",
                "signal",
                "gmail",
                "bee",
            )

    @patch("vadimgest.search.__main__.cmd_search")
    def test_search_with_all_flags(self, mock_cmd):
        with patch("sys.argv", ["vadimgest search", "query", "--md", "--raw", "-n", "20",
                                "--full", "--json", "--chat", "Dev", "--folder", "People"]):
            main()
            mock_cmd.assert_called_once()
            args = mock_cmd.call_args
            # Positional arg is the query
            assert args[0][0] == "query"

    @patch("vadimgest.search.__main__.cmd_search_vec")
    def test_search_vec(self, mock_cmd):
        with patch("sys.argv", ["vadimgest search", "hello", "--md", "--vec", "--provider", "gemini"]):
            main()
            mock_cmd.assert_called_once()

    @patch("vadimgest.search.__main__.cmd_search_hybrid")
    def test_search_hybrid(self, mock_cmd):
        with patch("sys.argv", ["vadimgest search", "hello", "--md", "--hybrid", "--provider", "openai"]):
            main()
            mock_cmd.assert_called_once()

    def test_vec_no_provider(self):
        with patch("sys.argv", ["vadimgest search", "hello", "--md", "--vec"]):
            with pytest.raises(SystemExit):
                main()

    def test_hybrid_no_provider(self):
        with patch("sys.argv", ["vadimgest search", "hello", "--md", "--hybrid"]):
            with pytest.raises(SystemExit):
                main()
