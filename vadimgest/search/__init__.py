"""vadimgest.search - FTS5 + vector search over Obsidian vault, skills, and JSONL."""

from .indexer import index, stats, get_db, reindex_stale, DEFAULT_DB, DEFAULT_VAULT, DEFAULT_JSONL_DIR
from .searcher import search, search_semantic, search_hybrid, Result

__all__ = ["index", "stats", "search", "search_semantic", "search_hybrid",
           "Result", "get_db", "reindex_stale", "DEFAULT_DB", "DEFAULT_VAULT", "DEFAULT_JSONL_DIR"]
