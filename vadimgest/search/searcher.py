"""FTS5 + vector search engine."""

import sqlite3
import re
from dataclasses import dataclass, field
from pathlib import Path

from .indexer import DEFAULT_DB, get_db, get_vec_db
from .scoring import extract_document_memory_score, memory_boost


def _literal_fts_query(query: str) -> str:
    return " AND ".join(f'"{token}"' for token in re.findall(r"\w+", query))


@dataclass
class Result:
    path: str
    source: str
    title: str
    snippet: str
    rank: float
    chat: str = ""
    folder: str = ""


def search(query: str, n: int = 10, db_path: Path = DEFAULT_DB,
           source: str | None = None, sources: tuple[str, ...] | None = None,
           md: bool = False, raw: bool = False,
           full: bool = False, chat: str | None = None,
           folder: str | None = None) -> list[Result]:
    """Search the FTS5 index.

    Args:
        source: filter to one source (backward-compatible shorthand)
        sources: filter to an explicit source allowlist
        md: include Obsidian vault
        raw: include JSONL sources
        chat: filter by chat/group name (substring match)
        folder: filter by folder (substring match)
    """
    if not db_path.exists():
        return []

    conn = get_db(db_path)

    fts_query = query
    where_extra, source_params = _source_filter_sql(source, sources, md, raw)

    # Metadata filters (UNINDEXED columns, filtered via WHERE)
    filter_parts = []
    filter_params = []
    if chat:
        filter_parts.append("AND chat LIKE ?")
        filter_params.append(f"%{chat}%")
    if folder:
        filter_parts.append("AND folder LIKE ?")
        filter_params.append(f"%{folder}%")
    filter_clause = " ".join(filter_parts)

    content_col = "content" if full else "snippet(docs, 3, '>>>', '<<<', '...', 40)"

    sql = f"""
        SELECT path, source, title, {content_col}, rank, chat, folder
        FROM docs
        WHERE docs MATCH ? {where_extra}
        {filter_clause}
        ORDER BY rank
        LIMIT ?
    """
    try:
        rows = conn.execute(
            sql,
            (fts_query, *source_params, *filter_params, n),
        ).fetchall()
    except sqlite3.OperationalError:
        literal_query = _literal_fts_query(query)
        if not literal_query:
            conn.close()
            return []
        rows = conn.execute(
            sql,
            (literal_query, *source_params, *filter_params, n),
        ).fetchall()

    conn.close()

    return [
        Result(path=r[0], source=r[1], title=r[2], snippet=r[3], rank=r[4],
               chat=r[5] or "", folder=r[6] or "")
        for r in rows
    ]


def _source_filter_sql(source: str | None, sources: tuple[str, ...] | None,
                       md: bool, raw: bool) -> tuple[str, tuple[str, ...]]:
    """Build a parameterized source filter."""
    md_sources = ("obsidian", "skills")
    if source:
        sources = (source,)
    if sources:
        unique_sources = tuple(dict.fromkeys(s for s in sources if s))
        placeholders = ",".join("?" for _ in unique_sources)
        return f"AND source IN ({placeholders})", unique_sources
    if md and not raw:
        return "AND source IN (?, ?)", md_sources
    if raw and not md:
        return "AND source NOT IN (?, ?)", md_sources
    return "", ()


def search_semantic(query: str, n: int = 10, db_path: Path = DEFAULT_DB,
                    source: str | None = None, sources: tuple[str, ...] | None = None,
                    md: bool = False, raw: bool = False,
                    full: bool = False, provider: str = "gemini",
                    chat: str | None = None, folder: str | None = None) -> list[Result]:
    """Pure embedding-based semantic search."""
    from .embedder import get_embedder, Embedder

    embedder = get_embedder(provider)
    query_vec = embedder.embed_one(query, task="query")
    query_blob = Embedder.serialize(query_vec)

    conn_vec = get_vec_db(db_path)
    conn_fts = get_db(db_path)

    vector_count = conn_vec.execute("SELECT COUNT(*) FROM vec_docs").fetchone()[0]
    if not vector_count:
        conn_vec.close()
        conn_fts.close()
        return []

    # sqlite-vec applies KNN before metadata filters, so filtered searches need
    # enough candidates to avoid losing a smaller source inside a large corpus.
    has_filter = bool(source or sources or md or raw or chat or folder)
    fetch_n = min(vector_count, max(n * 5, 1000 if has_filter else n))
    rows = conn_vec.execute(
        "SELECT doc_id, distance FROM vec_docs WHERE embedding MATCH ? AND k = ? ORDER BY distance",
        (query_blob, fetch_n)
    ).fetchall()

    # Build source filter for post-filtering
    src_sql, src_params = _source_filter_sql(source, sources, md, raw)

    results = []
    for doc_id, distance in rows:
        if len(results) >= n:
            break
        row = conn_fts.execute(
            f"SELECT path, source, title, content, chat, folder FROM docs WHERE rowid = ? {src_sql}",
            (doc_id, *src_params)
        ).fetchone()
        if not row:
            continue
        # Metadata filters
        if chat and chat.lower() not in (row[4] or "").lower():
            continue
        if folder and folder.lower() not in (row[5] or "").lower():
            continue
        snippet = row[3][:200] if not full else row[3]
        results.append(Result(
            path=row[0], source=row[1], title=row[2], snippet=snippet,
            rank=distance, chat=row[4] or "", folder=row[5] or ""
        ))

    conn_vec.close()
    conn_fts.close()
    return results


def search_hybrid(query: str, n: int = 10, db_path: Path = DEFAULT_DB,
                  source: str | None = None, sources: tuple[str, ...] | None = None,
                  md: bool = False, raw: bool = False,
                  full: bool = False, provider: str = "gemini",
                  chat: str | None = None, folder: str | None = None,
                  rrf_k: int = 60) -> list[Result]:
    """Hybrid search: FTS5 + embedding with RRF fusion."""
    top_k = 50

    fts_results = search(query, n=top_k, db_path=db_path, source=source, sources=sources,
                         md=md, raw=raw, full=full, chat=chat, folder=folder)
    sem_results = search_semantic(query, n=top_k, db_path=db_path, source=source,
                                  sources=sources,
                                  md=md, raw=raw, full=full, provider=provider,
                                  chat=chat, folder=folder)

    # RRF fusion
    scores: dict[str, float] = {}
    result_map: dict[str, Result] = {}

    for rank, r in enumerate(fts_results, 1):
        scores[r.path] = scores.get(r.path, 0) + 1.0 / (rrf_k + rank)
        result_map[r.path] = r

    for rank, r in enumerate(sem_results, 1):
        scores[r.path] = scores.get(r.path, 0) + 1.0 / (rrf_k + rank)
        if r.path not in result_map:
            result_map[r.path] = r

    if scores:
        conn = get_db(db_path)
        placeholders = ",".join("?" for _ in scores)
        rows = conn.execute(
            f"SELECT path, content FROM docs WHERE path IN ({placeholders})",
            tuple(scores),
        ).fetchall()
        conn.close()
        content_by_path = dict(rows)
        for path in scores:
            fact_score = extract_document_memory_score(content_by_path.get(path, ""))
            scores[path] *= memory_boost(fact_score)

    sorted_paths = sorted(scores.keys(), key=lambda p: -scores[p])

    results = []
    for path in sorted_paths[:n]:
        r = result_map[path]
        r.rank = scores[path]
        results.append(r)

    return results
