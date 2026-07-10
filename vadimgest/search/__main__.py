#!/usr/bin/env python3
"""
vadimgest search - FTS5 + vector search over Obsidian vault, skills, and JSONL.

Usage:
    # FTS5 full-text search (default)
    vadimgest search "query" --md               # Obsidian + skills
    vadimgest search "query" --raw              # JSONL sources only
    vadimgest search "query" -s telegram        # Specific JSONL source
    vadimgest search "query" --sources telegram,signal,gmail,bee
    vadimgest search "query" --md --raw         # Everything
    vadimgest search "query" -n 20 --full       # More results, full content
    vadimgest search "query" --json             # JSON output
    vadimgest search "query" --raw --chat "X"   # Filter by chat
    vadimgest search "query" --md --folder "People"  # Filter by folder

    # Semantic search (embedding-based)
    vadimgest search "query" --md --vec --provider gemini
    vadimgest search "query" --raw --vec --provider ollama

    # Hybrid search (FTS5 + embeddings + RRF fusion)
    vadimgest search "query" --md --hybrid --provider gemini

    # Index management
    vadimgest search index                      # Build/update FTS5 index
    vadimgest search index --rebuild            # Full rebuild
    vadimgest search index --rebuild-source gmail
    vadimgest search stats                      # Index statistics

    # Embedding management
    vadimgest search embed --provider local --sources obsidian,skills,telegram,signal,gmail,bee
    vadimgest search embed --provider ollama --limit 100  # First N docs
    vadimgest search embed --stats              # Embedding coverage
"""

import json
import sys
import time
from pathlib import Path

from .indexer import DEFAULT_DB, DEFAULT_VAULT, DEFAULT_JSONL_DIR, index, stats, reindex_stale
from .searcher import search, search_semantic, search_hybrid


def _ensure_index(db_path: Path):
    """Auto-index if database doesn't exist, and reindex stale JSONL sources."""
    if not db_path.exists():
        print("First run - building index...", file=sys.stderr, flush=True)
        t0 = time.time()
        result = index(db_path=db_path)
        dt = time.time() - t0
        total = sum(r.get("added", 0) + r.get("total", 0) for r in result.values())
        print(f"Indexed {total} docs ({dt:.1f}s)", file=sys.stderr)
    else:
        stale = reindex_stale(db_path=db_path)
        if stale:
            added = sum(r.get("added", 0) for r in stale.values())
            sources = ", ".join(f"{s}(+{r['added']})" for s, r in stale.items())
            print(f"Auto-indexed {added} new: {sources}", file=sys.stderr)


def _openable_path(result) -> str:
    prefix = f"{result.source}:"
    if result.source not in {"obsidian", "skills"} and result.path.startswith(prefix):
        line = result.path[len(prefix):]
        if line.isdigit():
            return f"{DEFAULT_JSONL_DIR / f'{result.source}.jsonl'}#L{int(line) + 1}"
    return result.path.split(":", 1)[1] if ":" in result.path else result.path


def _print_results(results, as_json: bool = False):
    """Format and print search results."""
    if not results:
        print("No results.")
        return

    if as_json:
        out = [{"path": _openable_path(r), "source": r.source, "title": r.title,
                "snippet": r.snippet, "rank": r.rank,
                "chat": r.chat, "folder": r.folder}
               for r in results]
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return

    for i, r in enumerate(results, 1):
        src_tag = f"\033[36m[{r.source}]\033[0m "
        chat_tag = f"\033[35m{r.chat}\033[0m " if r.chat else ""
        print(f"\033[1m{i}. {src_tag}{chat_tag}{r.title}\033[0m")
        display_path = _openable_path(r)
        print(f"   {display_path}")
        if r.snippet:
            snippet = r.snippet.replace(">>>", "\033[33m").replace("<<<", "\033[0m")
            for line in snippet.split("\n")[:3]:
                line = line.strip()
                if line:
                    print(f"   {line}")
        print()


def cmd_search(query: str, n: int = 10, source: str | None = None,
               sources: tuple[str, ...] | None = None,
               md: bool = False, raw: bool = False, full: bool = False,
               as_json: bool = False, chat: str | None = None,
               folder: str | None = None, db_path: Path = DEFAULT_DB):
    _ensure_index(db_path)
    results = search(query, n=n, db_path=db_path, source=source, sources=sources,
                     md=md, raw=raw,
                     full=full, chat=chat, folder=folder)
    _print_results(results, as_json)


def cmd_search_vec(query: str, n: int = 10, source: str | None = None,
                   sources: tuple[str, ...] | None = None,
                   md: bool = False, raw: bool = False, full: bool = False,
                   as_json: bool = False, provider: str = "gemini",
                   chat: str | None = None, folder: str | None = None,
                   db_path: Path = DEFAULT_DB):
    _ensure_index(db_path)
    results = search_semantic(query, n=n, db_path=db_path, source=source, sources=sources,
                              md=md, raw=raw,
                              full=full, provider=provider, chat=chat, folder=folder)
    _print_results(results, as_json)


def cmd_search_hybrid(query: str, n: int = 10, source: str | None = None,
                      sources: tuple[str, ...] | None = None,
                      md: bool = False, raw: bool = False, full: bool = False,
                      as_json: bool = False, provider: str = "gemini",
                      chat: str | None = None, folder: str | None = None,
                      db_path: Path = DEFAULT_DB):
    _ensure_index(db_path)
    results = search_hybrid(query, n=n, db_path=db_path, source=source, sources=sources,
                            md=md, raw=raw,
                            full=full, provider=provider, chat=chat, folder=folder)
    _print_results(results, as_json)


def cmd_index(rebuild: bool = False, exclude: set[str] | None = None,
              vault: Path = DEFAULT_VAULT, jsonl_dir: Path = DEFAULT_JSONL_DIR,
              db_path: Path = DEFAULT_DB, md_only: bool = False,
              rebuild_sources: set[str] | None = None):
    print(f"Indexing...")
    if exclude:
        print(f"  Excluding: {', '.join(sorted(exclude))}")
    if rebuild_sources:
        print(f"  Rebuilding sources: {', '.join(sorted(rebuild_sources))}")
    t0 = time.time()
    results = index(vault=vault, jsonl_dir=jsonl_dir, db_path=db_path,
                    rebuild=rebuild, exclude=exclude, md_only=md_only,
                    rebuild_sources=rebuild_sources)
    dt = time.time() - t0

    print(f"Done in {dt:.1f}s:")
    total_added = 0
    for source, r in sorted(results.items()):
        added = r.get("added", 0)
        total_added += added
        total = r.get("total", 0)
        extra = ""
        if r.get("unchanged"):
            extra = f"  unchanged={r['unchanged']}"
        if r.get("updated"):
            extra += f"  updated={r['updated']}"
        if r.get("removed"):
            extra += f"  removed={r['removed']}"
        if r.get("skipped"):
            extra = f"  skipped={r['skipped']}"
        print(f"  {source:20} +{added:>6} / {total:>6} total{extra}")
    print(f"  {'TOTAL':20} +{total_added:>6}")


def cmd_embed(provider: str, limit: int | None = None, db_path: Path = DEFAULT_DB,
              rebuild: bool = False, sources: tuple[str, ...] | None = None,
              batch_size: int = 10, max_batch_chars: int = 64_000):
    from .indexer import index_embeddings
    print(f"Embedding with {provider}...", file=sys.stderr, flush=True)
    t0 = time.time()
    result = index_embeddings(
        db_path=db_path,
        provider=provider,
        limit=limit,
        rebuild=rebuild,
        sources=sources,
        batch_size=batch_size,
        max_batch_chars=max_batch_chars,
    )
    dt = time.time() - t0
    print(f"Done in {dt:.1f}s: embedded={result['embedded']}, "
          f"skipped={result['skipped']}, pruned={result.get('pruned', 0)}, "
          f"total={result['total']}")


def cmd_embed_stats(db_path: Path = DEFAULT_DB):
    from .indexer import embed_stats
    s = embed_stats(db_path)
    print(f"Total docs: {s['total_docs']}")
    print(f"Embedded:   {s['embedded']}")
    print(f"Coverage:   {s['coverage']}%")
    print(f"Space:      {s.get('embedding_space') or 'unknown'}")
    if s.get("embedding_sources"):
        print(f"Sources:    {', '.join(s['embedding_sources'])}")
    for source, count in s.get("source_counts", {}).items():
        print(f"  {source:20} {count:>8}")


def cmd_stats(db_path: Path = DEFAULT_DB):
    s = stats(db_path)

    if s["total"] == 0:
        print("No index yet. Run: python3 -m vadimgest.search index")
        return

    mb = s["db_size"] / 1024 / 1024
    print(f"Database: {db_path} ({mb:.1f} MB)")
    print(f"Total indexed: {s['total']}")
    print()
    for source, count in sorted(s["sources"].items(), key=lambda x: -x[1]):
        print(f"  {source:20} {count:>8}")


def main():
    args = sys.argv[1:]

    if not args or args[0] in ("-h", "--help"):
        print(__doc__.strip())
        return

    # --- Commands ---

    if args[0] == "index":
        rebuild = "--rebuild" in args
        md_only = "--md-only" in args
        exclude = set()
        rebuild_sources = set()
        i = 1
        while i < len(args):
            if args[i] == "--exclude" and i + 1 < len(args):
                exclude.add(args[i + 1])
                i += 2
            elif args[i] == "--rebuild-source" and i + 1 < len(args):
                rebuild_sources.add(args[i + 1])
                i += 2
            else:
                i += 1
        cmd_index(
            rebuild=rebuild,
            exclude=exclude or None,
            md_only=md_only,
            rebuild_sources=rebuild_sources or None,
        )
        return

    if args[0] == "stats":
        cmd_stats()
        return

    if args[0] == "embed":
        provider = None
        limit = None
        show_stats = False
        rebuild = False
        sources = None
        batch_size = 10
        max_batch_chars = 64_000
        i = 1
        while i < len(args):
            if args[i] == "--provider" and i + 1 < len(args):
                provider = args[i + 1]
                i += 2
            elif args[i] == "--limit" and i + 1 < len(args):
                limit = int(args[i + 1])
                i += 2
            elif args[i] == "--stats":
                show_stats = True
                i += 1
            elif args[i] == "--rebuild":
                rebuild = True
                i += 1
            elif args[i] == "--sources" and i + 1 < len(args):
                sources = tuple(s.strip() for s in args[i + 1].split(",") if s.strip())
                i += 2
            elif args[i] == "--batch-size" and i + 1 < len(args):
                batch_size = int(args[i + 1])
                i += 2
            elif args[i] == "--max-batch-chars" and i + 1 < len(args):
                max_batch_chars = int(args[i + 1])
                i += 2
            else:
                i += 1
        if show_stats:
            cmd_embed_stats()
        elif not provider:
            print("Error: --provider required. Use: --provider local|gemini|openai|ollama")
            sys.exit(1)
        else:
            cmd_embed(
                provider=provider,
                limit=limit,
                rebuild=rebuild,
                sources=sources,
                batch_size=batch_size,
                max_batch_chars=max_batch_chars,
            )
        return

    # --- Search mode ---
    query = args[0]
    n = 10
    full = False
    as_json = False
    source = None
    sources = None
    md = False
    raw = False
    chat = None
    folder = None
    vec = False
    hybrid = False
    provider = None

    i = 1
    while i < len(args):
        if args[i] == "-n" and i + 1 < len(args):
            n = int(args[i + 1])
            i += 2
        elif args[i] in ("-s", "--source") and i + 1 < len(args):
            source = args[i + 1]
            i += 2
        elif args[i] == "--sources" and i + 1 < len(args):
            sources = tuple(s.strip() for s in args[i + 1].split(",") if s.strip())
            i += 2
        elif args[i] == "--chat" and i + 1 < len(args):
            chat = args[i + 1]
            i += 2
        elif args[i] == "--folder" and i + 1 < len(args):
            folder = args[i + 1]
            i += 2
        elif args[i] == "--provider" and i + 1 < len(args):
            provider = args[i + 1]
            i += 2
        elif args[i] == "--md":
            md = True
            i += 1
        elif args[i] == "--raw":
            raw = True
            i += 1
        elif args[i] == "--full":
            full = True
            i += 1
        elif args[i] == "--json":
            as_json = True
            i += 1
        elif args[i] == "--vec":
            vec = True
            i += 1
        elif args[i] == "--hybrid":
            hybrid = True
            i += 1
        else:
            i += 1

    if not md and not raw and not source and not sources:
        print("Specify scope: --md, --raw, -s SOURCE, or --sources SOURCE1,SOURCE2")
        sys.exit(1)

    if (vec or hybrid) and not provider:
        print("Error: --provider required with --vec/--hybrid. Use: --provider gemini|openai|ollama")
        sys.exit(1)

    if hybrid:
        cmd_search_hybrid(query, n=n, source=source, sources=sources,
                          md=md, raw=raw, full=full,
                          as_json=as_json, provider=provider, chat=chat, folder=folder)
    elif vec:
        cmd_search_vec(query, n=n, source=source, sources=sources,
                       md=md, raw=raw, full=full,
                       as_json=as_json, provider=provider, chat=chat, folder=folder)
    else:
        cmd_search(query, n=n, source=source, sources=sources,
                   md=md, raw=raw, full=full,
                   as_json=as_json, chat=chat, folder=folder)


if __name__ == "__main__":
    main()
