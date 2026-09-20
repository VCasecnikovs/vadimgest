"""Bounded local-model check. Run: python -m tests.retrieval_quality [model-name].

Synthetic facts only; results measure this fixture, not production recall quality.
Downloads the selected free fastembed model when absent from the cache.
"""
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

from vadimgest.search.embedder import LocalEmbedder
from vadimgest.search.indexer import get_db, index_embeddings
from vadimgest.search.searcher import search_semantic

# Four English and four Russian facts, each with an English and Russian paraphrase.
CASES = [
    ("Elena agreed to send the written advisor proposal on Tuesday evening.",
     "When will the advisory offer arrive?", "Когда пришлют письменное предложение советнику?"),
    ("The robot trial was postponed because its battery overheated.",
     "Why was the robotics experiment delayed?", "Почему перенесли испытание робота?"),
    ("The client paid seven thousand five hundred euros; the transfer reached the bank on Monday.",
     "How much money actually arrived in the account?", "Сколько денег уже поступило на банковский счёт?"),
    ("Priya offered an introduction to the laboratory director, but has not made the introduction yet.",
     "Was the connection to the lab director already arranged?", "Знакомство с руководителем лаборатории уже состоялось?"),
    ("Дмитрий отказался от поездки в Берлин из-за отмены конференции.",
     "Why did Dmitry cancel his Berlin travel?", "По какой причине Дмитрий никуда не поедет?"),
    ("Соглашение подписано, но инвестор ещё не перевёл деньги.",
     "Has the investor transferred funds after signing?", "Инвестиции уже поступили после подписания договора?"),
    ("Новая прошивка сломала калибровку датчиков, поэтому вернули предыдущую версию.",
     "Why was the firmware rolled back?", "Из-за чего пришлось откатить обновление устройства?"),
    ("Мария прислала образцы ткани курьером; посылка будет в четверг.",
     "When will the fabric samples be delivered?", "Когда курьер привезёт материалы от Марии?"),
]


def main():
    if len(sys.argv) > 1:
        os.environ["VADIMGEST_LOCAL_MODEL"] = sys.argv[1]
    started = time.monotonic()
    embedder = LocalEmbedder()
    facts = [case[0] for case in CASES]
    queries = [query for case in CASES for query in case[1:]]
    expected = [i for i in range(len(CASES)) for _ in range(2)]
    doc_vectors = embedder.embed(facts)
    query_vectors = embedder.embed(queries, task="query")
    winners = [max(range(len(facts)), key=lambda i: sum(a * b for a, b in zip(q, doc_vectors[i])))
               for q in query_vectors]
    short = {lang: sum(winners[i] == expected[i] for i in range(offset, len(queries), 2))
             for lang, offset in (("en", 0), ("ru", 1))}
    with tempfile.TemporaryDirectory() as directory:
        db_path = Path(directory) / "test.db"
        conn = get_db(db_path)
        for i, fact in enumerate(facts):
            source = "obsidian" if i % 2 else "telegram"
            path = f"{source}:{i}"
            content = "Routine agenda and small talk. " * 150 + fact + "\nGeneral scheduling discussion. " * 150
            conn.execute("INSERT INTO docs(path, source, title, content) VALUES (?, ?, ?, ?)",
                         (path, source, f"Synthetic record {i}", content))
            conn.execute("INSERT INTO meta(path, source, size) VALUES (?, ?, ?)", (path, source, len(content)))
            for start, end in embedder.passages(content):
                assert len(embedder._tokenizer.encode(content[start:end], add_special_tokens=False).ids) <= embedder._max_tokens
        conn.commit()
        conn.close()
        with patch("vadimgest.search.embedder.get_embedder", return_value=embedder):
            stats = index_embeddings(db_path, provider="local", sources=("obsidian", "telegram"), batch_size=16)
            hits = [search_semantic(q, n=1, db_path=db_path, provider="local")[0] for q in queries]
        middle = {lang: sum(hits[i].path.endswith(f":{expected[i]}") for i in range(offset, len(queries), 2))
                  for lang, offset in (("en", 0), ("ru", 1))}
        evidence = sum(facts[expected[i]] in hit.snippet for i, hit in enumerate(hits))
        print(json.dumps({"model": embedder._MODEL, "questions_per_language": len(CASES),
                          "short_top1": short, "middle_top1": middle, "correct_evidence_snippets": evidence,
                          "indexed_docs": stats["embedded"], "passages": stats["passages"],
                          "seconds": round(time.monotonic() - started, 1)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
