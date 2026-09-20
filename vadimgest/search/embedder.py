"""Embedding providers for semantic search."""

import os
import struct
import time
from abc import ABC, abstractmethod


class Embedder(ABC):
    """Abstract embedding provider. All implementations output 768-dim vectors."""
    dim: int = 768

    @abstractmethod
    def embed(self, texts: list[str], task: str = "document") -> list[list[float]]:
        """Embed a batch of texts. task='document' for indexing, 'query' for search."""
        ...

    def embed_one(self, text: str, task: str = "document") -> list[float]:
        return self.embed([text], task=task)[0]

    def space(self, provider: str) -> str:
        model = getattr(self, "_MODEL", None) or getattr(self, "model", None) or self.__class__.__name__
        return f"{provider}:{model}:{self.dim}"

    def passages(self, text: str):
        """Overlapping spans for providers without a local tokenizer."""
        for start in range(0, max(1, len(text)), 1080):
            end = min(start + 1200, len(text))
            yield start, end
            if end == len(text):
                break

    @staticmethod
    def serialize(vec: list[float]) -> bytes:
        """Pack float list to BLOB for sqlite-vec."""
        return struct.pack(f"{len(vec)}f", *vec)


class GeminiEmbedder(Embedder):
    """Gemini embedding-001 via REST API. Free tier: 1500 req/min."""

    def __init__(self, api_key: str | None = None):
        import httpx
        self.api_key = api_key or os.environ.get("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not set. Get one at https://aistudio.google.com/")
        self._client = httpx.Client(timeout=30.0)

    _TASK_MAP = {"document": "RETRIEVAL_DOCUMENT", "query": "RETRIEVAL_QUERY"}

    def _batch_embed(self, texts: list[str], task: str = "document") -> list[list[float]]:
        """Batch embed with retry on 429."""
        task_type = self._TASK_MAP.get(task, "RETRIEVAL_DOCUMENT")
        requests = [
            {"model": "models/gemini-embedding-001",
             "content": {"parts": [{"text": t[:8000]}]},
             "taskType": task_type,
             "outputDimensionality": self.dim}
            for t in texts
        ]
        for attempt in range(4):
            resp = self._client.post(
                "https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:batchEmbedContents",
                headers={"x-goog-api-key": self.api_key},
                json={"requests": requests},
            )
            if resp.status_code == 200:
                return [e["values"] for e in resp.json()["embeddings"]]
            if resp.status_code == 429:
                wait = 2 ** attempt
                time.sleep(wait)
                continue
            break  # non-retryable error
        return None  # signal failure

    def embed(self, texts: list[str], task: str = "document") -> list[list[float]]:
        result = self._batch_embed(texts, task=task)
        if result is not None:
            return result
        # Fallback: one by one with rate limiting
        import sys
        task_type = self._TASK_MAP.get(task, "RETRIEVAL_DOCUMENT")
        print(f"  Batch failed, falling back to single...", file=sys.stderr)
        results = []
        for t in texts:
            for attempt in range(3):
                r = self._client.post(
                    "https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent",
                    headers={"x-goog-api-key": self.api_key},
                    json={"content": {"parts": [{"text": t[:8000]}]},
                          "taskType": task_type,
                          "outputDimensionality": self.dim},
                )
                if r.status_code == 200:
                    results.append(r.json()["embedding"]["values"])
                    break
                if r.status_code == 429:
                    time.sleep(2 ** attempt)
                    continue
                results.append([0.0] * self.dim)
                break
            else:
                results.append([0.0] * self.dim)
        return results


class OpenAIEmbedder(Embedder):
    """OpenAI text-embedding-3-small. Truncated to 768 dims via API."""

    def __init__(self, api_key: str | None = None):
        import httpx
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY not set")
        self._client = httpx.Client(timeout=30.0)

    def embed(self, texts: list[str], task: str = "document") -> list[list[float]]:
        resp = self._client.post(
            "https://api.openai.com/v1/embeddings",
            headers={"Authorization": f"Bearer {self.api_key}"},
            json={"model": "text-embedding-3-small",
                  "input": [t[:8000] for t in texts],
                  "dimensions": self.dim},
        )
        resp.raise_for_status()
        data = resp.json()["data"]
        return [d["embedding"] for d in sorted(data, key=lambda x: x["index"])]


class OllamaEmbedder(Embedder):
    """Ollama nomic-embed-text. Local, free, 768-dim."""

    def __init__(self, model: str = "nomic-embed-text",
                 base_url: str = "http://localhost:11434"):
        import httpx
        self.model = model
        self._client = httpx.Client(base_url=base_url, timeout=120.0)

    def embed(self, texts: list[str], task: str = "document") -> list[list[float]]:
        # Ollama sums input lengths for context limit - embed one at a time
        results = []
        for t in texts:
            try:
                resp = self._client.post(
                    "/api/embed",
                    json={"model": self.model, "input": t[:4000]},
                )
                resp.raise_for_status()
                results.append(resp.json()["embeddings"][0])
            except Exception:
                results.append([0.0] * self.dim)
        return results


class LocalEmbedder(Embedder):
    """Local embeddings; VADIMGEST_LOCAL_MODEL selects a compatible 768-dim model.

    Changing models requires --rebuild and the same selection for every reader.
    """

    _MODEL = "BAAI/bge-base-en-v1.5"
    _QUERY_PREFIX = "Represent this sentence for searching relevant passages: "

    def __init__(self):
        from fastembed import TextEmbedding

        self._MODEL = os.environ.get("VADIMGEST_LOCAL_MODEL", self._MODEL)
        self._model = TextEmbedding(model_name=self._MODEL)
        if self._model.embedding_size != self.dim:
            raise ValueError(f"Local model must output {self.dim} dimensions")
        from copy import deepcopy
        self._tokenizer = deepcopy(self._model.model.tokenizer)
        # Short overlapping passages retain individual facts; MPNet was trained
        # with a 128-token sentence limit, including special tokens.
        self._max_tokens = min(126, self._tokenizer.truncation["max_length"] - 2)
        self._tokenizer.no_truncation()
        self._tokenizer.no_padding()

    def passages(self, text: str):
        offsets = self._tokenizer.encode(text, add_special_tokens=False).offsets
        if not offsets:
            yield 0, len(text)
            return
        overlap = min(48, self._max_tokens // 4)
        i = 0
        while i < len(offsets):
            last = min(i + self._max_tokens, len(offsets))
            start = offsets[i][0] if i else 0
            end = offsets[last - 1][1] if last < len(offsets) else len(text)
            # A slice beginning inside a word can retokenize to more tokens.
            while last > i + 1 and len(self._tokenizer.encode(text[start:end], add_special_tokens=False).ids) > self._max_tokens:
                last -= 1
                end = offsets[last - 1][1]
            yield start, end
            if last == len(offsets):
                break
            i = max(i + 1, last - overlap)

    def embed(self, texts: list[str], task: str = "document") -> list[list[float]]:
        if task == "query" and self._MODEL.startswith("BAAI/bge-base-en"):
            texts = [self._QUERY_PREFIX + text for text in texts]
        vectors = [vector.tolist() for vector in self._model.embed(texts)]
        # sqlite-vec uses L2 distance; normalize models that return unnormalized vectors.
        for vector in vectors:
            norm = sum(v * v for v in vector) ** 0.5 or 1.0
            vector[:] = [value / norm for value in vector]
        return vectors


PROVIDERS = {
    "gemini": GeminiEmbedder,
    "openai": OpenAIEmbedder,
    "ollama": OllamaEmbedder,
    "local": LocalEmbedder,
}


def get_embedder(provider: str = "gemini") -> Embedder:
    """Factory function."""
    cls = PROVIDERS.get(provider)
    if not cls:
        raise ValueError(f"Unknown provider: {provider}. Choose from: {', '.join(PROVIDERS)}")
    return cls()
