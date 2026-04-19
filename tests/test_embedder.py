"""Tests for vadimgest/search/embedder.py - all embedding providers."""

import struct
from unittest.mock import patch, MagicMock

import pytest

from vadimgest.search.embedder import (
    Embedder,
    GeminiEmbedder,
    OpenAIEmbedder,
    OllamaEmbedder,
    get_embedder,
    PROVIDERS,
)


# ===========================================================================
# GeminiEmbedder - _batch_embed retry + fallback (lines 57-92)
# ===========================================================================

class TestGeminiEmbedder:
    def _make_embedder(self):
        """Create GeminiEmbedder with a fake API key."""
        with patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"}):
            return GeminiEmbedder()

    def test_batch_embed_success(self):
        """Happy path: batch embed returns vectors."""
        emb = self._make_embedder()
        fake_resp = MagicMock()
        fake_resp.status_code = 200
        fake_resp.json.return_value = {
            "embeddings": [{"values": [0.1] * 768}, {"values": [0.2] * 768}]
        }
        emb._client = MagicMock()
        emb._client.post.return_value = fake_resp

        result = emb.embed(["text1", "text2"])
        assert len(result) == 2
        assert len(result[0]) == 768

    def test_batch_embed_429_retries(self):
        """Lines 57-60: 429 triggers retry with backoff."""
        emb = self._make_embedder()
        resp_429 = MagicMock()
        resp_429.status_code = 429

        resp_ok = MagicMock()
        resp_ok.status_code = 200
        resp_ok.json.return_value = {"embeddings": [{"values": [0.1] * 768}]}

        emb._client = MagicMock()
        emb._client.post.side_effect = [resp_429, resp_ok]

        with patch("time.sleep"):
            result = emb.embed(["text1"])

        assert len(result) == 1
        assert emb._client.post.call_count == 2

    def test_batch_embed_non_retryable_error_falls_back(self):
        """Lines 61-62: Non-retryable error returns None, triggering fallback."""
        emb = self._make_embedder()
        resp_500 = MagicMock()
        resp_500.status_code = 500

        # Batch fails with 500
        # Single fallback succeeds
        resp_ok_single = MagicMock()
        resp_ok_single.status_code = 200
        resp_ok_single.json.return_value = {"embedding": {"values": [0.3] * 768}}

        emb._client = MagicMock()
        # First call: batch -> 500 (returns None)
        # Then fallback single calls succeed
        emb._client.post.side_effect = [resp_500, resp_ok_single]

        result = emb.embed(["text1"])
        assert len(result) == 1
        assert result[0] == [0.3] * 768

    def test_batch_embed_all_429s_falls_back(self):
        """Lines 57-62: All 4 batch retries fail with 429, then fallback to single."""
        emb = self._make_embedder()
        resp_429 = MagicMock()
        resp_429.status_code = 429

        resp_ok_single = MagicMock()
        resp_ok_single.status_code = 200
        resp_ok_single.json.return_value = {"embedding": {"values": [0.5] * 768}}

        emb._client = MagicMock()
        # 4 batch 429s + 1 single success
        emb._client.post.side_effect = [resp_429, resp_429, resp_429, resp_429, resp_ok_single]

        with patch("time.sleep"):
            result = emb.embed(["text1"])

        assert len(result) == 1
        assert result[0] == [0.5] * 768

    def test_fallback_single_429_retries(self):
        """Lines 85-86: Single embed 429 retry in fallback mode."""
        emb = self._make_embedder()

        # Batch fails
        resp_500 = MagicMock()
        resp_500.status_code = 500

        resp_429 = MagicMock()
        resp_429.status_code = 429

        resp_ok = MagicMock()
        resp_ok.status_code = 200
        resp_ok.json.return_value = {"embedding": {"values": [0.7] * 768}}

        emb._client = MagicMock()
        emb._client.post.side_effect = [resp_500, resp_429, resp_ok]

        with patch("time.sleep"):
            result = emb.embed(["text1"])

        assert len(result) == 1
        assert result[0] == [0.7] * 768

    def test_fallback_single_non_retryable_returns_zeros(self):
        """Lines 88-89: Non-retryable error in single mode returns zero vector."""
        emb = self._make_embedder()

        resp_500 = MagicMock()
        resp_500.status_code = 500

        emb._client = MagicMock()
        # Batch 500, then single 500
        emb._client.post.side_effect = [resp_500, resp_500]

        result = emb.embed(["text1"])
        assert len(result) == 1
        assert result[0] == [0.0] * 768

    def test_fallback_single_all_429s_returns_zeros(self):
        """Lines 90-91: All single retries fail with 429 -> zero vector."""
        emb = self._make_embedder()

        resp_500_batch = MagicMock()
        resp_500_batch.status_code = 500

        resp_429 = MagicMock()
        resp_429.status_code = 429

        emb._client = MagicMock()
        # Batch 500, then 3 single 429s (exhausts retries)
        emb._client.post.side_effect = [resp_500_batch, resp_429, resp_429, resp_429]

        with patch("time.sleep"):
            result = emb.embed(["text1"])

        assert len(result) == 1
        assert result[0] == [0.0] * 768

    def test_embed_multiple_texts_fallback(self):
        """Lines 73-91: Multiple texts in fallback mode."""
        emb = self._make_embedder()

        resp_500 = MagicMock()
        resp_500.status_code = 500

        resp_ok1 = MagicMock()
        resp_ok1.status_code = 200
        resp_ok1.json.return_value = {"embedding": {"values": [0.1] * 768}}

        resp_ok2 = MagicMock()
        resp_ok2.status_code = 200
        resp_ok2.json.return_value = {"embedding": {"values": [0.2] * 768}}

        emb._client = MagicMock()
        emb._client.post.side_effect = [resp_500, resp_ok1, resp_ok2]

        result = emb.embed(["text1", "text2"])
        assert len(result) == 2
        assert result[0] == [0.1] * 768
        assert result[1] == [0.2] * 768

    def test_task_map(self):
        """Verify task types map correctly."""
        emb = self._make_embedder()
        assert emb._TASK_MAP["document"] == "RETRIEVAL_DOCUMENT"
        assert emb._TASK_MAP["query"] == "RETRIEVAL_QUERY"

    def test_init_with_explicit_key(self):
        """Constructor with explicit api_key."""
        emb = GeminiEmbedder(api_key="my-key")
        assert emb.api_key == "my-key"

    def test_init_from_env(self):
        """Constructor reads from GEMINI_API_KEY env."""
        with patch.dict("os.environ", {"GEMINI_API_KEY": "env-key"}):
            emb = GeminiEmbedder()
        assert emb.api_key == "env-key"

    def test_init_no_key_raises(self):
        """Missing API key raises ValueError."""
        with patch.dict("os.environ", {}, clear=True):
            import os
            old = os.environ.pop("GEMINI_API_KEY", None)
            try:
                with pytest.raises(ValueError, match="GEMINI_API_KEY"):
                    GeminiEmbedder()
            finally:
                if old:
                    os.environ["GEMINI_API_KEY"] = old


# ===========================================================================
# OpenAIEmbedder (lines 99-115)
# ===========================================================================

class TestOpenAIEmbedder:
    def test_init_with_key(self):
        """Line 99-103: Constructor with explicit key."""
        emb = OpenAIEmbedder(api_key="sk-test")
        assert emb.api_key == "sk-test"

    def test_init_from_env(self):
        """Line 100: Reads OPENAI_API_KEY from env."""
        with patch.dict("os.environ", {"OPENAI_API_KEY": "sk-env"}):
            emb = OpenAIEmbedder()
        assert emb.api_key == "sk-env"

    def test_init_no_key_raises(self):
        """Line 102: Missing key raises ValueError."""
        with patch.dict("os.environ", {}, clear=True):
            import os
            old = os.environ.pop("OPENAI_API_KEY", None)
            try:
                with pytest.raises(ValueError, match="OPENAI_API_KEY"):
                    OpenAIEmbedder()
            finally:
                if old:
                    os.environ["OPENAI_API_KEY"] = old

    def test_embed_success(self):
        """Lines 106-115: Successful embedding with sorting by index."""
        emb = OpenAIEmbedder(api_key="sk-test")
        fake_resp = MagicMock()
        fake_resp.status_code = 200
        fake_resp.raise_for_status = MagicMock()
        fake_resp.json.return_value = {
            "data": [
                {"index": 1, "embedding": [0.2] * 768},
                {"index": 0, "embedding": [0.1] * 768},
            ]
        }
        emb._client = MagicMock()
        emb._client.post.return_value = fake_resp

        result = emb.embed(["text1", "text2"])
        assert len(result) == 2
        # Should be sorted by index
        assert result[0] == [0.1] * 768
        assert result[1] == [0.2] * 768

    def test_embed_raises_on_error(self):
        """Lines 113: raise_for_status propagates HTTP errors."""
        emb = OpenAIEmbedder(api_key="sk-test")
        fake_resp = MagicMock()
        fake_resp.raise_for_status.side_effect = Exception("401 Unauthorized")
        emb._client = MagicMock()
        emb._client.post.return_value = fake_resp

        with pytest.raises(Exception, match="401"):
            emb.embed(["text1"])

    def test_embed_truncates_long_text(self):
        """Lines 110: Input truncated to 8000 chars."""
        emb = OpenAIEmbedder(api_key="sk-test")
        fake_resp = MagicMock()
        fake_resp.raise_for_status = MagicMock()
        fake_resp.json.return_value = {
            "data": [{"index": 0, "embedding": [0.1] * 768}]
        }
        emb._client = MagicMock()
        emb._client.post.return_value = fake_resp

        long_text = "x" * 20000
        emb.embed([long_text])

        call_json = emb._client.post.call_args[1]["json"]
        assert len(call_json["input"][0]) == 8000


# ===========================================================================
# OllamaEmbedder (lines 123-140)
# ===========================================================================

class TestOllamaEmbedder:
    def test_init_defaults(self):
        """Lines 123-125: Default model and base_url."""
        emb = OllamaEmbedder()
        assert emb.model == "nomic-embed-text"

    def test_init_custom(self):
        """Lines 121-122: Custom model and base_url."""
        emb = OllamaEmbedder(model="mxbai-embed-large", base_url="http://remote:11434")
        assert emb.model == "mxbai-embed-large"

    def test_embed_success(self):
        """Lines 129-137: Successful embedding one at a time."""
        emb = OllamaEmbedder()
        fake_resp = MagicMock()
        fake_resp.status_code = 200
        fake_resp.raise_for_status = MagicMock()
        fake_resp.json.return_value = {"embeddings": [[0.1] * 768]}
        emb._client = MagicMock()
        emb._client.post.return_value = fake_resp

        result = emb.embed(["text1", "text2"])
        assert len(result) == 2
        assert emb._client.post.call_count == 2

    def test_embed_error_returns_zeros(self):
        """Lines 138-139: Exception returns zero vector."""
        emb = OllamaEmbedder()
        emb._client = MagicMock()
        emb._client.post.side_effect = Exception("connection refused")

        result = emb.embed(["text1"])
        assert len(result) == 1
        assert result[0] == [0.0] * 768

    def test_embed_mixed_success_and_failure(self):
        """Lines 129-139: Mix of successful and failed embeddings."""
        emb = OllamaEmbedder()

        resp_ok = MagicMock()
        resp_ok.raise_for_status = MagicMock()
        resp_ok.json.return_value = {"embeddings": [[0.5] * 768]}

        emb._client = MagicMock()
        emb._client.post.side_effect = [resp_ok, Exception("timeout"), resp_ok]

        result = emb.embed(["text1", "text2", "text3"])
        assert len(result) == 3
        assert result[0] == [0.5] * 768
        assert result[1] == [0.0] * 768  # failed
        assert result[2] == [0.5] * 768

    def test_embed_truncates_to_4000(self):
        """Lines 134: Input truncated to 4000 chars."""
        emb = OllamaEmbedder()
        fake_resp = MagicMock()
        fake_resp.raise_for_status = MagicMock()
        fake_resp.json.return_value = {"embeddings": [[0.1] * 768]}
        emb._client = MagicMock()
        emb._client.post.return_value = fake_resp

        long_text = "y" * 10000
        emb.embed([long_text])

        call_json = emb._client.post.call_args[1]["json"]
        assert len(call_json["input"]) == 4000


# ===========================================================================
# get_embedder factory (line 146-151)
# ===========================================================================

class TestGetEmbedder:
    def test_all_providers_registered(self):
        assert "gemini" in PROVIDERS
        assert "openai" in PROVIDERS
        assert "ollama" in PROVIDERS

    def test_get_embedder_ollama(self):
        """Ollama doesn't need API key."""
        emb = get_embedder("ollama")
        assert isinstance(emb, OllamaEmbedder)

    def test_get_embedder_unknown(self):
        with pytest.raises(ValueError, match="Unknown provider"):
            get_embedder("anthropic")


# ===========================================================================
# Embedder base class
# ===========================================================================

class TestEmbedderBase:
    def test_embed_one_delegates(self):
        """embed_one calls embed with single-element list."""

        class TestEmb(Embedder):
            def embed(self, texts, task="document"):
                return [[float(len(t))] * self.dim for t in texts]

        emb = TestEmb()
        result = emb.embed_one("hello")
        assert len(result) == 768
        assert result[0] == 5.0

    def test_serialize_empty(self):
        blob = Embedder.serialize([])
        assert blob == b""

    def test_serialize_single(self):
        blob = Embedder.serialize([1.0])
        assert len(blob) == 4
        val = struct.unpack("f", blob)[0]
        assert abs(val - 1.0) < 1e-6
