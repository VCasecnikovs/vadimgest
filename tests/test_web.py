"""Tests for vadimgest web dashboard - API endpoints and JS integrity."""

import json
import os
import subprocess
import re
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from vadimgest.store import DataStore
from vadimgest.web.app import _STATIC_DEPS


@pytest.fixture
def store(tmp_path):
    return DataStore(tmp_path)


@pytest.fixture
def app(store):
    from vadimgest.web.app import create_app
    app = create_app(store)
    app.config["TESTING"] = True
    return app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def fresh_env(tmp_path, monkeypatch):
    """Fully isolated environment simulating a fresh install.

    Redirects XDG dirs and env vars so config, data, and credentials
    all live in tmp_path. No real config leaks in.
    """
    config_home = tmp_path / "config"
    data_home = tmp_path / "data"
    config_home.mkdir()
    data_home.mkdir()

    monkeypatch.setenv("XDG_CONFIG_HOME", str(config_home))
    monkeypatch.setenv("VADIMGEST_DATA_DIR", str(data_home))
    monkeypatch.delenv("VADIMGEST_CONFIG", raising=False)

    # Clear any env vars that sources check for credentials
    for key in list(os.environ):
        if key.startswith("TELEGRAM_") or key.startswith("GITHUB_"):
            monkeypatch.delenv(key, raising=False)

    # Point the home dotfolder lookup at an empty tmp dir so the real
    # ~/.vadimgest/config.yaml doesn't leak into the "fresh install" scenario.
    fake_home_config = tmp_path / "vadimgest_home"
    monkeypatch.setattr("vadimgest.config._HOME_CONFIG_DIR", fake_home_config)

    # Clear the lru_cache so load_config reads from the new location
    from vadimgest.config import load_config
    load_config.cache_clear()

    yield {
        "config_home": config_home,
        "data_home": data_home,
        "config_file": config_home / "vadimgest" / "config.yaml",
        "env_file": config_home / "vadimgest" / ".env",
    }

    load_config.cache_clear()


@pytest.fixture
def fresh_client(fresh_env):
    """Flask test client backed by a completely empty data store."""
    from vadimgest.web.app import create_app
    store = DataStore(fresh_env["data_home"])
    app = create_app(store)
    app.config["TESTING"] = True
    return app.test_client(), fresh_env


# ---------------------------------------------------------------------------
# HTML & JS integrity
# ---------------------------------------------------------------------------

class TestDashboardHTML:
    def test_index_returns_200(self, client):
        resp = client.get("/")
        assert resp.status_code == 200

    def test_index_is_html(self, client):
        resp = client.get("/")
        assert b"<!DOCTYPE html>" in resp.data

    def test_has_theme_toggle(self, client):
        resp = client.get("/")
        assert b"toggleTheme" in resp.data

    def test_has_all_tabs(self, client):
        resp = client.get("/")
        html = resp.data.decode()
        for tab in ["Dashboard", "Observatory", "Sources", "Messages", "Docs"]:
            assert tab in html, f"Tab '{tab}' missing from dashboard"

    def test_has_api_fetch_calls(self, client):
        resp = client.get("/")
        html = resp.data.decode()
        for endpoint in ["/api/sources", "/api/runs", "/api/config", "/api/queues", "/api/consumers", "/api/observatory", "/api/data/overview", "/api/data/browse", "/api/data/search"]:
            assert endpoint in html, f"API endpoint '{endpoint}' not referenced in JS"

    def test_messages_tab_opens_data_explorer(self, client):
        resp = client.get("/")
        html = resp.data.decode()

        assert 'data-tab="data"' in html
        assert 'id="tab-data"' in html
        assert "if (target === 'data') renderData();" in html

    def test_source_drawer_exposes_latest_records(self, client):
        resp = client.get("/")
        html = resp.data.decode()

        assert "Latest Records" in html
        assert "loadDrawerRecords" in html
        assert "openDataExplorer" in html
        assert "/api/data/browse?source=" in html

    def test_js_syntax_valid(self, client):
        """Extract JS from the page and validate with node --check."""
        resp = client.get("/")
        html = resp.data.decode()
        scripts = re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL)
        assert len(scripts) >= 2, "Expected at least 2 script blocks"

        main_script = scripts[1]
        assert len(main_script) > 1000, "Main script block suspiciously small"

        node = _find_node()
        if node is None:
            pytest.skip("node not found in PATH")

        tmp = Path("/tmp/vg_test_script.js")
        tmp.write_text(main_script)
        result = subprocess.run([node, "--check", str(tmp)], capture_output=True, text=True)
        tmp.unlink(missing_ok=True)
        assert result.returncode == 0, f"JS syntax error:\n{result.stderr}"

    def test_js_onclick_escaping(self, client):
        """Verify onclick handlers have properly escaped quotes."""
        resp = client.get("/")
        html = resp.data.decode()
        scripts = re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL)
        main_script = scripts[1]
        for match in re.finditer(r"""onclick="(\w+)\(""", main_script):
            # Find the full onclick value - look for the pattern where
            # a JS string builds an onclick with concatenation
            pass
        # The real check: the escaped quote pattern should use \\' not \'
        # In the rendered JS, onclick="func('')" is broken, onclick="func(\'\')" is correct
        assert "onclick=\"openDrawer('" not in main_script, \
            "openDrawer onclick has unescaped quotes - will break JS"

    def test_install_map_matches_backend_allowlists(self, client):
        """Every JS INSTALL_MAP entry must resolve to a package the backend
        actually accepts. This catches tools routed to the wrong method
        (e.g. a Go binary mapped to pipx)."""
        resp = client.get("/")
        html = resp.data.decode()
        scripts = re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL)
        main_script = scripts[1]

        m = re.search(r"const INSTALL_MAP\s*=\s*\{([^}]+)\}", main_script)
        assert m, "INSTALL_MAP not found in JS"
        entries = re.findall(r"(\w+)\s*:\s*'(\w+)'", m.group(1))
        assert len(entries) >= 3, f"INSTALL_MAP has too few entries: {entries}"

        for tool, method in entries:
            # Bird uses @steipete/bird as package name in JS onclick
            pkg = "@steipete/bird" if tool == "bird" else tool
            resp = client.post("/api/install",
                               json={"package": pkg, "method": method})
            assert resp.status_code != 400, \
                f"INSTALL_MAP routes '{tool}' to '{method}' but backend " \
                f"rejects package '{pkg}' with that method (got 400)"

    def test_dark_theme_css_vars(self, client):
        resp = client.get("/")
        html = resp.data.decode()
        assert "--accent: #34d399" in html, "Dark theme accent color missing"
        assert "--bg: #09090b" in html, "Dark theme background missing"

    def test_light_theme_css_vars(self, client):
        resp = client.get("/")
        html = resp.data.decode()
        assert '[data-theme="light"]' in html, "Light theme CSS block missing"
        assert "--accent: #10b981" in html, "Light theme accent color missing"


# ---------------------------------------------------------------------------
# API: GET endpoints
# ---------------------------------------------------------------------------

class TestAPIGet:
    def test_sources_returns_list(self, client):
        resp = client.get("/api/sources")
        assert resp.status_code == 200
        data = resp.get_json()
        assert isinstance(data, list)

    def test_observatory_returns_unified_status(self, fresh_client, monkeypatch):
        client, _ = fresh_client
        monkeypatch.setattr(
            "vadimgest.web.app._fetch_json_url",
            lambda url, timeout=1.5: ({"stats": {"health_score": 95}, "services": [], "cron_jobs": []}, None),
        )

        resp = client.get("/api/observatory")

        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] in {"healthy", "degraded", "broken", "unknown"}
        assert data["positioning"]["vadimgest"].startswith("personal source-of-truth")
        assert {"server", "edge", "sources", "search", "queues", "klava"} <= {s["key"] for s in data["subsystems"]}
        assert data["klava"]["reachable"] is True

    def test_observatory_treats_unreachable_klava_as_unknown(self, fresh_client, monkeypatch):
        client, _ = fresh_client
        monkeypatch.setattr(
            "vadimgest.web.app._fetch_json_url",
            lambda url, timeout=1.5: (None, "connection refused"),
        )

        resp = client.get("/api/observatory")

        assert resp.status_code == 200
        data = resp.get_json()
        assert data["klava"]["status"] == "unknown"
        assert data["klava"]["reachable"] is False
        assert "connection refused" in data["klava"]["error"]

    def test_observatory_reports_edge_pending_records(self, fresh_client, monkeypatch):
        client, env = fresh_client
        monkeypatch.setattr(
            "vadimgest.web.app._fetch_json_url",
            lambda url, timeout=1.5: (None, "not running"),
        )
        from vadimgest.config import save_edge_config
        from vadimgest.store import DataStore

        save_edge_config({
            "enabled": True,
            "server_url": "https://bakeneko.test",
            "device_id": "macbook-test",
            "sources": ["local"],
        })
        store = DataStore(env["data_home"])
        store.append("local", {"id": "one", "type": "note"})
        store.append("local", {"id": "two", "type": "note"})
        (env["data_home"] / "edge_state.json").write_text(json.dumps({
            "sources": {"local": {"uploaded_line": 1, "updated_at": "2026-06-10T00:00:00+00:00"}},
            "last_run": {
                "ok": True,
                "device_id": "macbook-test",
                "hostname": "macbook",
                "finished_at": "2026-06-10T00:00:00+00:00",
                "errors": [],
            },
        }))

        resp = client.get("/api/observatory")

        assert resp.status_code == 200
        data = resp.get_json()
        assert data["edge"]["local_agent"]["pending_total"] == 1
        assert data["edge"]["local_agent"]["sources"][0]["source"] == "local"

    def test_sources_have_required_fields(self, client):
        resp = client.get("/api/sources")
        data = resp.get_json()
        required = {"name", "display_name", "enabled", "available", "records",
                     "edge_records", "edge_last_ts", "origin", "edge_active",
                     "dependencies", "config_schema", "current_config", "defaults"}
        for source in data:
            missing = required - set(source.keys())
            assert not missing, f"Source '{source.get('name')}' missing fields: {missing}"

    def test_sources_report_edge_origin_counts(self, fresh_client):
        client, env = fresh_client
        from vadimgest.store import DataStore

        store = DataStore(env["data_home"])
        store.append("telegram", {"id": "main-1", "type": "message", "text": "main"})
        store.append("telegram", {
            "id": "edge-1",
            "type": "message",
            "text": "edge",
            "edge": {
                "device_id": "macbook-test",
                "source": "telegram",
                "received_at": "2026-06-28T20:00:00+00:00",
            },
        })

        resp = client.get("/api/sources")
        data = resp.get_json()
        telegram = next(s for s in data if s["name"] == "telegram")

        assert telegram["records"] == 2
        assert telegram["edge_records"] == 1
        assert telegram["edge_active"] is True
        assert telegram["origin"] == "mixed"
        assert telegram["edge_last_ts"] == "2026-06-28T20:00:00+00:00"

    def test_sources_use_persisted_edge_origin_counts(self, fresh_client):
        client, env = fresh_client
        from vadimgest.edge import save_edge_source_stats
        from vadimgest.store import DataStore

        store = DataStore(env["data_home"])
        store.append("telegram", {
            "id": "edge-1",
            "type": "message",
            "text": "edge",
            "edge": {
                "device_id": "macbook-test",
                "source": "telegram",
                "received_at": "2026-06-28T19:00:00+00:00",
            },
        })
        source_file = store.sources_dir / "telegram.jsonl"
        stat = source_file.stat()
        save_edge_source_stats({
            "sources": {
                "telegram": {
                    "edge_records": 1,
                    "edge_last_ts": "2026-06-28T20:00:00+00:00",
                    "cache_key": f"{stat.st_mtime_ns}:{stat.st_size}",
                }
            }
        }, env["data_home"])

        resp = client.get("/api/sources")
        telegram = next(s for s in resp.get_json() if s["name"] == "telegram")

        assert telegram["edge_records"] == 1
        assert telegram["edge_last_ts"] == "2026-06-28T20:00:00+00:00"

    def test_dashboard_includes_edge_source_labels(self, client):
        resp = client.get("/")
        html = resp.get_data(as_text=True)

        assert "Loaded in vadimgest" in html
        assert "Loaded Sources" in html
        assert "Show empty/setup sources" in html
        assert "last record" in html
        assert "edge records" in html
        assert "Received from edge" in html
        assert "Main collector" in html

    def test_sources_have_valid_categories(self, client):
        resp = client.get("/api/sources")
        data = resp.get_json()
        valid_cats = {"messaging", "email", "calendar", "meetings", "dev",
                      "activity", "files", "social", "knowledge", ""}
        for source in data:
            cat = source.get("category", "")
            assert cat in valid_cats, f"Source '{source['name']}' has unknown category '{cat}'"

    def test_sources_dependencies_structure(self, client):
        resp = client.get("/api/sources")
        data = resp.get_json()
        for source in data:
            deps = source.get("dependencies", {})
            for key in ("python", "cli", "credentials", "os"):
                assert key in deps, f"Source '{source['name']}' missing deps key '{key}'"
                assert isinstance(deps[key], list), f"deps['{key}'] should be list"

    def test_stats_returns_dict(self, client):
        resp = client.get("/api/stats")
        assert resp.status_code == 200
        data = resp.get_json()
        assert isinstance(data, dict)

    def test_runs_returns_list(self, client):
        resp = client.get("/api/runs")
        assert resp.status_code == 200
        data = resp.get_json()
        assert isinstance(data, list)

    def test_consumers_returns_dict(self, client):
        resp = client.get("/api/consumers")
        assert resp.status_code == 200
        data = resp.get_json()
        assert isinstance(data, dict)

    def test_queues_returns_structure(self, client):
        resp = client.get("/api/queues")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "consumers" in data
        assert "rows" in data
        assert "totals" in data
        assert "updated" in data
        assert isinstance(data["consumers"], list)
        assert isinstance(data["rows"], list)

    def test_sources_count_preserved_jsonl_lines(self, fresh_client):
        client, env = fresh_client
        source_file = env["data_home"] / "sources" / "telegram.jsonl"
        source_file.write_text(
            json.dumps({"_ingested_at": "2026-06-30T00:00:00+00:00", "data": {"type": "message", "text": "one"}}) + "\n" +
            json.dumps({"_ingested_at": "2026-06-30T00:01:00+00:00", "data": {"type": "message", "text": "two"}}) + "\n"
        )

        resp = client.get("/api/sources")

        assert resp.status_code == 200
        telegram = next(s for s in resp.get_json() if s["name"] == "telegram")
        assert telegram["records"] == 2

    def test_queues_count_preserved_jsonl_lines(self, fresh_client):
        client, env = fresh_client
        source_file = env["data_home"] / "sources" / "telegram.jsonl"
        source_file.write_text(
            json.dumps({"id": "one", "type": "message"}) + "\n" +
            json.dumps({"id": "two", "type": "message"}) + "\n"
        )
        checkpoint = {
            "consumer": "intake",
            "positions": {"telegram": {"line": 1, "id": None}},
            "updated_at": "2026-06-30T00:00:00+00:00",
        }
        (env["data_home"] / "checkpoints" / "intake.json").write_text(json.dumps(checkpoint))

        resp = client.get("/api/queues")

        assert resp.status_code == 200
        telegram = next(r for r in resp.get_json()["rows"] if r["source"] == "telegram")
        assert telegram["total"] == 2
        assert telegram["pending"]["intake"] == 1

    def test_config_returns_paths(self, client):
        resp = client.get("/api/config")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "data_dir" in data
        assert "env_file" in data
        assert "has_config" in data

    def test_data_overview_counts_preserved_jsonl_lines(self, fresh_client):
        client, env = fresh_client
        source_file = env["data_home"] / "sources" / "direct.jsonl"
        source_file.write_text(
            json.dumps({"_ingested_at": "2026-06-30T00:00:00+00:00", "data": {"type": "message", "text": "one"}}) + "\n" +
            json.dumps({"_ingested_at": "2026-06-30T00:01:00+00:00", "data": {"type": "message", "text": "two"}}) + "\n"
        )

        resp = client.get("/api/data/overview")

        assert resp.status_code == 200
        data = resp.get_json()
        direct = next(s for s in data["sources"] if s["name"] == "direct")
        assert direct["records"] == 2
        assert direct["types"]["message"] == 2

    def test_data_browse_can_return_full_preserved_record(self, fresh_client):
        client, env = fresh_client
        long_text = "x" * 700
        source_file = env["data_home"] / "sources" / "telegram.jsonl"
        source_file.write_text(json.dumps({"id": "m1", "type": "message", "text": long_text}) + "\n")

        compact = client.get("/api/data/browse?source=telegram&limit=1").get_json()
        full = client.get("/api/data/browse?source=telegram&limit=1&full=1").get_json()

        assert compact["records"][0]["text"] == "x" * 500 + "..."
        assert full["records"][0]["text"] == long_text


# ---------------------------------------------------------------------------
# API: PUT /api/sources/<name>
# ---------------------------------------------------------------------------

class TestAPIUpdateSource:
    def test_enable_source(self, client):
        resp = client.put("/api/sources/telegram",
                          json={"enabled": True})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True
        assert data["saved"]["enabled"] is True

    def test_disable_source(self, client):
        resp = client.put("/api/sources/telegram",
                          json={"enabled": False})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["saved"]["enabled"] is False

    def test_update_config_field(self, client):
        resp = client.put("/api/sources/telegram",
                          json={"config": {"max_messages_per_chat": 500}})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True

    def test_unknown_source_returns_404(self, client):
        resp = client.put("/api/sources/nonexistent",
                          json={"enabled": True})
        assert resp.status_code == 404

    def test_empty_body_returns_ok(self, client):
        resp = client.put("/api/sources/telegram",
                          json={})
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# API: PUT /api/credentials
# ---------------------------------------------------------------------------

class TestAPICredentials:
    def test_save_credentials(self, client):
        resp = client.put("/api/credentials",
                          json={"TEST_API_KEY": "secret123"})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True
        assert "TEST_API_KEY" in data["saved"]

    def test_empty_values_skipped(self, client):
        resp = client.put("/api/credentials",
                          json={"EMPTY_KEY": ""})
        assert resp.status_code == 200
        data = resp.get_json()
        assert "EMPTY_KEY" not in data["saved"]


# ---------------------------------------------------------------------------
# API: POST /api/sync
# ---------------------------------------------------------------------------

class TestAPISync:
    def test_sync_requires_source(self, client):
        resp = client.post("/api/sync", json={})
        assert resp.status_code == 400

    def test_sync_starts_for_valid_source(self, client):
        resp = client.post("/api/sync", json={"source": "telegram"})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True
        assert "telegram" in data["message"]


# ---------------------------------------------------------------------------
# API: POST /api/install
# ---------------------------------------------------------------------------

class TestAPIInstall:
    def test_install_requires_package(self, client):
        resp = client.post("/api/install", json={})
        assert resp.status_code == 400

    def test_install_rejects_unlisted_pip(self, client):
        resp = client.post("/api/install",
                           json={"package": "malicious-package", "method": "pip"})
        assert resp.status_code == 400
        assert "not allowed" in resp.get_json()["error"]

    def test_install_rejects_unlisted_brew(self, client):
        resp = client.post("/api/install",
                           json={"package": "wget", "method": "brew"})
        assert resp.status_code == 400

    def test_install_accepts_brew_with_tap(self, client):
        """sigtop, wacli, gog, and gh should be in the brew allowlist."""
        for pkg in ("sigtop", "wacli", "gog", "gh"):
            resp = client.post("/api/install",
                               json={"package": pkg, "method": "brew"})
            assert resp.status_code != 400, f"brew package '{pkg}' should be allowed"

    def test_install_rejects_unlisted_pipx(self, client):
        resp = client.post("/api/install",
                           json={"package": "bad-tool", "method": "pipx"})
        assert resp.status_code == 400

    def test_install_rejects_unlisted_npm(self, client):
        resp = client.post("/api/install",
                           json={"package": "evil-pkg", "method": "npm"})
        assert resp.status_code == 400

    def test_install_accepts_npm_bird(self, client):
        """@steipete/bird should be in the npm allowlist."""
        resp = client.post("/api/install",
                           json={"package": "@steipete/bird", "method": "npm"})
        assert resp.status_code != 400, "@steipete/bird should be allowed via npm"

    def test_install_rejects_unknown_method(self, client):
        resp = client.post("/api/install",
                           json={"package": "telethon", "method": "cargo"})
        assert resp.status_code == 400

    def test_allowed_pip_packages(self, client):
        """Verify the allowlist contains expected packages."""
        allowed = {"telethon", "python-dotenv", "pysqlite3", "sqlite-vec",
                   "httpx", "flask", "linkedin-api", "requests", "playwright"}
        # Test one that should be allowed (will actually try to install,
        # so we just check it doesn't return 400)
        resp = client.post("/api/install",
                           json={"package": "telethon", "method": "pip"})
        assert resp.status_code != 400, "telethon should be in pip allowlist"


# ---------------------------------------------------------------------------
# Dependency chain completeness - every source dep must be installable
# ---------------------------------------------------------------------------

class TestDependencyChain:
    """Verify every source's every dependency has a working install path."""

    def test_every_cli_tool_has_install_method(self, client):
        """Every CLI dependency across all sources must appear in JS
        INSTALL_MAP and resolve to a valid backend install method."""
        resp = client.get("/")
        html = resp.data.decode()
        scripts = re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL)
        main_script = scripts[1]

        m = re.search(r"const INSTALL_MAP\s*=\s*\{([^}]+)\}", main_script)
        assert m, "INSTALL_MAP not found in JS"
        install_map = dict(re.findall(r"(\w+)\s*:\s*'(\w+)'", m.group(1)))

        resp = client.get("/api/sources")
        sources = resp.get_json()
        for source in sources:
            cli_deps = source["dependencies"].get("cli", [])
            for tool in cli_deps:
                assert tool in install_map, \
                    f"Source '{source['name']}' needs CLI tool '{tool}' " \
                    f"but it's not in INSTALL_MAP"

    def test_every_python_dep_in_pip_allowlist(self, client):
        """Every Python dependency must be installable via pip."""
        resp = client.get("/api/sources")
        sources = resp.get_json()
        for source in sources:
            py_deps = source["dependencies"].get("python", [])
            for pkg in py_deps:
                resp2 = client.post("/api/install",
                                    json={"package": pkg, "method": "pip"})
                assert resp2.status_code != 400, \
                    f"Source '{source['name']}' needs Python package '{pkg}' " \
                    f"but pip rejects it (not in allowlist)"

    def test_brew_setup_endpoint_exists(self, client):
        """brew_setup method must be accepted (for installing Homebrew itself)."""
        resp = client.post("/api/install",
                           json={"package": "homebrew", "method": "brew_setup"})
        # Should not be 400 (unknown method) - may be 200 (already installed)
        # or 500 (install failed) but the method itself must be recognized
        assert resp.status_code != 400 or "Unknown method" not in resp.get_json().get("error", ""), \
            "brew_setup method not recognized"

    def test_npm_setup_endpoint_exists(self, client):
        """npm_setup method must be accepted (for installing Node.js)."""
        resp = client.post("/api/install",
                           json={"package": "node", "method": "npm_setup"})
        assert resp.status_code != 400 or "Unknown method" not in resp.get_json().get("error", ""), \
            "npm_setup method not recognized"

    def test_brew_missing_returns_needs_brew(self, client):
        """When brew is not in PATH, install should return needs_brew error
        so the UI can offer to install Homebrew."""
        import shutil
        with patch.object(shutil, "which", side_effect=lambda x: None if x == "brew" else "/usr/bin/" + x):
            resp = client.post("/api/install",
                               json={"package": "gh", "method": "brew"})
            data = resp.get_json()
            assert data["error"] == "needs_brew", \
                f"Expected 'needs_brew' error, got: {data}"

    def test_npm_missing_returns_needs_npm(self, client):
        """When npm is not in PATH, install should return needs_npm error."""
        import shutil
        with patch.object(shutil, "which", side_effect=lambda x: None if x == "npm" else "/usr/bin/" + x):
            resp = client.post("/api/install",
                               json={"package": "@steipete/bird", "method": "npm"})
            data = resp.get_json()
            assert data["error"] == "needs_npm", \
                f"Expected 'needs_npm' error, got: {data}"


# ---------------------------------------------------------------------------
# API: POST /api/config/init
# ---------------------------------------------------------------------------

class TestAPIConfigInit:
    def test_config_init(self, client):
        resp = client.post("/api/config/init")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True
        assert "path" in data


# ---------------------------------------------------------------------------
# API: SSE endpoint
# ---------------------------------------------------------------------------

class TestAPIEvents:
    def test_events_returns_stream(self, client):
        resp = client.get("/api/events")
        assert resp.status_code == 200
        assert resp.content_type.startswith("text/event-stream")


# ---------------------------------------------------------------------------
# Integration: source drawer data completeness
# ---------------------------------------------------------------------------

class TestSourceDrawerData:
    """Verify each source provides enough data for the drawer to render."""

    def test_all_sources_have_ready_info(self, client):
        resp = client.get("/api/sources")
        for source in resp.get_json():
            if source["available"]:
                assert source["ready"] is not None, \
                    f"Source '{source['name']}' available but ready=None"
                assert "ok" in source["ready"], \
                    f"Source '{source['name']}' ready info missing 'ok' key"

    def test_all_sources_serializable(self, client):
        """Ensure no Path objects leak into JSON response."""
        resp = client.get("/api/sources")
        raw = resp.data.decode()
        # If a Path leaked, it would show as PosixPath(...) or WindowsPath(...)
        assert "PosixPath(" not in raw, "Path object leaked into JSON response"
        assert "WindowsPath(" not in raw, "Path object leaked into JSON response"

    def test_env_status_matches_credentials(self, client):
        resp = client.get("/api/sources")
        for source in resp.get_json():
            cred_deps = source["dependencies"]["credentials"]
            env_status = source.get("env_status", {})
            for cred in cred_deps:
                assert cred in env_status, \
                    f"Source '{source['name']}': credential '{cred}' missing from env_status"


# ===========================================================================
# Fresh install journey - zero config, build everything from dashboard
# ===========================================================================

class TestFreshInstallJourney:
    """Simulate a brand-new user with no config, no data, no credentials.
    Walk through every step of setting up sources via the dashboard."""

    def test_01_no_config_shows_banner(self, fresh_client):
        client, env = fresh_client
        resp = client.get("/api/config")
        data = resp.get_json()
        assert data["has_config"] is False, "Fresh install should have no config"

    def test_02_sources_all_disabled(self, fresh_client):
        client, env = fresh_client
        resp = client.get("/api/sources")
        data = resp.get_json()
        assert len(data) > 0, "Should still list available sources"
        for source in data:
            assert source["enabled"] is False, \
                f"Source '{source['name']}' should be disabled on fresh install"

    def test_03_no_records_exist(self, fresh_client):
        client, env = fresh_client
        resp = client.get("/api/stats")
        data = resp.get_json()
        total = sum(s.get("records", 0) for s in data.values())
        assert total == 0, "Fresh install should have zero records"

    def test_04_no_sync_history(self, fresh_client):
        client, env = fresh_client
        resp = client.get("/api/runs")
        assert resp.get_json() == []

    def test_05_no_consumers(self, fresh_client):
        client, env = fresh_client
        resp = client.get("/api/consumers")
        assert resp.get_json() == {}

    def test_06_queues_empty(self, fresh_client):
        client, env = fresh_client
        resp = client.get("/api/queues")
        data = resp.get_json()
        assert data["consumers"] == []
        assert data["totals"] == {}

    def test_07_create_config(self, fresh_client):
        client, env = fresh_client
        resp = client.post("/api/config/init")
        data = resp.get_json()
        assert data["ok"] is True
        assert env["config_file"].exists(), "Config file should be created"

        # Verify config has all sources set to disabled
        with open(env["config_file"]) as f:
            raw = yaml.safe_load(f)
        from vadimgest.config import _SOURCE_DEFAULTS
        for name in _SOURCE_DEFAULTS:
            assert name in raw, f"Source '{name}' missing from generated config"
            assert raw[name]["enabled"] is False

    def test_08_enable_source(self, fresh_client):
        client, env = fresh_client
        # Create config first
        client.post("/api/config/init")

        # Enable telegram
        resp = client.put("/api/sources/telegram", json={"enabled": True})
        assert resp.get_json()["ok"] is True

        # Verify it's now enabled
        resp = client.get("/api/sources")
        tg = next(s for s in resp.get_json() if s["name"] == "telegram")
        assert tg["enabled"] is True

    def test_09_set_credentials(self, fresh_client):
        client, env = fresh_client
        client.post("/api/config/init")

        # Set Telegram credentials
        resp = client.put("/api/credentials", json={
            "TELEGRAM_API_ID": "12345",
            "TELEGRAM_API_HASH": "abc123hash",
        })
        data = resp.get_json()
        assert data["ok"] is True
        assert "TELEGRAM_API_ID" in data["saved"]
        assert "TELEGRAM_API_HASH" in data["saved"]

        # Verify .env file was created
        assert env["env_file"].exists()
        env_content = env["env_file"].read_text()
        assert "TELEGRAM_API_ID=12345" in env_content
        assert "TELEGRAM_API_HASH=abc123hash" in env_content

        # Verify env vars are now set in process
        assert os.environ.get("TELEGRAM_API_ID") == "12345"

        # Verify credentials were saved (env_status empty since creds are now built-in)
        resp = client.get("/api/sources")
        tg = next(s for s in resp.get_json() if s["name"] == "telegram")
        assert tg["env_status"] == {} or tg["env_status"].get("TELEGRAM_API_ID") is True

    def test_10_configure_source_fields(self, fresh_client):
        client, env = fresh_client
        client.post("/api/config/init")

        # Set config fields for telegram
        resp = client.put("/api/sources/telegram", json={
            "enabled": True,
            "config": {
                "max_messages_per_chat": 500,
                "monitored_folders": ["Work", "Family"],
                "transcribe_voice": True,
            }
        })
        assert resp.get_json()["ok"] is True

        # Read config file and verify
        with open(env["config_file"]) as f:
            raw = yaml.safe_load(f)
        assert raw["telegram"]["enabled"] is True
        assert raw["telegram"]["max_messages_per_chat"] == 500
        assert raw["telegram"]["monitored_folders"] == ["Work", "Family"]
        assert raw["telegram"]["transcribe_voice"] is True

    def test_11_install_rejects_bad_packages(self, fresh_client):
        """Security: verify install endpoint only allows whitelisted packages."""
        client, env = fresh_client
        bad_attempts = [
            {"package": "evil-package", "method": "pip"},
            {"package": "rm-rf", "method": "brew"},
            {"package": "backdoor", "method": "pipx"},
            {"package": "telethon", "method": "apt"},
        ]
        for attempt in bad_attempts:
            resp = client.post("/api/install", json=attempt)
            assert resp.status_code == 400, \
                f"Should reject: {attempt}"

    def test_12_install_allows_listed_packages(self, fresh_client):
        """Verify all packages in the allowlist are accepted (not 400)."""
        client, env = fresh_client
        pip_allowed = [
            "telethon", "python-dotenv", "pysqlite3", "sqlite-vec",
            "httpx", "flask", "linkedin-api", "requests", "playwright",
        ]
        for pkg in pip_allowed:
            resp = client.post("/api/install", json={"package": pkg, "method": "pip"})
            assert resp.status_code != 400, \
                f"Package '{pkg}' should be in pip allowlist but got 400"

    def test_13_sync_trigger_works(self, fresh_client):
        client, env = fresh_client
        client.post("/api/config/init")
        client.put("/api/sources/obsidian", json={"enabled": True})

        resp = client.post("/api/sync", json={"source": "obsidian"})
        data = resp.get_json()
        assert data["ok"] is True

    def test_14_enable_disable_roundtrip(self, fresh_client):
        """Enable, then disable, verify state changes persist."""
        client, env = fresh_client
        client.post("/api/config/init")

        # Enable
        client.put("/api/sources/signal", json={"enabled": True})
        resp = client.get("/api/sources")
        sig = next(s for s in resp.get_json() if s["name"] == "signal")
        assert sig["enabled"] is True

        # Disable
        client.put("/api/sources/signal", json={"enabled": False})
        resp = client.get("/api/sources")
        sig = next(s for s in resp.get_json() if s["name"] == "signal")
        assert sig["enabled"] is False

        # Verify config file reflects the change
        with open(env["config_file"]) as f:
            raw = yaml.safe_load(f)
        assert raw["signal"]["enabled"] is False

    def test_15_all_sources_can_be_enabled(self, fresh_client):
        """Enable every single source and verify the state."""
        client, env = fresh_client
        client.post("/api/config/init")

        resp = client.get("/api/sources")
        all_names = [s["name"] for s in resp.get_json()]

        for name in all_names:
            resp = client.put(f"/api/sources/{name}", json={"enabled": True})
            assert resp.status_code == 200, f"Failed to enable {name}"
            assert resp.get_json()["ok"] is True

        # Verify all are now enabled
        resp = client.get("/api/sources")
        for source in resp.get_json():
            assert source["enabled"] is True, \
                f"Source '{source['name']}' should be enabled"

    def test_16_credentials_update_existing(self, fresh_client):
        """Set a credential, then update it - verify overwrite."""
        client, env = fresh_client
        client.post("/api/config/init")

        client.put("/api/credentials", json={"MY_KEY": "old_value"})
        client.put("/api/credentials", json={"MY_KEY": "new_value"})

        content = env["env_file"].read_text()
        assert content.count("MY_KEY=") == 1, "Should not duplicate key"
        assert "MY_KEY=new_value" in content

    def test_17_multiple_sources_config_isolation(self, fresh_client):
        """Configure two sources, verify they don't bleed into each other."""
        client, env = fresh_client
        client.post("/api/config/init")

        client.put("/api/sources/telegram", json={
            "config": {"max_messages_per_chat": 100}
        })
        client.put("/api/sources/gmail", json={
            "config": {"page_size": 50}
        })

        with open(env["config_file"]) as f:
            raw = yaml.safe_load(f)
        assert raw["telegram"]["max_messages_per_chat"] == 100
        assert "page_size" not in raw.get("telegram", {})
        assert raw["gmail"]["page_size"] == 50
        assert "max_messages_per_chat" not in raw.get("gmail", {})

    def test_18_drawer_data_for_every_source(self, fresh_client):
        """Every source must provide enough data for the drawer to render."""
        client, env = fresh_client
        resp = client.get("/api/sources")
        for source in resp.get_json():
            assert "name" in source
            assert "display_name" in source
            assert "dependencies" in source
            deps = source["dependencies"]
            assert isinstance(deps.get("python"), list)
            assert isinstance(deps.get("cli"), list)
            assert isinstance(deps.get("credentials"), list)
            assert "config_schema" in source
            assert "defaults" in source
            assert "current_config" in source
            assert isinstance(source.get("records"), int)

    def test_19_full_setup_journey(self, fresh_client):
        """End-to-end: fresh install → config → enable → creds → verify ready."""
        client, env = fresh_client

        # 1. No config
        resp = client.get("/api/config")
        assert resp.get_json()["has_config"] is False

        # 2. Create config
        resp = client.post("/api/config/init")
        assert resp.get_json()["ok"] is True

        # 3. Now has config
        resp = client.get("/api/config")
        assert resp.get_json()["has_config"] is True

        # 4. Enable obsidian (no external deps needed)
        client.put("/api/sources/obsidian", json={
            "enabled": True,
            "config": {"vault_path": str(env["data_home"] / "test_vault")}
        })

        # 5. Verify it shows as enabled
        resp = client.get("/api/sources")
        obs = next(s for s in resp.get_json() if s["name"] == "obsidian")
        assert obs["enabled"] is True
        assert obs["available"] is True

        # 6. Verify no records yet
        assert obs["records"] == 0

        # 7. Header stats should work
        resp = client.get("/api/stats")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Mac source install & setup checklist
# ---------------------------------------------------------------------------

class TestMacSourceInstall:
    """Verify mac-dependent sources load correctly and render setup info."""

    MAC_SOURCES = [
        name for name, deps in _STATIC_DEPS.items()
        if any("macos" in v for v in deps.get("os", []))
    ]

    @pytest.mark.skipif(sys.platform != "darwin", reason="macOS only")
    def test_mac_sources_import_on_macos(self):
        """All sources with os: ['macos'] in _STATIC_DEPS should import their syncer class on macOS."""
        from vadimgest.ingest.sources import get_syncer_class
        for name in self.MAC_SOURCES:
            cls = get_syncer_class(name)
            assert cls is not None, (
                f"Source '{name}' requires macOS and we're on macOS, "
                f"but its syncer class failed to import"
            )

    @pytest.mark.skipif(sys.platform != "darwin", reason="macOS only")
    def test_check_ready_returns_dict_with_ok(self):
        """For loadable mac sources, check_ready() must return a dict with 'ok' key."""
        from vadimgest.ingest.sources import get_syncer_class
        for name in self.MAC_SOURCES:
            cls = get_syncer_class(name)
            if cls is None:
                continue
            result = cls.check_ready()
            assert isinstance(result, dict), (
                f"Source '{name}' check_ready() returned {type(result)}, expected dict"
            )
            assert "ok" in result, (
                f"Source '{name}' check_ready() result missing 'ok' key: {result}"
            )

    def test_static_deps_mac_sources_have_directories(self):
        """Sources in _STATIC_DEPS with macOS requirement must have a source directory."""
        sources_dir = Path(__file__).parent.parent / "vadimgest" / "ingest" / "sources"
        for name in self.MAC_SOURCES:
            source_dir = sources_dir / name
            assert source_dir.is_dir(), (
                f"Source '{name}' listed in _STATIC_DEPS with macOS dep "
                f"but directory {source_dir} does not exist"
            )

    def test_api_sources_include_os_deps(self, client):
        """GET /api/sources must include 'os' in dependencies for mac sources."""
        resp = client.get("/api/sources")
        data = resp.get_json()
        source_map = {s["name"]: s for s in data}
        for name in self.MAC_SOURCES:
            assert name in source_map, f"Mac source '{name}' not in /api/sources"
            deps = source_map[name]["dependencies"]
            assert "os" in deps, (
                f"Source '{name}' dependencies missing 'os' key"
            )
            assert isinstance(deps["os"], list), (
                f"Source '{name}' deps['os'] should be a list"
            )
            assert len(deps["os"]) > 0, (
                f"Source '{name}' has macOS in _STATIC_DEPS but os deps list is empty in API"
            )

    def test_install_rejects_unknown_tool(self, client):
        """POST /api/install with an unknown tool name should return 400."""
        resp = client.post("/api/install",
                           json={"package": "nonexistent-tool-xyz", "method": "brew"})
        assert resp.status_code == 400

    def test_install_handles_brew_tool(self, client):
        """POST /api/install with sigtop/brew should not return 400 (allowlist accepts it)."""
        resp = client.post("/api/install",
                           json={"package": "sigtop", "method": "brew"})
        # 400 = rejected by allowlist (should NOT happen for sigtop)
        # 200 = installed successfully (brew present and works)
        # 500 = brew present but install failed, or brew not found
        assert resp.status_code != 400, (
            f"sigtop should be in brew allowlist, got 400: {resp.get_json()}"
        )


# ---------------------------------------------------------------------------
# Sync endpoint
# ---------------------------------------------------------------------------

class TestSyncEndpoint:
    """Verify /api/sources/<name>/sync for various sources."""

    def test_obsidian_sync_returns_200(self, client):
        """Obsidian has no external deps - sync should return 200."""
        resp = client.post("/api/sources/obsidian/sync")
        assert resp.status_code == 200, (
            f"Obsidian sync should succeed (no external deps), "
            f"got {resp.status_code}: {resp.get_json()}"
        )

    def test_nonexistent_source_returns_404(self, client):
        """Syncing a source that doesn't exist should return 404."""
        resp = client.post("/api/sources/nonexistent/sync")
        assert resp.status_code == 404

    def test_telegram_sync_returns_200_or_400_or_500(self, client):
        """Telegram sync returns 200 (success), 400 (missing deps), or 500 (runtime error)."""
        resp = client.post("/api/sources/telegram/sync")
        assert resp.status_code in (200, 400, 500), (
            f"Expected 200/400/500, got {resp.status_code}: {resp.get_json()}"
        )
        if resp.status_code == 400:
            assert "not available" in resp.get_json()["error"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_node():
    """Find node binary, return path or None."""
    import shutil
    return shutil.which("node")
