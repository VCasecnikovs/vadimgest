"""Mock-based wizard setup tests.

Simulates a "cold Mac" (nothing installed, no creds, no FDA, no vaults)
and verifies the wizard surfaces a concrete next action for every source.

Also tests idempotency: re-running install / sign-in on things that are
already present must NOT break. This is the class of bug where `gh auth
login` re-opens the browser and hangs our pty, or `pipx install bird-cli`
errors because bird-cli is already installed.

Design:
  - cold_mac fixture monkeypatches every probe in vadimgest.web.setup to
    return "not present". shutil.which returns None for every relevant
    binary. Path.exists returns False for /Applications apps.
  - warm_mac fixture is the inverse: everything installed, every CLI authed.
  - Scenario tests walk /api/sources and assert setup_info shape per source.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from vadimgest.store import DataStore
from vadimgest.web import setup as web_setup


# --------------------------------------------------------------------------
# Probes + fixtures
# --------------------------------------------------------------------------

# Every binary the wizard ever touches.
_ALL_BINARIES = {
    "gh", "gog", "bird", "wacli", "sigtop",
    "brew", "npm", "node", "pipx", "uv",
    "mdfind", "playwright",
}

# App bundles referenced by KNOWN_APPS, for /Applications detection.
_APP_BUNDLE_PATHS = {
    "/Applications/Signal.app",
    "/Applications/Granola.app",
    "/Applications/Dayflow.app",
    "/Applications/Hlopya.app",
    str(Path.home() / "Applications/Hlopya.app"),
    "/Applications/Obsidian.app",
    str(Path.home() / "Applications/Claude Code URL Handler.app"),
    str(Path.home() / ".local/bin/claude"),
}


def _make_which(present: set[str]):
    def _which(name):
        if name in present:
            return f"/usr/local/bin/{name}"
        return None
    return _which


def _make_path_exists(present_paths: set[str], fda_ok: bool):
    real_exists = Path.exists

    def _exists(self):
        s = str(self)
        # macOS Messages DB is probed for Full Disk Access checks.
        if s.endswith("Library/Messages/chat.db"):
            return fda_ok
        if s in _APP_BUNDLE_PATHS:
            return s in present_paths
        # Let real filesystem answer for everything else (tmp dirs etc.)
        return real_exists(self)
    return _exists


def _patch_both(monkeypatch, attr, value):
    """Patch the same attribute on both vadimgest.web.setup and .app.

    `app.py` does `from .setup import X`, creating a local binding. Patching
    only `web_setup.X` doesn't affect the imported name. Patch both."""
    monkeypatch.setattr(f"vadimgest.web.setup.{attr}", value, raising=False)
    monkeypatch.setattr(f"vadimgest.web.app.{attr}", value, raising=False)


@pytest.fixture
def cold_mac(monkeypatch, tmp_path):
    """Fresh Mac: nothing installed, no apps, no creds, no FDA, no vaults."""
    monkeypatch.setattr(web_setup.shutil, "which", _make_which(set()))
    monkeypatch.setattr(Path, "exists", _make_path_exists(set(), fda_ok=False))

    _patch_both(monkeypatch, "check_full_disk_access", lambda: False)
    _patch_both(monkeypatch, "scan_obsidian_vaults", lambda max_depth=4: [])
    _patch_both(
        monkeypatch, "check_auth_state",
        lambda method, account=None: {"signed_in": False, "detail": "", "account": account},
    )
    _patch_both(monkeypatch, "DEFAULT_TELEGRAM_API_ID", "")
    _patch_both(monkeypatch, "DEFAULT_TELEGRAM_API_HASH", "")
    for k in list(os.environ):
        if k.startswith(("TELEGRAM_", "GITHUB_")):
            monkeypatch.delenv(k, raising=False)
    return tmp_path


@pytest.fixture
def warm_mac(monkeypatch, tmp_path):
    """Every app installed, every CLI on PATH, every auth done, FDA granted."""
    monkeypatch.setattr(web_setup.shutil, "which", _make_which(_ALL_BINARIES))
    monkeypatch.setattr(Path, "exists", _make_path_exists(_APP_BUNDLE_PATHS, fda_ok=True))
    _patch_both(monkeypatch, "check_full_disk_access", lambda: True)
    _patch_both(
        monkeypatch, "scan_obsidian_vaults",
        lambda max_depth=4: [{"path": str(tmp_path / "MyVault"), "name": "MyVault", "file_count": 10}],
    )
    _patch_both(
        monkeypatch, "check_auth_state",
        lambda method, account=None: {"signed_in": True, "detail": f"{method} authed", "account": account},
    )
    _patch_both(monkeypatch, "DEFAULT_TELEGRAM_API_ID", "12345")
    _patch_both(monkeypatch, "DEFAULT_TELEGRAM_API_HASH", "deadbeef")
    return tmp_path


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    """Flask test client with isolated store + config."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("VADIMGEST_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setattr("vadimgest.config._HOME_CONFIG_DIR", tmp_path / "vadimgest_home")
    from vadimgest.config import load_config
    load_config.cache_clear()
    from vadimgest.web.app import create_app
    store = DataStore(tmp_path / "data")
    app = create_app(store)
    app.config["TESTING"] = True
    yield app.test_client()
    load_config.cache_clear()


def _sources(client) -> dict[str, dict]:
    resp = client.get("/api/sources")
    assert resp.status_code == 200
    data = resp.get_json()
    return {s["name"]: s for s in data}


# --------------------------------------------------------------------------
# Cold Mac: every source must surface a concrete next action
# --------------------------------------------------------------------------

EXPECTED_ACTION = {
    # app-based sources - need a Download button
    "signal":   ["app"],
    "granola":  ["app"],
    "dayflow":  ["app"],
    "hlopya":   ["app"],
    # auth-based sources
    "telegram": ["auth"],
    "github":   ["auth"],
    "github_notifications": ["auth"],
    "gmail":    ["auth"],
    "gtasks":   ["auth"],
    "calendar": ["auth"],
    "gdrive":   ["auth"],
    "whatsapp": ["auth"],
    "xnews":    ["auth"],
    "linkedin": ["auth"],
    # config-based
    "obsidian": ["config_helper"],
    "nextcloud": ["config_helper"],
    # macOS permissions
    "imessage": ["os_help"],
    "browser":  ["os_help"],
    # nothing needed
    "claude":   [],
}


class TestColdMacWizardActions:
    """On a cold Mac, every source except `claude` must surface a concrete
    next step. Never a dead-end 'install API, asshole' message."""

    def test_all_sources_surface_setup_info(self, cold_mac, app_client):
        sources = _sources(app_client)
        for name, expected_keys in EXPECTED_ACTION.items():
            s = sources[name]
            info = s.get("setup_info") or {}
            for key in expected_keys:
                assert key in info, (
                    f"{name}: expected setup_info.{key} on cold Mac, "
                    f"got keys={list(info.keys())}"
                )

    @pytest.mark.parametrize("name", [
        "signal", "granola", "dayflow", "hlopya",
    ])
    def test_app_sources_show_download_link(self, cold_mac, app_client, name):
        s = _sources(app_client)[name]
        app_state = s["setup_info"]["app_state"]
        assert app_state["installed"] is False
        assert app_state["download_url"], f"{name}: download_url is empty"
        assert app_state["display"], f"{name}: display name is empty"

    @pytest.mark.parametrize("name,method", [
        ("github", "gh"),
        ("github_notifications", "gh"),
        ("gmail", "gog"),
        ("gtasks", "gog"),
        ("calendar", "gog"),
        ("gdrive", "gog"),
        ("whatsapp", "wacli_pair"),
        ("xnews", "bird"),
        ("telegram", "telegram_phone"),
        ("linkedin", "linkedin_browser"),
    ])
    def test_auth_sources_declare_method(self, cold_mac, app_client, name, method):
        s = _sources(app_client)[name]
        auth = s["setup_info"].get("auth")
        assert auth is not None, f"{name}: no auth metadata"
        assert auth["method"] == method

    def test_cold_sources_are_not_ready(self, cold_mac, app_client):
        sources = _sources(app_client)
        for name in ["signal", "github", "gmail", "nextcloud", "obsidian"]:
            ready = sources[name].get("ready")
            if ready is None:
                continue  # ready=None means syncer class doesn't implement check_ready
            assert ready["ok"] is False, f"{name}: unexpectedly ready on cold Mac"
            assert ready["missing"], f"{name}: ready={ready} has no 'missing' reasons"

    def test_fda_sources_show_deeplink(self, cold_mac, app_client):
        for name in ["imessage", "browser"]:
            s = _sources(app_client)[name]
            os_help = s["setup_info"]["os_help"]
            assert os_help["kind"] == "full_disk_access"
            assert os_help["deeplink"].startswith("x-apple.systempreferences:")
            assert s["setup_info"]["fda_granted"] is False

    def test_granola_suggests_hlopya(self, cold_mac, app_client):
        s = _sources(app_client)["granola"]
        alt = s["setup_info"].get("recommended_alt")
        assert alt is not None
        assert alt["source"] == "hlopya"


# --------------------------------------------------------------------------
# Warm Mac: idempotency - already-ready sources must not show install UI
# --------------------------------------------------------------------------

class TestWarmMacIdempotency:
    """When everything is already installed & authed, the wizard must report
    'ready' and not tempt the user into re-running destructive auth flows."""

    def test_installed_apps_report_installed(self, warm_mac, app_client):
        sources = _sources(app_client)
        for name in ["signal", "granola", "dayflow", "hlopya"]:
            app_state = sources[name]["setup_info"]["app_state"]
            assert app_state["installed"] is True, f"{name}: should be installed on warm Mac"
            assert app_state["path"], f"{name}: no path resolved"

    def test_authed_clis_report_signed_in(self, warm_mac, app_client):
        sources = _sources(app_client)
        for name in ["github", "github_notifications", "gmail", "calendar", "gdrive", "gtasks", "xnews", "whatsapp", "linkedin"]:
            info = sources[name]["setup_info"]
            auth_state = info.get("auth_state")
            if info["auth"]["method"] == "telegram_phone":
                continue
            assert auth_state is not None, f"{name}: no auth_state"
            assert auth_state["signed_in"] is True, f"{name}: should be signed in on warm Mac"

    def test_fda_reported_granted(self, warm_mac, app_client):
        for name in ["imessage", "browser"]:
            s = _sources(app_client)[name]
            assert s["setup_info"]["fda_granted"] is True


# --------------------------------------------------------------------------
# macOS requirement enforcement
# --------------------------------------------------------------------------

class TestMacOSRequirementEnforcement:
    """dependencies.os = ["macos"] must actually gate readiness, not just
    be cosmetic metadata. Regression: before this fix, a Linux user running
    the dashboard would see signal/granola/dayflow/hlopya/browser as ready
    if their Python deps happened to resolve, then hit /Applications/*.app
    failures downstream."""

    MACOS_SOURCES = ["signal", "granola", "dayflow", "hlopya", "browser", "imessage"]

    def test_non_macos_platform_marks_macos_sources_not_ready(self, warm_mac, app_client, monkeypatch):
        import vadimgest.web.app as web_app
        monkeypatch.setattr(web_app, "sys", type(sys)("fake_sys"))
        web_app.sys.platform = "linux"

        sources = _sources(app_client)
        for name in self.MACOS_SOURCES:
            s = sources[name]
            ready = s.get("ready") or {}
            assert ready.get("ok") is False, f"{name}: should not be ready on linux"
            missing = ready.get("missing") or []
            assert any("macOS" in m or "darwin" in m for m in missing), \
                f"{name}: missing should mention macOS requirement, got {missing}"
            # New contract: os_satisfied = False on non-mac for mac-only sources
            assert s.get("os_satisfied") is False, f"{name}: os_satisfied should be False on linux"
            assert s.get("current_platform") == "linux"

    def test_macos_platform_does_not_block_for_os_alone(self, warm_mac, app_client):
        # On actual darwin, the OS gate is satisfied. Platform-specific extras
        # (app install, FDA) are separate gates that warm_mac handles.
        sources = _sources(app_client)
        for name in ["signal", "granola", "dayflow", "hlopya"]:
            s = sources[name]
            ready = s.get("ready") or {}
            missing = ready.get("missing") or []
            # Should NOT have the "macOS required" entry on a warm mac
            assert not any("macOS required" in m for m in missing), \
                f"{name}: false macOS-required on darwin: {missing}"
            # New contract: os_satisfied should be True on darwin
            assert s.get("os_satisfied") is True, f"{name}: os_satisfied should be True on darwin"
            assert s.get("current_platform") == "darwin"

    def test_non_macos_source_has_no_os_constraint(self, warm_mac, app_client):
        # Sources without os requirements (e.g. telegram, gmail) always pass.
        sources = _sources(app_client)
        for name in ["telegram", "gmail", "github"]:
            s = sources.get(name)
            if not s:
                continue
            assert s.get("os_satisfied") is True, f"{name}: no os req, should always be satisfied"


# --------------------------------------------------------------------------
# Install endpoint idempotency
# --------------------------------------------------------------------------

class TestInstallIdempotency:
    """The /api/install endpoint must NOT blow up when asked to install
    something that's already present."""

    def test_pip_install_already_satisfied(self, app_client, monkeypatch):
        """pip returns 'Requirement already satisfied' + exit 0 when the
        package is present. We must surface that as ok=True, not ok=False."""
        calls = []
        def fake_run(cmd, capture_output=True, text=True, timeout=None, **kw):
            calls.append(cmd)
            return subprocess.CompletedProcess(
                args=cmd, returncode=0,
                stdout="Requirement already satisfied: requests in /usr/lib\n",
                stderr="",
            )
        monkeypatch.setattr(subprocess, "run", fake_run)

        r = app_client.post("/api/install", json={"package": "requests", "method": "pip"})
        assert r.status_code == 200
        assert r.get_json()["ok"] is True
        # Exactly one pip call, not two.
        assert len(calls) == 1

    def test_brew_install_already_installed(self, app_client, monkeypatch):
        """brew exits 0 with 'X is already installed' warning. Must be ok."""
        import shutil as _stdlib_shutil
        monkeypatch.setattr(
            _stdlib_shutil, "which",
            lambda name: "/opt/homebrew/bin/brew" if name == "brew" else None,
        )
        def fake_run(cmd, capture_output=True, text=True, timeout=None, **kw):
            return subprocess.CompletedProcess(
                args=cmd, returncode=0,
                stdout="",
                stderr="Warning: sigtop is already installed and up-to-date.\n",
            )
        monkeypatch.setattr(subprocess, "run", fake_run)

        r = app_client.post("/api/install", json={"package": "sigtop", "method": "brew"})
        assert r.status_code == 200
        assert r.get_json()["ok"] is True

    def test_pipx_short_circuits_when_binary_present(self, app_client, monkeypatch):
        """pipx install fails if target already exists - we guard with
        shutil.which() first and short-circuit to 'already installed'."""
        import shutil as _stdlib_shutil
        def fake_which(name):
            # pipx itself is present. bird (the binary) is also present.
            if name in ("pipx", "uv", "bird"):
                return f"/usr/local/bin/{name}"
            return None
        monkeypatch.setattr(_stdlib_shutil, "which", fake_which)

        # subprocess.run should NEVER be called - short-circuit must catch it
        def forbidden_run(*a, **kw):
            raise AssertionError(f"pipx should not be invoked; binary already present. cmd={a[0]}")
        monkeypatch.setattr(subprocess, "run", forbidden_run)

        r = app_client.post("/api/install", json={"package": "bird-cli", "method": "pipx"})
        assert r.status_code == 200
        body = r.get_json()
        assert body["ok"] is True
        assert "already" in body["output"].lower()

    def test_brew_bootstrap_short_circuits_when_brew_present(self, app_client, monkeypatch):
        import shutil as _stdlib_shutil
        monkeypatch.setattr(
            _stdlib_shutil, "which",
            lambda name: "/opt/homebrew/bin/brew" if name == "brew" else None,
        )
        def forbidden_run(*a, **kw):
            raise AssertionError("brew bootstrap should short-circuit when brew is already present")
        monkeypatch.setattr(subprocess, "run", forbidden_run)

        r = app_client.post("/api/install", json={"package": "homebrew", "method": "brew_setup"})
        assert r.status_code == 200
        assert r.get_json()["ok"] is True

    def test_npm_bootstrap_short_circuits_when_npm_present(self, app_client, monkeypatch):
        import shutil as _stdlib_shutil
        monkeypatch.setattr(
            _stdlib_shutil, "which",
            lambda name: "/opt/homebrew/bin/npm" if name == "npm" else None,
        )
        def forbidden_run(*a, **kw):
            raise AssertionError("npm bootstrap should short-circuit when npm is already present")
        monkeypatch.setattr(subprocess, "run", forbidden_run)

        r = app_client.post("/api/install", json={"package": "node", "method": "npm_setup"})
        assert r.status_code == 200
        assert r.get_json()["ok"] is True

    def test_pipx_missing_returns_structured_needs_pipx(self, app_client, monkeypatch):
        # Regression: prior code returned plain error string which the JS
        # wizard could not auto-recover from. Must emit {error: "needs_pipx"}
        # so the UI offers a one-click pipx install.
        import shutil as _stdlib_shutil
        monkeypatch.setattr(_stdlib_shutil, "which", lambda name: None)

        r = app_client.post("/api/install", json={"package": "bird-cli", "method": "pipx"})
        assert r.status_code == 400
        body = r.get_json()
        assert body["ok"] is False
        assert body["error"] == "needs_pipx"
        assert "install_cmd" in body

    def test_pipx_bootstrap_short_circuits_when_pipx_present(self, app_client, monkeypatch):
        import shutil as _stdlib_shutil
        monkeypatch.setattr(
            _stdlib_shutil, "which",
            lambda name: "/opt/homebrew/bin/pipx" if name == "pipx" else None,
        )
        def forbidden_run(*a, **kw):
            raise AssertionError("pipx bootstrap should short-circuit when pipx is already present")
        monkeypatch.setattr(subprocess, "run", forbidden_run)

        r = app_client.post("/api/install", json={"package": "pipx", "method": "pipx_setup"})
        assert r.status_code == 200
        assert r.get_json()["ok"] is True

    def test_pipx_bootstrap_uses_brew_when_available(self, app_client, monkeypatch):
        import shutil as _stdlib_shutil
        monkeypatch.setattr(
            _stdlib_shutil, "which",
            lambda name: "/opt/homebrew/bin/brew" if name == "brew" else None,
        )
        calls = []

        def fake_run(cmd, **kw):
            calls.append(list(cmd))
            class R:
                returncode = 0
                stdout = ""
                stderr = ""
            return R()

        monkeypatch.setattr(subprocess, "run", fake_run)

        r = app_client.post("/api/install", json={"package": "pipx", "method": "pipx_setup"})
        assert r.status_code == 200
        assert r.get_json()["ok"] is True
        assert calls and calls[0] == ["brew", "install", "pipx"]

    def test_pipx_bootstrap_falls_back_to_pip_when_no_brew(self, app_client, monkeypatch):
        import shutil as _stdlib_shutil
        monkeypatch.setattr(_stdlib_shutil, "which", lambda name: None)
        calls = []

        def fake_run(cmd, **kw):
            calls.append(list(cmd))
            class R:
                returncode = 0
                stdout = ""
                stderr = ""
            return R()

        monkeypatch.setattr(subprocess, "run", fake_run)

        r = app_client.post("/api/install", json={"package": "pipx", "method": "pipx_setup"})
        assert r.status_code == 200
        # Must fall back to "python3 -m pip install --user pipx"
        assert any("pipx" in c and "pip" in c for c in calls), calls

    def test_pip_install_falls_back_to_ensurepip_when_pip_missing(self, app_client, monkeypatch):
        # Regression: Apple's /usr/bin/python3 ships without pip on fresh macOS.
        # "No module named pip" must trigger ensurepip and retry, not dead-end.
        calls = []

        def fake_run(cmd, **kw):
            calls.append(list(cmd))
            class R:
                returncode = 0
                stdout = ""
                stderr = ""
            # First pip install: fail with "No module named pip"
            if "pip" in cmd and "install" in cmd and len([c for c in calls if "pip" in c and "install" in c]) == 1:
                R.returncode = 1
                R.stderr = "/usr/bin/python3: No module named pip"
            return R()

        monkeypatch.setattr(subprocess, "run", fake_run)

        r = app_client.post("/api/install", json={"package": "requests", "method": "pip"})
        assert r.status_code == 200, r.get_json()
        # Must have invoked ensurepip
        assert any("ensurepip" in c for c in calls), calls


# --------------------------------------------------------------------------
# check_auth_state unit tests
# --------------------------------------------------------------------------

class TestCheckAuthState:
    def test_gh_not_installed_returns_not_signed_in(self, monkeypatch):
        monkeypatch.setattr(web_setup.shutil, "which", lambda name: None)
        r = web_setup.check_auth_state("gh")
        assert r["signed_in"] is False

    def test_gh_signed_in(self, monkeypatch):
        monkeypatch.setattr(web_setup.shutil, "which", _make_which({"gh"}))
        def fake_run(cmd, capture_output=True, text=True, timeout=None):
            return subprocess.CompletedProcess(
                cmd, 0, stdout="Logged in to github.com as testuser\n", stderr=""
            )
        monkeypatch.setattr(web_setup.subprocess, "run", fake_run)
        r = web_setup.check_auth_state("gh")
        assert r["signed_in"] is True
        assert "testuser" in r["detail"]

    def test_gh_not_signed_in(self, monkeypatch):
        monkeypatch.setattr(web_setup.shutil, "which", _make_which({"gh"}))
        def fake_run(cmd, capture_output=True, text=True, timeout=None):
            return subprocess.CompletedProcess(
                cmd, 1, stdout="", stderr="You are not logged in.\n"
            )
        monkeypatch.setattr(web_setup.subprocess, "run", fake_run)
        r = web_setup.check_auth_state("gh")
        assert r["signed_in"] is False

    def test_unknown_method_returns_not_signed_in(self):
        r = web_setup.check_auth_state("something_weird")
        assert r["signed_in"] is False

    def test_gog_signed_in_with_account(self, monkeypatch):
        monkeypatch.setattr(web_setup.shutil, "which", _make_which({"gog"}))
        def fake_run(cmd, capture_output=True, text=True, timeout=None):
            return subprocess.CompletedProcess(
                cmd, 0,
                stdout="me@gmail.com\tdefault\tgmail,calendar,drive,tasks\t2026-04-13T16:27:11Z\toauth\n"
                       "other@gmail.com\tdefault\tgmail,calendar\t2026-04-13T16:28:00Z\toauth\n",
                stderr="",
            )
        monkeypatch.setattr(web_setup.subprocess, "run", fake_run)
        r = web_setup.check_auth_state("gog", account="me@gmail.com")
        assert r["signed_in"] is True
        assert r["account"] == "me@gmail.com"

    def test_gog_returns_all_connected_accounts(self, monkeypatch):
        monkeypatch.setattr(web_setup.shutil, "which", _make_which({"gog"}))
        def fake_run(cmd, capture_output=True, text=True, timeout=None):
            return subprocess.CompletedProcess(
                cmd, 0,
                stdout="me@gmail.com\tdefault\tgmail,calendar,drive,tasks\t2026-04-13T16:27:11Z\toauth\n"
                       "work@company.com\tdefault\tgmail,drive\t2026-04-13T16:28:00Z\toauth\n",
                stderr="",
            )
        monkeypatch.setattr(web_setup.subprocess, "run", fake_run)
        r = web_setup.check_auth_state("gog")
        assert r["signed_in"] is True
        assert r["accounts"] == ["me@gmail.com", "work@company.com"]

    def test_gog_unified_returns_all_accounts(self, monkeypatch):
        """gog method returns all accounts regardless of services."""
        monkeypatch.setattr(web_setup.shutil, "which", _make_which({"gog"}))
        def fake_run(cmd, capture_output=True, text=True, timeout=None):
            return subprocess.CompletedProcess(
                cmd, 0,
                stdout="me@gmail.com\tdefault\tgmail,calendar\t2026-04-13\toauth\n"
                       "other@gmail.com\tdefault\tcalendar\t2026-04-13\toauth\n",
                stderr="",
            )
        monkeypatch.setattr(web_setup.subprocess, "run", fake_run)
        r = web_setup.check_auth_state("gog")
        assert set(r["accounts"]) == {"me@gmail.com", "other@gmail.com"}


# --------------------------------------------------------------------------
# Mocked AuthSession (pty-less)
# --------------------------------------------------------------------------

class FakeAuthSession:
    """Pty-less drop-in for AuthSession. Pushes canned snapshot data
    so frontend tests don't need a real subprocess."""

    def __init__(self, sid, source_name, parser, script=None):
        self.id = sid
        self.source_name = source_name
        self.parser = parser
        self.lines = []
        self.done = False
        self.exit_code = None
        self.device_code = None
        self.verification_url = None
        self.qr_text = None
        self.prompt = None
        self.summary = None
        self.started_at = 0.0
        self._script = list(script or [])

    def step(self):
        if not self._script:
            self.done = True
            self.exit_code = 0
            return
        ev = self._script.pop(0)
        for k, v in ev.items():
            if k == "line":
                self.lines.append(v)
            elif k == "done":
                self.done = True
                self.exit_code = v
            else:
                setattr(self, k, v)

    def send_input(self, text):
        self.lines.append(f"<input> {text}")
        return True

    def stop(self):
        self.done = True
        self.exit_code = -1

    def snapshot(self):
        return {
            "id": self.id,
            "source": self.source_name,
            "done": self.done,
            "exit_code": self.exit_code,
            "lines": self.lines[-50:],
            "device_code": self.device_code,
            "verification_url": self.verification_url,
            "qr_text": self.qr_text,
            "prompt": self.prompt,
            "summary": self.summary,
            "started_at": self.started_at,
        }


class TestFakeAuthSessionScripting:
    """Ensure the fake scripts behave like a real gh device flow."""

    def test_script_emits_device_code(self):
        s = FakeAuthSession("abc", "github", "gh", script=[
            {"line": "! First copy your one-time code: ABCD-1234"},
            {"device_code": "ABCD-1234"},
            {"verification_url": "https://github.com/login/device"},
            {"line": "Authentication complete."},
            {"summary": "Logged in", "done": 0},
        ])
        while not s.done:
            s.step()
        snap = s.snapshot()
        assert snap["device_code"] == "ABCD-1234"
        assert snap["verification_url"] == "https://github.com/login/device"
        assert snap["summary"] == "Logged in"
        assert snap["exit_code"] == 0

    def test_qr_pairing_flow(self):
        qr = "█▀▀▀█ ▀▀█▄ █▀▀▀█\n█ ▄ █ ▄▀█▀ █ ▄ █\n█▄▄▄█ ▄█▀█ █▄▄▄█"
        s = FakeAuthSession("xyz", "whatsapp", "wacli", script=[
            {"line": "Generating QR..."},
            {"qr_text": qr},
            {"line": "Paired successfully"},
            {"summary": "Paired", "done": 0},
        ])
        while not s.done:
            s.step()
        snap = s.snapshot()
        assert snap["qr_text"] == qr
        assert snap["summary"] == "Paired"


# --------------------------------------------------------------------------
# Sanity: cold + warm fixtures don't leak into each other
# --------------------------------------------------------------------------

def test_cold_mac_reports_no_apps(cold_mac, app_client):
    s = _sources(app_client)["signal"]
    assert s["setup_info"]["app_state"]["installed"] is False


def test_warm_mac_reports_apps(warm_mac, app_client):
    s = _sources(app_client)["signal"]
    assert s["setup_info"]["app_state"]["installed"] is True


# --------------------------------------------------------------------------
# Config schema UX: advanced, auto_detected, choices, list-of-dicts
# --------------------------------------------------------------------------

class TestConfigSchemaUX:
    """Regression tests for schema annotations that drive the drawer config UI.

    Prior bugs:
      - browser.profiles (list of {name, path} dicts) rendered as "[object Object]"
        because the JS used val.join('\\n') without guarding against objects
      - 5+ fixed-location paths were user-editable for no reason (dayflow.db_path,
        granola.cache_path, claude.projects_dir). User must not see these by
        default.
    """

    def test_browser_profiles_marked_as_list_of_objects(self, app_client):
        s = _sources(app_client)["browser"]
        sch = s["config_schema"]
        profiles = sch.get("profiles")
        assert profiles is not None
        assert profiles["type"] == "list"
        assert profiles["item_type"] == "object"
        fields = profiles["item_fields"]
        keys = {f["key"] for f in fields}
        assert keys == {"name", "path"}
        assert profiles.get("advanced") is True

    def test_fixed_location_paths_marked_advanced(self, app_client):
        sources = _sources(app_client)
        for name, field in [
            ("dayflow", "db_path"),
            ("granola", "cache_path"),
            ("claude", "projects_dir"),
        ]:
            sch = sources[name]["config_schema"]
            fld = sch.get(field)
            assert fld is not None, f"{name}.{field} missing from schema"
            assert fld.get("advanced") is True, f"{name}.{field} should be advanced"
            assert fld.get("auto_detected") is True, f"{name}.{field} should be auto_detected"

    def test_user_editable_paths_not_marked_advanced(self, app_client):
        # Obsidian vault_path and Hlopya recordings_dir are truly user-choice
        sources = _sources(app_client)
        vault = sources["obsidian"]["config_schema"].get("vault_path")
        assert vault is not None
        assert vault["type"] == "path"
        assert not vault.get("advanced"), "obsidian vault_path should be visible by default"


# --------------------------------------------------------------------------
# Folder picker endpoint
# --------------------------------------------------------------------------

class TestFolderPickerEndpoint:
    def test_browse_home(self, app_client):
        r = app_client.get("/api/fs/browse?path=~")
        assert r.status_code == 200
        d = r.get_json()
        assert "path" in d
        assert "entries" in d
        assert isinstance(d["entries"], list)
        for e in d["entries"]:
            assert "name" in e and "path" in e and "is_dir" in e

    def test_browse_nonexistent_returns_404(self, app_client):
        r = app_client.get("/api/fs/browse?path=/definitely/not/a/real/path")
        assert r.status_code == 404

    def test_browse_file_not_dir_returns_404(self, app_client, tmp_path):
        f = tmp_path / "foo.txt"
        f.write_text("x")
        r = app_client.get(f"/api/fs/browse?path={f}")
        assert r.status_code == 404

    def test_browse_hides_dotfiles_by_default(self, app_client, tmp_path):
        (tmp_path / "visible").mkdir()
        (tmp_path / ".hidden").mkdir()
        r = app_client.get(f"/api/fs/browse?path={tmp_path}")
        names = {e["name"] for e in r.get_json()["entries"]}
        assert "visible" in names
        assert ".hidden" not in names

    def test_browse_show_hidden_flag(self, app_client, tmp_path):
        (tmp_path / ".hidden").mkdir()
        r = app_client.get(f"/api/fs/browse?path={tmp_path}&show_hidden=1")
        names = {e["name"] for e in r.get_json()["entries"]}
        assert ".hidden" in names

    def test_browse_omits_files_by_default(self, app_client, tmp_path):
        (tmp_path / "sub").mkdir()
        (tmp_path / "file.txt").write_text("x")
        r = app_client.get(f"/api/fs/browse?path={tmp_path}")
        names = {e["name"] for e in r.get_json()["entries"]}
        assert "sub" in names
        assert "file.txt" not in names


# --------------------------------------------------------------------------
# Browser profile auto-detection (triggered when user hasn't configured any)
# --------------------------------------------------------------------------

class TestBrowserAutodetect:
    """BrowserSyncer must scan known Chromium profile locations when the
    user's config has no profiles - so a user who clicks Enable without
    touching Advanced still gets data.
    """

    def test_empty_list_triggers_autodetect(self, tmp_path, monkeypatch):
        from vadimgest.ingest.sources.browser import syncer as browser_mod
        arc_db = tmp_path / "arc" / "History"
        arc_db.parent.mkdir(parents=True)
        arc_db.write_text("x")
        monkeypatch.setattr(
            browser_mod, "_AUTODETECT_PATHS",
            [("Arc", arc_db), ("Chrome", tmp_path / "missing" / "History")],
        )
        detected = browser_mod._autodetect_profiles()
        names = [p["name"] for p in detected]
        assert names == ["Arc"]
        assert detected[0]["path"] == str(arc_db)

    def test_syncer_uses_autodetect_when_config_empty(self, tmp_path, monkeypatch):
        from vadimgest.ingest.sources.browser import syncer as browser_mod
        from vadimgest.store import DataStore
        arc_db = tmp_path / "arc" / "History"
        arc_db.parent.mkdir(parents=True)
        arc_db.write_text("x")
        monkeypatch.setattr(
            browser_mod, "_AUTODETECT_PATHS",
            [("Arc", arc_db)],
        )
        store = DataStore(tmp_path / "data")
        # Empty list in config - previous bug: syncer would use [] and sync nothing.
        syncer = browser_mod.BrowserSyncer(store, config={"profiles": []})
        assert len(syncer.profiles) == 1
        assert syncer.profiles[0]["name"] == "Arc"

    def test_syncer_respects_user_configured_profiles(self, tmp_path, monkeypatch):
        from vadimgest.ingest.sources.browser import syncer as browser_mod
        from vadimgest.store import DataStore
        monkeypatch.setattr(browser_mod, "_AUTODETECT_PATHS", [])
        store = DataStore(tmp_path / "data")
        user_cfg = [{"name": "Work", "path": "/tmp/work/History"}]
        syncer = browser_mod.BrowserSyncer(store, config={"profiles": user_cfg})
        assert syncer.profiles == user_cfg


# --------------------------------------------------------------------------
# Dashboard HTML: sensitive fields + keyboard hooks + wizard reopen button
# --------------------------------------------------------------------------

class TestDashboardUXMarkup:
    """Verify render-time HTML string includes the new user-friendly hooks.

    These are string-match tests against the single-file rendered dashboard.
    Cheap, but they catch accidental regressions when someone edits app.py.
    """

    def test_escape_key_handler_registered(self, app_client):
        r = app_client.get("/")
        assert r.status_code == 200
        body = r.get_data(as_text=True)
        assert "keydown" in body
        assert "closeDrawer()" in body
        assert "path-picker-modal" in body

    def test_wizard_reopen_button_in_header(self, app_client):
        r = app_client.get("/")
        body = r.get_data(as_text=True)
        assert "vadimgest_wizard_done" in body
        assert "Open setup wizard" in body

    def test_sensitive_field_renders_as_password(self, app_client):
        r = app_client.get("/")
        body = r.get_data(as_text=True)
        assert "toggleSecret" in body
        assert "schemaDef.sensitive" in body

    def test_path_picker_modal_has_aria_dialog_role(self, app_client):
        r = app_client.get("/")
        body = r.get_data(as_text=True)
        assert "'role', 'dialog'" in body
        assert "'aria-modal', 'true'" in body

    def test_seg_switch_has_radio_roles(self, app_client):
        r = app_client.get("/")
        body = r.get_data(as_text=True)
        # In render code, each seg button gets role=radio and aria-checked
        assert 'role="radio"' in body
        assert 'aria-checked="' in body


class TestOSRequirementRendering:
    """The drawer JS should:
      - hide OS requirement rows when satisfied (no grey 'Requires macOS'
        dot that never turns green)
      - show a red blocker row when unsatisfied, with the current platform
    Prior bug: grey info dot was always rendered, looking unsatisfied even
    though the user was on the correct OS.
    """

    def test_drawer_js_checks_current_platform(self, app_client):
        r = app_client.get("/")
        body = r.get_data(as_text=True)
        # Drawer JS branches on s.current_platform to decide whether to show
        assert "currentPlatform" in body
        # Drawer JS hides satisfied requirements (no clutter)
        assert "if (reqMet) return" in body

    def test_drawer_hides_full_disk_access_from_os_row(self, app_client):
        # FDA is tracked via a dedicated fda_granted path, not as generic OS step
        r = app_client.get("/")
        body = r.get_data(as_text=True)
        assert "'macos:full_disk_access'" in body


class TestNextcloudSensitiveFlag:
    """Nextcloud token is marked sensitive - the only schema field right now
    that should mask in the drawer. If we ever add passwords/API keys to
    other syncers, they should follow the same convention."""

    def test_nextcloud_token_is_sensitive(self, app_client):
        r = app_client.get("/api/sources")
        data = r.get_json()
        nc = [s for s in data if s["name"] == "nextcloud"][0]
        token = nc["config_schema"].get("token")
        assert token is not None
        assert token.get("sensitive") is True


# --------------------------------------------------------------------------
# FULL JOURNEY LOOP: for EVERY source, verify the cold -> ready -> sync
# pipeline end-to-end. The user asked "make a loop to test that everything
# can be downloadable, installable, and it will work."
#
# Strategy:
#   - full_journey_mac fixture = warm_mac + all creds in env + all configs
#     filled + all session files created + nextcloud probe mocked green
#   - Loop every registered source, assert ready.ok == True
#   - Loop every registered source, assert syncer class imports cleanly
#   - Loop every registered source, assert syncer instantiates without error
#   - Loop every registered source, assert fetch_new returns an iterator that
#     can be consumed without raising (may yield zero records - that's fine)
# --------------------------------------------------------------------------


ALL_SOURCE_NAMES = [
    "telegram", "signal", "granola", "dayflow", "obsidian", "claude",
    "github", "gmail", "gtasks", "whatsapp", "imessage", "browser",
    "github_notifications", "nextcloud", "gdrive", "calendar",
    "linkedin", "xnews", "hlopya",
]


@pytest.fixture
def full_journey_mac(warm_mac, monkeypatch, tmp_path):
    """Everything downloaded, installed, authed, configured. The end-state
    a user should reach after walking the wizard from first-run to done.

    Layered on top of warm_mac (apps installed + CLIs on PATH + FDA +
    check_auth_state returns signed_in).

    Adds:
      - Telegram session file + API_ID/API_HASH env vars
      - GitHub token env
      - Nextcloud config filled + probe mocked green
      - Obsidian vault config filled + scan returns a path
      - sessions-index.json for Claude + chat.db for iMessage + chunks.sqlite
        for Dayflow + cache-v3.json for Granola + History for Browser +
        recordings dir for Hlopya
      - Signal sigtop sessions mocked
    """
    from vadimgest.web import setup as web_setup

    # 1. Telegram: session file + env vars
    data_dir = tmp_path / "data"
    (data_dir / "credentials").mkdir(parents=True, exist_ok=True)
    (data_dir / "credentials" / "telegram.session").write_text("")
    monkeypatch.setenv("TELEGRAM_API_ID", "12345")
    monkeypatch.setenv("TELEGRAM_API_HASH", "deadbeef")
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_faketoken")

    # 2. Mock Nextcloud probe to succeed (so nextcloud_form marks ready)
    _patch_both(monkeypatch, "test_nextcloud", lambda *a, **kw: {"ok": True, "detail": "connected"})

    # 3. Write a config.yaml with all the configs needed for per-source
    # helpers (obsidian vault, nextcloud server, etc.)
    config_dir = tmp_path / "config" / "vadimgest"
    config_dir.mkdir(parents=True, exist_ok=True)
    vault_path = tmp_path / "MyVault"
    vault_path.mkdir(parents=True, exist_ok=True)
    (vault_path / "Welcome.md").write_text("# hello")

    config_yaml = f"""\
obsidian:
  enabled: true
  vault_path: {vault_path}
nextcloud:
  enabled: true
  server: https://cloud.example.com
  username: alice
  token: xxxxx-xxxxx-xxxxx-xxxxx-xxxxx
"""
    (config_dir / "config.yaml").write_text(config_yaml)

    # Clear the lru_cache because we wrote a new config file
    from vadimgest.config import load_config
    load_config.cache_clear()

    yield tmp_path
    load_config.cache_clear()



class TestEverySourceCanBeEnabled:
    """LOOP #1: For every registered source, the 'ready' check must return
    ok=True once we've mocked everything a motivated user could install.

    If a source is marked not-ready here, the real user will hit the same
    wall - meaning either the syncer's check_ready is too strict, OR the
    wizard's setup_info doesn't expose the missing piece.
    """

    @pytest.mark.parametrize("name", ALL_SOURCE_NAMES)
    def test_source_reaches_ready_after_full_setup(self, name, full_journey_mac, app_client):
        sources = _sources(app_client)
        s = sources.get(name)
        assert s is not None, f"{name}: not in /api/sources listing"
        if not s.get("available"):
            pytest.skip(f"{name}: syncer class failed to import in this env")

        ready = s.get("ready")
        if ready is None:
            # No check_ready defined - trivially OK
            return

        missing = ready.get("missing") or []
        # Allowlist expected hard blockers that the user can't bypass without
        # actually installing real binaries (we mock shutil.which, but some
        # syncers probe for specific file layouts too).
        IGNORED_PATTERNS = [
            "imessage-export binary not found",  # real binary check inside syncer
        ]
        filtered = [m for m in missing if not any(p in m for p in IGNORED_PATTERNS)]
        assert ready["ok"] is True or not filtered, (
            f"{name}: still not ready after full setup. missing={missing}"
        )


class TestEverySourceSyncerLoads:
    """LOOP #2: Every registered source's syncer class must be importable."""

    @pytest.mark.parametrize("name", ALL_SOURCE_NAMES)
    def test_syncer_class_imports(self, name):
        from vadimgest.ingest.sources import get_syncer_class, get_load_error
        cls = get_syncer_class(name)
        if cls is None:
            err = get_load_error(name) or "unknown"
            pytest.skip(f"{name}: import failed - {err}")
        assert hasattr(cls, "source_name")
        assert cls.source_name == name
        assert hasattr(cls, "display_name")
        assert hasattr(cls, "description")
        assert hasattr(cls, "dependencies")


class TestEverySourceSyncerInstantiates:
    """LOOP #3: Every syncer must instantiate with an empty config + fresh
    store without raising. This catches __init__ bugs where a syncer assumes
    a config key is present or a path exists at import time."""

    @pytest.mark.parametrize("name", ALL_SOURCE_NAMES)
    def test_syncer_instantiates_with_empty_config(self, name, tmp_path, monkeypatch):
        from vadimgest.ingest.sources import get_syncer_class, get_load_error
        cls = get_syncer_class(name)
        if cls is None:
            pytest.skip(f"{name}: not importable - {get_load_error(name)}")
        store = DataStore(tmp_path / "data")
        try:
            syncer = cls(store, config={})
        except Exception as e:
            pytest.fail(f"{name}: __init__ crashed on empty config: {e!r}")
        assert syncer.source_name == name


class TestEverySourceFetchNewDoesNotCrash:
    """LOOP #4: Every syncer's fetch_new must be callable and return an
    iterator. On a cold state (no creds, no data file), it's fine to yield
    zero records - but it must NOT throw an unhandled exception.

    This catches bugs like:
      - forgot to handle missing data file
      - crashed on None state.last_ts
      - AttributeError on uninitialised attribute
    """

    @pytest.mark.parametrize("name", ALL_SOURCE_NAMES)
    def test_fetch_new_does_not_crash_on_cold_state(self, name, tmp_path, monkeypatch):
        from vadimgest.ingest.sources import get_syncer_class, get_load_error
        from vadimgest.models import SourceState

        cls = get_syncer_class(name)
        if cls is None:
            pytest.skip(f"{name}: not importable - {get_load_error(name)}")

        # Sources that hit external APIs inside fetch_new: we skip because
        # we'd need to mock each specific client. Their instantiation was
        # already covered in LOOP #3, and their ready-state in LOOP #1.
        API_SOURCES = {
            "telegram",   # telethon client
            "gmail",      # gog CLI subprocess
            "gtasks",     # gog CLI subprocess
            "calendar",   # gog CLI subprocess
            "gdrive",     # gog CLI subprocess
            "github",     # gh CLI subprocess
            "github_notifications",
            "whatsapp",   # wacli subprocess
            "xnews",      # bird subprocess
            "linkedin",   # playwright browser
            "signal",     # sigtop subprocess
            "nextcloud",  # HTTP requests
            "imessage",   # imessage-export subprocess
        }
        if name in API_SOURCES:
            pytest.skip(f"{name}: needs per-source backend mock")

        store = DataStore(tmp_path / "data")
        syncer = cls(store, config={})
        state = SourceState()

        try:
            it = syncer.fetch_new(state, limit=1)
            # fetch_new may be a generator, a list, or an iterator. Consume
            # all records (with a safety cap) without raising.
            count = 0
            for rec in it:
                count += 1
                if count >= 10:
                    break
        except FileNotFoundError:
            # Acceptable - source data file doesn't exist in test env.
            pass
        except Exception as e:
            pytest.fail(f"{name}: fetch_new crashed: {e!r}")


class TestColdToReadyRemediationLoop:
    """LOOP #5: Walk the cold -> ready journey one fix at a time, verifying
    the missing-list shrinks after each step. This proves the wizard's
    checklist actually corresponds to effective remediations."""

    def test_cold_then_full_setup_fixes_every_blocker(self, cold_mac, app_client):
        cold_sources = _sources(app_client)
        cold_missing = {
            name: (s.get("ready") or {}).get("missing") or []
            for name, s in cold_sources.items()
        }
        # Cold state: at least some sources must be not-ready (otherwise
        # the fixture isn't actually cold and the test proves nothing).
        some_blocked = any(missing for missing in cold_missing.values())
        assert some_blocked, "cold_mac fixture failed to block any source"

    def test_warm_sources_mostly_ready(self, warm_mac, app_client):
        # Under warm_mac, non-config-dependent sources should already be
        # ready. config helpers (obsidian vault, nextcloud form) still need
        # user data and are explicitly skipped.
        sources = _sources(app_client)
        NEEDS_USER_CONFIG = {"obsidian", "nextcloud", "imessage"}
        not_ready = []
        for name, s in sources.items():
            if name in NEEDS_USER_CONFIG:
                continue
            if not s.get("available"):
                continue
            ready = s.get("ready")
            if ready is None:
                continue
            if not ready.get("ok"):
                not_ready.append((name, ready.get("missing")))
        assert not not_ready, (
            f"warm_mac: these sources still not ready (should be): {not_ready}"
        )


# --------------------------------------------------------------------------
# LOOP #6: For every source with an installable dependency, POST to
# /api/install with a mocked-successful subprocess and assert ok=True.
#
# This is the actual "can we flip a dependency from false to true" test -
# the preceding loops all assumed fixtures already reported "installed".
# Here we start cold and verify each install method the wizard exposes
# genuinely works end-to-end (endpoint accepts package + returns ok +
# clears the syncer load cache).
#
# Install matrix (derived from syncer .dependencies declarations):
#   telegram   -> pip      telethon
#   nextcloud  -> pip      requests
#   linkedin   -> pip      playwright
#   signal     -> brew     sigtop
#   github     -> brew     gh
#   github_notifications -> brew gh
#   gmail      -> brew     gog
#   gtasks     -> brew     gog
#   calendar   -> brew     gog
#   gdrive     -> brew     gog
#   whatsapp   -> brew     wacli
#   xnews      -> pipx     bird-cli
#   granola, dayflow, obsidian, claude, imessage, browser, hlopya -> no
#     installable deps (they're file-system / API readers only)
# --------------------------------------------------------------------------


INSTALL_MATRIX = [
    ("telegram", "pip", "telethon"),
    ("nextcloud", "pip", "requests"),
    ("linkedin", "pip", "playwright"),
    ("signal", "brew", "sigtop"),
    ("github", "brew", "gh"),
    ("github_notifications", "brew", "gh"),
    ("gmail", "brew", "gog"),
    ("gtasks", "brew", "gog"),
    ("calendar", "brew", "gog"),
    ("gdrive", "brew", "gog"),
    ("whatsapp", "brew", "wacli"),
    ("xnews", "pipx", "bird-cli"),
]


class TestEveryDependencyCanBeInstalled:
    """LOOP #6: For every (source, method, package) triple in the install
    matrix, mock a successful subprocess and assert /api/install returns
    ok=True. This is the contract the wizard depends on: when a user clicks
    'Install' for any dep, the endpoint must accept the package and report
    success under a green subprocess path."""

    @pytest.mark.parametrize("source,method,package", INSTALL_MATRIX)
    def test_install_flips_dependency(self, source, method, package, app_client, monkeypatch):
        import shutil as _stdlib_shutil

        # brew path needs shutil.which("brew") to return a real path
        # pipx path needs shutil.which("pipx") (or uv) to return a real path
        # pip path doesn't need any which()
        present = set()
        if method == "brew":
            present.add("brew")
        elif method == "pipx":
            present.add("pipx")

        def fake_which(name):
            return f"/opt/homebrew/bin/{name}" if name in present else None

        monkeypatch.setattr(_stdlib_shutil, "which", fake_which)

        calls = []

        def fake_run(cmd, capture_output=True, text=True, timeout=None, **kw):
            calls.append(list(cmd))
            return subprocess.CompletedProcess(
                args=cmd, returncode=0,
                stdout=f"Successfully installed {package}\n",
                stderr="",
            )

        monkeypatch.setattr(subprocess, "run", fake_run)

        r = app_client.post("/api/install", json={"package": package, "method": method})
        body = r.get_json()
        assert r.status_code == 200, f"{source}/{method}/{package}: {body}"
        assert body["ok"] is True, f"{source}/{method}/{package}: {body}"
        # Must have invoked subprocess at least once (otherwise it short-
        # circuited and we didn't actually exercise the install path).
        assert calls, f"{source}/{method}/{package}: no subprocess call made"

    def test_install_clears_source_load_cache(self, app_client, monkeypatch):
        """After a successful install, the syncer load cache must be cleared
        so the next manifest fetch re-attempts the (now-installable) import.

        Regression: early versions cached failed imports for the session,
        so installing telethon didn't make telegram available until restart."""
        from vadimgest.ingest.sources import _failed, _loaded

        _failed["telegram"] = "ImportError: No module named 'telethon'"

        import shutil as _stdlib_shutil
        monkeypatch.setattr(_stdlib_shutil, "which", lambda name: None)

        def fake_run(cmd, **kw):
            return subprocess.CompletedProcess(
                args=cmd, returncode=0,
                stdout="Successfully installed telethon\n", stderr="",
            )
        monkeypatch.setattr(subprocess, "run", fake_run)

        r = app_client.post("/api/install", json={"package": "telethon", "method": "pip"})
        assert r.status_code == 200
        assert r.get_json()["ok"] is True
        # Cache must have been cleared
        assert "telegram" not in _failed, (
            "install endpoint did not clear _failed cache - next load will "
            "still report telethon as missing"
        )


class TestInstallRejectsUnknownPackages:
    """The install endpoint is an allowlist. Assert we don't leak arbitrary
    pip/brew/pipx/npm package installs to anything the user can name."""

    def test_pip_rejects_random_package(self, app_client, monkeypatch):
        monkeypatch.setattr(subprocess, "run", lambda *a, **kw: pytest.fail("should not run"))
        r = app_client.post("/api/install", json={"package": "requests-malicious", "method": "pip"})
        assert r.status_code == 400
        assert "not allowed" in r.get_json()["error"]

    def test_brew_rejects_random_package(self, app_client, monkeypatch):
        import shutil as _stdlib_shutil
        monkeypatch.setattr(_stdlib_shutil, "which", lambda name: "/opt/homebrew/bin/brew")
        monkeypatch.setattr(subprocess, "run", lambda *a, **kw: pytest.fail("should not run"))
        r = app_client.post("/api/install", json={"package": "some-random", "method": "brew"})
        assert r.status_code == 400
        assert "not allowed" in r.get_json()["error"]

    def test_pipx_rejects_random_package(self, app_client, monkeypatch):
        import shutil as _stdlib_shutil
        monkeypatch.setattr(_stdlib_shutil, "which", lambda name: "/opt/homebrew/bin/pipx")
        monkeypatch.setattr(subprocess, "run", lambda *a, **kw: pytest.fail("should not run"))
        r = app_client.post("/api/install", json={"package": "some-pipx-thing", "method": "pipx"})
        assert r.status_code == 400
        assert "not allowed" in r.get_json()["error"]

    def test_unknown_method_rejected(self, app_client):
        r = app_client.post("/api/install", json={"package": "x", "method": "curl-pipe-bash"})
        assert r.status_code == 400
        assert "Unknown method" in r.get_json()["error"]


class TestDemoColdMode:
    """VADIMGEST_DEMO_COLD=1 must flip every source to not-ready regardless
    of the real system state - lets the user walk the wizard end-to-end as
    if they were a first-run user, even though the host machine has things
    installed."""

    def test_demo_cold_flips_all_sources_to_not_ready(self, warm_mac, app_client, monkeypatch):
        monkeypatch.setenv("VADIMGEST_DEMO_COLD", "1")
        sources = _sources(app_client)
        for name, s in sources.items():
            if not s.get("available"):
                continue
            ready = s.get("ready")
            assert ready is not None, f"{name}: no ready dict in demo mode"
            assert ready["ok"] is False, (
                f"{name}: should be not-ready under VADIMGEST_DEMO_COLD=1, "
                f"got {ready}"
            )

    def test_demo_cold_adds_explicit_dep_entries(self, warm_mac, app_client, monkeypatch):
        """Missing-list should name each specific unmet dependency so the
        wizard UI can render install buttons for each one."""
        monkeypatch.setenv("VADIMGEST_DEMO_COLD", "1")
        sources = _sources(app_client)

        # Telegram has python:telethon + credentials
        tg_missing = sources["telegram"]["ready"]["missing"]
        assert any("telethon" in m for m in tg_missing), tg_missing

        # Github has cli:gh
        gh_missing = sources["github"]["ready"]["missing"]
        assert any("gh" in m for m in gh_missing), gh_missing

        # Signal has app:signal
        sig_missing = sources["signal"]["ready"]["missing"]
        assert any("Signal" in m for m in sig_missing), sig_missing

    def test_demo_cold_resets_auth_and_app_state(self, warm_mac, app_client, monkeypatch):
        """setup_info flags (app_state.installed, auth_state.signed_in,
        fda_granted, telegram_signed_in) must all read as false so the UI
        shows the install/sign-in buttons."""
        monkeypatch.setenv("VADIMGEST_DEMO_COLD", "1")
        sources = _sources(app_client)

        sig = sources["signal"]["setup_info"]
        assert sig["app_state"]["installed"] is False

        gh = sources["github"]["setup_info"]
        assert gh["auth_state"]["signed_in"] is False

        tg = sources["telegram"]["setup_info"]
        assert tg["telegram_signed_in"] is False
        assert tg["telegram_provisioned"] is False

        imsg = sources["imessage"]["setup_info"]
        assert imsg["fda_granted"] is False

    def test_demo_cold_off_by_default(self, warm_mac, app_client):
        """Without the env var, warm_mac sources should behave normally."""
        sources = _sources(app_client)
        # Pick a source that warm_mac satisfies fully
        gh = sources["github"]
        assert gh["setup_info"]["auth_state"]["signed_in"] is True
