"""Setup helpers for the dashboard wizard.

Handles:
- macOS app detection (Signal Desktop, Granola, Dayflow, Hlopya)
- Interactive CLI auth sessions (gh, gog, bird, wacli) via pseudoterminal
- Telegram auth flow (in-dashboard SMS code entry)
- Obsidian vault scanning
- Nextcloud connection testing

All state is in-memory - auth sessions live for the dashboard process lifetime.
"""

from __future__ import annotations

import asyncio
import json
import os
import pty
import re
import select
import shutil
import signal
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable


# ---- macOS app detection ------------------------------------------------

# name -> (bundle_id, display_name, download_url)
KNOWN_APPS = {
    "signal": {
        "bundle_id": "org.whispersystems.signal-desktop",
        "display": "Signal Desktop",
        "download_url": "https://signal.org/download/macos/",
        "paths": ["/Applications/Signal.app"],
    },
    "granola": {
        "bundle_id": "com.granola.app",
        "display": "Granola",
        "download_url": "https://granola.ai/download",
        "paths": ["/Applications/Granola.app"],
    },
    "dayflow": {
        "bundle_id": "com.dayflow.app",
        "display": "Dayflow",
        "download_url": "https://www.dayflow.so/",
        "paths": ["/Applications/Dayflow.app"],
    },
    "hlopya": {
        "bundle_id": "com.vadims.hlopya",
        "display": "Hlopya",
        "download_url": "https://github.com/VCasecnikovs/hlopya",
        "paths": [
            "/Applications/Hlopya.app",
            str(Path.home() / "Applications/Hlopya.app"),
        ],
    },
    "obsidian": {
        "bundle_id": "md.obsidian",
        "display": "Obsidian",
        "download_url": "https://obsidian.md/download",
        "paths": ["/Applications/Obsidian.app"],
    },
    "claude_code": {
        "bundle_id": "com.anthropic.claude-code-url-handler",
        "display": "Claude Code",
        "download_url": "https://docs.anthropic.com/en/docs/claude-code/overview",
        "paths": [
            str(Path.home() / "Applications/Claude Code URL Handler.app"),
            str(Path.home() / ".local/bin/claude"),
        ],
    },
}


def check_app(key: str) -> dict:
    """Check if a known macOS app is installed.

    Returns {"installed": bool, "path": str|None, "display": str, "download_url": str}
    """
    info = KNOWN_APPS.get(key)
    if not info:
        return {"installed": False, "path": None, "display": key, "download_url": ""}

    found_path = None
    for p in info["paths"]:
        if Path(p).exists():
            found_path = p
            break

    if not found_path and shutil.which("mdfind"):
        try:
            out = subprocess.run(
                ["mdfind", f'kMDItemCFBundleIdentifier == "{info["bundle_id"]}"'],
                capture_output=True, text=True, timeout=5,
            )
            if out.returncode == 0 and out.stdout.strip():
                found_path = out.stdout.strip().splitlines()[0]
        except Exception:
            pass

    return {
        "installed": bool(found_path),
        "path": found_path,
        "display": info["display"],
        "download_url": info["download_url"],
    }


# ---- Full Disk Access detection ----------------------------------------

def check_full_disk_access() -> bool:
    """Test whether this Python process has macOS Full Disk Access.

    We probe ~/Library/Messages/chat.db (which requires FDA even to stat()).
    Returns True if accessible, False otherwise.
    """
    messages_db = Path.home() / "Library/Messages/chat.db"
    if not messages_db.exists():
        return False
    try:
        with open(messages_db, "rb") as f:
            f.read(16)
        return True
    except (PermissionError, OSError):
        return False


# ---- Obsidian vault scanner --------------------------------------------

def scan_obsidian_vaults(max_depth: int = 4) -> list[dict]:
    """Find Obsidian vaults by looking for .obsidian directories.

    Scans ~/Documents, ~/Dropbox, ~/iCloud Drive, ~/Google Drive at up to
    max_depth levels. Returns list of {"path": str, "name": str, "size_mb": int, "file_count": int}.
    """
    candidates: list[dict] = []
    home = Path.home()
    roots = [
        home,
        home / "Documents",
        home / "Dropbox",
        home / "Library/Mobile Documents/com~apple~CloudDocs",
        home / "Library/CloudStorage",
    ]

    seen = set()
    for root in roots:
        if not root.exists():
            continue
        try:
            for obs_dir in _find_obsidian_dirs(root, max_depth):
                vault_path = obs_dir.parent
                key = str(vault_path.resolve())
                if key in seen:
                    continue
                seen.add(key)
                md_count = 0
                try:
                    for p in vault_path.rglob("*.md"):
                        md_count += 1
                        if md_count > 9999:
                            break
                except Exception:
                    pass
                candidates.append({
                    "path": str(vault_path),
                    "name": vault_path.name,
                    "file_count": md_count,
                })
        except Exception:
            continue

    candidates.sort(key=lambda c: -c["file_count"])
    return candidates


def _find_obsidian_dirs(root: Path, max_depth: int):
    """Yield .obsidian directories found under root, up to max_depth."""
    if max_depth < 0:
        return
    skip = {".Trash", "node_modules", ".git", "Library", ".cache", "venv", ".venv"}
    try:
        for child in root.iterdir():
            if child.name.startswith(".") and child.name != ".obsidian":
                continue
            if child.name in skip:
                continue
            if not child.is_dir():
                continue
            if child.name == ".obsidian":
                yield child
                continue
            if max_depth > 0:
                yield from _find_obsidian_dirs(child, max_depth - 1)
    except (PermissionError, OSError):
        return


# ---- CLI auth state probes ---------------------------------------------

# Per-method "are you already signed in?" probes. Each returns
# {"signed_in": bool, "detail": str} - "detail" is empty when unknown.
#
# These let the wizard show "Signed in" instead of "Sign in" when the underlying
# CLI already has a valid session, which avoids the idempotency trap (running
# `gh auth login` when already authed re-opens the browser and hangs).

def _run_cli(cmd: list[str], timeout: float = 5) -> tuple[int, str]:
    if not shutil.which(cmd[0]):
        return (127, "")
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return (r.returncode, (r.stdout + r.stderr))
    except Exception:
        return (-1, "")


def check_auth_state(method: str, account: str | None = None) -> dict:
    """Is the underlying CLI already signed in for this method?

    Returns {"signed_in": bool, "detail": str, "account": str|None}.
    Unknown state => signed_in=False with empty detail (safer: show "Sign in").
    """
    if method == "gh":
        rc, out = _run_cli(["gh", "auth", "status"])
        if rc == 0:
            return {"signed_in": True, "detail": _first_line(out), "account": None}
        return {"signed_in": False, "detail": "", "account": None}

    if method == "gog" or method.startswith("gog_"):
        rc, out = _run_cli(["gog", "auth", "list"])
        if rc != 0:
            return {"signed_in": False, "detail": "", "account": account}
        lines = [ln.strip() for ln in out.splitlines() if ln.strip() and not ln.startswith("config")]
        all_accounts = []
        for ln in lines:
            parts = ln.split("\t")
            if len(parts) >= 3:
                all_accounts.append(parts[0])
        if account:
            if account in all_accounts:
                return {"signed_in": True, "detail": f"{account}", "account": account}
            return {"signed_in": False, "detail": "", "account": account}
        if all_accounts:
            return {
                "signed_in": True,
                "detail": ", ".join(all_accounts),
                "account": all_accounts[0],
                "accounts": all_accounts,
            }
        return {"signed_in": False, "detail": "", "account": None}

    if method == "bird":
        rc, out = _run_cli(["bird", "whoami"])
        if rc == 0:
            return {"signed_in": True, "detail": _first_line(out), "account": None}
        return {"signed_in": False, "detail": "", "account": None}

    if method == "linkedin_browser":
        browser_dir = Path.home() / ".linkedin_browser" / "Default"
        if browser_dir.is_dir() and any(browser_dir.iterdir()):
            return {"signed_in": True, "detail": "Browser session available", "account": None}
        return {"signed_in": False, "detail": "", "account": None}

    if method == "wacli_pair":
        rc, out = _run_cli(["wacli", "auth", "status"])
        if rc == 0 and ("authenticated" in out.lower() or "paired" in out.lower() or "connected" in out.lower()):
            return {"signed_in": True, "detail": _first_line(out), "account": None}
        return {"signed_in": False, "detail": "", "account": None}

    return {"signed_in": False, "detail": "", "account": None}


def _first_line(s: str) -> str:
    for ln in s.splitlines():
        ln = ln.strip()
        if ln:
            return ln[:120]
    return ""


# ---- Nextcloud tester --------------------------------------------------

def test_nextcloud(server: str, username: str, token: str) -> dict:
    """Test a Nextcloud WebDAV connection. Returns {ok, message}."""
    try:
        import requests
    except ImportError:
        return {"ok": False, "message": "Python 'requests' package not installed"}

    if not server or not username or not token:
        return {"ok": False, "message": "server, username, and token all required"}

    server = server.rstrip("/")
    if not server.startswith(("http://", "https://")):
        server = "https://" + server

    url = f"{server}/remote.php/dav/files/{username}/"
    try:
        r = requests.request("PROPFIND", url, auth=(username, token),
                             headers={"Depth": "0"}, timeout=10)
        if r.status_code == 207:
            return {"ok": True, "message": f"Connected as {username}"}
        if r.status_code == 401:
            return {"ok": False, "message": "Auth failed - check username/app password"}
        if r.status_code == 404:
            return {"ok": False, "message": "User path not found - check username"}
        return {"ok": False, "message": f"HTTP {r.status_code}"}
    except requests.exceptions.ConnectionError:
        return {"ok": False, "message": "Can't connect to server - check URL"}
    except requests.exceptions.Timeout:
        return {"ok": False, "message": "Server timeout"}
    except Exception as e:
        return {"ok": False, "message": str(e)[:200]}


# ---- Interactive CLI auth sessions -------------------------------------

_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[a-zA-Z]|\x1b\][^\x07]*\x07|\r")


def strip_ansi(s: str) -> str:
    return _ANSI_RE.sub("", s).replace("\x07", "")


@dataclass
class AuthSession:
    """An interactive CLI session backed by a pseudoterminal."""
    id: str
    command: list[str]
    source_name: str
    parser: str = "generic"  # generic|gh|gog|bird|wacli
    env: dict = field(default_factory=dict)

    pid: int | None = None
    master_fd: int | None = None
    buffer: str = ""
    lines: list[str] = field(default_factory=list)
    done: bool = False
    exit_code: int | None = None
    started_at: float = field(default_factory=time.time)
    thread: threading.Thread | None = None

    # Parser output - structured state the UI consumes
    device_code: str | None = None
    verification_url: str | None = None
    qr_text: str | None = None
    prompt: str | None = None
    summary: str | None = None

    def start(self):
        pid, fd = pty.fork()
        if pid == 0:
            env = os.environ.copy()
            env.update(self.env)
            env["TERM"] = "dumb"
            env["NO_COLOR"] = "1"
            env["CI"] = "1"
            try:
                os.execvpe(self.command[0], self.command, env)
            except Exception:
                os._exit(127)
        self.pid = pid
        self.master_fd = fd
        self.thread = threading.Thread(target=self._pump, daemon=True)
        self.thread.start()

    def _pump(self):
        while not self.done:
            try:
                r, _, _ = select.select([self.master_fd], [], [], 0.5)
                if r:
                    try:
                        chunk = os.read(self.master_fd, 4096)
                    except OSError:
                        chunk = b""
                    if not chunk:
                        self._finish()
                        return
                    text = chunk.decode("utf-8", errors="replace")
                    text = strip_ansi(text)
                    self.buffer += text
                    self._flush_lines()
                    self._parse()
                try:
                    done_pid, status = os.waitpid(self.pid, os.WNOHANG)
                    if done_pid == self.pid:
                        self.exit_code = os.WEXITSTATUS(status) if os.WIFEXITED(status) else -1
                        self._finish()
                        return
                except ChildProcessError:
                    self._finish()
                    return
            except Exception:
                self._finish()
                return

    def _flush_lines(self):
        while "\n" in self.buffer:
            line, self.buffer = self.buffer.split("\n", 1)
            line = line.rstrip()
            if line:
                self.lines.append(line)

    def _finish(self):
        if self.done:
            return
        self.done = True
        if self.buffer.strip():
            self.lines.append(self.buffer.strip())
            self.buffer = ""
        try:
            if self.master_fd is not None:
                os.close(self.master_fd)
                self.master_fd = None
        except Exception:
            pass

    def _parse(self):
        """Extract structured state from the raw buffer."""
        combined = "\n".join(self.lines[-30:]) + "\n" + self.buffer

        if self.parser in ("gh", "generic"):
            m = re.search(r"one-time code:\s*([A-Z0-9-]+)", combined, re.I)
            if m:
                self.device_code = m.group(1)
            m = re.search(r"https://github\.com/login/device", combined)
            if m:
                self.verification_url = "https://github.com/login/device"
            if "Logged in" in combined or "already logged in" in combined.lower():
                self.summary = "Logged in"

        if self.parser == "gog":
            m = re.search(r"(https?://\S+)", combined)
            if m and "google.com" in m.group(1):
                self.verification_url = m.group(1)
            m = re.search(r"enter (?:the )?code:?\s*([A-Z0-9-]+)", combined, re.I)
            if m:
                self.device_code = m.group(1)
            if "Authenticated as" in combined or "success" in combined.lower():
                self.summary = "Authenticated"

        if self.parser == "bird":
            m = re.search(r"(https?://\S+)", combined)
            if m:
                self.verification_url = m.group(1)
            if "authenticated" in combined.lower() or "signed in" in combined.lower():
                self.summary = "Signed in"

        if self.parser == "wacli":
            lines = combined.split("\n")
            qr_lines = [ln for ln in lines if set(ln.strip()) <= {" ", "█", "▄", "▀", "░", "▒", "▓", "■", "□", "◼", "◻", "⬛", "⬜"} and len(ln.strip()) > 20]
            if qr_lines:
                self.qr_text = "\n".join(qr_lines[-30:])
            if "paired" in combined.lower() or "connected" in combined.lower():
                self.summary = "Paired"

    def send_input(self, text: str):
        if self.master_fd is None or self.done:
            return False
        try:
            os.write(self.master_fd, (text + "\n").encode("utf-8"))
            return True
        except Exception:
            return False

    def stop(self):
        if self.done:
            return
        try:
            if self.pid:
                os.kill(self.pid, signal.SIGTERM)
        except Exception:
            pass
        self._finish()

    def snapshot(self) -> dict:
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


class AuthSessionManager:
    def __init__(self):
        self._sessions: dict[str, AuthSession] = {}
        self._lock = threading.Lock()

    def create(self, source_name: str, command: list[str], parser: str = "generic",
               env: dict | None = None) -> AuthSession:
        sess = AuthSession(
            id=uuid.uuid4().hex[:12],
            source_name=source_name,
            command=command,
            parser=parser,
            env=env or {},
        )
        sess.start()
        with self._lock:
            self._sessions[sess.id] = sess
        return sess

    def get(self, sid: str) -> AuthSession | None:
        with self._lock:
            return self._sessions.get(sid)

    def list_active(self) -> list[dict]:
        with self._lock:
            return [s.snapshot() for s in self._sessions.values() if not s.done]

    def cleanup(self, max_age: float = 3600):
        cutoff = time.time() - max_age
        with self._lock:
            dead = [sid for sid, s in self._sessions.items()
                    if s.done and s.started_at < cutoff]
            for sid in dead:
                del self._sessions[sid]


# Commands per auth method - what CLI to spawn
AUTH_COMMANDS = {
    "gh": {
        "command": ["gh", "auth", "login", "--web", "-h", "github.com", "-p", "https", "--skip-ssh-key"],
        "env": {"GH_PROMPT_DISABLED": "1"},
        "parser": "gh",
    },
    "gog": {
        "command_fn": lambda account: ["gog", "auth", "add", account],
        "parser": "gog",
    },
    # bird has no interactive auth - it reads cookies from browsers automatically.
    # Auth check via check_auth_state("bird") which runs `bird whoami`.

    "wacli_pair": {
        "command": ["wacli", "auth"],
        "parser": "wacli",
    },
}


# ---- Telegram auth (custom, uses telethon directly) --------------------

DEFAULT_TELEGRAM_API_ID = (
    os.environ.get("VADIMGEST_SHARED_TELEGRAM_API_ID", "")
    or os.environ.get("TELEGRAM_API_ID", "")
)
DEFAULT_TELEGRAM_API_HASH = (
    os.environ.get("VADIMGEST_SHARED_TELEGRAM_API_HASH", "")
    or os.environ.get("TELEGRAM_API_HASH", "")
)


@dataclass
class TelegramAuthSession:
    id: str
    phone: str = ""
    phone_code_hash: str = ""
    client: object = None
    loop: object = None
    thread: threading.Thread | None = None
    needs_2fa: bool = False
    done: bool = False
    error: str | None = None
    user: dict | None = None

    def snapshot(self) -> dict:
        return {
            "id": self.id,
            "phone": self.phone,
            "needs_2fa": self.needs_2fa,
            "done": self.done,
            "error": self.error,
            "user": self.user,
        }


class TelegramAuthManager:
    def __init__(self):
        self._sessions: dict[str, TelegramAuthSession] = {}
        self._lock = threading.Lock()

    def start(self, phone: str, api_id: str, api_hash: str, session_path: str) -> TelegramAuthSession:
        sess = TelegramAuthSession(id=uuid.uuid4().hex[:12], phone=phone)

        def run():
            try:
                from telethon import TelegramClient
            except ImportError:
                sess.error = "telethon not installed"
                sess.done = True
                return

            loop = asyncio.new_event_loop()
            sess.loop = loop
            asyncio.set_event_loop(loop)
            client = TelegramClient(session_path, int(api_id), api_hash)
            sess.client = client

            async def do_send():
                await client.connect()
                if await client.is_user_authorized():
                    me = await client.get_me()
                    sess.user = {"id": me.id, "first_name": me.first_name or "", "username": me.username or ""}
                    sess.done = True
                    await client.disconnect()
                    return
                result = await client.send_code_request(phone)
                sess.phone_code_hash = result.phone_code_hash

            try:
                loop.run_until_complete(do_send())
            except Exception as e:
                sess.error = str(e)
                sess.done = True
                try:
                    loop.run_until_complete(client.disconnect())
                except Exception:
                    pass
                return

            if not sess.done:
                loop.run_forever()

        sess.thread = threading.Thread(target=run, daemon=True)
        sess.thread.start()
        sess.thread.join(timeout=15)

        with self._lock:
            self._sessions[sess.id] = sess
        return sess

    def verify(self, sid: str, code: str, password: str | None = None) -> dict:
        sess = self._sessions.get(sid)
        if not sess:
            return {"ok": False, "error": "session not found"}
        if sess.done and sess.user:
            return {"ok": True, "user": sess.user}
        if sess.done:
            return {"ok": False, "error": sess.error or "session ended"}
        if sess.error and not sess.needs_2fa:
            return {"ok": False, "error": sess.error}

        loop = sess.loop
        if not loop or not loop.is_running():
            return {"ok": False, "error": "session expired - start a new one"}

        result = {"ok": False, "error": None, "user": None}

        async def do_verify():
            from telethon.errors import SessionPasswordNeededError
            try:
                if sess.needs_2fa:
                    if not password:
                        result["error"] = "2FA password required"
                        return
                    await sess.client.sign_in(password=password)
                else:
                    await sess.client.sign_in(phone=sess.phone, code=code,
                                              phone_code_hash=sess.phone_code_hash)
                me = await sess.client.get_me()
                sess.user = {"id": me.id, "first_name": me.first_name or "", "username": me.username or ""}
                sess.done = True
                result["ok"] = True
                result["user"] = sess.user
                await sess.client.disconnect()
            except SessionPasswordNeededError:
                sess.needs_2fa = True
                result["error"] = "2FA password required"
                result["needs_2fa"] = True
            except Exception as e:
                result["error"] = str(e)

        try:
            fut = asyncio.run_coroutine_threadsafe(do_verify(), loop)
            fut.result(timeout=15)
        except Exception as e:
            result["error"] = str(e)

        if sess.done and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)

        return result

    def cancel(self, sid: str):
        sess = self._sessions.get(sid)
        if not sess:
            return
        sess.done = True
        try:
            loop = sess.loop
            if sess.client and loop and loop.is_running():
                async def dc():
                    await sess.client.disconnect()
                asyncio.run_coroutine_threadsafe(dc(), loop).result(timeout=5)
                loop.call_soon_threadsafe(loop.stop)
        except Exception:
            pass


AUTH_MANAGER = AuthSessionManager()
TELEGRAM_AUTH = TelegramAuthManager()
