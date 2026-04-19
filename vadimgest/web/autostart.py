"""Autostart service management for vadimgest.

Installs/removes system services so the dashboard and sync daemon
start on boot. Supports launchd (macOS) and systemd (Linux).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

LAUNCHD_LABELS = ["com.vadimgest.dashboard", "com.vadimgest.daemon"]
SYSTEMD_UNITS = ["vadimgest-dashboard", "vadimgest-daemon"]


def is_installed() -> bool:
    if sys.platform == "darwin":
        agents = Path.home() / "Library" / "LaunchAgents"
        return all((agents / f"{label}.plist").exists() for label in LAUNCHD_LABELS)
    elif sys.platform == "linux":
        units = Path.home() / ".config" / "systemd" / "user"
        return all((units / f"{name}.service").exists() for name in SYSTEMD_UNITS)
    return False


def install(port: int = 8484, interval: int = 300):
    python = sys.executable
    if sys.platform == "darwin":
        _install_launchd(python, port, interval)
    elif sys.platform == "linux":
        _install_systemd(python, port, interval)
    else:
        raise RuntimeError(f"Autostart not supported on {sys.platform}")


def uninstall(keep_running: bool = False):
    if sys.platform == "darwin":
        _uninstall_launchd(keep_running=keep_running)
    elif sys.platform == "linux":
        _uninstall_systemd(keep_running=keep_running)
    else:
        raise RuntimeError(f"Autostart not supported on {sys.platform}")


def _build_path() -> str:
    """Build a PATH that includes common tool locations for readiness checks."""
    home = str(Path.home())
    candidates = [
        f"{home}/bin",
        f"{home}/.local/bin",
        "/opt/homebrew/bin",
        "/opt/homebrew/sbin",
        "/usr/local/bin",
        "/usr/bin",
        "/bin",
        "/usr/sbin",
        "/sbin",
    ]
    import os
    for p in os.environ.get("PATH", "").split(":"):
        if p and p not in candidates:
            candidates.append(p)
    return ":".join(p for p in candidates if Path(p).is_dir())


def _install_launchd(python: str, port: int, interval: int):
    agents_dir = Path.home() / "Library" / "LaunchAgents"
    agents_dir.mkdir(parents=True, exist_ok=True)

    path_value = _build_path()

    services = {
        "com.vadimgest.dashboard": {
            "args": [python, "-m", "vadimgest", "serve", "--no-open", "--port", str(port)],
            "log": "/tmp/vadimgest-dashboard.log",
        },
        "com.vadimgest.daemon": {
            "args": [python, "-m", "vadimgest", "daemon", "--interval", str(interval)],
            "log": "/tmp/vadimgest-daemon.log",
        },
    }

    for label, svc in services.items():
        plist_path = agents_dir / f"{label}.plist"
        if plist_path.exists():
            subprocess.run(["launchctl", "unload", str(plist_path)], capture_output=True)

        args_xml = "\n".join(f"        <string>{a}</string>" for a in svc["args"])
        plist = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>
    <key>ProgramArguments</key>
    <array>
{args_xml}
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>{path_value}</string>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{svc["log"]}</string>
    <key>StandardErrorPath</key>
    <string>{svc["log"]}</string>
</dict>
</plist>
"""
        plist_path.write_text(plist)
        subprocess.run(["launchctl", "load", str(plist_path)], capture_output=True)


def _uninstall_launchd(keep_running: bool = False):
    agents_dir = Path.home() / "Library" / "LaunchAgents"
    skip = {"com.vadimgest.dashboard"} if keep_running else set()
    for label in LAUNCHD_LABELS:
        plist_path = agents_dir / f"{label}.plist"
        if plist_path.exists():
            if label not in skip:
                subprocess.run(["launchctl", "unload", str(plist_path)], capture_output=True)
            plist_path.unlink()


def _install_systemd(python: str, port: int, interval: int):
    unit_dir = Path.home() / ".config" / "systemd" / "user"
    unit_dir.mkdir(parents=True, exist_ok=True)

    services = {
        "vadimgest-dashboard": {
            "desc": "vadimgest dashboard",
            "exec": f"{python} -m vadimgest serve --no-open --port {port}",
        },
        "vadimgest-daemon": {
            "desc": "vadimgest sync daemon",
            "exec": f"{python} -m vadimgest daemon --interval {interval}",
        },
    }

    for name, svc in services.items():
        unit_path = unit_dir / f"{name}.service"
        unit = f"""[Unit]
Description={svc["desc"]}
After=network.target

[Service]
ExecStart={svc["exec"]}
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
"""
        unit_path.write_text(unit)

    subprocess.run(["systemctl", "--user", "daemon-reload"], capture_output=True)
    for name in services:
        subprocess.run(["systemctl", "--user", "enable", "--now", name], capture_output=True)


def _uninstall_systemd(keep_running: bool = False):
    unit_dir = Path.home() / ".config" / "systemd" / "user"
    skip = {"vadimgest-dashboard"} if keep_running else set()
    for name in SYSTEMD_UNITS:
        if name not in skip:
            subprocess.run(["systemctl", "--user", "disable", "--now", name], capture_output=True)
        else:
            subprocess.run(["systemctl", "--user", "disable", name], capture_output=True)
        unit_path = unit_dir / f"{name}.service"
        if unit_path.exists():
            unit_path.unlink()
