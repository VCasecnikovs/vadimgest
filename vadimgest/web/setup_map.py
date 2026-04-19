"""Per-source setup metadata.

This maps source_name to user-facing setup instructions and auth metadata.
The wizard uses this to render concrete next steps beyond just "install package".

Schema per source (all optional):
    app: str  -- key into setup.KNOWN_APPS for macOS app detection + download link
    auth: dict  -- {"method": str, "label": str, "needs_account": bool, "account_label": str}
                   Method is one of: gh | gog | bird | wacli_pair | telegram_phone | linkedin_browser
    config_helper: str  -- hint for what the drawer should render ("obsidian_vault_picker", "nextcloud_form")
    os_help: str|list  -- structured OS requirement help (e.g. Full Disk Access deep link)
    post_install_hint: str  -- short one-line tip shown under install buttons
    recommended_alt: dict  -- {"source": "hlopya", "reason": "..."} suggestion
"""

SOURCE_SETUP = {
    "telegram": {
        "auth": {
            "method": "telegram_phone",
            "label": "Sign in to Telegram",
            "needs_account": False,
        },
        "post_install_hint": "We'll send an SMS code to your phone, just like Telegram Desktop.",
    },
    "signal": {
        "app": "signal",
        "post_install_hint": "Signal Desktop reads messages from its local database - you need it installed and signed in.",
    },
    "granola": {
        "app": "granola",
        "recommended_alt": {
            "source": "hlopya",
            "reason": "Hlopya is an open-source meeting recorder - same result, no Granola subscription.",
        },
    },
    "dayflow": {
        "app": "dayflow",
    },
    "obsidian": {
        "config_helper": "obsidian_vault_picker",
        "post_install_hint": "Point vadimgest at your existing vault directory.",
    },
    "claude": {
        "post_install_hint": "Reads Claude Code session files from ~/.claude - works out of the box.",
    },
    "github": {
        "auth": {
            "method": "gh",
            "label": "Sign in to GitHub",
            "needs_account": False,
        },
        "post_install_hint": "One-click OAuth with a device code - no terminal needed.",
    },
    "github_notifications": {
        "auth": {
            "method": "gh",
            "label": "Sign in to GitHub",
            "needs_account": False,
        },
        "post_install_hint": "Uses the same login as GitHub Issues.",
    },
    "gmail": {
        "auth": {
            "method": "gog",
            "label": "Connect Google account",
            "needs_account": True,
            "account_label": "Google account",
            "account_placeholder": "name@gmail.com",
            "multi": True,
        },
        "post_install_hint": "You can add multiple Gmail accounts.",
    },
    "gtasks": {
        "auth": {
            "method": "gog",
            "label": "Connect Google account",
            "needs_account": True,
            "account_label": "Google account",
            "account_placeholder": "name@gmail.com",
            "multi": True,
        },
    },
    "calendar": {
        "auth": {
            "method": "gog",
            "label": "Connect Google account",
            "needs_account": True,
            "account_label": "Google account",
            "account_placeholder": "name@gmail.com",
            "multi": True,
        },
    },
    "gdrive": {
        "auth": {
            "method": "gog",
            "label": "Connect Google account",
            "needs_account": True,
            "account_label": "Google account",
            "account_placeholder": "name@gmail.com",
            "multi": True,
        },
    },
    "whatsapp": {
        "auth": {
            "method": "wacli_pair",
            "label": "Pair with WhatsApp",
            "needs_account": False,
        },
        "post_install_hint": "Scan the QR code from your phone's WhatsApp \u2192 Settings \u2192 Linked Devices.",
    },
    "imessage": {
        "os_help": {
            "kind": "full_disk_access",
            "label": "Grant Full Disk Access",
            "instructions": "Terminal.app (or wherever you launched vadimgest) needs Full Disk Access to read ~/Library/Messages/chat.db.",
            "deeplink": "x-apple.systempreferences:com.apple.preference.security?Privacy_AllFiles",
        },
    },
    "browser": {
        "os_help": {
            "kind": "full_disk_access",
            "label": "Grant Full Disk Access (for Safari)",
            "instructions": "Safari history lives inside ~/Library - needs Full Disk Access. Chrome/Firefox don't.",
            "deeplink": "x-apple.systempreferences:com.apple.preference.security?Privacy_AllFiles",
        },
    },
    "linkedin": {
        "auth": {
            "method": "linkedin_browser",
            "label": "Sign in to LinkedIn",
            "needs_account": False,
        },
        "post_install_hint": "Opens a real Chromium window once - log in like you would in a normal browser. We save the session.",
    },
    "nextcloud": {
        "config_helper": "nextcloud_form",
        "post_install_hint": "You'll need an app password - we'll link to where you create it.",
    },
    "xnews": {
        "auth": {
            "method": "bird",
            "label": "Sign in to X / Twitter",
            "needs_account": False,
        },
        "post_install_hint": "bird reads cookies from your browser. Sign in to x.com in Safari or Google Chrome first.",
    },
    "hlopya": {
        "app": "hlopya",
        "post_install_hint": "Open-source meeting recorder. Drop-in replacement for Granola.",
    },
}


def get_setup_info(source_name: str) -> dict:
    """Return setup metadata for a source (empty dict if none)."""
    return SOURCE_SETUP.get(source_name, {})


def enrich_manifest(manifest: dict) -> dict:
    """Merge setup info into a manifest dict."""
    for name, m in manifest.items():
        info = get_setup_info(name)
        if info:
            m["setup_info"] = info
    return manifest
