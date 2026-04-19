"""Tests for source manifest system.

Every vadimgest source must declare a manifest describing:
- What it is (display_name, description, category)
- What it needs (dependencies: python packages, CLI tools, credentials, OS)
- What config fields it exposes (config_schema)
- How to check if it can run (check_ready)
"""

import importlib
import pytest
from vadimgest.ingest.sources import _SYNCER_REGISTRY, get_syncer_class, all_source_names
from vadimgest.ingest.sources.base import BaseSyncer, CronSyncer


# --- Manifest structure ---

VALID_CATEGORIES = {
    "messaging", "email", "calendar", "files",
    "dev", "activity", "meetings", "social", "knowledge",
}

VALID_DEP_KEYS = {"python", "cli", "credentials", "os"}

ALL_SOURCES = list(_SYNCER_REGISTRY.keys())


class TestManifestExists:
    """Every registered source must have manifest attributes."""

    @pytest.mark.parametrize("source_name", ALL_SOURCES)
    def test_has_display_name(self, source_name):
        cls = get_syncer_class(source_name)
        if cls is None:
            pytest.skip(f"{source_name} not loadable (missing deps)")
        assert hasattr(cls, "display_name"), f"{source_name} missing display_name"
        assert isinstance(cls.display_name, str)
        assert len(cls.display_name) > 0

    @pytest.mark.parametrize("source_name", ALL_SOURCES)
    def test_has_description(self, source_name):
        cls = get_syncer_class(source_name)
        if cls is None:
            pytest.skip(f"{source_name} not loadable (missing deps)")
        assert hasattr(cls, "description"), f"{source_name} missing description"
        assert isinstance(cls.description, str)
        assert len(cls.description) > 0

    @pytest.mark.parametrize("source_name", ALL_SOURCES)
    def test_has_category(self, source_name):
        cls = get_syncer_class(source_name)
        if cls is None:
            pytest.skip(f"{source_name} not loadable (missing deps)")
        assert hasattr(cls, "category"), f"{source_name} missing category"
        assert cls.category in VALID_CATEGORIES, (
            f"{source_name} category '{cls.category}' not in {VALID_CATEGORIES}"
        )

    @pytest.mark.parametrize("source_name", ALL_SOURCES)
    def test_has_dependencies(self, source_name):
        cls = get_syncer_class(source_name)
        if cls is None:
            pytest.skip(f"{source_name} not loadable (missing deps)")
        assert hasattr(cls, "dependencies"), f"{source_name} missing dependencies"
        deps = cls.dependencies
        assert isinstance(deps, dict)
        # Must have all required keys
        for key in VALID_DEP_KEYS:
            assert key in deps, f"{source_name} dependencies missing '{key}'"
            assert isinstance(deps[key], list), f"{source_name} deps['{key}'] must be list"

    @pytest.mark.parametrize("source_name", ALL_SOURCES)
    def test_has_config_schema(self, source_name):
        cls = get_syncer_class(source_name)
        if cls is None:
            pytest.skip(f"{source_name} not loadable (missing deps)")
        assert hasattr(cls, "config_schema"), f"{source_name} missing config_schema"
        assert isinstance(cls.config_schema, dict)


class TestManifestContent:
    """Validate manifest content makes sense."""

    @pytest.mark.parametrize("source_name", ALL_SOURCES)
    def test_source_name_matches_registry(self, source_name):
        cls = get_syncer_class(source_name)
        if cls is None:
            pytest.skip(f"{source_name} not loadable (missing deps)")
        assert cls.source_name == source_name

    @pytest.mark.parametrize("source_name", ALL_SOURCES)
    def test_config_schema_fields_have_type(self, source_name):
        cls = get_syncer_class(source_name)
        if cls is None:
            pytest.skip(f"{source_name} not loadable (missing deps)")
        for field_name, field_def in cls.config_schema.items():
            assert "type" in field_def, (
                f"{source_name}.config_schema['{field_name}'] missing 'type'"
            )
            assert field_def["type"] in ("str", "int", "bool", "list", "path"), (
                f"{source_name}.config_schema['{field_name}'] invalid type '{field_def['type']}'"
            )

    @pytest.mark.parametrize("source_name", ALL_SOURCES)
    def test_os_deps_are_valid(self, source_name):
        cls = get_syncer_class(source_name)
        if cls is None:
            pytest.skip(f"{source_name} not loadable (missing deps)")
        valid_os = {"macos", "macos:full_disk_access", "linux"}
        for os_dep in cls.dependencies["os"]:
            assert os_dep in valid_os, f"{source_name} invalid os dep: '{os_dep}'"


class TestCheckReady:
    """Each source must implement check_ready() classmethod."""

    @pytest.mark.parametrize("source_name", ALL_SOURCES)
    def test_has_check_ready(self, source_name):
        cls = get_syncer_class(source_name)
        if cls is None:
            pytest.skip(f"{source_name} not loadable (missing deps)")
        assert hasattr(cls, "check_ready"), f"{source_name} missing check_ready()"
        assert callable(cls.check_ready)

    @pytest.mark.parametrize("source_name", ALL_SOURCES)
    def test_check_ready_returns_dict(self, source_name):
        cls = get_syncer_class(source_name)
        if cls is None:
            pytest.skip(f"{source_name} not loadable (missing deps)")
        result = cls.check_ready()
        assert isinstance(result, dict), f"check_ready() must return dict, got {type(result)}"
        assert "ok" in result, "check_ready() must return dict with 'ok' key"
        assert isinstance(result["ok"], bool)

    @pytest.mark.parametrize("source_name", ALL_SOURCES)
    def test_check_ready_has_missing_when_not_ok(self, source_name):
        cls = get_syncer_class(source_name)
        if cls is None:
            pytest.skip(f"{source_name} not loadable (missing deps)")
        result = cls.check_ready()
        if not result["ok"]:
            assert "missing" in result, "check_ready() must include 'missing' list when not ok"
            assert isinstance(result["missing"], list)
            assert len(result["missing"]) > 0


class TestManifestAPI:
    """Test the manifest collection API used by dashboard."""

    def test_get_all_manifests(self):
        """get_all_manifests() returns dict of all sources with metadata."""
        from vadimgest.ingest.sources import get_all_manifests
        manifests = get_all_manifests()
        assert isinstance(manifests, dict)
        # Should have entry for every registered source
        assert set(manifests.keys()) == set(ALL_SOURCES)

    def test_manifest_structure(self):
        """Each manifest entry has required fields."""
        from vadimgest.ingest.sources import get_all_manifests
        manifests = get_all_manifests()
        required_fields = {
            "display_name", "description", "category",
            "dependencies", "config_schema", "loadable",
        }
        for name, manifest in manifests.items():
            for field in required_fields:
                assert field in manifest, f"manifest['{name}'] missing '{field}'"

    def test_unloadable_source_still_in_manifests(self):
        """Sources with missing deps should still appear with loadable=False."""
        from vadimgest.ingest.sources import get_all_manifests
        manifests = get_all_manifests()
        for name, manifest in manifests.items():
            assert isinstance(manifest["loadable"], bool)

    def test_manifest_includes_enabled_status(self):
        """Manifest should include whether source is enabled in config."""
        from vadimgest.ingest.sources import get_all_manifests
        manifests = get_all_manifests()
        for name, manifest in manifests.items():
            assert "enabled" in manifest, f"manifest['{name}'] missing 'enabled'"
            assert isinstance(manifest["enabled"], bool)

    def test_manifest_includes_ready_status(self):
        """Manifest should include check_ready results for loadable sources."""
        from vadimgest.ingest.sources import get_all_manifests
        manifests = get_all_manifests()
        for name, manifest in manifests.items():
            assert "ready" in manifest, f"manifest['{name}'] missing 'ready'"
            # ready is the check_ready() result dict or None if not loadable
            if manifest["loadable"]:
                assert isinstance(manifest["ready"], dict)
                assert "ok" in manifest["ready"]
