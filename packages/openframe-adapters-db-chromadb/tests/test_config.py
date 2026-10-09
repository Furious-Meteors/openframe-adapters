"""
tests/test_config.py
======================
Unit tests for ChromaDBSettings.
"""
from __future__ import annotations

from openframe.adapters.db.chromadb import ChromaDBSettings
from openframe.core.config import BaseAdapterSettings


class TestChromaDBSettings:
    def test_instantiates_with_no_args(self) -> None:
        """Every field has a default — no required env vars."""
        s = ChromaDBSettings()
        assert s.chroma_host == "localhost"

    def test_chroma_host_default(self) -> None:
        s = ChromaDBSettings()
        assert s.chroma_host == "localhost"

    def test_chroma_port_default(self) -> None:
        s = ChromaDBSettings()
        assert s.chroma_port == 8000

    def test_chroma_ssl_default(self) -> None:
        s = ChromaDBSettings()
        assert s.chroma_ssl is False

    def test_chroma_tenant_default(self) -> None:
        s = ChromaDBSettings()
        assert s.chroma_tenant == "default_tenant"

    def test_chroma_database_default(self) -> None:
        s = ChromaDBSettings()
        assert s.chroma_database == "default_database"

    def test_chroma_collection_default_empty(self) -> None:
        s = ChromaDBSettings()
        assert s.chroma_collection == ""

    def test_chroma_collection_settable(self) -> None:
        s = ChromaDBSettings(chroma_collection="items")
        assert s.chroma_collection == "items"

    def test_adapter_name_default(self) -> None:
        s = ChromaDBSettings()
        assert s.adapter_name == "chromadb"

    def test_connection_timeout_inherited_default(self) -> None:
        s = ChromaDBSettings()
        assert s.connection_timeout == 30.0

    def test_operation_timeout_inherited_default(self) -> None:
        s = ChromaDBSettings()
        assert s.operation_timeout == 10.0

    def test_max_retries_inherited_default(self) -> None:
        s = ChromaDBSettings()
        assert s.max_retries == 3

    def test_is_subclass_of_base_adapter_settings(self) -> None:
        assert issubclass(ChromaDBSettings, BaseAdapterSettings)

    def test_chroma_port_reads_from_env(self, monkeypatch) -> None:
        monkeypatch.setenv("CHROMA_PORT", "9000")
        s = ChromaDBSettings()
        assert s.chroma_port == 9000

    def test_chroma_host_reads_from_env(self, monkeypatch) -> None:
        monkeypatch.setenv("CHROMA_HOST", "chroma.internal")
        s = ChromaDBSettings()
        assert s.chroma_host == "chroma.internal"
