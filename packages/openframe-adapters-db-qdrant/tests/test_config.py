"""
tests/test_config.py
======================
Unit tests for QdrantSettings.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from openframe.adapters.db.qdrant import QdrantSettings
from openframe.core.config import BaseAdapterSettings


class TestQdrantSettings:
    def test_instantiates_with_url(self) -> None:
        s = QdrantSettings(qdrant_url="http://localhost:6333")
        assert s.qdrant_url == "http://localhost:6333"

    def test_missing_url_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError):
            QdrantSettings()  # type: ignore[call-arg]

    def test_api_key_default_none(self) -> None:
        s = QdrantSettings(qdrant_url="http://localhost:6333")
        assert s.qdrant_api_key is None

    def test_prefer_grpc_default_false(self) -> None:
        s = QdrantSettings(qdrant_url="http://localhost:6333")
        assert s.qdrant_prefer_grpc is False

    def test_https_default_none(self) -> None:
        s = QdrantSettings(qdrant_url="http://localhost:6333")
        assert s.qdrant_https is None

    def test_adapter_name_default(self) -> None:
        s = QdrantSettings(qdrant_url="http://localhost:6333")
        assert s.adapter_name == "qdrant"

    def test_connection_timeout_inherited_default(self) -> None:
        s = QdrantSettings(qdrant_url="http://localhost:6333")
        assert s.connection_timeout == 30.0

    def test_operation_timeout_inherited_default(self) -> None:
        s = QdrantSettings(qdrant_url="http://localhost:6333")
        assert s.operation_timeout == 10.0

    def test_max_retries_inherited_default(self) -> None:
        s = QdrantSettings(qdrant_url="http://localhost:6333")
        assert s.max_retries == 3

    def test_is_subclass_of_base_adapter_settings(self) -> None:
        assert issubclass(QdrantSettings, BaseAdapterSettings)

    def test_prefer_grpc_reads_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("QDRANT_PREFER_GRPC", "true")
        s = QdrantSettings(qdrant_url="http://localhost:6333")
        assert s.qdrant_prefer_grpc is True
