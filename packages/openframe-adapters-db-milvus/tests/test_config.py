"""
tests/test_config.py
======================
Unit tests for MilvusSettings.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from openframe.adapters.db.milvus import MilvusSettings
from openframe.core.config import BaseAdapterSettings


class TestMilvusSettings:
    def test_instantiates_with_milvus_uri(self) -> None:
        s = MilvusSettings(milvus_uri="http://localhost:19530")
        assert s.milvus_uri == "http://localhost:19530"

    def test_missing_milvus_uri_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError):
            MilvusSettings()  # type: ignore[call-arg]

    def test_milvus_token_default(self) -> None:
        s = MilvusSettings(milvus_uri="http://localhost:19530")
        assert s.milvus_token == ""

    def test_milvus_db_name_default(self) -> None:
        s = MilvusSettings(milvus_uri="http://localhost:19530")
        assert s.milvus_db_name == "default"

    def test_metric_type_default(self) -> None:
        s = MilvusSettings(milvus_uri="http://localhost:19530")
        assert s.metric_type == "COSINE"

    def test_vector_dim_default(self) -> None:
        s = MilvusSettings(milvus_uri="http://localhost:19530")
        assert s.vector_dim == 128

    def test_adapter_name_default(self) -> None:
        s = MilvusSettings(milvus_uri="http://localhost:19530")
        assert s.adapter_name == "milvus"

    def test_connection_timeout_inherited_default(self) -> None:
        s = MilvusSettings(milvus_uri="http://localhost:19530")
        assert s.connection_timeout == 30.0

    def test_operation_timeout_inherited_default(self) -> None:
        s = MilvusSettings(milvus_uri="http://localhost:19530")
        assert s.operation_timeout == 10.0

    def test_max_retries_inherited_default(self) -> None:
        s = MilvusSettings(milvus_uri="http://localhost:19530")
        assert s.max_retries == 3

    def test_is_subclass_of_base_adapter_settings(self) -> None:
        assert issubclass(MilvusSettings, BaseAdapterSettings)

    def test_milvus_token_reads_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MILVUS_URI", "http://localhost:19530")
        monkeypatch.setenv("MILVUS_TOKEN", "root:Milvus")
        s = MilvusSettings()
        assert s.milvus_token == "root:Milvus"
