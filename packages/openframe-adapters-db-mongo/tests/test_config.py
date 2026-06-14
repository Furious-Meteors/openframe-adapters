"""
tests/test_config.py
======================
Unit tests for MongoSettings.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from openframe.adapters.db.mongo import MongoSettings
from openframe.core.config import BaseAdapterSettings


class TestMongoSettings:
    def test_instantiates_with_required_fields(self) -> None:
        s = MongoSettings(
            mongo_url="mongodb://u:p@localhost:27017",
            mongo_database="mydb",
        )
        assert s.mongo_url == "mongodb://u:p@localhost:27017"
        assert s.mongo_database == "mydb"

    def test_missing_mongo_url_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError):
            MongoSettings(mongo_database="mydb")  # type: ignore[call-arg]

    def test_missing_mongo_database_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError):
            MongoSettings(mongo_url="mongodb://u:p@localhost:27017")  # type: ignore[call-arg]

    def test_mongo_min_pool_size_default(self) -> None:
        s = MongoSettings(mongo_url="mongodb://u:p@localhost", mongo_database="db")
        assert s.mongo_min_pool_size == 5

    def test_mongo_max_pool_size_default(self) -> None:
        s = MongoSettings(mongo_url="mongodb://u:p@localhost", mongo_database="db")
        assert s.mongo_max_pool_size == 20

    def test_mongo_server_selection_timeout_ms_default(self) -> None:
        s = MongoSettings(mongo_url="mongodb://u:p@localhost", mongo_database="db")
        assert s.mongo_server_selection_timeout_ms == 5000

    def test_mongo_tls_default(self) -> None:
        s = MongoSettings(mongo_url="mongodb://u:p@localhost", mongo_database="db")
        assert s.mongo_tls is False

    def test_adapter_name_default(self) -> None:
        s = MongoSettings(mongo_url="mongodb://u:p@localhost", mongo_database="db")
        assert s.adapter_name == "mongo"

    def test_connection_timeout_inherited_default(self) -> None:
        s = MongoSettings(mongo_url="mongodb://u:p@localhost", mongo_database="db")
        assert s.connection_timeout == 30.0

    def test_max_pool_size_reads_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MONGO_MAX_POOL_SIZE", "50")
        s = MongoSettings(mongo_url="mongodb://u:p@localhost", mongo_database="db")
        assert s.mongo_max_pool_size == 50

    def test_is_subclass_of_base_adapter_settings(self) -> None:
        assert issubclass(MongoSettings, BaseAdapterSettings)
