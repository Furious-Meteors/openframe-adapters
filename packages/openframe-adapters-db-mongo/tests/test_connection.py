"""
tests/test_connection.py
==========================
Unit tests for get_mongo_client() and _client_cache.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pymongo.errors
import pytest

from openframe.adapters.db.mongo import MongoSettings, get_mongo_client
from openframe.adapters.db.mongo.connection import _client_cache
from openframe.core.exceptions import AdapterConfigurationError


@pytest.fixture(autouse=True)
def clear_client_cache() -> object:
    """Ensure _client_cache is clean before and after every test."""
    _client_cache.clear()
    yield
    _client_cache.clear()


@pytest.fixture
def settings() -> MongoSettings:
    return MongoSettings(
        mongo_url="mongodb://u:p@localhost:27017",
        mongo_database="db",
    )


@pytest.fixture
def settings_alt() -> MongoSettings:
    return MongoSettings(
        mongo_url="mongodb://u:p@localhost:27017/db2",
        mongo_database="db2",
    )


class TestGetMongoClient:
    def test_returns_cached_client_on_second_call(
        self, settings: MongoSettings
    ) -> None:
        fake_client = MagicMock()
        with patch(
            "openframe.adapters.db.mongo.connection.AsyncIOMotorClient",
            return_value=fake_client,
        ):
            c1 = get_mongo_client(settings)
            c2 = get_mongo_client(settings)

        assert c1 is c2
        assert c1 is fake_client

    def test_different_urls_produce_different_clients(
        self, settings: MongoSettings, settings_alt: MongoSettings
    ) -> None:
        client_a = MagicMock(name="client_a")
        client_b = MagicMock(name="client_b")
        with patch(
            "openframe.adapters.db.mongo.connection.AsyncIOMotorClient",
            side_effect=[client_a, client_b],
        ):
            c1 = get_mongo_client(settings)
            c2 = get_mongo_client(settings_alt)

        assert c1 is not c2

    def test_client_stored_in_cache(self, settings: MongoSettings) -> None:
        fake_client = MagicMock()
        with patch(
            "openframe.adapters.db.mongo.connection.AsyncIOMotorClient",
            return_value=fake_client,
        ):
            get_mongo_client(settings)

        assert _client_cache[settings.mongo_url] is fake_client

    def test_configuration_error_raises_adapter_configuration_error(
        self, settings: MongoSettings
    ) -> None:
        with patch(
            "openframe.adapters.db.mongo.connection.AsyncIOMotorClient",
            side_effect=pymongo.errors.ConfigurationError("bad url"),
        ):
            with pytest.raises(AdapterConfigurationError) as exc_info:
                get_mongo_client(settings)

        assert exc_info.value.operation == "init"

    def test_client_passes_correct_kwargs(self, settings: MongoSettings) -> None:
        fake_client = MagicMock()
        with patch(
            "openframe.adapters.db.mongo.connection.AsyncIOMotorClient",
            return_value=fake_client,
        ) as mock_cls:
            get_mongo_client(settings)

        _, kwargs = mock_cls.call_args
        assert kwargs["serverSelectionTimeoutMS"] == settings.mongo_server_selection_timeout_ms
        assert kwargs["minPoolSize"] == settings.mongo_min_pool_size
        assert kwargs["maxPoolSize"] == settings.mongo_max_pool_size
        assert kwargs["tls"] == settings.mongo_tls
        assert kwargs["tlsAllowInvalidCertificates"] == settings.mongo_tls_allow_invalid_certs
