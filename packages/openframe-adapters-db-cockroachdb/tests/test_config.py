"""
tests/test_config.py
======================
Unit tests for CockroachdbSettings.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from openframe.adapters.db.cockroachdb import CockroachdbSettings
from openframe.core.config import BaseAdapterSettings


class TestCockroachdbSettings:
    def test_instantiates_with_cockroachdb_url(self) -> None:
        s = CockroachdbSettings(cockroachdb_url="postgresql://u:p@localhost:26257/db")
        assert s.cockroachdb_url == "postgresql://u:p@localhost:26257/db"

    def test_missing_cockroachdb_url_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError):
            CockroachdbSettings()  # type: ignore[call-arg]

    def test_pool_size_default(self) -> None:
        s = CockroachdbSettings(cockroachdb_url="postgresql://u:p@localhost:26257/db")
        assert s.pool_size == 10

    def test_pool_command_timeout_default(self) -> None:
        s = CockroachdbSettings(cockroachdb_url="postgresql://u:p@localhost:26257/db")
        assert s.pool_command_timeout == 60.0

    def test_adapter_name_default(self) -> None:
        s = CockroachdbSettings(cockroachdb_url="postgresql://u:p@localhost:26257/db")
        assert s.adapter_name == "cockroachdb"

    def test_connection_timeout_inherited_default(self) -> None:
        s = CockroachdbSettings(cockroachdb_url="postgresql://u:p@localhost:26257/db")
        assert s.connection_timeout == 30.0

    def test_pool_size_reads_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POOL_SIZE", "20")
        s = CockroachdbSettings(cockroachdb_url="postgresql://u:p@localhost:26257/db")
        assert s.pool_size == 20

    def test_is_subclass_of_base_adapter_settings(self) -> None:
        assert issubclass(CockroachdbSettings, BaseAdapterSettings)

    def test_pool_max_queries_default(self) -> None:
        s = CockroachdbSettings(cockroachdb_url="postgresql://u:p@localhost:26257/db")
        assert s.pool_max_queries == 50_000

    def test_pool_max_inactive_conn_lifetime_default(self) -> None:
        s = CockroachdbSettings(cockroachdb_url="postgresql://u:p@localhost:26257/db")
        assert s.pool_max_inactive_conn_lifetime == 300.0

    def test_operation_timeout_inherited_default(self) -> None:
        s = CockroachdbSettings(cockroachdb_url="postgresql://u:p@localhost:26257/db")
        assert s.operation_timeout == 10.0

    def test_max_retries_inherited_default(self) -> None:
        s = CockroachdbSettings(cockroachdb_url="postgresql://u:p@localhost:26257/db")
        assert s.max_retries == 3
