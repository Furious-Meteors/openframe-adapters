"""
tests/test_config.py
======================
Unit tests for MariadbSettings.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from openframe.adapters.db.mariadb import MariadbSettings
from openframe.core.config import BaseAdapterSettings


class TestMariadbSettings:
    def test_instantiates_with_database_url(self) -> None:
        s = MariadbSettings(database_url="mysql://u:p@localhost/db")
        assert s.database_url == "mysql://u:p@localhost/db"

    def test_missing_database_url_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError):
            MariadbSettings()  # type: ignore[call-arg]

    def test_pool_size_default(self) -> None:
        s = MariadbSettings(database_url="mysql://u:p@localhost/db")
        assert s.pool_size == 10

    def test_pool_recycle_default(self) -> None:
        s = MariadbSettings(database_url="mysql://u:p@localhost/db")
        assert s.pool_recycle == 300.0

    def test_pool_connect_timeout_default(self) -> None:
        s = MariadbSettings(database_url="mysql://u:p@localhost/db")
        assert s.pool_connect_timeout == 10.0

    def test_adapter_name_default(self) -> None:
        s = MariadbSettings(database_url="mysql://u:p@localhost/db")
        assert s.adapter_name == "mariadb"

    def test_connection_timeout_inherited_default(self) -> None:
        s = MariadbSettings(database_url="mysql://u:p@localhost/db")
        assert s.connection_timeout == 30.0

    def test_pool_size_reads_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POOL_SIZE", "20")
        s = MariadbSettings(database_url="mysql://u:p@localhost/db")
        assert s.pool_size == 20

    def test_is_subclass_of_base_adapter_settings(self) -> None:
        assert issubclass(MariadbSettings, BaseAdapterSettings)

    def test_operation_timeout_inherited_default(self) -> None:
        s = MariadbSettings(database_url="mysql://u:p@localhost/db")
        assert s.operation_timeout == 10.0

    def test_max_retries_inherited_default(self) -> None:
        s = MariadbSettings(database_url="mysql://u:p@localhost/db")
        assert s.max_retries == 3
