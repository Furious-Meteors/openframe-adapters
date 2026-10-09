"""
tests/test_config.py
======================
Unit tests for OracleSettings.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from openframe.adapters.db.oracle import OracleSettings
from openframe.core.config import BaseAdapterSettings


class TestOracleSettings:
    def test_instantiates_with_required_fields(self) -> None:
        s = OracleSettings(oracle_dsn="app/secret@localhost:1521/orclpdb")
        assert s.oracle_dsn == "app/secret@localhost:1521/orclpdb"

    def test_missing_oracle_dsn_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError):
            OracleSettings()  # type: ignore[call-arg]

    def test_pool_min_default(self) -> None:
        s = OracleSettings(oracle_dsn="a/b@host:1521/svc")
        assert s.pool_min == 1

    def test_pool_max_default(self) -> None:
        s = OracleSettings(oracle_dsn="a/b@host:1521/svc")
        assert s.pool_max == 10

    def test_pool_increment_default(self) -> None:
        s = OracleSettings(oracle_dsn="a/b@host:1521/svc")
        assert s.pool_increment == 1

    def test_pool_timeout_default(self) -> None:
        s = OracleSettings(oracle_dsn="a/b@host:1521/svc")
        assert s.pool_timeout == 60

    def test_adapter_name_default(self) -> None:
        s = OracleSettings(oracle_dsn="a/b@host:1521/svc")
        assert s.adapter_name == "oracle"

    def test_connection_timeout_inherited_default(self) -> None:
        s = OracleSettings(oracle_dsn="a/b@host:1521/svc")
        assert s.connection_timeout == 30.0

    def test_operation_timeout_inherited_default(self) -> None:
        s = OracleSettings(oracle_dsn="a/b@host:1521/svc")
        assert s.operation_timeout == 10.0

    def test_max_retries_inherited_default(self) -> None:
        s = OracleSettings(oracle_dsn="a/b@host:1521/svc")
        assert s.max_retries == 3

    def test_pool_max_reads_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POOL_MAX", "25")
        s = OracleSettings(oracle_dsn="a/b@host:1521/svc")
        assert s.pool_max == 25

    def test_is_subclass_of_base_adapter_settings(self) -> None:
        assert issubclass(OracleSettings, BaseAdapterSettings)
