"""
tests/test_config.py
======================
Unit tests for CassandraSettings.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from openframe.adapters.db.cassandra import CassandraSettings
from openframe.core.config import BaseAdapterSettings


class TestCassandraSettings:
    def test_instantiates_with_contact_points(self) -> None:
        s = CassandraSettings(cassandra_contact_points=["10.0.0.1"])
        assert s.cassandra_contact_points == ["10.0.0.1"]

    def test_missing_contact_points_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError):
            CassandraSettings()  # type: ignore[call-arg]

    def test_port_default(self) -> None:
        s = CassandraSettings(cassandra_contact_points=["10.0.0.1"])
        assert s.cassandra_port == 9042

    def test_keyspace_default_is_none(self) -> None:
        s = CassandraSettings(cassandra_contact_points=["10.0.0.1"])
        assert s.cassandra_keyspace is None

    def test_adapter_name_default(self) -> None:
        s = CassandraSettings(cassandra_contact_points=["10.0.0.1"])
        assert s.adapter_name == "cassandra"

    def test_connection_timeout_inherited_default(self) -> None:
        s = CassandraSettings(cassandra_contact_points=["10.0.0.1"])
        assert s.connection_timeout == 30.0

    def test_operation_timeout_inherited_default(self) -> None:
        s = CassandraSettings(cassandra_contact_points=["10.0.0.1"])
        assert s.operation_timeout == 10.0

    def test_max_retries_inherited_default(self) -> None:
        s = CassandraSettings(cassandra_contact_points=["10.0.0.1"])
        assert s.max_retries == 3

    def test_core_connections_per_host_default(self) -> None:
        s = CassandraSettings(cassandra_contact_points=["10.0.0.1"])
        assert s.cassandra_core_connections_per_host == 2

    def test_username_password_default_to_none(self) -> None:
        s = CassandraSettings(cassandra_contact_points=["10.0.0.1"])
        assert s.cassandra_username is None
        assert s.cassandra_password is None

    def test_port_reads_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CASSANDRA_PORT", "9043")
        s = CassandraSettings(cassandra_contact_points=["10.0.0.1"])
        assert s.cassandra_port == 9043

    def test_contact_points_reads_from_env_as_json_list(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("CASSANDRA_CONTACT_POINTS", '["10.0.0.1", "10.0.0.2"]')
        s = CassandraSettings()
        assert s.cassandra_contact_points == ["10.0.0.1", "10.0.0.2"]

    def test_is_subclass_of_base_adapter_settings(self) -> None:
        assert issubclass(CassandraSettings, BaseAdapterSettings)
