"""
tests/test_config.py
======================
Unit tests for InfluxDBSettings.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from openframe.adapters.db.influxdb import InfluxDBSettings
from openframe.core.config import BaseAdapterSettings


def _settings(**overrides) -> InfluxDBSettings:
    base = dict(
        influxdb_url="http://localhost:8086",
        influxdb_token="tok",
        influxdb_org="org",
        influxdb_bucket="bucket",
    )
    base.update(overrides)
    return InfluxDBSettings(**base)


class TestInfluxDBSettings:
    def test_instantiates_with_required_fields(self) -> None:
        s = _settings()
        assert s.influxdb_url == "http://localhost:8086"
        assert s.influxdb_token == "tok"
        assert s.influxdb_org == "org"
        assert s.influxdb_bucket == "bucket"

    @pytest.mark.parametrize(
        "missing",
        ["influxdb_url", "influxdb_token", "influxdb_org", "influxdb_bucket"],
    )
    def test_missing_required_field_raises_validation_error(self, missing: str) -> None:
        kwargs = dict(
            influxdb_url="http://localhost:8086",
            influxdb_token="tok",
            influxdb_org="org",
            influxdb_bucket="bucket",
        )
        kwargs.pop(missing)
        with pytest.raises(ValidationError):
            InfluxDBSettings(**kwargs)  # type: ignore[arg-type]

    def test_client_timeout_ms_default(self) -> None:
        s = _settings()
        assert s.client_timeout_ms == 10_000

    def test_lookback_default(self) -> None:
        s = _settings()
        assert s.lookback == "-30d"

    def test_adapter_name_default(self) -> None:
        s = _settings()
        assert s.adapter_name == "influxdb"

    def test_connection_timeout_inherited_default(self) -> None:
        s = _settings()
        assert s.connection_timeout == 30.0

    def test_operation_timeout_inherited_default(self) -> None:
        s = _settings()
        assert s.operation_timeout == 10.0

    def test_max_retries_inherited_default(self) -> None:
        s = _settings()
        assert s.max_retries == 3

    def test_is_subclass_of_base_adapter_settings(self) -> None:
        assert issubclass(InfluxDBSettings, BaseAdapterSettings)

    def test_client_timeout_ms_reads_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CLIENT_TIMEOUT_MS", "5000")
        s = _settings()
        assert s.client_timeout_ms == 5000
