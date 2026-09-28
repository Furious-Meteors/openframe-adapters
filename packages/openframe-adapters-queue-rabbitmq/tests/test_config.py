"""
tests/test_config.py — openframe-adapters-queue-rabbitmq
=============================================================
Unit tests for RabbitmqSettings.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from openframe.adapters.queue.rabbitmq import RabbitmqSettings
from openframe.core.config import BaseAdapterSettings


class TestRabbitmqSettings:
    def test_instantiates_with_required_fields(self):
        s = RabbitmqSettings(rabbitmq_url="amqp://guest:guest@localhost:5672/")
        assert s.rabbitmq_url == "amqp://guest:guest@localhost:5672/"

    def test_missing_url_raises_validation_error(self):
        with pytest.raises(ValidationError):
            RabbitmqSettings()

    def test_rabbitmq_queue_default(self):
        s = RabbitmqSettings(rabbitmq_url="amqp://localhost/")
        assert s.rabbitmq_queue == "openframe"

    def test_rabbitmq_durable_default(self):
        s = RabbitmqSettings(rabbitmq_url="amqp://localhost/")
        assert s.rabbitmq_durable is True

    def test_rabbitmq_prefetch_count_default(self):
        s = RabbitmqSettings(rabbitmq_url="amqp://localhost/")
        assert s.rabbitmq_prefetch_count == 10

    def test_rabbitmq_reconnect_interval_default(self):
        s = RabbitmqSettings(rabbitmq_url="amqp://localhost/")
        assert s.rabbitmq_reconnect_interval == 5.0

    def test_adapter_name_default(self):
        s = RabbitmqSettings(rabbitmq_url="amqp://localhost/")
        assert s.adapter_name == "rabbitmq"

    def test_connection_timeout_inherited_default(self):
        s = RabbitmqSettings(rabbitmq_url="amqp://localhost/")
        assert s.connection_timeout == 30.0

    def test_operation_timeout_inherited_default(self):
        s = RabbitmqSettings(rabbitmq_url="amqp://localhost/")
        assert s.operation_timeout == 10.0

    def test_reads_rabbitmq_prefetch_count_from_env(self, monkeypatch):
        monkeypatch.setenv("RABBITMQ_PREFETCH_COUNT", "50")
        s = RabbitmqSettings(rabbitmq_url="amqp://localhost/")
        assert s.rabbitmq_prefetch_count == 50

    def test_reads_rabbitmq_url_from_env(self, monkeypatch):
        monkeypatch.setenv("RABBITMQ_URL", "amqp://envhost/")
        s = RabbitmqSettings()
        assert s.rabbitmq_url == "amqp://envhost/"

    def test_is_subclass_of_base_adapter_settings(self):
        assert issubclass(RabbitmqSettings, BaseAdapterSettings)
