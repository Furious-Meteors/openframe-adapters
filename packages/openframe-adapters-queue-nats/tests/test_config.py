"""
tests/test_config.py — openframe-adapters-queue-nats
=======================================================
Unit tests for NatsSettings.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from openframe.adapters.queue.nats import NatsSettings
from openframe.core.config import BaseAdapterSettings


class TestNatsSettings:
    def test_instantiates_with_required_fields(self):
        s = NatsSettings(nats_servers="nats://localhost:4222")
        assert s.nats_servers == "nats://localhost:4222"

    def test_missing_servers_raises_validation_error(self):
        with pytest.raises(ValidationError):
            NatsSettings()

    def test_nats_subject_default(self):
        s = NatsSettings(nats_servers="nats://localhost:4222")
        assert s.nats_subject == "openframe"

    def test_nats_stream_default(self):
        s = NatsSettings(nats_servers="nats://localhost:4222")
        assert s.nats_stream == "OPENFRAME"

    def test_nats_durable_name_default(self):
        s = NatsSettings(nats_servers="nats://localhost:4222")
        assert s.nats_durable_name == "openframe-consumer"

    def test_nats_deliver_policy_default(self):
        s = NatsSettings(nats_servers="nats://localhost:4222")
        assert s.nats_deliver_policy == "all"

    def test_nats_ack_wait_seconds_default(self):
        s = NatsSettings(nats_servers="nats://localhost:4222")
        assert s.nats_ack_wait_seconds == 30.0

    def test_nats_max_deliver_default(self):
        s = NatsSettings(nats_servers="nats://localhost:4222")
        assert s.nats_max_deliver == 5

    def test_nats_max_reconnect_attempts_default(self):
        s = NatsSettings(nats_servers="nats://localhost:4222")
        assert s.nats_max_reconnect_attempts == 60

    def test_nats_user_default(self):
        s = NatsSettings(nats_servers="nats://localhost:4222")
        assert s.nats_user == ""

    def test_adapter_name_default(self):
        s = NatsSettings(nats_servers="nats://localhost:4222")
        assert s.adapter_name == "nats"

    def test_connection_timeout_inherited_default(self):
        s = NatsSettings(nats_servers="nats://localhost:4222")
        assert s.connection_timeout == 30.0

    def test_reads_nats_max_deliver_from_env(self, monkeypatch):
        monkeypatch.setenv("NATS_MAX_DELIVER", "9")
        s = NatsSettings(nats_servers="nats://localhost:4222")
        assert s.nats_max_deliver == 9

    def test_is_subclass_of_base_adapter_settings(self):
        assert issubclass(NatsSettings, BaseAdapterSettings)

    def test_server_list_splits_comma_separated_servers(self):
        s = NatsSettings(nats_servers="nats://a:4222, nats://b:4222,nats://c:4222")
        assert s.server_list == ["nats://a:4222", "nats://b:4222", "nats://c:4222"]

    def test_server_list_single_server(self):
        s = NatsSettings(nats_servers="nats://localhost:4222")
        assert s.server_list == ["nats://localhost:4222"]
