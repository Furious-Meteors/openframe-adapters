"""
tests/test_config.py — openframe-adapters-queue-kafka
=======================================================
Unit tests for KafkaSettings.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from openframe.adapters.queue.kafka import KafkaSettings
from openframe.core.config import BaseAdapterSettings


class TestKafkaSettings:
    def test_instantiates_with_required_fields(self):
        s = KafkaSettings(kafka_bootstrap_servers="localhost:9092")
        assert s.kafka_bootstrap_servers == "localhost:9092"

    def test_missing_bootstrap_servers_raises_validation_error(self):
        with pytest.raises(ValidationError):
            KafkaSettings()

    def test_kafka_topic_default(self):
        s = KafkaSettings(kafka_bootstrap_servers="localhost:9092")
        assert s.kafka_topic == "openframe"

    def test_kafka_group_id_default(self):
        s = KafkaSettings(kafka_bootstrap_servers="localhost:9092")
        assert s.kafka_group_id == "openframe-group"

    def test_kafka_auto_offset_reset_default(self):
        s = KafkaSettings(kafka_bootstrap_servers="localhost:9092")
        assert s.kafka_auto_offset_reset == "earliest"

    def test_kafka_max_poll_records_default(self):
        s = KafkaSettings(kafka_bootstrap_servers="localhost:9092")
        assert s.kafka_max_poll_records == 10

    def test_kafka_session_timeout_ms_default(self):
        s = KafkaSettings(kafka_bootstrap_servers="localhost:9092")
        assert s.kafka_session_timeout_ms == 30_000

    def test_kafka_security_protocol_default(self):
        s = KafkaSettings(kafka_bootstrap_servers="localhost:9092")
        assert s.kafka_security_protocol == "PLAINTEXT"

    def test_kafka_sasl_mechanism_default(self):
        s = KafkaSettings(kafka_bootstrap_servers="localhost:9092")
        assert s.kafka_sasl_mechanism == ""

    def test_adapter_name_default(self):
        s = KafkaSettings(kafka_bootstrap_servers="localhost:9092")
        assert s.adapter_name == "kafka"

    def test_connection_timeout_inherited_default(self):
        s = KafkaSettings(kafka_bootstrap_servers="localhost:9092")
        assert s.connection_timeout == 30.0

    def test_reads_kafka_max_poll_records_from_env(self, monkeypatch):
        monkeypatch.setenv("KAFKA_MAX_POLL_RECORDS", "50")
        s = KafkaSettings(kafka_bootstrap_servers="localhost:9092")
        assert s.kafka_max_poll_records == 50

    def test_is_subclass_of_base_adapter_settings(self):
        assert issubclass(KafkaSettings, BaseAdapterSettings)
