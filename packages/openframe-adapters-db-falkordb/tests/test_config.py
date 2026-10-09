"""
tests/test_config.py — openframe-adapters-db-falkordb
=========================================================
Unit tests for FalkorDBSettings.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from openframe.adapters.db.falkordb import FalkorDBSettings
from openframe.core.config import BaseAdapterSettings


class TestFalkorDBSettings:
    def test_instantiates_with_defaults(self):
        s = FalkorDBSettings()
        assert s.falkordb_host == "localhost"
        assert s.falkordb_port == 6379

    def test_falkordb_password_default_is_none(self):
        s = FalkorDBSettings()
        assert s.falkordb_password is None

    def test_falkordb_ssl_default_is_false(self):
        s = FalkorDBSettings()
        assert s.falkordb_ssl is False

    def test_falkordb_socket_timeout_default(self):
        s = FalkorDBSettings()
        assert s.falkordb_socket_timeout == 5.0

    def test_falkordb_socket_connect_timeout_default(self):
        s = FalkorDBSettings()
        assert s.falkordb_socket_connect_timeout == 5.0

    def test_falkordb_graph_name_default(self):
        s = FalkorDBSettings()
        assert s.falkordb_graph_name == "openframe"

    def test_falkordb_node_label_default(self):
        s = FalkorDBSettings()
        assert s.falkordb_node_label == "Entity"

    def test_adapter_name_default(self):
        s = FalkorDBSettings()
        assert s.adapter_name == "falkordb"

    def test_connection_timeout_inherited_default(self):
        s = FalkorDBSettings()
        assert s.connection_timeout == 30.0

    def test_reads_falkordb_host_from_env(self, monkeypatch):
        monkeypatch.setenv("FALKORDB_HOST", "graph.example.com")
        s = FalkorDBSettings()
        assert s.falkordb_host == "graph.example.com"

    def test_reads_falkordb_port_from_env(self, monkeypatch):
        monkeypatch.setenv("FALKORDB_PORT", "7000")
        s = FalkorDBSettings()
        assert s.falkordb_port == 7000

    def test_is_subclass_of_base_adapter_settings(self):
        assert issubclass(FalkorDBSettings, BaseAdapterSettings)

    # ── node label validation — the Cypher-injection guard ─────────────

    def test_accepts_safe_node_label(self):
        s = FalkorDBSettings(falkordb_node_label="Person")
        assert s.falkordb_node_label == "Person"

    def test_accepts_underscore_and_digits_after_first_char(self):
        s = FalkorDBSettings(falkordb_node_label="Person_2")
        assert s.falkordb_node_label == "Person_2"

    def test_rejects_label_starting_with_digit(self):
        with pytest.raises(ValidationError):
            FalkorDBSettings(falkordb_node_label="2Person")

    def test_rejects_label_with_space(self):
        with pytest.raises(ValidationError):
            FalkorDBSettings(falkordb_node_label="Person Node")

    def test_rejects_label_with_cypher_injection_payload(self):
        """
        Regression test: a label value containing a Cypher clause
        terminator/injection payload must be rejected at config-load
        time, since repository.py interpolates the label directly into
        every query string it builds.
        """
        with pytest.raises(ValidationError):
            FalkorDBSettings(falkordb_node_label="Entity) DETACH DELETE (n")

    def test_rejects_empty_label(self):
        with pytest.raises(ValidationError):
            FalkorDBSettings(falkordb_node_label="")

    def test_rejects_empty_graph_name(self):
        with pytest.raises(ValidationError):
            FalkorDBSettings(falkordb_graph_name="")
