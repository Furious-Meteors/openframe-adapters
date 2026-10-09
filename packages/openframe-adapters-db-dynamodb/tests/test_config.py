"""
tests/test_config.py
======================
Unit tests for DynamoDBSettings.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from openframe.adapters.db.dynamodb import DynamoDBSettings
from openframe.core.config import BaseAdapterSettings


class TestDynamoDBSettings:
    def test_instantiates_with_region_and_table(self) -> None:
        s = DynamoDBSettings(aws_region="us-east-1", dynamodb_table_name="items")
        assert s.aws_region == "us-east-1"
        assert s.dynamodb_table_name == "items"

    def test_missing_aws_region_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError):
            DynamoDBSettings(dynamodb_table_name="items")  # type: ignore[call-arg]

    def test_missing_table_name_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError):
            DynamoDBSettings(aws_region="us-east-1")  # type: ignore[call-arg]

    def test_missing_both_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError):
            DynamoDBSettings()  # type: ignore[call-arg]

    def test_endpoint_url_default_none(self) -> None:
        s = DynamoDBSettings(aws_region="us-east-1", dynamodb_table_name="items")
        assert s.endpoint_url is None

    def test_endpoint_url_can_be_set(self) -> None:
        s = DynamoDBSettings(
            aws_region="us-east-1",
            dynamodb_table_name="items",
            endpoint_url="http://localhost:8000",
        )
        assert s.endpoint_url == "http://localhost:8000"

    def test_credentials_default_none(self) -> None:
        s = DynamoDBSettings(aws_region="us-east-1", dynamodb_table_name="items")
        assert s.aws_access_key_id is None
        assert s.aws_secret_access_key is None
        assert s.aws_session_token is None

    def test_adapter_name_default(self) -> None:
        s = DynamoDBSettings(aws_region="us-east-1", dynamodb_table_name="items")
        assert s.adapter_name == "dynamodb"

    def test_connection_timeout_inherited_default(self) -> None:
        s = DynamoDBSettings(aws_region="us-east-1", dynamodb_table_name="items")
        assert s.connection_timeout == 30.0

    def test_operation_timeout_inherited_default(self) -> None:
        s = DynamoDBSettings(aws_region="us-east-1", dynamodb_table_name="items")
        assert s.operation_timeout == 10.0

    def test_max_retries_inherited_default(self) -> None:
        s = DynamoDBSettings(aws_region="us-east-1", dynamodb_table_name="items")
        assert s.max_retries == 3

    def test_is_subclass_of_base_adapter_settings(self) -> None:
        assert issubclass(DynamoDBSettings, BaseAdapterSettings)

    def test_aws_region_reads_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AWS_REGION", "eu-west-1")
        monkeypatch.setenv("DYNAMODB_TABLE_NAME", "items")
        s = DynamoDBSettings()
        assert s.aws_region == "eu-west-1"

    def test_endpoint_url_reads_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AWS_REGION", "us-east-1")
        monkeypatch.setenv("DYNAMODB_TABLE_NAME", "items")
        monkeypatch.setenv("ENDPOINT_URL", "http://localhost:8000")
        s = DynamoDBSettings()
        assert s.endpoint_url == "http://localhost:8000"
