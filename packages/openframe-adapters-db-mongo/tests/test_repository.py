"""
tests/test_repository.py — openframe-adapters-db-mongo
========================================================
Contract tests (RepositoryContractTests) run first, then adapter-specific
unit tests covering MongoDB error mapping and driver behaviour.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock

import pymongo.errors
import pytest
from bson import ObjectId

from openframe.adapters.db.mongo import MongoRepository, MongoSettings
from openframe.core.exceptions import AdapterConfigurationError, AdapterConnectionError, AdapterQueryError, AdapterTimeoutError
from openframe.core.ports import BaseRepository
from openframe.core.testing import RepositoryContractTests


# ── Contract tests — must pass for every BaseRepository implementation ─────

class TestMongoRepositoryContracts(RepositoryContractTests):
    """
    MongoRepository passes the full openframe contract suite.

    All 18 RepositoryContractTests run against a stateful in-memory mock.
    The mock simulates Motor's Collection API so create→get, list pagination,
    update, and delete all behave like a real MongoDB collection.
    No real MongoDB required.
    """

    @pytest.fixture
    def repository(self, mock_settings):
        import openframe.adapters.db.mongo.connection as conn_module

        # In-memory document store: str(_id) → raw doc dict (without "id" mirror key)
        store: dict = {}

        # ── Stateful Motor collection mock ────────────────────────────────
        mock_col = MagicMock()

        async def _insert_one(doc):
            d = dict(doc)
            id_str = str(d.get("_id", ""))
            # Store raw doc without "id" mirror — _serialise_doc adds it on read
            raw = {"_id": id_str}
            raw.update({k: v for k, v in d.items() if k not in ("_id", "id")})
            store[id_str] = raw
            r = MagicMock()
            r.inserted_id = id_str
            return r

        async def _find_one(query, *args, **kwargs):
            raw_id = query.get("_id")
            id_str = str(raw_id) if raw_id is not None else None
            if id_str is None:
                return None
            return dict(store[id_str]) if id_str in store else None

        def _make_cursor():
            """Return a fresh cursor with isolated skip/limit state."""
            state = {"skip": 0, "limit": 0}
            cur = MagicMock()

            def _skip(n):
                state["skip"] = n
                return cur

            def _limit(n):
                state["limit"] = n
                return cur

            async def _to_list(*args, **kwargs):
                docs = list(store.values())
                s = state["skip"]
                ln = state["limit"]
                sliced = docs[s: s + ln] if ln else docs[s:]
                return [dict(d) for d in sliced]

            cur.skip = MagicMock(side_effect=_skip)
            cur.limit = MagicMock(side_effect=_limit)
            cur.to_list = AsyncMock(side_effect=_to_list)
            return cur

        async def _count_documents(*args, **kwargs):
            return len(store)

        async def _find_one_and_update(query, update, *args, **kwargs):
            raw_id = query.get("_id")
            id_str = str(raw_id) if raw_id is not None else None
            if id_str is None or id_str not in store:
                return None
            doc = dict(store[id_str])
            doc.update(update.get("$set", {}))
            store[id_str] = doc
            return dict(doc)

        async def _delete_one(query, *args, **kwargs):
            raw_id = query.get("_id")
            id_str = str(raw_id) if raw_id is not None else ""
            r = MagicMock()
            if id_str in store:
                del store[id_str]
                r.deleted_count = 1
            else:
                r.deleted_count = 0
            return r

        mock_col.insert_one = AsyncMock(side_effect=_insert_one)
        mock_col.find_one = AsyncMock(side_effect=_find_one)
        mock_col.find = MagicMock(side_effect=lambda *a, **kw: _make_cursor())
        mock_col.count_documents = AsyncMock(side_effect=_count_documents)
        mock_col.find_one_and_update = AsyncMock(side_effect=_find_one_and_update)
        mock_col.delete_one = AsyncMock(side_effect=_delete_one)
        mock_col.list_collection_names = AsyncMock(return_value=["items"])

        # ── Wire mock client → db → collection ───────────────────────────
        db = MagicMock()
        db.__getitem__ = MagicMock(return_value=mock_col)
        db.list_collection_names = AsyncMock(return_value=["items"])

        client = MagicMock()
        client.__getitem__ = MagicMock(return_value=db)
        client.admin = MagicMock()
        client.admin.command = AsyncMock(return_value={"ok": 1})
        client.close = MagicMock()

        conn_module._client_cache[mock_settings.mongo_url] = client
        r = MongoRepository(mock_settings, collection="items")
        yield r
        conn_module._client_cache.clear()
        store.clear()

    @pytest.fixture
    def port(self, repository):
        return repository

    @pytest.fixture
    def make_entity(self):
        # Return the Mongo-native entity format (with both _id and id) so that
        # assert result == entity passes after _serialise_doc adds the id mirror.
        def _make(id: str, name: str = "test") -> dict:
            return {"_id": id, "id": id, "name": name}
        return _make


# ── Adapter-specific tests — beyond what the contract covers ───────────────


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

VALID_OID = "507f1f77bcf86cd799439011"


def make_doc(data: dict) -> dict:
    """Return a plain dict simulating a motor document."""
    return dict(data)


class TestProtocolConformance:
    def test_isinstance_base_repository(self, repo: MongoRepository) -> None:
        assert isinstance(repo, BaseRepository)


class TestInit:
    def test_missing_collection_raises_configuration_error(
        self, mock_settings: MongoSettings
    ) -> None:
        with pytest.raises(AdapterConfigurationError):
            MongoRepository(mock_settings)

    def test_collection_from_init_arg(self, mock_settings: MongoSettings) -> None:
        repo = MongoRepository(mock_settings, collection="papers")
        assert repo._coll_name == "papers"

    def test_collection_from_class_attribute(self, mock_settings: MongoSettings) -> None:
        class PaperRepo(MongoRepository):
            _collection = "papers"

        repo = PaperRepo(mock_settings)
        assert repo._coll_name == "papers"


class TestNormaliseId:
    def test_valid_objectid_string_returns_objectid(self, repo: MongoRepository) -> None:
        result = repo._normalise_id(VALID_OID)
        assert isinstance(result, ObjectId)
        assert str(result) == VALID_OID

    def test_invalid_objectid_string_returns_plain_string(self, repo: MongoRepository) -> None:
        result = repo._normalise_id("custom-string-id")
        assert result == "custom-string-id"
        assert isinstance(result, str)


class TestSerialiseDoc:
    def test_converts_objectid_to_string(self, repo: MongoRepository) -> None:
        oid = ObjectId(VALID_OID)
        doc = {"_id": oid, "name": "test"}
        result = repo._serialise_doc(doc)
        assert result["_id"] == VALID_OID
        assert isinstance(result["_id"], str)

    def test_adds_id_key_mirroring_id(self, repo: MongoRepository) -> None:
        oid = ObjectId(VALID_OID)
        doc = {"_id": oid, "name": "test"}
        result = repo._serialise_doc(doc)
        assert result["id"] == VALID_OID

    def test_does_not_overwrite_existing_id_key(self, repo: MongoRepository) -> None:
        doc = {"_id": ObjectId(VALID_OID), "id": "custom", "name": "x"}
        result = repo._serialise_doc(doc)
        assert result["id"] == "custom"

    def test_string_id_still_stringified(self, repo: MongoRepository) -> None:
        doc = {"_id": "plain-string"}
        result = repo._serialise_doc(doc)
        assert result["_id"] == "plain-string"
        assert result["id"] == "plain-string"


class TestGet:
    async def test_get_found_returns_dict(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        doc = {"_id": ObjectId(VALID_OID), "name": "paper"}
        mock_collection.find_one.return_value = doc
        result = await repo.get(VALID_OID)
        mock_collection.find_one.assert_called_once()
        assert result is not None
        assert result["_id"] == VALID_OID
        assert isinstance(result["_id"], str)

    async def test_get_uses_objectid_filter_for_valid_hex(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        mock_collection.find_one.return_value = None
        await repo.get(VALID_OID)
        call_args = mock_collection.find_one.call_args[0][0]
        assert isinstance(call_args["_id"], ObjectId)

    async def test_get_uses_string_filter_for_non_objectid(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        mock_collection.find_one.return_value = None
        await repo.get("custom-id")
        call_args = mock_collection.find_one.call_args[0][0]
        assert call_args["_id"] == "custom-id"

    async def test_get_not_found_returns_none(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        mock_collection.find_one.return_value = None
        result = await repo.get(VALID_OID)
        assert result is None

    async def test_get_id_field_always_string(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        mock_collection.find_one.return_value = {"_id": ObjectId(VALID_OID)}
        result = await repo.get(VALID_OID)
        assert isinstance(result["_id"], str)  # type: ignore[index]

    async def test_get_connection_failure_raises_adapter_connection_error(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        mock_collection.find_one.side_effect = pymongo.errors.ConnectionFailure("down")
        with pytest.raises(AdapterConnectionError) as exc_info:
            await repo.get(VALID_OID)
        assert exc_info.value.operation == "get"

    async def test_get_operation_failure_raises_adapter_query_error(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        mock_collection.find_one.side_effect = pymongo.errors.OperationFailure("denied")
        with pytest.raises(AdapterQueryError):
            await repo.get(VALID_OID)

    async def test_get_timeout_raises_adapter_timeout_error(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        mock_collection.find_one.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError) as exc_info:
            await repo.get(VALID_OID)
        assert exc_info.value.operation == "get"


class TestList:
    async def test_list_returns_docs_and_count(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        docs = [{"_id": ObjectId(VALID_OID), "name": "a"}]
        cursor = mock_collection.find.return_value
        cursor.to_list.return_value = docs
        mock_collection.count_documents.return_value = 42

        entities, count = await repo.list(limit=10, offset=0)
        assert count == 42
        assert len(entities) == 1
        assert entities[0]["_id"] == VALID_OID

    async def test_list_calls_skip_and_limit(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        cursor = mock_collection.find.return_value
        cursor.to_list.return_value = []
        mock_collection.count_documents.return_value = 0

        await repo.list(limit=5, offset=20)
        cursor.skip.assert_called_once_with(20)
        cursor.limit.assert_called_once_with(5)

    async def test_list_operation_failure_raises_adapter_query_error(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        cursor = mock_collection.find.return_value
        cursor.to_list.side_effect = pymongo.errors.OperationFailure("err")
        with pytest.raises(AdapterQueryError):
            await repo.list(limit=10, offset=0)

    async def test_list_timeout_raises_adapter_timeout_error(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        cursor = mock_collection.find.return_value
        cursor.to_list.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.list(limit=10, offset=0)


class TestCreate:
    async def test_create_returns_stored_doc(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        oid = ObjectId(VALID_OID)
        insert_result = MagicMock()
        insert_result.inserted_id = oid
        mock_collection.insert_one.return_value = insert_result
        mock_collection.find_one.return_value = {"_id": oid, "name": "paper"}

        result = await repo.create({"name": "paper"})
        assert result["_id"] == VALID_OID

    async def test_create_maps_id_to_mongo_id(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        oid = ObjectId(VALID_OID)
        insert_result = MagicMock()
        insert_result.inserted_id = oid
        mock_collection.insert_one.return_value = insert_result
        mock_collection.find_one.return_value = {"_id": oid, "name": "x"}

        await repo.create({"id": VALID_OID, "name": "x"})
        inserted_doc = mock_collection.insert_one.call_args[0][0]
        assert "_id" in inserted_doc
        assert "id" not in inserted_doc

    async def test_create_with_existing_mongo_id_uses_it_directly(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        oid = ObjectId(VALID_OID)
        insert_result = MagicMock()
        insert_result.inserted_id = oid
        mock_collection.insert_one.return_value = insert_result
        mock_collection.find_one.return_value = {"_id": oid}

        await repo.create({"_id": VALID_OID, "name": "x"})
        inserted_doc = mock_collection.insert_one.call_args[0][0]
        assert "_id" in inserted_doc

    async def test_create_duplicate_key_raises_adapter_query_error(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        mock_collection.insert_one.side_effect = pymongo.errors.DuplicateKeyError("dup")
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.create({"name": "x"})
        assert exc_info.value.operation == "create"

    async def test_create_write_error_raises_adapter_query_error(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        mock_collection.insert_one.side_effect = pymongo.errors.WriteError("write fail")
        with pytest.raises(AdapterQueryError):
            await repo.create({"name": "x"})

    async def test_create_timeout_raises_adapter_timeout_error(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        mock_collection.insert_one.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.create({"name": "x"})


class TestUpdate:
    async def test_update_found_returns_updated_doc(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        oid = ObjectId(VALID_OID)
        mock_collection.find_one_and_update.return_value = {"_id": oid, "name": "updated"}
        result = await repo.update({"_id": VALID_OID, "name": "updated"})
        assert result is not None
        assert result["_id"] == VALID_OID
        assert result["name"] == "updated"

    async def test_update_not_found_returns_none(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        mock_collection.find_one_and_update.return_value = None
        result = await repo.update({"_id": VALID_OID, "name": "ghost"})
        assert result is None

    async def test_update_entity_with_id_key_works(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        oid = ObjectId(VALID_OID)
        mock_collection.find_one_and_update.return_value = {"_id": oid, "name": "x"}
        result = await repo.update({"id": VALID_OID, "name": "x"})
        assert result is not None

    async def test_update_missing_id_raises_adapter_query_error(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        with pytest.raises(AdapterQueryError) as exc_info:
            await repo.update({"name": "no id here"})
        assert exc_info.value.operation == "update"

    async def test_update_operation_failure_raises_adapter_query_error(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        mock_collection.find_one_and_update.side_effect = pymongo.errors.OperationFailure("fail")
        with pytest.raises(AdapterQueryError):
            await repo.update({"_id": VALID_OID, "name": "x"})

    async def test_update_timeout_raises_adapter_timeout_error(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        mock_collection.find_one_and_update.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.update({"_id": VALID_OID, "name": "x"})


class TestDelete:
    async def test_delete_existing_returns_true(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        result_mock = MagicMock()
        result_mock.deleted_count = 1
        mock_collection.delete_one.return_value = result_mock
        assert await repo.delete(VALID_OID) is True

    async def test_delete_missing_returns_false(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        result_mock = MagicMock()
        result_mock.deleted_count = 0
        mock_collection.delete_one.return_value = result_mock
        assert await repo.delete(VALID_OID) is False

    async def test_delete_connection_failure_raises_adapter_connection_error(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        mock_collection.delete_one.side_effect = pymongo.errors.ConnectionFailure("gone")
        with pytest.raises(AdapterConnectionError):
            await repo.delete(VALID_OID)

    async def test_delete_timeout_raises_adapter_timeout_error(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        mock_collection.delete_one.side_effect = asyncio.TimeoutError()
        with pytest.raises(AdapterTimeoutError):
            await repo.delete(VALID_OID)

    async def test_delete_write_error_raises_adapter_query_error(
        self, repo: MongoRepository, mock_collection: MagicMock
    ) -> None:
        mock_collection.delete_one.side_effect = pymongo.errors.WriteError("write fail")
        with pytest.raises(AdapterQueryError):
            await repo.delete(VALID_OID)


class TestClose:
    async def test_close_removes_client_from_cache(
        self, repo: MongoRepository, mock_client: MagicMock
    ) -> None:
        import openframe.adapters.db.mongo.connection as conn_module

        conn_module._client_cache[repo._settings.mongo_url] = mock_client
        await repo.close()
        assert repo._settings.mongo_url not in conn_module._client_cache

    async def test_close_calls_client_close(
        self, repo: MongoRepository, mock_client: MagicMock
    ) -> None:
        import openframe.adapters.db.mongo.connection as conn_module

        conn_module._client_cache[repo._settings.mongo_url] = mock_client
        await repo.close()
        mock_client.close.assert_called_once()


class TestSubclassOverride:
    async def test_subclass_doc_to_entity_returns_typed_object(
        self, mock_settings: MongoSettings, mock_client: MagicMock, mock_collection: MagicMock
    ) -> None:
        import openframe.adapters.db.mongo.connection as conn_module

        @dataclass
        class Paper:
            name: str
            id: str = ""

        class PaperRepo(MongoRepository[Paper]):
            _collection = "artifacts"

            def _doc_to_entity(self, doc: dict) -> Paper:
                return Paper(name=doc["name"], id=doc.get("_id", ""))

            def _entity_to_doc(self, entity: Paper) -> dict:
                return {"name": entity.name}

        conn_module._client_cache[mock_settings.mongo_url] = mock_client
        repo = PaperRepo(mock_settings)

        oid = ObjectId(VALID_OID)
        mock_collection.find_one.return_value = {"_id": oid, "name": "Attention Is All You Need"}
        result = await repo.get(VALID_OID)
        assert isinstance(result, Paper)
        assert result.name == "Attention Is All You Need"
        conn_module._client_cache.clear()
