import unittest
from unittest.mock import AsyncMock
import uuid
import random
from typing import List
from app.models.enum import DistanceMeasure
from app.db.client import ClickHouseClient
from app.db.queries import Queries
from app.services.vector_service import ClickHouseQueryExecutor, ContentStorage


def generate_uuid() -> str:
    """UUID generation."""
    return str(uuid.uuid4())


def generate_vector() -> List[float]:
    """Generating a vector of 512 floating point numbers."""
    return [random.uniform(-1.0, 1.0) for _ in range(512)]


class TestClickHouseQueryExecutor(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        """Initializing the test environment."""
        self.client = AsyncMock(spec=ClickHouseClient)
        self.database = "test_db"
        self.executor = ClickHouseQueryExecutor(self.client, self.database)

    async def test_chunked_iterable(self) -> None:
        """Testing the partitioning of an iterated object into chunks."""
        data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        chunk_size = 3
        chunks = []
        async for chunk in self.executor.chunked_iterable(data, chunk_size):
            chunks.append(chunk)
        self.assertEqual(chunks, [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10]])

    async def test_insert_data_batch(self) -> None:
        """Testing data insertion in batches."""
        id1, id2 = generate_uuid(), generate_uuid()
        vector1, vector2 = generate_vector(), generate_vector()
        data_batch = [(id1, vector1), (id2, vector2)]
        table_name = "test_table"
        id_column = "id"
        vector_column = "vector"

        await self.executor.insert_data_batch(
            data_batch, table_name, id_column, vector_column
        )

        self.client.get_cursor.assert_called_once()
        cursor = self.client.get_cursor.return_value.__aenter__.return_value
        cursor.execute.assert_called_once_with(
            Queries.INSERT_DATA.format(
                database=self.database,
                table=table_name,
                ids=id_column,
                vectors=vector_column,
            ),
            data_batch,
        )

    async def test_insert_data_parallel(self) -> None:
        """Testing parallel data insertion."""
        id1, id2, id3 = generate_uuid(), generate_uuid(), generate_uuid()
        vector1, vector2, vector3 = (
            generate_vector(),
            generate_vector(),
            generate_vector(),
        )
        data = [(id1, vector1), (id2, vector2), (id3, vector3)]
        batch_size = 2
        table_name = "test_table"
        id_column = "id"
        vector_column = "vector"

        await self.executor.insert_data_parallel(
            data, batch_size, table_name, id_column, vector_column
        )

        self.assertEqual(self.client.get_cursor.call_count, 2)

    async def test_search_similar_vectors(self) -> None:
        """Testing the search for similar vectors."""
        input_vectors = [generate_vector(), generate_vector()]
        count = 5
        measure_type = DistanceMeasure.L2
        table_name = "test_table"
        id_column = "id"
        vector_column = "vector"

        cursor = self.client.get_cursor.return_value.__aenter__.return_value
        cursor.fetchall.return_value = [
            (generate_uuid(), generate_vector()) for _ in range(2)
        ]

        result = await self.executor.search_similar_vectors(
            input_vectors, count, measure_type, table_name, id_column, vector_column
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(len(result[1]["data"]), 2)
        self.assertEqual(result[1]["meta"]["returned"], 2)
        self.assertEqual(result[1]["meta"]["limit"], count)

    async def test_get_existing_ids(self) -> None:
        """Testing for obtaining existing IDs."""
        id1, id2, id3 = generate_uuid(), generate_uuid(), generate_uuid()
        ids = [id1, id2, id3]
        table_name = "test_table"
        id_column = "id"

        cursor = self.client.get_cursor.return_value.__aenter__.return_value
        cursor.fetchall.return_value = [(id1,), (id2,)]

        existing_ids = await self.executor.get_existing_ids(ids, table_name, id_column)

        self.assertEqual(existing_ids, [id1, id2])

    async def test_delete_by_ids(self) -> None:
        """Testing deletion by ID."""
        id1, id2, id3 = generate_uuid(), generate_uuid(), generate_uuid()
        ids = [id1, id2, id3]
        table_name = "test_table"
        id_column = "id"

        cursor = self.client.get_cursor.return_value.__aenter__.return_value
        cursor.fetchall.return_value = [(id1,), (id2,)]

        await self.executor.delete_by_ids(ids, table_name, id_column)

        self.assertEqual(self.client.get_cursor.call_count, 2)

        cursor.execute.assert_called_with(
            Queries.DELETE_UUID.format(
                database=self.database,
                table=table_name,
                id_column=id_column,
                ids_str=f"'{id1}', '{id2}', '{id3}'",
            )
        )


class TestContentStorage(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        """Initializing the test environment."""
        self.host = "localhost"
        self.port = 9000
        self.user = "user"
        self.password = "password"
        self.database = "test_db"
        self.storage = ContentStorage(
            self.host, self.port, self.user, self.password, self.database
        )
        self.storage._client = AsyncMock(spec=ClickHouseClient)
        self.storage._query_executor = AsyncMock(spec=ClickHouseQueryExecutor)

    async def test_insert_data(self) -> None:
        """Testing data insertion."""
        id1, id2 = generate_uuid(), generate_uuid()
        vector1, vector2 = generate_vector(), generate_vector()
        data = [(id1, vector1), (id2, vector2)]
        table_name = "test_table"
        id_column = "id"
        vector_column = "vector"

        await self.storage.insert_data(data, table_name, id_column, vector_column)

        self.storage._query_executor.insert_data_parallel.assert_called_once_with(
            data,
            batch_size=1000,
            table_name=table_name,
            id_column=id_column,
            vector_column=vector_column,
        )

    async def test_search_vectors(self) -> None:
        """Testing vector search."""
        input_vectors = [generate_vector(), generate_vector()]
        count = 5
        measure_type = DistanceMeasure.L2
        table_name = "test_table"
        id_column = "id"
        vector_column = "vector"

        await self.storage.search_vectors(
            input_vectors, count, measure_type, id_column, table_name, vector_column
        )

        self.storage._query_executor.search_similar_vectors.assert_called_once_with(
            input_vectors, count, measure_type, table_name, id_column, vector_column
        )

    async def test_delete_by_ids(self) -> None:
        """Testing deletion by ID."""
        id1, id2, id3 = generate_uuid(), generate_uuid(), generate_uuid()
        ids = [id1, id2, id3]
        table_name = "test_table"
        id_column = "id"

        await self.storage.delete_by_ids(ids, table_name, id_column)

        self.storage._query_executor.delete_by_ids.assert_called_once_with(
            ids, table_name, id_column
        )

    async def test_get_cursor(self) -> None:
        """Testing cursor acquisition."""
        await self.storage.get_cursor()
        self.storage._client.get_cursor.assert_called_once()


if __name__ == "__main__":
    unittest.main()
