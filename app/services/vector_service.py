import asyncio
import logging
from itertools import islice
from typing import List, Dict, Any, AsyncGenerator
from app.db.queries import Queries
from app.models.enum import DistanceMeasure
from app.db.client import ClickHouseClient


class ClickHouseQueryExecutor:
    """Executor of SQL queries to ClickHouse"""

    def __init__(self, client: ClickHouseClient, database: str):
        self._client = client
        self.database = database

    async def chunked_iterable(
        self, iterable: List, size: int
    ) -> AsyncGenerator[list[Any], None]:
        """Generates data chunks of the specified size."""
        it = iter(iterable)
        while chunk := list(islice(it, size)):
            yield chunk

    async def insert_data_batch(
        self, data_batch: List, table_name: str, id_column: str, vector_column: str
    ) -> None:
        """Inserts one batch of data into ClickHouse."""
        async with await self._client.get_cursor() as cursor:
            await cursor.execute(
                Queries.INSERT_DATA.format(
                    database=self.database,
                    table=table_name,
                    ids=id_column,
                    vectors=vector_column,
                ),
                data_batch,
            )

    async def insert_data_parallel(
        self,
        data: List,
        batch_size: int,
        table_name: str,
        id_column: str,
        vector_column: str,
    ) -> None:
        """
        Inserts data into ClickHouse in batches.

        :param data: List of data.
        :param batch_size: Size of one batch.
        :param table_name: Name of the table to insert data into.
        :param id_column: Name of the ID column.
        :param vector_column: Name of the vector column.
        """
        if not data:
            return

        tasks = [
            self.insert_data_batch(batch, table_name, id_column, vector_column)
            async for batch in self.chunked_iterable(data, batch_size)
        ]
        await asyncio.gather(*tasks)

    async def search_similar_vectors(
        self,
        input_vectors: List[List[float]],
        count: int,
        measure_type: DistanceMeasure,
        table_name: str,
        id_column: str,
        vector_column: str,
    ) -> Dict:
        """
        Searches for similar vectors.

        :param input_vectors: Input vectors.
        :param count: Number of similar objects (limit).
        :param measure_type: Distance calculation method ("l2" or "cosine").
        :param table_name: Name of the table to search in.
        :param: id_column: Name of the column id
        :param vector_column: Name of the column containing vectors.
        :return: Dictionary with results.
        """
        results_dict = {}

        async with await self._client.get_cursor() as cursor:
            for index, input_vector in enumerate(input_vectors, start=1):
                vector_str = "[" + ",".join(map(str, input_vector)) + "]"

                query_template = (
                    Queries.SEARCH_SIMILAR_L2Distance
                    if measure_type == DistanceMeasure.L2
                    else Queries.SEARCH_SIMILAR_cosineDistance
                )

                query = query_template.format(
                    database=self.database,
                    table=table_name,
                    id_column=id_column,
                    vector_column=vector_column,
                    vector=vector_str,
                    count=count,
                )

                await cursor.execute(query)
                result = await cursor.fetchall()

                results_dict[index] = {
                    "data": result,
                    "meta": {"returned": len(result), "limit": count},
                }

        return results_dict

    async def get_existing_ids(
        self, ids: List[str], table_name: str, id_column: str
    ) -> List[str]:
        """
        Checks which IDs exist in the table.

        :param ids: A list of IDs to delete.
        :param table_name: Name of the table to delete from.
        :param id_column: Name of the ID column.
        """
        ids_str = ", ".join(f"'{id}'" for id in ids)
        query = Queries.SELECT_UUID.format(
            database=self.database,
            table=table_name,
            id_column=id_column,
            ids_str=ids_str,
        )

        async with await self._client.get_cursor() as cursor:
            await cursor.execute(query)
            result = await cursor.fetchall()

        return [row[0] for row in result]

    async def delete_by_ids(
        self, ids: List[str], table_name: str, id_column: str
    ) -> None:
        """
        Deletes records from the table by their IDs.

        :param ids: A list of IDs to delete.
        :param table_name: Name of the table to delete from.
        :param id_column: Name of the ID column.
        """
        if not ids:
            return

        existing_ids = await self.get_existing_ids(ids, table_name, id_column)
        if not existing_ids:
            logging.warning("The transmitted IDs were not found in the database.")
            return

        ids_str = ", ".join(f"'{id}'" for id in ids)
        query = Queries.DELETE_UUID.format(
            database=self.database,
            table=table_name,
            id_column=id_column,
            ids_str=ids_str,
        )

        async with await self._client.get_cursor() as cursor:
            await cursor.execute(query)


class ContentStorage:
    """
    A class for managing content storage operations with ClickHouse.
    """

    def __init__(self, host, port, user, password, database):
        self._client = ClickHouseClient(host, port, user, password, database)
        self._query_executor = ClickHouseQueryExecutor(self._client, database)
        self.database = database

    async def connect(self):
        """Explicitly establish a connection to ClickHouse."""
        await self._client.connect()

    async def close(self):
        """Explicitly close the ClickHouse connection."""
        await self._client.close()

    async def insert_data(
        self, data: List[tuple], table_name: str, id_column: str, vector_column: str
    ):
        """Inserts data into ClickHouse in batches"""
        await self._query_executor.insert_data_parallel(
            data,
            batch_size=1000,
            table_name=table_name,
            id_column=id_column,
            vector_column=vector_column,
        )

    async def search_vectors(
        self,
        input_vectors: List[List[float]],
        count: int,
        measure_type: DistanceMeasure,
        id_column: str,
        table_name: str,
        vector_column: str,
    ):
        """Searches for similar vectors in ClickHouse."""
        return await self._query_executor.search_similar_vectors(
            input_vectors, count, measure_type, table_name, id_column, vector_column
        )

    async def delete_by_ids(self, ids: List[str], table_name: str, id_column: str):
        """Deletes records from ClickHouse by their IDs."""
        await self._query_executor.delete_by_ids(ids, table_name, id_column)

    async def get_cursor(self):
        """Get a cursor for executing SQL queries"""
        return await self._client.get_cursor()
