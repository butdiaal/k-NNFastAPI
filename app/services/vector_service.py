import asyncio
from typing import List, Dict, Generator
from clickhouse_driver import Client
from clickhouse_driver.dbapi import connect as async_connect
from itertools import islice
from asynch import connect
from app.db.queries import Queries
from app.config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DATABASE,
)


class ClickHouseConnector:
    """
    A class for managing ClickHouse connection.
    """

    def __init__(
        self,
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    ):
        """
        Initialize ClickHouse client.

        :param host: ClickHouse host
        :param port: ClickHouse port
        :param user: User name
        :param password: Password
        :param database: Database name
        """
        self.client = Client(
            host=host, port=port, user=user, password=password, database=database
        )
        self.async_client = None
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    async def connect_async(self):
        """Асинхронное подключение к ClickHouse"""
        self.async_client = await connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )

    def get_client(self) -> Client:
        return self.client

    async def chunked_iterable(
        self, iterable: List, size: int
    ) -> Generator[List, None, None]:
        """Generates data chunks of the specified size."""
        it = iter(iterable)
        while chunk := list(islice(it, size)):
            yield chunk

    async def insert_data_batch(
        self,
        database: str,
        table: str,
        id_column: str,
        vector_column: str,
        data_batch: List,
    ) -> None:
        """Inserts one batch of data into ClickHouse."""
        query = Queries.INSERT_DATA.format(
            database=database, table=table, ids=id_column, vectors=vector_column
        )
        async with self.async_client.cursor() as cursor:
            await cursor.execute(query, data_batch)

    async def insert_data_parallel(
        self,
        database: str,
        table: str,
        id_column: str,
        vector_column: str,
        data: List,
        batch_size: int = 1000,
        max_workers: int = 4,
    ) -> None:
        """
        Inserts data into ClickHouse in batches with multi-threaded processing.

        :param database: Database name.
        :param table: Table name.
        :param id_column: ID column.
        :param vector_column: Vector column.
        :param data: List of data.
        :param batch_size: Size of one batch.
        :param max_workers: Number of threads.
        """
        if not data:
            return

        if self.async_client is None:
            await self.connect_async()

        batches = [batch async for batch in self.chunked_iterable(data, batch_size)]
        tasks = [
            self.insert_data_batch(database, table, id_column, vector_column, batch)
            for batch in batches
        ]
        await asyncio.gather(*tasks)

    async def search_similar_vectors(
        self,
        input_vectors: List[List[float]],
        database: str,
        table: str,
        id_column: str,
        vector_column: str,
        count: int = 10,
        offset: int = 0,
        measure_type: str = "l2",
    ) -> Dict:
        """
        Searches for similar vectors.

        :param input_vectors: Input vectors.
        :param database: Database name.
        :param table: Table name.
        :param id_column: ID column.
        :param vector_column: Column with vectors.
        :param count: Number of similar objects (limit).
        :param offset: Offset.
        :param measure_type: Distance calculation method ("l2" or "cosine").
        :return: Dictionary with results.
        """
        results_dict = {}

        if self.async_client is None:
            await self.connect_async()

        async with self.async_client.cursor() as cursor:
            for index, input_vector in enumerate(input_vectors, start=1):
                vector_str = "[" + ",".join(map(str, input_vector)) + "]"

                if measure_type == "l2":
                    query = Queries.SEARCH_SIMILAR_L2Distance.format(
                        vector=vector_str,
                        database=database,
                        table=table,
                        id_column=id_column,
                        vector_column=vector_column,
                        count=count,
                        offset=offset,
                    )
                elif measure_type == "cosine":
                    query = Queries.SEARCH_SIMILAR_cosineDistance.format(
                        vector=vector_str,
                        database=database,
                        table=table,
                        id_column=id_column,
                        vector_column=vector_column,
                        count=count,
                        offset=offset,
                    )
                else:
                    raise ValueError(f"Unsupported measure type: {measure_type}")

                await cursor.execute(query)
                result = await cursor.fetchall()

                results_dict[index] = {
                    "data": result,
                    "meta": {"returned": len(result), "offset": offset, "limit": count},
                }

        return results_dict

    async def delete_by_ids(
        self, database: str, table: str, id_column: str, ids: List[str]
    ) -> None:
        """
        Deletes records from the table by their IDs.

        :param database: Database name.
        :param table: The name of the table.
        :param id_column: The column name for document IDs.
        :param ids: A list of IDs to delete.
        """
        if not ids:
            return

        if self.async_client is None:
            await self.connect_async()

        ids_str = ", ".join(f"'{id}'" for id in ids)
        query = Queries.DELETE_UUID.format(
            database=database,
            table=table,
            id_column=id_column,
            ids_str=ids_str,
        )
        async with self.async_client.cursor() as cursor:
            await cursor.execute(query)


class ContentStorage:
    """
    A class for managing content storage operations with ClickHouse.
    """

    def __init__(
        self,
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    ):
        """
        Initialize ContentStorage with ClickHouse client.

        :param host: ClickHouse host
        :param port: ClickHouse port
        :param user: ClickHouse user
        :param password: ClickHouse password
        :param database: ClickHouse database
        """
        self.connector = ClickHouseConnector(host, port, user, password, database)
        self.client = None
        self.database = database

    def connect(self):
        """
        Connect to the ClickHouse database.
        """
        if not isinstance(self.client, Client):
            self.client = self.connector.get_client()
        return self.client

    def close(self):
        """
        Close the ClickHouse connection.
        If the client is not None, disconnection process is executed.
        """
        if self.client:
            self.client.disconnect()
            self.client = None

    async def insert_data(
        self,
        table: str,
        id_column: str,
        vector_column: str,
        data: List[tuple],
        batch_size: int = 1000,
        max_workers: int = 4,
    ):
        await self.connector.insert_data_parallel(
            database=self.database,
            table=table,
            id_column=id_column,
            vector_column=vector_column,
            data=data,
            batch_size=batch_size,
            max_workers=max_workers,
        )

    async def search_vectors(
        self,
        input_vectors: List[List[float]],
        table: str,
        id_column: str,
        vector_column: str,
        count: int = 10,
        offset: int = 0,
        measure_type: str = "l2",
    ) -> Dict:
        return await self.connector.search_similar_vectors(
            input_vectors=input_vectors,
            database=self.database,
            table=table,
            id_column=id_column,
            vector_column=vector_column,
            count=count,
            offset=offset,
            measure_type=measure_type,
        )

    async def delete_by_ids(self, table: str, id_column: str, ids: List[str]):
        await self.connector.delete_by_ids(
            database=self.database, table=table, id_column=id_column, ids=ids
        )
