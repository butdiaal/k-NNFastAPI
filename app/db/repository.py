import logging
from clickhouse_driver import errors
from app.services.vector_service import ContentStorage
from app.db.queries import Queries


class ClickHouseRepository:
    def __init__(self, connection: ContentStorage):
        """
        Repository for managing database operations in ClickHouse.

        """
        self.connection = connection
        self.database = connection.database

    async def check_db_exists(self) -> bool:
        """Check if the database exists."""
        await self.connection.connect()

        try:
            async with await self.connection.get_cursor() as cursor:
                await cursor.execute(Queries.SHOW_DATABASES)
                databases = {db[0] for db in await cursor.fetchall()}
                return self.database in databases
        except errors.Error as e:
            logging.error(f"Error checking database existence: {e}")
            return False

    async def check_table_exists(self, table_name: str) -> bool:
        """Check if a specific table exists in the database."""
        await self.connection.connect()

        try:
            async with await self.connection.get_cursor()  as cursor:
                await cursor.execute(Queries.SHOW_TABLES.format(database=self.database))
                tables = {table[0] for table in await cursor.fetchall()}
                return table_name in tables
        except errors.Error as e:
            logging.error(f"Error checking table existence: {e}")
            return False

    async def create_database(self) -> None:
        """Create the database if it does not exist."""
        await self.connection.connect()

        try:
            async with await self.connection.get_cursor()  as cursor:
                await cursor.execute(Queries.CREATE_DATABASE.format(database=self.database))
                logging.info(f"Database '{self.database}' created successfully.")
        except errors.Error as e:
            logging.error(f"Error creating database: {e}")
            raise

    async def create_table(self, table_name: str, id_column: str, vector_column: str) -> None:
        """
        Create a table if it does not exist.

        :param table_name: Table name.
        :param id_column: Column name for unique IDs.
        :param vector_column: Column name for vector data.
        """
        await self.connection.connect()
        if self.connection.connect is None:
            logging.error("ClickHouse connection is not initialized!")
            raise RuntimeError("ClickHouse connection is not established.")

        try:
            async with await self.connection.get_cursor() as cursor:
                await cursor.execute(Queries.SET_EXPERIMENTAL)

                await cursor.execute(
                    Queries.CREATE_TABLE.format(
                        database=self.database, table=table_name, ids=id_column, vectors=vector_column
                    )
                )

                await cursor.execute(
                    Queries.ADD_INDEX_L2.format(
                        database=self.database, table=table_name, ids=id_column, vectors=vector_column
                    )
                )

                await cursor.execute(
                    Queries.ADD_INDEX_cosine.format(
                        database=self.database, table=table_name, ids=id_column, vectors=vector_column
                    )
                )

                logging.info(f"Table '{table_name}' created successfully in database '{self.database}'.")
        except errors.Error as e:
            logging.error(f"Error creating table: {e}")
            raise

    async def ensure_db_and_table(self, table_name: str, id_column: str, vector_column: str) -> None:
        """
        Ensure that the database and table exist, creating them if necessary

        :param table_name: Table name.
        :param id_column: Column name for unique IDs.
        :param vector_column: Column name for vector data.
        """
        try:
            if not await self.check_db_exists():
                logging.warning(f"Database '{self.database}' does not exist. Creating...")
                await self.create_database()

            if not await self.check_table_exists(table_name):
                logging.warning(f"Table '{table_name}' does not exist. Creating...")
                await self.create_table(table_name, id_column, vector_column)

            logging.info(f"Database '{self.database}' and table '{table_name}' are ready.")
        except Exception as e:
            logging.error(f"Error ensuring database and table: {e}")
            raise
