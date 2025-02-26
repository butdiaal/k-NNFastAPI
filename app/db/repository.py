import logging
from clickhouse_driver import errors
from app.services.vector_service import ContentStorage
from app.db.queries import Queries


class ClickHouseRepository:
    def __init__(self, connection: ContentStorage):
        self.client = connection.connect()
        self.database = connection.database

    async def check_db_exists(self) -> bool:
        """
        Checks if the database exists.

        :return: True if the database exists, False otherwise.
        """
        try:
            databases = {db[0] for db in self.client.execute(Queries.SHOW_DATABASES)}
            return self.database in databases
        except errors.Error as e:
            logging.error(f"Error checking if database exists: {e}")
            raise

    async def check_table_exists(self, table_name: str) -> bool:
        """
        Checks if a specific table exists in the database.

        :param table_name: The name of the table to check.
        :return: True if the table exists, False otherwise.
        """
        try:
            tables = {
                table[0]
                for table in self.client.execute(
                    Queries.SHOW_TABLES.format(database=self.database)
                )
            }
            return table_name in tables
        except errors.Error as e:
            logging.error(f"Error checking if table exists: {e}")
            raise

    async def create_database(self) -> None:
        """
        Creates the database if it does not exist.
        """
        try:
            self.client.execute(Queries.CREATE_DATABASE.format(database=self.database))
            logging.info(f"Database '{self.database}' created.")
        except errors.Error as e:
            logging.error(f"Error creating database: {e}")
            raise

    async def create_table(self, table_name: str, ids: str, vectors: str) -> None:
        """
        Creates a table in the database if it does not exist.

        :param table_name: The name of the table to create.
        :param ids: The column name for unique identifiers.
        :param vectors: The column name for storing vector data.
        """
        try:
            self.client.execute(Queries.SET_EXPERIMENTAL)

            self.client.execute(
                Queries.CREATE_TABLE.format(
                    database=self.database, table=table_name, ids=ids, vectors=vectors
                )
            )

            self.client.execute(
                Queries.ADD_INDEX_L2.format(
                    database=self.database, table=table_name, ids=ids, vectors=vectors
                )
            )

            self.client.execute(
                Queries.ADD_INDEX_COSINE.format(
                    database=self.database, table=table_name, ids=ids, vectors=vectors
                )
            )

            logging.info(f"Table '{table_name}' in database '{self.database}' created.")
        except errors.Error as e:
            logging.error(f"Error creating table: {e}")
            raise

    async def ensure_db_and_table(
        self, table_name: str, ids: str, vectors: str
    ) -> None:
        """
        Ensures the database and table exist, creating them if necessary.

        :param table_name: The name of the table.
        :param ids: The column name for unique identifiers.
        :param vectors: The column name for storing vector data.
        """
        try:
            if not await self.check_db_exists():
                logging.warning(
                    f"Database '{self.database}' does not exist. Creating it..."
                )
                await self.create_database()

            if not await self.check_table_exists(table_name):
                logging.warning(f"Table '{table_name}' does not exist. Creating it...")
                await self.create_table(table_name, ids, vectors)

            logging.info(
                f"Database '{self.database}' and table '{table_name}' are ready."
            )
        except Exception as e:
            logging.error(f"Error ensuring database and table: {e}")
            raise
