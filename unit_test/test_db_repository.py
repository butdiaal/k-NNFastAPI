import unittest
from unittest.mock import AsyncMock, patch
from clickhouse_driver import errors
from app.services.vector_service import ContentStorage
from app.db.queries import Queries
from app.db.repository import ClickHouseRepository


class TestClickHouseRepository(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        """Initialize the test environment."""
        self.connection = AsyncMock(spec=ContentStorage)
        self.connection.database = "test_db"
        self.repository = ClickHouseRepository(self.connection)

    async def test_check_db_exists_true(self) -> None:
        """Test checking if the database exists (database exists)."""
        self.connection.get_cursor.return_value.__aenter__.return_value.fetchall.return_value = [
            ("test_db",),
            ("other_db",),
        ]

        result = await self.repository.check_db_exists()
        self.assertTrue(result)

    async def test_check_db_exists_false(self) -> None:
        """Test checking if the database exists (database does not exist)."""
        self.connection.get_cursor.return_value.__aenter__.return_value.fetchall.return_value = [
            ("other_db",),
        ]

        result = await self.repository.check_db_exists()
        self.assertFalse(result)

    async def test_check_db_exists_error(self) -> None:
        """Test checking if the database exists (error during query execution)."""
        self.connection.get_cursor.return_value.__aenter__.return_value.execute.side_effect = errors.Error(
            "Connection error"
        )

        result = await self.repository.check_db_exists()
        self.assertFalse(result)

    async def test_check_table_exists_true(self) -> None:
        """Test checking if the table exists (table exists)."""
        self.connection.get_cursor.return_value.__aenter__.return_value.fetchall.return_value = [
            ("test_table",),
            ("other_table",),
        ]

        result = await self.repository.check_table_exists("test_table")
        self.assertTrue(result)

    async def test_check_table_exists_false(self) -> None:
        """Test checking if the table exists (table does not exist)."""
        self.connection.get_cursor.return_value.__aenter__.return_value.fetchall.return_value = [
            ("other_table",),
        ]

        result = await self.repository.check_table_exists("test_table")
        self.assertFalse(result)

    async def test_check_table_exists_error(self) -> None:
        """Test checking if the table exists (error during query execution)."""
        self.connection.get_cursor.return_value.__aenter__.return_value.execute.side_effect = errors.Error(
            "Connection error"
        )

        result = await self.repository.check_table_exists("test_table")
        self.assertFalse(result)

    async def test_create_database_success(self) -> None:
        """Test successful creation of the database."""
        await self.repository.create_database()

        self.connection.get_cursor.return_value.__aenter__.return_value.execute.assert_called_once_with(
            Queries.CREATE_DATABASE.format(database="test_db")
        )

    async def test_create_database_error(self) -> None:
        """Test database creation failure."""
        self.connection.get_cursor.return_value.__aenter__.return_value.execute.side_effect = errors.Error(
            "Database creation failed"
        )

        with self.assertRaises(errors.Error):
            await self.repository.create_database()

    async def test_create_table_success(self) -> None:
        """Test successful creation of the table."""
        table_name = "test_table"
        id_column = "id"
        vector_column = "vector"

        await self.repository.create_table(table_name, id_column, vector_column)

        cursor = self.connection.get_cursor.return_value.__aenter__.return_value
        cursor.execute.assert_any_call(Queries.SET_EXPERIMENTAL)
        cursor.execute.assert_any_call(
            Queries.CREATE_TABLE.format(
                database="test_db",
                table=table_name,
                ids=id_column,
                vectors=vector_column,
            )
        )
        cursor.execute.assert_any_call(
            Queries.ADD_INDEX_L2.format(
                database="test_db",
                table=table_name,
                ids=id_column,
                vectors=vector_column,
            )
        )
        cursor.execute.assert_any_call(
            Queries.ADD_INDEX_cosine.format(
                database="test_db",
                table=table_name,
                ids=id_column,
                vectors=vector_column,
            )
        )

    async def test_create_table_error(self) -> None:
        """Test table creation failure."""
        table_name = "test_table"
        id_column = "id"
        vector_column = "vector"

        self.connection.get_cursor.return_value.__aenter__.return_value.execute.side_effect = errors.Error(
            "Table creation failed"
        )

        with self.assertRaises(errors.Error):
            await self.repository.create_table(table_name, id_column, vector_column)

    @patch.object(ClickHouseRepository, "create_database", new_callable=AsyncMock)
    @patch.object(ClickHouseRepository, "create_table", new_callable=AsyncMock)
    async def test_ensure_db_and_table_success(
        self, mock_create_table, mock_create_database
    ) -> None:
        """Test successful creation of the database and table (if they do not exist)."""
        table_name = "test_table"
        id_column = "id"
        vector_column = "vector"

        self.repository.check_db_exists = AsyncMock(return_value=False)
        self.repository.check_table_exists = AsyncMock(return_value=False)

        await self.repository.ensure_db_and_table(table_name, id_column, vector_column)

        mock_create_database.assert_called_once()
        mock_create_table.assert_called_once_with(table_name, id_column, vector_column)

    @patch.object(ClickHouseRepository, "create_database", new_callable=AsyncMock)
    @patch.object(ClickHouseRepository, "create_table", new_callable=AsyncMock)
    async def test_ensure_db_and_table_db_exists(
        self, mock_create_table, mock_create_database
    ) -> None:
        """Test when the database already exists, but the table does not."""
        table_name = "test_table"
        id_column = "id"
        vector_column = "vector"

        self.repository.check_db_exists = AsyncMock(return_value=True)
        self.repository.check_table_exists = AsyncMock(return_value=False)

        await self.repository.ensure_db_and_table(table_name, id_column, vector_column)

        mock_create_database.assert_not_called()
        mock_create_table.assert_called_once_with(table_name, id_column, vector_column)

    @patch.object(ClickHouseRepository, "create_database", new_callable=AsyncMock)
    @patch.object(ClickHouseRepository, "create_table", new_callable=AsyncMock)
    async def test_ensure_db_and_table_table_exists(
        self, mock_create_table, mock_create_database
    ) -> None:
        """Test when both the database and table already exist."""
        table_name = "test_table"
        id_column = "id"
        vector_column = "vector"

        self.repository.check_db_exists = AsyncMock(return_value=True)
        self.repository.check_table_exists = AsyncMock(return_value=True)

        await self.repository.ensure_db_and_table(table_name, id_column, vector_column)

        mock_create_database.assert_not_called()
        mock_create_table.assert_not_called()

    @patch.object(ClickHouseRepository, "create_database", new_callable=AsyncMock)
    async def test_ensure_db_and_table_error(self, mock_create_database) -> None:
        """Test failure during database or table creation."""
        table_name = "test_table"
        id_column = "id"
        vector_column = "vector"

        mock_create_database.side_effect = errors.Error("Database error")
        self.repository.check_db_exists = AsyncMock(return_value=False)

        with self.assertRaises(errors.Error):
            await self.repository.ensure_db_and_table(
                table_name, id_column, vector_column
            )


if __name__ == "__main__":
    unittest.main()
