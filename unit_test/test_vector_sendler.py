import json
import os
import unittest
from unittest.mock import AsyncMock, patch
from typing import Dict, Any
from scripts.vector_sendler import (
    is_host_available,
    JSONSender,
    process_file,
    process_input,
)

TEST_DIR = "test_data"
TEST_FILE = os.path.join(TEST_DIR, "test.json")
TEST_RESPONSE_FILE = os.path.join(TEST_DIR, "response.json")


class TestHostAvailability(unittest.IsolatedAsyncioTestCase):
    """Test host availability."""

    @patch("scripts.vector_sendler.asyncio.open_connection", new_callable=AsyncMock)
    async def test_host_available(self, mock_open_connection: AsyncMock) -> None:
        """Test successful host availability."""
        mock_open_connection.return_value = (AsyncMock(), AsyncMock())
        result = await is_host_available("localhost", 4000)
        self.assertTrue(result)

    @patch(
        "scripts.vector_sendler.asyncio.open_connection",
        side_effect=ConnectionRefusedError,
    )
    async def test_host_unavailable(self, mock_open_connection: AsyncMock) -> None:
        """Test host unavailability."""
        mock_open_connection.return_value = (AsyncMock(), AsyncMock())
        result = await is_host_available("localhost", 4000)
        self.assertFalse(result)


class TestJSONSender(unittest.IsolatedAsyncioTestCase):
    """Test sending JSON data to the API."""

    def setUp(self) -> None:
        """Create a test directory if it doesn't exist."""
        os.makedirs(TEST_DIR, exist_ok=True)

    def tearDown(self) -> None:
        """Remove files after the test."""
        if os.path.exists(TEST_RESPONSE_FILE):
            os.remove(TEST_RESPONSE_FILE)
        if os.path.exists(TEST_DIR):
            os.rmdir(TEST_DIR)

    @patch("scripts.vector_sendler.is_host_available", new_callable=AsyncMock)
    @patch("scripts.vector_sendler.aiohttp.ClientSession.post", new_callable=AsyncMock)
    async def test_send_request_success(
        self, mock_post: AsyncMock, mock_host_available: AsyncMock
    ) -> None:
        """Test successful JSON data sending."""
        mock_host_available.return_value = True

        mock_response = AsyncMock()
        mock_response.json.return_value = {"status": "success"}
        mock_response.raise_for_status = AsyncMock()

        mock_post.return_value.__aenter__.return_value = mock_response

        data: Dict[str, Any] = {"test": "data"}
        await JSONSender.send_request(
            "localhost", 4000, "insert", data, TEST_RESPONSE_FILE
        )

        with open(TEST_RESPONSE_FILE, "r") as f:
            response = json.load(f)
        self.assertEqual(response, {"status": "success"})
        mock_post.assert_called_once()

    @patch("scripts.vector_sendler.is_host_available", new_callable=AsyncMock)
    async def test_send_request_host_unavailable(
        self, mock_host_available: AsyncMock
    ) -> None:
        """Test sending data when the host is unavailable."""
        mock_host_available.return_value = False
        data: Dict[str, Any] = {"test": "data"}

        await JSONSender.send_request(
            "localhost", 4000, "insert", data, TEST_RESPONSE_FILE
        )

        with open(TEST_RESPONSE_FILE, "r") as f:
            response = json.load(f)
        self.assertIn("error", response)


class TestProcessFile(unittest.IsolatedAsyncioTestCase):
    """Test processing a single JSON file."""

    def setUp(self) -> None:
        """Create a test directory and JSON file."""
        os.makedirs(TEST_DIR, exist_ok=True)
        with open(TEST_FILE, "w") as f:
            json.dump({"key": "value"}, f)

    def tearDown(self) -> None:
        """Remove test files after the test."""
        if os.path.exists(TEST_FILE):
            os.remove(TEST_FILE)
        if os.path.exists(TEST_RESPONSE_FILE):
            os.remove(TEST_RESPONSE_FILE)
        if os.path.exists(TEST_DIR):
            os.rmdir(TEST_DIR)

    @patch("scripts.vector_sendler.JSONSender.send_insert", new_callable=AsyncMock)
    async def test_process_file_insert(self, mock_send_insert: AsyncMock) -> None:
        """Test processing a file for data insertion."""
        await process_file(TEST_FILE, TEST_RESPONSE_FILE, "localhost", 4000, "insert")
        mock_send_insert.assert_called_once()


class TestProcessInput(unittest.IsolatedAsyncioTestCase):
    """Test processing a directory with JSON files."""

    def setUp(self) -> None:
        """Create a test directory with a JSON file."""
        os.makedirs(TEST_DIR, exist_ok=True)
        with open(TEST_FILE, "w") as f:
            json.dump({"key": "value"}, f)

    def tearDown(self) -> None:
        """Remove the test directory after the test."""
        if os.path.exists(TEST_FILE):
            os.remove(TEST_FILE)
        if os.path.exists(TEST_DIR):
            os.rmdir(TEST_DIR)

    @patch("scripts.vector_sendler.process_file", new_callable=AsyncMock)
    async def test_process_input_directory(self, mock_process_file: AsyncMock) -> None:
        """Test processing a directory with JSON files."""
        await process_input(TEST_DIR, TEST_DIR, "localhost", 4000, "insert")
        mock_process_file.assert_called_once()


if __name__ == "__main__":
    unittest.main()
