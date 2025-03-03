import asyncio
from asynch import connect


class ClickHouseClient:
    """
    A class for managing ClickHouse connection.
    """

    def __init__(self, host, port, user, password, database):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._client = None
        self._lock = asyncio.Lock()

    async def connect(self):
        """Asynchronous connection to ClickHouse."""
        async with self._lock:
            if self._client is None:
                self._client = await connect(
                    host=self._host,
                    port=self._port,
                    user=self._user,
                    password=self._password,
                    database=self._database,
                )

    async def close(self):
        """Close async connection."""
        if self._client:
            await self._client.close()
            self._client = None

    async def get_cursor(self):
        """Returns the cursor for executing queries"""
        if self._client is None:
            await self.connect()
        return self._client.cursor()
