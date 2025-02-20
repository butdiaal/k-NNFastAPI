import logging
import argparse
from clickhouse_driver import Client


class Queries:
    """
    A collection of SQL queries for ClickHouse operations.
    """

    INSERT_DATA = "INSERT INTO {database}.{table} ({ids}, {vectors}) VALUES"

    SEARCH_SIMILAR_L2Distance = """
            WITH {vector} AS reference_vector
            SELECT {id_column}, L2Distance({vector_column}, reference_vector) AS distance
            FROM {database}.{table}
            ORDER BY distance
            LIMIT {count}
        """

    SEARCH_SIMILAR_cosineDistance = """
            WITH {vector} AS reference_vector
            SELECT {id_column}, cosineDistance({vector_column}, reference_vector) AS distance
            FROM {database}.{table}
            ORDER BY distance
            LIMIT {count}
        """
    DELETE_UUID = """
            DELETE FROM TABLE {database}.{table}
                WHERE {id_column} IN ({ids_str})
        """

class ClickHouseConnection:
    """
    A class for managing ClickHouse connection.
    """

    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.client = Client(host=host, port=port, user=user, password=password)
        self.database = database
        logging.info("Successfully connected to ClickHouse.")

    def get_client(self) -> Client:
        return self.client

