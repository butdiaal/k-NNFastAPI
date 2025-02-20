import logging
import argparse
from clickhouse_driver import Client


class Queries:
    """
    A collection of SQL queries for ClickHouse operations.
    """

    CREATE_DATABASE = "CREATE DATABASE IF NOT EXISTS {database}"
    SET_EXPETEMENTAL = """SET allow_experimental_vector_similarity_index = 1"""
    CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS {database}.{table}
            (
                {ids} UUID,
                {vectors} Array(Float64)
            )
            ENGINE = MergeTree()
            ORDER BY {ids}
        """
    ADD_INDEX_L2 = """
            ALTER TABLE {database}.{table} 
            ADD INDEX idx_l2 {vectors} 
            TYPE vector_similarity('hnsw', 'L2Distance') 
            GRANULARITY 1
        """
    ADD_INDEX_cosine = """
            ALTER TABLE {database}.{table} 
            ADD INDEX idx_cosine {vectors} 
            TYPE vector_similarity('hnsw', 'cosineDistance') 
            GRANULARITY 1
        """

    SHOW_DATABASES = "SHOW DATABASES"
    SHOW_TABLES = "SHOW TABLES FROM {database}"

    INSERT_DATA = "INSERT INTO {database}.{table} ({ids}, {vectors}) VALUES"
    SELECT_ALL = "SELECT * FROM {database}.{table}"
    CHECK_TABLE = "SHOW CREATE TABLE {database}.{table}"

    GET_VECTORS = "SELECT {ids}, {vectors} FROM {database}.{table}"

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

