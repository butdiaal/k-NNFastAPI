class Queries:
    """
    A collection of SQL queries for ClickHouse operations.
    """

    SHOW_DATABASES = """SHOW DATABASES"""
    CREATE_DATABASE = """CREATE DATABASE IF NOT EXISTS {database}"""
    SHOW_TABLES = """SHOW TABLES FROM {database}"""
    SET_EXPERIMENTAL = """SET allow_experimental_vector_similarity_index = 1"""
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

    INSERT_DATA = """INSERT INTO {database}.{table} ({ids}, {vectors}) VALUES """

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

    SELECT_UUID = """SELECT {id_column} FROM {database}.{table} WHERE {id_column} IN ({ids_str})"""
    DELETE_UUID = """
            DELETE FROM {database}.{table}
            WHERE {id_column} IN ({ids_str})
        """
