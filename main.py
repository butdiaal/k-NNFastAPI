import logging
import argparse
import os
import json
import uvicorn
from typing import List, Tuple, Dict
from clickhouse_driver import Client, errors
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from utils import Queries, ClickHouseConnection
logging.basicConfig(level=logging.INFO)

app = FastAPI()


class ClickHouseRepository:
    """
    Handles database connection and data insertion into ClickHouse.
    """

    def __init__(self, connection: ClickHouseConnection):
        """Initializes the repository with an existing ClickHouse connection."""
        self.client = connection.get_client()
        self.database = connection.database

    def insert_data(
        self, database: str, table: str, data: List[Tuple[str, List[float]]]
    ) -> None:
        """
        Inserts data into the specified ClickHouse table.

        :param database: Database name.
        :param table: Table name.
        :param data: List of tuples containing document IDs and vector data.
        """
        if not data:
            logging.error("No data to insert.")
            return

        try:
            query = Queries.INSERT_DATA.format(database=database, table=table)
            self.client.execute(query, data)
            logging.info(
                f"Successfully inserted {len(data)} records into '{database}.{table}'."
            )
        except errors.ServerException as e:
            logging.error(f"Error inserting data into ClickHouse: {e}")

    def search_similar_vectors(
        self,
        input_vectors: List[List[float]],
        table: str,
        id_column: str,
        vector_column: str,
        count: int,
        measure_type: str = "l2",
    ) -> Dict[int, List[Tuple[str, float]]]:
        """
        Finds the most similar vectors using the specified distance function in ClickHouse.

        :param input_vectors: A list of input vectors.
        :param table: The name of the table.
        :param id_column: The column name for document IDs.
        :param vector_column: The column name for vector data.
        :param count: The number of most similar vectors to retrieve.
        :param measure_type: The type of distance measure to use ("l2" or "cosine").
        :return: A dictionary where keys are indices of input vectors and values are lists of tuples with document IDs and distances.
        """
        results_dict = {}

        for index, input_vector in enumerate(input_vectors, start=1):
            vector_str = "[" + ",".join(map(str, input_vector)) + "]"

            if measure_type == "l2":
                query = Queries.SEARCH_SIMILAR_L2Distance.format(
                    vector=vector_str,
                    database=self.database,
                    table=table,
                    id_column=id_column,
                    vector_column=vector_column,
                    count=count,
                )
            elif measure_type == "cosine":
                query = Queries.SEARCH_SIMILAR_CosineDistance.format(
                    vector=vector_str,
                    database=self.database,
                    table=table,
                    id_column=id_column,
                    vector_column=vector_column,
                    count=count,
                )
            else:
                raise ValueError(f"Unsupported measure type: {measure_type}")

            result = self.client.execute(query)
            results_dict[index] = result

        return results_dict

    def delete_by_ids(self, table: str, id_column: str, ids: List[str]) -> None:
        """
        Deletes records from the table by their IDs.

        :param table: The name of the table.
        :param id_column: The column name for document IDs.
        :param ids: A list of IDs to delete.
        """
        if not ids:
            logging.warning("No IDs provided for deletion.")
            return

        try:
            ids_str = ", ".join(f"'{id}'" for id in ids)
            query = Queries.DELETE_UUID.format(
                database=self.database, table=table, id_column=id_column, ids_str=ids_str)
            self.client.execute(query)
            logging.info(f"Deleted {len(ids)} records from '{self.database}.{table}'.")
        except errors.ServerException as e:
            logging.error(f"Error deleting records from ClickHouse: {e}")
            raise


def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments.

    :return: Parsed arguments as a namespace object.
    """
    parser = argparse.ArgumentParser(description="Upload data to ClickHouse")

    parser.add_argument("--host", default="localhost", help="ClickHouse host")
    parser.add_argument("--port", type=int, default=9000, help="ClickHouse port")
    parser.add_argument("-u", "--user", default="default", help="ClickHouse user")
    parser.add_argument("-p", "--password", default="", help="ClickHouse password")
    parser.add_argument("--database", default="db_master", help="Database name")
    parser.add_argument("--table", default="element", help="Table name")
    parser.add_argument("--ids", default="doc_id", help="ID column name")
    parser.add_argument("--vectors", default="centroid", help="Vector column name")



    return parser.parse_args()


class VectorData(BaseModel):
    id: str
    vector: List[float]

class InsertRequest(BaseModel):
    data: List[VectorData]


class SearchRequest(BaseModel):
    vectors: List[List[float]]
    measure_type: str = "l2"
    count: int = 10


class DeleteRequest(BaseModel):
    ids: List[str]


@app.post("/insert/")
def insert_data(
    request: InsertRequest,
    table: str = Query(default="element", description="ClickHouse table name"),
    id_column: str = Query(default="doc_id", description="Column name for document IDs"),
    vector_column: str = Query(default="centroid", description="Column name for vector data"),
):
    """
    Inserts a list of vectors into the ClickHouse table.
    """
    try:
        args = parse_arguments()
        connection = ClickHouseConnection(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
        )

        db = ClickHouseRepository(connection)
        db.insert_data(args.database, table, request.data)

        return {"message": f"Successfully inserted {len(request.data)} records into ClickHouse."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/search/")
def search_similar_vectors(
    request: SearchRequest,
    table: str = Query(default="element", description="ClickHouse table name"),
    id_column: str = Query(default="doc_id", description="Column name for document IDs"),
    vector_column: str = Query(default="centroid", description="Column name for vector data"),
):
    """
    Performs a similarity search for the given vectors using the specified measure type.
    """
    try:
        args = parse_arguments()
        connection = ClickHouseConnection(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
        )

        db = ClickHouseRepository(connection)

        similar_vectors = db.search_similar_vectors(
            request.vectors,
            table,
            id_column,
            vector_column,
            request.count,
            request.measure_type,
        )

        return {"results": similar_vectors}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/delete/")
def delete_records(
    request: DeleteRequest,
    table: str = Query(default="element", description="ClickHouse table name"),
    id_column: str = Query(default="doc_id", description="Column name for document IDs"),
):
    """
    Deletes records from the table by their IDs.
    """
    try:
        args = parse_arguments()
        connection = ClickHouseConnection(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
        )

        db = ClickHouseRepository(connection)
        db.delete_by_ids(table, id_column, request.ids)

        return {"message": f"Deleted {len(request.ids)} records from '{table}'."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)