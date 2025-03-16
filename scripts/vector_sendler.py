"""
JSON API Client

This script provides a command-line interface for interacting with a JSON-based API.
It supports three main operations: insert, search, and delete. The script can process
individual JSON files or directories containing multiple JSON files.

Supported endpoints:
- insertion: Sends data to the database for insertion.
- Search: Requests data from the database that meets the specified criteria.
- deletion: Sends a request to delete data from the database.

Usage Examples:

1. Insert data from a single JSON file:
   python vector_sendler.py --endpoint insert --input_path data.json --output_path response.json

2. Search for data with specific parameters:
   python vector_sendler.py --endpoint search --input_path data.json --output_path search_results.json --count 5 --measure_type cosine

3. Delete data using a JSON file:
   python vector_sendler.py --endpoint delete --input_path data.json --output_path delete_response.json

4. Process all JSON files in a directory:
   python vector_sendler.py --endpoint insert --input_path data_directory/ --output_path responses/

Arguments:
--host: The host address of the API server (default: localhost).
--port: The port number of the API server (default: 4000).
--endpoint: The API endpoint to use (required, choices: insert, search, delete).
--input_path: Path to the JSON file or directory containing JSON files.
--output_path: Path to save the server's response or error details.
--count: Number of results to return (for search only, default: 10).
--measure_type: Distance measure type (for search only, choices: l2, cosine).
"""

import argparse
import json
import logging
import aiohttp
import asyncio
import os

from aiohttp import ClientResponse

logging.basicConfig(level=logging.INFO)


async def is_host_available(host: str, port: int, timeout: int = 5) -> bool:
    """
    Checks if the host and port are available.

    :param host: The host to check.
    :param port: The port to check.
    :param timeout: Connection timeout in seconds.
    :return: True if the host and port are available, otherwise False.
    """
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=timeout
        )
        await writer.close()
        await writer.wait_closed()
        return True
    except (asyncio.TimeoutError, ConnectionRefusedError, OSError) as e:
        logging.error(f"Error checking host {host}:{port}: {e}")
        return False


class JSONSender:
    """A utility class for sending JSON data to a specified API endpoint."""

    @staticmethod
    async def send_request(
        host: str,
        port: int,
        endpoint: str,
        data: dict,
        output_file: str,
        params: dict = None,
    ) -> None:
        """
        Send JSON data to the specified API endpoint and save the response or error to a file.

        :param host: The host address of the API server.
        :param port: The port number of the API server.
        :param endpoint: The API endpoint (e.g., 'insert', 'search', 'delete').
        :param data: The JSON data to send.
        :param output_file: Path to save the server's response or error details.
        :param params: Query parameters (for example, count, measure_type for search).
        """

        url = f"http://{host}:{port}/{endpoint}"
        headers = {"Content-Type": "application/json"}

        if not await is_host_available(host, port):
            error_message = {"error": f"Host {host}:{port} is not available"}

            with open(output_file, "w") as file:
                json.dump(error_message, file, indent=4)
            logging.error(f"Host {host}:{port} is not available. Data not sent.")
            return

        try:
            async with aiohttp.ClientSession() as session:
                response: ClientResponse
                async with await session.post(
                    url, headers=headers, json=data, params=params
                ) as response:
                    await response.raise_for_status()
                    response_data = await response.json()

                    with open(output_file, "w") as file:
                        json.dump(response_data, file, indent=4)

                    logging.info(f"Response saved to {output_file}")

        except aiohttp.ClientError as e:
            error_message = {"error": str(e)}
            with open(output_file, "w") as f:
                json.dump(error_message, f, indent=4)

            logging.error(f"Error sending data. Details saved to {output_file}")

    @staticmethod
    async def send_insert(host: str, port: int, data: dict, output_file: str) -> None:
        """Method for sending an insert request."""
        await JSONSender.send_request(host, port, "insert", data, output_file)

    @staticmethod
    async def send_search(
        host: str,
        port: int,
        data: dict,
        output_file: str,
        count: int = None,
        measure_type: str = None,
    ) -> None:
        """Method for sending a search query with parameters."""
        data.update({"measure_type": measure_type, "count": count})
        await JSONSender.send_request(host, port, "search", data, output_file)

    @staticmethod
    async def send_delete(host: str, port: int, data: dict, output_file: str) -> None:
        """Method for sending a delete request."""
        await JSONSender.send_request(host, port, "delete", data, output_file)


async def process_file(
    input_path: str,
    output_path: str,
    host: str,
    port: int,
    endpoint: str,
    count: int = None,
    measure_type: str = None,
) -> None:
    """
    Processes a single JSON file: downloads its data and sends it to the server.

    :param input_path: Path to the JSON file.
    :param output_path: The path to save the response.
    :param host: The server host.
    :param port: The server port.
    :param endpoint: API endpoint.
    :param count: Number of results to return (for search only).
    :param measure_type: Distance measure type (for search only).
    """
    try:
        with open(input_path, "r") as json_file:
            data = json.load(json_file)

        if endpoint == "insert":
            await JSONSender.send_insert(host, port, data, output_path)
        elif endpoint == "search":
            await JSONSender.send_search(
                host, port, data, output_path, count=count, measure_type=measure_type
            )
        elif endpoint == "delete":
            await JSONSender.send_delete(host, port, data, output_path)
        else:
            logging.error(f"Unknown endpoint: {endpoint}")
    except FileNotFoundError:
        logging.error(f"File not found: {input_path}")
    except json.JSONDecodeError:
        logging.error(f"Invalid JSON format: {input_path}")


async def process_input(
    input_path: str,
    output_path: str,
    host: str,
    port: int,
    endpoint: str,
    count: int = None,
    measure_type: str = None,
) -> None:
    """
    Processes input data: if a file is specified, data is sent from it; if a folder is specified, all JSON files inside it are processed.

    :param input_path: The path to the file or directory.
    :param output_path: The path to the file to save the response.
    :param host: The server host.
    :param port: The server port.
    :param endpoint: API endpoint.
    :param count: Number of results to return (for search only).
    :param measure_type: Distance measure type (for search only).
    """
    if os.path.isdir(input_path):
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        tasks = []

        for filename in os.listdir(input_path):
            file_path = os.path.join(input_path, filename)
            if os.path.isfile(file_path) and filename.endswith(".json"):
                response_path = os.path.join(output_path, f"response_{filename}")
                if endpoint == "search":
                    tasks.append(
                        process_file(
                            file_path,
                            response_path,
                            host,
                            port,
                            endpoint,
                            count,
                            measure_type,
                        )
                    )
                else:
                    tasks.append(
                        process_file(file_path, response_path, host, port, endpoint)
                    )

        await asyncio.gather(*tasks)
    else:
        if endpoint == "search":
            await process_file(
                input_path, output_path, host, port, endpoint, count, measure_type
            )
        else:
            await process_file(input_path, output_path, host, port, endpoint)


def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments.

    :return: Parsed arguments as a namespace object.
    """
    parser = argparse.ArgumentParser(description="Upload data to FastAPI")

    parser.add_argument(
        "--host", default="localhost", help="The host to run the server on"
    )
    parser.add_argument(
        "--port", type=int, default=4000, help="The port for starting the server"
    )
    parser.add_argument(
        "--endpoint",
        choices=["insert", "search", "delete"],
        required=True,
        help="API endpoint",
    )
    parser.add_argument(
        "--input_path",
        default="dataset.json",
        help="The path to the JSON file or directory for uploading data",
    )
    parser.add_argument(
        "--output_path",
        default="response.json",
        help="Fail for saving server responses",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of returned results (for search only)",
    )
    parser.add_argument(
        "--measure_type",
        choices=["l2", "cosine"],
        required=True,
        help="Metrica (for search only)",
    )

    return parser.parse_args()


async def main() -> None:
    """Main function to execute the script logic."""

    args = parse_arguments()
    await process_input(
        args.input_path,
        args.output_path,
        args.host,
        args.port,
        args.endpoint,
        args.count,
        args.measure_type,
    )


if __name__ == "__main__":
    asyncio.run(main())
