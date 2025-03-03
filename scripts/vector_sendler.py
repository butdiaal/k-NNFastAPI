import argparse
import json
import logging
import aiohttp
import asyncio
import os

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
        writer.close()
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
                async with session.post(
                    url, headers=headers, json=data, params=params
                ) as response:
                    response.raise_for_status()
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
        count: int = 10,
        measure_type: str = "l2",
    ) -> None:
        """Method for sending a search query with parameters."""
        data.update({"measure_type": measure_type, "count": count})
        await JSONSender.send_request(host, port, "search", data, output_file)

    @staticmethod
    async def send_delete(host: str, port: int, data: dict, output_file: str) -> None:
        """Method for sending a delete request."""
        await JSONSender.send_request(host, port, "delete", data, output_file)


async def process_file(
    input_path: str, output_file: str, host: str, port: int, endpoint: str
) -> None:
    """
    Processes a single JSON file: downloads its data and sends it to the server.

     :param input_path: Path to the JSON file.
     :param output_file: The path to save the response.
     :param host: The server host.
     :param port: The server port.
     :param endpoint: API endpoint.
    """
    try:
        with open(input_path, "r") as json_file:
            data = json.load(json_file)

        if endpoint == "insert":
            await JSONSender.send_insert(host, port, data, output_file)
        elif endpoint == "search":
            await JSONSender.send_search(
                host, port, data, output_file, count=10, measure_type="L2"
            )
        elif endpoint == "delete":
            await JSONSender.send_delete(host, port, data, output_file)
        else:
            logging.error(f"Unknown endpoint: {endpoint}")
    except FileNotFoundError:
        logging.error(f"File not found: {input_path}")
    except json.JSONDecodeError:
        logging.error(f"Invalid JSON format: {input_path}")


async def process_input(
    input_path: str, output_file: str, host: str, port: int, endpoint: str
) -> None:
    """
    Processes the input data: if it is a file, it sends it, if it is a directory, it processes all the JSON files in it.

     :param input_path: The path to the file or directory.
     :param output_file: The path to the file or directory to save the response.
     :param host: The server host.
     :param port: The server port.
     :param endpoint: API endpoint.
    """
    if os.path.isdir(input_path):
        if not os.path.exists(output_file):
            os.makedirs(output_file)

        tasks = []
        for filename in os.listdir(input_path):
            file_path = os.path.join(input_path, filename)
            if os.path.isfile(file_path) and filename.endswith(".json"):
                response_path = os.path.join(output_file, f"response_{filename}")
                tasks.append(
                    process_file(file_path, response_path, host, port, endpoint)
                )

        await asyncio.gather(*tasks)
    else:
        await process_file(input_path, output_file, host, port, endpoint)


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
        "--output_file",
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
        help="Metrica (only for search)",
    )

    return parser.parse_args()


async def main() -> None:
    """Main function to execute the script logic."""

    args = parse_arguments()
    await process_input(
        args.input_path, args.output_file, args.host, args.port, args.endpoint
    )


if __name__ == "__main__":
    asyncio.run(main())
