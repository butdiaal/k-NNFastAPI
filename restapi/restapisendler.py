import argparse
import json
import logging
import requests

logging.basicConfig(level=logging.INFO)

class JSONSender:
    @staticmethod
    def send_data(host: str, port: int, endpoint: str, data: dict, output_file: str) -> None:
        url = f"http://{host}:{port}/{endpoint}"
        headers = {'Content-Type': 'application/json'}

        try:
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()
            response_data = response.json()

            # Сохранение ответа в файл
            with open(output_file, "w") as f:
                json.dump(response_data, f, indent=4)

            logging.info(f"Response saved to {output_file}")

        except requests.exceptions.RequestException as e:
            error_message = {"error": str(e)}
            with open(output_file, "w") as f:
                json.dump(error_message, f, indent=4)

            logging.error(f"Error sending data. Details saved to {output_file}")


def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments.

    :return: Parsed arguments as a namespace object.
    """
    parser = argparse.ArgumentParser(description="Upload data to FastAPI")

    parser.add_argument('--host', default="localhost", help='The host to run the server on')
    parser.add_argument('--port', type=int, default=4000, help='The port for starting the server')
    parser.add_argument("--endpoint", help="API endpoint (insert, search, delete)")
    parser.add_argument('--input_file', help="The JSON file from which the data is received")
    parser.add_argument('--output_file', default="response.json", help="The file to save server response")

    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    try:
        with open(args.input_file, "r") as json_file:
            data = json.load(json_file)

        JSONSender.send_data(args.host, args.port, args.endpoint, data, args.output_file)
    except FileNotFoundError:
        logging.error(f"File not found: {args.input_file}")


if __name__ == "__main__":
    main()
