import os
import requests

CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
CLICKHOUSE_SERVICE_NAME = os.getenv("CLICKHOUSE_SERVICE_NAME", "clickhouse-service")

def get_service_address():
    url = f"http://{CONSUL_HOST}:8500/v1/catalog/service/{CLICKHOUSE_SERVICE_NAME}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        services = response.json()
        if services:
            service = services[0]
            return service["Address"], service["ServicePort"]
        else:
            raise ValueError(f"Service {CLICKHOUSE_SERVICE_NAME} not found in Consul")
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to get service address from Consul: {e}")

CLICKHOUSE_HOST, CLICKHOUSE_PORT = get_service_address()
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "db_master")
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE", "element")
CLICKHOUSE_IDS = os.getenv("CLICKHOUSE_IDS", "doc_id")
CLICKHOUSE_VECTORS = os.getenv("CLICKHOUSE_VECTORS", "centroid")
BATCH_SIZE = int(os.getenv("CLICKHOUSE_BATCH_SIZE", 1000))