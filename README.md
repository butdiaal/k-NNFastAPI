# Vector Search API

## Description
This service provides an API for working with vector data in ClickHouse.  
You can add, search for similar, and delete vectors.

## **Launching with Docker**
The service can be started using Docker:

```sh
docker-compose down -v

docker-compose up --build

docker run -d --name my_fastapi_app -p 4000:8000  fastapi_image 
```

## API Endpoints

### 1. Insert Data (`/insert`)
- **Description**: Insert data into ClickHouse.
- **Request Body**:
  ```json
  {
    "data": [
      ["550e8400-e29b-41d4-a716-446655440000", [0.1, 0.2, 0.3, ..., 0.512]],
      ["f47ac10b-58cc-4372-a567-0e02b2c3d479", [0.1, 0.2, 0.3, ..., 0.512]]
    ]
  }

- **Response example**:
  ```json 
  {
      "status": "success",
      "message": "Successfully inserted 2 records."
  }

⸻

### 2. Search for similar vectors (`/search`)
- **Description**: It searches for similar vectors by metric.
- **Request Body**:
  ```json
  {
    "vectors": [[0.1, 0.2, 0.3, ..., 0.512]],
    "measure_type": "l2",
    "count": 5
  }

- **Response example**:
  ```json 
   "status": "success",
   "message": "Successfully retrieved similar vectors.",
   "result": [
        ["550e8400-e29b-41d4-a716-446655440000", 0.90],
        ["f47ac10b-58cc-4372-a567-0e02b2c3d479", 0.85]
   ]

⸻


### 3. Deleting data(`/delete`)
- **Description**: Deletes vectors by their ID.
- **Request Body**:
  ```json
  {
    "ids": ["550e8400-e29b-41d4-a716-446655440000", "f47ac10b-58cc-4372-a567-0e02b2c3d479"]
  }

- **Response example**:
  ```json 
  {
    "status": "success",
    "message": "Deleted 2 records."
  }

⸻

## Using the client (vector_sendler.py)

The client (vector_sendler.py) allows you to send requests via the command line.

### Call examples

- #### To insert:
  ```sh
  python client.py --host localhost --port 4000 --endpoint insert --input_path data.json --output_path response.json
  ```
- #### For the search:
  ```sh
  python client.py --host localhost --port 4000 --endpoint search --input_path query.json --output_path search_response.json --measure_type l2 --count 10
  ```
- #### To delete:
  ```sh
  python client.py --host localhost --port 4000 --endpoint delete --input_path delete.json --output_path delete_response.json
  ```
