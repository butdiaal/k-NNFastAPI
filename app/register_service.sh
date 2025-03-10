#!/bin/bash
CONSUL_HOST=${CONSUL_HOST:-"consul"}
CLICKHOUSE_HOST=${CLICKHOUSE_HOST:-"some1-clickhouse-server"}
CLICKHOUSE_PORT=${CLICKHOUSE_PORT:-9000}

curl -X PUT --data '{
  "Name": "clickhouse-service",
  "Address": "'$CLICKHOUSE_HOST'",
  "Port": '$CLICKHOUSE_PORT'
}' http://$CONSUL_HOST:8500/v1/agent/service/register