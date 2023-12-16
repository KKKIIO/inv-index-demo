#!/bin/sh
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{
    "name": "postgres-connector-0",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "localhost",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname" : "postgres",
        "topic.prefix": "postgres-0",
        "table.include.list": "public.orders",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false"
    }
}'
