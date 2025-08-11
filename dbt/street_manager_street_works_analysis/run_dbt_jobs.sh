#!/bin/bash

# Exit on any error
set -e

echo "Starting dbt run..."

# Run dbt models
dbt run --vars "{motherduck_token: $MOTHERDUCK_TOKEN, motherduck_db_name: $MOTHERDB}"

echo "Starting dbt tests..."

# Run dbt tests
dbt test
