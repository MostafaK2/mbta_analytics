FROM python:3.14-slim

WORKDIR /app

# Only the dbt project — mirrors how mbta-python only packages ingestion/
COPY . /app/dbt/mbta_analytics

RUN pip install --no-cache-dir --break-system-packages dbt-bigquery

WORKDIR /app/dbt/mbta_analytics

# Bake in packages (dbt_utils, codegen) at build time so runs don't need
# network access to hub.getdbt.com every single execution
RUN dbt deps

# To run command: docker build -t mbta-dbt:latest .
# To run with command: 