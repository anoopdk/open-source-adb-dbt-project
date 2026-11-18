# Implementation notes

## Primary evidence paths

- `pyspark_dbt/bronze_ingestion.ipynb`
- `pyspark_dbt/silver_transformation.ipynb`
- `pyspark_dbt/utils/`

## Stack

- PySpark
- dbt (target pattern)
- Delta / lakehouse concepts
- Medallion architecture

## Delivery practice

1. Confirm requirements and engagement level.
2. Align architecture and acceptance criteria.
3. Implement / document against fixture or sanitised data.
4. Validate and prepare handover pack (`docs/` + `decks/`).
