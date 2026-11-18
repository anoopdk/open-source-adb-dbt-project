# Architecture

## Context diagram

```mermaid
flowchart LR
  SRC[Operational extracts] --> PSP[PySpark ingestion]
  PSP --> BR[Bronze]
  BR --> SIL[Silver PySpark transforms]
  SIL --> DBT[dbt models / tests]
  DBT --> GOLD[Gold analytics marts]
```

## Implementation flow

```mermaid
flowchart TB
  N1[bronze_ingestion.ipynb] --> N2[silver_transformation.ipynb]
  U[utils/util_silver.py] --> N2
  N2 --> D[dbt modelling track]
```

## Design notes

- Prefer layered medallion / governed platform patterns where applicable.
- Keep configuration and secrets outside Git.
- Preserve auditability: documentation must match repository evidence.
