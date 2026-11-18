# PySpark + dbt Lakehouse Delivery

**Delivered by:** [Zynx Technologies Ltd](https://www.zynxtechnologies.com/)  
**Portfolio host (pre-fork):** `anoopdk/open-source-adb-dbt-project`  
**Domain client (sanitised):** Zynx x Logistics Client  
**Engagement type:** Reference Implementation Delivery  
**Portfolio version:** `1.0.0-portfolio`

> Inspection note: Client identity is domain-aliased for public portfolio use. This repository is structured for authenticity review of delivery artefacts, architecture, and engagement scope.

## Project overview

The Logistics Client needed a modular lakehouse pattern that separates heavy distributed ingestion/cleansing (PySpark) from declarative analytics modelling (dbt), so platform and analytics engineers can own layers independently.

### Solution delivered

Zynx delivered a PySpark bronze/silver notebook path with reusable utilities and a documented medallion + dbt collaboration model suitable for Azure Databricks-style enterprise platforms.

## Engagement levels

| # | Level |
| --- | --- |
| 1 | Architecture Alignment |
| 2 | PySpark Ingestion Delivery |
| 3 | Silver Transformation Delivery |
| 4 | Analytics Engineering Handover |

| Field | Value |
| --- | --- |
| Engagement posture | Delivery portfolio artefact prepared for audit / inspection |
| Confidentiality | Domain alias only; no real client legal name in public docs |
| Primary artefacts | Code/notebooks/docs already in-repo + Zynx standard pack under `docs/` and `decks/` |

## Architecture

```mermaid
flowchart LR
  SRC[Operational extracts] --> PSP[PySpark ingestion]
  PSP --> BR[Bronze]
  BR --> SIL[Silver PySpark transforms]
  SIL --> DBT[dbt models / tests]
  DBT --> GOLD[Gold analytics marts]
```

### Implementation flow

```mermaid
flowchart TB
  N1[bronze_ingestion.ipynb] --> N2[silver_transformation.ipynb]
  U[utils/util_silver.py] --> N2
  N2 --> D[dbt modelling track]
```

## Technology stack

`PySpark`, `dbt (target pattern)`, `Delta / lakehouse concepts`, `Medallion architecture`

## Repository map

| Path | Purpose |
| --- | --- |
| `README.md` | Project overview + engagement levels |
| `docs/` | Problem, solution, architecture, implementation, runbook, audit factsheet |
| `decks/` | 3-slide delivery overview (Markdown + HTML) |
| `src/` / project subtree | Implementation assets (or placeholders where design-led) |
| `tests/` | Tests or placeholders |
| `scripts/` | Operational / helper scripts |
| `configs/` | Non-secret configuration templates |
| `.env.example` | Environment variable placeholders |

### Key existing delivery paths

- `pyspark_dbt/bronze_ingestion.ipynb`
- `pyspark_dbt/silver_transformation.ipynb`
- `pyspark_dbt/utils/`

## How to inspect this repository

1. Read this README (overview + engagement).
2. Open `docs/audit/PROJECT-FACTSHEET.md`.
3. Review architecture in `docs/03-architecture.md`.
4. Walk implementation evidence listed in the factsheet.
5. Open `decks/overview.html` for the 3-slide executive summary.

## Related Zynx context

Zynx Technologies Ltd provides IT consulting across cloud, data analytics, and digital delivery. This repository is part of the public delivery portfolio hosted on `anoopdk` prior to organisation-level fork.

## License

See `LICENSE`.
