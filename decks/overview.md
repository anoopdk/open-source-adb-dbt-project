# PySpark + dbt Lakehouse Delivery

> Zynx Technologies | 3-slide delivery overview

---

## Slide 1: Context & problem

- Zynx x Logistics Client
- Engagement: Reference Implementation Delivery
- The Logistics Client needed a modular lakehouse pattern that separates heavy distributed ingestion/cleansing (PySpark) from declarative analytics modelling (dbt), so platform and analytics engineers can own layers independently.
- Engagement levels: Architecture Alignment -> PySpark Ingestion Delivery -> Silver Transformation Delivery -> Analytics Engineering Handover

---

## Slide 2: Solution architecture & stack

- Zynx delivered a PySpark bronze/silver notebook path with reusable utilities and a documented medallion + dbt collaboration model suitable for Azure Databricks-style enterprise platforms.
- Stack: PySpark, dbt (target pattern), Delta / lakehouse concepts, Medallion architecture
- Architecture documented in docs/03-architecture.md with Mermaid diagrams

---

## Slide 3: Implementation highlights & inspection

- Key evidence: pyspark_dbt/bronze_ingestion.ipynb, pyspark_dbt/silver_transformation.ipynb, pyspark_dbt/utils/
- Open docs/audit/PROJECT-FACTSHEET.md for the audit index
- Repository standardised to Zynx industrial portfolio structure
- Portfolio tag target: 1.0.0-portfolio
