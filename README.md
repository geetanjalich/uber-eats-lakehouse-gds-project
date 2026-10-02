# Uber Eats Marketplace Lakehouse

This repository implements an end-to-end Databricks lakehouse for an Uber Eats-style food delivery marketplace. It is designed for Databricks Free Edition while keeping the architecture close to patterns used in production data platforms.

The solution includes Unity Catalog setup, Auto Loader ingestion, a medallion architecture, incremental batch processing, SCD Type 2 dimensions, optimized facts and aggregates, native Spark SQL data quality checks, quarantine tables, Databricks Jobs, Databricks Asset Bundles, GitHub CI/CD templates, unit tests, and Genie-ready analytical views.

![Uber Eats Marketplace Lakehouse Architecture](docs/images/uber-eats-marketplace-lakehouse-architecture.png)

![Uber Eats Marketplace Lakehouse Data Model](docs/images/uber-eats-marketplace-lakehouse-data-model.png)

Start with the overview, then use the technical guide as the execution runbook:

- [Project Overview And Business Outcomes](docs/01_PROJECT_OVERVIEW_AND_BUSINESS_OUTCOMES.md)
- [Technical Setup Deployment And Execution Guide](docs/02_TECHNICAL_SETUP_DEPLOYMENT_AND_EXECUTION_GUIDE.md)
- [Genie Agent Instructions](docs/03_GENIE_AGENT_INSTRUCTIONS.md)
