# TVMaze Data Platform

## Overview

This project ingests TVMaze API data and implements a Medallion Architecture using Databricks.

Layers:

- Bronze
- Silver
- Gold

## Source APIs

Shows:
https://api.tvmaze.com/shows

Episodes:
https://api.tvmaze.com/shows/{id}/episodes

Cast:
https://api.tvmaze.com/shows/{id}/cast

## Architecture

TVMaze API
    |
    v
Bronze Layer
    |
    v
Silver Layer
    |
    v
Gold Layer
    |
    v
Unity Catalog

## Bronze Layer

Tables:

- b_shows
- b_episodes
- b_cast

Features:

- Raw ingestion
- Schema evolution
- Delta format
- Unity Catalog registration

## Silver Layer

Tables:

- s_shows
- s_episodes
- s_cast

Features:

- Data cleansing
- Null handling
- Incremental loading
- Audit columns
- Partitioning

## Gold Layer

Tables:

- dim_shows
- dim_cast
- fact_episodes

Features:

- Business-ready datasets
- Reporting layer

## Workflow

Orchestrated through Databricks Workflows.

Job Launcher:

orchestration/job_launcher

## Access Control

Implemented using Unity Catalog GRANT statements.

## Technologies

- Databricks
- PySpark
- Delta Lake
- Unity Catalog
- Databricks Asset Bundles