# TVMaze Data Engineering Project

## Overview

This project implements an end-to-end Medallion Architecture pipeline on Databricks using data from the TVMaze APIs.

The solution ingests Shows, Episodes, and Cast data, processes it through Bronze, Silver, and Gold layers, and creates business-ready analytical datasets.

---

# Architecture

```text
TVMaze APIs
    |
    v
Bronze Layer
    |
    v
Silver Layer
    |
    v
Gold Layer
```

---

# Source APIs

### Shows

```text
https://api.tvmaze.com/shows
```

### Episodes

```text
https://api.tvmaze.com/shows/{show_id}/episodes
```

### Cast

```text
https://api.tvmaze.com/shows/{show_id}/cast
```

---

# Bronze Layer

## Purpose

Store raw API data with minimal transformations.

## Bronze Tables

| Table      |
| ---------- |
| b_shows    |
| b_episodes |
| b_cast     |

## Processing

### Shows

Incremental ingestion using source CDC column:

```text
updated
```

Logic:

```text
updated > MAX(updated)
```

### Episodes

Incremental ingestion using:

```text
LEFT ANTI JOIN
```

Business Key:

```text
id
```

### Cast

Incremental ingestion using:

```text
LEFT ANTI JOIN
```

Business Key:

```text
show_id
person_id
character_id
```

## Transformations

* API ingestion
* JSON flattening
* Column normalization
* Incremental filtering
* Delta append

---

# Silver Layer

## Purpose

Clean, standardize, and enrich Bronze data.

## Silver Tables

| Table          |
| -------------- |
| s_shows        |
| s_episodes     |
| s_cast         |
| fact_show_data |

## Transformations

### Shows

* Data type casting
* Null handling
* CDC using updated timestamp
* Partition generation

### Episodes

* Data type casting
* Date conversion
* Incremental processing

### Cast

* Data type casting
* Incremental processing

---

# Fact Table

## fact_show_data

Created by joining:

```text
s_shows
s_episodes
s_cast
```

Contains:

* Show Information
* Episode Information
* Cast Information
* Genre Information

Sample Columns:

```text
show_id
show_name
language
genre
season
episode_id
episode_name
runtime
person_id
cast_name
character_name
```

---

# Performance Optimizations

## Broadcast Join

Applied on:

```text
s_shows
```

Reason:

```text
Small dimension table
Avoid shuffle
Improve join performance
```

Implementation:

```python
broadcast(shows_df)
```

---

## Incremental Loading

### Shows

```text
updated > max(updated)
```

### Episodes

```text
LEFT ANTI JOIN
```

### Cast

```text
LEFT ANTI JOIN
```

### Fact Table

```text
LEFT ANTI JOIN
```

---

## Data Skew Handling

Skew validation implemented.

Technique documented:

```text
Salting
```

Approach:

```python
floor(rand() * 10)
```

---

## Delta Optimization

Applied:

```sql
OPTIMIZE table_name
```

Benefits:

* Small file compaction
* Faster query execution
* Better storage layout

---

# Gold Layer

## Purpose

Provide business-ready reporting datasets.

## Gold Tables

### g_episodes_per_season

Calculates:

```text
Episodes per Show per Season
```

---

### g_avg_runtime_per_show

Calculates:

```text
Average Runtime per Show
```

---

### g_top_cast_members

Calculates:

```text
Top 10 Cast Members
```

Uses:

```text
Window Function
ROW_NUMBER()
```

---

### g_common_genres

Calculates:

```text
Most Common Genres
```

---

# Databricks Features Used

## Delta Lake

* ACID Transactions
* Schema Enforcement
* Schema Evolution
* Delta Optimization

## Unity Catalog

* Data Governance
* Access Control
* Lineage
* Auditing

## Databricks Workflows

Used for orchestration of:

```text
Bronze
Silver
Gold
```

pipelines.

## Databricks Asset Bundles

Used for deployment across:

```text
DEV
UAT
PROD
```

environments.

---

# Project Structure

```text
tvmaze_project
│
├── config
│   └── tvmaze_config.py
│
├── notebooks
│   ├── bronze
│   │   └── load_to_bronze
│   │
│   ├── silver
│   │   └── load_to_silver
│   │
│   ├── gold
│   │   ├── load_fact_show_data
│   │   └── load_show_analytics
│   │
│   └── orchestration
│       └── job_launcher
│
├── resources
│   └── tvmaze_job.yml
│
└── databricks.yml
```

---

# End-to-End Flow

```text
Shows API
Episodes API
Cast API
      |
      v
 Bronze Layer
      |
      v
 Silver Layer
      |
      v
 fact_show_data
      |
      v
 Gold Analytics Tables
      |
      v
 Reporting & Analytics
```

---

# Key Concepts Demonstrated

* API Data Ingestion
* Medallion Architecture
* Incremental Loading
* CDC Processing
* Broadcast Join
* Data Skew Analysis
* Salting Technique
* Delta Lake
* Unity Catalog
* Window Functions
* Databricks Workflows
* Databricks Asset Bundles
* Performance Optimization
