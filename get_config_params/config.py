# Databricks notebook source
env = "dev"
catalog = "workspace"
base_path = "dbfs:/FileStore/tvmaze"

# COMMAND ----------

# DBTITLE 1,config
tvmaze_config = {
    "shows": {
        "source_api_url": "https://api.tvmaze.com/shows",
        "entity_name": "shows",
        "load_type": "full",
        "write_mode": "overwrite",
        "schema_evolution": "Y",
        "file_format": "json",
        "bronze_key_ids": "id",
        "silver_key_ids": "id",
        "gold_key_ids": "show_id",
        "bronze_table_name": "b_shows",
        "silver_table_name": "s_shows",
        "gold_table_name": "dim_shows",
        "bronze_schema_name": f"{env}_tvmaze_bronze",
        "silver_schema_name": f"{env}_tvmaze_silver",
        "gold_schema_name": f"{env}_tvmaze_gold",
        "bronze_partition_col": [],
        "silver_partition_col": ["partition_year","partition_month","partition_day"],
        "gold_partition_col": ["partition_year","partition_month","partition_day"],
        "audit_columns": ["created_date"],
        "cdc_column": "updated",
        "bronze_key_column": [],
        "cols_to_encrypt_bronze": [],
        "cols_to_encrypt_silver": []
    },
    "episodes": {
        "source_api_url": "https://api.tvmaze.com/shows/{}/episodes",
        "entity_name": "episodes",
        "load_type": "full",
        "write_mode": "overwrite",
        "schema_evolution": "Y",
        "file_format": "json",
        "bronze_key_ids": "id",
        "silver_key_ids": "episode_id",
        "gold_key_ids": "episode_id",
        "cdc_column": "",
        "bronze_table_name": "b_episodes",
        "silver_table_name": "s_episodes",
        "gold_table_name": "fact_episodes",
        "bronze_schema_name": f"{env}_tvmaze_bronze",
        "silver_schema_name": f"{env}_tvmaze_silver",
        "gold_schema_name": f"{env}_tvmaze_gold",
        "bronze_partition_col": [],
        "silver_partition_col": [
            "partition_year",
            "partition_month",
            "partition_day"
        ],
        "gold_partition_col": [
            "partition_year",
            "partition_month",
            "partition_day"
        ],
        "audit_columns": ["created_date"],
        "bronze_key_column": [],
        "cols_to_encrypt_bronze": [],
        "cols_to_encrypt_silver": []
    },
    "cast": {
        "source_api_url": "https://api.tvmaze.com/shows/{}/cast",
        "entity_name": "cast",
        "load_type": "full",
        "write_mode": "overwrite",
        "schema_evolution": "Y",
        "file_format": "json",
        "bronze_key_ids": "show_id",
        "silver_key_ids": "person_id",
        "gold_key_ids": "person_id",
        "cdc_column": "person_updated",
        "bronze_table_name": "b_cast",
        "silver_table_name": "s_cast",
        "gold_table_name": "dim_cast",
        "bronze_schema_name": f"{env}_tvmaze_bronze",
        "silver_schema_name": f"{env}_tvmaze_silver",
        "gold_schema_name": f"{env}_tvmaze_gold",
        "bronze_partition_col": [],
        "silver_partition_col": [
            "partition_year",
            "partition_month",
            "partition_day"
        ],
        "gold_partition_col": [
            "partition_year",
            "partition_month",
            "partition_day"
        ],
        "audit_columns": ["created_date"],
        "bronze_key_column": [],
        "cols_to_encrypt_bronze": [],
        "cols_to_encrypt_silver": []
    }
}