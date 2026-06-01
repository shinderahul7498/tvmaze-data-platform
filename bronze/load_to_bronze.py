# Databricks notebook source
# MAGIC %run /Workspace/Users/shinderahul9763@gmail.com/shinderahul9763@gmail.com_ce/tvmaze-data-platform/get_config_params/config

# COMMAND ----------

# MAGIC %run /Workspace/Users/shinderahul9763@gmail.com/shinderahul9763@gmail.com_ce/tvmaze-data-platform/utils/common_functions

# COMMAND ----------

import pandas as pd
import requests
from pyspark.sql.functions import current_timestamp
import json

# COMMAND ----------

dbutils.widgets.dropdown("entity_name","shows",
    ["shows", "episodes", "cast"]
)

# COMMAND ----------

entity = dbutils.widgets.get("entity_name")
config = tvmaze_config[entity]
source_api_url = config["source_api_url"]
bronze_table_name = config["bronze_table_name"]
bronze_schema_name = config["bronze_schema_name"]
bronze_partition_col = config["bronze_partition_col"]
bronze_key_ids = config["bronze_key_ids"]
write_mode = config["write_mode"]
logger.info(f"starting bronze load for {entity}")

# COMMAND ----------

try:
    # Shows
    if entity == "shows":
        df = fetch_api_data(source_api_url)

    # Episodes
    elif entity == "episodes":
        shows_config = tvmaze_config["shows"]
        shows_table = f"{shows_config['bronze_schema_name']}.{shows_config['bronze_table_name']}"
        shows_df = spark.table(shows_table)
        df = None
        for show_ids in get_ids_in_batches(shows_df, batch_size=100):
            batch_df = fetch_api_data(source_api_url, show_ids, add_show_id=True)
            if df is None:
                df = batch_df
            else:
                df = df.unionByName(batch_df, allowMissingColumns=True)

    # Cast
    elif entity == "cast":
        shows_config = tvmaze_config["shows"]
        shows_table = f"{shows_config['bronze_schema_name']}.{shows_config['bronze_table_name']}"
        shows_df = spark.table(shows_table)
        df = None
        for show_ids in get_ids_in_batches(shows_df, batch_size=100):
            batch_df = fetch_api_data(source_api_url, show_ids, add_show_id=True)
            if df is None:
                df = batch_df
            else:
                df = df.unionByName(batch_df, allowMissingColumns=True)
    else:
        raise Exception(f"invalid entity : {entity}")

    # Clean column names
    df = normalize_column_names(df)
except Exception as e:
    logger.error(f"Error occurred during data fetch for entity '{entity}': {str(e)}", exc_info=True)
    raise

# COMMAND ----------

# WRITE IN BRONZE TABLE
if df is not None and df.count() > 0:
    write_dataframe(
        logger,
        bronze_schema_name,
        bronze_table_name,
        bronze_partition_col,
        table_keys=bronze_key_ids,
        df=df,
        write_mode=write_mode,
        schema_evolution=config["schema_evolution"]
    )
    logger.info(f"{entity} bronze load completed")
    print(f"{entity} bronze load completed")
else:
    logger.info(f"No data to write for {entity}")
    print(f"No data to write for {entity}")