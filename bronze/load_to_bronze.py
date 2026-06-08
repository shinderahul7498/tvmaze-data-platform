# Databricks notebook source
# MAGIC %run /Workspace/Users/shinderahul9763@gmail.com/shinderahul9763@gmail.com_ce/tvmaze-data-platform/get_config_params/config

# COMMAND ----------

# MAGIC %run /Workspace/Users/shinderahul9763@gmail.com/shinderahul9763@gmail.com_ce/tvmaze-data-platform/utils/common_functions

# COMMAND ----------

import pandas as pd
import requests
from pyspark.sql.functions import current_timestamp
import json
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "uat", "prod"])
dbutils.widgets.dropdown("entity_name", "shows", ["shows", "episodes", "cast"])

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
        df = normalize_column_names(df)
        bronze_table = f"{bronze_schema_name}.{bronze_table_name}"
        if spark.catalog.tableExists(bronze_table):
            max_updated = spark.table(bronze_table).agg({"updated": "max"}).collect()[0][0]
            df = df.filter(F.col("updated") > max_updated)

    # Episodes
    elif entity == "episodes":
        shows_config = tvmaze_config["shows"]
        shows_table = f"{shows_config['bronze_schema_name']}.{shows_config['bronze_table_name']}"
        shows_df = spark.table(shows_table)
        df = None
        for show_ids in get_ids_in_batches(shows_df, batch_size=100):
            batch_df = fetch_api_data(source_api_url, show_ids, add_show_id=True)
            df = batch_df if df is None else df.unionByName(batch_df, allowMissingColumns=True)
        df = normalize_column_names(df)
        bronze_table = f"{bronze_schema_name}.{bronze_table_name}"
        if spark.catalog.tableExists(bronze_table):
            bronze_df = spark.table(bronze_table)
            df = df.alias("src").join(bronze_df.alias("tgt"), F.col("src.id") == F.col("tgt.id"), "left_anti")

    # Cast
    elif entity == "cast":
        shows_config = tvmaze_config["shows"]
        shows_table = f"{shows_config['bronze_schema_name']}.{shows_config['bronze_table_name']}"
        shows_df = spark.table(shows_table)
        df = None
        for show_ids in get_ids_in_batches(shows_df, batch_size=100):
            batch_df = fetch_api_data(source_api_url, show_ids, add_show_id=True)
            df = batch_df if df is None else df.unionByName(batch_df, allowMissingColumns=True)
        df = normalize_column_names(df)
        bronze_table = f"{bronze_schema_name}.{bronze_table_name}"
        if spark.catalog.tableExists(bronze_table):
            bronze_df = spark.table(bronze_table)
            df = df.alias("src").join(
                bronze_df.alias("tgt"),
                (F.col("src.show_id") == F.col("tgt.show_id")) &
                (F.col("src.person_id") == F.col("tgt.person_id")) &
                (F.col("src.character_id") == F.col("tgt.character_id")),
                "left_anti"
            )
    else:
        raise Exception(f"invalid entity : {entity}")
except Exception as e:
    logger.error(f"Error occurred during data fetch for entity '{entity}' : {str(e)}", exc_info=True)
    raise

# COMMAND ----------

if df is not None and df.count() > 0:
    write_dataframe(
        logger,
        bronze_schema_name,
        bronze_table_name,
        bronze_partition_col,
        table_keys=bronze_key_ids,
        df=df,
        write_mode="append",
        schema_evolution=config["schema_evolution"]
    )
    logger.info(f"{entity} bronze load completed")
else:
    logger.info(f"No data to write for {entity}")