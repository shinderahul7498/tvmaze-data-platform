# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %run /Workspace/Users/shinderahul9763@gmail.com/shinderahul9763@gmail.com_ce/tvmaze-data-platform/get_config_params/config

# COMMAND ----------

# MAGIC %run /Workspace/Users/shinderahul9763@gmail.com/shinderahul9763@gmail.com_ce/tvmaze-data-platform/utils/common_functions

# COMMAND ----------

from pyspark.sql.functions import (
    countDistinct,
    avg,
    count,
    row_number,
    col,
    current_user
)
from pyspark.sql.window import Window
# widgets
dbutils.widgets.dropdown(
    "entity_name",
    "show_analytics",
    ["show_analytics"]
)
dbutils.widgets.dropdown("env", "dev", ["dev", "uat", "prod"])

# COMMAND ----------

entity = dbutils.widgets.get("entity_name")
config = tvmaze_config[entity]
silver_schema_name = config["silver_schema_name"]
silver_table_name = config["silver_table_name"]
gold_schema_name = config["gold_schema_name"]
gold_tables = config["gold_tables"]
write_mode = config["write_mode"]
fact_df = spark.table(f"{silver_schema_name}.{silver_table_name}")
logger.info(f"reading : {silver_schema_name}.{silver_table_name}")

# COMMAND ----------

# DBTITLE 1,load query
def get_gold_analytics(logger, silver_schema_name, silver_table_name, gold_tables):
    fact_df = spark.table(f"{silver_schema_name}.{silver_table_name}")
    analytics_dfs = {}

    try:
        analytics_dfs[gold_tables["episodes_per_season"]] = fact_df.groupBy("show_id", "show_name", "season").agg(countDistinct("episode_id").alias("total_episodes"))
    except Exception as e:
        logger.error(f"episodes per season failed: {str(e)}")

    try:
        analytics_dfs[gold_tables["avg_runtime_per_show"]] = fact_df.groupBy("show_id", "show_name").agg(avg("runtime").alias("avg_runtime"))
    except Exception as e:
        logger.error(f"avg runtime failed: {str(e)}")

    try:
        cast_df = fact_df.groupBy("person_id", "cast_name").agg(countDistinct("show_id").alias("total_shows"))
        window_spec = Window.orderBy(col("total_shows").desc())
        analytics_dfs[gold_tables["top_cast_members"]] = cast_df.withColumn("rank", row_number().over(window_spec)).filter(col("rank") <= 10)
    except Exception as e:
        logger.error(f"top cast members failed: {str(e)}")

    try:
        analytics_dfs[gold_tables["common_genres"]] = fact_df.groupBy("genre").agg(count("*").alias("genre_count"))
    except Exception as e:
        logger.error(f"common genres failed: {str(e)}")

    return analytics_dfs

# COMMAND ----------

analytics_dfs = get_gold_analytics(
    logger,
    silver_schema_name,
    silver_table_name,
    gold_tables
)

for table_name, df in analytics_dfs.items():
    df = (
        df.withColumn("dw_updated_timestamp", current_timestamp())
          .withColumn("dw_updated_by", current_user())
    )

    write_dataframe(
        logger,
        gold_schema_name,
        table_name,
        [],
        [],
        df,
        write_mode,
        config["schema_evolution"]
    )

    spark.sql(f"OPTIMIZE {gold_schema_name}.{table_name}")

    logger.info(f"{table_name} loaded successfully")