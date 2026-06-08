# Databricks notebook source
# MAGIC %run /Workspace/Users/shinderahul9763@gmail.com/shinderahul9763@gmail.com_ce/tvmaze-data-platform/get_config_params/config

# COMMAND ----------

# MAGIC %run /Workspace/Users/shinderahul9763@gmail.com/shinderahul9763@gmail.com_ce/tvmaze-data-platform/utils/common_functions

# COMMAND ----------

from pyspark.sql.functions import (
    explode,
    current_timestamp,
    current_user,
    broadcast,
    floor, rand, array, lit
)

# widgets
dbutils.widgets.dropdown("entity_name","fact_show_data",["fact_show_data"])
dbutils.widgets.dropdown("env", "dev", ["dev", "uat", "prod"])

# COMMAND ----------

entity = dbutils.widgets.get("entity_name")
config = tvmaze_config[entity]
silver_schema_name = config["silver_schema_name"]
silver_table_name = config["silver_table_name"]
silver_partition_col = config["silver_partition_col"]
silver_key_ids = config["silver_key_ids"]
write_mode = "append"
cdc_column = config["cdc_column"]
cluster_columns = config.get("cluster_columns",[])

logger.info(
    f"starting silver load for {entity}"
)

# COMMAND ----------

def get_data_from_silver(logger,silver_schema_name,silver_table_name):

    shows_table = f"{silver_schema_name}.s_shows"
    episodes_table = f"{silver_schema_name}.s_episodes"
    cast_table = f"{silver_schema_name}.s_cast"
    target_table = f"{silver_schema_name}.{silver_table_name}"

    logger.info(f"reading silver tables : {shows_table}, {episodes_table}, {cast_table}")

    shows_df = spark.table(shows_table)
    episodes_df = spark.table(episodes_table)
    cast_df = spark.table(cast_table)

    # ==========================================================
    # Data Skew Validation
    # ==========================================================
    skew_df = (
        episodes_df
        .groupBy("show_id")
        .count()
        .orderBy(col("count").desc())
    )

    logger.info("Top skewed show_ids")
    # ==========================================================
    # Salting Technique
    # ==========================================================
    episodes_df = episodes_df.withColumn("salt", floor(rand() * 10))

    shows_df = shows_df.withColumn(
        "salt",
        explode(array(lit(0), lit(1), lit(2), lit(3), lit(4), lit(5), lit(6), lit(7), lit(8), lit(9)))
    )

    logger.info("applying broadcast join with salting")

    df = (
        episodes_df.alias("e")
        .join(
            broadcast(shows_df.alias("s")),
            [col("e.show_id") == col("s.show_id"), col("e.salt") == col("s.salt")],
            "inner"
        )
        .join(
            cast_df.alias("c"),
            col("e.show_id") == col("c.show_id"),
            "left"
        )
        .select(
            col("e.show_id").alias("show_id"),
            col("s.show_name"),
            col("s.language"),
            explode(col("s.genres")).alias("genre"),
            col("e.season"),
            col("e.episode_id"),
            col("e.episode_name"),
            col("e.air_date").alias("airdate"),
            col("e.runtime"),
            col("c.person_id"),
            col("c.person_name").alias("cast_name"),
            col("c.character_id"),
            col("c.character_name"),
            current_timestamp().alias("dw_updated_timestamp"),
            current_user().alias("dw_updated_by")
        )
        .distinct()
    )

    # ==========================================================
    # Incremental Load
    # ==========================================================
    if spark.catalog.tableExists(target_table):
        logger.info(f"fact table exists : {target_table}")
        target_df = spark.table(target_table)
        df = (
            df.alias("src")
            .join(
                target_df.alias("tgt"),
                [
                    col("src.show_id") == col("tgt.show_id"),
                    col("src.episode_id") == col("tgt.episode_id"),
                    col("src.person_id") == col("tgt.person_id")
                ],
                "left_anti"
            )
        )
    else:
        logger.info(f"fact table does not exist : {target_table}")
    return df

# COMMAND ----------

# DBTITLE 1,Cell 6
# get different silver tables data
df = get_data_from_silver(logger,silver_schema_name,silver_table_name)

if df.count() > 0:
    # write silver
    df = clean_dataset(df)

   # Write Silver table
    write_dataframe(
        logger,
        silver_schema_name,
        silver_table_name,
        silver_partition_col,
        table_keys=silver_key_ids,
        df=df,
        write_mode=write_mode,
        schema_evolution=config["schema_evolution"]
    )

    #Applying Liquide Clustering
    apply_liquid_clustering(logger,silver_schema_name,silver_table_name,cluster_columns)
    
    #Applying optimize
    spark.sql(f"OPTIMIZE {silver_schema_name}.{silver_table_name}")

else:
    logger.info(f"No data to load for {entity} silver table")
    print(f"No data to load for {entity} silver table")