# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %run /Workspace/Users/shinderahul9763@gmail.com/shinderahul9763@gmail.com_ce/tvmaze-data-platform/get_config_params/config

# COMMAND ----------

# MAGIC %run /Workspace/Users/shinderahul9763@gmail.com/shinderahul9763@gmail.com_ce/tvmaze-data-platform/utils/common_functions

# COMMAND ----------

from pyspark.sql.functions import current_user
# widgets
dbutils.widgets.dropdown("entity_name","episodes",["episodes"])

# COMMAND ----------

entity = dbutils.widgets.get("entity_name")
config = tvmaze_config[entity]
bronze_schema_name = config["bronze_schema_name"]
bronze_table_name = config["bronze_table_name"]
silver_schema_name = config["silver_schema_name"]
silver_table_name = config["silver_table_name"]
silver_partition_col = config["silver_partition_col"]
silver_key_ids = config["silver_key_ids"]
cdc_column = config["cdc_column"]
write_mode = "append"

logger.info(
    f"starting silver load for {entity}"
)

# COMMAND ----------

def get_data_from_bronze(
    logger,
    bronze_schema_name,
    bronze_table_name,
    cdc_column,
    silver_schema_name,
    silver_table_name
):
    bronze_table = f"{bronze_schema_name}.{bronze_table_name}"
    silver_table = f"{silver_schema_name}.{silver_table_name}"
    logger.info(f"reading bronze table : {bronze_table}")

    if spark.catalog.tableExists(silver_table):
        logger.info(f"silver table exists : {silver_table}")
        df = spark.sql(f"""
            SELECT DISTINCT
                CAST(b.id AS BIGINT) AS episode_id,
                CAST(b.show_id AS BIGINT) AS show_id,
                TRIM(b.name) AS episode_name,
                CAST(b.season AS INT) AS season,
                CAST(b.number AS INT) AS episode_number,
                b.type,
                CASE WHEN TRIM(airdate) = '' THEN NULL ELSE TO_DATE(airdate,'yyyy-MM-dd') END AS air_date,
                b.airtime,
                TO_TIMESTAMP(b.airstamp) AS air_timestamp,
                COALESCE(b.runtime, 0) AS runtime,
                b.summary,
                COALESCE(b.rating_average, 0) AS rating_average,
                b.image_medium,
                b.image_original,
                b.url,
                b._links_self_href AS links_self_href,
                b._links_show_href AS links_show_href,
                b._links_show_name AS links_show_name,
                b.created_date as created_at,
                YEAR(CASE WHEN TRIM(airdate) = '' THEN NULL ELSE TO_DATE(airdate,'yyyy-MM-dd') END) AS partition_year,
                MONTH(CASE WHEN TRIM(airdate) = '' THEN NULL ELSE TO_DATE(airdate,'yyyy-MM-dd') END) AS partition_month,
                DAY(CASE WHEN TRIM(airdate) = '' THEN NULL ELSE TO_DATE(airdate,'yyyy-MM-dd') END) AS partition_day,
                CURRENT_TIMESTAMP() AS dw_updated_timestamp,
                current_user() AS dw_updated_by
            FROM {bronze_table} b
            LEFT ANTI JOIN {silver_table} s
            ON b.id = s.episode_id
            WHERE b.id IS NOT NULL
        """)
    else:
        logger.info(f"silver table does not exist : {silver_table}")
        df = spark.sql(f"""
            SELECT DISTINCT
                CAST(id AS BIGINT) AS episode_id,
                CAST(show_id AS BIGINT) AS show_id,
                TRIM(name) AS episode_name,
                CAST(season AS INT) AS season,
                CAST(number AS INT) AS episode_number,
                type,
                CASE WHEN TRIM(airdate) = '' THEN NULL ELSE TO_DATE(airdate,'yyyy-MM-dd') END AS air_date,
                airtime,
                TO_TIMESTAMP(airstamp) AS air_timestamp,
                COALESCE(runtime, 0) AS runtime,
                summary,
                COALESCE(rating_average, 0) AS rating_average,
                image_medium,
                image_original,
                url,
                _links_self_href AS links_self_href,
                _links_show_href AS links_show_href,
                _links_show_name AS links_show_name,
                created_date,
                YEAR(CASE WHEN TRIM(airdate) = '' THEN NULL ELSE TO_DATE(airdate,'yyyy-MM-dd') END) AS partition_year,
                MONTH(CASE WHEN TRIM(airdate) = '' THEN NULL ELSE TO_DATE(airdate,'yyyy-MM-dd') END) AS partition_month,
                DAY(CASE WHEN TRIM(airdate) = '' THEN NULL ELSE TO_DATE(airdate,'yyyy-MM-dd') END) AS partition_day,
                CURRENT_TIMESTAMP() AS dw_updated_timestamp,
                current_user() AS dw_updated_by
            FROM {bronze_table}
            WHERE id IS NOT NULL
        """)
    logger.info(f"records loaded from bronze : {df.count()}")
    return df

# COMMAND ----------

# get bronze data
df = get_data_from_bronze(logger,bronze_schema_name,bronze_table_name,cdc_column,silver_schema_name,silver_table_name)

if df is not None and df.count() > 0:
    # write silver
    df = clean_dataset(df)
    write_dataframe(logger,silver_schema_name,silver_table_name,silver_partition_col,table_keys=silver_key_ids,df=df,write_mode=write_mode,schema_evolution=config["schema_evolution"])
    
    logger.info(f"{entity} silver load completed")
    print(f"{entity} silver load completed")
else:
    logger.info(f"No data to load for {entity} silver table")
    print(f"No data to load for {entity} silver table")