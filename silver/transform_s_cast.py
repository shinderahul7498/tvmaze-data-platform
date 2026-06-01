# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %run /Workspace/Users/shinderahul9763@gmail.com/shinderahul9763@gmail.com_ce/tvmaze-data-platform/get_config_params/config

# COMMAND ----------

# MAGIC %run /Workspace/Users/shinderahul9763@gmail.com/shinderahul9763@gmail.com_ce/tvmaze-data-platform/utils/common_functions

# COMMAND ----------

from pyspark.sql.functions import current_user
# widgets
dbutils.widgets.dropdown("entity_name","cast",["cast"])

# COMMAND ----------

entity = dbutils.widgets.get("entity_name")
config = tvmaze_config[entity]
bronze_schema_name = config["bronze_schema_name"]
bronze_table_name = config["bronze_table_name"]
silver_schema_name = config["silver_schema_name"]
silver_table_name = config["silver_table_name"]
silver_partition_col = config["silver_partition_col"]
silver_key_ids = config["silver_key_ids"]
write_mode = "append"
cdc_column = config["cdc_column"]

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
            SELECT *
            FROM (
                SELECT DISTINCT
                    CAST(show_id AS BIGINT) AS show_id,
                    CAST(person_id AS BIGINT) AS person_id,
                    person_name,
                    person_gender,
                    TO_DATE(person_birthday) AS person_birthday,
                    TO_DATE(person_deathday) AS person_deathday,
                    person_country_name,
                    person_country_code,
                    person_country_timezone,
                    person_url,
                    person_image_medium,
                    person_image_original,
                    CAST(character_id AS BIGINT) AS character_id,
                    character_name,
                    character_url,
                    character_image_medium,
                    character_image_original,
                    self,
                    voice,
                    CAST(person_updated AS BIGINT) AS person_updated,
                    TO_TIMESTAMP(FROM_UNIXTIME(person_updated)) AS person_updated_time,
                    created_date as created_at,
                    YEAR(TO_TIMESTAMP(FROM_UNIXTIME(person_updated))) AS partition_year,
                    MONTH(TO_TIMESTAMP(FROM_UNIXTIME(person_updated))) AS partition_month,
                    DAY(TO_TIMESTAMP(FROM_UNIXTIME(person_updated))) AS partition_day,
                    CURRENT_TIMESTAMP() AS dw_updated_timestamp,
                    current_user() AS dw_updated_by
                FROM {bronze_table}
                WHERE person_id IS NOT NULL
            ) src
                WHERE TO_TIMESTAMP(FROM_UNIXTIME({cdc_column})) >
                (
                    SELECT COALESCE(
                        MAX(
                            TO_TIMESTAMP(FROM_UNIXTIME({cdc_column}))
                        ),
                        TIMESTAMP('1900-01-01')
                    )
                    FROM {silver_table}
                )
        """)
    else:
        logger.info(f"silver table does not exist : {silver_table}")
        df = spark.sql(f"""
            SELECT DISTINCT
                CAST(show_id AS BIGINT) AS show_id,
                CAST(person_id AS BIGINT) AS person_id,
                person_name,
                person_gender,
                TO_DATE(person_birthday) AS person_birthday,
                TO_DATE(person_deathday) AS person_deathday,
                person_country_name,
                person_country_code,
                person_country_timezone,
                person_url,
                person_image_medium,
                person_image_original,
                CAST(character_id AS BIGINT) AS character_id,
                character_name,
                character_url,
                character_image_medium,
                character_image_original,
                self,
                voice,
                CAST(person_updated AS BIGINT) AS person_updated,
                TO_TIMESTAMP(FROM_UNIXTIME(person_updated)) AS person_updated_time,
                created_date as created_at,
                YEAR(TO_TIMESTAMP(FROM_UNIXTIME(person_updated))) AS partition_year,
                MONTH(TO_TIMESTAMP(FROM_UNIXTIME(person_updated))) AS partition_month,
                DAY(TO_TIMESTAMP(FROM_UNIXTIME(person_updated))) AS partition_day,
                CURRENT_TIMESTAMP() AS dw_updated_timestamp,
                current_user() AS dw_updated_by
            FROM {bronze_table}
            WHERE person_id IS NOT NULL
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