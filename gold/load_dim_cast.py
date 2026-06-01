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
silver_schema_name = config["silver_schema_name"]
silver_table_name = config["silver_table_name"]
gold_schema_name = config["gold_schema_name"]
gold_table_name = config["gold_table_name"]
gold_partition_col = config["gold_partition_col"]
gold_key_ids = config["gold_key_ids"]
silver_key_ids = config["silver_key_ids"]
write_mode = "append"
cdc_column = config["cdc_column"]

logger.info(
    f"starting silver load for {entity}"
)

# COMMAND ----------

# DBTITLE 1,load query
def get_data_from_silver(
    logger,
    silver_schema_name,
    silver_table_name,
    cdc_column,
    gold_schema_name,
    gold_table_name
):
    silver_table = f"{silver_schema_name}.{silver_table_name}"
    gold_table = f"{gold_schema_name}.{gold_table_name}"

    if spark.catalog.tableExists(gold_table):
        df = spark.sql(f"""
            SELECT *
            FROM (
                SELECT DISTINCT
                    show_id,
                    person_id,
                    person_name,
                    person_gender,
                    person_birthday,
                    person_country_name,
                    character_id,
                    character_name,
                    person_updated,
                    person_updated_time,
                    partition_year,
                    partition_month,
                    partition_day,
                    CURRENT_TIMESTAMP() AS dw_updated_timestamp,
                    current_user() AS dw_updated_by
                FROM {silver_table}
            ) src
            WHERE person_updated_time >
            (
                SELECT COALESCE(
                    MAX(person_updated_time),
                    TIMESTAMP('1900-01-01')
                )
                FROM {gold_table}
            )
        """)
    else:
        df = spark.sql(f"""
            SELECT DISTINCT
                show_id,
                person_id,
                person_name,
                person_gender,
                person_birthday,
                person_country_name,
                character_id,
                character_name,
                person_updated,
                person_updated_time,
                partition_year,
                partition_month,
                partition_day,
                CURRENT_TIMESTAMP() AS dw_updated_timestamp,
                current_user() AS dw_updated_by
            FROM {silver_table}
        """)
    return df

# COMMAND ----------

# get silver data
df = get_data_from_silver(logger,silver_schema_name,silver_table_name,cdc_column,gold_schema_name,gold_table_name)

if df is not None and df.count() > 0:
    # write gold
    df = clean_dataset(df)

    write_dataframe(logger,gold_schema_name,gold_table_name,gold_partition_col,table_keys=gold_key_ids,df=df,write_mode=write_mode,schema_evolution=config["schema_evolution"])
    
    logger.info(f"{entity} gold load completed")
    print(f"{entity} gold load completed")
else:
    logger.info(f"No data to load for {entity} gold table")
    print(f"No data to load for {entity} gold table")