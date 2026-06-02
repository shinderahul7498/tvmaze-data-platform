# Databricks notebook source
# DBTITLE 1,Logger import
# --------------------------------------------------
# logger
# --------------------------------------------------
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("tvmaze_logger")

# COMMAND ----------

def fetch_api_data(api_url, show_ids=None, add_show_id=False):
    final_data = []
    if show_ids:
        for sid in show_ids:
            try:
                data = requests.get(api_url.format(sid)).json()
                if add_show_id:
                    for row in data:
                        row["show_id"] = sid
                final_data.extend(data)
            except Exception as e:
                logger.error(f"show_id={sid}, error={str(e)}")
    else:
        final_data = requests.get(api_url).json()
    pdf = pd.json_normalize(final_data)
    return spark.createDataFrame(pdf)

# COMMAND ----------

def get_ids_in_batches(df, id_column="id", batch_size=100):
    batch_ids = []
    for row in df.select(id_column).toLocalIterator():
        batch_ids.append(row[id_column])
        if len(batch_ids) >= batch_size:
            yield batch_ids
            batch_ids = []
    if batch_ids:
        yield batch_ids

# COMMAND ----------

# DBTITLE 1,Write data in delta tables
from pyspark.sql.functions import current_timestamp

def write_dataframe(logger,schema_name,table_name,partition_cols,table_keys,df,write_mode="overwrite",schema_evolution="Y"):
    try:
        full_table_name = f"{schema_name}.{table_name}"
        logger.info(f"started writing : {full_table_name}")
        logger.info(f"table_keys : {table_keys}")
        if "created_date" not in df.columns:
            df = df.withColumn("created_date", current_timestamp())
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
        writer = df.write.format("delta").mode(write_mode)
        if schema_evolution == "Y":
            writer = writer.option("mergeSchema", "true")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.saveAsTable(full_table_name)
        # spark.sql(f"REFRESH TABLE {full_table_name}")
        logger.info(f"table registered : {full_table_name}")
        logger.info(f"completed writing : {full_table_name}")
    except Exception as e:
        logger.error(f"error while writing dataframe : {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,Normalizing column names
def normalize_column_names(df):
    for col_name in df.columns:
        new_col_name = col_name \
            .replace(".", "_") \
            .replace(" ", "_") \
            .replace("-", "_") \
            .replace("(", "") \
            .replace(")", "") \
            .lower()
        df = df.withColumnRenamed(
            col_name,
            new_col_name
        )
    return df

# COMMAND ----------

from pyspark.sql.types import StringType
from pyspark.sql.functions import col, trim, when


def clean_dataset(df, default_value="NAFS"):

    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                when(
                    col(field.name).isNull(),
                    default_value
                ).when(
                    trim(col(field.name)) == "",
                    default_value
                ).otherwise(
                    trim(col(field.name))
                )
            )

    return df

# COMMAND ----------

def apply_liquid_clustering(logger,schema_name,table_name,cluster_columns):

    if not cluster_columns:
        return

    cluster_cols = ",".join(
        cluster_columns
    )

    spark.sql(f"""
        ALTER TABLE
        {schema_name}.{table_name}
        CLUSTER BY (
            {cluster_cols}
        )
    """)

    logger.info(
        f"liquid clustering applied "
        f"on {cluster_cols}"
    )