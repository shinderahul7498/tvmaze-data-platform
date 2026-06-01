# Databricks notebook source
dbutils.widgets.text("env", "dev")

# COMMAND ----------

env = dbutils.widgets.get("env")

union_query = "\nUNION ALL\n".join([
    f"""
    SELECT
    '{table_name}' AS table_name,
    COUNT(*) AS record_count
    FROM {env}_tvmaze_{layer}.{table}
    """
    for table_name, layer, table in [
        ("bronze_shows", "bronze", "b_shows"),
        ("silver_shows", "silver", "s_shows"),
        ("dim_shows", "gold", "dim_shows"),
        ("bronze_cast", "bronze", "b_cast"),
        ("silver_cast", "silver", "s_cast"),
        ("dim_cast", "gold", "dim_cast"),
        ("bronze_episodes", "bronze", "b_episodes"),
        ("silver_episodes", "silver", "s_episodes"),
        ("fact_episodes", "gold", "fact_episodes"),
    ]
])

display(spark.sql(union_query))