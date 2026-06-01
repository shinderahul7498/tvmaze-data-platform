# Databricks notebook source
# COMMAND ----------

dbutils.widgets.dropdown(
    "entity_name",
    "ALL",
    ["ALL", "shows", "cast", "episodes"]
)

dbutils.widgets.dropdown(
    "layer",
    "ALL",
    ["ALL", "bronze", "silver", "gold"]
)

entity_name = dbutils.widgets.get("entity_name")
layer = dbutils.widgets.get("layer")

# COMMAND ----------

# COMMAND ----------

if layer in ["ALL", "bronze"]:

    bronze_entities = (
        ["shows", "cast", "episodes"]
        if entity_name == "ALL"
        else [entity_name]
    )

    for entity in bronze_entities:

        print(f"Starting Bronze Load : {entity}")

        dbutils.notebook.run(
            "../bronze/load_to_bronze",
            0,
            {
                "entity_name": entity
            }
        )
## Silver Layer

if layer in ["ALL", "silver"]:

    silver_notebooks = {
        "shows": "../silver/transform_s_shows",
        "cast": "../silver/transform_s_cast",
        "episodes": "../silver/transform_s_episodes"
    }

    entities = (
        silver_notebooks.keys()
        if entity_name == "ALL"
        else [entity_name]
    )

    for entity in entities:

        print(f"Starting Silver Load : {entity}")

        dbutils.notebook.run(
            silver_notebooks[entity],
            0
        )

## Gold Layer

if layer in ["ALL", "gold"]:

    gold_notebooks = {
        "shows": "../gold/load_dim_shows",
        "cast": "../gold/load_dim_cast",
        "episodes": "../gold/load_fact_episodes"
    }

    entities = (
        gold_notebooks.keys()
        if entity_name == "ALL"
        else [entity_name]
    )

    for entity in entities:

        print(f"Starting Gold Load : {entity}")

        dbutils.notebook.run(
            gold_notebooks[entity],
            0
        )

print("TVMaze Pipeline Completed Successfully")