CREATE CATALOG IF NOT EXISTS dev_tvmaze;

GRANT USE CATALOG
ON CATALOG dev_tvmaze
TO `account users`;

GRANT USE SCHEMA
ON SCHEMA dev_tvmaze_gold
TO `account users`;

GRANT SELECT
ON TABLE dev_tvmaze_gold.dim_shows
TO `account users`;

GRANT SELECT
ON TABLE dev_tvmaze_gold.dim_cast
TO `account users`;

GRANT SELECT
ON TABLE dev_tvmaze_gold.fact_episodes
TO `account users`;