# TVMaze Architecture

## Bronze

Raw API ingestion.

Tables:

- b_shows
- b_cast
- b_episodes

## Silver

Business transformations.

Tables:

- s_shows
- s_cast
- s_episodes

## Gold

Reporting tables.

Tables:

- dim_shows
- dim_cast
- fact_episodes

## Incremental Strategy

Shows:
updated timestamp based CDC

Cast:
person_updated timestamp based CDC

Episodes:
LEFT ANTI JOIN on episode_id

## Schema Evolution

Implemented using Delta mergeSchema.