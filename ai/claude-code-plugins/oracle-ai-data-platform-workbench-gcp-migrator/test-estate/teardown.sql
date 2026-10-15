-- gcp-aidp test estate: remove everything seed.sql and MANUAL_STEPS.md created
-- in BigQuery. Drops only the `migration_test` dataset and its contents.
-- Delete the scheduled query and the Cloud Storage bucket from MANUAL_STEPS.md
-- separately (see the end of that file).
DROP SCHEMA IF EXISTS migration_test CASCADE;
