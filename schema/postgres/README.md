# SQL storage schema

`v1.sql` is the canonical rrjj storage schema for PostgreSQL and CockroachDB.
Host applications can copy it into their migration system and rename the three
tables and event index as needed.

Start rrjj with the matching `--database-*-table` options. The default
`--database-schema-mode=validate` performs read-only `information_schema`
queries and verifies:

- required columns and no unexpected columns;
- data types and nullability;
- defaults required by rrjj writes; and
- primary-key columns and order.

The checks and event timestamp index in `v1.sql` remain part of the canonical
schema, but startup validation does not require their names or inspect their
expressions.
