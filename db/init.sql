-- Runs once, on first container init, alongside the POSTGRES_DB-created
-- `rateguard` database. `rateguard_test` mirrors Redis's separate DB-index-1
-- test isolation, but as a genuinely separate database rather than a
-- convention the app has to honor -- a stronger isolation boundary.
CREATE DATABASE rateguard_test;
