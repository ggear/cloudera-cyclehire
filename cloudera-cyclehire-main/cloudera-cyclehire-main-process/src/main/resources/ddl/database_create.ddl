--
-- Cyclehire Database Create
--

CREATE DATABASE IF NOT EXISTS cyclehire
COMMENT 'TFL Cyclehire database'
LOCATION '${hiveconf:cyclehire.database.location}';
