--
-- Cyclehire Database Create
--

CREATE DATABASE IF NOT EXISTS ${hivevar:cyclehire.database.name}
COMMENT 'TFL Cyclehire database'
LOCATION '${hivevar:cyclehire.database.location}';
