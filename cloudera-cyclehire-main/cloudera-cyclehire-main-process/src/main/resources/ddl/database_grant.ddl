--
-- Cyclehire Database Grants
--

CREATE ROLE hive;
GRANT ALL ON SERVER server1 TO ROLE hive;
GRANT ALL ON DATABASE cyclehire TO ROLE hive;
GRANT ROLE hive TO GROUP hive;
