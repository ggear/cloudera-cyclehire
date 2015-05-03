--
-- Cyclehire Database Grants
--

CREATE ROLE ${hivevar:${hivevar:cyclehire.user};
GRANT ALL ON SERVER ${hivevar:cyclehire.server.name} TO ROLE ${hivevar:${hivevar:cyclehire.user};
GRANT ALL ON DATABASE ${hivevar:cyclehire.database.name} TO ROLE ${hivevar:${hivevar:cyclehire.user};
GRANT ROLE ${hivevar:${hivevar:cyclehire.user} TO GROUP ${hivevar:${hivevar:cyclehire.user};
