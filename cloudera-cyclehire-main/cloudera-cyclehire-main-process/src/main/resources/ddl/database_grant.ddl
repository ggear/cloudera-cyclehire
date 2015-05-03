--
-- Cyclehire Database Grants
--

CREATE ROLE ${hivevar:cyclehire.user};
GRANT ALL ON SERVER ${hivevar:cyclehire.server.name} TO ROLE ${hivevar:cyclehire.user};
GRANT ALL ON DATABASE ${hivevar:cyclehire.database.name} TO ROLE ${hivevar:cyclehire.user};
GRANT ROLE ${hivevar:cyclehire.user} TO GROUP ${hivevar:cyclehire.user};
