--
-- Schema Create
--

CREATE TABLE IF NOT EXISTS somedata (
	col1 INT,
	col2 INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
