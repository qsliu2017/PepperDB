-- Boolean type
CREATE TABLE booltest (a boolean, b boolean);
INSERT INTO booltest VALUES (true, false);
INSERT INTO booltest VALUES (false, true);
INSERT INTO booltest VALUES (true, true);
SELECT * FROM booltest;
-- WHERE with boolean column
SELECT * FROM booltest WHERE a;
SELECT * FROM booltest WHERE NOT b;
-- Boolean literals in SELECT
SELECT true, false;
