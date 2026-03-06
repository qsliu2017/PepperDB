-- Smallint (int2) type
CREATE TABLE int2test (a smallint, b smallint);
INSERT INTO int2test VALUES (1, 100);
INSERT INTO int2test VALUES (-32768, 32767);
INSERT INTO int2test VALUES (0, 0);
SELECT * FROM int2test;
SELECT a + b FROM int2test WHERE a = 1;
SELECT * FROM int2test ORDER BY a;
