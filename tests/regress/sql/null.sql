-- NULL handling
CREATE TABLE nulltest (a int, b int);
INSERT INTO nulltest VALUES (1, 10);
INSERT INTO nulltest VALUES (2, NULL);
INSERT INTO nulltest VALUES (NULL, 30);
SELECT * FROM nulltest;
-- IS NULL / IS NOT NULL
SELECT * FROM nulltest WHERE b IS NULL;
SELECT * FROM nulltest WHERE a IS NOT NULL;
-- NULL propagation in arithmetic
SELECT a + b FROM nulltest;
-- NULL comparison (NULL = NULL is NULL, not true)
SELECT * FROM nulltest WHERE a = NULL;
