-- Float types (real / double precision)
CREATE TABLE floattest (a real, b double precision);
INSERT INTO floattest VALUES (1.5, 3.14);
INSERT INTO floattest VALUES (-2.5, 0.0);
INSERT INTO floattest VALUES (0.0, 100.001);
SELECT * FROM floattest;
SELECT a + b FROM floattest WHERE a > 0.0;
SELECT * FROM floattest ORDER BY b;
-- Arithmetic
SELECT 1.5 + 2.5;
SELECT 10.0 / 3.0;
