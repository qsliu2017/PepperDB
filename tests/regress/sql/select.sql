-- Column selection, WHERE filtering, expressions in target list
CREATE TABLE t1 (a int, b int);
INSERT INTO t1 VALUES (1, 10);
INSERT INTO t1 VALUES (2, 20);
INSERT INTO t1 VALUES (3, 30);
-- Column selection
SELECT a FROM t1;
SELECT b, a FROM t1;
SELECT * FROM t1;
-- WHERE filtering
SELECT * FROM t1 WHERE a = 2;
SELECT * FROM t1 WHERE b > 15;
SELECT * FROM t1 WHERE a >= 2 AND b <= 25;
-- Expressions in target list
SELECT a, b, a + b FROM t1;
SELECT a * 2 FROM t1;
