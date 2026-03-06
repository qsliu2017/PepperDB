-- Comprehensive int4 arithmetic, comparison, modulo, unary minus
CREATE TABLE i4 (a int, b int);
INSERT INTO i4 VALUES (10, 3);
INSERT INTO i4 VALUES (-7, 2);
INSERT INTO i4 VALUES (0, 5);

-- Arithmetic
SELECT a + b FROM i4 ORDER BY a;
SELECT a - b FROM i4 ORDER BY a;
SELECT a * b FROM i4 ORDER BY a;
SELECT a / b FROM i4 ORDER BY a;
SELECT a % b FROM i4 ORDER BY a;

-- Unary minus
SELECT -a FROM i4 ORDER BY a;

-- Comparison
SELECT * FROM i4 WHERE a > 0 ORDER BY a;
SELECT * FROM i4 WHERE a <= 0 ORDER BY a;

-- Expression in SELECT
SELECT a, b, a + b, a * b, a % b FROM i4 ORDER BY a;
