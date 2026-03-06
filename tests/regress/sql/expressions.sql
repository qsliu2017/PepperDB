-- Type casting, operator precedence, parentheses
-- CAST
SELECT CAST(42 AS bigint);
SELECT CAST(3.14 AS int);
SELECT CAST(1 AS text);

-- PostgreSQL :: cast syntax
SELECT 42::bigint;
SELECT 100::smallint;

-- Operator precedence: * before +
SELECT 2 + 3 * 4;
SELECT (2 + 3) * 4;

-- Nested parentheses
SELECT ((1 + 2) * (3 + 4));

-- Mixed arithmetic
SELECT 10 / 3;
SELECT 10 % 3;
SELECT -5 + 3;
SELECT -(5 + 3);
