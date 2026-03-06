-- Column aliases
SELECT 1 AS one;
SELECT 1 + 2 AS sum;

CREATE TABLE al (x int, y int);
INSERT INTO al VALUES (10, 20);
INSERT INTO al VALUES (30, 40);

-- Column alias in SELECT
SELECT x AS first_col, y AS second_col FROM al ORDER BY x;

-- Expression alias
SELECT x + y AS total FROM al ORDER BY x;

-- Alias does not affect ORDER BY (use original column name)
SELECT x AS a FROM al ORDER BY x;
