-- GROUP BY and aggregate functions
CREATE TABLE sales (dept text, item text, amount int, qty int);
INSERT INTO sales VALUES ('electronics', 'phone', 500, 10);
INSERT INTO sales VALUES ('electronics', 'laptop', 1200, 5);
INSERT INTO sales VALUES ('electronics', 'phone', 500, 8);
INSERT INTO sales VALUES ('clothing', 'shirt', 30, 100);
INSERT INTO sales VALUES ('clothing', 'pants', 50, 60);
INSERT INTO sales VALUES ('clothing', 'shirt', 30, 80);
INSERT INTO sales VALUES ('food', 'apple', 2, 500);
INSERT INTO sales VALUES ('food', 'bread', 5, 200);

-- COUNT
SELECT COUNT(*) FROM sales;
SELECT dept, COUNT(*) FROM sales GROUP BY dept ORDER BY dept;

-- SUM
SELECT dept, SUM(qty) FROM sales GROUP BY dept ORDER BY dept;

-- AVG
SELECT dept, AVG(amount) FROM sales GROUP BY dept ORDER BY dept;

-- MIN / MAX
SELECT dept, MIN(amount), MAX(amount) FROM sales GROUP BY dept ORDER BY dept;

-- Multiple aggregates
SELECT dept, COUNT(*), SUM(qty), MIN(amount), MAX(amount) FROM sales GROUP BY dept ORDER BY dept;

-- GROUP BY multiple columns
SELECT dept, item, SUM(qty) FROM sales GROUP BY dept, item ORDER BY dept, item;

-- HAVING
SELECT dept, SUM(qty) FROM sales GROUP BY dept HAVING SUM(qty) > 100 ORDER BY dept;

-- Aggregate without GROUP BY (whole table)
SELECT SUM(qty), AVG(amount), MIN(qty), MAX(qty) FROM sales;

-- COUNT with column (excludes NULLs)
CREATE TABLE cn2 (a int, b int);
INSERT INTO cn2 VALUES (1, 10);
INSERT INTO cn2 VALUES (2, NULL);
INSERT INTO cn2 VALUES (3, 30);
SELECT COUNT(*), COUNT(b) FROM cn2;
