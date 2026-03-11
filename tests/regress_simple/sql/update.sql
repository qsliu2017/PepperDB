--
-- UPDATE
-- Test basic UPDATE functionality.
--

CREATE TABLE update_test (
    a   INT,
    b   INT,
    c   TEXT
);

INSERT INTO update_test VALUES (5, 10, 'foo');
INSERT INTO update_test VALUES (10, 15, 'bar');
INSERT INTO update_test VALUES (20, 25, 'baz');

SELECT * FROM update_test ORDER BY a;

-- update all rows
UPDATE update_test SET c = 'updated';

SELECT * FROM update_test ORDER BY a;

-- update with WHERE clause
UPDATE update_test SET b = 100 WHERE a = 5;

SELECT * FROM update_test ORDER BY a;

-- update with expression
UPDATE update_test SET a = a + 1;

SELECT * FROM update_test ORDER BY a;

-- update multiple columns
UPDATE update_test SET a = 0, b = 0, c = 'reset' WHERE a = 11;

SELECT * FROM update_test ORDER BY a;

-- update with no matching rows
UPDATE update_test SET a = 999 WHERE a = 12345;

SELECT * FROM update_test ORDER BY a;

DROP TABLE update_test;
