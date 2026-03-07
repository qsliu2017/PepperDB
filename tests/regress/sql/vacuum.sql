-- VACUUM tests
CREATE TABLE vac_test (id INT, val TEXT);
INSERT INTO vac_test VALUES (1, 'keep'), (2, 'delete'), (3, 'keep');
DELETE FROM vac_test WHERE id = 2;

-- Before vacuum, data is correct
SELECT * FROM vac_test ORDER BY id;

-- Run vacuum on specific table
VACUUM vac_test;

-- After vacuum, same data visible
SELECT * FROM vac_test ORDER BY id;

-- Vacuum all tables
VACUUM;

SELECT * FROM vac_test ORDER BY id;
