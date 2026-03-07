-- CREATE INDEX basic tests

CREATE TABLE idx_test (id INT, val TEXT);

INSERT INTO idx_test VALUES (3, 'three');
INSERT INTO idx_test VALUES (1, 'one');
INSERT INTO idx_test VALUES (2, 'two');
INSERT INTO idx_test VALUES (5, 'five');
INSERT INTO idx_test VALUES (4, 'four');

-- Create index on existing data
CREATE INDEX idx_test_id ON idx_test (id);

-- Verify data is still accessible after index creation
SELECT * FROM idx_test ORDER BY id;

-- Insert more data (should maintain index)
INSERT INTO idx_test VALUES (6, 'six');
INSERT INTO idx_test VALUES (0, 'zero');

SELECT * FROM idx_test ORDER BY id;

-- Update a row (should maintain index)
UPDATE idx_test SET val = 'ONE' WHERE id = 1;

SELECT * FROM idx_test ORDER BY id;

-- Delete a row
DELETE FROM idx_test WHERE id = 3;

SELECT * FROM idx_test ORDER BY id;

-- Create index on text column
CREATE INDEX idx_test_val ON idx_test (val);

SELECT * FROM idx_test ORDER BY val;

-- Drop table (should clean up indexes)
DROP TABLE idx_test;
