--
-- TRUNCATE
-- Test basic TRUNCATE TABLE functionality.
--

CREATE TABLE trunctest (a integer, b text);

INSERT INTO trunctest VALUES (1, 'one');
INSERT INTO trunctest VALUES (2, 'two');
INSERT INTO trunctest VALUES (3, 'three');

SELECT * FROM trunctest;

TRUNCATE trunctest;

SELECT * FROM trunctest;

-- verify table is usable after truncate
INSERT INTO trunctest VALUES (4, 'four');

SELECT * FROM trunctest;

-- TRUNCATE TABLE (explicit TABLE keyword)
TRUNCATE TABLE trunctest;

SELECT * FROM trunctest;

DROP TABLE trunctest;
