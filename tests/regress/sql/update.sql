-- UPDATE statement
CREATE TABLE upd (a int, b int);
INSERT INTO upd VALUES (1, 10);
INSERT INTO upd VALUES (2, 20);
INSERT INTO upd VALUES (3, 30);
-- SET single column with WHERE
UPDATE upd SET b = 99 WHERE a = 2;
SELECT * FROM upd ORDER BY a;
-- SET with expression
UPDATE upd SET b = b + 100 WHERE a = 1;
SELECT * FROM upd ORDER BY a;
-- SET multiple columns
UPDATE upd SET a = 10, b = 200 WHERE a = 3;
SELECT * FROM upd ORDER BY a;
-- No-match update
UPDATE upd SET b = 0 WHERE a = 999;
SELECT * FROM upd ORDER BY a;
-- Update all rows
UPDATE upd SET b = 0;
SELECT * FROM upd ORDER BY a;
