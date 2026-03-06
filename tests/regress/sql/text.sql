-- Text and varchar types
CREATE TABLE texttest (a text, b varchar);
INSERT INTO texttest VALUES ('hello', 'world');
INSERT INTO texttest VALUES ('foo', 'bar');
INSERT INTO texttest VALUES ('abc', 'xyz');
SELECT * FROM texttest;
SELECT * FROM texttest WHERE a = 'foo';
SELECT * FROM texttest ORDER BY a;
SELECT b FROM texttest ORDER BY b DESC;
