-- String operations: comparison, concatenation
CREATE TABLE s (a text, b text);
INSERT INTO s VALUES ('hello', 'world');
INSERT INTO s VALUES ('abc', 'def');
INSERT INTO s VALUES ('zzz', 'aaa');

-- Ordering
SELECT * FROM s ORDER BY a;

-- Comparison
SELECT * FROM s WHERE a > 'b' ORDER BY a;
SELECT * FROM s WHERE a = 'abc';

-- Concatenation with ||
SELECT 'hello' || ' ' || 'world';
SELECT a || ' ' || b FROM s ORDER BY a;
