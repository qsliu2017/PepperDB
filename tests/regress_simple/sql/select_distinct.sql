--
-- SELECT_DISTINCT
-- Simplified from PostgreSQL's select_distinct.sql -- IS DISTINCT FROM only.
--

CREATE TABLE disttable (f1 integer);

INSERT INTO disttable VALUES(1);
INSERT INTO disttable VALUES(2);
INSERT INTO disttable VALUES(3);
INSERT INTO disttable VALUES(NULL);

-- basic cases
SELECT f1, f1 IS DISTINCT FROM 2 as "not 2" FROM disttable;

SELECT f1, f1 IS DISTINCT FROM NULL as "not null" FROM disttable;

SELECT f1, f1 IS DISTINCT FROM f1 as "false" FROM disttable;

SELECT f1, f1 IS DISTINCT FROM f1+1 as "not null" FROM disttable;

-- check that optimizer constant-folds it properly
SELECT 1 IS DISTINCT FROM 2 as "yes";

SELECT 2 IS DISTINCT FROM 2 as "no";

SELECT 2 IS DISTINCT FROM null as "yes";

SELECT null IS DISTINCT FROM null as "no";

-- negated form
SELECT 1 IS NOT DISTINCT FROM 2 as "no";

SELECT 2 IS NOT DISTINCT FROM 2 as "yes";

SELECT 2 IS NOT DISTINCT FROM null as "no";

SELECT null IS NOT DISTINCT FROM null as "yes";

--
-- Clean up
--

DROP TABLE disttable;
