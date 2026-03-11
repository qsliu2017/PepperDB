--
-- SUBSELECT
-- Simplified from PostgreSQL's subselect.sql
--

SELECT 1 AS one WHERE 1 IN (SELECT 1);

SELECT 1 AS zero WHERE 1 NOT IN (SELECT 1);

SELECT 1 AS zero WHERE 1 IN (SELECT 2);

-- Check grammar's handling of extra parens in assorted contexts

SELECT * FROM (SELECT 1 AS x) ss;
SELECT * FROM ((SELECT 1 AS x)) ss;

SELECT * FROM ((SELECT 1 AS x)), ((SELECT * FROM ((SELECT 2 AS y))));

-- Set up some simple test tables

CREATE TABLE SUBSELECT_TBL (
  f1 integer,
  f2 integer,
  f3 double precision
);

INSERT INTO SUBSELECT_TBL VALUES (1, 2, 3);
INSERT INTO SUBSELECT_TBL VALUES (2, 3, 4);
INSERT INTO SUBSELECT_TBL VALUES (3, 4, 5);
INSERT INTO SUBSELECT_TBL VALUES (1, 1, 1);
INSERT INTO SUBSELECT_TBL VALUES (2, 2, 2);
INSERT INTO SUBSELECT_TBL VALUES (3, 3, 3);
INSERT INTO SUBSELECT_TBL VALUES (6, 7, 8);
INSERT INTO SUBSELECT_TBL VALUES (8, 9, NULL);

SELECT * FROM SUBSELECT_TBL;

-- Uncorrelated subselects

SELECT f1 AS "Constant Select" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT 1);

SELECT f1 AS "Uncorrelated Field" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL);

-- Correlated subselects

SELECT f1 AS "Correlated Field", f2 AS "Second Field"
  FROM SUBSELECT_TBL upper
  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL WHERE f1 = upper.f1);

SELECT f1 AS "Correlated Field", f3 AS "Second Field"
  FROM SUBSELECT_TBL upper
  WHERE f1 IN
    (SELECT f2 FROM SUBSELECT_TBL WHERE CAST(upper.f2 AS float) = f3);

--
-- Clean up
--

DROP TABLE SUBSELECT_TBL;
