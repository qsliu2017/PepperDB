--
-- TEST_SETUP (PepperDB trimmed) --- shared fixtures for the pg_regress subset.
--
-- This is a REDUCED copy of ref/postgres/src/test/regress/sql/test_setup.sql.
-- pg_regress runs `test_setup` first in the schedule; the harness (run.sh)
-- points the `test_setup` slot at THIS file (via the outputdir-first override
-- pg_regress applies to sql/NAME.sql) so only fixtures the current PepperDB
-- server can actually build are seeded. Every removal below is because the
-- server rejects or mis-handles the upstream statement TODAY; each is owned by
-- a later plan-004 step and will be restored as the server grows.
--
-- REMOVED categories (verified empirically against the running server):
--   * \getenv / \set regresslib and the C `regresslib` extension bits
--     -- no C-function loading; COPY-from-file and dlsuffix unused here.
--   * GRANT ALL ON SCHEMA public TO public
--     -- "syntax error at or near SCHEMA" (GRANT ... ON SCHEMA not parsed).
--   * CREATE TABLESPACE regress_tblspace
--     -- accepted, but unused by this subset; dropped to keep setup minimal.
--   * CHAR_TBL / VARCHAR_TBL (char(4) / varchar(4))
--     -- "syntax error at or near char(4)" -- parameterized char/varchar typmod
--     not parsed yet (owned by the string/varlena step).
--   * FLOAT8_TBL, TEXT_TBL, INT8_TBL data
--     -- INSERT fails ("cache lookup failed for type ... syscache not warm";
--     float literal crashes the backend; int8 multi-literal VALUES corrupts).
--     The empty tables are created where a later test only needs the relation
--     to exist; rows come back when the input path is fixed.
--   * POINT_TBL / geometry, person/emp/student/stud_emp/road/ihighway/shighway
--     and every INHERITS table -- geometry types + table inheritance unsupported.
--   * onek / onek2 / tenk1 / tenk2 (COPY ... FROM :'filename')
--     -- \set filename + COPY-from-file fixtures; large data, not loadable yet.
--   * CREATE TYPE ... AS ENUM / AS RANGE -- enum + range types unsupported.
--   * CREATE FUNCTION ... LANGUAGE C / sql / SQL-body -- executable functions
--     (binary_coercible, part_hash*, fipshash) unsupported (owned by the
--     PL/pgSQL + SQL-function step).
--   * CREATE OPERATOR CLASS ... USING hash -- operator/opclass DDL unsupported.
--
-- KEPT: the plain relational integer fixtures the current subset relies on
-- (INT2_TBL, INT4_TBL), which the server builds and populates cleanly.
--

SET synchronous_commit = on;

CREATE TABLE INT2_TBL(f1 int2);

INSERT INTO INT2_TBL(f1) VALUES
  ('0   '),
  ('  1234 '),
  ('    -1234'),
  ('32767'),  -- largest and smallest values
  ('-32767');

CREATE TABLE INT4_TBL(f1 int4);

INSERT INTO INT4_TBL(f1) VALUES
  ('   0  '),
  ('123456     '),
  ('    -123456'),
  ('2147483647'),  -- largest and smallest values
  ('-2147483647');

-- char/varchar fixtures (char(4)/varchar(4)): the string step (10) parses the
-- parameterized typmod, so these are restored from upstream test_setup.sql. The
-- char.sql / varchar.sql tests DROP the TEMP tables they build, then reference
-- these.
CREATE TABLE CHAR_TBL(f1 char(4));

INSERT INTO CHAR_TBL (f1) VALUES
  ('a'),
  ('ab'),
  ('abcd'),
  ('abcd    ');

CREATE TABLE VARCHAR_TBL(f1 varchar(4));

INSERT INTO VARCHAR_TBL (f1) VALUES
  ('a'),
  ('ab'),
  ('abcd'),
  ('abcd    ');
