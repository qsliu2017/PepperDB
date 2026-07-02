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
--   * \getenv libdir/dlsuffix + \set regresslib and the C `regresslib`
--     extension bits -- no C-function loading. (\getenv abs_srcdir IS kept:
--     the COPY fixtures below use it, and pg_regress exports PG_ABS_SRCDIR.)
--   * GRANT ALL ON SCHEMA public TO public
--     -- "syntax error at or near SCHEMA" (GRANT ... ON SCHEMA not parsed).
--   * CREATE TABLESPACE regress_tblspace
--     -- accepted, but unused by this subset; dropped to keep setup minimal.
--   * FLOAT8_TBL data
--     -- float8 input of non-integral literals is still blocked by the numeric
--     step (12); INSERT crashes on the float literal path. Restored with rows
--     when step 12 lands.
--   * POINT_TBL / geometry, person/emp/student/stud_emp/road/ihighway/shighway
--     and every INHERITS table -- geometry types + table inheritance unsupported.
--   * CREATE TYPE ... AS ENUM / AS RANGE -- enum + range types unsupported.
--   * CREATE FUNCTION ... LANGUAGE C / sql / SQL-body -- executable functions
--     (binary_coercible, part_hash*, fipshash) unsupported (owned by the
--     PL/pgSQL + SQL-function step).
--   * CREATE OPERATOR CLASS ... USING hash -- operator/opclass DDL unsupported.
--   * VACUUM / VACUUM ANALYZE after each fixture -- upstream runs them for
--     plan stability; re-add per-fixture when a test's plan output needs it.
--
-- KEPT (loads cleanly): INT2_TBL, INT4_TBL, TEXT_TBL, CHAR_TBL, VARCHAR_TBL,
-- the COPY-loaded onek/tenk1 families (onek2/tenk2 via CTAS), and an EMPTY
-- INT8_TBL (rows blocked on int8 literal input, step 12).
--

-- directory paths are passed to us in environment variables (pg_regress sets
-- PG_ABS_SRCDIR to the absolute --inputdir; run.sh passes the submodule path).
\getenv abs_srcdir PG_ABS_SRCDIR

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

-- INT8_TBL: created EMPTY -- the upstream INSERT is blocked on int8 input of
-- unquoted big integer literals ("numeric literal ... not supported yet",
-- owned by the numeric step 12). The relation exists so tests that only need
-- it to resolve do; rows come back with step 12.
CREATE TABLE INT8_TBL(q1 int8, q2 int8);

CREATE TABLE TEXT_TBL (f1 text);

INSERT INTO TEXT_TBL VALUES
  ('doh!'),
  ('hi de ho neighbor');

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

CREATE TABLE onek (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);

\set filename :abs_srcdir '/data/onek.data'
COPY onek FROM :'filename';

CREATE TABLE onek2 AS SELECT * FROM onek;

CREATE TABLE tenk1 (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);

\set filename :abs_srcdir '/data/tenk.data'
COPY tenk1 FROM :'filename';

CREATE TABLE tenk2 AS SELECT * FROM tenk1;
