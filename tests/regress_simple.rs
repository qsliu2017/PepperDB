//! Simplified PostgreSQL regression tests.
//!
//! Each test contains a curated subset of a PostgreSQL regression test,
//! including only the parts PepperDB can currently handle (basic DDL/DML,
//! SELECT via DataFusion). The SQL and expected output live in
//! tests/regress_simple/sql/ and tests/regress_simple/expected/.

mod common;

async fn run_simple_test(name: &str) {
    common::run_test("tests/regress_simple", name, &[]).await;
}

macro_rules! simple_test {
    ($($name:ident),* $(,)?) => {
        $( #[tokio::test] async fn $name() { run_simple_test(stringify!($name)).await; } )*
    };
}

simple_test!(case, select_distinct, subselect, truncate,);
