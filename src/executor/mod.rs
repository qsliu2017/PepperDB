use futures::stream;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::Type;
use pgwire::error::PgWireResult;
use std::sync::Arc;

use crate::parser::{Expr, Statement};

pub fn execute(stmt: Statement) -> PgWireResult<Response<'static>> {
    match stmt {
        Statement::Select(exprs) => execute_select(exprs),
    }
}

fn execute_select(exprs: Vec<Expr>) -> PgWireResult<Response<'static>> {
    let fields: Vec<FieldInfo> = exprs
        .iter()
        .map(|expr| {
            let ty = match expr {
                Expr::Integer(_) => Type::INT4,
                Expr::StringLiteral(_) => Type::TEXT,
            };
            FieldInfo::new("?column?".to_owned(), None, None, ty, FieldFormat::Text)
        })
        .collect();

    let schema = Arc::new(fields);

    let mut encoder = DataRowEncoder::new(schema.clone());
    for expr in &exprs {
        match expr {
            Expr::Integer(i) => encoder.encode_field(&i)?,
            Expr::StringLiteral(s) => encoder.encode_field(&s)?,
        }
    }
    let row = encoder.finish();

    Ok(Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![row]),
    )))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn execute_select_integer() {
        let stmt = Statement::Select(vec![Expr::Integer(1)]);
        let resp = execute(stmt).unwrap();
        assert!(matches!(resp, Response::Query(_)));
    }
}
