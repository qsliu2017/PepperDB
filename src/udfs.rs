//! PostgreSQL-compatible UDFs registered with DataFusion.

use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

/// Register all PG-compatible UDFs on the session.
pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(BoolEq::new()));
    ctx.register_udf(ScalarUDF::from(BoolNe::new()));
    ctx.register_udf(ScalarUDF::from(PgInputIsValid::new()));
}

// Helper: convert ColumnarValue args to arrays
fn args_to_arrays(
    args: &[ColumnarValue],
    num_rows: usize,
) -> datafusion::error::Result<Vec<Arc<dyn Array>>> {
    args.iter()
        .map(|a| match a {
            ColumnarValue::Array(arr) => Ok(arr.clone()),
            ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows),
        })
        .collect()
}

// -- booleq -----------------------------------------------------------------

#[derive(Debug)]
struct BoolEq {
    sig: Signature,
}

impl BoolEq {
    fn new() -> Self {
        Self {
            sig: Signature::new(
                TypeSignature::Exact(vec![DataType::Boolean, DataType::Boolean]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for BoolEq {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "booleq"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        num_rows: usize,
    ) -> datafusion::error::Result<ColumnarValue> {
        let arrays = args_to_arrays(args, num_rows)?;
        let a = arrays[0].as_any().downcast_ref::<BooleanArray>().unwrap();
        let b = arrays[1].as_any().downcast_ref::<BooleanArray>().unwrap();
        let result: BooleanArray = a
            .iter()
            .zip(b.iter())
            .map(|(x, y)| match (x, y) {
                (Some(x), Some(y)) => Some(x == y),
                _ => None,
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// -- boolne -----------------------------------------------------------------

#[derive(Debug)]
struct BoolNe {
    sig: Signature,
}

impl BoolNe {
    fn new() -> Self {
        Self {
            sig: Signature::new(
                TypeSignature::Exact(vec![DataType::Boolean, DataType::Boolean]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for BoolNe {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "boolne"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        num_rows: usize,
    ) -> datafusion::error::Result<ColumnarValue> {
        let arrays = args_to_arrays(args, num_rows)?;
        let a = arrays[0].as_any().downcast_ref::<BooleanArray>().unwrap();
        let b = arrays[1].as_any().downcast_ref::<BooleanArray>().unwrap();
        let result: BooleanArray = a
            .iter()
            .zip(b.iter())
            .map(|(x, y)| match (x, y) {
                (Some(x), Some(y)) => Some(x != y),
                _ => None,
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

// -- pg_input_is_valid ------------------------------------------------------

#[derive(Debug)]
struct PgInputIsValid {
    sig: Signature,
}

impl PgInputIsValid {
    fn new() -> Self {
        Self {
            sig: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for PgInputIsValid {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "pg_input_is_valid"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        num_rows: usize,
    ) -> datafusion::error::Result<ColumnarValue> {
        let arrays = args_to_arrays(args, num_rows)?;
        let values = arrays[0].as_any().downcast_ref::<StringArray>().unwrap();
        let types = arrays[1].as_any().downcast_ref::<StringArray>().unwrap();
        let result: BooleanArray = values
            .iter()
            .zip(types.iter())
            .map(|(val, typ)| match (val, typ) {
                (Some(v), Some(t)) => Some(validate_input(v, t)),
                _ => None,
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

/// Check if a string value is valid for a given PG type name (public for executor).
pub fn validate_input_public(value: &str, type_name: &str) -> bool {
    validate_input(value, type_name)
}

/// Check if a string value is valid for a given PG type name.
fn validate_input(value: &str, type_name: &str) -> bool {
    match type_name {
        "bool" | "boolean" => {
            let t = value.trim().to_ascii_lowercase();
            matches!(
                t.as_str(),
                "t" | "true"
                    | "y"
                    | "yes"
                    | "on"
                    | "1"
                    | "f"
                    | "false"
                    | "n"
                    | "no"
                    | "off"
                    | "of"
                    | "0"
            )
        }
        "int2" | "smallint" => value.trim().parse::<i16>().is_ok(),
        "int4" | "integer" | "int" => value.trim().parse::<i32>().is_ok(),
        "int8" | "bigint" => value.trim().parse::<i64>().is_ok(),
        "float4" | "real" => value.trim().parse::<f32>().is_ok(),
        "float8" | "double precision" => value.trim().parse::<f64>().is_ok(),
        _ => true,
    }
}
