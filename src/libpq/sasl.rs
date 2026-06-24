//! Translated from PostgreSQL src/include/libpq/sasl.h
//
// Backend SASL mechanism interface. The pg_be_sasl_mech routine struct (a vtable of
// callbacks) -> a trait (routine-struct.md). Mechanisms are a closed set (SCRAM,
// plus OAuth), dispatched statically.

use crate::libpq::libpq_be::Port;

// Status codes for message exchange -> enum (sequential ordinals, returned by exchange()).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgSaslExchangeStatus {
    Continue = 0, // PG_SASL_EXCHANGE_CONTINUE
    Success = 1,  // PG_SASL_EXCHANGE_SUCCESS
    Failure = 2,  // PG_SASL_EXCHANGE_FAILURE
}

pub const PG_SASL_EXCHANGE_CONTINUE: i32 = 0;
pub const PG_SASL_EXCHANGE_SUCCESS: i32 = 1;
pub const PG_SASL_EXCHANGE_FAILURE: i32 = 2;

// Max accepted size of SASL messages.
pub const PG_MAX_SASL_MESSAGE_LENGTH: i32 = 1024;

// Output of a SASL exchange() step: the next/outcome message (None = nothing to
// send) plus an optional server-log DETAIL string. Folds C's char **output /
// int *outputlen / const char **logdetail out-params into the return.
pub struct SaslExchangeOutput {
    pub output: Option<Vec<u8>>,
    pub logdetail: Option<String>,
}

// pg_be_sasl_mech vtable -> trait. `init()`'s opaque `void *` state becomes the
// trait's associated State type (one real type per mechanism). max_message_length
// is a per-mechanism constant.
pub trait BeSaslMech {
    // Per-connection mechanism state (init()'s returned void *).
    type State;

    // The maximum size allowed for client SASLResponses.
    const MAX_MESSAGE_LENGTH: i32;

    // Append supported mechanism names (each '\0'-terminated) into `buf`.
    // StringInfo -> &mut Vec<u8>.
    fn get_mechanisms(&self, port: &Port, buf: &mut Vec<u8>);

    // Initialize per-connection state. shadow_pass = stored secret or None.
    fn init(&self, port: &Port, mech: &str, shadow_pass: Option<&str>) -> Self::State;

    // Produce a server challenge. `input` = client response (None if client-first
    // with no initial response). Returns the exchange status + output/logdetail.
    fn exchange(
        &self,
        state: &mut Self::State,
        input: Option<&[u8]>,
    ) -> (PgSaslExchangeStatus, SaslExchangeOutput);
}

// Common implementation for auth.c. Returns the exchange status; logdetail folded
// into the Result/output at the boundary. Kept as a stub.
pub fn CheckSASLAuth(
    mech: &impl BeSaslMech,
    port: &mut Port,
    shadow_pass: Option<&str>,
) -> (i32, Option<String>) {
    unimplemented!()
}
