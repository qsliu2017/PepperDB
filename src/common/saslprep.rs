//! Translated from PostgreSQL src/include/common/saslprep.h

/// Error codes for `pg_saslprep`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgSaslprepError {
    /// Out of memory (frontend only).
    Oom,
    /// Input is not a valid UTF-8 string.
    InvalidUtf8,
    /// Output would contain prohibited characters.
    Prohibited,
}

/// SASLprep-normalize `input`; returns the prepared string on success.
pub fn pg_saslprep(input: &str) -> Result<String, PgSaslprepError> {
    let _ = input;
    unimplemented!()
}
