//! Translated from PostgreSQL src/include/common/percentrepl.h

/// Replace `%`-placeholders in `instr`. `letters[i]` maps to `values[i]`.
/// The C variadic (letter, value) pairs become parallel slices.
pub fn replace_percent_placeholders(
    instr: &str,
    param_name: &str,
    letters: &str,
    values: &[&str],
) -> String {
    let _ = (instr, param_name, letters, values);
    unimplemented!()
}
