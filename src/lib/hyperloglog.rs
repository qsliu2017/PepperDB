//! Translated from PostgreSQL src/include/lib/hyperloglog.h
//!
//! Approximate distinct-count estimator using fixed memory. In-memory; the
//! `uint8 *hashesArr` register array becomes a `Vec<u8>`.

/// HyperLogLog estimator state.
pub struct hyperLogLogState {
    pub registerWidth: u8, // register width, in bits ("k")
    pub nRegisters: usize, // number of registers
    pub alphaMM: f64,      // alpha * m^2
    pub hashesArr: Vec<u8>, // register array
    pub arrSize: usize,    // size of hashesArr
}

/// initHyperLogLog: initialize with the given register-width parameter.
pub fn initHyperLogLog(_cState: &mut hyperLogLogState, _bwidth: u8) {
    unimplemented!()
}

/// initHyperLogLogError: initialize sized for the given relative error.
pub fn initHyperLogLogError(_cState: &mut hyperLogLogState, _error: f64) {
    unimplemented!()
}

/// addHyperLogLog: incorporate one 32-bit hash.
pub fn addHyperLogLog(_cState: &mut hyperLogLogState, _hash: u32) {
    unimplemented!()
}

/// estimateHyperLogLog: estimate the cardinality.
pub fn estimateHyperLogLog(_cState: &hyperLogLogState) -> f64 {
    unimplemented!()
}

/// freeHyperLogLog: free the register array (RAII; provided for parity).
pub fn freeHyperLogLog(_cState: &mut hyperLogLogState) {
    unimplemented!()
}
