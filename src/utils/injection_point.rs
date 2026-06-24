//! Translated from PostgreSQL src/include/utils/injection_point.h

/// Callback launched by an injection point. The void* private_data/arg are
/// opaque byte slices here; real state-capture is left to Phase 2.
pub type InjectionPointCallback = fn(name: &str, private_data: &[u8], arg: &mut [u8]);

// USE_INJECTION_POINTS is off for our build, so the macros are no-ops.
pub fn injection_point_load(name: &str) {
    let _ = name;
}

pub fn injection_point(name: &str) {
    let _ = name;
}

pub fn injection_point_cached(name: &str) {
    let _ = name;
}

pub fn is_injection_point_attached(name: &str) -> bool {
    let _ = name;
    false
}

pub fn injection_point_shmem_size() -> usize {
    unimplemented!()
}

pub fn injection_point_shmem_init() {
    unimplemented!()
}

pub fn injection_point_attach(
    name: &str,
    library: &str,
    function: &str,
    private_data: &[u8],
) {
    let _ = (name, library, function, private_data);
    unimplemented!()
}

pub fn injection_point_run(name: &str, arg: &mut [u8]) {
    let _ = (name, arg);
    unimplemented!()
}

pub fn injection_point_detach(name: &str) -> bool {
    let _ = name;
    unimplemented!()
}
