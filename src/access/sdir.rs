//! Translated from PostgreSQL src/include/access/sdir.h

/// Direction for scanning a table or an index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ScanDirection {
    Backward = -1,
    NoMovement = 0,
    Forward = 1,
}

/// Net effect of two direction specifications (relies on +1/-1 encoding).
pub const fn scan_direction_combine(a: ScanDirection, b: ScanDirection) -> i32 {
    (a as i32) * (b as i32)
}

pub const fn scan_direction_is_valid(direction: ScanDirection) -> bool {
    matches!(
        direction,
        ScanDirection::Backward | ScanDirection::NoMovement | ScanDirection::Forward
    )
}

pub const fn scan_direction_is_backward(direction: ScanDirection) -> bool {
    matches!(direction, ScanDirection::Backward)
}

pub const fn scan_direction_is_no_movement(direction: ScanDirection) -> bool {
    matches!(direction, ScanDirection::NoMovement)
}

pub const fn scan_direction_is_forward(direction: ScanDirection) -> bool {
    matches!(direction, ScanDirection::Forward)
}
