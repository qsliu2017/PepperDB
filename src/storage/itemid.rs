//! Translated from PostgreSQL src/include/storage/itemid.h

/// A line pointer on a buffer page.
///
/// On-disk C bitfield `lp_off:15, lp_flags:2, lp_len:15` packed into one 32-bit
/// word. On little-endian the first field occupies the low bits:
///   bits  0..14 = lp_off, bits 15..16 = lp_flags, bits 17..31 = lp_len.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct ItemIdData(pub u32);

const _: () = assert!(core::mem::size_of::<ItemIdData>() == 4);
const _: () = assert!(core::mem::align_of::<ItemIdData>() == 4);

/// lp_flags states. UNUSED is available for re-use; others are not.
pub const LP_UNUSED: u8 = 0; // unused (should always have lp_len=0)
pub const LP_NORMAL: u8 = 1; // used (should always have lp_len>0)
pub const LP_REDIRECT: u8 = 2; // HOT redirect (should have lp_len=0)
pub const LP_DEAD: u8 = 3; // dead, may or may not have storage

/// Item offsets/lengths when not stored in an ItemIdData.
pub type ItemOffset = u16;
pub type ItemLength = u16;

const OFF_BITS: u32 = 15;
const FLAGS_BITS: u32 = 2;
const OFF_MASK: u32 = (1 << OFF_BITS) - 1; // 0x7fff
const FLAGS_MASK: u32 = (1 << FLAGS_BITS) - 1; // 0x3
const LEN_MASK: u32 = (1 << 15) - 1; // 0x7fff
const FLAGS_SHIFT: u32 = OFF_BITS; // 15
const LEN_SHIFT: u32 = OFF_BITS + FLAGS_BITS; // 17

impl ItemIdData {
    pub const fn lp_off(self) -> u16 {
        (self.0 & OFF_MASK) as u16
    }

    pub const fn lp_flags(self) -> u8 {
        ((self.0 >> FLAGS_SHIFT) & FLAGS_MASK) as u8
    }

    pub const fn lp_len(self) -> u16 {
        ((self.0 >> LEN_SHIFT) & LEN_MASK) as u16
    }

    pub(crate) const fn set_off(&mut self, off: u16) {
        self.0 = (self.0 & !OFF_MASK) | ((off as u32) & OFF_MASK);
    }

    pub(crate) const fn set_flags(&mut self, flags: u8) {
        self.0 = (self.0 & !(FLAGS_MASK << FLAGS_SHIFT))
            | (((flags as u32) & FLAGS_MASK) << FLAGS_SHIFT);
    }

    pub(crate) const fn set_len(&mut self, len: u16) {
        self.0 = (self.0 & !(LEN_MASK << LEN_SHIFT)) | (((len as u32) & LEN_MASK) << LEN_SHIFT);
    }

    /// True iff item identifier is in use.
    pub const fn is_used(self) -> bool {
        self.lp_flags() != LP_UNUSED
    }

    /// True iff item identifier is in state NORMAL.
    pub const fn is_normal(self) -> bool {
        self.lp_flags() == LP_NORMAL
    }

    /// True iff item identifier is in state REDIRECT.
    pub const fn is_redirected(self) -> bool {
        self.lp_flags() == LP_REDIRECT
    }

    /// True iff item identifier is in state DEAD.
    pub const fn is_dead(self) -> bool {
        self.lp_flags() == LP_DEAD
    }

    /// True iff item identifier has associated storage.
    pub const fn has_storage(self) -> bool {
        self.lp_len() != 0
    }

    /// In a REDIRECT pointer, lp_off holds offset number for next line pointer.
    pub const fn get_redirect(self) -> u16 {
        self.lp_off()
    }

    /// Set the item identifier to be UNUSED, with no storage.
    pub const fn set_unused(&mut self) {
        self.set_flags(LP_UNUSED);
        self.set_off(0);
        self.set_len(0);
    }

    /// Set the item identifier to be NORMAL, with the specified storage.
    pub const fn set_normal(&mut self, off: u16, len: u16) {
        self.set_flags(LP_NORMAL);
        self.set_off(off);
        self.set_len(len);
    }

    /// Set the item identifier to be REDIRECT, with the specified link.
    pub const fn set_redirect(&mut self, link: u16) {
        self.set_flags(LP_REDIRECT);
        self.set_off(link);
        self.set_len(0);
    }

    /// Set the item identifier to be DEAD, with no storage.
    pub const fn set_dead(&mut self) {
        self.set_flags(LP_DEAD);
        self.set_off(0);
        self.set_len(0);
    }

    /// Set the item identifier to be DEAD, keeping its existing storage.
    pub const fn mark_dead(&mut self) {
        self.set_flags(LP_DEAD);
    }
}
