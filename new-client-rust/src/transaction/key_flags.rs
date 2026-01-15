//! Per-key transaction metadata (flags).
//!
//! This module is inspired by `client-go`'s `kv/keyflags.go`. It provides a compact, bitflag-based
//! representation for per-key metadata and a set of flag operations (`FlagsOp`) that can be
//! applied to a key inside a transaction.
//!
//! The flags are stored in the transaction buffer and influence request lowering (e.g. mutation
//! op selection and assertions) and commit protocol behavior (e.g. prewrite-only keys).

/// Per-key metadata associated with a transaction key.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct KeyFlags(u16);

impl KeyFlags {
    const PRESUME_KEY_NOT_EXISTS: u16 = 1 << 0;
    const PREWRITE_ONLY: u16 = 1 << 1;
    const ASSERT_EXIST: u16 = 1 << 2;
    const ASSERT_NOT_EXIST: u16 = 1 << 3;

    /// Returns whether the key is presumed to not exist.
    pub fn has_presume_key_not_exists(self) -> bool {
        self.0 & (Self::PRESUME_KEY_NOT_EXISTS) != 0
    }

    /// Returns whether the key is prewrite-only (excluded from the commit phase).
    pub fn has_prewrite_only(self) -> bool {
        self.0 & (Self::PREWRITE_ONLY) != 0
    }

    /// Returns whether the key is asserted to exist.
    pub fn has_assert_exist(self) -> bool {
        self.0 & Self::ASSERT_EXIST != 0 && self.0 & Self::ASSERT_NOT_EXIST == 0
    }

    /// Returns whether the key is asserted to not exist.
    pub fn has_assert_not_exist(self) -> bool {
        self.0 & Self::ASSERT_NOT_EXIST != 0 && self.0 & Self::ASSERT_EXIST == 0
    }

    /// Returns whether the key's assertion is "unknown" (both bits set).
    pub fn has_assert_unknown(self) -> bool {
        self.0 & Self::ASSERT_EXIST != 0 && self.0 & Self::ASSERT_NOT_EXIST != 0
    }

    /// Returns whether any assertion flag is set.
    pub fn has_assertion_flags(self) -> bool {
        self.0 & (Self::ASSERT_EXIST | Self::ASSERT_NOT_EXIST) != 0
    }

    /// Apply a flag operation.
    #[must_use]
    pub fn apply(mut self, op: FlagsOp) -> Self {
        match op {
            FlagsOp::SetPresumeKeyNotExists => self.0 |= Self::PRESUME_KEY_NOT_EXISTS,
            FlagsOp::DelPresumeKeyNotExists => self.0 &= !Self::PRESUME_KEY_NOT_EXISTS,
            FlagsOp::SetPrewriteOnly => self.0 |= Self::PREWRITE_ONLY,
            FlagsOp::DelPrewriteOnly => self.0 &= !Self::PREWRITE_ONLY,
            FlagsOp::SetAssertExist => {
                self.0 &= !Self::ASSERT_NOT_EXIST;
                self.0 |= Self::ASSERT_EXIST;
            }
            FlagsOp::SetAssertNotExist => {
                self.0 &= !Self::ASSERT_EXIST;
                self.0 |= Self::ASSERT_NOT_EXIST;
            }
            FlagsOp::SetAssertUnknown => self.0 |= Self::ASSERT_EXIST | Self::ASSERT_NOT_EXIST,
            FlagsOp::SetAssertNone => self.0 &= !(Self::ASSERT_EXIST | Self::ASSERT_NOT_EXIST),
        }
        self
    }
}

/// An operation that mutates [`KeyFlags`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlagsOp {
    SetPresumeKeyNotExists,
    DelPresumeKeyNotExists,
    SetPrewriteOnly,
    DelPrewriteOnly,
    SetAssertExist,
    SetAssertNotExist,
    SetAssertUnknown,
    SetAssertNone,
}
