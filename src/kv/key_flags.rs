//! Key flag helpers mirroring client-go `kv/keyflags.go`.

/// KeyFlags are metadata associated with key.
///
/// Notice that the highest bit is used by red-black tree in client-go's memdb implementation, so
/// it should not be used for additional flags.
///
/// This maps to client-go `kv.KeyFlags`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct KeyFlags(u16);

/// Byte size of [`KeyFlags`].
///
/// This maps to client-go `kv.FlagBytes`.
#[doc(alias = "FlagBytes")]
pub const FLAG_BYTES: usize = 2;

const FLAG_PRESUME_KNE: u16 = 1 << 0;
const FLAG_KEY_LOCKED: u16 = 1 << 1;
const FLAG_NEED_LOCKED: u16 = 1 << 2;
const FLAG_KEY_LOCKED_VAL_EXIST: u16 = 1 << 3;
const FLAG_NEED_CHECK_EXISTS: u16 = 1 << 4;
const FLAG_PREWRITE_ONLY: u16 = 1 << 5;
const FLAG_IGNORED_IN_2PC: u16 = 1 << 6;
const FLAG_READABLE: u16 = 1 << 7;
const FLAG_NEWLY_INSERTED: u16 = 1 << 8;

const FLAG_ASSERT_EXIST: u16 = 1 << 9;
const FLAG_ASSERT_NOT_EXIST: u16 = 1 << 10;

const FLAG_NEED_CONSTRAINT_CHECK_IN_PREWRITE: u16 = 1 << 11;
const FLAG_PREVIOUS_PRESUME_KNE: u16 = 1 << 12;
const FLAG_KEY_LOCKED_IN_SHARE_MODE: u16 = 1 << 13;

const PERSISTENT_FLAGS: u16 = FLAG_KEY_LOCKED
    | FLAG_KEY_LOCKED_VAL_EXIST
    | FLAG_NEED_CONSTRAINT_CHECK_IN_PREWRITE
    | FLAG_KEY_LOCKED_IN_SHARE_MODE;

impl KeyFlags {
    #[must_use]
    pub const fn from_bits(bits: u16) -> Self {
        Self(bits)
    }

    #[must_use]
    pub const fn bits(self) -> u16 {
        self.0
    }

    /// Returns whether the key must be asserted to exist in 2PC.
    #[must_use]
    pub const fn has_assert_exist(self) -> bool {
        (self.0 & FLAG_ASSERT_EXIST) != 0 && (self.0 & FLAG_ASSERT_NOT_EXIST) == 0
    }

    /// Returns whether the key must be asserted to not exist in 2PC.
    #[must_use]
    pub const fn has_assert_not_exist(self) -> bool {
        (self.0 & FLAG_ASSERT_NOT_EXIST) != 0 && (self.0 & FLAG_ASSERT_EXIST) == 0
    }

    /// Returns whether the key's assertion is marked as unknown.
    #[must_use]
    pub const fn has_assert_unknown(self) -> bool {
        (self.0 & FLAG_ASSERT_EXIST) != 0 && (self.0 & FLAG_ASSERT_NOT_EXIST) != 0
    }

    /// Returns whether the key's assertion is set.
    #[must_use]
    pub const fn has_assertion_flags(self) -> bool {
        (self.0 & FLAG_ASSERT_EXIST) != 0 || (self.0 & FLAG_ASSERT_NOT_EXIST) != 0
    }

    /// Returns whether the key is marked to lazily check existence.
    #[must_use]
    pub const fn has_presume_key_not_exists(self) -> bool {
        (self.0 & (FLAG_PRESUME_KNE | FLAG_PREVIOUS_PRESUME_KNE)) != 0
    }

    /// Returns whether the key has acquired pessimistic lock.
    #[must_use]
    pub const fn has_locked(self) -> bool {
        (self.0 & FLAG_KEY_LOCKED) != 0
    }

    /// Returns whether the key has been locked in share mode.
    #[must_use]
    pub const fn has_locked_in_share_mode(self) -> bool {
        (self.0 & FLAG_KEY_LOCKED_IN_SHARE_MODE) != 0
    }

    /// Returns whether the key needs to be locked.
    #[must_use]
    pub const fn has_need_locked(self) -> bool {
        (self.0 & FLAG_NEED_LOCKED) != 0
    }

    /// Returns whether the value exists when the key is locked.
    #[must_use]
    pub const fn has_locked_value_exists(self) -> bool {
        (self.0 & FLAG_KEY_LOCKED_VAL_EXIST) != 0
    }

    /// Returns whether the key needs to check existence when it has been locked.
    #[must_use]
    pub const fn has_need_check_exists(self) -> bool {
        (self.0 & FLAG_NEED_CHECK_EXISTS) != 0
    }

    /// Returns whether the key should be used in 2PC commit phase.
    #[must_use]
    pub const fn has_prewrite_only(self) -> bool {
        (self.0 & FLAG_PREWRITE_ONLY) != 0
    }

    /// Returns whether the key will be ignored in 2PC.
    #[must_use]
    pub const fn has_ignored_in_2pc(self) -> bool {
        (self.0 & FLAG_IGNORED_IN_2PC) != 0
    }

    /// Returns whether the in-transaction operations is able to read the key.
    #[must_use]
    pub const fn has_readable(self) -> bool {
        (self.0 & FLAG_READABLE) != 0
    }

    /// Returns whether the key needs to check conflict in prewrite.
    #[must_use]
    pub const fn has_need_constraint_check_in_prewrite(self) -> bool {
        (self.0 & FLAG_NEED_CONSTRAINT_CHECK_IN_PREWRITE) != 0
    }

    /// Returns the persistent subset of flags.
    #[must_use]
    pub const fn and_persistent(self) -> KeyFlags {
        KeyFlags(self.0 & PERSISTENT_FLAGS)
    }

    /// Returns whether the key is newly inserted with value length > 0.
    #[must_use]
    pub const fn has_newly_inserted(self) -> bool {
        (self.0 & FLAG_NEWLY_INSERTED) != 0
    }
}

impl From<u16> for KeyFlags {
    fn from(bits: u16) -> Self {
        KeyFlags::from_bits(bits)
    }
}

impl From<KeyFlags> for u16 {
    fn from(flags: KeyFlags) -> Self {
        flags.bits()
    }
}

impl std::ops::BitOr for KeyFlags {
    type Output = KeyFlags;

    fn bitor(self, rhs: Self) -> Self::Output {
        KeyFlags(self.0 | rhs.0)
    }
}

impl std::ops::BitOrAssign for KeyFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

impl std::ops::BitAnd for KeyFlags {
    type Output = KeyFlags;

    fn bitand(self, rhs: Self) -> Self::Output {
        KeyFlags(self.0 & rhs.0)
    }
}

impl std::ops::BitAndAssign for KeyFlags {
    fn bitand_assign(&mut self, rhs: Self) {
        self.0 &= rhs.0;
    }
}

impl std::ops::Not for KeyFlags {
    type Output = KeyFlags;

    fn not(self) -> Self::Output {
        KeyFlags(!self.0)
    }
}

/// FlagsOp describes [`KeyFlags`] modify operation.
///
/// This maps to client-go `kv.FlagsOp` constants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FlagsOp {
    SetPresumeKeyNotExists,
    DelPresumeKeyNotExists,
    SetKeyLocked,
    DelKeyLocked,
    SetNeedLocked,
    DelNeedLocked,
    SetKeyLockedValueExists,
    SetKeyLockedValueNotExists,
    DelNeedCheckExists,
    SetPrewriteOnly,
    SetIgnoredIn2PC,
    SetReadable,
    SetNewlyInserted,
    SetAssertExist,
    SetAssertNotExist,
    SetAssertUnknown,
    SetAssertNone,
    SetNeedConstraintCheckInPrewrite,
    DelNeedConstraintCheckInPrewrite,
    SetPreviousPresumeKNE,
    SetKeyLockedInShareMode,
    SetKeyLockedInExclusiveMode,
}

/// Apply [`FlagsOp`] to the given [`KeyFlags`].
///
/// This maps to client-go `kv.ApplyFlagsOps`.
#[must_use]
pub fn apply_flags_ops(mut origin: KeyFlags, ops: impl IntoIterator<Item = FlagsOp>) -> KeyFlags {
    for op in ops {
        match op {
            FlagsOp::SetPresumeKeyNotExists => {
                origin.0 |= FLAG_PRESUME_KNE | FLAG_NEED_CHECK_EXISTS;
            }
            FlagsOp::DelPresumeKeyNotExists => {
                origin.0 &= !(FLAG_PRESUME_KNE | FLAG_NEED_CHECK_EXISTS);
            }
            FlagsOp::SetKeyLocked => {
                origin.0 |= FLAG_KEY_LOCKED;
            }
            FlagsOp::DelKeyLocked => {
                origin.0 &= !FLAG_KEY_LOCKED;
            }
            FlagsOp::SetNeedLocked => {
                origin.0 |= FLAG_NEED_LOCKED;
            }
            FlagsOp::DelNeedLocked => {
                origin.0 &= !FLAG_NEED_LOCKED;
            }
            FlagsOp::SetKeyLockedValueExists => {
                origin.0 |= FLAG_KEY_LOCKED_VAL_EXIST;
                origin.0 &= !FLAG_NEED_CONSTRAINT_CHECK_IN_PREWRITE;
            }
            FlagsOp::DelNeedCheckExists => {
                origin.0 &= !FLAG_NEED_CHECK_EXISTS;
            }
            FlagsOp::SetKeyLockedValueNotExists => {
                origin.0 &= !FLAG_KEY_LOCKED_VAL_EXIST;
                origin.0 &= !FLAG_NEED_CONSTRAINT_CHECK_IN_PREWRITE;
            }
            FlagsOp::SetPrewriteOnly => {
                origin.0 |= FLAG_PREWRITE_ONLY;
            }
            FlagsOp::SetIgnoredIn2PC => {
                origin.0 |= FLAG_IGNORED_IN_2PC;
            }
            FlagsOp::SetReadable => {
                origin.0 |= FLAG_READABLE;
            }
            FlagsOp::SetNewlyInserted => {
                origin.0 |= FLAG_NEWLY_INSERTED;
            }
            FlagsOp::SetAssertExist => {
                origin.0 &= !FLAG_ASSERT_NOT_EXIST;
                origin.0 |= FLAG_ASSERT_EXIST;
            }
            FlagsOp::SetAssertNotExist => {
                origin.0 &= !FLAG_ASSERT_EXIST;
                origin.0 |= FLAG_ASSERT_NOT_EXIST;
            }
            FlagsOp::SetAssertUnknown => {
                origin.0 |= FLAG_ASSERT_NOT_EXIST;
                origin.0 |= FLAG_ASSERT_EXIST;
            }
            FlagsOp::SetAssertNone => {
                origin.0 &= !FLAG_ASSERT_EXIST;
                origin.0 &= !FLAG_ASSERT_NOT_EXIST;
            }
            FlagsOp::SetNeedConstraintCheckInPrewrite => {
                origin.0 |= FLAG_NEED_CONSTRAINT_CHECK_IN_PREWRITE;
            }
            FlagsOp::DelNeedConstraintCheckInPrewrite => {
                origin.0 &= !FLAG_NEED_CONSTRAINT_CHECK_IN_PREWRITE;
            }
            FlagsOp::SetPreviousPresumeKNE => {
                origin.0 |= FLAG_PREVIOUS_PRESUME_KNE;
            }
            FlagsOp::SetKeyLockedInShareMode => {
                origin.0 |= FLAG_KEY_LOCKED_IN_SHARE_MODE;
            }
            FlagsOp::SetKeyLockedInExclusiveMode => {
                origin.0 &= !FLAG_KEY_LOCKED_IN_SHARE_MODE;
            }
        }
    }
    origin
}
