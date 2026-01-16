//! Disk-full behavior hints for TiKV commands.

/// Disk-full handling hint for TiKV operations.
///
/// This is mapped to `kvrpcpb::Context.disk_full_opt`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub enum DiskFullOpt {
    /// Operations are not allowed when disk is almost full or already full.
    #[default]
    NotAllowedOnFull,
    /// Operations are allowed when disk is almost full.
    AllowedOnAlmostFull,
    /// Operations are allowed when disk is already full.
    AllowedOnAlreadyFull,
}

impl From<DiskFullOpt> for i32 {
    fn from(value: DiskFullOpt) -> Self {
        match value {
            DiskFullOpt::NotAllowedOnFull => 0,
            DiskFullOpt::AllowedOnAlmostFull => 1,
            DiskFullOpt::AllowedOnAlreadyFull => 2,
        }
    }
}
