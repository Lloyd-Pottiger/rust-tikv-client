// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use std::fmt;

mod bound_range;
pub mod codec;
mod get;
mod key;
mod key_flags;
mod kvpair;
mod value;

pub use bound_range::BoundRange;
pub use bound_range::IntoOwnedRange;
pub use get::with_return_commit_ts;
pub use get::BatchGetOption;
pub use get::BatchGetOptions;
pub use get::BatchGetter;
pub use get::GetOption;
pub use get::GetOptions;
pub use get::GetOrBatchGetOption;
pub use get::Getter;
pub use get::ValueEntry;
pub use key::cmp_key;
pub use key::next_key;
pub use key::prefix_next_key;
pub use key::Key;
pub use key::KeyRange;
pub use key::KvPairTTL;
pub use key_flags::apply_flags_ops;
pub use key_flags::FlagsOp;
pub use key_flags::KeyFlags;
pub use key_flags::FLAG_BYTES;
pub use kvpair::KvPair;
pub use value::Value;

pub use crate::store_vars::access_location_type;
pub use crate::store_vars::global_store_limit;
pub use crate::store_vars::set_store_limit;
pub use crate::store_vars::store_limit;
pub use crate::store_vars::with_store_limit;
pub use crate::store_vars::AccessLocationType;
pub use crate::transaction::DEFAULT_VARS;
pub use crate::transaction::DEF_BACKOFF_LOCK_FAST_MS;
pub use crate::transaction::DEF_BACKOFF_WEIGHT;
pub use crate::ReplicaReadType;
pub use crate::Variables;

/// Wrapper that formats a byte slice as uppercase hexadecimal.
///
/// This is primarily exposed for diagnostics and debug-style output in public
/// key/value types such as [`Key`](crate::kv::Key) and [`KvPair`](crate::kv::KvPair).
pub struct HexRepr<'a>(pub &'a [u8]);

impl fmt::Display for HexRepr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02X}")?;
        }
        Ok(())
    }
}
