// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use std::fmt;

mod bound_range;
pub mod codec;
mod key;
mod kvpair;
mod options;
mod value;
mod value_entry;

pub use bound_range::BoundRange;
pub use bound_range::IntoOwnedRange;
pub use key::Key;
pub use key::KvPairTTL;
pub use kvpair::KvPair;
pub use options::batch_get_to_get_options;
pub use options::with_return_commit_ts;
pub use options::BatchGetOption;
pub use options::BatchGetOptions;
pub use options::GetOption;
pub use options::GetOptions;
pub use options::GetOrBatchGetOption;
pub use value::Value;
pub use value_entry::ValueEntry;

struct HexRepr<'a>(pub &'a [u8]);

impl fmt::Display for HexRepr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02X}")?;
        }
        Ok(())
    }
}
