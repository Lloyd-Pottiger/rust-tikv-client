// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Small option types for KV reads.
//!
//! This ports the core behavior of `client-go/kv` read options (as tested in
//! `client-go/kv/kv_test.go`), but uses Rust-native types.

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GetOptions {
    return_commit_ts: bool,
}

impl GetOptions {
    pub fn return_commit_ts(&self) -> bool {
        self.return_commit_ts
    }

    pub fn apply(&mut self, options: &[GetOption]) {
        for opt in options {
            match opt {
                GetOption::ReturnCommitTs => self.return_commit_ts = true,
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BatchGetOptions {
    return_commit_ts: bool,
}

impl BatchGetOptions {
    pub fn return_commit_ts(&self) -> bool {
        self.return_commit_ts
    }

    pub fn apply(&mut self, options: &[BatchGetOption]) {
        for opt in options {
            match opt {
                BatchGetOption::ReturnCommitTs => self.return_commit_ts = true,
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GetOption {
    ReturnCommitTs,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchGetOption {
    ReturnCommitTs,
}

pub fn with_return_commit_ts() -> GetOrBatchGetOption {
    GetOrBatchGetOption::ReturnCommitTs
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GetOrBatchGetOption {
    ReturnCommitTs,
}

impl From<GetOrBatchGetOption> for GetOption {
    fn from(value: GetOrBatchGetOption) -> Self {
        match value {
            GetOrBatchGetOption::ReturnCommitTs => GetOption::ReturnCommitTs,
        }
    }
}

impl From<GetOrBatchGetOption> for BatchGetOption {
    fn from(value: GetOrBatchGetOption) -> Self {
        match value {
            GetOrBatchGetOption::ReturnCommitTs => BatchGetOption::ReturnCommitTs,
        }
    }
}

pub fn batch_get_to_get_options(options: Option<&[BatchGetOption]>) -> Vec<GetOption> {
    match options {
        Some(opts) => opts.iter().copied().map(|o| match o {
            BatchGetOption::ReturnCommitTs => GetOption::ReturnCommitTs,
        }).collect(),
        None => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_options_matches_client_go_test() {
        let mut opts = GetOptions::default();
        opts.apply(&[]);
        assert_eq!(opts, GetOptions::default());

        let mut opts = GetOptions::default();
        opts.apply(&[with_return_commit_ts().into()]);
        assert!(opts.return_commit_ts());

        let mut opts = GetOptions::default();
        opts.apply(&batch_get_to_get_options(None));
        assert_eq!(opts, GetOptions::default());

        let mut opts = GetOptions::default();
        let batch = [BatchGetOption::from(with_return_commit_ts())];
        opts.apply(&batch_get_to_get_options(Some(&batch)));
        assert!(opts.return_commit_ts());
    }

    #[test]
    fn batch_get_options_matches_client_go_test() {
        let mut opts = BatchGetOptions::default();
        opts.apply(&[]);
        assert_eq!(opts, BatchGetOptions::default());

        let mut opts = BatchGetOptions::default();
        opts.apply(&[BatchGetOption::from(with_return_commit_ts())]);
        assert!(opts.return_commit_ts());
    }
}

