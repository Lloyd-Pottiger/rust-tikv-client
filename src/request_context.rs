// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Stable wrapper types for common `kvrpcpb::Context` fields.
//!
//! TiKV RPCs attach extra metadata via `kvrpcpb::Context` (e.g. priority,
//! isolation level). We expose a small, stable set of enums so applications
//! don't need to depend on the generated protobuf types directly.

/// The priority of commands executed by TiKV.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum CommandPriority {
    /// Normal is the default value.
    #[default]
    Normal = 0,
    Low = 1,
    High = 2,
}

/// Transaction isolation level for reads.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum IsolationLevel {
    /// Snapshot isolation (default).
    #[default]
    Si = 0,
    /// Read committed.
    Rc = 1,
    /// Read committed + extra check for more recent versions.
    RcCheckTs = 2,
}

/// Used to tell TiKV whether operations are allowed or not on different disk usages.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum DiskFullOpt {
    /// Disallow operations on both almost-full and already-full disks.
    #[default]
    NotAllowedOnFull = 0,
    /// Allow operations when disk is almost full.
    AllowedOnAlmostFull = 1,
    /// Allow operations when disk is already full.
    AllowedOnAlreadyFull = 2,
}

/// A structured builder for the `kvrpcpb::Context.request_source` label.
///
/// This matches the label format used by TiKV's Go client:
/// - `unknown` when neither `source_type` nor `explicit_type` is set (regardless of `internal`).
/// - Otherwise: `{internal|external}_{source|unknown}[_explicit]`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct RequestSource {
    internal: bool,
    source_type: String,
    explicit_type: String,
}

impl RequestSource {
    /// Create a new request source descriptor.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether this request is considered internal.
    #[must_use]
    pub fn internal(mut self, internal: bool) -> Self {
        self.internal = internal;
        self
    }

    /// Set the primary request source type.
    #[must_use]
    pub fn source_type(mut self, source_type: impl Into<String>) -> Self {
        self.source_type = source_type.into();
        self
    }

    /// Set the explicit request source type (for example, a session or task type).
    #[must_use]
    pub fn explicit_type(mut self, explicit_type: impl Into<String>) -> Self {
        self.explicit_type = explicit_type.into();
        self
    }

    fn is_empty(&self) -> bool {
        self.source_type.is_empty() && self.explicit_type.is_empty()
    }
}

impl std::fmt::Display for RequestSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const SOURCE_UNKNOWN: &str = "unknown";
        const INTERNAL_REQUEST: &str = "internal";
        const EXTERNAL_REQUEST: &str = "external";

        if self.is_empty() {
            return f.write_str(SOURCE_UNKNOWN);
        }

        let origin = if self.internal {
            INTERNAL_REQUEST
        } else {
            EXTERNAL_REQUEST
        };
        let source = if self.source_type.is_empty() {
            SOURCE_UNKNOWN
        } else {
            self.source_type.as_str()
        };

        write!(f, "{origin}_{source}")?;

        if !self.explicit_type.is_empty() && self.explicit_type != self.source_type {
            write!(f, "_{}", self.explicit_type)?;
        }

        Ok(())
    }
}

/// Returns true if `request_source` represents an internal request.
///
/// This matches client-go's `IsInternalRequest` behavior (checks for the `"internal"` prefix).
pub fn is_internal_request_source(request_source: &str) -> bool {
    request_source.starts_with("internal")
}

#[cfg(test)]
mod tests {
    use super::{is_internal_request_source, RequestSource};

    #[test]
    fn test_request_source_formatting_matches_client_go() {
        assert_eq!(RequestSource::default().to_string(), "unknown");
        assert_eq!(RequestSource::new().internal(true).to_string(), "unknown");

        assert_eq!(
            RequestSource::new()
                .internal(true)
                .source_type("gc")
                .to_string(),
            "internal_gc"
        );
        assert_eq!(
            RequestSource::new()
                .internal(false)
                .source_type("gc")
                .to_string(),
            "external_gc"
        );

        assert_eq!(
            RequestSource::new().explicit_type("br").to_string(),
            "external_unknown_br"
        );
        assert_eq!(
            RequestSource::new()
                .source_type("br")
                .explicit_type("br")
                .to_string(),
            "external_br"
        );
        assert_eq!(
            RequestSource::new()
                .internal(true)
                .source_type("gc")
                .explicit_type("stats")
                .to_string(),
            "internal_gc_stats"
        );

        assert!(is_internal_request_source("internal_gc"));
        assert!(is_internal_request_source("internal_gc_stats"));
        assert!(!is_internal_request_source("external_gc"));
        assert!(!is_internal_request_source("unknown"));
    }
}
