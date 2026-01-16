//! Prometheus metrics integration.
//!
//! This crate records a small set of core RPC counters and duration histograms. When the
//! `prometheus` feature is disabled, metrics collection is compiled out.

/// Returns `true` if the crate is compiled with Prometheus metrics enabled.
pub const fn is_enabled() -> bool {
    cfg!(feature = "prometheus")
}

/// Gather all registered Prometheus metrics in the text exposition format.
///
/// Returns `None` if the crate is built without the `prometheus` feature.
pub fn gather_as_text() -> Option<String> {
    #[cfg(feature = "prometheus")]
    {
        use prometheus::Encoder as _;

        crate::stats::ensure_metrics_registered();
        let metric_families = prometheus::gather();

        let mut buf = Vec::new();
        let encoder = prometheus::TextEncoder::new();
        encoder.encode(&metric_families, &mut buf).ok()?;
        String::from_utf8(buf).ok()
    }

    #[cfg(not(feature = "prometheus"))]
    {
        None
    }
}
