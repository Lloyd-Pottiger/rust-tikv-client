//! Prometheus metrics integration.
//!
//! This crate records a small set of core RPC counters and duration histograms. When the
//! `prometheus` feature is disabled, metrics collection is compiled out.

/// Returns `true` if the crate is compiled with Prometheus metrics enabled.
pub const fn is_enabled() -> bool {
    cfg!(feature = "prometheus")
}

/// Ensure all core metrics are registered.
///
/// Returns `false` if the crate is built without the `prometheus` feature.
pub fn register() -> bool {
    #[cfg(feature = "prometheus")]
    {
        crate::stats::ensure_metrics_registered();
        true
    }

    #[cfg(not(feature = "prometheus"))]
    {
        false
    }
}

/// Gather all registered Prometheus metrics in the text exposition format.
///
/// Returns `None` if the crate is built without the `prometheus` feature.
pub fn gather_as_text() -> Option<String> {
    #[cfg(feature = "prometheus")]
    {
        use prometheus::Encoder as _;

        register();
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

#[cfg(test)]
mod tests {
    #[test]
    fn register_and_gather_is_safe_when_disabled() {
        // This test runs in both feature sets; in `--no-default-features` it should be a no-op.
        let _ = super::register();
        let _ = super::gather_as_text();
    }

    #[cfg(feature = "prometheus")]
    #[test]
    fn gather_contains_core_metrics() {
        super::register();
        let text = super::gather_as_text().expect("prometheus feature enabled");
        assert!(
            text.contains("tikv_request_total"),
            "missing expected metric name"
        );
    }
}
