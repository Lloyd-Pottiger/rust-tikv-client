//! Prometheus metrics helpers.
//!
//! The client registers metrics into the default Prometheus registry. This module provides small
//! convenience helpers so users can export metrics without depending on the `prometheus` crate
//! directly.

use prometheus::Encoder;
use prometheus::TextEncoder;

/// Gather all metrics from the default Prometheus registry.
pub fn gather() -> Vec<prometheus::proto::MetricFamily> {
    prometheus::gather()
}

/// Gather all metrics and encode them using the Prometheus text format.
pub fn gather_as_text() -> std::io::Result<Vec<u8>> {
    let metric_families = gather();
    encode_as_text(&metric_families)
}

/// Encode the provided metric families using the Prometheus text format.
pub fn encode_as_text(
    metric_families: &[prometheus::proto::MetricFamily],
) -> std::io::Result<Vec<u8>> {
    let encoder = TextEncoder::new();
    let mut buf = Vec::new();
    encoder
        .encode(metric_families, &mut buf)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))?;
    Ok(buf)
}
