use tikv_client::metrics;

#[test]
fn metrics_module_exposes_gather_helpers() {
    let metric_families = metrics::gather();
    let encoded = metrics::encode_as_text(&metric_families).expect("encode provided metrics");
    assert!(!encoded.is_empty());

    let gathered_encoded = metrics::gather_as_text().expect("encode gathered metrics as text");
    assert!(!gathered_encoded.is_empty());
}
