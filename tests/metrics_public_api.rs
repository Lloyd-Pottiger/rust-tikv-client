use tikv_client::metrics;

#[test]
fn metrics_module_exposes_gather_helpers() {
    let _ = metrics::gather();
    let _ = metrics::gather_as_text().expect("encode metrics as text");
}
