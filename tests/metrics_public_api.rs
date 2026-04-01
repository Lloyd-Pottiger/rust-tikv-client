use prometheus::IntCounter;
use prometheus::Opts;
use tikv_client::metrics;

#[test]
fn metrics_module_exposes_gather_helpers() {
    let metric_name = "tikv_client_metrics_public_api_test_counter";
    let counter =
        IntCounter::with_opts(Opts::new(metric_name, "test counter")).expect("create test counter");
    prometheus::register(Box::new(counter.clone())).expect("register test counter");
    counter.inc();

    let metric_families = metrics::gather();
    let encoded = metrics::encode_as_text(&metric_families).expect("encode provided metrics");
    assert!(String::from_utf8_lossy(&encoded).contains(metric_name));

    let gathered_encoded = metrics::gather_as_text().expect("encode gathered metrics as text");
    assert!(String::from_utf8_lossy(&gathered_encoded).contains(metric_name));
}
