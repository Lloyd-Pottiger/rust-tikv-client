use std::any::Any;

use tikv_client::coprocessor::{self, lowering};
use tikv_client::{Error, Result, Timestamp, TimestampExt};

#[test]
fn coprocessor_public_api_exposes_reexports_and_downcast_helpers() {
    let _: Option<coprocessor::Request> = None;
    let _: Option<coprocessor::Response> = None;
    let _: Option<coprocessor::BatchRequest> = None;
    let _: Option<coprocessor::BatchResponse> = None;
    let _: Option<coprocessor::RegionInfo> = None;
    let _: Option<coprocessor::KeyRange> = None;
    let _: Option<coprocessor::CoprocessorResponseStream> = None;
    let _: Option<coprocessor::BatchCoprocessorResponseStream> = None;
    let _: fn(Box<dyn Any>) -> Result<coprocessor::CoprocessorResponseStream> =
        coprocessor::downcast_coprocessor_response_stream;
    let _: fn(Box<dyn Any>) -> Result<coprocessor::BatchCoprocessorResponseStream> =
        coprocessor::downcast_batch_coprocessor_response_stream;

    let err = coprocessor::downcast_coprocessor_response_stream(Box::new(()) as Box<dyn Any>)
        .expect_err("unexpected downcast success");
    assert!(matches!(err, Error::InternalError { .. }));

    let err = coprocessor::downcast_batch_coprocessor_response_stream(Box::new(()) as Box<dyn Any>)
        .expect_err("unexpected batch downcast success");
    assert!(matches!(err, Error::InternalError { .. }));
}

#[test]
fn coprocessor_public_api_exposes_lowering_helpers() {
    let start_ts = <Timestamp as TimestampExt>::from_version(42);

    let request = lowering::new_coprocessor_request(
        7,
        b"payload".to_vec(),
        vec!["a".to_owned().."b".to_owned()],
        start_ts.clone(),
    );
    assert_eq!(request.tp, 7);
    assert_eq!(request.data, b"payload".to_vec());
    assert_eq!(request.start_ts, 42);
    assert_eq!(request.ranges.len(), 1);
    assert_eq!(request.ranges[0].start, b"a".to_vec());
    assert_eq!(request.ranges[0].end, b"b".to_vec());

    let batch = lowering::new_batch_coprocessor_request(
        11,
        b"batch".to_vec(),
        vec![coprocessor::RegionInfo {
            region_id: 9,
            ..Default::default()
        }],
        start_ts,
        123,
    );
    assert_eq!(batch.tp, 11);
    assert_eq!(batch.data, b"batch".to_vec());
    assert_eq!(batch.start_ts, 42);
    assert_eq!(batch.schema_ver, 123);
    assert_eq!(batch.regions.len(), 1);
    assert_eq!(batch.regions[0].region_id, 9);
}

#[test]
fn coprocessor_public_api_exposes_stream_request_accessors() {
    let start_ts = <Timestamp as TimestampExt>::from_version(64);

    let mut request: coprocessor::CoprocessorStreamRequest =
        lowering::new_coprocessor_stream_request(
            5,
            b"stream".to_vec(),
            vec!["k1".to_owned().."k9".to_owned()],
            start_ts,
        );
    assert_eq!(request.inner().tp, 5);
    assert_eq!(request.inner().start_ts, 64);

    request.inner_mut().tp = 6;
    request.inner_mut().data = b"mutated".to_vec();

    let inner = request.into_inner();
    assert_eq!(inner.tp, 6);
    assert_eq!(inner.data, b"mutated".to_vec());
    assert_eq!(inner.ranges.len(), 1);
    assert_eq!(inner.ranges[0].start, b"k1".to_vec());
    assert_eq!(inner.ranges[0].end, b"k9".to_vec());
}
