use tikv_client::util;

#[test]
fn util_failpoint_public_api_exposes_enable_and_eval() {
    let _: fn() = util::enable_failpoints;
    let _: fn(&str) -> Result<Option<String>, util::FailpointsDisabledError> = util::eval_failpoint;
}
