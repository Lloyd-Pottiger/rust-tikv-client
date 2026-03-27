use tikv_client::util;

#[test]
fn util_israce_public_api_exposes_race_enabled_constant() {
    let _: bool = util::israce::RACE_ENABLED;
}
