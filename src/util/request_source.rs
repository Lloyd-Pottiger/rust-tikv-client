// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go client parity: `client-go/util/request_source.go`.

/// Build a request source string compatible with client-go.
///
/// Format:
/// - Both types empty => `"unknown"`
/// - Otherwise: `"internal|external" + "_" + request_source_type_or_unknown + ["_" + explicit_type]`
pub(crate) fn build_request_source(
    is_internal: bool,
    request_source_type: &str,
    explicit_request_source_type: &str,
) -> String {
    if request_source_type.is_empty() && explicit_request_source_type.is_empty() {
        return "unknown".to_owned();
    }

    let prefix = if is_internal { "internal" } else { "external" };
    let request_source_type = if request_source_type.is_empty() {
        "unknown"
    } else {
        request_source_type
    };

    if explicit_request_source_type.is_empty() {
        format!("{prefix}_{request_source_type}")
    } else {
        format!("{prefix}_{request_source_type}_{explicit_request_source_type}")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RequestSource {
    pub(crate) request_source_internal: bool,
    pub(crate) request_source_type: String,
    pub(crate) explicit_request_source_type: String,
}

impl RequestSource {
    pub(crate) fn get_request_source(&self) -> String {
        build_request_source(
            self.request_source_internal,
            &self.request_source_type,
            &self.explicit_request_source_type,
        )
    }
}

pub(crate) fn get_request_source(rs: Option<&RequestSource>) -> String {
    rs.map_or_else(|| "unknown".to_owned(), RequestSource::get_request_source)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_get_request_source() {
        let mut rs = RequestSource {
            request_source_internal: true,
            request_source_type: "test".to_owned(),
            explicit_request_source_type: "lightning".to_owned(),
        };

        assert_eq!(rs.get_request_source(), "internal_test_lightning");

        rs.request_source_internal = false;
        assert_eq!(rs.get_request_source(), "external_test_lightning");

        assert_eq!(get_request_source(None), "unknown");

        rs = RequestSource::default();
        assert_eq!(rs.get_request_source(), "unknown");

        rs.request_source_type = "test".to_owned();
        assert_eq!(rs.get_request_source(), "external_test");

        rs.request_source_type.clear();
        rs.explicit_request_source_type = "lightning".to_owned();
        assert_eq!(rs.get_request_source(), "external_unknown_lightning");
    }

    #[test]
    fn test_build_request_source() {
        assert_eq!(
            build_request_source(true, "test", "lightning"),
            "internal_test_lightning"
        );
        assert_eq!(
            build_request_source(false, "test", "lightning"),
            "external_test_lightning"
        );
        assert_eq!(build_request_source(false, "test", ""), "external_test");
        assert_eq!(
            build_request_source(false, "", "lightning"),
            "external_unknown_lightning"
        );
        assert_eq!(build_request_source(true, "", ""), "unknown");
    }
}
