// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! Network traffic accounting.
//!
//! This module is a small subset of client-go's `internal/locate/networkCollector`:
//! - per-request unpacked request/response bytes counters (client-side execution details)
//! - stale-read request/response bytes and request counters

use std::sync::atomic::{AtomicI64, Ordering};

use prost::Message;

/// Whether an RPC target is within the local zone (AZ) or crosses zones.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum AccessLocation {
    #[default]
    Unknown,
    LocalZone,
    CrossZone,
}

impl AccessLocation {
    fn is_cross_zone(self) -> bool {
        matches!(self, Self::CrossZone)
    }
}

/// A small set of per-request execution details (client-side).
///
/// This mirrors the parts of client-go's `util.ExecDetails` used by network metrics collection.
#[derive(Debug)]
pub(crate) struct ExecDetails {
    pub(crate) unpacked_bytes_sent_kv_total: AtomicI64,
    pub(crate) unpacked_bytes_sent_kv_cross_zone: AtomicI64,
    pub(crate) unpacked_bytes_received_kv_total: AtomicI64,
    pub(crate) unpacked_bytes_received_kv_cross_zone: AtomicI64,
}

impl Default for ExecDetails {
    fn default() -> Self {
        Self {
            unpacked_bytes_sent_kv_total: AtomicI64::new(0),
            unpacked_bytes_sent_kv_cross_zone: AtomicI64::new(0),
            unpacked_bytes_received_kv_total: AtomicI64::new(0),
            unpacked_bytes_received_kv_cross_zone: AtomicI64::new(0),
        }
    }
}

impl ExecDetails {
    fn add_sent_kv(&self, bytes: usize, is_cross_zone: bool) {
        let bytes = bytes as i64;
        self.unpacked_bytes_sent_kv_total
            .fetch_add(bytes, Ordering::SeqCst);
        if is_cross_zone {
            self.unpacked_bytes_sent_kv_cross_zone
                .fetch_add(bytes, Ordering::SeqCst);
        }
    }

    fn add_received_kv(&self, bytes: usize, is_cross_zone: bool) {
        let bytes = bytes as i64;
        self.unpacked_bytes_received_kv_total
            .fetch_add(bytes, Ordering::SeqCst);
        if is_cross_zone {
            self.unpacked_bytes_received_kv_cross_zone
                .fetch_add(bytes, Ordering::SeqCst);
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct NetworkCollector {
    pub(crate) stale_read: bool,
}

impl NetworkCollector {
    pub(crate) fn on_req<Req: Message>(
        &self,
        req: Option<&Req>,
        access_location: AccessLocation,
        details: Option<&ExecDetails>,
    ) {
        let Some(req) = req else {
            return;
        };

        let size = req.encoded_len();
        if size == 0 {
            return;
        }

        let is_cross_zone = access_location.is_cross_zone();
        if let Some(details) = details {
            details.add_sent_kv(size, is_cross_zone);
        }

        if self.stale_read {
            crate::stats::observe_stale_read_request(is_cross_zone, size);
        }
    }

    pub(crate) fn on_resp<Req: Message, Resp: Message>(
        &self,
        _req: Option<&Req>,
        resp: Option<&Resp>,
        access_location: AccessLocation,
        details: Option<&ExecDetails>,
    ) {
        let Some(resp) = resp else {
            return;
        };

        let size = resp.encoded_len();
        if size == 0 {
            return;
        }

        let is_cross_zone = access_location.is_cross_zone();
        if let Some(details) = details {
            details.add_received_kv(size, is_cross_zone);
        }

        if self.stale_read {
            crate::stats::observe_stale_read_response(is_cross_zone, size);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::kvrpcpb;

    #[test]
    fn network_collector_on_req_accumulates_exec_details_bytes() {
        let details = ExecDetails::default();

        let req1 = kvrpcpb::GetRequest {
            context: Some(kvrpcpb::Context {
                busy_threshold_ms: 50,
                ..Default::default()
            }),
            key: b"key".to_vec(),
            ..Default::default()
        };

        let req2 = kvrpcpb::GetRequest {
            context: Some(kvrpcpb::Context {
                stale_read: true,
                ..Default::default()
            }),
            key: b"key".to_vec(),
            ..Default::default()
        };

        NetworkCollector { stale_read: false }.on_req(
            Some(&req1),
            AccessLocation::LocalZone,
            Some(&details),
        );
        assert_eq!(
            details
                .unpacked_bytes_sent_kv_total
                .load(Ordering::SeqCst),
            10
        );
        assert_eq!(
            details
                .unpacked_bytes_sent_kv_cross_zone
                .load(Ordering::SeqCst),
            0
        );

        NetworkCollector { stale_read: false }.on_req(
            Some(&req2),
            AccessLocation::LocalZone,
            Some(&details),
        );
        assert_eq!(
            details
                .unpacked_bytes_sent_kv_total
                .load(Ordering::SeqCst),
            20
        );
    }

    #[cfg(feature = "prometheus")]
    #[test]
    fn network_collector_on_req_records_stale_read_metrics() {
        let details = ExecDetails::default();

        let req1 = kvrpcpb::GetRequest {
            context: Some(kvrpcpb::Context {
                busy_threshold_ms: 50,
                ..Default::default()
            }),
            key: b"key".to_vec(),
            ..Default::default()
        };

        let req2 = kvrpcpb::GetRequest {
            context: Some(kvrpcpb::Context {
                stale_read: true,
                ..Default::default()
            }),
            key: b"key".to_vec(),
            ..Default::default()
        };

        crate::metrics::register();
        let before_out = crate::stats::stale_read_out_bytes(false);
        let before_req = crate::stats::stale_read_req_count(false);

        NetworkCollector { stale_read: false }.on_req(
            Some(&req1),
            AccessLocation::LocalZone,
            Some(&details),
        );
        assert_eq!(
            crate::stats::stale_read_out_bytes(false),
            before_out,
            "non-stale request should not update stale-read bytes metrics"
        );
        assert_eq!(
            crate::stats::stale_read_req_count(false),
            before_req,
            "non-stale request should not update stale-read req counter"
        );

        NetworkCollector { stale_read: true }.on_req(
            Some(&req2),
            AccessLocation::LocalZone,
            Some(&details),
        );

        assert_eq!(
            crate::stats::stale_read_out_bytes(false) - before_out,
            10
        );
        assert_eq!(crate::stats::stale_read_req_count(false) - before_req, 1);
    }

    #[test]
    fn network_collector_on_resp_accumulates_exec_details_bytes() {
        let details = ExecDetails::default();

        let req = kvrpcpb::GetRequest {
            key: b"key".to_vec(),
            ..Default::default()
        };
        let resp1 = kvrpcpb::GetResponse {
            value: b"value".to_vec(),
            ..Default::default()
        };
        let resp2 = kvrpcpb::GetResponse {
            value: b"stale-value".to_vec(),
            ..Default::default()
        };

        NetworkCollector { stale_read: false }.on_resp(
            Some(&req),
            Some(&resp1),
            AccessLocation::LocalZone,
            Some(&details),
        );

        assert_eq!(
            details
                .unpacked_bytes_received_kv_total
                .load(Ordering::SeqCst),
            7
        );
        assert_eq!(
            details
                .unpacked_bytes_received_kv_cross_zone
                .load(Ordering::SeqCst),
            0
        );

        NetworkCollector { stale_read: false }.on_resp(
            Some(&req),
            Some(&resp2),
            AccessLocation::LocalZone,
            Some(&details),
        );
        assert_eq!(
            details
                .unpacked_bytes_received_kv_total
                .load(Ordering::SeqCst),
            20
        );
    }

    #[test]
    fn network_collector_cross_zone_tracks_cross_zone_bytes() {
        let details = ExecDetails::default();

        let req = kvrpcpb::GetRequest {
            context: Some(kvrpcpb::Context {
                busy_threshold_ms: 50,
                ..Default::default()
            }),
            key: b"key".to_vec(),
            ..Default::default()
        };
        let resp = kvrpcpb::GetResponse {
            value: b"value".to_vec(),
            ..Default::default()
        };

        NetworkCollector { stale_read: false }.on_req(
            Some(&req),
            AccessLocation::CrossZone,
            Some(&details),
        );
        assert_eq!(
            details
                .unpacked_bytes_sent_kv_total
                .load(Ordering::SeqCst),
            10
        );
        assert_eq!(
            details
                .unpacked_bytes_sent_kv_cross_zone
                .load(Ordering::SeqCst),
            10
        );

        NetworkCollector { stale_read: false }.on_resp(
            Some(&req),
            Some(&resp),
            AccessLocation::CrossZone,
            Some(&details),
        );
        assert_eq!(
            details
                .unpacked_bytes_received_kv_total
                .load(Ordering::SeqCst),
            7
        );
        assert_eq!(
            details
                .unpacked_bytes_received_kv_cross_zone
                .load(Ordering::SeqCst),
            7
        );
    }

    #[cfg(feature = "prometheus")]
    #[test]
    fn network_collector_on_resp_records_stale_read_metrics() {
        let details = ExecDetails::default();

        let req = kvrpcpb::GetRequest {
            key: b"key".to_vec(),
            ..Default::default()
        };
        let resp1 = kvrpcpb::GetResponse {
            value: b"value".to_vec(),
            ..Default::default()
        };
        let resp2 = kvrpcpb::GetResponse {
            value: b"stale-value".to_vec(),
            ..Default::default()
        };

        crate::metrics::register();
        let before_in = crate::stats::stale_read_in_bytes(false);

        NetworkCollector { stale_read: false }.on_resp(
            Some(&req),
            Some(&resp1),
            AccessLocation::LocalZone,
            Some(&details),
        );
        assert_eq!(
            crate::stats::stale_read_in_bytes(false),
            before_in,
            "non-stale response should not update stale-read bytes metrics"
        );

        NetworkCollector { stale_read: true }.on_resp(
            Some(&req),
            Some(&resp2),
            AccessLocation::LocalZone,
            Some(&details),
        );

        assert_eq!(
            details
                .unpacked_bytes_received_kv_total
                .load(Ordering::SeqCst),
            20
        );
        assert_eq!(crate::stats::stale_read_in_bytes(false) - before_in, 13);
    }
}
