use std::fmt;
use std::time::Duration;

use crate::kvrpcpb;

fn format_duration(d: Duration) -> String {
    if d.is_zero() {
        return "0s".to_owned();
    }

    let nanos = d.as_nanos();
    let (unit_nanos, suffix) = if nanos >= 1_000_000_000 {
        (1_000_000_000_u128, "s")
    } else if nanos >= 1_000_000 {
        (1_000_000_u128, "ms")
    } else if nanos >= 1_000 {
        (1_000_u128, "µs")
    } else {
        (1_u128, "ns")
    };

    let raw = nanos as f64 / unit_nanos as f64;
    let rounded = if raw < 10.0 {
        (raw * 100.0).round() / 100.0
    } else {
        (raw * 10.0).round() / 10.0
    };

    // Trim trailing zeros for readability (match Go's Duration.String behavior).
    let mut s = if raw < 10.0 {
        format!("{rounded:.2}")
    } else {
        format!("{rounded:.1}")
    };
    while s.ends_with('0') {
        s.pop();
    }
    if s.ends_with('.') {
        s.pop();
    }
    s.push_str(suffix);
    s
}

fn push(buf: &mut String, key: &'static str, d: Duration) {
    if d.is_zero() {
        return;
    }
    if !buf.is_empty() {
        buf.push_str(", ");
    }
    buf.push_str(key);
    buf.push_str(": ");
    buf.push_str(&format_duration(d));
}

impl fmt::Display for kvrpcpb::TimeDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut buf = String::new();
        push(
            &mut buf,
            "total_process_time",
            Duration::from_millis(self.process_wall_time_ms),
        );
        push(
            &mut buf,
            "total_wait_time",
            Duration::from_millis(self.wait_wall_time_ms),
        );
        push(
            &mut buf,
            "total_kv_read_wall_time",
            Duration::from_millis(self.kv_read_wall_time_ms),
        );
        push(
            &mut buf,
            "tikv_wall_time",
            Duration::from_nanos(self.total_rpc_wall_time_ns),
        );

        if buf.is_empty() {
            return Ok(());
        }
        write!(f, "time_detail: {{{buf}}}")
    }
}

impl fmt::Display for kvrpcpb::TimeDetailV2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut buf = String::new();
        push(
            &mut buf,
            "total_process_time",
            Duration::from_nanos(self.process_wall_time_ns),
        );
        push(
            &mut buf,
            "total_suspend_time",
            Duration::from_nanos(self.process_suspend_wall_time_ns),
        );
        push(
            &mut buf,
            "total_wait_time",
            Duration::from_nanos(self.wait_wall_time_ns),
        );
        push(
            &mut buf,
            "total_kv_read_wall_time",
            Duration::from_nanos(self.kv_read_wall_time_ns),
        );
        push(
            &mut buf,
            "tikv_grpc_process_time",
            Duration::from_nanos(self.kv_grpc_process_time_ns),
        );
        push(
            &mut buf,
            "tikv_grpc_wait_time",
            Duration::from_nanos(self.kv_grpc_wait_time_ns),
        );
        push(
            &mut buf,
            "tikv_wall_time",
            Duration::from_nanos(self.total_rpc_wall_time_ns),
        );

        if buf.is_empty() {
            return Ok(());
        }
        write!(f, "time_detail: {{{buf}}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_detail_display_matches_client_go_time_detail_string() {
        let detail = kvrpcpb::TimeDetail {
            kv_read_wall_time_ms: 2,
            total_rpc_wall_time_ns: 3_000_000,
            ..Default::default()
        };
        assert_eq!(
            detail.to_string(),
            "time_detail: {total_kv_read_wall_time: 2ms, tikv_wall_time: 3ms}"
        );

        let detail = kvrpcpb::TimeDetailV2 {
            process_wall_time_ns: 2_000_000,
            process_suspend_wall_time_ns: 3_000_000,
            wait_wall_time_ns: 4_000_000,
            kv_read_wall_time_ns: 5_000_000,
            kv_grpc_process_time_ns: 6_000_000,
            kv_grpc_wait_time_ns: 7_000_000,
            total_rpc_wall_time_ns: 8_000_000,
        };
        assert_eq!(
            detail.to_string(),
            "time_detail: {total_process_time: 2ms, total_suspend_time: 3ms, total_wait_time: 4ms, total_kv_read_wall_time: 5ms, tikv_grpc_process_time: 6ms, tikv_grpc_wait_time: 7ms, tikv_wall_time: 8ms}"
        );
    }
}
