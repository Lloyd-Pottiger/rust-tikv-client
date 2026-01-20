// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Utilities for parsing/formatting GC time strings.
//!
//! This ports client-go's `util.CompatibleParseGCTime` behavior, which is used to load gc_worker
//! timestamps saved in a historically-compatible format. The Go implementation:
//! - parses the "old" layout (`20060102-15:04:05 -0700`) which also accepts fractional seconds;
//! - if parsing fails, drops the last space-separated field and tries again.
//!
//! This allows inputs like:
//! - `20181218-19:53:37 +0800 CST`
//! - `20181218-19:53:37.000 +0800 +08`

use std::fmt;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

// client-go's `GCTimeFormat`.
const GC_TIME_FORMAT: &str = "20060102-15:04:05.000 -0700";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GcTimeParseError {
    value: String,
}

impl fmt::Display for GcTimeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Keep the message close to client-go for easier cross-referencing.
        write!(
            f,
            "string {:?} doesn't have a prefix that matches format {:?}",
            self.value, GC_TIME_FORMAT
        )
    }
}

impl std::error::Error for GcTimeParseError {}

/// Parse a gc_worker time string, matching client-go's compatibility behavior.
pub(crate) fn compatible_parse_gc_time(value: &str) -> Result<SystemTime, GcTimeParseError> {
    if let Ok(t) = parse_gc_time_old(value) {
        return Ok(t);
    }

    // Drop the last space-separated field and retry (timezone name, `+08`, trailing space, ...).
    if let Some((prefix, _suffix)) = value.rsplit_once(' ') {
        if let Ok(t) = parse_gc_time_old(prefix) {
            return Ok(t);
        }
    }

    Err(GcTimeParseError {
        value: value.to_owned(),
    })
}

fn parse_gc_time_old(value: &str) -> Result<SystemTime, ()> {
    let (datetime, offset) = value.split_once(' ').ok_or(())?;

    // Reject extra fields: the "old" parse must match the full string exactly.
    if offset.contains(' ') {
        return Err(());
    }

    let (year, month, day, hour, minute, second, nanos) = parse_datetime(datetime)?;
    let offset_secs = parse_offset_seconds(offset)?;

    let days = days_from_civil(year, month, day);
    let local_secs = days
        .checked_mul(86_400)
        .and_then(|v| v.checked_add(hour as i64 * 3600 + minute as i64 * 60 + second as i64))
        .ok_or(())?;

    // The timestamp is in local time with a known offset.
    let utc_secs = local_secs.checked_sub(offset_secs).ok_or(())?;
    system_time_from_unix_timestamp(utc_secs, nanos)
}

type DateTimeParts = (i32, u32, u32, u32, u32, u32, u32);

fn parse_datetime(s: &str) -> Result<DateTimeParts, ()> {
    let bytes = s.as_bytes();
    if bytes.len() < 17 {
        return Err(());
    }

    let year = parse_u32_fixed(bytes, 0, 4).ok_or(())? as i32;
    let month = parse_u32_fixed(bytes, 4, 2).ok_or(())?;
    let day = parse_u32_fixed(bytes, 6, 2).ok_or(())?;
    if bytes.get(8).copied() != Some(b'-') {
        return Err(());
    }
    let hour = parse_u32_fixed(bytes, 9, 2).ok_or(())?;
    if bytes.get(11).copied() != Some(b':') {
        return Err(());
    }
    let minute = parse_u32_fixed(bytes, 12, 2).ok_or(())?;
    if bytes.get(14).copied() != Some(b':') {
        return Err(());
    }
    let second = parse_u32_fixed(bytes, 15, 2).ok_or(())?;

    // Basic range checks to avoid weird overflows.
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(());
    }
    if hour > 23 || minute > 59 || second > 59 {
        return Err(());
    }

    if bytes.len() == 17 {
        return Ok((year, month, day, hour, minute, second, 0));
    }

    if bytes.get(17).copied() != Some(b'.') {
        return Err(());
    }

    let mut i = 18;
    let mut frac: u32 = 0;
    let mut digits: u32 = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if !b.is_ascii_digit() {
            return Err(());
        }
        if digits == 9 {
            return Err(());
        }
        frac = frac * 10 + (b - b'0') as u32;
        digits += 1;
        i += 1;
    }
    if digits == 0 {
        return Err(());
    }

    // Scale the parsed fraction to nanoseconds.
    let nanos = frac * 10u32.pow(9 - digits);
    Ok((year, month, day, hour, minute, second, nanos))
}

fn parse_offset_seconds(s: &str) -> Result<i64, ()> {
    let bytes = s.as_bytes();
    if bytes.len() != 5 {
        return Err(());
    }
    let sign = match bytes[0] {
        b'+' => 1i64,
        b'-' => -1i64,
        _ => return Err(()),
    };
    let hours = parse_u32_fixed(bytes, 1, 2).ok_or(())? as i64;
    let mins = parse_u32_fixed(bytes, 3, 2).ok_or(())? as i64;
    if hours > 23 || mins > 59 {
        return Err(());
    }
    Ok(sign * (hours * 3600 + mins * 60))
}

fn parse_u32_fixed(bytes: &[u8], start: usize, len: usize) -> Option<u32> {
    if start.checked_add(len)? > bytes.len() {
        return None;
    }
    let mut v: u32 = 0;
    for &b in &bytes[start..start + len] {
        if !b.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (b - b'0') as u32;
    }
    Some(v)
}

fn system_time_from_unix_timestamp(secs: i64, nanos: u32) -> Result<SystemTime, ()> {
    let total_nanos = secs as i128 * 1_000_000_000i128 + nanos as i128;
    if total_nanos >= 0 {
        let nanos_u64 = u64::try_from(total_nanos).map_err(|_| ())?;
        Ok(UNIX_EPOCH + Duration::from_nanos(nanos_u64))
    } else {
        let nanos_u64 = u64::try_from(-total_nanos).map_err(|_| ())?;
        Ok(UNIX_EPOCH - Duration::from_nanos(nanos_u64))
    }
}

pub(crate) fn format_gc_time_fixed_offset(t: SystemTime, offset_seconds: i32) -> String {
    let offset_nanos = offset_seconds as i128 * 1_000_000_000i128;
    let utc_nanos: i128 = match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as i128,
        Err(e) => -(e.duration().as_nanos() as i128),
    };
    let local_nanos = utc_nanos + offset_nanos;

    let local_secs = local_nanos.div_euclid(1_000_000_000) as i64;
    let nanos = local_nanos.rem_euclid(1_000_000_000) as u32;
    let millis = nanos / 1_000_000;

    let days = local_secs.div_euclid(86_400);
    let secs_of_day = local_secs.rem_euclid(86_400);

    let (year, month, day) = civil_from_days(days);
    let hour = (secs_of_day / 3600) as u32;
    let minute = ((secs_of_day % 3600) / 60) as u32;
    let second = (secs_of_day % 60) as u32;

    let (sign, off_hours, off_mins) = offset_hhmm(offset_seconds);
    format!(
        "{year:04}{month:02}{day:02}-{hour:02}:{minute:02}:{second:02}.{millis:03} {sign}{off_hours:02}{off_mins:02}",
    )
}

fn offset_hhmm(offset_seconds: i32) -> (char, i32, i32) {
    if offset_seconds >= 0 {
        let s = offset_seconds;
        return ('+', s / 3600, (s % 3600) / 60);
    }
    let s = offset_seconds.saturating_abs();
    ('-', s / 3600, (s % 3600) / 60)
}

// Howard Hinnant date algorithms (public domain): convert civil date <-> days since Unix epoch.
// https://howardhinnant.github.io/date_algorithms.html

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let mut y = year as i64;
    let m = month as i64;
    let d = day as i64;

    y -= if m <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 }.div_euclid(400);
    let yoe = y - era * 400; // [0, 399]
    let mp = m + if m > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2).div_euclid(5) + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe.div_euclid(4) - yoe.div_euclid(100) + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 }.div_euclid(146_097);
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe.div_euclid(1460) + doe.div_euclid(36_524) - doe.div_euclid(146_096))
        .div_euclid(365); // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe.div_euclid(4) - yoe.div_euclid(100));
    let mp = (5 * doy + 2).div_euclid(153); // [0, 11]
    let d = doy - (153 * mp + 2).div_euclid(5) + 1; // [1, 31]
    let m = mp + if mp < 10 { 3 } else { -9 }; // [1, 12]
    let year = y + if m <= 2 { 1 } else { 0 };

    (year as i32, m as u32, d as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compatible_parse_gc_time_matches_client_go_test() {
        let values = [
            "20181218-19:53:37 +0800 CST",
            "20181218-19:53:37 +0800 MST",
            "20181218-19:53:37 +0800 FOO",
            "20181218-19:53:37 +0800 +08",
            "20181218-19:53:37 +0800",
            "20181218-19:53:37 +0800 ",
            "20181218-11:53:37 +0000",
            "20181218-11:53:37.000 +0000",
            "20181218-19:53:37.000 +0800 +08",
        ];

        let invalid_values = [
            "",
            " ",
            "foo",
            "20181218-11:53:37",
            "20181218-19:53:37 +0800CST",
            "20181218-19:53:37 +0800 FOO BAR",
            "20181218-19:53:37 +0800FOOOOOOO BAR",
            "20181218-19:53:37 ",
        ];

        let expected = UNIX_EPOCH + Duration::from_secs(1_545_134_017);
        let expected_formatted = "20181218-19:53:37.000 +0800";

        for value in values {
            let t = compatible_parse_gc_time(value).unwrap();
            assert_eq!(t, expected);
            assert_eq!(format_gc_time_fixed_offset(t, 8 * 3600), expected_formatted);
        }

        for value in invalid_values {
            assert!(compatible_parse_gc_time(value).is_err(), "value={value:?}");
        }
    }
}
