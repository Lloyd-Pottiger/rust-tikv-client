use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

/// Format used by client-go `gc_worker` to store times.
///
/// This is a Go-style layout string and is provided for parity and diagnostics.
pub const GC_TIME_FORMAT: &str = "20060102-15:04:05.000 -0700";

/// Errors returned by [`compatible_parse_gc_time`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("string \"{value}\" doesn't has a prefix that matches format \"{GC_TIME_FORMAT}\"")]
pub struct GcTimeParseError {
    value: String,
}

impl GcTimeParseError {
    /// Returns the original input value.
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// Parses a GC time string saved by client-go `gc_worker`.
///
/// This mirrors client-go `util.CompatibleParseGCTime` and is compatible with:
/// - optional fractional seconds (`.000`)
/// - an optional trailing field after the timezone offset (for example, `CST`/`MST`/`+08`)
/// - a trailing space
pub fn compatible_parse_gc_time(value: &str) -> Result<SystemTime, GcTimeParseError> {
    parse_gc_time_prefix(value)
        .or_else(|_| {
            let parts: Vec<&str> = value.split(' ').collect();
            let prefix = parts[..parts.len().saturating_sub(1)].join(" ");
            parse_gc_time_prefix(&prefix)
        })
        .map_err(|_| GcTimeParseError {
            value: value.to_owned(),
        })
}

fn parse_gc_time_prefix(value: &str) -> Result<SystemTime, ()> {
    let bytes = value.as_bytes();
    if bytes.len() < 23 {
        return Err(());
    }

    if bytes.get(8) != Some(&b'-') || bytes.get(11) != Some(&b':') || bytes.get(14) != Some(&b':') {
        return Err(());
    }

    let year: i64 = value[0..4].parse().map_err(|_| ())?;
    let month: u32 = value[4..6].parse().map_err(|_| ())?;
    let day: u32 = value[6..8].parse().map_err(|_| ())?;
    let hour: u32 = value[9..11].parse().map_err(|_| ())?;
    let minute: u32 = value[12..14].parse().map_err(|_| ())?;
    let second: u32 = value[15..17].parse().map_err(|_| ())?;

    if !(1..=12).contains(&month) || hour > 23 || minute > 59 || second > 59 {
        return Err(());
    }
    let max_day = days_in_month(year, month);
    if day == 0 || day > max_day {
        return Err(());
    }

    let mut pos = 17;
    let mut nanos: u32 = 0;
    if bytes.get(pos) == Some(&b'.') {
        pos += 1;
        let start = pos;
        while let Some(&b) = bytes.get(pos) {
            if b == b' ' {
                break;
            }
            if !b.is_ascii_digit() {
                return Err(());
            }
            pos += 1;
        }
        if start == pos {
            return Err(());
        }
        let frac = &value[start..pos];
        if frac.len() > 9 {
            return Err(());
        }
        let mut frac_nanos: u32 = frac.parse().map_err(|_| ())?;
        for _ in frac.len()..9 {
            frac_nanos = frac_nanos.saturating_mul(10);
        }
        nanos = frac_nanos;
    }

    if bytes.get(pos) != Some(&b' ') {
        return Err(());
    }
    pos += 1;

    if bytes.len() != pos + 5 {
        return Err(());
    }

    let sign = match bytes.get(pos) {
        Some(b'+') => 1i64,
        Some(b'-') => -1i64,
        _ => return Err(()),
    };
    let offset_hours: i64 = value[pos + 1..pos + 3].parse().map_err(|_| ())?;
    let offset_minutes: i64 = value[pos + 3..pos + 5].parse().map_err(|_| ())?;
    if !(0..=23).contains(&offset_hours) || !(0..=59).contains(&offset_minutes) {
        return Err(());
    }
    let offset_seconds = offset_hours
        .checked_mul(3600)
        .and_then(|v| {
            offset_minutes
                .checked_mul(60)
                .and_then(|m| v.checked_add(m))
        })
        .and_then(|v| sign.checked_mul(v))
        .ok_or(())?;

    let days = days_from_civil(year, month, day);
    let mut unix_seconds = days
        .checked_mul(86_400)
        .and_then(|v| {
            i64::from(hour)
                .checked_mul(3600)
                .and_then(|h| v.checked_add(h))
        })
        .and_then(|v| {
            i64::from(minute)
                .checked_mul(60)
                .and_then(|m| v.checked_add(m))
        })
        .and_then(|v| v.checked_add(i64::from(second)))
        .ok_or(())?;
    unix_seconds = unix_seconds.checked_sub(offset_seconds).ok_or(())?;

    unix_seconds_nanos_to_system_time(unix_seconds, nanos)
}

fn unix_seconds_nanos_to_system_time(unix_seconds: i64, nanos: u32) -> Result<SystemTime, ()> {
    if nanos >= 1_000_000_000 {
        return Err(());
    }

    let nanos = u64::from(nanos);
    if unix_seconds >= 0 {
        UNIX_EPOCH
            .checked_add(Duration::from_secs(unix_seconds as u64))
            .and_then(|t| t.checked_add(Duration::from_nanos(nanos)))
            .ok_or(())
    } else {
        UNIX_EPOCH
            .checked_sub(Duration::from_secs((-unix_seconds) as u64))
            .and_then(|t| t.checked_add(Duration::from_nanos(nanos)))
            .ok_or(())
    }
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

// Ported from Howard Hinnant's "days from civil" algorithm.
//
// Returns days since 1970-01-01.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = year - i64::from(month <= 2);
    let era = if y >= 0 { y } else { y - 399 }.div_euclid(400);
    let yoe = y - era * 400;
    let m = i64::from(month);
    let d = i64::from(day);
    let mp = m + if m > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2).div_euclid(5) + d - 1;
    let doe = yoe * 365 + yoe.div_euclid(4) - yoe.div_euclid(100) + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compatible_parse_gc_time() {
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

        let expected = UNIX_EPOCH
            .checked_add(Duration::from_secs(1_545_134_017))
            .expect("expected system time");

        for value in values {
            let parsed = compatible_parse_gc_time(value).expect("parse should succeed");
            assert_eq!(parsed, expected, "value={value:?}");
        }

        for value in invalid_values {
            assert!(compatible_parse_gc_time(value).is_err(), "value={value:?}");
        }
    }
}
