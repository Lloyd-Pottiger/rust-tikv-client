/// Byte size units used for formatting.
const BYTE_SIZE_GB: i64 = 1 << 30;
const BYTE_SIZE_MB: i64 = 1 << 20;
const BYTE_SIZE_KB: i64 = 1 << 10;
const BYTE_SIZE_B: i64 = 1;

/// Format bytes in a human-readable way.
///
/// This mirrors client-go `util.FormatBytes` (unit selection + decimal pruning).
pub fn format_bytes(num_bytes: i64) -> String {
    if num_bytes <= BYTE_SIZE_KB {
        return bytes_to_string(num_bytes);
    }

    let (unit, unit_str) = byte_unit(num_bytes);
    if unit == BYTE_SIZE_B {
        return bytes_to_string(num_bytes);
    }

    let v = num_bytes as f64 / unit as f64;
    let decimal = if num_bytes % unit == 0 {
        0
    } else if v < 10.0 {
        2
    } else {
        1
    };

    match decimal {
        0 => format!("{v:.0} {unit_str}"),
        1 => format!("{v:.1} {unit_str}"),
        2 => format!("{v:.2} {unit_str}"),
        _ => format!("{v} {unit_str}"),
    }
}

/// Convert a byte count into a human-readable string.
///
/// This mirrors client-go `util.BytesToString`.
pub fn bytes_to_string(num_bytes: i64) -> String {
    let gb = num_bytes as f64 / BYTE_SIZE_GB as f64;
    if gb > 1.0 {
        return format!("{gb} GB");
    }

    let mb = num_bytes as f64 / BYTE_SIZE_MB as f64;
    if mb > 1.0 {
        return format!("{mb} MB");
    }

    let kb = num_bytes as f64 / BYTE_SIZE_KB as f64;
    if kb > 1.0 {
        return format!("{kb} KB");
    }

    format!("{num_bytes} Bytes")
}

fn byte_unit(bytes: i64) -> (i64, &'static str) {
    if bytes > BYTE_SIZE_GB {
        (BYTE_SIZE_GB, "GB")
    } else if bytes > BYTE_SIZE_MB {
        (BYTE_SIZE_MB, "MB")
    } else if bytes > BYTE_SIZE_KB {
        (BYTE_SIZE_KB, "KB")
    } else {
        (BYTE_SIZE_B, "Bytes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_to_string_small_values() {
        assert_eq!(bytes_to_string(100), "100 Bytes");
        assert_eq!(bytes_to_string(1024), "1024 Bytes");
    }

    #[test]
    fn test_format_bytes_kb_and_mb() {
        assert_eq!(format_bytes(1024), "1024 Bytes");
        assert_eq!(format_bytes(2048), "2 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");

        assert_eq!(format_bytes(10 * BYTE_SIZE_MB), "10 MB");
        assert_eq!(
            format_bytes(5 * BYTE_SIZE_MB + 512 * BYTE_SIZE_KB),
            "5.50 MB"
        );
    }
}
