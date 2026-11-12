//! Exact Clinger fast-path decimal parser, fast integer parser, and plain decimal formatter.

const MAX_EXACT_MANTISSA: u64 = 1u64 << 53;
const MAX_EXACT_POWER_OF_TEN: usize = 22;
const MAX_DIGITS: usize = 19;

static POWERS_OF_TEN: [f64; 23] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
];

/// Parses a plain decimal number from UTF-8 bytes into an `f64`, returning `(value, bytes_consumed)`.
/// Uses the Clinger fast path (exact integer mantissa / exact power of 10) and falls back to `fast_float2`.
#[inline]
pub fn parse_decimal_fast(text: &[u8]) -> Option<(f64, usize)> {
    let len = text.len();
    if len == 0 {
        return None;
    }

    let mut index = 0usize;
    let is_negative = text[0] == b'-';
    if is_negative {
        index = 1;
    }

    let mut mantissa: u64 = 0;
    let mut significant_digits: usize = 0;

    let int_start = index;
    // Unchecked pointer walk bounded by `len` eliminates bounds checks in the inner loop.
    while index < len {
        let b = unsafe { *text.get_unchecked(index) };
        let digit = b.wrapping_sub(b'0');
        if digit > 9 {
            break;
        }
        mantissa = mantissa.wrapping_mul(10).wrapping_add(digit as u64);
        significant_digits += (mantissa != 0) as usize;
        index += 1;
    }
    let int_digits = index - int_start;
    let mut frac_digits = 0usize;

    if index < len && unsafe { *text.get_unchecked(index) } == b'.' {
        index += 1;
        let frac_start = index;
        while index < len {
            let b = unsafe { *text.get_unchecked(index) };
            let digit = b.wrapping_sub(b'0');
            if digit > 9 {
                break;
            }
            mantissa = mantissa.wrapping_mul(10).wrapping_add(digit as u64);
            significant_digits += (mantissa != 0) as usize;
            index += 1;
        }
        frac_digits = index - frac_start;
    }

    if int_digits + frac_digits == 0
        || significant_digits > MAX_DIGITS
        || mantissa > MAX_EXACT_MANTISSA
        || frac_digits > MAX_EXACT_POWER_OF_TEN
        || (index < len && (unsafe { *text.get_unchecked(index) } | 0x20) == b'e')
    {
        return parse_decimal_slow(text);
    }

    let result = (mantissa as f64) / unsafe { *POWERS_OF_TEN.get_unchecked(frac_digits) };
    Some((if is_negative { -result } else { result }, index))
}

#[cold]
#[inline(never)]
fn parse_decimal_slow(text: &[u8]) -> Option<(f64, usize)> {
    fast_float2::parse_partial::<f64, _>(text).ok()
}

/// Fast ASCII signed 64-bit integer parser.
#[inline]
pub fn parse_i64_fast(text: &[u8]) -> Option<(i64, usize)> {
    let len = text.len();
    if len == 0 {
        return None;
    }

    let mut index = 0usize;
    let neg = text[0] == b'-';
    if neg || text[0] == b'+' {
        index = 1;
        if index >= len {
            return None;
        }
    }

    let start = index;
    let mut acc: u64 = 0;

    while index < len {
        let d = unsafe { *text.get_unchecked(index) }.wrapping_sub(b'0');
        if d > 9 {
            break;
        }
        acc = acc.wrapping_mul(10).wrapping_add(d as u64);
        index += 1;
    }

    let digits = index - start;
    if digits == 0 {
        return None;
    }

    // Overflow guard for >= 19 digits.
    if digits >= 19 {
        return parse_i64_slow(&text[..index]).map(|v| (v, index));
    }

    let val = if neg {
        (acc as i64).wrapping_neg()
    } else {
        acc as i64
    };
    Some((val, index))
}

#[cold]
#[inline(never)]
fn parse_i64_slow(slice: &[u8]) -> Option<i64> {
    core::str::from_utf8(slice).ok()?.parse::<i64>().ok()
}

/// Fast ASCII unsigned integer formatter onto a byte slice.
#[inline]
pub fn format_u64(mut v: u64, dst: &mut [u8]) -> usize {
    if v == 0 {
        dst[0] = b'0';
        return 1;
    }
    let mut buf = [0u8; 20];
    let mut pos = 20;
    while v > 0 {
        pos -= 1;
        buf[pos] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    let len = 20 - pos;
    dst[..len].copy_from_slice(&buf[pos..]);
    len
}

#[inline]
pub fn format_i64(v: i64, dst: &mut [u8]) -> usize {
    if v < 0 {
        dst[0] = b'-';
        1 + format_u64(v.wrapping_neg() as u64, &mut dst[1..])
    } else {
        format_u64(v as u64, dst)
    }
}

/// Formats an `f64` as plain decimal UTF-8 text (no scientific notation), rounded to 15 significant digits.
/// Eliminates binary noise like `0.30000000000000004` -> `0.3`, matching `PlainDecimalFormatter`.
#[inline]
pub fn format_plain_decimal(value: f64, dst: &mut [u8]) -> Option<usize> {
    if !value.is_finite() {
        return None;
    }
    if value == 0.0 {
        if dst.is_empty() {
            return None;
        }
        dst[0] = b'0';
        return Some(1);
    }

    let neg = value < 0.0;
    let abs_val = value.abs();

    // Fast path for integers up to 10^15 (very common for prices like 60000.0).
    if abs_val < 1e15 && abs_val.fract() == 0.0 {
        let mut pos = 0;
        if neg {
            if dst.is_empty() {
                return None;
            }
            dst[0] = b'-';
            pos = 1;
        }
        let n = format_u64(abs_val as u64, &mut dst[pos..]);
        return Some(pos + n);
    }

    // Round to 15 significant digits via scale factor, or format with ryu and round digits.
    let mut ryu_buf = ryu::Buffer::new();
    let s = ryu_buf.format_finite(abs_val).as_bytes();

    // Extract significant digits and decimal exponent from ryu output.
    let mut digits = [0u8; 32];
    let mut digit_count = 0usize;
    let mut point_pos: Option<usize> = None;
    let mut exp: i32 = 0;
    let mut i = 0usize;

    while i < s.len() {
        let b = s[i];
        if b == b'e' || b == b'E' {
            i += 1;
            let (e, _) = parse_i64_fast(&s[i..])?;
            exp = e as i32;
            break;
        } else if b == b'.' {
            point_pos = Some(digit_count);
        } else if digit_count < digits.len() {
            digits[digit_count] = b;
            digit_count += 1;
        }
        i += 1;
    }

    // Strip leading zeros to find first non-zero digit.
    let mut first_nz = 0usize;
    while first_nz < digit_count && digits[first_nz] == b'0' {
        first_nz += 1;
    }
    if first_nz == digit_count {
        dst[0] = b'0';
        return Some(1);
    }

    let orig_point = point_pos.unwrap_or(digit_count) as i32;
    // `point_at` is the position of the decimal point relative to `digits[first_nz..]`.
    let mut point_at = (orig_point - first_nz as i32) + exp;
    let sig_src = &mut digits[first_nz..digit_count];
    let mut sig_len = sig_src.len();

    // Round to 15 significant digits if longer.
    if sig_len > 15 {
        let round_up = sig_src[15] >= b'5';
        sig_len = 15;
        if round_up {
            let mut k = 15;
            let mut carry = true;
            while k > 0 && carry {
                k -= 1;
                if sig_src[k] == b'9' {
                    sig_src[k] = b'0';
                } else {
                    sig_src[k] += 1;
                    carry = false;
                }
            }
            if carry {
                sig_src[0] = b'1';
                sig_len = 1;
                point_at += 1;
            }
        }
    }

    // Trim trailing zeros.
    while sig_len > 1 && sig_src[sig_len - 1] == b'0' {
        sig_len -= 1;
    }

    let mut pos = 0usize;
    if neg {
        if dst.len() <= pos {
            return None;
        }
        dst[pos] = b'-';
        pos += 1;
    }

    if point_at <= 0 {
        let zeros = (-point_at) as usize;
        if dst.len() < pos + 2 + zeros + sig_len {
            return None;
        }
        dst[pos] = b'0';
        dst[pos + 1] = b'.';
        pos += 2;
        dst[pos..pos + zeros].fill(b'0');
        pos += zeros;
        dst[pos..pos + sig_len].copy_from_slice(&sig_src[..sig_len]);
        pos += sig_len;
    } else if sig_len <= point_at as usize {
        let zeros = point_at as usize - sig_len;
        if dst.len() < pos + sig_len + zeros {
            return None;
        }
        dst[pos..pos + sig_len].copy_from_slice(&sig_src[..sig_len]);
        pos += sig_len;
        dst[pos..pos + zeros].fill(b'0');
        pos += zeros;
    } else {
        let p = point_at as usize;
        if dst.len() < pos + sig_len + 1 {
            return None;
        }
        dst[pos..pos + p].copy_from_slice(&sig_src[..p]);
        pos += p;
        dst[pos] = b'.';
        pos += 1;
        dst[pos..pos + (sig_len - p)].copy_from_slice(&sig_src[p..sig_len]);
        pos += sig_len - p;
    }

    Some(pos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fast_decimal_exact() {
        assert_eq!(parse_decimal_fast(b"67123.45"), Some((67123.45, 8)));
        assert_eq!(parse_decimal_fast(b"0.001"), Some((0.001, 5)));
        assert_eq!(parse_decimal_fast(b"-12.5"), Some((-12.5, 5)));
        assert_eq!(parse_decimal_fast(b"1e-7"), Some((1e-7, 4)));
    }

    #[test]
    fn test_plain_decimal_formatter() {
        let mut buf = [0u8; 64];
        let n = format_plain_decimal(0.1 + 0.2, &mut buf).unwrap();
        assert_eq!(&buf[..n], b"0.3");
        let n2 = format_plain_decimal(60000.0, &mut buf).unwrap();
        assert_eq!(&buf[..n2], b"60000");
        let n3 = format_plain_decimal(1e-7, &mut buf).unwrap();
        assert_eq!(&buf[..n3], b"0.0000001");
    }
}

