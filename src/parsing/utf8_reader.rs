//! Cursor-based zero-allocation UTF-8 JSON field reader (`BinanceUtf8Reader` equivalent).
//! Checks right at the cursor first (almost always a hit), falling back to SIMD `memmem`.

use memchr::{memchr, memchr2, memmem};

use crate::parsing::fast_decimal::{parse_decimal_fast, parse_i64_fast};
use crate::primitives::Symbol;

pub const OBJECT_START: usize = 1;

#[inline(always)]
fn find_value(data: &[u8], key: &[u8], cursor: usize) -> Option<usize> {
    let mut pos = cursor;
    if pos < data.len() && unsafe { *data.get_unchecked(pos) } == b',' {
        pos += 1;
    }
    if data.len().saturating_sub(pos) >= key.len() && &data[pos..pos + key.len()] == key {
        return Some(pos + key.len());
    }
    find_value_slow(data, key, cursor)
}

#[cold]
#[inline(never)]
fn find_value_slow(data: &[u8], key: &[u8], cursor: usize) -> Option<usize> {
    if cursor < data.len() {
        if let Some(ahead) = memmem::find(&data[cursor..], key) {
            return Some(cursor + ahead + key.len());
        }
    }
    memmem::find(data, key).map(|idx| idx + key.len())
}

#[inline(always)]
pub fn read_i64(data: &[u8], key: &[u8], cursor: &mut usize) -> Option<i64> {
    let start = find_value(data, key, *cursor)?;
    let (val, consumed) = parse_i64_fast(&data[start..])?;
    *cursor = start + consumed;
    Some(val)
}

#[inline(always)]
pub fn read_quoted_f64(data: &[u8], key: &[u8], cursor: &mut usize) -> Option<f64> {
    let start = find_value(data, key, *cursor)?;
    if start >= data.len() || unsafe { *data.get_unchecked(start) } != b'"' {
        return None;
    }
    let (val, consumed) = parse_decimal_fast(&data[start + 1..])?;
    let end = start + 1 + consumed;
    if end >= data.len() || unsafe { *data.get_unchecked(end) } != b'"' {
        return None;
    }
    *cursor = end + 1;
    Some(val)
}

#[inline(always)]
pub fn read_symbol(data: &[u8], key: &[u8], cursor: &mut usize) -> Option<Symbol> {
    let start = find_value(data, key, *cursor)?;
    if start >= data.len() || unsafe { *data.get_unchecked(start) } != b'"' {
        return None;
    }
    let rest = &data[start + 1..];
    let len = memchr(b'"', rest)?;
    let sym = Symbol::try_from_bytes(&rest[..len])?;
    *cursor = start + len + 2;
    Some(sym)
}

#[inline(always)]
pub fn read_bool(data: &[u8], key: &[u8], cursor: &mut usize) -> Option<bool> {
    let start = find_value(data, key, *cursor)?;
    let rest = &data[start..];
    if rest.starts_with(b"true") {
        *cursor = start + 4;
        Some(true)
    } else if rest.starts_with(b"false") {
        *cursor = start + 5;
        Some(false)
    } else {
        None
    }
}

#[inline(always)]
pub fn seek_str<'a>(data: &'a [u8], key: &[u8], cursor: &mut usize) -> Option<&'a [u8]> {
    let start = find_value(data, key, *cursor)?;
    if start >= data.len() || unsafe { *data.get_unchecked(start) } != b'"' {
        return None;
    }
    let rest = &data[start + 1..];
    let len = memchr(b'"', rest)?;
    *cursor = start + len + 2;
    Some(&rest[..len])
}

#[inline(always)]
pub fn seek_value(data: &[u8], key: &[u8], cursor: &mut usize) -> bool {
    if let Some(start) = find_value(data, key, *cursor) {
        *cursor = start;
        true
    } else {
        false
    }
}

#[inline(always)]
pub fn skip_optional(data: &[u8], key: &[u8], cursor: &mut usize) {
    let mut pos = *cursor;
    if pos < data.len() && unsafe { *data.get_unchecked(pos) } == b',' {
        pos += 1;
    }
    if data.len().saturating_sub(pos) < key.len() || &data[pos..pos + key.len()] != key {
        return;
    }
    pos += key.len();
    if pos >= data.len() {
        return;
    }
    if data[pos] == b'"' {
        if let Some(len) = memchr(b'"', &data[pos + 1..]) {
            *cursor = pos + len + 2;
        }
        return;
    }
    if let Some(end) = memchr2(b',', b'}', &data[pos..]) {
        *cursor = pos + end;
    }
}

