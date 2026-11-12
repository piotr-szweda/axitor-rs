//! Zero-copy, zero-allocation streaming UTF-8 JSON scanner for richer cold-path / REST / WS-API payloads.
//! Uses `memchr` SIMD scanning directly over `&'a [u8]` with zero heap allocations.

use memchr::memchr;

use crate::parsing::fast_decimal::{parse_decimal_fast, parse_i64_fast};
use crate::primitives::{ClientOrderId, Symbol};

pub struct JsonScanner<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> JsonScanner<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    #[inline(always)]
    pub fn pos(&self) -> usize {
        self.pos
    }

    #[inline(always)]
    pub fn skip_ws(&mut self) {
        while self.pos < self.buf.len() {
            match unsafe { *self.buf.get_unchecked(self.pos) } {
                b' ' | b'\t' | b'\r' | b'\n' => self.pos += 1,
                _ => break,
            }
        }
    }

    #[inline(always)]
    pub fn peek(&mut self) -> Option<u8> {
        self.skip_ws();
        self.buf.get(self.pos).copied()
    }

    #[inline(always)]
    pub fn expect(&mut self, byte: u8) -> bool {
        self.skip_ws();
        if self.pos < self.buf.len() && unsafe { *self.buf.get_unchecked(self.pos) } == byte {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    #[inline(always)]
    pub fn next_comma_or_end(&mut self, end: u8) -> Result<bool, ()> {
        self.skip_ws();
        if self.pos >= self.buf.len() {
            return Err(());
        }
        let b = unsafe { *self.buf.get_unchecked(self.pos) };
        if b == b',' {
            self.pos += 1;
            Ok(true)
        } else if b == end {
            self.pos += 1;
            Ok(false)
        } else {
            Err(())
        }
    }

    /// Reads a JSON string slice (un-escaped fast path via `memchr`).
    #[inline(always)]
    pub fn read_str_bytes(&mut self) -> Option<&'a [u8]> {
        self.skip_ws();
        if self.pos >= self.buf.len() || self.buf[self.pos] != b'"' {
            return None;
        }
        let start = self.pos + 1;
        let mut scan = start;
        loop {
            let offset = memchr(b'"', &self.buf[scan..])?;
            let q = scan + offset;
            // Fast path: check preceding byte for backslash escape.
            if q > start && unsafe { *self.buf.get_unchecked(q - 1) } == b'\\' {
                let mut slashes = 0usize;
                let mut k = q;
                while k > start && unsafe { *self.buf.get_unchecked(k - 1) } == b'\\' {
                    slashes += 1;
                    k -= 1;
                }
                if slashes & 1 == 1 {
                    scan = q + 1;
                    continue;
                }
            }
            self.pos = q + 1;
            return Some(&self.buf[start..q]);
        }
    }

    /// Reads an object property key and consumes the following `:`.
    #[inline(always)]
    pub fn read_key(&mut self) -> Option<&'a [u8]> {
        let key = self.read_str_bytes()?;
        if !self.expect(b':') {
            return None;
        }
        Some(key)
    }

    /// Consumes `null` if present.
    #[inline(always)]
    pub fn try_consume_null(&mut self) -> bool {
        self.skip_ws();
        if self.buf[self.pos..].starts_with(b"null") {
            self.pos += 4;
            true
        } else {
            false
        }
    }

    /// Reads a raw JSON number token slice.
    #[inline(always)]
    pub fn read_number_bytes(&mut self) -> Option<&'a [u8]> {
        self.skip_ws();
        let start = self.pos;
        while self.pos < self.buf.len() {
            match unsafe { *self.buf.get_unchecked(self.pos) } {
                b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E' => self.pos += 1,
                _ => break,
            }
        }
        if self.pos > start {
            Some(&self.buf[start..self.pos])
        } else {
            None
        }
    }

    /// Reads an `i64` whether encoded as a bare JSON number or quoted string.
    #[inline(always)]
    pub fn read_i64(&mut self) -> Option<i64> {
        self.skip_ws();
        if self.pos < self.buf.len() && self.buf[self.pos] == b'"' {
            let s = self.read_str_bytes()?;
            let (v, n) = parse_i64_fast(s)?;
            if n == s.len() { Some(v) } else { None }
        } else {
            let start = self.pos;
            let (v, n) = parse_i64_fast(&self.buf[start..])?;
            self.pos = start + n;
            Some(v)
        }
    }

    #[inline(always)]
    pub fn read_i32(&mut self) -> Option<i32> {
        let v = self.read_i64()?;
        i32::try_from(v).ok()
    }

    /// Reads an `f64` whether encoded as a quoted string (`"67123.45"`) or bare JSON number.
    #[inline(always)]
    pub fn read_f64(&mut self) -> Option<f64> {
        self.skip_ws();
        if self.pos < self.buf.len() && self.buf[self.pos] == b'"' {
            let s = self.read_str_bytes()?;
            let (v, n) = parse_decimal_fast(s)?;
            if n == s.len() { Some(v) } else { None }
        } else {
            let start = self.pos;
            let (v, n) = parse_decimal_fast(&self.buf[start..])?;
            self.pos = start + n;
            Some(v)
        }
    }

    #[inline(always)]
    pub fn read_bool(&mut self) -> Option<bool> {
        self.skip_ws();
        let rest = &self.buf[self.pos..];
        if rest.starts_with(b"true") {
            self.pos += 4;
            Some(true)
        } else if rest.starts_with(b"false") {
            self.pos += 5;
            Some(false)
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn read_symbol(&mut self) -> Option<Symbol> {
        let b = self.read_str_bytes()?;
        Symbol::try_from_bytes(b)
    }

    #[inline(always)]
    pub fn read_client_order_id(&mut self) -> Option<ClientOrderId> {
        let b = self.read_str_bytes()?;
        ClientOrderId::try_from_bytes(b)
    }

    /// Advances to and returns the next element in an array `[...]`.
    #[inline]
    pub fn next_element(&mut self) -> Option<&'a [u8]> {
        self.skip_ws();
        if self.pos >= self.buf.len() {
            return None;
        }
        if self.buf[self.pos] == b'[' {
            self.pos += 1;
            self.skip_ws();
        }
        if self.pos < self.buf.len() && self.buf[self.pos] == b']' {
            self.pos += 1;
            return None;
        }
        if self.pos < self.buf.len() && self.buf[self.pos] == b',' {
            self.pos += 1;
            self.skip_ws();
        }
        self.skip_value()
    }

    /// Skips any JSON value (object, array, string, number, bool, null) and returns its raw byte span.
    #[inline]
    pub fn skip_value(&mut self) -> Option<&'a [u8]> {
        self.skip_ws();
        if self.pos >= self.buf.len() {
            return None;
        }
        let start = self.pos;
        match unsafe { *self.buf.get_unchecked(self.pos) } {
            b'"' => {
                let _ = self.read_str_bytes()?;
            }
            b'{' | b'[' => {
                let mut depth = 0i32;
                while self.pos < self.buf.len() {
                    let b = unsafe { *self.buf.get_unchecked(self.pos) };
                    match b {
                        b'"' => {
                            let _ = self.read_str_bytes()?;
                            continue;
                        }
                        b'{' | b'[' => depth += 1,
                        b'}' | b']' => {
                            depth -= 1;
                            if depth == 0 {
                                self.pos += 1;
                                return Some(&self.buf[start..self.pos]);
                            }
                        }
                        _ => {}
                    }
                    self.pos += 1;
                }
                return None;
            }
            b't' => {
                if !self.buf[self.pos..].starts_with(b"true") {
                    return None;
                }
                self.pos += 4;
            }
            b'f' => {
                if !self.buf[self.pos..].starts_with(b"false") {
                    return None;
                }
                self.pos += 5;
            }
            b'n' => {
                if !self.buf[self.pos..].starts_with(b"null") {
                    return None;
                }
                self.pos += 4;
            }
            _ => {
                let _ = self.read_number_bytes()?;
            }
        }
        Some(&self.buf[start..self.pos])
    }
}

