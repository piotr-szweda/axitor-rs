use base64::Engine as _;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsOpcode {
    Continuation = 0x0,
    Text = 0x1,
    Binary = 0x2,
    Close = 0x8,
    Ping = 0x9,
    Pong = 0xA,
}

impl WsOpcode {
    #[inline]
    pub fn from_byte(b: u8) -> Option<Self> {
        match b & 0x0F {
            0x0 => Some(Self::Continuation),
            0x1 => Some(Self::Text),
            0x2 => Some(Self::Binary),
            0x8 => Some(Self::Close),
            0x9 => Some(Self::Ping),
            0xA => Some(Self::Pong),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParsedWsUrl {
    pub host_port: String,
    pub host_header: String,
    pub path: String,
}

impl ParsedWsUrl {
    pub fn parse(url: &str, extra_path: &str) -> Result<Self, String> {
        let rest = url
            .strip_prefix("ws://")
            .ok_or_else(|| format!("unsupported WebSocket scheme (expected ws://): {url}"))?;

        let (authority, base_path) = match rest.find('/') {
            Some(idx) => (&rest[..idx], &rest[idx..]),
            None => (rest, ""),
        };

        let host_port = if authority.contains(':') {
            authority.to_string()
        } else {
            format!("{authority}:80")
        };

        let mut path = String::with_capacity(base_path.len() + extra_path.len() + 2);
        let base_trimmed = base_path.trim_end_matches('/');
        if !base_trimmed.is_empty() {
            path.push_str(base_trimmed);
        }
        if !extra_path.is_empty() {
            if !extra_path.starts_with('/') && !extra_path.starts_with('?') {
                path.push('/');
            }
            path.push_str(extra_path);
        }
        if path.is_empty() {
            path.push('/');
        }

        Ok(Self {
            host_port,
            host_header: authority.to_string(),
            path,
        })
    }
}

/// Fast non-cryptographic xorshift64 for RFC 6455 client masking keys.
#[derive(Debug, Clone)]
pub struct FastMaskRng {
    state: u64,
}

impl FastMaskRng {
    #[inline]
    pub fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 {
                0x9E37_79B9_7F4A_7C15
            } else {
                seed
            },
        }
    }

    #[inline]
    pub fn next_mask(&mut self) -> [u8; 4] {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        (x as u32).to_ne_bytes()
    }
}

/// Word-at-a-time RFC 6455 XOR masking directly into `dst`.
#[inline]
pub fn copy_masked(src: &[u8], dst: &mut [u8], mask: [u8; 4]) {
    let n = src.len();
    debug_assert!(dst.len() >= n);
    let mask32 = u32::from_ne_bytes(mask);
    let mask64 = ((mask32 as u64) << 32) | (mask32 as u64);

    let mut i = 0usize;
    unsafe {
        let sp = src.as_ptr();
        let dp = dst.as_mut_ptr();
        while i + 8 <= n {
            let chunk = (sp.add(i) as *const u64).read_unaligned();
            (dp.add(i) as *mut u64).write_unaligned(chunk ^ mask64);
            i += 8;
        }
        while i < n {
            *dp.add(i) = *sp.add(i) ^ mask[i & 3];
            i += 1;
        }
    }
}

/// Encodes an RFC 6455 masked client frame into `out` and returns total encoded byte length.
/// `out` must have at least `payload.len() + 14` bytes.
#[inline]
pub fn encode_masked_frame(
    opcode: WsOpcode,
    payload: &[u8],
    mask: [u8; 4],
    out: &mut [u8],
) -> usize {
    let len = payload.len();
    out[0] = 0x80 | (opcode as u8);
    let hdr_len = if len <= 125 {
        out[1] = 0x80 | (len as u8);
        2
    } else if len <= 65_535 {
        out[1] = 0x80 | 126;
        let be = (len as u16).to_be_bytes();
        out[2] = be[0];
        out[3] = be[1];
        4
    } else {
        out[1] = 0x80 | 127;
        let be = (len as u64).to_be_bytes();
        out[2..10].copy_from_slice(&be);
        10
    };

    out[hdr_len..hdr_len + 4].copy_from_slice(&mask);
    let payload_off = hdr_len + 4;
    copy_masked(payload, &mut out[payload_off..payload_off + len], mask);
    payload_off + len
}

/// Connects a `TcpStream` with `TCP_NODELAY` and completes an HTTP/1.1 WebSocket Upgrade.
/// Any excess bytes received after `\r\n\r\n` are preserved inside `initial_buf`.
pub async fn connect_and_handshake(
    url: &ParsedWsUrl,
    initial_buf: &mut WsReadBuffer,
) -> Result<TcpStream, String> {
    let mut stream = TcpStream::connect(&url.host_port)
        .await
        .map_err(|e| format!("tcp connect {} failed: {e}", url.host_port))?;
    let _ = stream.set_nodelay(true);

    // Deterministic-enough per-connection key using stack entropy from pointer + hash
    let mut hasher = Sha256::new();
    let ptr_val = (&stream as *const TcpStream as usize).to_le_bytes();
    hasher.update(ptr_val);
    hasher.update(url.path.as_bytes());
    let digest = hasher.finalize();
    let mut key_b64 = [0u8; 24];
    let key_len = base64::engine::general_purpose::STANDARD
        .encode_slice(&digest[..16], &mut key_b64)
        .map_err(|_| "base64 encode failed".to_string())?;
    let key_str = std::str::from_utf8(&key_b64[..key_len]).unwrap_or("dGhlIHNhbXBsZSBub25jZQ==");

    let req = format!(
        "GET {} HTTP/1.1\r\n\
         Host: {}\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Key: {}\r\n\
         Sec-WebSocket-Version: 13\r\n\r\n",
        url.path, url.host_header, key_str
    );

    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| format!("ws handshake write failed: {e}"))?;

    initial_buf.reset();
    let header_end = loop {
        if let Some(end) = find_header_end(initial_buf.readable()) {
            break end;
        }
        let n = initial_buf
            .read_more(&mut stream)
            .await
            .map_err(|e| format!("ws handshake read failed: {e}"))?;
        if n == 0 {
            return Err("ws handshake EOF before HTTP headers completed".to_string());
        }
        if initial_buf.readable_len() > 16 * 1024 {
            return Err("ws handshake HTTP response headers too large".to_string());
        }
    };

    let hdr_bytes = &initial_buf.readable()[..header_end];
    if !hdr_bytes.starts_with(b"HTTP/1.1 101") && !hdr_bytes.starts_with(b"HTTP/1.0 101") {
        let preview = String::from_utf8_lossy(&hdr_bytes[..hdr_bytes.len().min(160)]);
        return Err(format!("ws upgrade rejected: {preview}"));
    }

    initial_buf.consume(header_end);
    initial_buf.compact_if_needed();
    Ok(stream)
}

#[inline]
fn find_header_end(buf: &[u8]) -> Option<usize> {
    memchr::memmem::find(buf, b"\r\n\r\n").map(|pos| pos + 4)
}

#[inline]
pub async fn write_masked_frame<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    opcode: WsOpcode,
    payload: &[u8],
    rng: &mut FastMaskRng,
    scratch: &mut Vec<u8>,
) -> std::io::Result<()> {
    let needed = payload.len() + 14;
    if scratch.len() < needed {
        scratch.resize(needed, 0);
    }
    let mask = rng.next_mask();
    let total = encode_masked_frame(opcode, payload, mask, scratch);
    writer.write_all(&scratch[..total]).await
}

/// Zero-copy WebSocket receive buffer with direct slicing into the contiguous ring/compaction buffer.
pub struct WsReadBuffer {
    buf: Vec<u8>,
    read_pos: usize,
    write_pos: usize,
    frag_buf: Vec<u8>,
    frag_opcode: Option<WsOpcode>,
}

#[derive(Debug)]
pub enum DecodedFrame<'a> {
    Message { opcode: WsOpcode, payload: &'a [u8] },
    Ping(&'a [u8]),
    Pong(&'a [u8]),
    Close(u16),
}

impl WsReadBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: vec![0u8; capacity.max(64 * 1024)],
            read_pos: 0,
            write_pos: 0,
            frag_buf: Vec::with_capacity(64 * 1024),
            frag_opcode: None,
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        self.read_pos = 0;
        self.write_pos = 0;
        self.frag_buf.clear();
        self.frag_opcode = None;
    }

    #[inline]
    pub fn readable(&self) -> &[u8] {
        &self.buf[self.read_pos..self.write_pos]
    }

    #[inline]
    pub fn readable_len(&self) -> usize {
        self.write_pos - self.read_pos
    }

    #[inline]
    pub fn consume(&mut self, n: usize) {
        self.read_pos += n;
        if self.read_pos == self.write_pos {
            self.read_pos = 0;
            self.write_pos = 0;
        }
    }

    #[inline]
    pub fn compact_if_needed(&mut self) {
        if self.read_pos == 0 {
            return;
        }
        let rem = self.write_pos - self.read_pos;
        if rem == 0 {
            self.read_pos = 0;
            self.write_pos = 0;
        } else if self.read_pos > self.buf.len() / 2 || self.write_pos == self.buf.len() {
            self.buf.copy_within(self.read_pos..self.write_pos, 0);
            self.read_pos = 0;
            self.write_pos = rem;
        }
    }

    pub async fn read_more<R: AsyncReadExt + Unpin>(&mut self, reader: &mut R) -> std::io::Result<usize> {
        self.compact_if_needed();
        if self.write_pos == self.buf.len() {
            self.buf.resize(self.buf.len() * 2, 0);
        }
        let n = reader.read(&mut self.buf[self.write_pos..]).await?;
        self.write_pos += n;
        Ok(n)
    }

    /// Attempts to decode the next complete frame from buffered bytes without heap allocation
    /// (except when a single logical message arrives fragmented across multiple RFC 6455 frames).
    /// Returns `Ok(None)` if more bytes are needed.
    pub fn next_frame(&mut self) -> Result<Option<DecodedFrame<'_>>, String> {
        loop {
            let avail = self.write_pos - self.read_pos;
            if avail < 2 {
                return Ok(None);
            }

            let base = self.read_pos;
            let b0 = self.buf[base];
            let b1 = self.buf[base + 1];
            let fin = (b0 & 0x80) != 0;
            let opcode = WsOpcode::from_byte(b0)
                .ok_or_else(|| format!("unsupported ws opcode: 0x{:02x}", b0 & 0x0F))?;
            let masked = (b1 & 0x80) != 0;
            let len7 = (b1 & 0x7F) as usize;

            let mut hdr = 2usize;
            let payload_len: usize = if len7 <= 125 {
                len7
            } else if len7 == 126 {
                if avail < 4 {
                    return Ok(None);
                }
                hdr = 4;
                u16::from_be_bytes([self.buf[base + 2], self.buf[base + 3]]) as usize
            } else {
                if avail < 10 {
                    return Ok(None);
                }
                hdr = 10;
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&self.buf[base + 2..base + 10]);
                u64::from_be_bytes(arr) as usize
            };

            let mask_bytes = if masked {
                if avail < hdr + 4 {
                    return Ok(None);
                }
                let m = [
                    self.buf[base + hdr],
                    self.buf[base + hdr + 1],
                    self.buf[base + hdr + 2],
                    self.buf[base + hdr + 3],
                ];
                hdr += 4;
                Some(m)
            } else {
                None
            };

            let total_len = hdr + payload_len;
            if avail < total_len {
                // Ensure buffer can actually fit large unfragmented frame without realloc loops
                if total_len > self.buf.len() {
                    self.compact_if_needed();
                    if total_len > self.buf.len() {
                        self.buf.resize(total_len.next_power_of_two(), 0);
                    }
                }
                return Ok(None);
            }

            let p_start = base + hdr;
            let p_end = p_start + payload_len;
            if let Some(mask) = mask_bytes {
                for (idx, byte) in self.buf[p_start..p_end].iter_mut().enumerate() {
                    *byte ^= mask[idx & 3];
                }
            }

            // Advance read cursor; data in `self.buf[p_start..p_end]` remains untouched until next read/compact!
            self.read_pos += total_len;
            if self.read_pos == self.write_pos {
                self.read_pos = 0;
                self.write_pos = 0;
            }

            match opcode {
                WsOpcode::Ping => {
                    return Ok(Some(DecodedFrame::Ping(&self.buf[p_start..p_end])));
                }
                WsOpcode::Pong => {
                    return Ok(Some(DecodedFrame::Pong(&self.buf[p_start..p_end])));
                }
                WsOpcode::Close => {
                    let code = if payload_len >= 2 {
                        u16::from_be_bytes([self.buf[p_start], self.buf[p_start + 1]])
                    } else {
                        1000
                    };
                    return Ok(Some(DecodedFrame::Close(code)));
                }
                WsOpcode::Text | WsOpcode::Binary => {
                    if fin && self.frag_opcode.is_none() {
                        return Ok(Some(DecodedFrame::Message {
                            opcode,
                            payload: &self.buf[p_start..p_end],
                        }));
                    }
                    self.frag_buf.clear();
                    self.frag_buf.extend_from_slice(&self.buf[p_start..p_end]);
                    self.frag_opcode = Some(opcode);
                }
                WsOpcode::Continuation => {
                    self.frag_buf.extend_from_slice(&self.buf[p_start..p_end]);
                    if fin {
                        let op = self.frag_opcode.take().unwrap_or(WsOpcode::Text);
                        return Ok(Some(DecodedFrame::Message {
                            opcode: op,
                            payload: self.frag_buf.as_slice(),
                        }));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let payload = br#"{"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT"}}"#;
        let mut wire = vec![0u8; payload.len() + 16];
        let mut rng = FastMaskRng::new(42);
        let len = encode_masked_frame(WsOpcode::Text, payload, rng.next_mask(), &mut wire);

        let mut rb = WsReadBuffer::new(4096);
        rb.buf[..len].copy_from_slice(&wire[..len]);
        rb.write_pos = len;

        match rb.next_frame().unwrap().unwrap() {
            DecodedFrame::Message { opcode, payload: p } => {
                assert_eq!(opcode, WsOpcode::Text);
                assert_eq!(p, payload);
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }
}

