//! Order book depth level reader (`BinanceDepthReader` & `BinanceOrderBookReader` equivalent).
//! Parses `[["price","qty"],...]` straight from UTF-8 in one pass with zero intermediate allocations.

use crate::parsing::fast_decimal::parse_decimal_fast;
use crate::parsing::utf8_reader;
use crate::primitives::{DepthLevel, OrderBookModel, OrderBookSnapshotModel};

const LEVEL_START: &[u8] = b"[\"";
const LEVEL_SEP: &[u8] = b"\",\"";
const LEVEL_END: &[u8] = b"\"]";

#[inline(always)]
fn try_open_array(data: &[u8], key: &[u8], pos: &mut usize) -> Option<bool> {
    if !utf8_reader::seek_value(data, key, pos) {
        return None;
    }
    if *pos >= data.len() || data[*pos] != b'[' {
        return None;
    }
    *pos += 1;
    if *pos < data.len() && data[*pos] == b']' {
        *pos += 1;
        return Some(true); // empty
    }
    Some(false)
}

#[inline(always)]
fn try_read_level(data: &[u8], pos: &mut usize) -> Option<DepthLevel> {
    let mut idx = *pos;
    if !data[idx..].starts_with(LEVEL_START) {
        return None;
    }
    idx += LEVEL_START.len();

    let (price, consumed) = parse_decimal_fast(&data[idx..])?;
    idx += consumed;

    if !data[idx..].starts_with(LEVEL_SEP) {
        return None;
    }
    idx += LEVEL_SEP.len();

    let (quantity, consumed) = parse_decimal_fast(&data[idx..])?;
    idx += consumed;

    if !data[idx..].starts_with(LEVEL_END) {
        return None;
    }
    *pos = idx + LEVEL_END.len();
    Some(DepthLevel { price, quantity })
}

#[inline(always)]
fn try_read_separator(data: &[u8], pos: &mut usize) -> Option<bool> {
    if *pos >= data.len() {
        return None;
    }
    let next = unsafe { *data.get_unchecked(*pos) };
    *pos += 1;
    if next == b']' {
        Some(true)
    } else if next == b',' {
        Some(false)
    } else {
        None
    }
}

/// Reads level array into a caller-owned slice, returning level count. Zero allocations.
#[inline]
pub fn read_levels_into(
    data: &[u8],
    key: &[u8],
    cursor: &mut usize,
    dst: &mut [DepthLevel],
) -> Option<usize> {
    let mut pos = *cursor;
    let mut is_empty = try_open_array(data, key, &mut pos)?;
    let mut count = 0usize;

    while !is_empty {
        if count >= dst.len() {
            return None;
        }
        dst[count] = try_read_level(data, &mut pos)?;
        count += 1;
        is_empty = try_read_separator(data, &mut pos)?;
    }

    *cursor = pos;
    Some(count)
}

/// Reads level array into a reusable `Vec<DepthLevel>` (clears first, keeps capacity). Zero allocs after warmup.
#[inline]
pub fn read_levels_vec(
    data: &[u8],
    key: &[u8],
    cursor: &mut usize,
    dst: &mut Vec<DepthLevel>,
) -> Option<()> {
    dst.clear();
    let mut pos = *cursor;
    let mut is_empty = try_open_array(data, key, &mut pos)?;

    while !is_empty {
        let level = try_read_level(data, &mut pos)?;
        dst.push(level);
        is_empty = try_read_separator(data, &mut pos)?;
    }

    *cursor = pos;
    Some(())
}

#[inline]
fn read_order_book_header(data: &[u8], has_times: bool) -> Option<(usize, i64, i64, i64)> {
    if data.is_empty() || data[0] != b'{' {
        return None;
    }
    let mut cursor = utf8_reader::OBJECT_START;
    utf8_reader::skip_optional(data, b"\"symbol\":", &mut cursor);
    utf8_reader::skip_optional(data, b"\"pair\":", &mut cursor);

    let last_update_id = utf8_reader::read_i64(data, b"\"lastUpdateId\":", &mut cursor)?;
    let (event_time, transaction_time) = if has_times {
        let e = utf8_reader::read_i64(data, b"\"E\":", &mut cursor)?;
        let t = utf8_reader::read_i64(data, b"\"T\":", &mut cursor)?;
        (e, t)
    } else {
        (0, 0)
    };
    Some((cursor, last_update_id, event_time, transaction_time))
}

/// Reads REST depth snapshot straight into caller-provided `bids` and `asks` buffers. Zero allocations.
#[inline]
pub fn read_order_book_into(
    body: &[u8],
    has_times: bool,
    bids: &mut [DepthLevel],
    asks: &mut [DepthLevel],
) -> Option<OrderBookSnapshotModel> {
    let (mut cursor, last_update_id, event_time, transaction_time) =
        read_order_book_header(body, has_times)?;
    let bid_count = read_levels_into(body, b"\"bids\":", &mut cursor, bids)?;
    let ask_count = read_levels_into(body, b"\"asks\":", &mut cursor, asks)?;
    Some(OrderBookSnapshotModel {
        last_update_id,
        event_time,
        transaction_time,
        bid_count,
        ask_count,
    })
}

/// Reads REST depth snapshot into freshly sized `Vec<DepthLevel>` using a reusable scratch buffer.
pub fn read_order_book(body: &[u8], has_times: bool) -> Option<OrderBookModel> {
    let (mut cursor, last_update_id, event_time, transaction_time) =
        read_order_book_header(body, has_times)?;
    let mut bids = Vec::with_capacity(1000);
    let mut asks = Vec::with_capacity(1000);
    read_levels_vec(body, b"\"bids\":", &mut cursor, &mut bids)?;
    read_levels_vec(body, b"\"asks\":", &mut cursor, &mut asks)?;
    bids.shrink_to_fit();
    asks.shrink_to_fit();
    Some(OrderBookModel {
        last_update_id,
        event_time,
        transaction_time,
        bids,
        asks,
    })
}

