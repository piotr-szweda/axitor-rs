//! Zero-allocation WebSocket stream parsers for Binance USD-M Futures.

use crate::parsing::depth::{read_levels_into, read_levels_vec};
use crate::parsing::utf8_reader;
use crate::primitives::{
    AggTradeModel, BookTickerModel, DepthLevel, DepthUpdateHeader, KlineUpdateModel,
    PartialDepthModel,
};

#[inline]
pub fn parse_book_ticker(data: &[u8]) -> Option<(BookTickerModel, i64)> {
    const PREFIX: &[u8] = b"{\"e\":\"bookTicker\"";
    let mut cursor = if data.starts_with(PREFIX) {
        PREFIX.len()
    } else {
        utf8_reader::OBJECT_START
    };

    let update_id = utf8_reader::read_i64(data, b"\"u\":", &mut cursor)?;
    let symbol = utf8_reader::read_symbol(data, b"\"s\":", &mut cursor)?;
    utf8_reader::skip_optional(data, b"\"ps\":", &mut cursor);

    let bid_price = utf8_reader::read_quoted_f64(data, b"\"b\":", &mut cursor)?;
    let bid_quantity = utf8_reader::read_quoted_f64(data, b"\"B\":", &mut cursor)?;
    let ask_price = utf8_reader::read_quoted_f64(data, b"\"a\":", &mut cursor)?;
    let ask_quantity = utf8_reader::read_quoted_f64(data, b"\"A\":", &mut cursor)?;
    let transaction_time = utf8_reader::read_i64(data, b"\"T\":", &mut cursor)?;
    let event_time = utf8_reader::read_i64(data, b"\"E\":", &mut cursor)?;

    let model = BookTickerModel {
        symbol,
        update_id,
        event_time,
        transaction_time,
        bid_price,
        bid_quantity,
        ask_price,
        ask_quantity,
    };
    Some((model, event_time))
}

#[inline]
pub fn parse_agg_trade(data: &[u8]) -> Option<(AggTradeModel, i64)> {
    const PREFIX: &[u8] = b"{\"e\":\"aggTrade\"";
    let mut cursor = if data.starts_with(PREFIX) {
        PREFIX.len()
    } else {
        utf8_reader::OBJECT_START
    };

    let event_time = utf8_reader::read_i64(data, b"\"E\":", &mut cursor)?;
    let aggregate_trade_id = utf8_reader::read_i64(data, b"\"a\":", &mut cursor)?;
    let symbol = utf8_reader::read_symbol(data, b"\"s\":", &mut cursor)?;
    let price = utf8_reader::read_quoted_f64(data, b"\"p\":", &mut cursor)?;
    let quantity = utf8_reader::read_quoted_f64(data, b"\"q\":", &mut cursor)?;
    utf8_reader::skip_optional(data, b"\"nq\":", &mut cursor);
    let first_trade_id = utf8_reader::read_i64(data, b"\"f\":", &mut cursor)?;
    let last_trade_id = utf8_reader::read_i64(data, b"\"l\":", &mut cursor)?;
    let trade_time = utf8_reader::read_i64(data, b"\"T\":", &mut cursor)?;
    let is_buyer_maker = utf8_reader::read_bool(data, b"\"m\":", &mut cursor)?;

    let model = AggTradeModel {
        symbol,
        aggregate_trade_id,
        event_time,
        trade_time,
        first_trade_id,
        last_trade_id,
        price,
        quantity,
        is_buyer_maker,
    };
    Some((model, event_time))
}

#[inline]
pub fn parse_depth_update(
    data: &[u8],
    bids: &mut Vec<DepthLevel>,
    asks: &mut Vec<DepthLevel>,
) -> Option<DepthUpdateHeader> {
    const PREFIX: &[u8] = b"{\"e\":\"depthUpdate\"";
    let mut cursor = if data.starts_with(PREFIX) {
        PREFIX.len()
    } else {
        utf8_reader::OBJECT_START
    };

    let event_time = utf8_reader::read_i64(data, b"\"E\":", &mut cursor)?;
    let transaction_time = utf8_reader::read_i64(data, b"\"T\":", &mut cursor)?;
    let symbol = utf8_reader::read_symbol(data, b"\"s\":", &mut cursor)?;
    utf8_reader::skip_optional(data, b"\"ps\":", &mut cursor);
    let first_update_id = utf8_reader::read_i64(data, b"\"U\":", &mut cursor)?;
    let final_update_id = utf8_reader::read_i64(data, b"\"u\":", &mut cursor)?;
    let previous_final_update_id = utf8_reader::read_i64(data, b"\"pu\":", &mut cursor)?;

    read_levels_vec(data, b"\"b\":", &mut cursor, bids)?;
    read_levels_vec(data, b"\"a\":", &mut cursor, asks)?;

    Some(DepthUpdateHeader {
        symbol,
        first_update_id,
        final_update_id,
        previous_final_update_id,
        event_time,
        transaction_time,
    })
}

#[inline]
pub fn parse_partial_depth(data: &[u8]) -> Option<(PartialDepthModel, i64)> {
    const PREFIX: &[u8] = b"{\"e\":\"depthUpdate\"";
    let mut cursor = if data.starts_with(PREFIX) {
        PREFIX.len()
    } else {
        utf8_reader::OBJECT_START
    };

    let mut model = PartialDepthModel::default();
    model.event_time = utf8_reader::read_i64(data, b"\"E\":", &mut cursor)?;
    model.transaction_time = utf8_reader::read_i64(data, b"\"T\":", &mut cursor)?;
    model.symbol = utf8_reader::read_symbol(data, b"\"s\":", &mut cursor)?;

    utf8_reader::skip_optional(data, b"\"ps\":", &mut cursor);
    utf8_reader::skip_optional(data, b"\"U\":", &mut cursor);
    model.final_update_id = utf8_reader::read_i64(data, b"\"u\":", &mut cursor)?;
    utf8_reader::skip_optional(data, b"\"pu\":", &mut cursor);

    model.bid_count = read_levels_into(data, b"\"b\":", &mut cursor, &mut model.bids)? as u8;
    model.ask_count = read_levels_into(data, b"\"a\":", &mut cursor, &mut model.asks)? as u8;

    let et = model.event_time;
    Some((model, et))
}

#[inline]
pub fn parse_kline_update(data: &[u8]) -> Option<(KlineUpdateModel, i64)> {
    const PREFIX: &[u8] = b"{\"e\":\"kline\"";
    let mut cursor = if data.starts_with(PREFIX) {
        PREFIX.len()
    } else {
        utf8_reader::OBJECT_START
    };

    let mut model = KlineUpdateModel::default();
    model.event_time = utf8_reader::read_i64(data, b"\"E\":", &mut cursor)?;
    model.symbol = utf8_reader::read_symbol(data, b"\"s\":", &mut cursor)?;
    if !utf8_reader::seek_value(data, b"\"k\":", &mut cursor)
        || cursor >= data.len()
        || data[cursor] != b'{'
    {
        return None;
    }
    cursor += 1;

    model.kline.open_time = utf8_reader::read_i64(data, b"\"t\":", &mut cursor)?;
    model.kline.close_time = utf8_reader::read_i64(data, b"\"T\":", &mut cursor)?;
    utf8_reader::skip_optional(data, b"\"s\":", &mut cursor);
    utf8_reader::skip_optional(data, b"\"i\":", &mut cursor);

    model.first_trade_id = utf8_reader::read_i64(data, b"\"f\":", &mut cursor)?;
    model.last_trade_id = utf8_reader::read_i64(data, b"\"L\":", &mut cursor)?;
    model.kline.open = utf8_reader::read_quoted_f64(data, b"\"o\":", &mut cursor)?;
    model.kline.close = utf8_reader::read_quoted_f64(data, b"\"c\":", &mut cursor)?;
    model.kline.high = utf8_reader::read_quoted_f64(data, b"\"h\":", &mut cursor)?;
    model.kline.low = utf8_reader::read_quoted_f64(data, b"\"l\":", &mut cursor)?;
    model.kline.volume = utf8_reader::read_quoted_f64(data, b"\"v\":", &mut cursor)?;
    let trade_count = utf8_reader::read_i64(data, b"\"n\":", &mut cursor)?;
    model.kline.trade_count = i32::try_from(trade_count).ok()?;
    model.is_closed = utf8_reader::read_bool(data, b"\"x\":", &mut cursor)?;
    model.kline.quote_volume = utf8_reader::read_quoted_f64(data, b"\"q\":", &mut cursor)?;
    model.kline.taker_buy_volume = utf8_reader::read_quoted_f64(data, b"\"V\":", &mut cursor)?;
    model.kline.taker_buy_quote_volume = utf8_reader::read_quoted_f64(data, b"\"Q\":", &mut cursor)?;

    let et = model.event_time;
    Some((model, et))
}

