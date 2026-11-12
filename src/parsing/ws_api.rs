//! Zero-copy WebSocket API envelope & endpoint response parsers.

use crate::parsing::json_scanner::JsonScanner;
use crate::primitives::{AccountBalanceAsset, AccountBalanceModel, Symbol, TickerPriceModel};

pub struct WsApiEnvelope<'a> {
    pub id: i64,
    pub status: i32,
    pub result: &'a [u8],
    pub exchange_code: i32,
}

/// Parses `{"id":123,"status":200,"result":...}` in one zero-alloc scan.
#[inline]
pub fn parse_ws_api_envelope<'a>(msg: &'a [u8]) -> Option<WsApiEnvelope<'a>> {
    let mut s = JsonScanner::new(msg);
    if !s.expect(b'{') {
        return None;
    }
    let mut id = 0i64;
    let mut has_id = false;
    let mut status = 0i32;
    let mut has_status = false;
    let mut result: &'a [u8] = &[];
    let mut exchange_code = 0i32;

    if s.peek() != Some(b'}') {
        loop {
            let key = s.read_key()?;
            match key {
                b"id" => {
                    if let Some(v) = s.read_i64() {
                        id = v;
                        has_id = true;
                    } else {
                        let _ = s.skip_value()?;
                    }
                }
                b"status" => {
                    if let Some(v) = s.read_i32() {
                        status = v;
                        has_status = true;
                    } else {
                        let _ = s.skip_value()?;
                    }
                }
                b"result" => {
                    result = s.skip_value()?;
                }
                b"error" => {
                    if s.peek() == Some(b'{') {
                        let _ = s.expect(b'{');
                        if s.peek() != Some(b'}') {
                            loop {
                                let ek = s.read_key()?;
                                if ek == b"code" {
                                    exchange_code = s.read_i32().unwrap_or(0);
                                } else {
                                    let _ = s.skip_value()?;
                                }
                                if !s.next_comma_or_end(b'}').ok()? {
                                    break;
                                }
                            }
                        } else {
                            let _ = s.expect(b'}');
                        }
                    } else {
                        let _ = s.skip_value()?;
                    }
                }
                _ => {
                    let _ = s.skip_value()?;
                }
            }
            if !s.next_comma_or_end(b'}').ok()? {
                break;
            }
        }
    } else {
        let _ = s.expect(b'}');
    }

    if !has_id || !has_status {
        return None;
    }
    Some(WsApiEnvelope {
        id,
        status,
        result,
        exchange_code,
    })
}

/// Fast-path id extractor for routing on the WS API receive loop (avoids scanning `result`).
#[inline]
pub fn extract_ws_api_id(msg: &[u8]) -> Option<i64> {
    // Hot path: `{"id":123,...`
    if msg.starts_with(b"{\"id\":") {
        if let Some((id, _)) = crate::parsing::fast_decimal::parse_i64_fast(&msg[6..]) {
            return Some(id);
        }
    }
    let mut s = JsonScanner::new(msg);
    if !s.expect(b'{') {
        return None;
    }
    if s.peek() != Some(b'}') {
        loop {
            let key = s.read_key()?;
            if key == b"id" {
                return s.read_i64();
            }
            s.skip_value()?;
            if !s.next_comma_or_end(b'}').ok()? {
                break;
            }
        }
    }
    None
}

#[inline]
pub fn parse_ticker_price(result: &[u8]) -> Option<TickerPriceModel> {
    let mut s = JsonScanner::new(result);
    if !s.expect(b'{') {
        return None;
    }
    let mut model = TickerPriceModel::default();
    if s.peek() != Some(b'}') {
        loop {
            let key = s.read_key()?;
            match key {
                b"symbol" => model.symbol = s.read_symbol()?,
                b"price" => model.price = s.read_f64()?,
                b"time" => model.time = s.read_i64()?,
                _ => {
                    s.skip_value()?;
                }
            }
            if !s.next_comma_or_end(b'}').ok()? {
                break;
            }
        }
    } else {
        let _ = s.expect(b'}');
    }
    Some(model)
}

pub fn parse_account_balance(result: &[u8]) -> Option<AccountBalanceModel> {
    let mut s = JsonScanner::new(result);
    if !s.expect(b'[') {
        return None;
    }
    let mut assets = Vec::with_capacity(16);
    if s.peek() == Some(b']') {
        let _ = s.expect(b']');
        return Some(AccountBalanceModel { assets });
    }
    loop {
        if !s.expect(b'{') {
            return None;
        }
        let mut a = AccountBalanceAsset::default();
        if s.peek() != Some(b'}') {
            loop {
                let key = s.read_key()?;
                if !s.try_consume_null() {
                    match key {
                        b"asset" => a.asset = s.read_symbol().unwrap_or(Symbol::EMPTY),
                        b"updateTime" => a.update_time = s.read_i64()?,
                        b"balance" => a.wallet_balance = s.read_f64()?,
                        b"crossWalletBalance" => a.cross_wallet_balance = s.read_f64()?,
                        b"crossUnPnl" => a.cross_unrealized_profit = s.read_f64()?,
                        b"availableBalance" => a.available_balance = s.read_f64()?,
                        b"maxWithdrawAmount" => a.max_withdraw_amount = s.read_f64()?,
                        b"marginAvailable" => a.margin_available = s.read_bool()?,
                        _ => {
                            s.skip_value()?;
                        }
                    }
                }
                if !s.next_comma_or_end(b'}').ok()? {
                    break;
                }
            }
        } else {
            let _ = s.expect(b'}');
        }
        assets.push(a);
        if !s.next_comma_or_end(b']').ok()? {
            break;
        }
    }
    Some(AccountBalanceModel { assets })
}

