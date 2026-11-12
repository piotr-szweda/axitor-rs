use crate::parsing::json_scanner::JsonScanner;
use crate::parsing::utf8_reader;
use crate::primitives::{
    AccountConfigUpdateModel, AccountUpdateBalance, AccountUpdateModel, AccountUpdatePosition,
    ClientOrderId, EventReasonType, ExecutionType, MarginCallModel, MarginCallPosition,
    MarginType, OrderSide, OrderStatus, OrderTradeUpdateModel, OrderType, PositionSide, Symbol,
    TimeInForce,
};
use memchr::memmem;

#[derive(Debug, Clone)]
pub enum UserDataEvent {
    OrderTradeUpdate(OrderTradeUpdateModel),
    AccountUpdate(AccountUpdateModel),
    MarginCall(MarginCallModel),
    AccountConfigUpdate(AccountConfigUpdateModel),
}

/// Dispatches any Binance User Data WebSocket stream event without heap allocation for orders.
#[inline]
pub fn parse_user_data_event(data: &[u8]) -> Option<UserDataEvent> {
    let event_type = extract_event_type(data)?;
    match event_type {
        b"ORDER_TRADE_UPDATE" => {
            parse_order_trade_update(data).map(UserDataEvent::OrderTradeUpdate)
        }
        b"ACCOUNT_UPDATE" => parse_account_update(data).map(UserDataEvent::AccountUpdate),
        b"MARGIN_CALL" => parse_margin_call(data).map(UserDataEvent::MarginCall),
        b"ACCOUNT_CONFIG_UPDATE" => {
            parse_account_config_update(data).map(UserDataEvent::AccountConfigUpdate)
        }
        _ => None,
    }
}

#[inline]
fn extract_event_type(data: &[u8]) -> Option<&[u8]> {
    let key = br#""e":""#;
    let idx = memmem::find(data, key)? + key.len();
    let end = memchr::memchr(b'"', &data[idx..])? + idx;
    Some(&data[idx..end])
}

/// Parses `ORDER_TRADE_UPDATE` event payload into flat `OrderTradeUpdateModel`.
pub fn parse_order_trade_update(data: &[u8]) -> Option<OrderTradeUpdateModel> {
    let mut model = OrderTradeUpdateModel::default();
    let mut cursor = 0;

    model.event_time = utf8_reader::read_i64(data, b"\"E\":", &mut cursor)?;
    model.transaction_time = utf8_reader::read_i64(data, b"\"T\":", &mut cursor).unwrap_or(model.event_time);

    let order_idx = memmem::find(data, br#""o":{"#)? + 5;
    let order_data = &data[order_idx..];
    let mut oc = 0;

    model.symbol = utf8_reader::read_symbol(order_data, b"\"s\":", &mut oc)?;
    if let Some(cid_bytes) = utf8_reader::seek_str(order_data, b"\"c\":", &mut oc) {
        model.client_order_id = ClientOrderId::try_from_bytes(cid_bytes).unwrap_or_default();
    }

    if let Some(s) = utf8_reader::seek_str(order_data, b"\"S\":", &mut oc) {
        model.side = OrderSide::from_bytes(s);
    }

    if let Some(o) = utf8_reader::seek_str(order_data, b"\"o\":", &mut oc) {
        model.order_type = OrderType::from_bytes(o);
    }

    if let Some(f) = utf8_reader::seek_str(order_data, b"\"f\":", &mut oc) {
        model.time_in_force = TimeInForce::from_bytes(f);
    }

    model.original_quantity = utf8_reader::read_quoted_f64(order_data, b"\"q\":", &mut oc).unwrap_or(0.0);
    model.original_price = utf8_reader::read_quoted_f64(order_data, b"\"p\":", &mut oc).unwrap_or(0.0);
    model.average_price = utf8_reader::read_quoted_f64(order_data, b"\"ap\":", &mut oc).unwrap_or(0.0);
    model.stop_price = utf8_reader::read_quoted_f64(order_data, b"\"sp\":", &mut oc).unwrap_or(0.0);

    if let Some(x) = utf8_reader::seek_str(order_data, b"\"x\":", &mut oc) {
        model.execution_type = ExecutionType::from_bytes(x);
    }

    if let Some(x_stat) = utf8_reader::seek_str(order_data, b"\"X\":", &mut oc) {
        model.order_status = OrderStatus::from_bytes(x_stat);
    }

    model.order_id = utf8_reader::read_i64(order_data, b"\"i\":", &mut oc).unwrap_or(0);
    model.last_filled_quantity = utf8_reader::read_quoted_f64(order_data, b"\"l\":", &mut oc).unwrap_or(0.0);
    model.cumulative_filled_quantity = utf8_reader::read_quoted_f64(order_data, b"\"z\":", &mut oc).unwrap_or(0.0);
    model.last_filled_price = utf8_reader::read_quoted_f64(order_data, b"\"L\":", &mut oc).unwrap_or(0.0);

    if let Some(n_val) = utf8_reader::seek_str(order_data, b"\"N\":", &mut oc) {
        model.commission_asset = Symbol::try_from_bytes(n_val).unwrap_or_default();
    }

    model.commission_amount = utf8_reader::read_quoted_f64(order_data, b"\"n\":", &mut oc).unwrap_or(0.0);
    model.trade_time = utf8_reader::read_i64(order_data, b"\"T\":", &mut oc).unwrap_or(model.transaction_time);
    model.trade_id = utf8_reader::read_i64(order_data, b"\"t\":", &mut oc).unwrap_or(0);
    model.bids_notional = utf8_reader::read_quoted_f64(order_data, b"\"b\":", &mut oc).unwrap_or(0.0);
    model.asks_notional = utf8_reader::read_quoted_f64(order_data, b"\"a\":", &mut oc).unwrap_or(0.0);
    model.is_maker = utf8_reader::read_bool(order_data, b"\"m\":", &mut oc).unwrap_or(false);
    model.is_reduce_only = utf8_reader::read_bool(order_data, b"\"R\":", &mut oc).unwrap_or(false);

    if let Some(ps) = utf8_reader::seek_str(order_data, b"\"ps\":", &mut oc) {
        model.position_side = PositionSide::from_bytes(ps);
    }

    model.realized_profit = utf8_reader::read_quoted_f64(order_data, b"\"rp\":", &mut oc).unwrap_or(0.0);
    Some(model)
}

/// Parses `ACCOUNT_UPDATE` event payload.
pub fn parse_account_update(data: &[u8]) -> Option<AccountUpdateModel> {
    let mut model = AccountUpdateModel::default();
    let mut cursor = 0;

    model.event_time = utf8_reader::read_i64(data, b"\"E\":", &mut cursor)?;
    model.transaction_time = utf8_reader::read_i64(data, b"\"T\":", &mut cursor).unwrap_or(model.event_time);

    let a_obj_key = br#""a":{"#;
    let a_start = memmem::find(data, a_obj_key)? + a_obj_key.len() - 1;
    let a_slice = &data[a_start..];
    let mut ac = 0;

    if let Some(m_val) = utf8_reader::seek_str(a_slice, b"\"m\":", &mut ac) {
        model.reason = EventReasonType::from_bytes(m_val);
    }

    // Balances "B":[...]
    if let Some(b_idx) = memmem::find(a_slice, br#""B":["#) {
        let b_slice = &a_slice[b_idx + 4..];
        let mut scanner = JsonScanner::new(b_slice);
        while let Some(elem) = scanner.next_element() {
            if elem.starts_with(b"{") {
                let mut bc = 0;
                let asset = utf8_reader::read_symbol(elem, b"\"a\":", &mut bc).unwrap_or_default();
                let wb = utf8_reader::read_quoted_f64(elem, b"\"wb\":", &mut bc).unwrap_or(0.0);
                let cwb = utf8_reader::read_quoted_f64(elem, b"\"cw\":", &mut bc).unwrap_or(0.0);
                let bc_val = utf8_reader::read_quoted_f64(elem, b"\"bc\":", &mut bc).unwrap_or(0.0);
                model.balances.push(AccountUpdateBalance {
                    asset,
                    wallet_balance: wb,
                    cross_wallet_balance: cwb,
                    balance_change: bc_val,
                });
            }
        }
    }

    // Positions "P":[...]
    if let Some(p_idx) = memmem::find(a_slice, br#""P":["#) {
        let p_slice = &a_slice[p_idx + 4..];
        let mut scanner = JsonScanner::new(p_slice);
        while let Some(elem) = scanner.next_element() {
            if elem.starts_with(b"{") {
                let mut pc = 0;
                let symbol = utf8_reader::read_symbol(elem, b"\"s\":", &mut pc).unwrap_or_default();
                let pa = utf8_reader::read_quoted_f64(elem, b"\"pa\":", &mut pc).unwrap_or(0.0);
                let ep = utf8_reader::read_quoted_f64(elem, b"\"ep\":", &mut pc).unwrap_or(0.0);
                let cr = utf8_reader::read_quoted_f64(elem, b"\"cr\":", &mut pc).unwrap_or(0.0);
                let up = utf8_reader::read_quoted_f64(elem, b"\"up\":", &mut pc).unwrap_or(0.0);
                let mt = if let Some(mt_raw) = utf8_reader::seek_str(elem, b"\"mt\":", &mut pc) {
                    MarginType::from_bytes(mt_raw)
                } else {
                    MarginType::Cross
                };
                let iw = utf8_reader::read_quoted_f64(elem, b"\"iw\":", &mut pc).unwrap_or(0.0);
                let ps = if let Some(ps_raw) = utf8_reader::seek_str(elem, b"\"ps\":", &mut pc) {
                    PositionSide::from_bytes(ps_raw)
                } else {
                    PositionSide::Both
                };

                model.positions.push(AccountUpdatePosition {
                    symbol,
                    position_amount: pa,
                    entry_price: ep,
                    accumulated_realized: cr,
                    unrealized_pnl: up,
                    margin_type: mt,
                    isolated_wallet: iw,
                    position_side: ps,
                });
            }
        }
    }

    Some(model)
}

/// Parses `MARGIN_CALL` event payload.
pub fn parse_margin_call(data: &[u8]) -> Option<MarginCallModel> {
    let mut model = MarginCallModel::default();
    let mut cursor = 0;

    model.event_time = utf8_reader::read_i64(data, b"\"E\":", &mut cursor)?;
    model.cross_wallet_balance = utf8_reader::read_quoted_f64(data, b"\"cw\":", &mut cursor).unwrap_or(0.0);

    if let Some(p_idx) = memmem::find(data, br#""p":["#) {
        let p_slice = &data[p_idx + 4..];
        let mut scanner = JsonScanner::new(p_slice);
        while let Some(elem) = scanner.next_element() {
            if elem.starts_with(b"{") {
                let mut pc = 0;
                let symbol = utf8_reader::read_symbol(elem, b"\"s\":", &mut pc).unwrap_or_default();
                let ps = if let Some(ps_raw) = utf8_reader::seek_str(elem, b"\"ps\":", &mut pc) {
                    PositionSide::from_bytes(ps_raw)
                } else {
                    PositionSide::Both
                };
                let pa = utf8_reader::read_quoted_f64(elem, b"\"pa\":", &mut pc).unwrap_or(0.0);
                let mt = if let Some(mt_raw) = utf8_reader::seek_str(elem, b"\"mt\":", &mut pc) {
                    MarginType::from_bytes(mt_raw)
                } else {
                    MarginType::Cross
                };
                let iw = utf8_reader::read_quoted_f64(elem, b"\"iw\":", &mut pc).unwrap_or(0.0);
                let mp = utf8_reader::read_quoted_f64(elem, b"\"mp\":", &mut pc).unwrap_or(0.0);
                let up = utf8_reader::read_quoted_f64(elem, b"\"up\":", &mut pc).unwrap_or(0.0);
                let mm = utf8_reader::read_quoted_f64(elem, b"\"mm\":", &mut pc).unwrap_or(0.0);

                model.positions.push(MarginCallPosition {
                    symbol,
                    position_side: ps,
                    position_amount: pa,
                    margin_type: mt,
                    isolated_wallet: iw,
                    mark_price: mp,
                    unrealized_pnl: up,
                    maintenance_margin_required: mm,
                });
            }
        }
    }

    Some(model)
}

/// Parses `ACCOUNT_CONFIG_UPDATE` event payload.
pub fn parse_account_config_update(data: &[u8]) -> Option<AccountConfigUpdateModel> {
    let mut model = AccountConfigUpdateModel::default();
    let mut cursor = 0;

    model.event_time = utf8_reader::read_i64(data, b"\"E\":", &mut cursor)?;
    model.transaction_time = utf8_reader::read_i64(data, b"\"T\":", &mut cursor).unwrap_or(model.event_time);

    // Can be leverage update "ac":{"s":"BTCUSDT","l":20}
    if let Some(ac_idx) = memmem::find(data, br#""ac":{"#) {
        let ac_slice = &data[ac_idx + 5..];
        let mut ac = 0;
        model.symbol = utf8_reader::read_symbol(ac_slice, b"\"s\":", &mut ac).unwrap_or_default();
        model.leverage = utf8_reader::read_i64(ac_slice, b"\"l\":", &mut ac).unwrap_or(0) as i32;
    }

    // Or multi-asset margin update "ai":{"j":true}
    if let Some(ai_idx) = memmem::find(data, br#""ai":{"#) {
        let ai_slice = &data[ai_idx + 5..];
        let mut ai = 0;
        model.is_multi_assets_margin = utf8_reader::read_bool(ai_slice, b"\"j\":", &mut ai).unwrap_or(false);
    }

    Some(model)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_order_trade_update_sample() {
        let json = br#"{"e":"ORDER_TRADE_UPDATE","E":1568879465651,"T":1568879465650,"o":{"s":"BTCUSDT","c":"TEST_ORDER_1","S":"BUY","o":"LIMIT","f":"GTC","q":"0.001","p":"10000","ap":"0","sp":"0","x":"NEW","X":"NEW","i":8886774,"l":"0","z":"0","L":"0","N":"USDT","n":"0","T":1568879465650,"t":0,"b":"0","a":"0","m":false,"R":false,"ps":"BOTH","rp":"0"}}"#;
        let event = parse_user_data_event(json).unwrap();
        match event {
            UserDataEvent::OrderTradeUpdate(o) => {
                assert_eq!(o.order_id, 8886774);
                assert_eq!(o.symbol.as_raw(), Symbol::parse("BTCUSDT").unwrap().as_raw());
                assert_eq!(o.client_order_id.as_bytes(), b"TEST_ORDER_1");
                assert_eq!(o.execution_type, ExecutionType::New);
                assert_eq!(o.order_status, OrderStatus::New);
                assert!((o.original_quantity - 0.001).abs() < 1e-9);
                assert!((o.original_price - 10000.0).abs() < 1e-9);
            }
            _ => panic!("wrong event type"),
        }
    }

    #[test]
    fn parse_account_update_sample() {
        let json = br#"{"e":"ACCOUNT_UPDATE","E":1564745798939,"T":1564745798938,"a":{"m":"ORDER","B":[{"a":"USDT","wb":"122624.12345678","cw":"100.12345678","bc":"50.12345678"}],"P":[{"s":"BTCUSDT","pa":"0.5","ep":"9000.5","cr":"200.0","up":"15.5","mt":"cross","iw":"0","ps":"BOTH"}]}}"#;
        let event = parse_user_data_event(json).unwrap();
        match event {
            UserDataEvent::AccountUpdate(a) => {
                assert_eq!(a.reason, EventReasonType::Order);
                assert_eq!(a.balances.len(), 1);
                assert!((a.balances[0].wallet_balance - 122624.12345678).abs() < 1e-6);
                assert_eq!(a.positions.len(), 1);
                assert!((a.positions[0].position_amount - 0.5).abs() < 1e-9);
            }
            _ => panic!("wrong event type"),
        }
    }
}
