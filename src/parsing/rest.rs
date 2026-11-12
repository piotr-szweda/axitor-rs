//! Zero-copy UTF-8 REST response parsers (Klines, ExchangeInfo, AccountInfoV2, LeverageBracket, Order).

use memchr::memchr;

use crate::parsing::fast_decimal::{parse_decimal_fast, parse_i64_fast};
use crate::parsing::json_scanner::JsonScanner;
use crate::primitives::{
    AccountInfoV2Asset, AccountInfoV2Model, AccountInfoV2Position, ExchangeInfoModel,
    ExchangeSymbol, Kline, LeverageBracketLevel, LeverageBracketModel, OrderModel, OrderSide,
    OrderStatus, OrderType, PositionSide, PriceMatch, SelfTradePreventionMode, Symbol,
    SymbolBrackets, SymbolFilters, SymbolStatus, TimeInForce, WorkingType,
};

#[inline(always)]
fn expect_byte(body: &[u8], pos: &mut usize, expected: u8) -> bool {
    if *pos < body.len() && unsafe { *body.get_unchecked(*pos) } == expected {
        *pos += 1;
        true
    } else {
        false
    }
}

#[inline(always)]
fn read_bare_i64(body: &[u8], pos: &mut usize) -> Option<i64> {
    let (v, n) = parse_i64_fast(&body[*pos..])?;
    *pos += n;
    Some(v)
}

#[inline(always)]
fn read_next_i64(body: &[u8], pos: &mut usize) -> Option<i64> {
    if !expect_byte(body, pos, b',') {
        return None;
    }
    read_bare_i64(body, pos)
}

#[inline(always)]
fn read_next_quoted_f64(body: &[u8], pos: &mut usize) -> Option<f64> {
    if !expect_byte(body, pos, b',') || !expect_byte(body, pos, b'"') {
        return None;
    }
    let (v, n) = parse_decimal_fast(&body[*pos..])?;
    *pos += n;
    if !expect_byte(body, pos, b'"') {
        return None;
    }
    Some(v)
}

#[inline(always)]
fn read_one_kline(body: &[u8], pos: &mut usize, k: &mut Kline) -> bool {
    let mut idx = *pos;
    if !expect_byte(body, &mut idx, b'[') {
        return false;
    }
    let Some(open_time) = read_bare_i64(body, &mut idx) else {
        return false;
    };
    let Some(open) = read_next_quoted_f64(body, &mut idx) else {
        return false;
    };
    let Some(high) = read_next_quoted_f64(body, &mut idx) else {
        return false;
    };
    let Some(low) = read_next_quoted_f64(body, &mut idx) else {
        return false;
    };
    let Some(close) = read_next_quoted_f64(body, &mut idx) else {
        return false;
    };
    let Some(volume) = read_next_quoted_f64(body, &mut idx) else {
        return false;
    };
    let Some(close_time) = read_next_i64(body, &mut idx) else {
        return false;
    };
    let Some(quote_volume) = read_next_quoted_f64(body, &mut idx) else {
        return false;
    };
    let Some(trade_count) = read_next_i64(body, &mut idx) else {
        return false;
    };
    let Ok(tc) = i32::try_from(trade_count) else {
        return false;
    };
    let Some(taker_buy_volume) = read_next_quoted_f64(body, &mut idx) else {
        return false;
    };
    let Some(taker_buy_quote_volume) = read_next_quoted_f64(body, &mut idx) else {
        return false;
    };
    let Some(end) = memchr(b']', &body[idx..]) else {
        return false;
    };
    k.open_time = open_time;
    k.close_time = close_time;
    k.open = open;
    k.high = high;
    k.low = low;
    k.close = close;
    k.volume = volume;
    k.quote_volume = quote_volume;
    k.taker_buy_volume = taker_buy_volume;
    k.taker_buy_quote_volume = taker_buy_quote_volume;
    k.trade_count = tc;
    *pos = idx + end + 1;
    true
}

/// Parses Binance Klines array straight into caller-provided `dst` buffer. Zero heap allocations.
#[inline]
pub fn parse_klines_into(body: &[u8], dst: &mut [Kline]) -> Option<usize> {
    let mut pos = 0usize;
    if !expect_byte(body, &mut pos, b'[') {
        return None;
    }
    if expect_byte(body, &mut pos, b']') {
        return Some(0);
    }
    let mut count = 0usize;
    loop {
        if count >= dst.len() || !read_one_kline(body, &mut pos, &mut dst[count]) {
            return None;
        }
        count += 1;
        if expect_byte(body, &mut pos, b']') {
            return Some(count);
        }
        if !expect_byte(body, &mut pos, b',') {
            return None;
        }
    }
}

pub fn parse_exchange_info(body: &[u8]) -> Option<ExchangeInfoModel> {
    let mut s = JsonScanner::new(body);
    if !s.expect(b'{') {
        return None;
    }
    let mut symbols: Option<Vec<ExchangeSymbol>> = None;
    if s.peek() != Some(b'}') {
        loop {
            let key = s.read_key()?;
            if key == b"symbols" {
                symbols = Some(read_exchange_symbols(&mut s)?);
            } else {
                s.skip_value()?;
            }
            if !s.next_comma_or_end(b'}').ok()? {
                break;
            }
        }
    } else {
        let _ = s.expect(b'}');
    }
    Some(ExchangeInfoModel { symbols: symbols? })
}

fn read_exchange_symbols(s: &mut JsonScanner<'_>) -> Option<Vec<ExchangeSymbol>> {
    if !s.expect(b'[') {
        return None;
    }
    let mut out = Vec::with_capacity(512);
    if s.peek() == Some(b']') {
        let _ = s.expect(b']');
        return Some(out);
    }
    loop {
        if let Some(sym) = read_exchange_symbol(s)? {
            out.push(sym);
        }
        if !s.next_comma_or_end(b']').ok()? {
            break;
        }
    }
    Some(out)
}

fn read_exchange_symbol(s: &mut JsonScanner<'_>) -> Option<Option<ExchangeSymbol>> {
    if !s.expect(b'{') {
        return None;
    }
    let mut item = ExchangeSymbol::default();
    let mut fits = false;
    if s.peek() != Some(b'}') {
        loop {
            let key = s.read_key()?;
            match key {
                b"symbol" => {
                    if let Some(sym) = s.read_symbol() {
                        item.symbol = sym;
                        fits = true;
                    }
                }
                b"status" => {
                    if let Some(b) = s.read_str_bytes() {
                        item.status = SymbolStatus::from_bytes(b);
                    }
                }
                b"filters" => {
                    read_filters(s, &mut item.filters)?;
                }
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
    Some(if fits { Some(item) } else { None })
}

#[derive(Clone, Copy)]
enum FilterKind {
    Other,
    Price,
    Lot,
    MarketLot,
    Notional,
}

fn read_filters(s: &mut JsonScanner<'_>, filters: &mut SymbolFilters) -> Option<()> {
    if !s.expect(b'[') {
        return None;
    }
    if s.peek() == Some(b']') {
        let _ = s.expect(b']');
        return Some(());
    }
    loop {
        read_filter(s, filters)?;
        if !s.next_comma_or_end(b']').ok()? {
            break;
        }
    }
    Some(())
}

fn read_filter(s: &mut JsonScanner<'_>, filters: &mut SymbolFilters) -> Option<()> {
    if !s.expect(b'{') {
        return None;
    }
    let mut kind = FilterKind::Other;
    let mut min_price = 0.0;
    let mut max_price = 0.0;
    let mut tick_size = 0.0;
    let mut min_qty = 0.0;
    let mut max_qty = 0.0;
    let mut step_size = 0.0;
    let mut notional = 0.0;

    if s.peek() != Some(b'}') {
        loop {
            let key = s.read_key()?;
            match key {
                b"filterType" => {
                    let v = s.read_str_bytes()?;
                    kind = match v {
                        b"PRICE_FILTER" => FilterKind::Price,
                        b"LOT_SIZE" => FilterKind::Lot,
                        b"MARKET_LOT_SIZE" => FilterKind::MarketLot,
                        b"MIN_NOTIONAL" => FilterKind::Notional,
                        _ => FilterKind::Other,
                    };
                }
                b"minPrice" => min_price = s.read_f64()?,
                b"maxPrice" => max_price = s.read_f64()?,
                b"tickSize" => tick_size = s.read_f64()?,
                b"minQty" => min_qty = s.read_f64()?,
                b"maxQty" => max_qty = s.read_f64()?,
                b"stepSize" => step_size = s.read_f64()?,
                b"notional" => notional = s.read_f64()?,
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

    match kind {
        FilterKind::Price => {
            filters.min_price = min_price;
            filters.max_price = max_price;
            filters.tick_size = tick_size;
        }
        FilterKind::Lot => {
            filters.min_quantity = min_qty;
            filters.max_quantity = max_qty;
            filters.step_size = step_size;
        }
        FilterKind::MarketLot => {
            filters.market_min_quantity = min_qty;
            filters.market_max_quantity = max_qty;
            filters.market_step_size = step_size;
        }
        FilterKind::Notional => {
            filters.min_notional = notional;
        }
        FilterKind::Other => {}
    }
    Some(())
}

pub fn parse_account_info_v2(body: &[u8]) -> Option<AccountInfoV2Model> {
    let mut s = JsonScanner::new(body);
    if !s.expect(b'{') {
        return None;
    }
    let mut model = AccountInfoV2Model::default();
    if s.peek() != Some(b'}') {
        loop {
            let key = s.read_key()?;
            if !s.try_consume_null() {
                match key {
                    b"positions" => model.positions = read_account_positions(&mut s)?,
                    b"assets" => model.assets = read_account_assets(&mut s)?,
                    b"feeTier" => model.fee_tier = s.read_i32()?,
                    b"feeBurn" => model.fee_burn = s.read_bool()?,
                    b"canTrade" => model.can_trade = s.read_bool()?,
                    b"canDeposit" => model.can_deposit = s.read_bool()?,
                    b"canWithdraw" => model.can_withdraw = s.read_bool()?,
                    b"multiAssetsMargin" => model.is_multi_assets_margin = s.read_bool()?,
                    b"tradeGroupId" => model.trade_group_id = s.read_i64()?,
                    b"totalInitialMargin" => model.total_initial_margin = s.read_f64()?,
                    b"totalMaintMargin" => model.total_maintenance_margin = s.read_f64()?,
                    b"totalWalletBalance" => model.total_wallet_balance = s.read_f64()?,
                    b"totalUnrealizedProfit" => model.total_unrealized_profit = s.read_f64()?,
                    b"totalMarginBalance" => model.total_margin_balance = s.read_f64()?,
                    b"totalPositionInitialMargin" => model.total_position_initial_margin = s.read_f64()?,
                    b"totalOpenOrderInitialMargin" => model.total_open_order_initial_margin = s.read_f64()?,
                    b"totalCrossWalletBalance" => model.total_cross_wallet_balance = s.read_f64()?,
                    b"totalCrossUnPnl" => model.total_cross_unrealized_profit = s.read_f64()?,
                    b"availableBalance" => model.available_balance = s.read_f64()?,
                    b"maxWithdrawAmount" => model.max_withdraw_amount = s.read_f64()?,
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
    Some(model)
}

fn read_account_assets(s: &mut JsonScanner<'_>) -> Option<Vec<AccountInfoV2Asset>> {
    if !s.expect(b'[') {
        return None;
    }
    let mut out = Vec::with_capacity(16);
    if s.peek() == Some(b']') {
        let _ = s.expect(b']');
        return Some(out);
    }
    loop {
        if !s.expect(b'{') {
            return None;
        }
        let mut a = AccountInfoV2Asset::default();
        if s.peek() != Some(b'}') {
            loop {
                let key = s.read_key()?;
                if !s.try_consume_null() {
                    match key.len() {
                        5 if key == b"asset" => a.asset = s.read_symbol().unwrap_or(Symbol::EMPTY),
                        10 if key == b"crossUnPnl" => a.cross_unrealized_profit = s.read_f64()?,
                        10 if key == b"updateTime" => a.update_time = s.read_i64()?,
                        11 if key == b"maintMargin" => a.maintenance_margin = s.read_f64()?,
                        13 if key == b"walletBalance" => a.wallet_balance = s.read_f64()?,
                        13 if key == b"marginBalance" => a.margin_balance = s.read_f64()?,
                        13 if key == b"initialMargin" => a.initial_margin = s.read_f64()?,
                        15 if key == b"marginAvailable" => a.margin_available = s.read_bool()?,
                        16 if key == b"unrealizedProfit" => a.unrealized_profit = s.read_f64()?,
                        16 if key == b"availableBalance" => a.available_balance = s.read_f64()?,
                        17 if key == b"maxWithdrawAmount" => a.max_withdraw_amount = s.read_f64()?,
                        18 if key == b"crossWalletBalance" => a.cross_wallet_balance = s.read_f64()?,
                        21 if key == b"positionInitialMargin" => a.position_initial_margin = s.read_f64()?,
                        22 if key == b"openOrderInitialMargin" => a.open_order_initial_margin = s.read_f64()?,
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
        out.push(a);
        if !s.next_comma_or_end(b']').ok()? {
            break;
        }
    }
    Some(out)
}

fn read_account_positions(s: &mut JsonScanner<'_>) -> Option<Vec<AccountInfoV2Position>> {
    if !s.expect(b'[') {
        return None;
    }
    let mut out = Vec::with_capacity(512);
    if s.peek() == Some(b']') {
        let _ = s.expect(b']');
        return Some(out);
    }
    loop {
        if !s.expect(b'{') {
            return None;
        }
        let mut p = AccountInfoV2Position::default();
        if s.peek() != Some(b'}') {
            loop {
                let key = s.read_key()?;
                if !s.try_consume_null() {
                    match key.len() {
                        6 if key == b"symbol" => p.symbol = s.read_symbol().unwrap_or(Symbol::EMPTY),
                        8 if key == b"leverage" => p.leverage = s.read_i32()?,
                        8 if key == b"isolated" => p.is_isolated = s.read_bool()?,
                        10 if key == b"entryPrice" => p.entry_price = s.read_f64()?,
                        10 if key == b"updateTime" => p.update_time = s.read_i64()?,
                        11 if key == b"positionAmt" => p.quantity = s.read_f64()?,
                        11 if key == b"maintMargin" => p.maintenance_margin = s.read_f64()?,
                        11 if key == b"maxNotional" => p.max_notional = s.read_f64()?,
                        12 if key == b"positionSide" => {
                            let b = s.read_str_bytes()?;
                            p.side = PositionSide::from_bytes(b);
                        }
                        13 if key == b"initialMargin" => p.initial_margin = s.read_f64()?,
                        14 if key == b"breakEvenPrice" => p.break_even_price = s.read_f64()?,
                        16 if key == b"unrealizedProfit" => p.unrealized_profit = s.read_f64()?,
                        21 if key == b"positionInitialMargin" => p.position_initial_margin = s.read_f64()?,
                        22 if key == b"openOrderInitialMargin" => p.open_order_initial_margin = s.read_f64()?,
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
        out.push(p);
        if !s.next_comma_or_end(b']').ok()? {
            break;
        }
    }
    Some(out)
}

pub fn parse_leverage_bracket(body: &[u8]) -> Option<LeverageBracketModel> {
    let mut s = JsonScanner::new(body);
    let mut symbols = Vec::with_capacity(512);
    match s.peek()? {
        b'{' => {
            if let Some(sb) = read_symbol_brackets(&mut s)? {
                symbols.push(sb);
            }
        }
        b'[' => {
            let _ = s.expect(b'[');
            if s.peek() != Some(b']') {
                loop {
                    if let Some(sb) = read_symbol_brackets(&mut s)? {
                        symbols.push(sb);
                    }
                    if !s.next_comma_or_end(b']').ok()? {
                        break;
                    }
                }
            } else {
                let _ = s.expect(b']');
            }
        }
        _ => return None,
    }
    Some(LeverageBracketModel { symbols })
}

fn read_symbol_brackets(s: &mut JsonScanner<'_>) -> Option<Option<SymbolBrackets>> {
    if !s.expect(b'{') {
        return None;
    }
    let mut item = SymbolBrackets {
        symbol: Symbol::EMPTY,
        brackets: Vec::new(),
        notional_coef: 1.0,
    };
    let mut fits = false;
    if s.peek() != Some(b'}') {
        loop {
            let key = s.read_key()?;
            match key {
                b"symbol" => {
                    if let Some(sym) = s.read_symbol() {
                        item.symbol = sym;
                        fits = true;
                    }
                }
                b"notionalCoef" => item.notional_coef = s.read_f64()?,
                b"brackets" => item.brackets = read_brackets(s)?,
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
    Some(if fits { Some(item) } else { None })
}

fn read_brackets(s: &mut JsonScanner<'_>) -> Option<Vec<LeverageBracketLevel>> {
    if !s.expect(b'[') {
        return None;
    }
    let mut out = Vec::with_capacity(12);
    if s.peek() == Some(b']') {
        let _ = s.expect(b']');
        return Some(out);
    }
    loop {
        if !s.expect(b'{') {
            return None;
        }
        let mut lvl = LeverageBracketLevel::default();
        if s.peek() != Some(b'}') {
            loop {
                let key = s.read_key()?;
                match key {
                    b"bracket" => lvl.bracket = s.read_i32()?,
                    b"initialLeverage" => lvl.initial_leverage = s.read_i32()?,
                    b"notionalCap" => lvl.notional_cap = s.read_f64()?,
                    b"notionalFloor" => lvl.notional_floor = s.read_f64()?,
                    b"maintMarginRatio" => lvl.maint_margin_ratio = s.read_f64()?,
                    b"cum" => lvl.cum = s.read_f64()?,
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
        out.push(lvl);
        if !s.next_comma_or_end(b']').ok()? {
            break;
        }
    }
    Some(out)
}

pub fn parse_order(body: &[u8]) -> Option<OrderModel> {
    let mut s = JsonScanner::new(body);
    if !s.expect(b'{') {
        return None;
    }
    let mut model = OrderModel::default();
    let mut found: u32 = 0;

    if s.peek() != Some(b'}') {
        loop {
            let key = s.read_key()?;
            if !s.try_consume_null() {
                match key.len() {
                    4 => match key {
                        b"type" => model.order_type = OrderType::from_bytes(s.read_str_bytes()?),
                        b"side" => model.side = OrderSide::from_bytes(s.read_str_bytes()?),
                        b"time" => model.create_time = s.read_i64()?,
                        _ => {
                            s.skip_value()?;
                        }
                    },
                    5 if key == b"price" => model.price = s.read_f64()?,
                    6 => match key {
                        b"symbol" => {
                            if let Some(sym) = s.read_symbol() {
                                model.symbol = sym;
                                found |= 1 << 1;
                            }
                        }
                        b"status" => {
                            model.status = OrderStatus::from_bytes(s.read_str_bytes()?);
                            found |= 1 << 3;
                        }
                        _ => {
                            s.skip_value()?;
                        }
                    },
                    7 => match key {
                        b"orderId" => {
                            model.order_id = s.read_i64()?;
                            found |= 1 << 0;
                        }
                        b"origQty" => model.quantity = s.read_f64()?,
                        _ => {
                            s.skip_value()?;
                        }
                    },
                    8 => match key {
                        b"avgPrice" => model.average_price = s.read_f64()?,
                        b"origType" => {
                            model.original_type = OrderType::from_bytes(s.read_str_bytes()?)
                        }
                        b"cumQuote" => model.filled_quote_quantity = s.read_f64()?,
                        _ => {
                            s.skip_value()?;
                        }
                    },
                    9 => match key {
                        b"stopPrice" => model.stop_price = s.read_f64()?,
                        b"priceRate" => model.callback_rate = s.read_f64()?,
                        _ => {
                            s.skip_value()?;
                        }
                    },
                    10 => match key {
                        b"updateTime" => model.update_time = s.read_i64()?,
                        b"reduceOnly" => model.is_reduce_only = s.read_bool()?,
                        b"priceMatch" => {
                            model.price_match = PriceMatch::from_bytes(s.read_str_bytes()?)
                        }
                        _ => {
                            s.skip_value()?;
                        }
                    },
                    11 => match key {
                        b"executedQty" => model.filled_quantity = s.read_f64()?,
                        b"timeInForce" => {
                            model.time_in_force = TimeInForce::from_bytes(s.read_str_bytes()?)
                        }
                        b"workingType" => {
                            model.working_type = WorkingType::from_bytes(s.read_str_bytes()?)
                        }
                        _ => {
                            s.skip_value()?;
                        }
                    },
                    12 => match key {
                        b"positionSide" => {
                            model.position_side = PositionSide::from_bytes(s.read_str_bytes()?)
                        }
                        b"priceProtect" => model.is_price_protect = s.read_bool()?,
                        b"goodTillDate" => model.good_till_date = s.read_i64()?,
                        _ => {
                            s.skip_value()?;
                        }
                    },
                    13 => match key {
                        b"clientOrderId" => {
                            if let Some(cid) = s.read_client_order_id() {
                                model.client_order_id = cid;
                            }
                            found |= 1 << 2;
                        }
                        b"closePosition" => model.is_close_position = s.read_bool()?,
                        b"activatePrice" => model.activation_price = s.read_f64()?,
                        _ => {
                            s.skip_value()?;
                        }
                    },
                    19 if key == b"selfTradePrevention" => {
                        model.self_trade_prevention_mode =
                            SelfTradePreventionMode::from_bytes(s.read_str_bytes()?)
                    }
                    23 if key == b"selfTradePreventionMode" => {
                        model.self_trade_prevention_mode =
                            SelfTradePreventionMode::from_bytes(s.read_str_bytes()?)
                    }
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

    if (found & 0b1111) != 0b1111 {
        return None;
    }
    Some(model)
}

