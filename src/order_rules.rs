//! Pre-flight order rounding and validation against exchange `SymbolFilters`.

use crate::primitives::{NewOrderRequest, OrderType, SymbolFilters};

const RELATIVE_TOLERANCE: f64 = 1e-12;
const STEP_TOLERANCE: f64 = 1e-9;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderRuleViolation {
    None,
    PriceBelowMin,
    PriceAboveMax,
    QuantityBelowMin,
    QuantityAboveMax,
    NotionalBelowMin,
}

#[inline(always)]
pub fn is_market_type(order_type: OrderType) -> bool {
    matches!(
        order_type,
        OrderType::Market
            | OrderType::StopMarket
            | OrderType::TakeProfitMarket
            | OrderType::TrailingStopMarket
    )
}

#[inline(always)]
pub fn round_price(filters: &SymbolFilters, price: f64) -> f64 {
    let tick = filters.tick_size;
    if tick <= 0.0 {
        return price;
    }
    let origin = filters.min_price;
    origin + ((price - origin) / tick).round() * tick
}

#[inline(always)]
pub fn round_quantity(filters: &SymbolFilters, quantity: f64, is_market: bool) -> f64 {
    let step = if is_market && filters.market_step_size > 0.0 {
        filters.market_step_size
    } else {
        filters.step_size
    };
    if step <= 0.0 {
        return quantity;
    }
    let origin = if is_market && filters.market_min_quantity > 0.0 {
        filters.market_min_quantity
    } else {
        filters.min_quantity
    };
    origin + (((quantity - origin) / step) + STEP_TOLERANCE).floor() * step
}

#[inline(always)]
fn is_below(val: f64, limit: f64) -> bool {
    val < limit - limit * RELATIVE_TOLERANCE
}

#[inline(always)]
fn is_above(val: f64, limit: f64) -> bool {
    limit + limit * RELATIVE_TOLERANCE < val
}

#[inline(always)]
pub fn check_price(filters: &SymbolFilters, price: f64) -> OrderRuleViolation {
    if filters.min_price > 0.0 && is_below(price, filters.min_price) {
        return OrderRuleViolation::PriceBelowMin;
    }
    if filters.max_price > 0.0 && is_above(price, filters.max_price) {
        return OrderRuleViolation::PriceAboveMax;
    }
    OrderRuleViolation::None
}

#[inline(always)]
pub fn check_quantity(filters: &SymbolFilters, quantity: f64, is_market: bool) -> OrderRuleViolation {
    let min_qty = if is_market && filters.market_min_quantity > 0.0 {
        filters.market_min_quantity
    } else {
        filters.min_quantity
    };
    let max_qty = if is_market && filters.market_max_quantity > 0.0 {
        filters.market_max_quantity
    } else {
        filters.max_quantity
    };
    if min_qty > 0.0 && is_below(quantity, min_qty) {
        return OrderRuleViolation::QuantityBelowMin;
    }
    if max_qty > 0.0 && is_above(quantity, max_qty) {
        return OrderRuleViolation::QuantityAboveMax;
    }
    OrderRuleViolation::None
}

#[inline(always)]
pub fn check_notional(filters: &SymbolFilters, price: f64, quantity: f64) -> OrderRuleViolation {
    if filters.min_notional > 0.0 && is_below(price * quantity, filters.min_notional) {
        return OrderRuleViolation::NotionalBelowMin;
    }
    OrderRuleViolation::None
}

/// Rounds and validates `req` in-place prior to transmission.
#[inline]
pub fn preflight_order(req: &mut NewOrderRequest) -> OrderRuleViolation {
    let is_market = is_market_type(req.order_type);
    if req.quantity > 0.0 {
        req.quantity = round_quantity(&req.filters, req.quantity, is_market);
        let qv = check_quantity(&req.filters, req.quantity, is_market);
        if qv != OrderRuleViolation::None {
            return qv;
        }
    }
    if req.price > 0.0 {
        req.price = round_price(&req.filters, req.price);
        let pv = check_price(&req.filters, req.price);
        if pv != OrderRuleViolation::None {
            return pv;
        }
        if !req.is_reduce_only && req.quantity > 0.0 {
            let nv = check_notional(&req.filters, req.price, req.quantity);
            if nv != OrderRuleViolation::None {
                return nv;
            }
        }
    }
    OrderRuleViolation::None
}

